# 基于 DSM/DSA 的 pg_volvec 内存中间层设计与改造方案

## 背景描述
目前 pg_volvec 在并行执行框架（QueryScheduler）中，处理如 Q20（Sort-Run 桥接）以及 Q9（多级 HashJoin 桥接）时，遇到了严重的“写盘瓶颈”。
根据 `gemini_review_issues.md` 报告：
1. **Q9 的多级 HashJoin**：中间产物通过 `SharedFileSet`（即底层 `BufFile`）强行落盘，缺乏中等规模数据的内存桥接层。
2. **Q20 的排序导出**：Worker 导出无序的 `DataChunk` 数据流到文件中，导致极高的系统调用和 I/O 放大。Leader 侧更是由于读取无序文件触发了全局重排。

**目标**：引入 PostgreSQL 的 DSM（Dynamic Shared Memory）和 DSA（Dynamic Shared Area）机制作为中间产物的交换媒介。优先将 Worker 处理的中间数据集缓存在 DSA 内存池中；仅在内存分配达到阈值（如 `work_mem` 或底层 DSA 扩展耗尽）时，才触发回退机制（Fallback）转为写盘。

---

## Proposed Changes

### 1. 核心架构设计: `DsaDataChunkBridge`
在原有的 `SharedFileSet` + `BufFile` 模型平级或外层，引入动态共享内存桥接器 `DsaDataChunkBridge`。
- **存储载体**：借助 PostgreSQL 的 `dsa_area` 创建一个可在所有并发 Worker 之间分配的共享内存池。
- **数据结构**：数据组织为 DSA Memory Blocks 链表，每个 Block 内能够序列化多个序列化之后的 `DataChunk`。使用 `dsa_pointer` 来作为分布式指针在多个 Process 间传递。
- **溢写感知 (Fallback to File)**：桥接器内部集成内存记账（Accounting），当累计分配的 DSA 空间超过预设阈值（例如 `work_mem`）或 `dsa_allocate_extended` 返回失败时，平滑降级（Spill to Disk），将后续的数据写入 `SharedFileSet` 产生的 `BufFile`。

***

### 2. HashJoin 的改造 (针对 Q9)

目前 Q9 执行流水线被波次（Wave）模式中的文件屏障打断。

#### [MODIFY] src/engine/parallel/runtime_execution.inc
- **生产者 (Hash Build)**：`HashBuildSource` 中的 Worker 导出中间结果时，先请求 `allocate_dsa_block()`。如果 DSA 成功分配，则进行 `memcpy` 将当前的数据压缩写入 `dsa_pointer` 对应的地址。
- **元数据同步**：Worker 完成当前阶段任务后，将会把产生的由一系列 `dsa_pointer` 组成的数组（或链表头指针）发布到固定/动态的主 DSM 控制结构中。
- **消费者 (Hash Probe / Finalize)**：`HashBuildFinalize` 读取对应的 `dsa_pointer`；由于其原生即处在同一 `dsa_area` 空间中，只要调用 `dsa_get_address` 即可转换为进程本地指针，直接读取结构并构建 Shared Hash Table，**彻底消除文件 I/O反序列化**。

#### [MODIFY] src/engine/parallel/runtime_lowering.inc
- 更新 `QueryScheduler` 下推调度策略：对于多层 HashJoin，允许将内存 HashBridge 配置为首选通信手段。

***

### 3. Sort/Merge 的改造 (针对 Q20)

Q20 瓶颈不仅在于文件 I/O，还在于 Worker “推无序数据”带来 Leader 巨大的全局 $O(N \log N)$ 排序负担。

#### [MODIFY] src/engine/parallel/runtime_worker_main.inc
- **Worker 局部排序 (Push-down Local Sort)**：Worker 在将其负责的一批数据（Morsel）处理完准备导出为 SortRun 之前，必须在本地使用 `std::sort` 或 `SimdSort` 生成有序结果。
- **DSA 导出**：按照前面设计的 `DsaDataChunkBridge`，将**有序流**写入分配的 DSA 块集合中，保留 `dsa_pointer`。
- **Leader 归并**：Leader 不需要执行全排序，而是拿到收集的 N 个 Worker 的 `dsa_pointer` 流集合作为一个 N-Way Merge-Sort 的游标源。以 $O(N \log K)$ 复杂度输出最终结果流，直接返回 PostgreSQL 的执行器输出槽。

***

### 4. 其它共用组件改造

#### [NEW] src/engine/core/parallel_dsa_bridge.hpp & .cpp
封装 `DsaDataChunkBridge` 及其配套元数据：
- `init_dsa_bridge(dsa_area* area)`
- `dsa_pointer append_data_chunk(chunk, &spilled_to_disk)`
- `DataChunkIterator get_chunk_iterator(dsa_pointer start_ptr, BufFile* spill_file)`

#### [MODIFY] src/engine/core/data_chunk.hpp
可能需要补全便于一键序列化/反序列化（例如规整为连续内存）到固定内存块的方法：`DataChunk::SerializeTo(char* buffer)` 和 `DataChunk::DeserializeFrom(const char* buffer)`。
