# Design Document: Parallel Deformer Workers for pg_volvec

## 1. 背景与动机 (Background & Motivation)
通过对 `pg_volvec` 扫描路径的性能分析（火焰图），我们发现：
*   **Page-wise Scan** 已成功消除了 MVCC 和行级管理开销。
*   **Deforming (行转列解析)** 占据了整个扫描阶段 **90% 以上** 的 CPU 时间。
*   传统的并行查询（Parallel Query）开销较重（涉及多套执行器状态同步、多个哈希表合并），且由于解析带宽限制，主进程依然容易成为瓶颈。

本项目提议一种**轻量级解析并行化**架构：将 CPU 密集型的 Deform 任务卸载到多个辅助 Worker 进程中，主进程作为消费者直接处理解析好的向量化 `DataChunk`。

## 2. 核心架构 (Architecture)

采用“一主多从”的流水线模型：

### 2.1 主进程 (Main Backend / Consumer)
*   **职责**:
    *   管理查询生命周期。
    *   分配扫描任务（Block 范围）给辅助 Worker。
    *   执行高层算子逻辑（如全局 Hash Aggregation、Join）。
    *   从共享内存队列中消费已就绪的 `DataChunk`。
*   **优势**: 专注于计算，不再受限于行存解析速度。

### 2.2 辅助解析进程 (Deform Workers / Producers)
*   **职责**:
    *   监听任务队列。
    *   从 Buffer Pool/ReadStream 读取 Page。
    *   执行 **JIT/C++ Deform**，将 Row 转换为 Columnar 格式存入 `DataChunk`。
    *   执行下推的 `Filter` 逻辑，仅传输匹配行。
    *   将填充完成的 `DataChunk` 发送至结果队列。
*   **优势**: 充分利用多核 CPU，通过增加“解析带宽”掩盖 Deform 开销。

## 3. 关键组件实现 (Implementation Details)

### 3.1 进程通信与内存管理
*   **动态后台进程 (Dynamic Background Workers)**: 使用 `RegisterDynamicBackgroundWorker` 在执行开始时按需启动。
*   **共享内存 (DSM)**: 使用 `dsm_create` 分配一块大内存，用于存放 `DataChunk` 环形缓冲区。
*   **消息传递 (shm_mq)**: 使用内核自带的 `shm_mq` 传递任务指令和 `DataChunk` 的元数据指针。

### 3.2 事务与可见性
*   **快照同步**: 主进程使用 `ExportSnapshot`，Worker 进程启动后立即 `ImportSnapshot`。
*   **一致性保证**: 确保 Worker 看到的每一行数据的可见性与主进程完全一致。

### 3.3 数据流动路径
1.  **Block Discovery**: 主进程通过 `TableScan` 快速确定待处理的 Block 范围。
2.  **Task Push**: 主进程将 Block Range 写入任务队列。
3.  **Parallel Parsing**: 多个 Deform Workers 并行读取 Block 并执行 Deform。
4.  **Vectorized Push**: Workers 将填满的 `DataChunk`（1024行）地址存入结果队列。
5.  **Compute**: 主进程 Pop `DataChunk` 并在其上运行向量化聚合/连接。

## 4. 优化平衡点 (Trade-offs & Tuning)

*   **Filter 下推**: 在 Worker 侧执行 Filter 可以极大地减少主进程需要处理的数据量和 IPC 通信量。
*   **Batch Size**: 任务分配的粒度（如每个任务 128 个 Block）需根据表大小和 CPU 核心数动态调整，以平衡负载均衡与调度开销。
*   **延迟优化**: 如果 Worker 解析速度极快，主进程几乎可以达到“零解析延迟”运行。

## 5. 扩展性评估 (Plugin Compatibility)

本方案**不需要修改 PostgreSQL 内核**：
*   所有 API 均属于 PostgreSQL 的插件公开 API。
*   核心逻辑完全封装在 `pg_volvec` 扩展内部。
*   与 `read_stream` 兼容：Worker 内部可以使用 `read_stream` 进行 I/O 预取。

## 6. 预期收益 (Expected Impact)

*   **吞吐量**: 扫描性能预期提升 **3x - 10x**（取决于分配的解析 Worker 数量）。
*   **CPU 利用率**: 消除主进程的单核解析瓶颈，实现真正的多核负载分担。
*   **响应时间**: 对于大表扫描类的 OLAP 查询，端到端延迟将显著降低。
