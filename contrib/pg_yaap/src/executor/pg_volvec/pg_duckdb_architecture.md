# PostgreSQL 基于 Pipeline 的多进程并行引擎架构指南
*(基于 DuckDB Morsel-Driven 架构的移植方案)*

## 1. 架构总览 (Architecture Overview)

本项目旨在将 DuckDB 极速的**基于 Pipeline 的 Morsel-Driven (数据块驱动) 多线程并行执行引擎**，移植为 PostgreSQL 的一个查询加速插件。

核心的架构冲突在于运行模型的差异：
*   **DuckDB**: **单进程多线程 (Single-Process, Multi-Thread)**。所有线程共享同一块 Heap 内存地址空间，可以通过原生指针极速共享状态。
*   **PostgreSQL**: **多进程 (Multi-Process, Process-per-Connection)**。采用 Leader-Worker 模型，Leader 进程（处理客户端连接的 backend）与多个动态拉起的 Worker 进程（BGWorker）之间内存完全隔离。若需协作，必须通过动态共享内存 (DSM/DSA)。

**核心工作流 (Leader-Worker 事件驱动协作模式)：**
1.  **准备阶段 (Leader)**：Leader 进程接收 PG 执行计划，将其翻译为跨进程安全的（基于 Offset/Index 的）`PhysicalOperatorTree`，并在**动态共享内存 (DSM/DSA)** 中完成所有的初始化资源分配（任务队列 Task Queue，事件依赖图 Event DAG）。
2.  **调度启动与唤醒 (Leader)**：Leader 进程触发整个 Event DAG 的根节点，调度器将处于叶子节点（无需前置依赖的 Source）的 Pipeline 拆分为初始的一批 Tasks，推入共享内存队列。此时，Leader 拉起（或唤醒）指定数量的 Worker 进程（例如 8 个）。
3.  **无情吞噬 (Workers)**：这 8 个 Worker 启动后，疯狂且并发地从共享调度器队列 (`TaskScheduler`) 中抢夺 Task 并执行。
4.  **事件流转与新任务生成 (Workers / Event System)**：由于 Morsel 机制，任务极快。
    *   **局部到全局 (Combine)**：每个 Worker 独立完成一个任务（耗尽 Morsel 预算）时，会**立即将其局部计算的状态 (Local State) 合并入全局状态 (Global State)**。
    *   **全局定型 (Finalize)**：当所有 Worker 都完成了该 Pipeline 的任务，Event 系统被通知。它会调度一个全局的 `PipelineFinishEvent`（通常是单进程执行），对刚刚合并好的全局状态做最终处理（如建好 Hash 表索引）。
    *   **解锁下游**：完成 Finalize 后，Event 系统自动递减下游依赖。如果下游 Pipeline 依赖清零，负责的 Worker 将直接将下游 Pipeline 拆分出新的 Tasks 压入队列。
5.  **结果返回与谢幕 (Leader & Workers)**：Leader 进程在这场狂欢中仅仅充当等待者，通过 `shm_mq` (共享消息队列) 接收最终由 Worker 发送过来的结果并转交客户端。Event DAG 走到尽头时，查询结束，所有 Worker 自动退场。

---

## 2. 内存隔离与共享边界 (Memory Isolation & Sharing Boundary)

理解什么是 Local 的，什么是 Global 的，决定了插件能否跑通以及多进程并发的性能上限。

### 2.1 必须放在进程私有内存 (Process-Local Memory) 中的数据
这些数据只在单个 Worker 进程内流转，**绝对不要**放入共享内存，以保证极致的 L1/L2 Cache 局部性和无锁计算性能。

*   **`DataChunk` (Morsel)**: 数据块在 Pipeline 操作符之间流转时，完全在当前执行该 Task 的进程堆内存中。
*   **`LocalSinkState`**: 管道的局部终点状态。每个 Worker 在处理 Morsel 时，在**自己私有内存**中建立局部状态（如局部 Hash 表），从而做到插入时完全无锁。
*   **`ThreadContext` / `ExecutionContext`**: 当前执行进程的上下文（如表达式计算的临时缓冲区）。

### 2.2 必须放在动态共享内存 (DSM / DSA) 中的数据
这些数据用于跨进程的任务调度、依赖协调以及 Pipeline 之间的数据交接。**特别注意：放在这里的结构绝对不能含有 C++ 原生指针！**

*   **跨进程安全的 `Pipeline` 与 `PhysicalOperator`**: 
    Leader 将算子树存入共享内存时，必须将原先的指针树改造为**连续内存数组**。算子间的父子关系、Pipeline 的依赖关系，必须通过**数组索引 (Index)** 或基于共享内存基址的**相对偏移量 (Offset / `dsa_pointer`)** 来表达。
*   **任务队列 (Task Queue)**: 无锁或基于 `LWLock` 保护的任务队列。
*   **事件依赖图 (Event DAG)**: 扁平化为数组存储在 DSM 中。计数器全部使用 `pg_atomic_uint32`。
*   **`GlobalSinkState` (Pipeline Breaker 产物)**: 供跨进程并发合并与下游 Pipeline 并发读取的全局状态。

### 2.3 Pipeline Breaker 的核心：Sink 状态流转三部曲 (Sink State Lifecycle)
在 DuckDB 架构中，Sink 不仅是逻辑终点，更是状态汇聚的中心。在多进程架构下，这里是跨越内存隔离墙的关键，严格遵循 `Local -> Combine -> Finalize` 三步曲。

**场景：并行 Hash Join 的 Build 阶段**
1.  **并行局部构建 (Local Sink)**: 
    *   8 个 Worker 并行从全局 Source 抢夺 Morsels。
    *   拿到 Morsel 的 Worker 将数据推进到 Sink 节点，在**进程私有堆内存**中写入 `LocalSinkState` (局部 Hash 表分片)。此阶段彻底无锁并发。
2.  **局部合并至全局 (Combine)**:
    *   当一个 Worker 的当前 Task 结束（Source 枯竭或达到限额）时，它会立刻调用 Sink 的 `Combine()`。
    *   该 Worker 将自己私有的 `LocalSinkState` 转移并**合并**到共享内存 (DSA) 中的 `GlobalSinkState` 里。
    *   *注意：因为此时有多个 Worker 可能同时处于 Combine 阶段，写入 `GlobalSinkState` 的过程必须用 PG 的共享锁 (`LWLock`) 或原子操作保护。*
3.  **全局定型与解锁下游 (Finalize)**:
    *   当所有的 Task 都执行完毕（所有 Worker 都完成了各自的 Combine 操作），触发 `PipelineFinishEvent`。
    *   该 Event 会生成一个单一的 `PipelineFinishTask`。
    *   拿到这个任务的某一个 Worker，将对 `GlobalSinkState` 执行最终的 `Finalize()` 操作（比如对刚刚用 LWLock 收集上来的各个 Hash 表分片建立全局指针数组）。
    *   **解锁下游**：Finalize 顺利结束后，该 Worker 通知 Event 系统，解开下游节点（如 Probe）的依赖。Probe Pipeline 被激活，立刻生成新的 Tasks 并推入调度器队列，等待所有处于空闲状态的 Worker 继续抢夺。

---

## 3. 核心组件映射 (Component Mapping: DuckDB -> PG)

| DuckDB 概念 (C++ / 单进程) | PostgreSQL 插件映射方案 (C & C++ / 多进程) |
| :--- | :--- |
| **`std::thread`** | **Background Workers (BGWorker)**: 由 Leader 进程动态拉起。不停从 Scheduler 取任务执行，无任务或报错时退出。 |
| **`ConcurrentQueue`** | **DSM 中的环形队列 / LWLock 保护的队列**: 存储跨进程安全的 Task 描述符。 |
| **`std::mutex` / `std::atomic`** | **`LWLock` / `pg_atomic_uint32`**: 分配在共享内存中的轻量级锁和原子变量。 |
| **`std::condition_variable`** | **`Latch` / `WaitEventSet`**: 队列为空时 Worker 调用 `WaitLatch()` 休眠；有新任务入队时被唤醒。 |
| **C++ Exceptions (`try-catch`)** | **异常隔离屏障**: C++ 异常必须在 Worker 入口处被 catch，将错误码写入 DSM 通知 Leader，随后 Worker 自行转为 PG `ereport` 并干净退出。**严禁直接跨越**。 |
| **`shared_ptr` / 原生指针** | **`dsa_pointer` / 基于基址的 Offset / 数组索引 Index**: 放入 DSM/DSA 的 Pipeline 和 Event 等结构必须完全脱水，使用索引寻址。 |

---

## 4. 插件目录结构设计 (Directory Structure)

为了保持代码的整洁，将 PostgreSQL C 宏生态与 C++ 向量化引擎解耦，建议采用以下目录结构：

```text
pg_duck_executor/ (项目根目录)
├── Makefile                      # PG 插件标准的 Makefile (使用 PGXS)
├── pg_duck_executor.control      # 插件控制文件
├── sql/                          # 插件安装 SQL (CREATE EXTENSION)
├── src/
│   ├── pg_entry/                 # 核心：处理与 PostgreSQL 内核 API、宏的交互
│   │   ├── pg_plugin_main.c      # _PG_init, Hook 注入点 (ExecutorStart/Run/Finish/End)
│   │   ├── pg_bgworker_main.c    # BGWorker 入口函数 (死循环取任务)
│   │   ├── pg_shmem_utils.c      # DSM / DSA 的申请、Attach 与销毁逻辑
│   │   ├── pg_type_converter.c   # PG TupleTableSlot <-> 引擎 DataChunk 转换
│   │   └── pg_error_barrier.cpp  # C++ 异常到 ereport 的转换层 (防火墙)
│   │
│   └── engine/                   # 核心：模仿 DuckDB 的 C++ 向量化执行引擎
│       ├── CMakeLists.txt        # 内部编译为静态库
│       ├── common/
│       │   ├── types/            # DataChunk, Vector 定义 (驻留私有内存)
│       │   ├── atomic_utils.hpp  # pg_atomic_uint32 的 C++ 封装
│       │   └── dsa_ptr.hpp       # 封装处理共享内存相对偏移量的智能结构
│       ├── execution/
│       │   ├── physical_operator.hpp  # 物理计划节点（必须设计为跨进程安全/扁平化结构）
│       │   ├── operator/              # 具体算子 (如 scan, join, aggregate)
│       │   └── physical_plan_generator.cpp # PG Plan -> 跨进程安全 OperatorTree 转换器
│       └── parallel/
│           ├── pipeline.hpp      # Pipeline 定义（基于 Index 或 Offset）
│           ├── pipeline_executor.cpp # 包含 Task 的 Execute() 以及关键的 PushFinalize()
│           ├── event_dag.hpp     # 共享内存友好的扁平化 Event DAG 数组与依赖流转机制
│           ├── task_scheduler.cpp# 共享内存队列管理、BGWorker 取任务与 Latch 唤醒逻辑
│           └── dsa_allocator.hpp # 封装对 PG DSA 的调用，用于 Global State 的分配
```