# pg_duckdbopt 当前架构与 DuckDB 对照

## 1. 设计哲学
`contrib/pg_duckdbopt` 现在不是“直接把 DuckDB upstream optimizer 嵌进来”的黑盒接入，而是一个 **DuckDB-like optimizer sandbox**：PG 侧先把 `Query` 翻译成一套本地逻辑树和表达式树，再在本地 IR 上逐步对齐 DuckDB 的优化器结构与行为。

*   **DuckDB 参考而非直接依赖**：目录下的 `duckdb/` 主要用来对照 upstream 实现，当前 Meson 构建不会把它链接进 extension。
*   **本地 IR 驱动**：`src/adapter/duckdb_adapter.*`、`src/planner/*`、`src/logical/*`、`src/optimizer/*`、`src/physical/*` 组成一套自包含的 logical/physical IR 与优化管线；其中 `src/logical/` 专门承接 planner/optimizer 共享的 expression/tree helper。
*   **执行仍然解耦**：当前仍是 EXPLAIN-only，重点是把 plan shape、join order、decorrelation 和 filter 行为稳定地暴露出来。

最近一轮边界整理已经把“共享 logical helper 误挂在 optimizer 下”和“physical operator 定义与 generator 混在一起”这两个遗留点清掉了。当前结构迁移后的状态已经重新跑过 `meson test --print-errorlogs`，回归仍然全绿。

## 1.1 当前草稿实现边界

当前 `contrib/pg_duckdbopt` 的边界应该这样理解：

*   构建方式：使用 Meson，`meson.build` 通过上一级安装目录的 `pg_config` 获取 PostgreSQL 18 server include、pkglibdir 和 extension 目录。
*   Hook 入口：`src/pg_duckdbopt.cpp` 注册 `planner_hook`，在 C++ 异常边界内尝试走插件规划，失败时回退到 `standard_planner` 或前一个 planner hook。
*   逻辑树：`src/adapter/duckdb_adapter.*` 定义了一组本插件内部的 DuckDB-like 逻辑节点和表达式节点，而不是直接使用 DuckDB upstream 的 `duckdb::LogicalOperator` 类。
*   优化器：本地 pipeline 已经不是 pass-through。当前已经显式拆成 `LogicalPlanner -> LogicalOptimizer -> PhysicalPlanner`：planner 侧通过独立的 `planner_normalizer` 模块承接 adapter 之后的 planner-normalization（目前至少包含已经迁入 `src/planner/` 的 `DecorrelateDependentJoin`、一轮 decorrelate 后的 EXISTS-style `MARK` cleanup，以及冗余 `DelimJoin` cleanup），optimizer 侧承接 `JoinPredicateExtraction`、`FilterPushdown`、`PredicatePropagation`、`ScanFilterFolding`、`CardinalityEstimator`、`JoinOrderOptimizer` 等 pure logical optimization pass。最近一轮里，本地 join IR 也已从“把 PG raw `JoinExpr` 枚举和值得特殊处理的 subquery join 混在一套数字里”修正成显式区分 `INNER/LEFT/FULL/RIGHT/SEMI/ANTI/MARK/SINGLE`。
*   Join order：本地实现已经拆成 query graph / filter info / relation manager / plan enumerator / reconstruct 等模块，形状上明显向 DuckDB DPhyp 靠拢。
*   Catalog/Stats：`src/catalog/pg_external_catalog.cpp` 仍然很薄，但 `optimizer_stats.*` 已经能读取 PG 侧部分统计信息做基数估算。
*   结果封装：`src/executor/duckdb_scan.cpp` 生成 `CustomScan`，把逻辑树与物理树 dump 到 explain 输出；`ExecDuckDBScan()` 仍显式报错，因此当前目标依然是 `EXPLAIN` 观察优化结果，而不是执行查询。

这个边界很重要：短期目标不是“一步到位接进 upstream DuckDB”，而是先把 **可观测、可回归、可逐步迁移** 的优化器骨架做实。

## 1.2 本地构建与验证

在仓库根目录执行：

```sh
meson setup --reconfigure contrib/pg_duckdbopt/build contrib/pg_duckdbopt
CCACHE_DIR=/tmp/pg_duckdbopt_ccache meson compile -C contrib/pg_duckdbopt/build
meson test -C contrib/pg_duckdbopt/build --print-errorlogs
```

产物：

```text
contrib/pg_duckdbopt/build/pg_duckdbopt.dylib
```

注意事项：

*   当前 macOS 构建使用 `-Wl,-undefined,dynamic_lookup`，让扩展在运行时从 PostgreSQL backend 解析符号。
*   如果 Meson 自动启用 ccache，而沙箱或 CI 不允许写 `~/Library/Caches/ccache`，需要把 `CCACHE_DIR` 指向 `/tmp` 或工作区内可写目录。
*   插件必须用目标 PostgreSQL 实例对应的 `pg_config` 编译。用 PG18 `pg_config` 编出的库不能加载进 PG19 backend，PostgreSQL 会报 server/library version mismatch。

## 2. 详细架构图 (逻辑库模式)

```text
PostgreSQL Process
      |
      | 1. Query Tree (Analyzed)
      v
+-----------------------------------------------------------+
| DuckPG Optimizer Wrapper (C++ Library Mode)               |
|                                                           |
|  +-----------------------------------------------------+  |
|  | [Step A] Translation Adapter                        |  |
|  | - Walk PG Query Tree                                |  |
|  | - Map Var(varno, attno) -> ColumnBinding(i, j)      |  |
|  | - Create DuckDB LogicalOperators (In-Memory)        |  |
|  | - Fallback to PG Planner for Unsupported Ops        |  |
|  +---------------------------|-------------------------+  |
|                              v                            |
|  +-----------------------------------------------------+  |
|  | [Step B] Metadata & Stats Bridge (External Catalog) |  |
|  | - Build Lightweight ExternalCatalog via PG RelCache |  |
|  | - Deep Map pg_statistic -> DuckDB BaseStatistics    |  |
|  |   (Histograms, MCV, NDV, null_count)                |  |
|  +---------------------------|-------------------------+  |
|                              v                            |
|  +-----------------------------------------------------+  |
|  | [Step C] DuckDB Optimizer Core (Algorithm)          |  |
|  | - Filter Pushdown Pass                              |  |
|  | - Join Order Optimizer (CBO - DP/Greedy)            |  |
|  | - Expression Rewriting & Constant Folding           |  |
|  | - Cost Model Calibration (Align with PG Executor)   |  |
|  +---------------------------|-------------------------+  |
|                              v                            |
|  +-----------------------------------------------------+  |
|  | [Step D] Result Reification                         |  |
|  | - Extract Optimized Tree Pointer                    |  |
|  | - Wrap into PG CustomScan Node                      |  |
|  +---------------------------|-------------------------+  |
|                              |                            |
+------------------------------|----------------------------+
      |
      | 2. PlannedStmt (CustomScan holding DuckDB Plan)
      v
PostgreSQL Executor (The "Perfect" Executor Interface)
```

---

## 3. 核心模块详解

### 3.1 翻译适配器 (The Adapter)
该模块将 PG 的 `Query` 对象（C 结构）转换为 DuckDB 的 `LogicalOperator` 对象（C++ 类）。
*   **绑定上下文 (Binding Context)**：维护一个映射表，将 PG 的 `varno` (Range Table Index) 转换为 DuckDB 内部算子的 `table_index`。
*   **算子递归构建**：
    *   `RangeTblEntry` -> `duckdb::LogicalGet`
    *   `JoinExpr` -> `duckdb::LogicalJoin`
    *   `TargetList` -> `duckdb::LogicalProjection`
*   **表达式对齐与 Fallback 机制**：将 PG 的 `OpExpr` 转换为 DuckDB 的 `BoundFunctionExpression` 等。由于 PG 使用 OID 标识操作符，而 DuckDB 使用 `ExpressionType`，此处存在**语义映射的鸿沟**。翻译器需要实现一个“安全子集”，如果遇到 DuckDB 无法识别的自定义类型或特殊操作符，必须立即 **Fallback 回退到 PG 原生 Planner**，以防 CBO 退化或失效。

### 3.2 统计信息与元数据桥接 (Metadata & Stats Bridge)
由于不使用 DuckDB 的存储引擎，优化器所需的代价数据必须手动“喂”入，且需要保证极高的保真度。
*   **外部目录桥接 (External Catalog)**：不要在每次优化时动态查询 RelCache，而是在插件启动时，利用 DuckDB 的 `ExternalCatalog` 接口构建一个轻量级的元数据缓存层。这比简单的 Mock 更能满足 `ClientContext` 对元数据的依赖，避免子查询展开时崩溃。
*   **统计信息深度映射 (Statistics Fidelity)**：
    *   **行数**：将 PG 的 `reltuples` 注入 `LogicalGet::cardinality`。
    *   **列统计深度翻译**：DuckDB 的 `JoinOrderOptimizer` 极度依赖高质量的统计信息。必须深入读取 `pg_statistic`，不仅要转换唯一值数（NDV）和空值比例，还需要将 PG 的**直方图数据 (Histograms)** 和**最常出现值 (MCV)** 深度翻译为 DuckDB 的内部统计结构（如 `BaseStatistics`）。这是触发 DuckDB 正确 Join Order 优化的核心难点。

### 3.3 当前三阶段驱动 (Planner / Optimizer / Physical Planner)
当前实现已经显式拆成三层，但还不是直接 new DuckDB `Optimizer`：

1. **`LogicalPlanner`**
   - 输入：PG analyzed `Query`
   - 输出：初始 logical plan
   - 当前职责：封装 `DuckDBAdapter::TranslatePGQuery(...)`，并在 `Normalize(...)` 中通过独立 `planner_normalizer` 组件承接 planner normalization；当前已把 `DecorrelateDependentJoin` 前移并物理迁入这一层，并在其后追加 EXISTS-style `MARK` filter/join cleanup 与 redundant `DelimJoin` cleanup
2. **`LogicalOptimizer`**
   - 输入：logical plan
   - 输出：优化后的 logical plan
   - 当前 pass 顺序：
     1. `JoinPredicateExtraction`
     2. `FilterPushdown`
     3. `PredicatePropagation`
     4. `ScanFilterFolding`
      5. `CardinalityEstimator`
      6. `JoinOrderOptimizer`
3. **`PhysicalPlanner`**
   - 输入：优化后的 logical plan
   - 输出：physical plan
   - 当前职责：从原来的内嵌 `PhysicalPlanGenerator` 中提升出来，作为独立阶段运行；公开入口现已拆到独立 `physical_planner.hpp/.cpp`，内部 generator 也已进一步拆到 `physical_plan_generator.hpp/.cpp`，再交给 `CustomScan` 封装 explain 输出

和 DuckDB upstream 的差距主要在于：虽然已经有了显式三阶段 driver，但还没有 DuckDB `optimizer.cpp` 里那种完整的 `RunBuiltInOptimizers()` orchestration 壳层；另外，planner 阶段虽然已经前移了 `DecorrelateDependentJoin`、一轮 EXISTS-style `MARK` cleanup，以及 redundant `DelimJoin` cleanup，但后面还需要继续把更多 mark/delim/subquery cleanup 收敛进 planner 层。当前这条边界也更干净了：planner 不再通过 `optimizer_core.hpp` 反向拿通用 tree/expression helper，而是改走中立的 `src/logical/`。最近一轮里，`planner_normalizer` 也已经开始从“一次性串行 3 个函数”向 Duck built-in normalizer 风格靠拢：`DecorrelateDependentJoin` 已补显式 change tracking，planner shell 现在会对 `decorrelate -> mark cleanup -> delim cleanup` 整轮做有界 fixed-point 收敛，而不再把 decorrelate 视为纯 one-shot；同时 adapter 也已把 `row_number()/rank()/dense_rank()/percent_rank()/cume_dist()` 这类当前本地 IR 能表达的 zero-arg window wrapper 翻进 `LogicalWindow`，让 correlated window wrapper case 不再直接 fallback 到 PG，aggregate wrapper 也开始像 DuckDB 那样 materialize correlated key，而不再让上层 projection 假性引用 aggregate child 的裸列；planner 侧在把 lifted equality 回挂到 join condition 时也开始去重，避免 `DISTINCT` wrapper 之类的 decorrelate case 在 explain 中出现重复条件。最新一轮回归还进一步锁定了更深一层的 wrapper 组合：带外层 `DISTINCT`、带中间 `projection/filter`、以及 `DISTINCT + filter` 的 correlated zero-arg window wrapper 现在都已有 explain regression 覆盖；此外，EXISTS-style marker filter 现在也能穿过 `LogicalWindow` wrapper，把 window 之下仍只被 `WHERE hit` 消费的 `MARK` join 继续收成 `SEMI`。再下一步里，adapter/physical explain 也已补上最小 `UNION ALL` setop 子树，因此 correlated `EXISTS (... UNION ALL ...)` 已能进入本地 IR 并通过 `LogicalSetOperation` / `PhysicalSetOperation` 观察计划；planner 侧的 setop-aware decorrelation 也不再只停留在保守 fallback：`PushDownSetOperation` 现在会沿 branch 的单子树 wrapper 链分析 correlated equality，并且在失败时保持事务式回退，不留下半改写状态；在此基础上，same-key `UNION ALL` 安全子集现在已经能稳定收成 semi join，而 mixed-key case 仍继续保守保留 dependent join。接下来最值钱的工作已经不再是继续挪目录，而是继续深化 planner normalization 和 `FlattenDependentJoins` 本身。

和 DuckDB upstream 对照时，当前最值得继续迁移的是 **filter family** 与 **decorrelation 后清理**，而不是盲目补一长串 execution-facing pass。像 `ColumnLifetimeAnalyzer`、`BuildProbeSideOptimizer`、动态 filter 这一类，当前本地 IR 还没有对应的数据结构，收益不会高。

### 3.4 结果封装 (CustomScan Wrapper)
将三阶段产出的 logical/physical plan 结果回传给 PG。
*   **CustomScan 节点**：在 PG 的 `PlannedStmt` 中插入一个 `CustomScan` 算子。
*   **私有数据区**：将 DuckDB 优化后的逻辑树根节点指针（`LogicalOperator*`）存放在 `CustomScan->custom_private` 中。
*   **接口转换**：通过 `CustomScanMethods`，让 PG 执行器在启动时能提取出这个指针，交给你的“完美执行器”。

---

## 4. 关键数据结构映射

| PostgreSQL 概念 (C) | DuckDB 概念 (C++) | 映射逻辑 |
| :--- | :--- | :--- |
| `Query` | `LogicalOperator` (Root) | 全局查询入口 |
| `RangeTblEntry` (REL) | `LogicalGet` | 对应叶子节点表扫描 |
| `Var (varno, varattno)` | `ColumnBinding` | 坐标系转换 |
| `OpExpr` (OID) | `BoundComparisonExpression` | 算子语义对齐 (需回退机制) |
| `pg_statistic` (MCV/Hist) | `BaseStatistics` | 统计信息深度注入 |
| `reltuples` | `TableStatistics::cardinality` | 基数注入 |
| `PlannedStmt` | `CustomScan` | 封装 C++ 指针的容器 |

---

## 5. 实施路线图建议

1.  **环境集成**：将 DuckDB 源码作为子模块，编译为静态库（剥离不需要的 Extension），由 PG 插件链接。
2.  **元数据缓存层搭建 (Metadata Bridge)**：利用 `ExternalCatalog` 创建一个全局单例的 `DatabaseInstance` 和 `ClientContext`，作为优化算法运行的健壮上下文。
3.  **翻译器与 Fallback 开发**：优先实现 `SELECT * FROM A JOIN B ON ...` 这一路径的翻译。定义一个“安全子集”，遇到不支持的特性立刻回退到 PG 优化器。
4.  **统计信息映射与 CBO 验证**：重点开发 `pg_statistic` 到 DuckDB `BaseStatistics` 的深度映射。通过观察不同数据分布下的 Join 顺序变化，验证 CBO 的有效性，并进行代价模型对齐。
5.  **封装回传**：通过 `CustomPath` 和 `CustomScan` 将结构返回给 PG 流程。

### 5.1 推荐的下一步落地顺序

1. 继续把 planner normalization 往 DuckDB `RunBuiltInOptimizers` 的形状收：先补更系统的 mark/delim/subquery cleanup orchestration，并继续把已工作的 wrapper 形态固化成回归。
2. 深化 `FlattenDependentJoins`：沿着已有的 operator-dispatch 框架继续补 correlated aggregate/join/window/setop 与更真实的 delim state。
3. 持续补 filter family：把 join-aware / semi-anti / wrapper-aware pushdown 做得更完整，并保持 explain 差异可观测。
4. 扩 `CardinalityEstimator`，优先补多列估算和更稳的 join cardinality。

### 5.2 必须尽早回答的架构问题

*   **目标是只借 DuckDB 的 join order / rewrite 结果，还是要完整保留 DuckDB logical plan？** 前者可以把优化结果反向映射回 PG Path/Plan，后者更偏向自定义执行器。
*   **执行端到底是谁？** 当前代码是 `EXPLAIN-only`，执行函数报错。如果最终由 PG executor 执行，应优先走 `CustomPath`/`Path` 生态并生成 PG plan；如果由 DuckDB/vectorized executor 执行，则需要定义 tuple slot、snapshot、MVCC 可见性和内存上下文边界。
*   **统计信息保真度的最低可用线是什么？** 只用 row count 会让 join reorder 很容易误判；但完整 MCV/histogram 映射成本很高，建议先确定 TPC-H 或目标查询集的最小统计需求。
*   **DuckDB upstream 依赖方式是什么？** 当前 repo 放了 DuckDB 源码目录，但 Meson 没有链接它。短期更现实的路径是继续“对照迁移行为”，而不是立即切成静态库直接接入。
*   **fallback 的粒度是什么？** 当前 fallback 是整个 query 回退 PG。后续如果要做子树级优化，需要定义 PG/DuckDB 两边表达式、参数、collation、null semantics 的严格边界。

---

## 6. 潜在风险与挑战总结

基于此架构，在实际落地时需要特别关注以下三个“摩擦点”：
1.  **表达式语义的“翻译失真”**：PG 和 DuckDB 操作符体系的差异可能导致 CBO 退化。必须依赖严格的 Fallback 机制。
2.  **统计信息的注入深度**：仅仅注入行数是不够的，Join Order 优化强依赖直方图和 MCV 数据的准确翻译，开发工作量较大。
3.  **ClientContext 的重度依赖**：优化过程中对元数据和状态的隐式依赖容易导致崩溃，必须依靠设计良好的 `ExternalCatalog` 作为桥梁来缓解。

---

## 7. 优势分析
*   **算法领先**：直接利用 DuckDB 优秀的多表连接优化（Join Reordering）和谓词下推能力，补齐 PG 在 OLAP 场景的优化短板。
*   **低侵入性**：不需要修改 PostgreSQL 源码，完全通过 Hook 和插件实现。
*   **轻量级**：由于不涉及执行和 IO，内存开销仅限于查询树的转换和优化过程。
