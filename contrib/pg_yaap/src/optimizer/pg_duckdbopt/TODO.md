# pg_duckdbopt 模块对照 TODO

这份表用来对照当前 `pg_duckdbopt` 的模块和 DuckDB 的对应实现，记录现在做到哪一步，以及下一步该补什么。**每次完成一轮有效开发后，都要同步更新这份文档**，避免代码进度和文档状态脱节。

当前状态补充：最近一轮目录边界整理已经完成，planner/optimizer 共享 helper 已从 `src/optimizer/` 抽到中立 `src/logical/`，`PhysicalPlanGenerator` 也已从 `physical_plan.hpp` 拆到独立 `physical_plan_generator.hpp/.cpp`。这轮结构迁移之后，`meson test --print-errorlogs` 仍保持全绿。

| 模块 | DuckDB 对应 | 当前状态 | 下一步 TODO | 优先级 |
|---|---|---|---|---|
| `planner + optimizer + physical pipeline` | `planner` + `optimizer.cpp` + physical planner | 已显式拆成 `LogicalPlanner -> LogicalOptimizer -> PhysicalPlanner -> CustomScan packaging` 三阶段 driver；`LogicalPlanner` 现已通过独立 `planner_normalizer` 模块承接 `DuckDBAdapter` 产出的初始 logical plan、位于 `src/planner/` 的 `DecorrelateDependentJoin`、EXISTS-style `MARK` cleanup，以及单独拆出的 redundant `DelimJoin` cleanup；planner/optimizer 共享的 expression/tree helper 已进一步抽到中立 `src/logical/`，`optimizer_core.hpp` 不再承载这些通用 helper，`PhysicalPlanGenerator` 也已从 `physical_plan.hpp` 拆到独立 `physical_plan_generator.hpp/.cpp`。此外，planner normalization 现在不再只是一次性串行调用，而是开始有了 DuckDB built-in normalizer 风格的 pass orchestration：`DecorrelateDependentJoin` 已补显式 change tracking，planner shell 会对 `decorrelate -> mark cleanup -> delim cleanup` 整轮做有界 fixed-point 收敛，而不再把 decorrelate 视为纯 one-shot；最新一轮里，EXISTS-style `MARK` filter 也已能继续穿过 `LogicalWindow` wrapper，下层 join 可进一步收成 `SEMI` | 继续把更多 DuckDB 式 planner normalization（尤其更完整 subquery rewrite / mark simplification / delim state 整理）补进 planner 阶段，并继续把 built-in orchestration 壳层做厚 | P0 |
| `FilterPushdown` | `filter_pushdown_optimizer` / `pushdown_*` | 已支持 `Filter -> CrossProduct`、`Filter -> Join(INNER/SEMI/ANTI/MARK/SINGLE 的现有策略)`、显式 `JOIN ... ON` 中单边 conjunct 下推、aggregate-aware pushdown（group-key filter 下推，aggregate-result filter 保留），以及基于 projection binding rewrite 的 alias/CTE/projection-over-join filter 下推；对 scalar boolean subquery 产出的 marker-only filter 仍保留 optimizer-side fallback，同时共享 DuckDB 风格 `CheckMarkToSemi` guard，marker 仍被上层 projection/aggregate 消费时不会误收。最新一轮里，本地 join IR 也已显式区分 `LEFT/FULL/RIGHT` 与 `SEMI/ANTI/MARK/SINGLE`，避免把 PG `JoinExpr` raw join type 错映射成 semi/anti family；对应 helper 已搬到 `src/logical/`，不再名义上隶属 optimizer | 继续补更完整的 projection/binder 重写与 mark-join 相关边界，逐步向 DuckDB 的 `pushdown_projection` / `pushdown_mark_join` 靠拢；planner/optimizer 两侧共享的 mark cleanup helper 还可继续扩展 | P0 |
| `JoinPredicateExtraction` | join predicate extraction | 已先于 filter pushdown 运行，可把 cross product 上的等值条件抽成 join，再让剩余 filter 继续下推 | 继续补 semi/anti/mark 相关边界 | P1 |
| `PredicatePropagation` | predicate propagation | 已能传播部分等价谓词 | 补更多列间等价类与常量传播 | P2 |
| `ScanFilterFolding` | scan filter folding | 已接入 | 补更多 scan 过滤折叠形态 | P2 |
| `CardinalityEstimator` | cardinality estimator / statistics | 已接 PG stats，能做基础估算 | 补多列估算、`count(distinct)`、更细 NDV 传播 | P0 |
| `DecorrelateDependentJoin` | subquery rewrite / dependent join flattening | 已不再只是 case-by-case strip；内部开始按 DuckDB `FlattenDependentJoins` 的思路重构成 operator-dispatch 的通用 decorrelate 框架，已拆出 `PushDownFilter/Projection/Aggregate/Distinct/Order/Window/Limit/Join/CrossProduct/Get` 入口，并已物理迁移到 `src/planner/decorrelate_dependent_join.{hpp,cpp}`，不再挂在 optimizer rule 体系里。 本地 IR 现已补上 `LogicalWindow`，而 adapter 也已接上 `row_number()/rank()/dense_rank()/percent_rank()/cume_dist()` window wrapper 的翻译，因此 correlated `ORDER BY/LIMIT` 不再只靠 guard 卡住，显式 zero-arg window wrapper case 也能进入本地 planner，并按 DuckDB `PushDownLimit` 的主思路改写成/保留为 window + filter；同时 `Projection/Distinct/Aggregate` 也开始 materialize correlated key，而不再把相关列藏在 child 里。最新一轮里，decorrelate 往 join condition 挂 lifted equality 时也开始去重，避免 `DISTINCT`-style wrapper case 在 explain 中出现重复条件；而 `DISTINCT + filter`、`projection + filter` 这类更深一层的 correlated window wrapper 组合也已补进回归，锁定它们继续走本地 planner/physical IR。再下一步里，`UNION ALL` setop 子树也已能进入本地 IR 和 explain；`PushDownSetOperation` 的失败路径已收紧成事务式，不再在安全子集判定失败时半途剥掉 branch filter，而 same-key `UNION ALL` 安全子集现在也已经能稳定从 dependent join 收成 semi join，mixed-key case 仍保守保留 dependent join | 继续把 correlated state 传递、更多 operator family（尤其更完整 join/window/setop/真实 delim state）和 deliminator-style cleanup 往 DuckDB 原版靠齐 | P0 |
| `JoinOrderOptimizer` | `join_order_optimizer` | 已切到 exact DPhyp + query graph 形态 | 继续把模块边界拆得更像 DuckDB 原版 | P0 |
| `DPhypEnumerator` | `PlanEnumerator` | 已按 DuckDB 的 CSG/CMP / CSG 递归走 | 后续抽成独立文件，并补更复杂 hyperedge 场景 | P0 |
| `DPhypQueryGraphEdges` | `QueryGraphEdges` | 已有本地版 query graph 抽象 | 把接口进一步贴齐 DuckDB 的 relation-set 语义 | P0 |
| `optimizer_stats` | statistics / cardinality helpers | 已能读 PG 统计信息并映射到内部 stats | 补更多统计来源与估算公式 | P1 |
| `physical_plan` | DuckDB physical operator builder | 已能把 logical plan 变成 explain 可见的 physical tree；physical operator 定义与 `PhysicalPlanGenerator` 现已分头放在 `physical_plan.hpp` 和 `physical_plan_generator.hpp/.cpp` | 补更多 physical operator 的语义对齐 | P1 |
| `duckdb_adapter` | binder / expression / operator glue | 本地 adapter 现已覆盖基础 subquery/window/wrapper 形态，并新增最小 `UNION ALL` setop 子树翻译，让 correlated `EXISTS (... UNION ALL ...)` 不再整条 fallback 到 PG；目前仍只支持 `UNION ALL`，setop-aware decorrelation 先保持保守 fallback，待 branch-key 匹配与 wrapper 提升路径稳定后再放开 | 继续扩 setop family，优先把 `UNION ALL` decorrelation 做成可稳定验证的安全子集，再考虑 `UNION`/`INTERSECT`/`EXCEPT` | P1 |
| `sql/join_order_graph.sql` | join order 回归样例 | 已覆盖基础 join graph | 增加 3 表/4 表/hyperedge/链式图回归 | P0 |
| `sql/subquery_decorrelation.sql` | correlated subquery 回归样例 | 已覆盖常见 EXISTS / IN / scalar subquery，以及 `EXISTS/IN` 在 scalar boolean 上下文里的 `MARK -> SEMI/ANTI` / 保留 `MARK` 边界；新增 nested correlated scalar aggregate、wrapper-aware nested `EXISTS`、correlated `row_number()/rank()/dense_rank()/percent_rank()/cume_dist()` window wrapper、`DISTINCT` wrapper 下 decorrelate join-condition dedup 回归、更深一层的 `DISTINCT/filter/projection` window-wrapper 组合 regression，以及 correlated `EXISTS (... UNION ALL ...)` 的两类回归：same-key 安全子集可收成 semi join，mixed-key case 继续保守保留 dependent join | 继续增加更复杂 nested correlated 场景 | P0 |
| `sql/query_wrappers.sql` | projection / distinct / order / limit | 已覆盖基础 wrapper，以及 alias/CTE/projection-over-join 的 filter 下推、aggregate group-key filter 下推、group-key 与 aggregate-output 混合谓词拆分、复杂 projection expression 的保守边界，以及 projected marker 不被误收成 `SEMI/ANTI` 的 safety case；新增裸投影 `EXISTS AS hit` 保持 `MARK` 的 planner-side regression，并补了显式 `LEFT JOIN` explain 回归来保护本地 join-type 编码；最新一轮再补 `window-over-hit` wrapper regression，保护 `WHERE hit` 能穿过 `LogicalWindow` 并把底层 `MARK` 收成 `SEMI` | 增加更多 wrapper 嵌套与边界输出 | P1 |
| `sql/scan_pushdown.sql` | scan pushdown 回归样例 | 已覆盖单表 filter 与 join-side filter 下推 | 增加 wrapper / subquery 组合场景 | P1 |

## 近期重点

1. 给 planner 阶段补更像 DuckDB `RunBuiltInOptimizers` 的 normalization orchestration 壳层，把 decorrelate 后的 mark/delim/subquery cleanup 继续往这里收。
2. 继续沿着 DuckDB `FlattenDependentJoins` 补 operator family，优先把 correlated aggregate/join/window/setop 和更真实的 delim state 往上推。
3. 补 `CardinalityEstimator` 的多列估算和 `count(distinct)`，让 join order/filter 选择更稳定。
4. 在目录边界已经整理完的前提下，继续补 filter family 和 wrapper-aware regression，把 explain 可观测差异做得更稳，并逐步把当前已工作的 window-wrapper 组合固化成长期回归。

## DuckDB 对照迁移优先级

1. **Planner normalization / subquery cleanup（最高优先级）**：目录边界已经基本理顺，下一步最值钱的是继续把 decorrelate 后的 mark/delim/subquery cleanup 收进 planner，逐步长成更像 DuckDB `RunBuiltInOptimizers` 的壳层。
2. **`FlattenDependentJoins` 深化**：继续沿着 `DecorrelateDependentJoin` 的 operator-dispatch 框架补 correlated state 传递，把 aggregate/join/window/setop/delim state 的差距继续缩小。
3. **Filter family**：继续沿着 `FilterPushdown` 往 DuckDB 的 join-aware / semi-anti / wrapper-aware pushdown 靠拢。这条线对 explain 输出仍然最敏感，只是现在不再需要先做目录清理。
4. **CardinalityEstimator 增强**：优先补多列估算、更稳的 join cardinality，以及 `count(distinct)` 这类会直接影响 join order/filter 选择的统计能力。
5. **暂不优先的 execution-facing pass**：`ColumnLifetimeAnalyzer`、`BuildProbeSideOptimizer`、动态 filter、真正可执行的 runtime/operator wiring 暂时不值得推进。当前插件目标仍是 EXPLAIN-only，本地 IR 也还没有为这些 pass 准备好对应结构。
