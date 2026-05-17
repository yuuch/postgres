# TPC-H todo

当前主线不再跟踪 `tpch_planner_todo.md`，后续统一记录在本文件。

## 当前状态

- 已修复并确认可在 10G 上通过 YAAP pipeline 执行：Q2、Q4、Q9、Q20。
- 已完成 60 秒超时 sweep（跳过 Q15 / Q19）。
- 当前剩余失败：Q13、Q16、Q18，均为 optimizer path admitted 之后 `lowering returned null`。
- 当前主线架构已经切到 **optimizer-only**：不再把 PG plan translator 视为支持路径。
- translator 内部后续统一向 **binding-first** 收敛，不再把 `varno/attno` 当作 optimizer path 上的权威列标识。

## 待处理顺序

1. Q13
   - 先复现 lowering 返回 null 的具体节点。
   - 对照 `pg_duckdb` 计划，确认是 optimizer 产物缺失还是 `yaap_opt_translator` 不支持。
   - 目标：先让查询稳定跑通，再回头细对齐 shape。

2. Q16
   - 重点看 anti join / distinct / top-N 相关 lowering。
   - 当前已定位到 binding dictionary 在多层 join / projection 输出顺序上的传播不稳定。
   - 优先修 join/projection/agg 的 output metadata 对齐，避免继续用 `varno/attno` 撞列。

3. Q18
   - 重点看 grouped subquery / semi join / order by limit 组合路径。
   - 这是三条里最可能涉及 correlated/grouped 子查询 lowering 的一条。

4. Q15
   - 修 fixture / explain 形式，不作为当前 lowering blocker。

5. Q19
   - 继续单独隔离处理，不能混进 sweep。

## 执行约束

- 计划对齐基线始终参考 `pg_duckdb` 导出的计划，不参考 PostgreSQL 原生 EXPLAIN。
- optimizer admitted 的查询必须从 optimizer physical plan 进入 `yaap_opt_translator`，不能改成 PG plan translator 兜底。
- 不再把 PG plan lowering 当作当前项目的受支持目标；相关遗留路径只算过渡代码，不作为修 bug 的依据。
- sweep 继续保持单条查询、60 秒超时、失败后继续后续查询。
- 每次修复后先只回归当前 query，再做批量 sweep。
