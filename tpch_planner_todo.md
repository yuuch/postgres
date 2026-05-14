# TPC-H planner alignment todo

当前基线：

- 对照基线始终是 `pg_duckdb` 插件导出的 `Custom Scan (DuckDBScan)` 计划，不是 PostgreSQL 原生 EXPLAIN。
- 本轮 sweep 已重新跑过 `q1..q22`：
  - **YAAP EXPLAIN 成功**：Q1-Q14、Q16-Q18、Q20-Q22
  - **pg_duckdb EXPLAIN 成功**：Q1-Q14、Q16-Q18、Q20-Q22
  - **Q15**：fixture 语法问题，`EXPLAIN create view ...` 直接报错，不是 planner shape 问题
  - **Q19**：仍会触发 backend segfault，必须单独隔离，不能混在常规 sweep 中间

当前新增进度：

- **Q11**
  - 已修复 `HAVING sum(...) > scalar-subquery` 和顶层 aggregate projection lowering。
  - 10G `tpch` 上 YAAP 已可稳定走 `yaap_pipeline`。
  - 按 `pg_duckdb` 基线看，主干 shape 已基本对齐。
- **Q20**
  - 已修复 mixed numeric residual compare 和 projection key 绑定错位。
  - 10G `tpch` 上 YAAP 已可稳定走 `yaap_pipeline`。
  - 按 `pg_duckdb` 基线看，主干 shape 已基本对齐。
- **Q21**
  - 已修复 projection schema descriptor 的 16 列硬限制，以及 semi/anti join 误保留整行 build payload 的问题。
  - 10G `tpch` 上 YAAP 已可稳定走 `yaap_pipeline`。
  - 由于 PostgreSQL 原生在 full-scale 上不适合作为结果基线，后续若做结果校验，按小库验证。

当前已基本对齐的主集：

- Q1 / Q3 / Q5 / Q6 / Q7 / Q8 / Q9 / Q10 / Q11 / Q14 / Q20

当前已跑通、后续再细对齐的主集：

- Q21

剩余工作按下面顺序处理。

## 修复顺序

1. **Q2**
   - 症状：`LEFT_DELIM_JOIN [type=SINGLE]` 相关路径基数爆炸，顶层直接到 `1.18e17` 量级。
   - 价值：这是最典型的 **correlated scalar subquery / SINGLE delim join cardinality** 问题，修完大概率会同时改善 Q17，并给 Q11/Q20/Q22 的相关子查询路径打基础。
   - 目标：对齐 `ps_supplycost = (select min(...))` 这类相关标量子查询的 join/filter/cardinality 行为，避免外层行数被重复放大。

2. **Q17**
   - 症状：同样是相关标量子查询，当前 `LEFT_DELIM_JOIN [type=SINGLE]` 之后行数炸到 `9.6e11`，而 `pg_duckdb` 仍在 `1.2e7` 量级。
   - 关系：和 Q2 属于同一类 bug；Q2 修完后应立刻复测 Q17。
   - 目标：对齐 `l_quantity < (select 0.2 * avg(...))` 这种 correlated aggregate threshold 的 cardinality。

3. **Q18**
   - 症状：`LEFT_DELIM_JOIN [type=SEMI]` + grouped subquery 路径基数仍明显爆炸，顶层到 `6.26e16`。
   - 判断：更像 **SEMI delim join + grouped subquery cardinality** 问题，不完全等同于 Q2/Q17 的 SINGLE 路径，但共享 correlated-subquery 基数传播问题。
   - 目标：对齐 `having sum(l_quantity) > 300` 这一类 grouped semi-subquery 的基数与 group 数估算。

4. **Q22**
   - 症状：`ANTI` + scalar avg subquery 路径仍偏重，当前 shape 稳定但不够干净。
   - 关系：也依赖 scalar / delim 基数传播修正。
   - 目标：对齐 customer 过滤与 `NOT EXISTS` / 平均值子查询组合路径。

5. **Q4**
   - 症状：当前 semi-subquery shape 已经比较合理。
   - 目标：主要是确认和 `pg_duckdb` 的 SEMI join 位置与行数是否仍有明显差距。

6. **Q12**
   - 症状：scan/filter/join/agg 主干稳定。
   - 目标：主要做行数与过滤位置的细节校正。

7. **Q13**
     - 症状：`LEFT JOIN + grouped count distribution` 路径还没细对。
     - 目标：关注 left-join 后分布统计与 group cardinality。

8. **Q16**
     - 症状：ANTI join 计划可跑，当前优先级较低。
     - 目标：等前面的 correlated-subquery 类问题收敛后再做细对齐。

9. **Q15**
     - 不是 planner 修复项，先单独改 fixture：把 `create view revenue0 ...` 包装成合法的 explain/test 形式。

10. **Q19**
     - 仍是 crash blocker。
     - 必须继续最后处理，并保持单独运行；在前面的 planner backlog 清掉前，不把 Q19 混进常规 sweep。

## 当前判断

结论不是 “22 条已经都一样了”。

当前还明确没完全收敛的，主要是：

1. **Q2 / Q17 / Q18 / Q22**：相关子查询 + delim join + cardinality 传播
2. **Q4 / Q12 / Q13 / Q16**：主干基本合理，但还没做细对齐
3. **Q15**：fixture/explain 问题
4. **Q19**：crash blocker

下一步直接做 **Q2**。

如果 Q2 修复正确，优先复测：

1. Q17
2. Q11
3. Q20
4. Q22

因为这几条最可能共享同一批 `LEFT_DELIM_JOIN / SINGLE / scalar subquery cardinality` 修正。

## 本轮 sweep 中最值得记住的现象

1. **Q2/Q17/Q18** 是当前最明显的 planner cardinality 异常来源，已经不是单纯的 join-order 小偏差，而是数量级错误。
2. **Q15** 仍是 fixture 问题，不要误归因到 optimizer。
3. **Q19** 仍会把实例打崩，后续 sweep 必须继续把它放最后，或者单独跑。
