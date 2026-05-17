# pg_yaap refactor SQL todo

## Goal

在 optimizer-only executor 重构之后，按批次重新把 TPC-H SQL 带回 YAAP pipeline。当前只把 **Q1 / Q6** 当作已重新确认的基线。

约束：

1. 只看 YAAP optimizer physical plan，不再参考或接回 PG plan translator。
2. lowering 缺 metadata 时，先查 optimizer 没传什么，再决定 executor/runtime 要补什么。
3. 每次只扩大一小批 SQL，避免一次回归面过大。
4. `Q15 / Q19` 继续单独放着，不纳入当前 refactor sweep。

## Batch 0: baseline

- [x] Q1
- [x] Q6

这两条已经重新确认可执行，并且走 `pg_yaap_path: path=pipeline detail=yaap_pipeline`。

## Batch 1: scan/filter/agg 与简单 join

1. [x] Q14
   - 已确认单表 filter + aggregate + expression lowering 在 refactor 后稳定。
2. [x] Q3
   - 已确认最小 join + filter + order 路径可用。
3. [x] Q5
   - 已确认多表 join 聚合路径可用。
4. [x] Q7
5. [x] Q8
6. Q10

目标：先把没有 correlated / delim / anti-semi 特殊路径的主流 join 查询重新拉回。

## Batch 2: 之前通过过、但需要在新架构下重验的 query

1. Q2
2. Q4
3. [x] Q9
4. Q11
5. Q12
6. Q17
7. Q22

目标：验证 binding-first 输出字典在 projection / aggregate / semi join 组合路径上仍然成立。

## Batch 3: 当前已知困难 query

1. [x] Q16
    - 重点看 distinct / anti join / top-N 组合。
2. Q13
   - 先定位 admitted 后 lowering returned null 的具体节点。
3. [x] Q18
    - 重点看 grouped subquery / semi join / order by limit。
4. Q20
   - 重点看 `LEFT_DELIM_JOIN` / `DELIM_SCAN` / correlated key metadata。
5. Q21
   - 重点看 semi/anti join lowering 与 correlated metadata。

目标：困难 query 一律先做 “optimizer 缺什么 metadata / executor 需要什么 runtime 能力” 的归因，再动代码。

## Sweep order

完整 sweep 暂定按下面顺序恢复：

`Q1 Q6 -> Q14 Q3 Q5 Q7 Q8 Q10 -> Q2 Q4 Q9 Q11 Q12 Q17 Q22 -> Q16 Q13 Q18 Q20 Q21`

## Per-query checklist

每条 SQL 都按同样流程：

1. `EXPLAIN` 看 optimizer physical plan 是否合理。
2. 单条执行，`statement_timeout = '60s'`。
3. 确认日志里有 `pg_yaap_path: path=pipeline detail=yaap_pipeline`。
4. 如果 lowering 失败，记录缺失的 optimizer metadata 或缺失的 runtime/operator 能力。

## Current validated set after refactor

- Q1: OK, pipeline, ~5s
- Q3: OK, pipeline, ~9s
- Q5: OK, pipeline, ~9s
- Q6: OK, pipeline
- Q7: OK, pipeline, ~9s
- Q8: OK, pipeline, ~10s
- Q9: OK, pipeline, ~15s
- Q16: OK, pipeline, result matches PostgreSQL on 10G
- Q18: OK, pipeline, result matches PostgreSQL on 10G
- Q14: OK, pipeline, ~6s
