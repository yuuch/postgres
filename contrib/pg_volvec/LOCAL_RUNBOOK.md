# pg_volvec Local Runbook

Last verified: 2026-04-03

## 1. 项目定位

`pg_volvec` 是挂在 PostgreSQL `contrib/` 下的向量化执行器原型。当前实现通过 `ExecutorStart_hook` / `ExecutorRun_hook` 拦截受支持的计划子树，并把它们改走 C++ 侧的 `DataChunk` 批处理引擎。

关键入口：

- 扩展目录：`contrib/pg_volvec`
- hook 入口：`src/bridge/pg_volvec.c`
- 向量执行器入口：`src/engine/executor.cpp`
- 表达式 lowering / 解释执行：`src/engine/expr.cpp`
- 表达式 JIT：`src/engine/llvmjit_expr.cpp`
- tuple deform JIT：`src/engine/llvmjit_deform_datachunk.cpp`

## 2. Meson 编译与安装

### 已验证的 builddir

当前本地实际使用的是仓库根目录下的：

```bash
/Users/chenyunwen/proj/postgres/build
```

对应的关键配置是：

- `prefix=/Users/chenyunwen/proj/postgres/installed`
- `llvm=enabled`
- `buildtype=debugoptimized`

### 全新初始化

```bash
meson setup build \
  --prefix=/Users/chenyunwen/proj/postgres/installed \
  -Dllvm=enabled \
  --buildtype=debugoptimized
```

### 增量编译

```bash
meson compile -C build pg_volvec
```

### 安装

```bash
meson install -C build --only-changed
```

### 推荐的重装与重启顺序

这台机器上，安装和重启不要并行做。一个稳妥顺序是：

```bash
meson compile -C build pg_volvec
meson install -C build --only-changed
./installed/bin/pg_ctl -D ~/data/pg_tpch restart -m fast
```

如果需要把 `elog(LOG)` 稳定打到文件里，当前本地更稳的方式是显式带 `-l` 重启：

```bash
./installed/bin/pg_ctl -D ~/data/pg_tpch restart -m fast -l ~/data/pg_tpch/logfile
```

### 关键安装产物

```text
installed/bin/postgres
installed/bin/psql
installed/lib/pg_volvec.dylib
installed/lib/pg_volvec.so
installed/share/extension/pg_volvec.control
installed/share/extension/pg_volvec--1.0.sql
```

## 3. 启动 `~/data/pg_tpch`

当前本地实测实例信息：

- `PGDATA=~/data/pg_tpch`
- 二进制来自 `installed/bin/postgres`
- 监听端口 `5432`
- Unix socket 在 `/tmp`

### 启动

```bash
./installed/bin/pg_ctl -D ~/data/pg_tpch -l ~/data/pg_tpch/logfile start
```

### 停止

```bash
./installed/bin/pg_ctl -D ~/data/pg_tpch stop
```

### 状态检查

```bash
./installed/bin/pg_ctl -D ~/data/pg_tpch status
./installed/bin/pg_isready -h /tmp -p 5432
```

### 基本确认

```bash
./installed/bin/psql -h /tmp -p 5432 -d postgres -Atqc \
  "SHOW shared_preload_libraries; SHOW port; SELECT current_setting('server_version');"
```

2026-04-03 本地确认值为：

- `shared_preload_libraries = pg_volvec`
- `port = 5432`
- `server_version = 19devel`

## 4. 连接 `tpch` 并跑 TPCH 查询

### 连接

```bash
./installed/bin/psql -h /tmp -p 5432 -d tpch
```

### 推荐的 session 级设置

除非你就是在测并行交互，否则先关掉并行：

```sql
SET max_parallel_workers_per_gather = 0;
SET max_parallel_workers = 0;
SET min_parallel_table_scan_size = '1000GB';
SET parallel_setup_cost = 1000000000;
SET parallel_tuple_cost = 1000000000;
```

### Q1

no-order 版本仍然是更纯粹的单表 `Agg` 热路径入口：

```bash
./installed/bin/psql -h /tmp -p 5432 -d tpch \
  -f contrib/pg_volvec/test_q1_no_parallel.sql
```

原始带 `ORDER BY` 的 Q1 现在也已经可以真正 offload：

```bash
./installed/bin/psql -h /tmp -p 5432 -d tpch \
  -f contrib/pg_volvec/test_q1_10g.sql
```

### Q6

```bash
./installed/bin/psql -h /tmp -p 5432 -d tpch \
  -f contrib/pg_volvec/test_q6_10g.sql
```

### 小 SQL 回归

```bash
./installed/bin/psql -h /tmp -p 5432 -d postgres \
  -f contrib/pg_volvec/sql/q1.sql

./installed/bin/psql -h /tmp -p 5432 -d postgres \
  -f contrib/pg_volvec/sql/q6.sql
```

## 5. Profiling 与崩溃调试

### flame graph 默认流程

优先用 `sample -> stackcollapse -> flamegraph`：

```bash
/usr/bin/sample "$backend_pid" 5 1 -mayDie -file /tmp/pg_volvec.sample.txt
awk -f /Users/chenyunwen/proj/postgres/FlameGraph/stackcollapse-sample.awk \
  /tmp/pg_volvec.sample.txt > /tmp/pg_volvec.folded.txt
perl /Users/chenyunwen/proj/postgres/FlameGraph/flamegraph.pl \
  --title "pg_volvec backend" \
  /tmp/pg_volvec.folded.txt > /tmp/pg_volvec.flame.svg
```

### 先拿 backend pid 再 profile

```sql
LOAD 'pg_volvec';
SET client_min_messages = warning;
SET pg_volvec.enabled = on;
SELECT pg_backend_pid();
SELECT pg_sleep(1);
... heavy query ...
```

后台跑 `psql`，拿到 pid 后用 `sample`。

### 先 attach LLDB 再复现宕机

```sql
SELECT pg_backend_pid();
```

然后另一终端：

```bash
lldb -p <backend_pid>
```

在 LLDB 里继续执行：

```text
(lldb) process continue
```

回到原来的 `psql` 会话执行目标 SQL。一旦 backend fault，先抓：

```text
(lldb) bt
(lldb) thread backtrace all
(lldb) frame variable
```

这套流程对 `jit deform`、`expr JIT`、page-wise scan 都很有效。

## 6. 2026-04-03 当前真实进展

### 已跑通的主路径

- 单表 `SeqScan -> optional qual -> Agg` 形状已可 offload
- `Limit -> Sort -> Agg` 形状已可 offload
- inner `HashJoin` 链已可 offload
- `SubqueryScan` 已可 offload
- `MergeJoin` 计划形状当前可通过 hash-join-backed fallback 先跑通
- tuple deform JIT 可以自动装载 `llvmjit` provider，不再需要手工 `LOAD 'llvmjit'`
- expression JIT 已真正接到执行路径里，不再只是代码里“有这个函数”
- 第一版列式 `VecSortState` 已接上，用于 final-result in-memory sort
- scan 与 join 两侧列裁剪都已接上
- owned string storage 已接上，并且字符串列现在也可走 deform JIT

### 已验证 offload 的 TPC-H 查询

当前本地 `~/data/pg_tpch` 上，已经与原生结果逐项对齐的 offload 查询有 18 条：

- Q1
- Q3
- Q4
- Q5
- Q6
- Q7
- Q8
- Q9
- Q10
- Q11
- Q12
- Q13
- Q14
- Q15
- Q16
- Q18
- Q19
- Q20
- Q22

另外两条已经确认会真正 offload，但验证边界还更窄：

- Q2：当前 live 数据集上的 full native diff 还没补完，因为原生查询较慢
- Q17：TPCH 大表上已确认 offload，小表 exact check 已对齐，但 full native TPCH-side diff 还没补完

### 当前计划支持边界

目前支持面已经比最早宽很多，但仍然不是通用 SQL 执行器：

- 支持：
  - `SeqScan`
  - `qual`
  - `Agg`
  - `Sort`
  - `Limit`
  - inner `HashJoin`
  - Q13 当前用到的 right/left outer hash join 子集
  - `SubqueryScan`
  - `MergeJoin` 计划形状的 hash-backed fallback
  - Q22 当前用到的 right anti 计划形状的 hash-backed fallback
- 仍未完成：
  - 更广的 outer join
  - 真正的 nested-loop / merge-based semi/anti join
  - `Materialize`
  - `Gather`
  - 更广的 `count(distinct ...)`
  - 真正的 vectorized merge join

当前 `Sort` 仍然是第一版实现：

- blocking
- in-memory single run
- 通过 row-ref 间接排序
- 目前只覆盖 Q1 这类顶层 final sort 场景

### 这轮做过的关键能力

#### 1. 列裁剪

scan 阶段不再固定 deform 前 16 列，而是从：

- scan `qual`
- 上层非 `SeqScan` targetlist

收集真正需要的 `Var`，构造 pruned `DeformProgram`。Q6 现在只会取必要列，Q1 也只 deform 查询实际要用到的列。

#### 2. fixed-point numeric

TPC-H 常见 `NUMERIC(15,2)` 现在走：

- 输入列：scaled `int64`
- `SUM/AVG`：widened integer accumulation

这条路径已经把早期 `numeric_float8_no_overflow()` / `strtod()` 热点从 scan/deform 路上移掉了。

#### 3. JIT deform

当前 deform JIT：

- 运行时输入是 `HeapTupleHeader`
- 直接写到 `DataChunk` 目标列数组

#### 4. vectorized sort

当前 sort 路径：

- 先 drain child
- materialize 为 sort 自己持有的 dense payload chunks
- 抽出 sort key lane
- 对 row refs 做间接排序
- gather 回输出 chunk

第一版没有 spill，也没有 multi-run merge，但已经足够覆盖 full Q1。
- 支持 query-driven 列裁剪
- 内联了 `scale=2` numeric fast path
- 可以自动装载 `llvmjit` provider

#### 5. JIT expr

`VecExprProgram` 当前依然先 lowering 成线性 step IR，但热路径已经不是解释器了。

现在对支持的表达式，LLVM 会生成 fused row loop：

- `dense/no-selection` 有单独快路径
- 已有 selection 的 batch 仍然走 JIT，但用另一条路径
- 中间值只活在 LLVM SSA 里，不再物化为 `tmp[]` 向量列

这正是 `(a - b) * c` 这类表达式想要的形态：

```text
res[i] = (a[i] - b[i]) * c[i]
```

而不是：

```text
tmp[i] = a[i] - b[i]
res[i] = tmp[i] * c[i]
```

### 正确性验证

当前已验证对齐的 18 条 TPC-H 查询都和原生 PostgreSQL 结果对齐：

- Q1
- Q3
- Q4
- Q5
- Q6
- Q7
- Q8
- Q9
- Q10
- Q11
- Q12
- Q13
- Q14
- Q15
- Q16
- Q18
- Q19
- Q22

另外，Q21 当前已经能在本地 `~/data/pg_tpch` 上真正 offload 并跑完，但还没有做成和上面 18 条同等级的完整验证闭环；后续也不再把它当成默认的下一条执行器目标。当前判断更偏向于：Q21 在本地主要受 PostgreSQL 对多表 join 加子链接形状的计划质量影响，而不是首先受 `pg_volvec` 执行器能力限制。

### 当前本地性能结果

以下数字都来自 `~/data/pg_tpch` 本地实例，session 内关并行，且属于开发机热缓存工程测量：

#### Q6，3 轮交替 benchmark

- 原生 PostgreSQL 平均：`3.72s`
- `pg_volvec` 平均：`2.88s`
- 约 `1.29x` 加速

#### Q1 no-order，3 轮交替 benchmark

- 原生 PostgreSQL 平均：`21.83s`
- `pg_volvec` 平均：`4.87s`
- 约 `4.48x` 加速

#### 当前多条 query 的本地 checkpoint

- Q3：约 `1.45x`
- Q4：约 `1.05x`
- Q6：约 `1.18x - 1.29x`
- Q10：当前仍慢于原生，属于后续优化对象
- Q12：在字符串 deform JIT 接回后，已从明显落后收敛到小幅领先
- Q14：约 `1.23x`

### 最新 flame graph 信号

最近几轮 flame graph 的主要结论：

- Q6 上，表达式解释执行已基本不再是热点，主瓶颈回到 I/O
- Q12 上，字符串列重新接回 deform JIT 后，`nocachegetattr` 热点已经被打掉
- Q14 上，per-side join pruning 与 compact inner payload 之后，hash build 已不再是主瓶颈

这说明前一轮的主要收益已经从“功能通了”转向“哪些路径还值得继续优化”。

## 7. 当前最值得继续收的方向

如果下一步继续做，优先级建议是：

1. 继续收 Q1 / Q6 / Q10 / Q12 / Q14 这些已验证路径的性能热点
2. 继续收 Q2 / Q17 的完整 live-dataset validation closure
3. 建更完整的 benchmark / regression harness
4. 把 `count(distinct ...)` 从当前已验证的标量-key 子集继续泛化
5. 对 Q21 保持“已能 offload、但暂不继续追执行器”的判断，除非后面 planner 形状先变得更合理
