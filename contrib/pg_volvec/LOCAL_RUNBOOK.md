# pg_volvec Local Runbook

Last verified: 2026-04-02

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

2026-04-02 本地确认值为：

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

当前真正支持并验证的是 no-order 版本：

```bash
./installed/bin/psql -h /tmp -p 5432 -d tpch \
  -f contrib/pg_volvec/test_q1_no_parallel.sql
```

原始带 `ORDER BY` 的 Q1 可以跑，但不会真正 offload：

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

## 6. 2026-04-02 当前真实进展

### 已跑通的主路径

- 单表 `SeqScan -> optional qual -> Agg` 形状已可 offload
- Q1 no-order 支持形状已验证正确
- Q6 已验证正确
- tuple deform JIT 可以自动装载 `llvmjit` provider，不再需要手工 `LOAD 'llvmjit'`
- expression JIT 已真正接到执行路径里，不再只是代码里“有这个函数”

### 当前计划支持边界

目前仍然是窄支持面：

- 支持：`SeqScan`、根部 `Agg`、plan `qual`
- 不支持：`Sort`、`Join`、`Gather`、`Materialize`、`Limit`、`Subquery Scan`

所以完整带 `ORDER BY` 的 Q1 仍然不会真正由 `pg_volvec` 接管。

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
- 支持 query-driven 列裁剪
- 内联了 `scale=2` numeric fast path
- 可以自动装载 `llvmjit` provider

#### 4. JIT expr

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

- Q6：`pg_volvec` 结果与原生 PostgreSQL 一致
- Q1 no-order：`pg_volvec` 结果与原生 PostgreSQL 一致

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

### 最新 flame graph 信号

Q6 新 flame graph 的主要结论：

- `VecExprProgram::evaluate` 只剩约 `1.08%`
- `pg_volvec_jit_store_numeric_int64_fast` 已不再是热点
- 主瓶颈已经回到 I/O：
  - `ReadBufferExtended` 约 `67.39%`
  - `pread` 约 `49.89%`

这说明 expression JIT 这轮优化已经真正把原先解释执行那块打下去了。

## 7. 当前最值得继续收的方向

如果下一步继续做，优先级建议是：

1. `no-null` / `dense` 的更强 expr kernel specialization
2. `sum(expr)` 继续 fusion，做到 `acc += expr(...)`
3. `Sort` 支持，把完整 Q1 带 `ORDER BY` 也纳入 offload
4. Hash Join / 多表查询
5. 更完整的 benchmark / regression harness
