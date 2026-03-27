# Design Document: LLVM JIT Deform to DataChunk for pg_vec

## 1. 设计目标 (Objective)
在 `pg_vec` (向量化执行引擎) 中实现一个深度的 JIT 优化：参考 PostgreSQL 原生的 `llvmjit_deform.c`，利用 LLVM 动态生成一段直接从 `HeapTuple` 解析并写入 `DataChunk`（列式结构）的机器码。

核心目的是**彻底消除中间的 `Datum[]` 数组开销和运行时的 `if-else` 分支**，实现极致的行转列（Row-to-Columnar）性能。

## 2. 核心差异对比 (Core Differences)

| 特性 | 原生 PG `slot_deform_tuple` | 自定义 `jit_deform_to_chunk` |
| :--- | :--- | :--- |
| **目标存储** | `slot->tts_values` (Datum数组) | `DataChunk->columns[i]->data` (原生类型连续数组) |
| **装箱/拆箱** | 强制转为 8 字节 Datum (需要符号扩展等) | 保持原生大小 (如 int32 直接 store i32)，**零转换** |
| **内存分配** | 需要维护 `tts_values` 和 `tts_isnull` 中间数组 | 数据直接从 Buffer Pool 搬运到 DataChunk 目标内存 |
| **常量折叠** | 有限（不知道数据的最终去向） | 完美（地址计算合并，直接生成最优访存指令） |

## 3. 目标函数签名 (Target JIT Function Signature)
JIT 生成的机器码对应的 C 函数签名如下：
```c
void jit_deform_batch_to_chunk(HeapTuple *tuples, int num_tuples, DataChunk *chunk);
```
在 JIT IR 内部，我们将展开列解析逻辑，并考虑内层包含一个 `for (int row_idx = 0; row_idx < num_tuples; row_idx++)` 的循环，以便 LLVM 优化器能进行自动向量化 (Auto-Vectorization)。

## 4. 核心实现步骤 (LLVM IR Generation Steps)

1. **定义基本类型与函数签名**:
   构建 `HeapTuple`, `DataChunk` 对应的 LLVM Struct Type，并创建函数。
2. **循环遍历 TupleDesc (在 C 层面，JIT 编译期)**:
   针对每一列，生成专属的解析 IR。
3. **处理 Null Bitmap**:
   读取 `HeapTupleHeader->t_infomask`。如果该列可空，生成位测试指令；如果为 Null，直接在 `chunk->columns[i]->nullmap` 对应位置置 1。
4. **处理对齐 (Alignment) 与偏移**:
   生成 `att_align_nominal` 的 IR。如果前置列都是定长非空，LLVM Pass 会自动将其优化为常数偏移。
5. **直接存入 DataChunk (Direct Store)**:
   计算源地址 `v_val_ptr = v_data_ptr + v_offset`。
   计算目标地址 `v_dest_ptr = chunk->columns[i]->data + row_idx * sizeof(type)`。
   执行 `LLVMBuildStore(b, v_val, v_dest_ptr)`。
6. **更新偏移**:
   如果是变长列，生成读取 Varlena Header 长度的 IR；如果是定长列，直接偏移常数。

## 5. 预期收益 (Expected Benefits)
- 消除 `Datum` 转换指令。
- 消除中间数组分配，大幅提升 L1/L2 Cache 命中率。
- CPU 分支预测失败率降至极低。
- 性能预期在纯标量类型上相比原生 JIT 解析有显著提升。

## 6. 当前实现评审 (Current Review Notes)

### 6.1 当前草稿和真实执行路径之间的差距
- 文档里写的是 `HeapTuple *tuples + num_tuples + DataChunk *chunk` 的 batch JIT，但当前 `pg_vec` 的 deformer 接口实际是单 tuple 入口：
  `append_tuple(HeapTuple tuple, uint16 row, ...)`
- 这意味着如果直接把当前草稿接进现有执行器，LLVM 并不会自动得到“整批循环”的优化机会，编译收益会比文档设想的小得多。
- 当前 `pg_vec` 还存在 input filter、late materialization、deform-safe filter 前推等逻辑；如果 JIT deform 绕过这些阶段，容易把本来可以延后的 payload decode 又提前做掉，反而带来性能回退。

### 6.2 当前草稿里最容易出错的点
- Null bitmap 处理还是占位逻辑，不能直接默认开启。
- 变长列路径还没有正确推进 offset，也没有对 short varlena / packed varlena 做完整处理。
- 目前 `pg_vec` 的目标列并不是“原样 Datum/varlena 指针”，而是 `int32/date32/decimal64/string ref` 这些引擎内部布局；JIT 代码如果直接把 varlena 指针塞进 chunk，会产生语义错误。
- 编译 key 不能只看 `TupleDesc`；还必须包含 `DeformProgram` 本身，否则不同 target 子集会复用错函数。

### 6.3 性能回退风险
- 如果每个 query 都无条件编译一个 LLVM deformer，会有明显的 compile overhead，小查询和短扫描会变慢。
- 如果 JIT 版本只能处理“无 filter、无 late materialization”的路径，而当前热点 query 大量依赖 filter 前推，那么默认强制切到 JIT 可能比解释版更差。
- 对 `numeric/string` 为主的 TPC-H 路径，单纯把 fixed-width deform JIT 化并不能触及主热点；如果默认打开但命中不了这些热点路径，收益会很有限。

## 7. 安全落地建议 (Safe Rollout)

### 7.1 默认策略
- `pg_vec` 可以有自己独立的开关，比如 `pg_vec.jit_deform = on`，不受 PostgreSQL 普通 `jit` 成本阈值控制。
- 但“默认开”不等于“无条件使用”。运行时仍然应该有一个严格的 support check，只在当前 JIT 能正确覆盖的 deform program 上启用。

### 7.2 第一阶段支持范围
- 只支持：
  - dense prefix deform program
  - `NOT NULL`
  - fixed-width 4-byte byval 列
  - `int32/date32`
- 对 `numeric/bpchar/text/varchar/string ref` 以及任何需要 late materialization 的路径，继续回退到当前 hand-written deformer。

### 7.3 构建要求
- `pg_vec` 的 LLVM JIT 文件应当只在 PostgreSQL 顶层 Meson 启用了 LLVM 时才编译。
- 也就是说，整库需要先用类似下面的方式配置：
  - `meson setup build_dir -Dllvm=enabled ...`
- `contrib/pg_vec/meson.build` 再基于 `llvm.found()` 条件性加入 JIT 源文件和 LLVM 的 `dependencies/cpp_args`。

## 8. 2026-03-27 运行时验证结论 (Runtime Findings)

### 8.1 先前设计里的两个真实装载问题
- `pg_vec` 的 LLVM deform 实现最初直接引用了 `llvmjit` provider 内部符号，例如 `TypeSizeT`、`StructHeapTupleData`、`llvm_create_context()`、`llvm_get_function()`。
- 在 macOS 下，这些符号默认不会从 `llvmjit.dylib` 对外导出，因此会出现“能编过，但 `LOAD 'pg_vec'` 时 `dlopen()` 失败”的问题。
- 当前原型通过给 `src/backend/jit/llvm/llvmjit.c` 中 `pg_vec` 实际用到的少量符号显式加 `PGDLLEXPORT`，暂时打通了这个装载问题。
- 这仍然说明：当前 `pg_vec` JIT 方案依赖了 PostgreSQL `llvmjit` provider 的内部 ABI，而不是稳定的 public API。

### 8.2 provider 生命周期已经改成 `pg_vec` 可独立驱动
- 当前实现仍然直接调用 `llvmjit` provider 的内部接口：
  - `llvm_create_context()`
  - `llvm_mutable_module()`
  - `llvm_expand_funcname()`
  - `llvm_get_function()`
- 但释放阶段不再依赖 PostgreSQL 普通 `jit` GUC 是否开启。
- 这次新增了 `llvm_release_context_direct()`，`pg_vec` 在 scanner 析构时直接释放 LLVM context，因此不再需要先 `SET jit = on; SELECT pg_jit_available();` 这种 workaround。
- 这说明 `pg_vec.jit_deform` 现在已经基本实现了“独立开关、独立生命周期”，只是仍然耦合在 `llvmjit` 的内部 ABI 上。

### 8.3 当前支持面已经扩大到真实 TPC-H 路径
- 现在的 LLVM deform 不再只支持 dense prefix 的 `int32/date32`。
- 当前 support check 已经覆盖：
  - sparse target lists
  - `int32`
  - `date32`
  - `decimal64_s2`
  - `bpchar1`
  - `NOT NULL` attributes up to `last_att_index`
- 同时，JIT 路径已经允许与当前 `pg_vec` 的 input filter 共存。
- 但是这里的共存方式是：
  - JIT 只负责 tuple -> DataChunk materialization
  - input filter 仍然在后续 `filter_chunk()` 中执行
  - 也就是说，filter 逻辑本身还没有被 JIT 化

### 8.4 修复过的两个关键正确性问题
- 第一类 bug 是 tuple header 字段索引错误：
  - JIT 代码最初把 `HeapTupleHeaderData.t_hoff` 当成 field `2`
  - 正确值其实是 `FIELDNO_HEAPTUPLEHEADERDATA_HOFF = 4`
  - 这会直接导致 varlena 指针错位、`compressed pglz data is corrupt`、甚至 backend crash
- 第二类 bug 是 deform-safe filter 的控制流错误：
  - 当前 hand-written deformer 可以在 append 阶段直接完成一部分 input filter
  - 但 JIT 路径目前只 materialize，不做 filter
  - 如果后续 `filter_chunk()` 仍然按“deformer 已经做过 filter”处理，就会把所有行错误放行
- 这两个问题修完之后，`Q6` 和 `Q1` 的 JIT 结果已经和 hand-written deformer 对齐。

### 8.5 当前真实命中情况
- 现在 `Q6` 会命中：
  - `pg_vec: JIT deform enabled for input with 4 targets`
- `Q1` 也会命中：
  - `pg_vec: JIT deform enabled for input with 7 targets`
- 实际上，这说明 LLVM deform 已经从“只能打最小 probe”推进到了真实的 TPC-H case。

### 8.6 当前收益
- 在当前 10GB `tpch` 实例上的单次 wall-clock / `EXPLAIN ANALYZE` 结果：
  - `Q1`, `pg_vec.jit_deform = off`: `12943.447 ms`
  - `Q1`, `pg_vec.jit_deform = on`: `11036.844 ms`
  - `Q6`, `pg_vec.jit_deform = off`: `5112.024 ms`
  - `Q6`, `pg_vec.jit_deform = on`: `5104.300 ms`
- 也就是说：
  - `Q1` 当前大约快了 `14.7%`
  - `Q6` 基本打平
- 这非常符合当前架构现状：
  - `Q1` 需要更多列 materialize，JIT 把 tuple walk + decode/store 的固定开销压下去后更容易见到收益
  - `Q6` 的瓶颈已经更多转移到 filter / expression / aggregate 输入计算，所以单纯 deform JIT 收益有限

### 8.6.1 当前已支持查询的全量对比
- 2026-03-27 在当前 LLVM build 和 live `tpch` 10GB 实例上，对已经支持的 TPC-H 查询做了一轮串行 `pg_vec.jit_deform=off/on` 对比。
- 统一设置：
  - `LOAD 'llvmjit'`
  - `LOAD 'pg_vec'`
  - `max_parallel_workers_per_gather=0`
  - `max_parallel_workers=0`
  - `jit=off`
  - `pg_vec.enabled=on`
- 结果如下：

| Query | `jit_deform=off` | `jit_deform=on` | 变化 | 备注 |
| :--- | ---: | ---: | ---: | :--- |
| `Q1` | `13603.259 ms` | `11284.806 ms` | `-17.0%` | 真正命中 JIT deform |
| `Q3` | `10438.288 ms` | `10065.938 ms` | `-3.6%` | 部分输入命中 JIT，部分输入因类型限制回退 |
| `Q5` | `8897.076 ms` | `8838.330 ms` | `-0.7%` | 部分输入命中 JIT，整体几乎打平 |
| `Q6` | `6163.872 ms` | `6523.782 ms` | `+5.8%` | 真正命中 JIT deform，但整体略慢 |
| `Q7` | `12747.009 ms` | `11933.677 ms` | `-6.4%` | 部分输入命中 JIT |
| `Q10` | `11577.647 ms` | `11328.013 ms` | `-2.2%` | 部分输入命中 JIT |
| `Q12` | `13156.336 ms` | `9889.846 ms` | `-24.8%` | JIT 未命中，因 `late materialization is active` |
| `Q14` | `7794.512 ms` | `7661.243 ms` | `-1.7%` | JIT 未命中，因 `late materialization is active` |
| `Q19` | `9575.155 ms` | `9556.728 ms` | `-0.2%` | JIT 未命中，因 `late materialization is active` |

- 这里要特别注意三类情况：
  - `Q1/Q6`：单表路径，确实直接测到了 JIT deform 的真实影响。
  - `Q3/Q5/Q7/Q10`：多输入查询里只有部分输入能用 JIT deform；其余输入仍会打印
    `JIT deform currently supports only int32/date32/decimal64_s2/bpchar1 targets`
    并回退。
  - `Q12/Q14/Q19`：当前因为 `late materialization`，JIT deform 根本不会启用，所以这些变化不能归因于 JIT 本身。

### 8.7 当前最合理的结论
- LLVM deform 已经不是纯粹的“技术可行性演示”了；它已经能在真实 TPC-H `Q1/Q6` 上工作，并且在 `Q1` 上带来可见收益。
- 但它还不是“默认保证显著加速”的成熟特性，原因有三点：
  - 目前只 JIT 了 deform，本身没有 JIT filter / expression / join
  - late materialization 路径仍然不会命中 JIT
  - 当前仍然依赖 `llvmjit` provider 的内部 ABI，而不是稳定 public API
- 因而下一阶段最有价值的方向是：
  - 继续让 JIT 和 late materialization 共存
  - 评估是否把部分 deform-safe filter 一并 lower 到 LLVM
  - 再决定是否值得继续扩展到更重的 join / expression hot path
