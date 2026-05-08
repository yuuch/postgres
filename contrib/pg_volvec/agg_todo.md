# pg_volvec HashAgg 优化路线图

> 目标：把当前 Q1 专用、单锁串行、固定 256 组的 HashAgg 重构成 DuckDB 风格的 radix-partitioned + vectorized + 自适应 HashAgg，覆盖 TPC-H 主流 group-by 查询。

## 现状速查（baseline）

| 维度 | 现状 | 文件 |
|---|---|---|
| 支持 plan | `T_Agg + AGG_HASHED + AGGSPLIT_SIMPLE + numCols≥1` | `translator.cpp:798-816` |
| 支持 agg | `COUNT(*)`, `SUM(int4/numeric)`, `AVG(numeric)` | `translator.cpp:1060-1159` |
| 哈希表 | 8B entry `[row_idx:u32 \| salt:u16 \| pad]`，开放寻址 + 线性探测 | `aggregate_hash_table.{hpp,cpp}` |
| 容量 | `next_pow2(2*max_groups)`，硬上限 2048，`max_groups=256` 硬编码 | `pipeline_descriptor.cpp:139` |
| Probe | 全表 spinlock，串行查找 | `aggregate_hash_table.cpp:102-129` |
| 全局 combine | 单一全局 AHT + 单 spinlock | `aggregate_hash_table.cpp:165-210` |
| 哈希 | 标量 FNV-1a 逐字节 | `tuple_data_ops.cpp:176-224` |
| Update kernel | switch over `TdcAggKind`，逐行 | `tuple_data_ops.cpp:305-354` |
| Payload | 8B 对齐，固定宽度，无 null bitmap，无变长 | `tuple_data_layout.cpp` |
| Agg state | plain int64（含 numeric） | `tuple_data_ops.hpp:154-156` |
| JIT | 仅 deform/expr；HashAgg 标量 C++ | — |
| Batch size | 1024 | `types.hpp:15-21` |

## 关键缺口（vs DuckDB）

- ❌ 无 radix partitioning（DuckDB 4-8 bits / 16-256 分区）
- ❌ 无 spill / 外部聚合
- ❌ 无 perfect HT（Q1 仅 4 组，理应直接索引）
- ❌ Probe 全局加锁（DuckDB 局部 HT 完全无锁）
- ❌ salt 仅 16B、不复用 hash 高位做 probe 步进
- ❌ 无 prefetch loop、无向量化 probe
- ❌ 无 HLL 自适应扩容
- ❌ aggregate state 仍是 plain int64（README 提到的 Wide128 累加未落地）
- ❌ 全局 combine 走单 spinlock，并行扩展性差
- ❌ 无 MIN/MAX

---

## Phase 1 — 解锁扩展性与正确性（高 ROI，1-2 周）

### P1.1 拆解全局 combine 锁 → N-way 分区全局表
- [x] 引入 N-way 分区全局表（N = `next_pow2(num_workers * 4)`，clamp 8-32）
- [x] 每个分区独立 spinlock + 独立 `TupleDataCollection`
- [x] combine 时按 `hash >> (64 - log2(N))` 路由
- 改动：`aggregate_hash_table.{hpp,cpp}`、`physical_hash_aggregate.cpp`
- 预期：4 worker combine 阶段 3-4×；为 P2 铺路

### P1.2 移除硬编码容量 + 加 Resize
- [x] `max_groups` 从 plan 估算（`Agg.plan_rows` / `numGroups`），加 headroom
- [x] AHT 加 `Resize()`，load factor > 0.67 倍增 + batch rehash
- 改动：`pipeline_descriptor.cpp`、`aggregate_hash_table.cpp`
- 预期：解锁 Q3/Q10/Q13 等中等基数

### P1.3 升级 ht_entry 布局到 DuckDB 风格
- [x] `[16-bit salt | 48-bit row_index]`，去掉 pad
- [x] salt 取 hash 高 16 位
- [x] probe 步进用 hash 高 5 位 +1：`offset += (hash >> 59) | 1`
- 改动：`aggregate_hash_table.{hpp,cpp}`
- 预期：5-10% 总时间

---

## Phase 2 — Radix Partitioning 改造（核心，2-3 周）

### P2.1 Sink 阶段引入 radix 分区局部 HT
- [x] 新文件 `radix_partitioned_hash_table.{hpp,cpp}`（当前以内联 `HashAggPartition` wrapper 落地，后续可拆文件）
- [x] 初始 4 bits = 16 分区（当前与全局表对齐：`next_pow2(workers*4)`，clamp 8-32）
- [x] 每分区独立 AHT + payload TDC
- [x] sink 路径按 hash 高位选分区

### P2.2 Combine 阶段 partition-parallel merge
- [x] 每个 COMBINE task 领一个 partition，独立合并所有 worker 的同号 partition
- [x] partition-owner 分支不再持 global partition mutex；每 partition 单 task owner
- [x] partition 数与 P1.1 全局表对齐

> Phase 2B 已完成：HashAgg COMBINE fanout 改为 per-partition task；worker local partition DSA registry 只发布 `dsa_pointer`，partition-owner task 扫描所有 worker 同号 local partition 合并到同号 global partition。保留 `worker_index` 仅用于现有 queue affinity，不表达 partition ownership。

### P2.3 行宽自适应分区数
- [x] <32B → 8 bits；32-64B → 7 bits；≥64B → 6 bits（DuckDB `RadixHTConfig`）

### P2.4 PartitionedTupleData 重构
- [x] `TupleDataCollection` → partition-major 数组（HashAgg local/global payload 内部已 partition-major）
- [ ] 为未来 spill 铺路

预期：高基数 group by 2-4×；并行近线性扩展

---

## Phase 3 — Probe 与 kernel 向量化（1-2 周）

### P3.1 Vectorized batch probe
- [x] sink chunk 级预计算 hash/partition（P3.1-compatible prepass；probe/update 语义保持逐行）
- [x] sink probe/update 两阶段拆分：先收集 canonical `{partition,row}`，再批量 update（AHT probe 仍保持逐行）
- [x] sink 改 batch lookup（partition-local mini-batch；AHT 内部 batch find-or-insert 复用同一 salt/step/match 逻辑）
- [x] prefetch loop（batch metadata prepass）：算 slot/step/salt、touch 首探测 entry
- [x] main lookup loop 首槽 fast path：empty/salt-hit 直接处理，collision 才进入完整 probe walk
- [x] main lookup loop 多槽 vector salt compare → compare_vector / empty_vector
- [x] batch group compare：layout-driven 多候选比较（无 Q1 layout 特判）
- [x] batch group compare 多候选向量化
- [x] batch update（gather-update on row_ids）

### P3.2 哈希函数升级
- [x] `int4/int64` 特化（直接 mix；`HashGroup`/`HashGroupRow`/AHT rehash 共用 typed mixer）
- [x] FNV-1a → xxhash3 / Murmur finalizer（当前使用 SplitMix/xxhash 风格 finalizer；外部 xxhash3 评估留到 Phase 5 profiling）
- 预期：哈希 2-3×

### P3.3 Aggregate update kernel 向量化
- [x] layout-driven aggregate update fast path：移除 canonical Q1 predicate，按 `TdcAggregateDesc` 生成通用 delta
- [x] `UpdateAggregatesBatch()` API：generic layout-driven grouped-delta，避免逐行重复写 accumulator
- [x] generic grouped-delta batch update：chunk 内按 canonical row 合并 delta，减少重复 accumulator 写
- [x] update 改 `(tdc_base, row_width, row_ids, src row_indices, n)` gather
- [x] 编译器 SIMD 友好循环（固定数组 + unrolled/vectorizable gathers；不写 ISA-specific Q1 offsets）

预期：HashAgg 整体 1.5-2×

---

## Phase 4 — Perfect HT（Q1 杀手级，3-5 天）

### P4.1 PerfectAggregateHashTable
- [x] translator/descriptor 检测：所有 group cols 为单字节 `CHAR/BPCHAR` 窄域且编码空间 ≤1024（不硬编码 Q1）
- [x] local sink 直接索引数组：`row_ids[encode(g1, g2, ...)]`，miss 才 append canonical row
- [ ] 新 operator `PhysicalPerfectHashAggregate`（v1 先内嵌为 `PhysicalHashAggregate` perfect mode；global combine 仍复用 AHT）

预期：Q1 单线程再 2×；多线程 combine 几乎零成本

---

## Phase 5 — 高级特性（按需）

- [ ] HLL 自适应扩容（DuckDB PR #17236）
- [ ] Skip lookups（高唯一率，PR #15321）
- [ ] Spill / external HT（路线图 Q3+）
- [ ] Wide128 aggregate state（防 numeric SUM 溢出，复用 `expr.cpp:459-555`）
- [ ] MIN/MAX kernel（解锁 Q5/Q10）
- [ ] HashAgg JIT（最低优先级）

---

## 落地节奏

```
Week 1-2: Phase 1
Week 3-5: Phase 2
Week 6-7: Phase 3
Week 8:   Phase 4
Phase 5:  按 query 覆盖按需推进
```

---

## 进度追踪

### 当前状态：Phase 3 generic probe/update hot-path slices 已验证

| Phase | 状态 | 备注 |
|---|---|---|
| Phase 1 | ✅ 完成 | P1.1/P1.2/P1.3 已落地；P2 partition-owner merge 另行推进 |
| Phase 2 | ✅ 完成 | Phase 2B partition-owner merge 已完成并通过 canonical Q1；spill-ready P2.4 仅保留未来 spill 铺路 |
| Phase 3 | ✅ 完成 | typed hash mixer、batch lookup API、prefetch metadata、多槽 vector salt compare、layout-driven multi-candidate group compare、generic row-id gather update、generic grouped-delta update 已落地；不再依赖 canonical Q1 layout 特判 |
| Phase 4 | 🚧 进行中 | v1 perfect local sink 已落地：CHAR/BPCHAR 单字节窄域由 translator 生成 descriptor spec；未新增独立 operator，global combine 仍复用 AHT |
| Phase 5 | ⏳ 未开始 | |

### Changelog
- 2026-05-07: 完成 P1.3 AHT entry/probe 改造：entry packed 为 `[salt:16 | row_index:48]`，salt 取 hash 高 16 位，probe 使用 hash 高 5 位生成 odd step。
- 2026-05-07: 完成 P1.2 的容量估算接线：`PhysicalHashAggregate` 携带 `estimated_groups`，descriptor 序列化该值，本地/全局 AHT/TDC 均按估算分配；Resize 仍待实现。
- 2026-05-07: 完成 P1.1 分区全局 combine：HashAgg shared payload 改为 DSA wrapper，包含 8-32 个 partition，每个 partition 独立 spinlock、AHT、TDC；Combine 按 hash 高位路由。
- 2026-05-07: 完成 P1.2 Resize：AHT 在 load factor ≥ 2/3 时新分配并 batch rehash；local/global partition TDC 支持容量不足时 grow + copy + rehash。
- 2026-05-07: 完成 Phase 2A：HashAgg local sink 改为 radix partition-major `{AHT,TDC}` 数组，sink 按 hash 高位路由；Combine 直接把 local partition i 合并到 global partition i；partition count 接入 row-width adaptive 上限。
- 2026-05-07: 完成 Phase 2B：HashAgg COMBINE 调度改为 per-partition fanout，`TaskDescriptor` 显式携带 `partition_id`，partition-owner 分支通过 DSA registry 合并所有 worker 同号 local partition；修复 worker exit 前未释放 C++ runtime/pipeline state 导致的 post-task segfault；canonical Q1 连跑 2 次通过（约 1976 ms / 1870 ms）。
- 2026-05-07: 启动 Phase 3 / P3.2：新增共享 typed hash mixer，`HashGroup`、`HashGroupRow` 与 AHT rehash 共用同一实现；Q1 的 `INT32` group key 不再走逐字节 FNV。canonical Q1 通过（约 1713 ms），w=8 row-count smoke 返回 4 groups。
- 2026-05-07: 完成 P3.1-compatible sink prepass：`SinkChunk()` 先对 1024-row chunk 预计算 `hashes[]` / `partitions[]`，后续 append/probe/rollback/`UpdateAggregates()`/resize 顺序不变。Meson build/install/restart 通过；canonical Q1 w=8 返回 4 groups，约 1747 ms。紧凑 `count(*) over grouped subquery` smoke 返回 4，但该 shape 当前按预期 fallback 到 PG。
- 2026-05-07: 完成 P3.3 canonical Q1 update fast path：`UpdateAggregates()` 增加纯 `TupleDataLayout` guard（2 个 INT32 group cols、8 个 agg、固定 src/offset/scale/row_width），匹配时走 unrolled `sum/avg/count` 更新，其他 layout 保持 generic switch。Meson build/install/restart 通过；canonical Q1 w=8 连跑返回 4 groups，约 1674 ms / 1600 ms。
- 2026-05-07: 完成 P3.1/P3.3 bridge slice：`SinkChunk()` 拆成 probe 阶段与 batch update 阶段，probe 阶段只记录 canonical `{partition,row_idx}`，resize 后重新解析 TDC row pointer，最后调用 `UpdateAggregatesBatch()`。AHT lookup/rollback/resize 语义不变，为后续真正 gather/SIMD update 铺路。Meson build/install/restart 通过；canonical Q1 w=8 连跑返回 4 groups，约 1654 ms / 1529 ms。
- 2026-05-07: 完成 P3.1 batch lookup API：新增 `AggregateHashTableFindOrInsertBatch()`，`SinkChunk()` 按 partition 组织 mini-batch，AHT 层在单次锁持有期间批量 append/scatter/probe/rollback，并复用单行 probe 的 salt/step/`MatchGroupRow` helper。resize 后再解析 canonical row pointer，避免旧 TDC 指针失效。Meson build/install/restart 通过；canonical Q1 w=8 连跑返回 4 groups，约 1836 ms / 1660 ms。prefetch/vector salt compare/batch group compare 仍待做。
- 2026-05-07: 完成 P3.1 prefetch metadata prepass：batch probe 入口先为每个 input 计算 `ProbeMeta{salt,slot,step}`，并 `__builtin_prefetch()` 首探测 entry；单行和 batch probe 均复用 `ProbeMeta` 版本的锁内 `FindOrInsertLocked()`，collision walk/claim/match 语义不变。Meson build/install/restart 通过；canonical Q1 w=8 连跑返回 4 groups，约 1618 ms / 1528 ms。vector salt compare / batch group compare 仍待做。
- 2026-05-07: 完成 P3.1 main lookup 首槽 fast path：batch probe 锁内先尝试 first-slot empty claim 或 salt-hit + `MatchGroupRow()`，只有 collision 才从第二个 probe slot 进入完整 `FindOrInsertFromSlotLocked()` walk。Meson build/install/restart 通过；canonical Q1 w=8 连跑返回 4 groups，约 1642 ms / 1558 ms。多槽 vector salt compare / batch group compare 仍待做。
- 2026-05-07: 完成 P3.1 batch group compare 的 Q1 fast path：导出 `IsCanonicalQ1HashAggLayout()`，`MatchGroupRow()` 命中 canonical Q1 时直接比较 offset 0/8 的两个 int32 group key，跳过逐列 `memcmp` loop；generic layout 保持原逻辑。Meson build/install/restart 通过；canonical Q1 w=8 连跑返回 4 groups，约 1655 ms / 1600 ms。真正多候选 vector group compare 仍待做。
- 2026-05-07: 完成 P3.3 Q1 grouped-delta batch update：`UpdateAggregatesBatch()` 在 canonical Q1 layout 下按 canonical row pointer 合并 chunk 内 `sum_qty/sum_base/sum_disc/sum_charge/sum_discount/count` delta，再对每个 group row 一次性写回 SUM/AVG pair/COUNT state；generic layout 仍 fallback。Meson build/install/restart 通过；canonical Q1 w=8 连跑返回 4 groups，约 1810 ms / 1621 ms。真正 row-id gather + NEON/AVX SIMD 仍待做。
- 2026-05-07: 完成 Phase 3 generic 化收尾：AHT batch probe 改为 unresolved-input iteration-major mini-vector，批量分类 empty/salt-hit/miss；salt-hit 通过 `MatchGroupBatch()` 做 layout-driven 多候选比较；移除 `IsCanonicalQ1HashAggLayout()` 及所有 Q1 offset/src-col 特判，`UpdateAggregatesBatch()`/`UpdateAggregatesGather()` 统一按 `TdcAggregateDesc` 合并 canonical row delta；hash finalizer 切到 SplitMix/xxhash 风格 finalizer。Meson build/install/restart 通过；canonical TPCH Q1 w=8 offload 返回 4 groups，`sum_*`/`charge`/`count_order` 与 native byte-exact（AVG 仍为既有 scale-2 简化）。
- 2026-05-08: 启动 Phase 4 v1：translator 基于 `ColumnSchema` 为所有 group columns 都是单字节 `CHAR/BPCHAR` 且编码空间 ≤1024 的聚合生成 `perfect_hash_capacity` descriptor spec；`PhysicalHashAggregate` 消费该 spec 进入 generic perfect local sink mode，用 `row_ids[encode(key)]` 直接定位 canonical row，避免 local AHT probe。Meson build/install/restart 通过；canonical TPCH Q1 w=8 offload 返回 4 groups，EXPLAIN ANALYZE 约 1708 ms。独立 `PhysicalPerfectHashAggregate` 与 global perfect combine 仍待做。
