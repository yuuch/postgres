# pg_volvec SIMD / cache-locality TODO

> 目标：把当前“列式存储 + 批处理调度”的 pipeline，推进到真正能吃到 SIMD / cache locality 红利的执行形态。

## 现状速查

| 层 | 现状 | 备注 |
|---|---|---|
| 存储 | `DataChunk` / `TupleDataLayout` 已列式 | 读写局部性比 tuple-at-a-time 好 |
| 执行 | 仍以 `for (row)` 标量循环为主 | 热点里大量 `memcpy` / per-row switch |
| SIMD | 没有显式 SIMD kernel | 依赖 LLVM 自动向量化，收益有限 |
| 过滤 | `SeqScan`/filter 是逐行 bool 判定 | 字符串前缀 LIKE 仍是标量前缀比较 |
| 投影 | `PhysicalProjection` 是逐行解释器 | 适合做 chunk-kernel 的第一候选 |
| 聚合 | HashAgg 有 batch update，但本质仍是标量 delta 合并 | 可以继续压缩为更少的写回次数 |
| Join | HashJoin 仍是逐行 probe 为主 | 对 Q14 这类 join-heavy query 影响很大 |

## 为什么 Q14 没快多少

Q14 主要是：

- join（`lineitem` × `part`）
- `CASE WHEN p_type LIKE 'PROMO%' ...`
- 两个 `SUM`
- 最后一个除法

这里最贵的部分不是纯算术，而是：

1. join / scan / hash probe 的标量循环
2. 字符串前缀判断不太适合直接 SIMD 化
3. 当前 projection/agg update 仍是 per-row 执行
4. 列式 layout 已存在，但还没转成“大块 fused kernel”

所以现在的收益更像：**cache-friendlier**，还没到 **SIMD-heavy**。

## 优先级 TODO

### P0 — 先把最热的 row loop 变成 chunk kernel
- [ ] `physical_projection.cpp`：把逐行 switch 改成 chunk-level kernel
- [ ] `output_sink.cpp`：减少 row-by-row `EncodeColumn`/`Scatter` 往返
- [ ] `tuple_data_ops.cpp`：把高频 copy/update 路径压成更少的批量写回

### P1 — 给 LLVM 一个更容易 vectorize 的形状
- [ ] `projection` / `agg update` 改成 straight-line SSA 风格
- [ ] 尽量 fuse `expr -> agg update`，减少中间 chunk 写回
- [ ] 给 dense/no-null 路径单独做 fast path
- [ ] 让常见 numeric op（mul/add/sub/div）更适合自动向量化

### P2 — 字符串/LIKE 专项优化
- [ ] 把 `LIKE 'prefix%'` 变成专门的 prefix kernel
- [ ] 允许短串直接走 inline prefix compare
- [ ] 尽量减少 `std::memcmp` 在每行上的调用次数

### P3 — Join / HashProbe SIMD 友好化
- [ ] Hash probe 预取更激进
- [ ] 批量处理多个 candidate row 的 hash/salt compare
- [ ] 减少分支爆炸，保留 cache-resident hot set

### P4 — 用 profiling 验证，而不是凭感觉
- [ ] 对 Q1 / Q6 / Q10 / Q14 跑 xctrace
- [ ] 对比 `expr` / `deform` / `hash_join` / `agg` / `sync_wait`
- [ ] 记录“SIMD 前后”同一 query 的 hot stack 变化

## 具体判定标准

达到以下任一项，说明 SIMD/局部性开始真正生效：

- `for (row)` 热点明显缩小，热点转向更少的 chunk kernels
- `expr` 与 `agg update` 的分支数下降
- `memcmp` / `memcpy` 频次明显下降
- Q14 这种 join-heavy query 出现可见的吞吐提升

## 当前建议顺序

1. 先做 `PhysicalProjection` chunk-kernel
2. 再做 `LIKE` prefix / numeric dense fast path
3. 再 fuse projection + aggregate update
4. 最后再碰 join SIMD / prefetch
