# Yet Another Analyze Process (YAAP)

简要概述

Yet Another Analyze Process（简称 YAAP）是一个面向 OLAP 的新型查询处理引擎，目标是成为一个可替换或增强 PostgreSQL 的分析执行层。项目由两大子系统组成：优化器（optimizer）与执行器（executor）。

设计目标

- 支持 TPC-H 级别的复杂分析查询并对其进行优化执行。
- 将优化器与执行器解耦，允许逐步替换 PostgreSQL 的planner与executor路径。
- 第一阶段目标：能正确且稳定地跑通 TPC-H 的 22 条查询。

当前状态（截至本说明）

- 执行器：能够接受 PostgreSQL 生成的 plan 并对若干典型用例执行（部分 Case 已可跑通）。
- 优化器：能解析 PostgreSQL 的查询并生成 YAAP 风格的 plan（部分 plan 已生成）。
- 集成：YAAP 的执行器尚未完全接入 YAAP 优化器生成的计划，当前执行器多使用来自 PG 的计划作为输入。
- 限制/已知问题：PostgreSQL 默认优化器在某些场景下效果欠佳；部分算子实现或边界条件仍需完善以覆盖 TPC-H 全集。

核心组件

1. 优化器（optimizer）
   - 输入：PostgreSQL 的解析产物 (parse tree/Query)。
   - 输出：YAAP 的物理计划（自定义 Plan 格式），目前能生成常见的扫描、过滤、连接、分组、聚合等算子组合。
   - 目标：生成更适合向量化执行器的物理计划（例如批次化扫描、列式传输、向量化 join/agg）。

2. 执行器（executor）
   - 输入：YAAP Plan（或暂时接受 PG Plan）。
   - 功能：面向列/向量的算子实现（scan、filter、projection、hash join、merge join、aggregation、sort 等），负责资源/内存管理与批处理控制。
   - 现状：已有基础算子与执行框架，能跑通部分典型用例。

设计/接口要点

- Plan 格式：建议定义一个清晰且紧凑的 YAAP Plan 结构（JSON 或 protobuf 可选），包括算子类型、输入输出 schema、分区/并行信息与算子参数。
- PG 兼容层：为尽量少改动 PG，提供一个 PlanAdapter，将 PG 的 Plan 转换为 YAAP Plan（并在优化器就绪后切换为直接使用 YAAP Plan）。
- 结果交互：定义 slot/tuple 与 YAAP 向量数据结构之间的映射策略，保证执行器能够以最小开销返回 PG 期望的结果。

快速上手（开发环境与构建）

前置要求：
- 已 checkout PostgreSQL 源码，并将本仓库放在 contrib/pg_yaap 目录下（当前路径即为本目录）。
- Meson + Ninja（与 PostgreSQL 的 Meson 构建体系兼容）。
- C/C++ 编译器、Postgres 开发头文件、LLVM（可选，如果使用 JIT）。

示例构建步骤（在仓库根目录运行）：

```bash
# 在 PostgreSQL 源码根目录
meson setup build --prefix=/path/to/installed --reconfigure
meson compile -C build contrib/pg_yaap
meson install -C build --only-changed
```

本地运行与验证：
- 启动本地 Postgres（使用安装目录的 bin/pg_ctl）并加载扩展（如果以 extension 形式集成）。
- 使用 psql 运行样例 SQL / TPC-H 查询，观察执行计划与结果。

TPC-H 验证（第一阶段目标）

准备：
- 使用 TPC-H 数据生成工具（dbgen）生成数据并加载到数据库中。
- 建议先使用 1GB 或 10GB 数据规模进行验收测试。

运行：
- 逐条运行 22 条 TPC-H 查询脚本，记录正确性（结果一致性）与性能（执行时间、内存峰值）。
- 建议先在单线程/禁用 PG 并行执行的环境下运行，以便对比 YAAP 的表现：
  - SET max_parallel_workers_per_gather = 0;
  - SET max_parallel_workers = 0;

开发注意事项与建议

- 边界覆盖：注意 Numeric/Decimal、Timestamp/Date 等类型在向量化路径的处理（是否需要缩放/精度处理）。
- 内存管理：统一内存上下文/arena，避免频繁分配导致性能波动。
- 逐步集成：优先实现并验证单表扫描 + agg、单表扫描 + filter、两个表的 hash join 等基础路径，再扩展到更复杂的 join chain 与子查询。
- 日志与可视化：为每条执行计划增加可选的详细执行日志（按算子逐批输出行数/耗时），便于性能调优。

短期路线图（建议里程碑）

- M1：定义 YAAP Plan 的序列化格式与 PG→YAAP 的适配器（接口契约）。
- M2：实现基础算子在执行器中的端到端执行（scan/filter/proj/agg/hash-join），通过单元测试验证正确性。
- M3：实现优化器到执行器的完整链路（优化器输出直接给执行器），并在小型 TPC-H 子集上通过验证。
- M4：覆盖 TPC-H 22 条查询，完成正确性验证并做性能基线对照。

贡献指南

- 提交前请先在 issue 中描述要实现的内容或 bug，以及复现步骤。
- Pull Request 要求：包含自测步骤、影响范围说明、必要时的基准脚本与对比数据。尽量把改动限制到最小模块范围。

联系方式

- 在仓库中使用 issue/PR 流程。提交问题时请附带复现用的 SQL、数据规模与期望结果。

致谢

- 本项目基于 PostgreSQL 架构与生态，借鉴了多个向量化执行引擎的设计理念。

---

（如果需要，后续可以把本 README 翻译为英文或拆分为多个文档：BUILD.md、DEVELOPMENT.md、ROADMAP.md。）
