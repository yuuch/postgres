/*-------------------------------------------------------------------------
 *
 * pipeline_descriptor.cpp
 *	  Cross-process IR helpers (3g.2-final step 5/6).
 *
 * Q1 runtime model after the Step 6 refactor:
 *   - Exactly two pipelines are serialized.
 *       P0: SeqScan(lineitem)+qual -> HashAggregate(sink)
 *       P1: HashAggregate(source) -> Order(sink+source) -> OutputSink
 *   - HashAggregate is one operator instance; the PARTIAL/FINAL split is gone.
 *   - A COMBINE event runs between Sink Finalize and Source GetData on that
 *     same PhysicalHashAggregate, DuckDB-faithful and leader-driven.
 *
 * Step 5 scope here is descriptor-only: we serialize already-built operator
 * metadata (schemas, group keys, aggregate descriptors, TupleDataLayout DSA
 * pointers) and reconstruct process-local operator objects on workers. Layout
 * construction itself moves to Step 10 translator work, where Plan* is in
 * scope; descriptor code remains plan-agnostic.
 *
 *-------------------------------------------------------------------------
 */

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

#include <cstring>

#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/meta_pipeline.hpp"
#include "parallel/pipeline/output_sink.hpp"
#include "parallel/pipeline/physical_hash_aggregate.hpp"
#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/physical_order.hpp"
#include "parallel/pipeline/physical_perfect_hash_aggregate.hpp"
#include "parallel/pipeline/physical_projection.hpp"
#include "parallel/pipeline/physical_seq_scan.hpp"
#include "parallel/pipeline/pipeline.hpp"
#include "parallel/pipeline/pipeline_descriptor.hpp"

namespace pg_volvec {
namespace pipeline {

namespace {

static uint64
DependencyMask(const Pipeline &pipeline)
{
	uint64 mask = 0;
	for (PipelineId dep : pipeline.depends_on)
	{
		Assert(dep < 64);
		mask |= (UINT64_C(1) << dep);
	}
	return mask;
}

static void
SerializeUInt16Vector(const PgVector<uint16_t> &values,
					   dsa_area *dsa,
					   dsa_pointer *out_dp,
					   uint16_t *out_count)
{
	*out_count = static_cast<uint16_t>(values.size());
	if (values.empty())
	{
		*out_dp = InvalidDsaPointer;
		return;
	}

	*out_dp = dsa_allocate0(dsa, sizeof(uint16_t) * values.size());
	std::memcpy(dsa_get_address(dsa, *out_dp), values.data(), sizeof(uint16_t) * values.size());
}

static void
SerializeAggFuncVector(const PgVector<AggFuncDesc> &values,
					 dsa_area *dsa,
					 dsa_pointer *out_dp,
					 uint16_t *out_count)
{
	*out_count = static_cast<uint16_t>(values.size());
	if (values.empty())
	{
		*out_dp = InvalidDsaPointer;
		return;
	}

	*out_dp = dsa_allocate0(dsa, sizeof(AggFuncDesc) * values.size());
	std::memcpy(dsa_get_address(dsa, *out_dp), values.data(), sizeof(AggFuncDesc) * values.size());
}

static void
SerializeProjectExprVector(const PgVector<ProjectExprDesc> &values,
					   dsa_area *dsa,
					   dsa_pointer *out_dp,
					   uint16_t *out_count)
{
	*out_count = static_cast<uint16_t>(values.size());
	if (values.empty())
	{
		*out_dp = InvalidDsaPointer;
		return;
	}

	*out_dp = dsa_allocate0(dsa, sizeof(ProjectExprDesc) * values.size());
	std::memcpy(dsa_get_address(dsa, *out_dp), values.data(), sizeof(ProjectExprDesc) * values.size());
}

static void
SerializeProjectStepVector(const PgVector<ProjectStep> &values,
					   dsa_area *dsa,
					   dsa_pointer *out_dp,
					   uint16_t *out_count)
{
	*out_count = static_cast<uint16_t>(values.size());
	if (values.empty())
	{
		*out_dp = InvalidDsaPointer;
		return;
	}

	*out_dp = dsa_allocate0(dsa, sizeof(ProjectStep) * values.size());
	std::memcpy(dsa_get_address(dsa, *out_dp), values.data(), sizeof(ProjectStep) * values.size());
}

static void
EmitSeqScan(const PhysicalSeqScan &op, OpDescriptor &out)
{
	out.kind = OpKind::SEQ_SCAN;
	out.n_children = 0;
	out.body.seq_scan.relid = op.relid();
	out.body.seq_scan.input_schema = op.input_schema_dp();
	out.body.seq_scan.output_schema = op.output_schema_dp();
	out.body.seq_scan.qual_bytecode = op.qual_desc_dp();
	out.body.seq_scan.shared_payload = op.shared_payload_dp();
}

static void
EmitHashAgg(const PhysicalHashAggregate &op, OpDescriptor &out, dsa_area *dsa)
{
	out.kind = op.type() == PhysicalOperatorType::PERFECT_HASH_AGGREGATE
		? OpKind::PERFECT_HASH_AGGREGATE
		: OpKind::HASH_AGGREGATE;
	out.n_children = 0;
	HashAggOpBody &body = out.kind == OpKind::PERFECT_HASH_AGGREGATE
		? out.body.perfect_hash_agg
		: out.body.hash_agg;
	body.input_schema = InvalidDsaPointer;
	body.output_schema = InvalidDsaPointer;
	body.layout = op.layout_dp();
	body.shared_payload = op.shared_payload_dp();
	body.max_groups = op.max_groups();
	body.perfect_hash_capacity = op.perfect_hash_capacity();
	SerializeUInt16Vector(op.group_keys(), dsa, &body.group_keys, &body.n_group_keys);
	SerializeAggFuncVector(op.agg_funcs(), dsa, &body.agg_funcs, &body.n_agg_funcs);
}

static void
EmitOrder(const PhysicalOrder &op, OpDescriptor &out)
{
	out.kind = OpKind::ORDER;
	out.n_children = 0;
	out.body.order.input_schema = InvalidDsaPointer;
	out.body.order.sort_keys = InvalidDsaPointer;
	out.body.order.key_layout = op.key_layout_dp();
	out.body.order.payload_layout = op.payload_layout_dp();
	out.body.order.shared_payload = op.shared_payload_dp();
	out.body.order.n_sort_keys = 0;
	out.body.order._pad0 = 0;
	out.body.order.max_rows = 256;
}

static void
EmitOutput(const OutputSink &op, OpDescriptor &out)
{
	out.kind = OpKind::OUTPUT;
	out.n_children = 0;
	out.body.output.input_schema = op.input_schema_dp();
	out.body.output.layout = op.layout_dp();
	out.body.output.shared_payload = op.shared_payload_dp();
	out.body.output.tdc_max_rows = op.tdc_max_rows();
	out.body.output._pad0 = 0;
}

static void
EmitProjection(const PhysicalProjection &op, OpDescriptor &out, dsa_area *dsa)
{
	out.kind = OpKind::PROJECTION;
	out.n_children = 0;
	out.body.project.input_schema = op.input_schema_dp();
	out.body.project.output_schema = op.output_schema_dp();
	SerializeProjectExprVector(op.expr_descs(), dsa,
		&out.body.project.expr_descs, &out.body.project.n_exprs);
	SerializeProjectStepVector(op.steps(), dsa,
		&out.body.project.steps, &out.body.project.n_steps_total);
	out.body.project._pad0 = 0;
}

static std::unique_ptr<PhysicalOperator>
ReconstructOp(const OpDescriptor &op, ExecCtx &ctx)
{
	(void) ctx;
	switch (op.kind)
	{
		case OpKind::SEQ_SCAN:
			return std::make_unique<PhysicalSeqScan>(
				op.body.seq_scan.relid,
				op.body.seq_scan.input_schema,
				op.body.seq_scan.output_schema,
				op.body.seq_scan.qual_bytecode,
				op.body.seq_scan.shared_payload,
				const_cast<OpDescriptor *>(&op));

		case OpKind::HASH_AGGREGATE:
		case OpKind::PERFECT_HASH_AGGREGATE:
		{
			const HashAggOpBody &body = op.kind == OpKind::PERFECT_HASH_AGGREGATE
				? op.body.perfect_hash_agg
				: op.body.hash_agg;
			PgVector<uint16_t> group_keys;
			if (body.n_group_keys > 0 && DsaPointerIsValid(body.group_keys))
			{
				auto *keys = static_cast<uint16_t *>(dsa_get_address(ctx.dsa, body.group_keys));
				group_keys.assign(keys, keys + body.n_group_keys);
			}

			PgVector<AggFuncDesc> agg_funcs;
			if (body.n_agg_funcs > 0 && DsaPointerIsValid(body.agg_funcs))
			{
				auto *aggs = static_cast<AggFuncDesc *>(dsa_get_address(ctx.dsa, body.agg_funcs));
				agg_funcs.assign(aggs, aggs + body.n_agg_funcs);
			}

			if (op.kind == OpKind::PERFECT_HASH_AGGREGATE)
				return std::make_unique<PhysicalPerfectHashAggregate>(
					body.layout,
					std::move(group_keys),
					std::move(agg_funcs),
					body.shared_payload,
					body.max_groups,
					body.perfect_hash_capacity,
					const_cast<OpDescriptor *>(&op));

			return std::make_unique<PhysicalHashAggregate>(
				body.layout,
				std::move(group_keys),
				std::move(agg_funcs),
				body.shared_payload,
				body.max_groups,
				body.perfect_hash_capacity,
				const_cast<OpDescriptor *>(&op));
		}

		case OpKind::ORDER:
			return std::make_unique<PhysicalOrder>(
				op.body.order.key_layout,
				op.body.order.payload_layout,
				op.body.order.shared_payload,
				const_cast<OpDescriptor *>(&op));

		case OpKind::OUTPUT:
			return std::make_unique<OutputSink>(
				op.body.output.input_schema,
				op.body.output.layout,
				op.body.output.shared_payload,
				op.body.output.tdc_max_rows,
				const_cast<OpDescriptor *>(&op));

		case OpKind::PROJECTION:
		{
			PgVector<ProjectExprDesc> expr_descs;
			if (op.body.project.n_exprs > 0 && DsaPointerIsValid(op.body.project.expr_descs))
			{
				auto *exprs = static_cast<ProjectExprDesc *>(dsa_get_address(ctx.dsa, op.body.project.expr_descs));
				expr_descs.assign(exprs, exprs + op.body.project.n_exprs);
			}

			PgVector<ProjectStep> steps;
			if (op.body.project.n_steps_total > 0 && DsaPointerIsValid(op.body.project.steps))
			{
				auto *step_ptr = static_cast<ProjectStep *>(dsa_get_address(ctx.dsa, op.body.project.steps));
				steps.assign(step_ptr, step_ptr + op.body.project.n_steps_total);
			}

			return std::make_unique<PhysicalProjection>(
				op.body.project.input_schema,
				op.body.project.output_schema,
				std::move(expr_descs),
				std::move(steps),
				op.body.project.expr_descs,
				op.body.project.steps,
				const_cast<OpDescriptor *>(&op));
		}
	}

	elog(ERROR, "pg_volvec: unknown OpKind %u during reconstruction", (unsigned) op.kind);
	return nullptr;
}

}  /* namespace */

dsa_pointer
SerializeTupleDataLayout(const TupleDataLayout &layout, dsa_area *dsa)
{
	dsa_pointer layout_dp = dsa_allocate0(dsa, sizeof(TupleDataLayout));
	std::memcpy(dsa_get_address(dsa, layout_dp), &layout, sizeof(TupleDataLayout));
	return layout_dp;
}

dsa_pointer
LeaderSerializePipelines(MetaPipelineBundle &bundle, dsa_area *dsa)
{
	if (bundle.pipelines.empty())
		return InvalidDsaPointer;

	const size_t pipeline_count = bundle.pipelines.size();
	dsa_pointer root_dp = dsa_allocate0(dsa, sizeof(PipelineDescriptor) * pipeline_count);
	auto *root = static_cast<PipelineDescriptor *>(dsa_get_address(dsa, root_dp));

	for (const auto &pipeline_uptr : bundle.pipelines)
	{
		const Pipeline &pipeline = *pipeline_uptr;
		PipelineDescriptor &pd = root[pipeline.id];
		pd.pipeline_id = pipeline.id;
		pd.op_count = 2 + static_cast<int32>(pipeline.ops.size());
		pd.dependency_mask = DependencyMask(pipeline);
		pd.global_source_state = InvalidDsaPointer;
		pd.global_sink_state = InvalidDsaPointer;
		pg_atomic_init_u32(&pd.task_slot_next, 0);

		pd.ops = dsa_allocate0(dsa, sizeof(OpDescriptor) * pd.op_count);
		auto *ops = static_cast<OpDescriptor *>(dsa_get_address(dsa, pd.ops));

		int32 idx = 0;
		switch (pipeline.source->type())
		{
			case PhysicalOperatorType::SEQ_SCAN:
				EmitSeqScan(static_cast<const PhysicalSeqScan &>(*pipeline.source), ops[idx]);
				static_cast<PhysicalSeqScan &>(*pipeline.source).AttachDescriptor(&ops[idx]);
				idx++;
				break;
			case PhysicalOperatorType::HASH_AGGREGATE:
			case PhysicalOperatorType::PERFECT_HASH_AGGREGATE:
				EmitHashAgg(static_cast<const PhysicalHashAggregate &>(*pipeline.source), ops[idx], dsa);
				static_cast<PhysicalHashAggregate &>(*pipeline.source).AttachDescriptor(&ops[idx]);
				idx++;
				break;
			case PhysicalOperatorType::ORDER:
				EmitOrder(static_cast<const PhysicalOrder &>(*pipeline.source), ops[idx]);
				static_cast<PhysicalOrder &>(*pipeline.source).AttachDescriptor(&ops[idx]);
				idx++;
				break;
			case PhysicalOperatorType::PROJECTION:
				EmitProjection(static_cast<const PhysicalProjection &>(*pipeline.source), ops[idx++], dsa);
				break;
			default:
				elog(ERROR, "pg_volvec: unsupported source operator type %u", (unsigned) pipeline.source->type());
		}

		for (PhysicalOperator *mid : pipeline.ops)
		{
			switch (mid->type())
			{
				case PhysicalOperatorType::HASH_AGGREGATE:
				case PhysicalOperatorType::PERFECT_HASH_AGGREGATE:
					EmitHashAgg(static_cast<const PhysicalHashAggregate &>(*mid), ops[idx], dsa);
					static_cast<PhysicalHashAggregate *>(mid)->AttachDescriptor(&ops[idx]);
					idx++;
					break;
				case PhysicalOperatorType::PROJECTION:
					EmitProjection(static_cast<const PhysicalProjection &>(*mid), ops[idx++], dsa);
					break;
				default:
					elog(ERROR, "pg_volvec: unsupported mid-pipeline operator type %u", (unsigned) mid->type());
			}
		}

		switch (pipeline.sink->type())
		{
			case PhysicalOperatorType::HASH_AGGREGATE:
			case PhysicalOperatorType::PERFECT_HASH_AGGREGATE:
				EmitHashAgg(static_cast<const PhysicalHashAggregate &>(*pipeline.sink), ops[idx], dsa);
				static_cast<PhysicalHashAggregate &>(*pipeline.sink).AttachDescriptor(&ops[idx]);
				break;
			case PhysicalOperatorType::ORDER:
				EmitOrder(static_cast<const PhysicalOrder &>(*pipeline.sink), ops[idx]);
				static_cast<PhysicalOrder &>(*pipeline.sink).AttachDescriptor(&ops[idx]);
				break;
			case PhysicalOperatorType::OUTPUT:
				EmitOutput(static_cast<const OutputSink &>(*pipeline.sink), ops[idx]);
				static_cast<OutputSink &>(*pipeline.sink).AttachDescriptor(&ops[idx]);
				break;
			default:
				elog(ERROR, "pg_volvec: unsupported sink operator type %u", (unsigned) pipeline.sink->type());
		}
	}

	return root_dp;
}

void
WorkerReconstructPipelines(PipelineSharedControl *ctl,
				   ExecCtx &worker_ctx,
				   PgVector<std::unique_ptr<Pipeline>> &out)
{
	if (ctl == nullptr || ctl->pipelines_root == InvalidDsaPointer || ctl->num_pipelines <= 0)
		return;

	auto *root = static_cast<PipelineDescriptor *>(dsa_get_address(worker_ctx.dsa, ctl->pipelines_root));
	for (int32 idx = 0; idx < ctl->num_pipelines; ++idx)
	{
		PipelineDescriptor &pd = root[idx];
		auto pipeline = std::make_unique<Pipeline>();
		pipeline->id = static_cast<PipelineId>(pd.pipeline_id);

		auto *ops = static_cast<OpDescriptor *>(dsa_get_address(worker_ctx.dsa, pd.ops));
		pipeline->source = ReconstructOp(ops[0], worker_ctx).release();
		for (int32 op_idx = 1; op_idx < pd.op_count - 1; ++op_idx)
			pipeline->ops.push_back(ReconstructOp(ops[op_idx], worker_ctx).release());
		pipeline->sink = ReconstructOp(ops[pd.op_count - 1], worker_ctx).release();

		for (PipelineId dep = 0; dep < 64; ++dep)
			if (pd.dependency_mask & (UINT64_C(1) << dep))
				pipeline->depends_on.push_back(dep);

		out.push_back(std::move(pipeline));
	}
}

void
StoreSharedPayloadOnDescriptor(const PhysicalOperator *op, dsa_pointer payload_dp)
{
	if (op == nullptr)
		return;

	/*
	 * Fix A2: fan out the payload pointer to EVERY DSA OpDescriptor slot this
	 * operator was attached to. The same C++ instance can appear in multiple
	 * pipelines (e.g. HashAgg as P_producer.sink and P_consumer.source), and
	 * LeaderSerializePipelines allocated an independent slot per pipeline.
	 * Workers reconstruct from per-pipeline slots, so a single-slot Store
	 * leaves the other pipelines' workers reading InvalidDsaPointer.
	 * See physical_hash_aggregate.hpp AttachDescriptor for the contract.
	 */
	switch (op->type())
	{
		case PhysicalOperatorType::SEQ_SCAN:
		{
			for (OpDescriptor *desc : static_cast<const PhysicalSeqScan *>(op)->descs())
				if (desc != nullptr)
					desc->body.seq_scan.shared_payload = payload_dp;
			break;
		}
		case PhysicalOperatorType::HASH_AGGREGATE:
		case PhysicalOperatorType::PERFECT_HASH_AGGREGATE:
		{
			const auto &dl = static_cast<const PhysicalHashAggregate *>(op)->descs();
			for (OpDescriptor *desc : dl)
			{
				if (desc != nullptr)
				{
					if (desc->kind == OpKind::PERFECT_HASH_AGGREGATE)
						desc->body.perfect_hash_agg.shared_payload = payload_dp;
					else
						desc->body.hash_agg.shared_payload = payload_dp;
				}
			}
			break;
		}
		case PhysicalOperatorType::ORDER:
		{
			for (OpDescriptor *desc : static_cast<const PhysicalOrder *>(op)->descs())
				if (desc != nullptr)
					desc->body.order.shared_payload = payload_dp;
			break;
		}
		case PhysicalOperatorType::OUTPUT:
		{
			for (OpDescriptor *desc : static_cast<const OutputSink *>(op)->descs())
				if (desc != nullptr)
					desc->body.output.shared_payload = payload_dp;
			break;
		}
		case PhysicalOperatorType::PROJECTION:
			break;
	}
}

dsa_pointer
LoadSharedPayloadFromDescriptor(const PhysicalOperator *op)
{
	if (op == nullptr)
		return InvalidDsaPointer;

	switch (op->type())
	{
		case PhysicalOperatorType::SEQ_SCAN:
		{
			OpDescriptor *desc = static_cast<const PhysicalSeqScan *>(op)->desc();
			return desc != nullptr ? desc->body.seq_scan.shared_payload : InvalidDsaPointer;
		}
		case PhysicalOperatorType::HASH_AGGREGATE:
		case PhysicalOperatorType::PERFECT_HASH_AGGREGATE:
		{
			OpDescriptor *desc = static_cast<const PhysicalHashAggregate *>(op)->desc();
			dsa_pointer ret = InvalidDsaPointer;
			if (desc != nullptr)
				ret = desc->kind == OpKind::PERFECT_HASH_AGGREGATE
					? desc->body.perfect_hash_agg.shared_payload
					: desc->body.hash_agg.shared_payload;
			return ret;
		}
		case PhysicalOperatorType::ORDER:
		{
			OpDescriptor *desc = static_cast<const PhysicalOrder *>(op)->desc();
			return desc != nullptr ? desc->body.order.shared_payload : InvalidDsaPointer;
		}
		case PhysicalOperatorType::OUTPUT:
		{
			OpDescriptor *desc = static_cast<const OutputSink *>(op)->desc();
			return desc != nullptr ? desc->body.output.shared_payload : InvalidDsaPointer;
		}
		case PhysicalOperatorType::PROJECTION:
			return InvalidDsaPointer;
	}

	return InvalidDsaPointer;
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */
