#include "parallel/pipeline/yaap_opt_translator_internal.hpp"

namespace pg_yaap {

using namespace optimizer_translator_detail;

OptimizerPlanSupportStatus
AnalyzeOptimizerPlanSupport(const OptimizerPlanBundle &bundle)
{
	if (bundle.physical_plan == nullptr)
		return OptimizerPlanSupportStatus{false, "root", "optimizer physical plan is null"};

	SupportContext ctx;
	ctx.stack.push_back(std::string("root(") + OptimizerOpTypeName(*bundle.physical_plan) + ")");
	return AnalyzeOptimizerPlanNode(*bundle.physical_plan, ctx);
}

std::string
DescribeOptimizerPlan(const OptimizerPlanBundle &bundle)
{
	if (bundle.physical_plan == nullptr)
		return "NULL";
	std::string out;
	out += "\n";
	AppendOptimizerPlanNodeTree(*bundle.physical_plan, "", true, true, out);
	if (!out.empty() && out.back() == '\n')
		out.pop_back();
	return out;
}

bool
CanExecuteOptimizerPlanSerial(const OptimizerPlanBundle &bundle)
{
	const PhysicalTableScan *scan = nullptr;
	std::vector<ColumnRef> output_cols;

	if (bundle.physical_plan == nullptr)
		return false;
	if (!ExtractScanShape(*bundle.physical_plan, scan, output_cols) || scan == nullptr)
		return false;
	return scan->relid != InvalidOid && scan->filters.empty();
}

std::unique_ptr<PipelineOperator>
TranslateOptimizerPlan(QueryDesc *queryDesc,
					   PgYaapQueryState *state,
					   const OptimizerPlanBundle &bundle)
{
	if (queryDesc == nullptr || state == nullptr || bundle.physical_plan == nullptr || state->runtime_dsa == nullptr)
		return nullptr;

	const OptimizerPlanSupportStatus support = AnalyzeOptimizerPlanSupport(bundle);
	if (!support.supported)
		return nullptr;

	if (pg_yaap_trace_hooks)
		elog(LOG, "pg_yaap: optimizer plan matched recursive node adapter path");

	OptimizerNodeTranslation node;
	if (pg_yaap_trace_hooks)
		elog(LOG, "pg_yaap: optimizer translator entering TranslateOptimizerNode");
	if (!TranslateOptimizerNode(*bundle.physical_plan, queryDesc, state, {}, nullptr, nullptr, nullptr, node) || node.op == nullptr)
		return nullptr;
	if (pg_yaap_trace_hooks)
		elog(LOG, "pg_yaap: optimizer translator built node cols=%zu schema=%zu", node.cols.size(), node.schema.size());

	auto root = BuildOutputContract(node, queryDesc, state);
	if (pg_yaap_trace_hooks)
		elog(LOG, "pg_yaap: optimizer translator built output contract root=%p", static_cast<void *>(root.get()));
	return root;
}

bool
ExecuteOptimizerPlanSerial(QueryDesc *queryDesc,
						   const OptimizerPlanBundle &bundle,
						   const char **failure_reason)
{
	const PhysicalTableScan *scan = nullptr;
	std::vector<ColumnRef> output_cols;
	Relation rel;
	TableScanDesc scan_desc;
	TupleTableSlot *scan_slot = nullptr;
	TupleTableSlot *proj_slot = nullptr;
	uint64 emitted = 0;
	bool need_projection = false;

	if (failure_reason != nullptr)
		*failure_reason = nullptr;
	if (queryDesc == nullptr || queryDesc->dest == nullptr || queryDesc->tupDesc == nullptr)
		return false;
	if (!ExtractScanShape(*bundle.physical_plan, scan, output_cols) || scan == nullptr)
		return false;
	if (scan->relid == InvalidOid || !scan->filters.empty())
		return false;

	rel = table_open(scan->relid, AccessShareLock);
	scan_desc = table_beginscan(rel, queryDesc->estate->es_snapshot, 0, NULL);
	scan_slot = table_slot_create(rel, NULL);
	need_projection = output_cols.size() != static_cast<size_t>(queryDesc->tupDesc->natts);
	if (!need_projection)
	{
		for (size_t i = 0; i < output_cols.size(); ++i)
		{
			if (output_cols[i].attno != static_cast<AttrNumber>(i + 1))
			{
				need_projection = true;
				break;
			}
		}
	}
	if (need_projection)
		proj_slot = MakeSingleTupleTableSlot(queryDesc->tupDesc, &TTSOpsVirtual);

	queryDesc->dest->rStartup(queryDesc->dest, static_cast<int>(queryDesc->operation), queryDesc->tupDesc);
	while (table_scan_getnextslot(scan_desc, ForwardScanDirection, scan_slot))
	{
		if (need_projection)
		{
			ExecClearTuple(proj_slot);
			for (size_t i = 0; i < output_cols.size(); ++i)
			{
				bool isnull = false;
				proj_slot->tts_values[i] = slot_getattr(scan_slot, output_cols[i].attno, &isnull);
				proj_slot->tts_isnull[i] = isnull;
			}
			ExecStoreVirtualTuple(proj_slot);
			queryDesc->dest->receiveSlot(proj_slot, queryDesc->dest);
		}
		else
			queryDesc->dest->receiveSlot(scan_slot, queryDesc->dest);
		emitted++;
	}
	queryDesc->dest->rShutdown(queryDesc->dest);

	if (proj_slot != nullptr)
		ExecDropSingleTupleTableSlot(proj_slot);
	ExecDropSingleTupleTableSlot(scan_slot);
	table_endscan(scan_desc);
	table_close(rel, AccessShareLock);
	queryDesc->estate->es_processed = emitted;
	return true;
}

TupleDesc
BuildOptimizerOutputTupleDesc(const OptimizerPlanBundle &bundle)
{
	if (bundle.output_targetlist == NIL)
		return nullptr;
	return ExecCleanTypeFromTL(bundle.output_targetlist);
}

}  // namespace pg_yaap
