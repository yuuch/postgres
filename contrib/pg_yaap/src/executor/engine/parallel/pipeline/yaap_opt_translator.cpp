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

std::unique_ptr<PipelineOperator>
BuildPipelineFromOptimizerPlan(QueryDesc *queryDesc,
							   PgYaapQueryState *state,
							   const OptimizerPlanBundle &bundle)
{
	if (queryDesc == nullptr || state == nullptr || bundle.physical_plan == nullptr || state->runtime_dsa == nullptr)
		return nullptr;

	const OptimizerPlanSupportStatus support = AnalyzeOptimizerPlanSupport(bundle);
	if (!support.supported)
		return nullptr;

	if (pg_yaap_trace_hooks)
		elog(LOG, "pg_yaap: optimizer plan matched pipeline builder path");

	OptimizerNodeTranslation node;
	if (pg_yaap_trace_hooks)
		elog(LOG, "pg_yaap: optimizer pipeline builder entering node lowering");
	if (!TranslateOptimizerNode(*bundle.physical_plan, queryDesc, state, {}, nullptr, nullptr, nullptr, node) || node.op == nullptr)
		return nullptr;
	if (pg_yaap_trace_hooks)
		elog(LOG, "pg_yaap: optimizer pipeline builder built node cols=%zu schema=%zu", node.cols.size(), node.schema.size());

	auto root = BuildOutputContract(node, queryDesc, state);
	if (pg_yaap_trace_hooks)
		elog(LOG, "pg_yaap: optimizer pipeline builder built output contract root=%p", static_cast<void *>(root.get()));
	return root;
}

TupleDesc
BuildOptimizerOutputTupleDesc(const OptimizerPlanBundle &bundle)
{
	if (bundle.output_targetlist == NIL)
		return nullptr;
	return ExecCleanTypeFromTL(bundle.output_targetlist);
}

}  // namespace pg_yaap
