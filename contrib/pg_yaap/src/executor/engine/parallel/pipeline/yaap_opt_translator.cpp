extern "C" {
#include "postgres.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/pg_type_d.h"
#include "datatype/timestamp.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/dsa.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

extern Datum numeric_mul(PG_FUNCTION_ARGS);
extern Datum numeric_int8(PG_FUNCTION_ARGS);
}

#include "yaap_opt_translator.hpp"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstring>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "adapter/yaap_adapter.hpp"
#include "optimizer_registry.hpp"
#include "parallel/pipeline/output_sink.hpp"
#include "parallel/pipeline/physical_hash_aggregate.hpp"
#include "parallel/pipeline/physical_hash_join.hpp"
#include "parallel/pipeline/physical_perfect_hash_aggregate.hpp"
#include "parallel/pipeline/physical_projection.hpp"
#include "parallel/pipeline/physical_seq_scan.hpp"
#include "parallel/pipeline/pipeline_descriptor.hpp"
#include "parallel/pipeline/translator_internal.hpp"
#include "parallel/pipeline/tuple_data_collection.hpp"

namespace pg_yaap {

extern "C" bool pg_yaap_trace_hooks;

namespace {

static constexpr int32 kNumericTypmodVarHdrSz = 4;

static int32
MakeNumericTypmod(int precision, int scale)
{
	return ((precision << 16) | (scale & 0x7ff)) + kNumericTypmodVarHdrSz;
}

using yaap::BoundAggregateExpression;
using yaap::BoundColumnRefExpression;
using yaap::BoundConjunctionExpression;
using yaap::BoundConstantExpression;
using yaap::BoundFunctionExpression;
using yaap::Expression;
using yaap::ExpressionType;
using yaap::PhysicalCrossProduct;
using yaap::PhysicalFilter;
using yaap::PhysicalHashAggregate;
using yaap::PhysicalHashJoin;
using yaap::PhysicalLimit;
using yaap::PhysicalOperator;
using yaap::PhysicalOperatorType;
using yaap::PhysicalOrderBy;
using yaap::PhysicalProjection;
using yaap::PhysicalTableScan;

using pg_yaap::pipeline::AggFuncDesc;
using pg_yaap::pipeline::ColumnDecodeKind;
using pg_yaap::pipeline::ColumnSchema;
using pg_yaap::pipeline::FilterExprDesc;
using pg_yaap::pipeline::FilterInputDesc;
using pg_yaap::pipeline::FilterStep;
using pg_yaap::pipeline::FilterStepOp;
using pg_yaap::pipeline::HashJoinFilterInputDesc;
using pg_yaap::pipeline::HashJoinOutputColumnDesc;
using pg_yaap::pipeline::HashJoinOutputSide;
using OutputSink = pg_yaap::pipeline::OutputSink;
template <typename T>
using PgVector = pg_yaap::PgVector<T>;
using PipelineOperator = pg_yaap::pipeline::PhysicalOperator;
using PipelineHashAggregate = pg_yaap::pipeline::PhysicalHashAggregate;
using PipelineHashJoin = pg_yaap::pipeline::PhysicalHashJoin;
using PipelinePerfectHashAggregate = pg_yaap::pipeline::PhysicalPerfectHashAggregate;
using PipelineProjection = pg_yaap::pipeline::PhysicalProjection;
using PipelineSeqScan = pg_yaap::pipeline::PhysicalSeqScan;
using pg_yaap::pipeline::ProjectExprDesc;
using pg_yaap::pipeline::ProjectOp;
using pg_yaap::pipeline::ProjectStep;
using pg_yaap::pipeline::QualOp;
using pg_yaap::pipeline::SortKeyDesc;
using pg_yaap::pipeline::TdcAggKind;
using pg_yaap::pipeline::TupleDataCollection;
using pg_yaap::pipeline::TupleDataLayout;
using pg_yaap::pipeline::translator_detail::BuildAggOutputSchemaDescriptor;
using pg_yaap::pipeline::translator_detail::BuildColumnOnlyLayout;
using pg_yaap::pipeline::translator_detail::BuildColumnOnlyLayoutForRefs;
using pg_yaap::pipeline::translator_detail::BuildHashGroupLayout;
using pg_yaap::pipeline::translator_detail::BuildHashJoinOutputMappings;
using pg_yaap::pipeline::translator_detail::BuildOrderedSeqScanColumns;
using pg_yaap::pipeline::translator_detail::BuildSchemaDescriptorFromColumns;
using pg_yaap::pipeline::translator_detail::BuildSeqScanColumns;
using pg_yaap::pipeline::translator_detail::ColumnNumericScale;
using pg_yaap::pipeline::translator_detail::ColumnRef;
using pg_yaap::pipeline::translator_detail::ExtractNumericTypmodScale;
using pg_yaap::pipeline::translator_detail::MapProjectedExprSchema;
using pg_yaap::pipeline::translator_detail::LookupRawColumn;
using pg_yaap::pipeline::translator_detail::Pow10Int64;
using pg_yaap::pipeline::translator_detail::RescaleInt64Constant;

constexpr int8_t kProjectionDivisionScale = 16;
constexpr int16_t kAvgNumericExtraScale = 12;

struct SupportContext {
	std::vector<std::string> stack;
};

struct MaterializedOptExpr {
	const Expression *expr = nullptr;
	int8_t scale = 0;
	uint8_t slot = 0;
};

struct AggBuildState {
	std::vector<AggFuncDesc> agg_funcs;
	std::vector<TdcAggKind> agg_kinds;
	std::vector<int16_t> agg_numeric_scales;
	std::vector<ProjectStep> project_steps;
	std::vector<ProjectExprDesc> project_exprs;
	std::vector<MaterializedOptExpr> materialized_exprs;
	TupleDataLayout hash_layout{};
	uint32_t perfect_hash_capacity = 0;
};

struct OptimizerNodeTranslation {
	std::unique_ptr<PipelineOperator> op;
	std::vector<ColumnRef> cols;
	std::vector<ColumnSchema> schema;
	std::vector<SortKeyDesc> final_sort_keys;
	uint64_t limit_count = 0;
	uint32_t estimated_groups = 0;
};

static const char *OptimizerOpTypeName(PhysicalOperatorType type);
static void AppendOptimizerPlanNode(const PhysicalOperator &op, std::string &out);
static OptimizerPlanSupportStatus AnalyzeOptimizerPlanNode(const PhysicalOperator &op, SupportContext &ctx);
static bool TranslateOptimizerNode(const PhysicalOperator &op,
								   QueryDesc *queryDesc,
								   PgYaapQueryState *state,
								   const std::vector<Expression *> &pending_filters,
								   const std::vector<ColumnRef> *required_output_cols,
								   OptimizerNodeTranslation &out);

static std::string
CurrentSupportPath(const SupportContext &ctx)
{
	if (ctx.stack.empty())
		return "root";

	std::string out;
	for (size_t i = 0; i < ctx.stack.size(); ++i)
	{
		if (i != 0)
			out += ".";
		out += ctx.stack[i];
	}
	return out;
}

static OptimizerPlanSupportStatus
MakeSupportOk()
{
	return OptimizerPlanSupportStatus{true, "", ""};
}

static OptimizerPlanSupportStatus
MakeSupportError(const SupportContext &ctx, const char *detail)
{
	return OptimizerPlanSupportStatus{
		false,
		CurrentSupportPath(ctx),
		detail != nullptr ? detail : "unsupported optimizer node"
	};
}

static OptimizerPlanSupportStatus
AnalyzeChildren(const PhysicalOperator &op, SupportContext &ctx, size_t expected_children)
{
	if (op.children.size() != expected_children)
		return MakeSupportError(ctx, "unexpected child count");

	for (size_t i = 0; i < op.children.size(); ++i)
	{
		if (op.children[i] == nullptr)
			return MakeSupportError(ctx, "null child");
		ctx.stack.push_back(std::string("child[") + std::to_string(i) + "]");
		OptimizerPlanSupportStatus child_status = AnalyzeOptimizerPlanNode(*op.children[i], ctx);
		ctx.stack.pop_back();
		if (!child_status.supported)
			return child_status;
	}

	return MakeSupportOk();
}

static OptimizerPlanSupportStatus
AnalyzeTableScanNode(const PhysicalTableScan &scan, SupportContext &ctx)
{
	if (scan.relid == InvalidOid)
		return MakeSupportError(ctx, "table scan relid is invalid");
	for (Expression *expr : scan.filters)
	{
		if (expr == nullptr)
			return MakeSupportError(ctx, "table scan filter is null");
	}
	return MakeSupportOk();
}

static OptimizerPlanSupportStatus
AnalyzeFilterNode(const PhysicalFilter &filter, SupportContext &ctx)
{
	for (Expression *expr : filter.expressions)
	{
		if (expr == nullptr)
			return MakeSupportError(ctx, "filter expression is null");
	}
	return AnalyzeChildren(filter, ctx, 1);
}

static OptimizerPlanSupportStatus
AnalyzeLimitNode(const PhysicalLimit &limit, SupportContext &ctx)
{
	if (limit.limit_offset != nullptr)
		return MakeSupportError(ctx, "LIMIT with OFFSET is not supported");
	if (limit.limit_count == nullptr)
		return MakeSupportError(ctx, "LIMIT count is missing");
	return AnalyzeChildren(limit, ctx, 1);
}

static OptimizerPlanSupportStatus
AnalyzeProjectionNode(const PhysicalProjection &projection, SupportContext &ctx)
{
	for (Expression *expr : projection.select_list)
	{
		if (expr == nullptr)
			return MakeSupportError(ctx, "projection expression is null");
		if (expr->type == ExpressionType::OPAQUE || expr->type == ExpressionType::BOUND_SUBQUERY)
			return MakeSupportError(ctx, "projection expression type is not supported");
	}
	return AnalyzeChildren(projection, ctx, 1);
}

static OptimizerPlanSupportStatus
AnalyzeHashJoinNode(const PhysicalHashJoin &join, SupportContext &ctx)
{
	return AnalyzeChildren(join, ctx, 2);
}

static OptimizerPlanSupportStatus
AnalyzeCrossProductNode(const PhysicalCrossProduct &join, SupportContext &ctx)
{
	return AnalyzeChildren(join, ctx, 2);
}

static OptimizerPlanSupportStatus
AnalyzeHashAggregateNode(const PhysicalHashAggregate &agg, SupportContext &ctx)
{
	for (Expression *expr : agg.groups)
	{
		if (expr == nullptr)
			return MakeSupportError(ctx, "aggregate group expression is null");
	}
	for (Expression *expr : agg.expressions)
	{
		if (expr == nullptr)
			return MakeSupportError(ctx, "aggregate expression is null");
		if (expr->type != ExpressionType::BOUND_AGGREGATE)
			return MakeSupportError(ctx, "aggregate expression must be BOUND_AGGREGATE");
	}
	return AnalyzeChildren(agg, ctx, 1);
}

static OptimizerPlanSupportStatus
AnalyzeOrderByNode(const PhysicalOrderBy &order, SupportContext &ctx)
{
	for (Expression *expr : order.orders)
	{
		if (expr == nullptr)
			return MakeSupportError(ctx, "order-by expression is null");
	}
	return AnalyzeChildren(order, ctx, 1);
}

static OptimizerPlanSupportStatus
AnalyzeOptimizerPlanNode(const PhysicalOperator &op, SupportContext &ctx)
{
	switch (op.type)
	{
		case PhysicalOperatorType::TABLE_SCAN:
			return AnalyzeTableScanNode(static_cast<const PhysicalTableScan &>(op), ctx);
		case PhysicalOperatorType::FILTER:
			return AnalyzeFilterNode(static_cast<const PhysicalFilter &>(op), ctx);
		case PhysicalOperatorType::LIMIT:
			return AnalyzeLimitNode(static_cast<const PhysicalLimit &>(op), ctx);
		case PhysicalOperatorType::PROJECTION:
			return AnalyzeProjectionNode(static_cast<const PhysicalProjection &>(op), ctx);
		case PhysicalOperatorType::HASH_JOIN:
			return AnalyzeHashJoinNode(static_cast<const PhysicalHashJoin &>(op), ctx);
		case PhysicalOperatorType::CROSS_PRODUCT:
			return AnalyzeCrossProductNode(static_cast<const PhysicalCrossProduct &>(op), ctx);
		case PhysicalOperatorType::HASH_GROUP_BY:
			return AnalyzeHashAggregateNode(static_cast<const PhysicalHashAggregate &>(op), ctx);
		case PhysicalOperatorType::ORDER_BY:
			return AnalyzeOrderByNode(static_cast<const PhysicalOrderBy &>(op), ctx);
		default:
			return MakeSupportError(ctx, "node type has no executor adapter yet");
	}
}

static const char *
OptimizerOpTypeName(PhysicalOperatorType type)
{
	switch (type)
	{
		case PhysicalOperatorType::TABLE_SCAN: return "TABLE_SCAN";
		case PhysicalOperatorType::PROJECTION: return "PROJECTION";
		case PhysicalOperatorType::FILTER: return "FILTER";
		case PhysicalOperatorType::DISTINCT: return "DISTINCT";
		case PhysicalOperatorType::SET_OPERATION: return "SET_OPERATION";
		case PhysicalOperatorType::LIMIT: return "LIMIT";
		case PhysicalOperatorType::WINDOW: return "WINDOW";
		case PhysicalOperatorType::HASH_JOIN: return "HASH_JOIN";
		case PhysicalOperatorType::DELIM_GET: return "DELIM_GET";
		case PhysicalOperatorType::CROSS_PRODUCT: return "CROSS_PRODUCT";
		case PhysicalOperatorType::HASH_GROUP_BY: return "HASH_GROUP_BY";
		case PhysicalOperatorType::ORDER_BY: return "ORDER_BY";
	}
	return "UNKNOWN";
}

static void
AppendOptimizerPlanNode(const PhysicalOperator &op, std::string &out)
{
	out += OptimizerOpTypeName(op.type);
	out += "(";
	out += std::to_string(op.estimated_cardinality);
	out += ")";
	for (const auto &child : op.children)
	{
		out += " -> ";
		if (child == nullptr)
		{
			out += "NULL";
			continue;
		}
		AppendOptimizerPlanNode(*child, out);
	}
}

static bool
UseInt32CharDecodeForType(Oid type_oid, int32 typmod)
{
	if (type_oid == CHAROID)
		return true;
	if (type_oid != BPCHAROID || typmod < VARHDRSZ)
		return false;
	return (typmod - VARHDRSZ) == 1;
}

static dsa_pointer
BuildFilterArray(dsa_area *dsa, const void *data, size_t elem_size, size_t count)
{
	if (count == 0)
		return InvalidDsaPointer;
	dsa_pointer dp = dsa_allocate0(dsa, elem_size * count);
	if (!DsaPointerIsValid(dp))
		return InvalidDsaPointer;
	std::memcpy(dsa_get_address(dsa, dp), data, elem_size * count);
	return dp;
}

static dsa_pointer
BuildCharArray(dsa_area *dsa, const std::vector<char> &bytes)
{
	if (bytes.empty())
		return InvalidDsaPointer;
	dsa_pointer dp = dsa_allocate0(dsa, bytes.size());
	if (!DsaPointerIsValid(dp))
		return InvalidDsaPointer;
	std::memcpy(dsa_get_address(dsa, dp), bytes.data(), bytes.size());
	return dp;
}

static dsa_pointer
BuildOutputTdc(dsa_area *dsa,
			   dsa_pointer layout_dp,
			   const TupleDataLayout &layout,
			   uint32_t row_capacity)
{
	const uint32_t heap_capacity = TupleDataCollectionDefaultHeapCapacity(&layout, row_capacity);
	dsa_pointer payload_dp = dsa_allocate0(
		dsa,
		pg_yaap::pipeline::TupleDataCollectionCheckedAllocSize(row_capacity, layout.row_width, heap_capacity));
	if (!DsaPointerIsValid(payload_dp))
		return InvalidDsaPointer;
	auto *tdc = static_cast<TupleDataCollection *>(dsa_get_address(dsa, payload_dp));
	TupleDataCollectionInit(tdc, row_capacity, layout.row_width, layout_dp, heap_capacity);
	return payload_dp;
}

static uint32_t
EstimateOutputRows(QueryDesc *qd)
{
	double plan_rows = 1024.0;
	if (qd != nullptr && qd->plannedstmt != nullptr && qd->plannedstmt->planTree != nullptr)
		plan_rows = qd->plannedstmt->planTree->plan_rows;
	const double cap_d = std::max(1024.0, std::min(static_cast<double>(1u << 20), plan_rows * 1.5));
	return static_cast<uint32_t>(cap_d);
}

static uint32_t
EstimateResultRows(QueryDesc *qd, uint32_t estimated_groups)
{
	if (estimated_groups > 0)
		return std::max<uint32_t>(1024u, std::min<uint32_t>(1u << 20, estimated_groups));
	return EstimateOutputRows(qd);
}

static uint32_t
EstimateInitialResultRows(QueryDesc *qd, uint32_t estimated_groups)
{
	return std::min<uint32_t>(8192u, EstimateResultRows(qd, estimated_groups));
}

static uint32_t
EstimateHashJoinBuildRows(size_t estimated_rows)
{
	const double rows = estimated_rows > 0 ? static_cast<double>(estimated_rows) : 1024.0;
	const double with_margin = std::max(1024.0, rows * 1.25);
	return static_cast<uint32_t>(std::min(static_cast<double>(1u << 26), with_margin));
}

static ColumnRef
BindingToColumnRef(const yaap::ColumnBinding &binding)
{
	return ColumnRef{
		static_cast<Index>(binding.table_index.index + 1),
		static_cast<AttrNumber>(binding.column_index.index + 1)
	};
}

static bool
SameColumnRef(const ColumnRef &lhs, const ColumnRef &rhs)
{
	return lhs.varno == rhs.varno && lhs.attno == rhs.attno;
}

static void
AppendUniqueColumnRef(const ColumnRef &ref, std::vector<ColumnRef> &out)
{
	for (const ColumnRef &existing : out)
	{
		if (SameColumnRef(existing, ref))
			return;
	}
	out.push_back(ref);
}

static void
CollectReferencedColumns(const Expression *expr, std::vector<ColumnRef> &out)
{
	if (expr == nullptr)
		return;
	switch (expr->type)
	{
		case ExpressionType::BOUND_COLUMN_REF:
			AppendUniqueColumnRef(BindingToColumnRef(static_cast<const BoundColumnRefExpression *>(expr)->binding), out);
			return;
		case ExpressionType::BOUND_FUNCTION:
		{
			const auto *func = static_cast<const BoundFunctionExpression *>(expr);
			for (const auto &child : func->children)
				CollectReferencedColumns(child.get(), out);
			return;
		}
		case ExpressionType::BOUND_AGGREGATE:
		{
			const auto *agg = static_cast<const BoundAggregateExpression *>(expr);
			for (const auto &child : agg->children)
				CollectReferencedColumns(child.get(), out);
			return;
		}
		case ExpressionType::BOUND_CONJUNCTION:
		{
			const auto *conj = static_cast<const BoundConjunctionExpression *>(expr);
			for (const auto &child : conj->children)
				CollectReferencedColumns(child.get(), out);
			return;
		}
		default:
			return;
	}
}

static void
FilterRequestedColumns(const std::vector<ColumnRef> &available,
					   const std::vector<ColumnRef> *required,
					   std::vector<ColumnRef> &out)
{
	out.clear();
	if (required == nullptr || required->empty())
	{
		out = available;
		return;
	}
	for (const ColumnRef &ref : *required)
	{
		for (const ColumnRef &candidate : available)
		{
			if (SameColumnRef(ref, candidate))
			{
				AppendUniqueColumnRef(candidate, out);
				break;
			}
		}
	}
}

static bool
LookupBindingColumn(const yaap::ColumnBinding &binding,
					const std::vector<ColumnRef> &cols,
					const std::vector<ColumnSchema> &schema,
					ColumnRef &out_ref,
					const ColumnSchema *&out_col)
{
	out_ref = BindingToColumnRef(binding);
	return LookupRawColumn(out_ref, cols, schema, out_col);
}

static bool
IsComparisonName(const std::string &name)
{
	return name == "<" || name == "<=" || name == "=" || name == ">=" || name == ">" || name == "<>" ||
		   name == "!=" || name == "~~";
}

static bool
MapComparisonNameToQualOp(const std::string &name, QualOp &out)
{
	if (name == "<=") out = QualOp::LE;
	else if (name == "<") out = QualOp::LT;
	else if (name == "=") out = QualOp::EQ;
	else if (name == ">=") out = QualOp::GE;
	else if (name == ">") out = QualOp::GT;
	else if (name == "<>" || name == "!=") out = QualOp::NE;
	else return false;
	return true;
}

static bool
TryParseUInt64(const std::string &text, uint64_t &out)
{
	const char *ptr = text.c_str();
	char *end = nullptr;
	errno = 0;
	unsigned long long value = std::strtoull(ptr, &end, 10);
	if (errno != 0 || end == ptr)
		return false;
	while (*end != '\0' && std::isspace(static_cast<unsigned char>(*end)))
		++end;
	if (*end != '\0')
		return false;
	out = static_cast<uint64_t>(value);
	return true;
}

static bool
IsLimitCoercionFunction(const std::string &name)
{
	return name == "int8" || name == "int4" || name == "int2" || name == "numeric" ||
		   name == "float8" || name == "float4";
}

static bool
TryParseLimitExpression(const Expression *expr, uint64_t &out)
{
	if (expr == nullptr)
		return false;
	if (expr->type == ExpressionType::BOUND_CONSTANT)
		return TryParseUInt64(static_cast<const BoundConstantExpression *>(expr)->value, out);
	if (expr->type == ExpressionType::BOUND_FUNCTION)
	{
		const auto *func = static_cast<const BoundFunctionExpression *>(expr);
		if (func->children.size() == 1 && IsLimitCoercionFunction(func->function_name))
			return TryParseLimitExpression(func->children[0].get(), out);
	}
	return false;
}

static bool
ConvertConstantToDatum(const BoundConstantExpression *constant, Oid target_type, int32 typmod, Datum &out)
{
	if (constant == nullptr || constant->is_null || !OidIsValid(target_type))
		return false;
	Oid input_func = InvalidOid;
	Oid ioparam = InvalidOid;
	getTypeInputInfo(target_type, &input_func, &ioparam);
	out = OidInputFunctionCall(input_func, const_cast<char *>(constant->value.c_str()), ioparam, typmod);
	return true;
}

static bool
ScaleNumericDatumToTargetScale(Datum numeric_datum, int8_t target_scale, int64_t &out_value)
{
	int64_t factor = 1;
	if (!Pow10Int64(target_scale, factor))
		return false;
	Datum factor_numeric = NumericGetDatum(int64_to_numeric(factor));
	Datum scaled = DirectFunctionCall2(numeric_mul, numeric_datum, factor_numeric);
	out_value = DatumGetInt64(DirectFunctionCall1(numeric_int8, scaled));
	return true;
}

static bool
ScaleNumericConstantToInt64(const BoundConstantExpression *constant, int8_t &out_scale, int64_t &out_value)
{
	if (constant == nullptr || constant->is_null)
		return false;
	const std::string &value = constant->value;
	size_t dot = value.find('.');
	out_scale = (dot == std::string::npos) ? 0 : static_cast<int8_t>(value.size() - dot - 1);
	Datum numeric_datum = 0;
	if (!ConvertConstantToDatum(constant, NUMERICOID, -1, numeric_datum))
		return false;
	return ScaleNumericDatumToTargetScale(numeric_datum, out_scale, out_value);
}

static bool
ScaleNumericConstantToTargetScale(const BoundConstantExpression *constant, int8_t target_scale, int64_t &out_value)
{
	if (constant == nullptr || constant->is_null)
		return false;
	Datum numeric_datum = 0;
	if (!ConvertConstantToDatum(constant, NUMERICOID, -1, numeric_datum))
		return false;
	return ScaleNumericDatumToTargetScale(numeric_datum, target_scale, out_value);
}

static bool
ScaleIntegralConstantToTargetScale(const BoundConstantExpression *constant, int8_t target_scale, int64_t &out_value)
{
	if (constant == nullptr || constant->is_null || target_scale < 0)
		return false;
	int64_t base_value = 0;
	if (pg_strcasecmp(constant->value.c_str(), "true") == 0)
		base_value = 1;
	else if (pg_strcasecmp(constant->value.c_str(), "false") == 0)
		base_value = 0;
	else
	{
		const char *ptr = constant->value.c_str();
		char *end = nullptr;
		errno = 0;
		long long value = std::strtoll(ptr, &end, 10);
		if (errno != 0 || end == ptr)
			return false;
		while (*end != '\0' && std::isspace(static_cast<unsigned char>(*end)))
			++end;
		if (*end != '\0')
			return false;
		base_value = static_cast<int64_t>(value);
	}
	return RescaleInt64Constant(base_value, 0, target_scale, out_value);
}

static bool
ScaleConstantToTargetScale(const BoundConstantExpression *constant, int8_t target_scale, int64_t &out_value)
{
	if (constant == nullptr || constant->is_null)
		return false;
	if (constant->value.find('.') != std::string::npos)
		return ScaleNumericConstantToTargetScale(constant, target_scale, out_value);
	return ScaleIntegralConstantToTargetScale(constant, target_scale, out_value);
}

static bool
TryExtractShortStringConst(const BoundConstantExpression *constant, uint8_t &out_len, int64_t &out_value)
{
	if (constant == nullptr || constant->is_null)
		return false;
	const size_t len = constant->value.size();
	if (len > 8)
		return false;
	out_len = static_cast<uint8_t>(len);
	out_value = 0;
	if (len > 0)
		std::memcpy(&out_value, constant->value.data(), len);
	return true;
}

static bool
StoreStringConstBytes(const BoundConstantExpression *constant,
					  Oid type_oid,
					  int32 typmod,
					  std::vector<char> &pool,
					  uint32_t &out_offset,
					  uint32_t &out_len,
					  uint64_t &out_inline_value)
{
	if (constant == nullptr || constant->is_null)
		return false;
	Datum datum = 0;
	if (!ConvertConstantToDatum(constant, type_oid, typmod, datum))
		return false;
	const char *ptr = nullptr;
	size_t len = 0;
	if (type_oid == BPCHAROID || type_oid == TEXTOID || type_oid == VARCHAROID)
	{
		text *txt = DatumGetTextPP(datum);
		ptr = VARDATA_ANY(txt);
		len = VARSIZE_ANY_EXHDR(txt);
	}
	else
	{
		ptr = constant->value.data();
		len = constant->value.size();
	}
	out_len = static_cast<uint32_t>(len);
	if (len <= sizeof(out_inline_value))
	{
		out_offset = UINT32_MAX;
		out_inline_value = 0;
		if (len > 0)
			std::memcpy(&out_inline_value, ptr, len);
		return true;
	}
	out_offset = static_cast<uint32_t>(pool.size());
	out_inline_value = 0;
	pool.insert(pool.end(), ptr, ptr + len);
	return true;
}

static bool
ExtractCharFilterConst(const BoundConstantExpression *constant, int32_t &out_value)
{
	if (constant == nullptr || constant->is_null)
		return false;
	if (constant->value.empty())
		return false;
	out_value = static_cast<unsigned char>(constant->value[0]);
	return true;
}

static bool
TryExtractLikePattern(const BoundConstantExpression *constant,
					  bool &out_prefix,
					  std::string &out_match)
{
	if (constant == nullptr || constant->is_null)
		return false;
	const std::string &pattern = constant->value;
	const size_t first_pct = pattern.find('%');
	const size_t first_us = pattern.find('_');
	if (first_us != std::string::npos)
		return false;
	if (first_pct == std::string::npos)
	{
		out_prefix = true;
		out_match = pattern;
		return true;
	}
	if (pattern.find('%', first_pct + 1) == std::string::npos)
	{
		if (first_pct == pattern.size() - 1)
		{
			out_prefix = true;
			out_match = pattern.substr(0, pattern.size() - 1);
			return true;
		}
		if (first_pct == 0)
		{
			out_prefix = false;
			out_match = pattern.substr(1);
			return !out_match.empty() && out_match.find('%') == std::string::npos;
		}
	}
	if (pattern.size() >= 2 && pattern.front() == '%' && pattern.back() == '%' &&
		pattern.substr(1, pattern.size() - 2).find('%') == std::string::npos)
	{
		out_prefix = false;
		out_match = pattern.substr(1, pattern.size() - 2);
		return !out_match.empty();
	}
	return false;
}

static bool
EvaluateDateExpression(const Expression *expr, DateADT &out)
{
	if (expr == nullptr)
		return false;
	if (expr->type == ExpressionType::BOUND_CONSTANT)
	{
		Datum datum = 0;
		if (!ConvertConstantToDatum(static_cast<const BoundConstantExpression *>(expr), DATEOID, -1, datum))
			return false;
		out = DatumGetDateADT(datum);
		return true;
	}
	if (expr->type != ExpressionType::BOUND_FUNCTION)
		return false;
	const auto *func = static_cast<const BoundFunctionExpression *>(expr);
	if (func->children.size() != 2 || (func->function_name != "+" && func->function_name != "-"))
		return false;
	DateADT base = 0;
	if (!EvaluateDateExpression(func->children[0].get(), base))
		return false;
	const auto *interval_const = dynamic_cast<const BoundConstantExpression *>(func->children[1].get());
	if (interval_const == nullptr || interval_const->is_null)
		return false;
	Datum interval_datum = 0;
	if (!ConvertConstantToDatum(interval_const, INTERVALOID, -1, interval_datum))
		return false;
	Datum result = (func->function_name == "+")
		? DirectFunctionCall2(date_pl_interval, DateADTGetDatum(base), interval_datum)
		: DirectFunctionCall2(date_mi_interval, DateADTGetDatum(base), interval_datum);
	Timestamp ts = DatumGetTimestamp(result);
	int64 days = ts / USECS_PER_DAY;
	int64 rem = ts % USECS_PER_DAY;
	if (rem < 0)
	{
		days -= 1;
		rem += USECS_PER_DAY;
	}
	if (rem != 0)
		return false;
	out = static_cast<DateADT>(days);
	return true;
}

static std::vector<bool>
ParseOrderDirections(const char *source_text, size_t nkeys)
{
	std::vector<bool> out(nkeys, true);
	if (source_text == nullptr || nkeys == 0)
		return out;
	std::string sql(source_text);
	std::string upper(sql);
	std::transform(upper.begin(), upper.end(), upper.begin(),
				   [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
	size_t order_pos = upper.find("ORDER BY");
	if (order_pos == std::string::npos)
		return out;
	size_t end_pos = upper.find("LIMIT", order_pos);
	if (end_pos == std::string::npos)
		end_pos = sql.find(';', order_pos);
	if (end_pos == std::string::npos)
		end_pos = sql.size();
	std::string clause = sql.substr(order_pos + 8, end_pos - (order_pos + 8));
	int depth = 0;
	size_t item_idx = 0;
	size_t start = 0;
	for (size_t i = 0; i <= clause.size() && item_idx < nkeys; ++i)
	{
		const char ch = (i < clause.size()) ? clause[i] : ',';
		if (ch == '(')
			++depth;
		else if (ch == ')')
			--depth;
		else if (ch == ',' && depth == 0)
		{
			std::string item = clause.substr(start, i - start);
			std::string item_upper(item);
			std::transform(item_upper.begin(), item_upper.end(), item_upper.begin(),
						   [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
			if (item_upper.find(" DESC") != std::string::npos)
				out[item_idx] = false;
			start = i + 1;
			++item_idx;
		}
	}
	return out;
}

static bool
BuildAllTableColumnRefs(Oid relid, Index varno, std::vector<ColumnRef> &out_cols)
{
	Relation rel = relation_open(relid, AccessShareLock);
	TupleDesc tupdesc = RelationGetDescr(rel);
	out_cols.clear();
	for (int attidx = 0; attidx < tupdesc->natts; ++attidx)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, attidx);
		if (attr->attisdropped)
			continue;
		out_cols.push_back(ColumnRef{varno, static_cast<AttrNumber>(attidx + 1)});
	}
	relation_close(rel, AccessShareLock);
	return !out_cols.empty();
}

static bool
LookupOrAddScanFilterInput(const ColumnSchema &col,
						   std::vector<FilterInputDesc> &inputs,
						   uint16_t &out_idx)
{
	for (size_t i = 0; i < inputs.size(); ++i)
	{
		if (inputs[i].attno == static_cast<uint16_t>(col.src_attno) &&
			inputs[i].decode_kind == col.decode_kind)
		{
			out_idx = static_cast<uint16_t>(i);
			return true;
		}
	}
	if (inputs.size() >= pg_yaap::pipeline::FILTER_MAX_INPUTS || col.src_attno <= 0)
		return false;
	out_idx = static_cast<uint16_t>(inputs.size());
	inputs.push_back(FilterInputDesc{
		static_cast<uint16_t>(col.src_attno),
		static_cast<uint8_t>(out_idx),
		col.decode_kind,
		0});
	return true;
}

static bool
LookupOrAddJoinFilterInput(const ColumnRef &ref,
						   const std::vector<ColumnRef> &left_cols,
						   const std::vector<ColumnSchema> &left_schema,
						   const std::vector<ColumnRef> &right_cols,
						   const std::vector<ColumnSchema> &right_schema,
						   std::vector<HashJoinFilterInputDesc> &inputs,
						   uint16_t &out_idx,
						   const ColumnSchema *&out_col)
{
	HashJoinFilterInputDesc desc{};
	if (LookupRawColumn(ref, left_cols, left_schema, out_col))
	{
		desc.side = HashJoinOutputSide::LEFT;
		desc.input_chunk_slot = out_col->chunk_slot;
		desc.decode_kind = out_col->decode_kind;
		desc._pad0 = 0;
	}
	else if (LookupRawColumn(ref, right_cols, right_schema, out_col))
	{
		desc.side = HashJoinOutputSide::RIGHT;
		desc.input_chunk_slot = out_col->chunk_slot;
		desc.decode_kind = out_col->decode_kind;
		desc._pad0 = 0;
	}
	else
		return false;

	for (size_t i = 0; i < inputs.size(); ++i)
	{
		const auto &existing = inputs[i];
		if (existing.side == desc.side &&
			existing.input_chunk_slot == desc.input_chunk_slot &&
			existing.decode_kind == desc.decode_kind)
		{
			out_idx = static_cast<uint16_t>(i);
			return true;
		}
	}
	if (inputs.size() >= pg_yaap::pipeline::FILTER_MAX_INPUTS)
		return false;
	out_idx = static_cast<uint16_t>(inputs.size());
	inputs.push_back(desc);
	return true;
}

static bool
AppendFilterExpr(std::vector<FilterExprDesc> &exprs,
				 const std::vector<FilterStep> &steps,
				 size_t first_step_idx,
				 uint16_t out_bool_reg)
{
	if (steps.size() < first_step_idx || exprs.size() >= pg_yaap::pipeline::FILTER_MAX_INPUTS)
		return false;
	exprs.push_back(FilterExprDesc{
		static_cast<uint16_t>(first_step_idx),
		static_cast<uint16_t>(steps.size() - first_step_idx),
		out_bool_reg,
		0});
	return true;
}

static bool
LowerScanFilterBoolExpr(const Expression *expr,
						const std::vector<ColumnRef> &cols,
						const std::vector<ColumnSchema> &schema,
						std::vector<FilterInputDesc> &inputs,
						std::vector<FilterStep> &steps,
						std::vector<char> &string_consts,
						uint16_t &next_bool_reg,
						uint16_t &out_bool_reg);

static bool
LowerJoinFilterBoolExpr(const Expression *expr,
						const std::vector<ColumnRef> &left_cols,
						const std::vector<ColumnSchema> &left_schema,
						const std::vector<ColumnRef> &right_cols,
						const std::vector<ColumnSchema> &right_schema,
						std::vector<HashJoinFilterInputDesc> &inputs,
						std::vector<FilterStep> &steps,
						std::vector<char> &string_consts,
						uint16_t &next_bool_reg,
						uint16_t &out_bool_reg);

static bool
LowerScanFilterCompare(const BoundFunctionExpression *func,
					   const std::vector<ColumnRef> &cols,
					   const std::vector<ColumnSchema> &schema,
					   std::vector<FilterInputDesc> &inputs,
					   std::vector<FilterStep> &steps,
					   std::vector<char> &string_consts,
					   uint16_t &next_bool_reg,
					   uint16_t &out_bool_reg)
{
	if (func == nullptr || func->children.size() != 2 || !IsComparisonName(func->function_name))
		return false;
	const auto *left_col = dynamic_cast<const BoundColumnRefExpression *>(func->children[0].get());
	const auto *right_col = dynamic_cast<const BoundColumnRefExpression *>(func->children[1].get());
	const auto *left_const = dynamic_cast<const BoundConstantExpression *>(func->children[0].get());
	const auto *right_const = dynamic_cast<const BoundConstantExpression *>(func->children[1].get());

	FilterStep step{};
	step._pad0 = 0;
	step.const_offset = UINT32_MAX;
	step.const_len = 0;
	step.const_value = 0;
	step.right_idx = 0;
	step.out_bool_reg = next_bool_reg++;

	auto build_const = [&](const BoundColumnRefExpression *col_expr,
						   const Expression *const_expr,
						   bool constant_on_right) -> bool
	{
		const auto *constant = dynamic_cast<const BoundConstantExpression *>(const_expr);
		ColumnRef ref{};
		const ColumnSchema *col = nullptr;
		if (!LookupBindingColumn(col_expr->binding, cols, schema, ref, col) || col == nullptr)
			return false;
		if (!LookupOrAddScanFilterInput(*col, inputs, step.left_idx))
			return false;

		if (col->decode_kind == ColumnDecodeKind::INT32_DATE)
		{
			DateADT date_const = 0;
			if (!EvaluateDateExpression(const_expr, date_const) ||
				!MapComparisonNameToQualOp(func->function_name, step.cmp_op))
				return false;
			step.op = FilterStepOp::INT32_CMP_CONST;
			step.const_value = static_cast<uint64_t>(DateADTGetDatum(date_const));
			return true;
		}
		if (constant == nullptr)
			return false;
		if (col->decode_kind == ColumnDecodeKind::INT32_INT4)
		{
			Datum datum = 0;
			if (!ConvertConstantToDatum(constant, INT4OID, -1, datum) ||
				!MapComparisonNameToQualOp(func->function_name, step.cmp_op))
				return false;
			step.op = FilterStepOp::INT32_CMP_CONST;
			step.const_value = static_cast<uint64_t>(DatumGetInt32(datum));
			return true;
		}
		if (col->decode_kind == ColumnDecodeKind::INT32_CHAR)
		{
			int32_t ch = 0;
			if (!ExtractCharFilterConst(constant, ch) || !MapComparisonNameToQualOp(func->function_name, step.cmp_op))
				return false;
			step.op = FilterStepOp::INT32_CMP_CONST;
			step.const_value = static_cast<uint64_t>(static_cast<uint32_t>(ch));
			return true;
		}
		if (col->decode_kind == ColumnDecodeKind::INT64_INT8)
		{
			Datum datum = 0;
			if (!ConvertConstantToDatum(constant, INT8OID, -1, datum) ||
				!MapComparisonNameToQualOp(func->function_name, step.cmp_op))
				return false;
			step.op = FilterStepOp::INT64_CMP_CONST;
			step.const_value = static_cast<uint64_t>(DatumGetInt64(datum));
			return true;
		}
		if (col->decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED)
		{
			int64_t value = 0;
			const int8_t scale = static_cast<int8_t>(ExtractNumericTypmodScale(col->typmod));
			if (!ScaleNumericConstantToTargetScale(constant, scale, value) ||
				!MapComparisonNameToQualOp(func->function_name, step.cmp_op))
				return false;
			step.op = FilterStepOp::INT64_CMP_CONST;
			step.const_value = static_cast<uint64_t>(value);
			return true;
		}
		if (col->decode_kind == ColumnDecodeKind::STRING_REF)
		{
			if (func->function_name == "=")
				step.op = FilterStepOp::STRING_EQ_CONST;
			else if (func->function_name == "<>" || func->function_name == "!=")
				step.op = FilterStepOp::STRING_NE_CONST;
			else if (func->function_name == "~~")
			{
				bool prefix = false;
				std::string match;
				if (!TryExtractLikePattern(constant, prefix, match))
					return false;
				auto pattern_const = BoundConstantExpression(match, false);
				if (!StoreStringConstBytes(&pattern_const,
										   col->type_oid,
										   col->typmod,
										   string_consts,
										   step.const_offset,
										   step.const_len,
										   step.const_value))
					return false;
				step.op = prefix ? FilterStepOp::STRING_PREFIX_LIKE : FilterStepOp::STRING_CONTAINS_LIKE;
				step.cmp_op = QualOp::EQ;
				return true;
			}
			else
				return false;
			if (!StoreStringConstBytes(constant,
									  col->type_oid,
									  col->typmod,
									  string_consts,
									  step.const_offset,
									  step.const_len,
									  step.const_value))
				return false;
			step.cmp_op = QualOp::EQ;
			return true;
		}
		return false;
	};

	if (left_col != nullptr && right_col == nullptr && build_const(left_col, func->children[1].get(), true))
	{
		out_bool_reg = step.out_bool_reg;
		steps.push_back(step);
		return true;
	}
	if (right_col != nullptr && left_col == nullptr && build_const(right_col, func->children[0].get(), false))
	{
		if (func->function_name == "<")
			step.cmp_op = QualOp::GT;
		else if (func->function_name == "<=")
			step.cmp_op = QualOp::GE;
		else if (func->function_name == ">")
			step.cmp_op = QualOp::LT;
		else if (func->function_name == ">=")
			step.cmp_op = QualOp::LE;
		out_bool_reg = step.out_bool_reg;
		steps.push_back(step);
		return true;
	}
	if (left_col == nullptr || right_col == nullptr)
		return false;

	ColumnRef left_ref{};
	ColumnRef right_ref{};
	const ColumnSchema *left_schema = nullptr;
	const ColumnSchema *right_schema = nullptr;
	if (!LookupBindingColumn(left_col->binding, cols, schema, left_ref, left_schema) ||
		!LookupBindingColumn(right_col->binding, cols, schema, right_ref, right_schema) ||
		left_schema == nullptr || right_schema == nullptr ||
		left_schema->decode_kind != right_schema->decode_kind ||
		!MapComparisonNameToQualOp(func->function_name, step.cmp_op))
		return false;
	if (left_schema->decode_kind == ColumnDecodeKind::INT32_DATE ||
		left_schema->decode_kind == ColumnDecodeKind::INT32_INT4 ||
		left_schema->decode_kind == ColumnDecodeKind::INT32_CHAR)
		step.op = FilterStepOp::INT32_CMP_VAR;
	else if (left_schema->decode_kind == ColumnDecodeKind::INT64_INT8 ||
			 left_schema->decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED)
		step.op = FilterStepOp::INT64_CMP_VAR;
	else
		return false;
	if (!LookupOrAddScanFilterInput(*left_schema, inputs, step.left_idx) ||
		!LookupOrAddScanFilterInput(*right_schema, inputs, step.right_idx))
		return false;
	out_bool_reg = step.out_bool_reg;
	steps.push_back(step);
	return true;
}

static bool
LowerScanFilterBoolExpr(const Expression *expr,
						const std::vector<ColumnRef> &cols,
						const std::vector<ColumnSchema> &schema,
						std::vector<FilterInputDesc> &inputs,
						std::vector<FilterStep> &steps,
						std::vector<char> &string_consts,
						uint16_t &next_bool_reg,
						uint16_t &out_bool_reg)
{
	if (expr == nullptr)
		return false;
	if (expr->type == ExpressionType::BOUND_FUNCTION)
		return LowerScanFilterCompare(static_cast<const BoundFunctionExpression *>(expr),
									  cols, schema, inputs, steps, string_consts, next_bool_reg, out_bool_reg);
	if (expr->type != ExpressionType::BOUND_CONJUNCTION)
		return false;
	const auto *conj = static_cast<const BoundConjunctionExpression *>(expr);
	if (conj->children.empty())
		return false;
	if (conj->bool_expr_type == NOT_EXPR)
	{
		if (conj->children.size() != 1 || next_bool_reg >= pg_yaap::pipeline::FILTER_MAX_BOOL_REGS)
			return false;
		uint16_t child_reg = 0;
		if (!LowerScanFilterBoolExpr(conj->children[0].get(), cols, schema, inputs, steps, string_consts, next_bool_reg, child_reg))
			return false;
		out_bool_reg = next_bool_reg++;
		steps.push_back(FilterStep{FilterStepOp::BOOL_NOT, QualOp::EQ, child_reg, 0, out_bool_reg, 0, UINT32_MAX, 0, 0});
		return true;
	}
	uint16_t left_reg = 0;
	if (!LowerScanFilterBoolExpr(conj->children[0].get(), cols, schema, inputs, steps, string_consts, next_bool_reg, left_reg))
		return false;
	for (size_t i = 1; i < conj->children.size(); ++i)
	{
		if (next_bool_reg >= pg_yaap::pipeline::FILTER_MAX_BOOL_REGS)
			return false;
		uint16_t right_reg = 0;
		if (!LowerScanFilterBoolExpr(conj->children[i].get(), cols, schema, inputs, steps, string_consts, next_bool_reg, right_reg))
			return false;
		out_bool_reg = next_bool_reg++;
		steps.push_back(FilterStep{
			conj->bool_expr_type == AND_EXPR ? FilterStepOp::BOOL_AND : FilterStepOp::BOOL_OR,
			QualOp::EQ,
			left_reg,
			right_reg,
			out_bool_reg,
			0,
			UINT32_MAX,
			0,
			0});
		left_reg = out_bool_reg;
	}
	out_bool_reg = left_reg;
	return true;
}

static bool
LowerJoinFilterCompare(const BoundFunctionExpression *func,
					   const std::vector<ColumnRef> &left_cols,
					   const std::vector<ColumnSchema> &left_schema,
					   const std::vector<ColumnRef> &right_cols,
					   const std::vector<ColumnSchema> &right_schema,
					   std::vector<HashJoinFilterInputDesc> &inputs,
					   std::vector<FilterStep> &steps,
					   std::vector<char> &string_consts,
					   uint16_t &next_bool_reg,
					   uint16_t &out_bool_reg)
{
	if (func == nullptr || func->children.size() != 2 || !IsComparisonName(func->function_name))
		return false;
	const auto *left_col = dynamic_cast<const BoundColumnRefExpression *>(func->children[0].get());
	const auto *right_col = dynamic_cast<const BoundColumnRefExpression *>(func->children[1].get());
	const auto *left_const = dynamic_cast<const BoundConstantExpression *>(func->children[0].get());
	const auto *right_const = dynamic_cast<const BoundConstantExpression *>(func->children[1].get());

	FilterStep step{};
	step._pad0 = 0;
	step.const_offset = UINT32_MAX;
	step.const_len = 0;
	step.const_value = 0;
	step.right_idx = 0;
	step.out_bool_reg = next_bool_reg++;

	auto locate = [&](const BoundColumnRefExpression *col_expr,
					  uint16_t &idx,
					  const ColumnSchema *&col) -> bool
	{
		return LookupOrAddJoinFilterInput(BindingToColumnRef(col_expr->binding),
										  left_cols, left_schema,
										  right_cols, right_schema,
										  inputs, idx, col);
	};

	auto build_const = [&](const BoundColumnRefExpression *col_expr,
						   const BoundConstantExpression *constant) -> bool
	{
		const ColumnSchema *col = nullptr;
		if (!locate(col_expr, step.left_idx, col) || col == nullptr)
			return false;
		if (col->decode_kind == ColumnDecodeKind::INT32_DATE)
		{
			DateADT date_const = 0;
			if (!EvaluateDateExpression(constant, date_const) ||
				!MapComparisonNameToQualOp(func->function_name, step.cmp_op))
				return false;
			step.op = FilterStepOp::INT32_CMP_CONST;
			step.const_value = static_cast<uint64_t>(DateADTGetDatum(date_const));
			return true;
		}
		if (col->decode_kind == ColumnDecodeKind::INT32_INT4)
		{
			Datum datum = 0;
			if (!ConvertConstantToDatum(constant, INT4OID, -1, datum) ||
				!MapComparisonNameToQualOp(func->function_name, step.cmp_op))
				return false;
			step.op = FilterStepOp::INT32_CMP_CONST;
			step.const_value = static_cast<uint64_t>(DatumGetInt32(datum));
			return true;
		}
		if (col->decode_kind == ColumnDecodeKind::INT32_CHAR)
		{
			int32_t ch = 0;
			if (!ExtractCharFilterConst(constant, ch) || !MapComparisonNameToQualOp(func->function_name, step.cmp_op))
				return false;
			step.op = FilterStepOp::INT32_CMP_CONST;
			step.const_value = static_cast<uint64_t>(static_cast<uint32_t>(ch));
			return true;
		}
		if (col->decode_kind == ColumnDecodeKind::INT64_INT8)
		{
			Datum datum = 0;
			if (!ConvertConstantToDatum(constant, INT8OID, -1, datum) ||
				!MapComparisonNameToQualOp(func->function_name, step.cmp_op))
				return false;
			step.op = FilterStepOp::INT64_CMP_CONST;
			step.const_value = static_cast<uint64_t>(DatumGetInt64(datum));
			return true;
		}
		if (col->decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED)
		{
			int64_t value = 0;
			const int8_t scale = static_cast<int8_t>(ExtractNumericTypmodScale(col->typmod));
			if (!ScaleNumericConstantToTargetScale(constant, scale, value) ||
				!MapComparisonNameToQualOp(func->function_name, step.cmp_op))
				return false;
			step.op = FilterStepOp::INT64_CMP_CONST;
			step.const_value = static_cast<uint64_t>(value);
			return true;
		}
		if (col->decode_kind == ColumnDecodeKind::STRING_REF)
		{
			if (func->function_name == "=")
				step.op = FilterStepOp::STRING_EQ_CONST;
			else if (func->function_name == "<>" || func->function_name == "!=")
				step.op = FilterStepOp::STRING_NE_CONST;
			else if (func->function_name == "~~")
			{
				bool prefix = false;
				std::string match;
				if (!TryExtractLikePattern(constant, prefix, match))
					return false;
				auto pattern_const = BoundConstantExpression(match, false);
				if (!StoreStringConstBytes(&pattern_const,
										   col->type_oid,
										   col->typmod,
										   string_consts,
										   step.const_offset,
										   step.const_len,
										   step.const_value))
					return false;
				step.op = prefix ? FilterStepOp::STRING_PREFIX_LIKE : FilterStepOp::STRING_CONTAINS_LIKE;
				step.cmp_op = QualOp::EQ;
				return true;
			}
			else
				return false;
			if (!StoreStringConstBytes(constant,
									  col->type_oid,
									  col->typmod,
									  string_consts,
									  step.const_offset,
									  step.const_len,
									  step.const_value))
				return false;
			step.cmp_op = QualOp::EQ;
			return true;
		}
		return false;
	};

	if (left_col != nullptr && right_const != nullptr && build_const(left_col, right_const))
	{
		out_bool_reg = step.out_bool_reg;
		steps.push_back(step);
		return true;
	}
	if (right_col != nullptr && left_const != nullptr && build_const(right_col, left_const))
	{
		if (func->function_name == "<")
			step.cmp_op = QualOp::GT;
		else if (func->function_name == "<=")
			step.cmp_op = QualOp::GE;
		else if (func->function_name == ">")
			step.cmp_op = QualOp::LT;
		else if (func->function_name == ">=")
			step.cmp_op = QualOp::LE;
		out_bool_reg = step.out_bool_reg;
		steps.push_back(step);
		return true;
	}
	if (left_col == nullptr || right_col == nullptr)
		return false;

	const ColumnSchema *left_col_schema = nullptr;
	const ColumnSchema *right_col_schema = nullptr;
	if (!locate(left_col, step.left_idx, left_col_schema) ||
		!locate(right_col, step.right_idx, right_col_schema) ||
		left_col_schema == nullptr || right_col_schema == nullptr ||
		left_col_schema->decode_kind != right_col_schema->decode_kind ||
		!MapComparisonNameToQualOp(func->function_name, step.cmp_op))
		return false;
	if (left_col_schema->decode_kind == ColumnDecodeKind::INT32_DATE ||
		left_col_schema->decode_kind == ColumnDecodeKind::INT32_INT4 ||
		left_col_schema->decode_kind == ColumnDecodeKind::INT32_CHAR)
		step.op = FilterStepOp::INT32_CMP_VAR;
	else if (left_col_schema->decode_kind == ColumnDecodeKind::INT64_INT8 ||
			 left_col_schema->decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED)
		step.op = FilterStepOp::INT64_CMP_VAR;
	else
		return false;
	out_bool_reg = step.out_bool_reg;
	steps.push_back(step);
	return true;
}

static bool
LowerJoinFilterBoolExpr(const Expression *expr,
						const std::vector<ColumnRef> &left_cols,
						const std::vector<ColumnSchema> &left_schema,
						const std::vector<ColumnRef> &right_cols,
						const std::vector<ColumnSchema> &right_schema,
						std::vector<HashJoinFilterInputDesc> &inputs,
						std::vector<FilterStep> &steps,
						std::vector<char> &string_consts,
						uint16_t &next_bool_reg,
						uint16_t &out_bool_reg)
{
	if (expr == nullptr)
		return false;
	if (expr->type == ExpressionType::BOUND_FUNCTION)
		return LowerJoinFilterCompare(static_cast<const BoundFunctionExpression *>(expr),
									  left_cols, left_schema, right_cols, right_schema,
									  inputs, steps, string_consts, next_bool_reg, out_bool_reg);
	if (expr->type != ExpressionType::BOUND_CONJUNCTION)
		return false;
	const auto *conj = static_cast<const BoundConjunctionExpression *>(expr);
	if (conj->children.empty())
		return false;
	if (conj->bool_expr_type == NOT_EXPR)
	{
		if (conj->children.size() != 1 || next_bool_reg >= pg_yaap::pipeline::FILTER_MAX_BOOL_REGS)
			return false;
		uint16_t child_reg = 0;
		if (!LowerJoinFilterBoolExpr(conj->children[0].get(),
									 left_cols, left_schema, right_cols, right_schema,
									 inputs, steps, string_consts, next_bool_reg, child_reg))
			return false;
		out_bool_reg = next_bool_reg++;
		steps.push_back(FilterStep{FilterStepOp::BOOL_NOT, QualOp::EQ, child_reg, 0, out_bool_reg, 0, UINT32_MAX, 0, 0});
		return true;
	}
	uint16_t left_reg = 0;
	if (!LowerJoinFilterBoolExpr(conj->children[0].get(),
								 left_cols, left_schema, right_cols, right_schema,
								 inputs, steps, string_consts, next_bool_reg, left_reg))
		return false;
	for (size_t i = 1; i < conj->children.size(); ++i)
	{
		if (next_bool_reg >= pg_yaap::pipeline::FILTER_MAX_BOOL_REGS)
			return false;
		uint16_t right_reg = 0;
		if (!LowerJoinFilterBoolExpr(conj->children[i].get(),
									 left_cols, left_schema, right_cols, right_schema,
									 inputs, steps, string_consts, next_bool_reg, right_reg))
			return false;
		out_bool_reg = next_bool_reg++;
		steps.push_back(FilterStep{
			conj->bool_expr_type == AND_EXPR ? FilterStepOp::BOOL_AND : FilterStepOp::BOOL_OR,
			QualOp::EQ,
			left_reg,
			right_reg,
			out_bool_reg,
			0,
			UINT32_MAX,
			0,
			0});
		left_reg = out_bool_reg;
	}
	out_bool_reg = left_reg;
	return true;
}

static bool
LowerScanFilters(const std::vector<Expression *> &filters,
				 const std::vector<ColumnRef> &cols,
				 const std::vector<ColumnSchema> &schema,
				 std::vector<FilterInputDesc> &inputs,
				 std::vector<FilterExprDesc> &exprs,
				 std::vector<FilterStep> &steps,
				 std::vector<char> &string_consts)
{
	uint16_t next_bool_reg = 0;
	for (Expression *expr : filters)
	{
		if (expr == nullptr)
			return false;
		const size_t first_step_idx = steps.size();
		uint16_t out_bool_reg = 0;
		if (!LowerScanFilterBoolExpr(expr, cols, schema, inputs, steps, string_consts, next_bool_reg, out_bool_reg) ||
			!AppendFilterExpr(exprs, steps, first_step_idx, out_bool_reg))
		{
			if (pg_yaap_trace_hooks)
			{
				if (expr->type == ExpressionType::BOUND_FUNCTION)
					elog(LOG, "pg_yaap: scan filter lowering failed on function %s",
						 static_cast<BoundFunctionExpression *>(expr)->function_name.c_str());
				else if (expr->type == ExpressionType::BOUND_CONJUNCTION)
					elog(LOG, "pg_yaap: scan filter lowering failed on conjunction type=%d",
						 static_cast<BoundConjunctionExpression *>(expr)->bool_expr_type);
				else
					elog(LOG, "pg_yaap: scan filter lowering failed on expr type=%d", static_cast<int>(expr->type));
			}
			return false;
		}
	}
	return true;
}

static bool
LowerJoinFilters(const std::vector<Expression *> &filters,
				 const std::vector<ColumnRef> &left_cols,
				 const std::vector<ColumnSchema> &left_schema,
				 const std::vector<ColumnRef> &right_cols,
				 const std::vector<ColumnSchema> &right_schema,
				 std::vector<HashJoinFilterInputDesc> &inputs,
				 std::vector<FilterExprDesc> &exprs,
				 std::vector<FilterStep> &steps,
				 std::vector<char> &string_consts,
				 uint16_t &out_bool_regs)
{
	uint16_t next_bool_reg = 0;
	for (Expression *expr : filters)
	{
		if (expr == nullptr)
			return false;
		const size_t first_step_idx = steps.size();
		uint16_t out_bool_reg = 0;
		if (!LowerJoinFilterBoolExpr(expr,
									 left_cols, left_schema,
									 right_cols, right_schema,
									 inputs, steps, string_consts, next_bool_reg, out_bool_reg) ||
			!AppendFilterExpr(exprs, steps, first_step_idx, out_bool_reg))
			return false;
	}
	out_bool_regs = next_bool_reg;
	return true;
}

static bool
LookupCachedOptimizerExpr(const Expression *expr,
						  const std::vector<MaterializedOptExpr> *cache,
						  int8_t &out_scale,
						  uint8_t &out_slot)
{
	if (cache == nullptr)
		return false;
	for (const auto &entry : *cache)
	{
		if (entry.expr == expr)
		{
			out_scale = entry.scale;
			out_slot = entry.slot;
			return true;
		}
	}
	return false;
}

static bool
AppendScaleProjectStep(uint8_t input_slot,
					   int8_t input_scale,
					   int8_t target_scale,
					   std::vector<ProjectStep> &steps,
					   uint8_t &next_int64_slot,
					   uint8_t &out_slot)
{
	if (input_scale == target_scale)
	{
		out_slot = input_slot;
		return true;
	}
	int64_t factor = 0;
	const int delta = static_cast<int>(target_scale) - static_cast<int>(input_scale);
	if (!Pow10Int64(std::abs(delta), factor) || next_int64_slot >= 16)
		return false;
	out_slot = next_int64_slot++;
	steps.push_back(ProjectStep{
		ProjectOp::NUMERIC_SCALE_VAR_CONST,
		input_slot,
		0,
		out_slot,
		delta >= 0 ? factor : -factor});
	return true;
}

static bool
LowerOptimizerBoolExpr(const Expression *expr,
					   std::vector<ProjectStep> &steps,
					   uint8_t &next_int64_slot,
					   const std::vector<ColumnRef> &cols,
					   const std::vector<ColumnSchema> &schema,
					   uint8_t &out_slot);

static bool
LowerOptimizerExpr(const Expression *expr,
				   std::vector<ProjectStep> &steps,
				   uint8_t &next_int64_slot,
				   const std::vector<ColumnRef> &cols,
				   const std::vector<ColumnSchema> &schema,
				   const std::vector<MaterializedOptExpr> *cache,
				   int8_t &out_scale,
				   uint8_t &out_slot);

static bool
LowerProjectionConstant(const BoundConstantExpression *constant,
						std::vector<ProjectStep> &steps,
						uint8_t &next_int64_slot,
						int8_t &out_scale,
						uint8_t &out_slot)
{
	if (constant == nullptr || constant->is_null || next_int64_slot >= 16)
		return false;
	int64_t value = 0;
	int8_t scale = 0;
	if (pg_strcasecmp(constant->value.c_str(), "true") == 0)
		value = 1;
	else if (pg_strcasecmp(constant->value.c_str(), "false") == 0)
		value = 0;
	else if (constant->value.find('.') != std::string::npos)
	{
		if (!ScaleNumericConstantToInt64(constant, scale, value))
			return false;
	}
	else
	{
		const char *ptr = constant->value.c_str();
		char *end = nullptr;
		errno = 0;
		long long parsed = std::strtoll(ptr, &end, 10);
		if (errno != 0 || end == ptr)
			return false;
		while (*end != '\0' && std::isspace(static_cast<unsigned char>(*end)))
			++end;
		if (*end != '\0')
			return false;
		value = static_cast<int64_t>(parsed);
	}
	out_slot = next_int64_slot++;
	out_scale = scale;
	steps.push_back(ProjectStep{ProjectOp::CONST_INT64, 0, 0, out_slot, value});
	return true;
}

static bool
LowerProjectionStringCompare(const BoundFunctionExpression *func,
							 std::vector<ProjectStep> &steps,
							 uint8_t &next_int64_slot,
							 const std::vector<ColumnRef> &cols,
							 const std::vector<ColumnSchema> &schema,
							 uint8_t &out_slot)
{
	if (func == nullptr || func->children.size() != 2)
		return false;
	const auto *lhs = dynamic_cast<const BoundColumnRefExpression *>(func->children[0].get());
	const auto *rhs = dynamic_cast<const BoundConstantExpression *>(func->children[1].get());
	if (lhs == nullptr || rhs == nullptr)
		return false;
	ColumnRef ref{};
	const ColumnSchema *col = nullptr;
	if (!LookupBindingColumn(lhs->binding, cols, schema, ref, col) || col == nullptr || col->decode_kind != ColumnDecodeKind::STRING_REF)
		return false;
	uint8_t const_len = 0;
	int64_t const_value = 0;
	if (!TryExtractShortStringConst(rhs, const_len, const_value) || next_int64_slot >= 16)
		return false;
	out_slot = next_int64_slot++;
	if (func->function_name == "=")
		steps.push_back(ProjectStep{ProjectOp::STRING_EQ_VAR_CONST, col->chunk_slot, const_len, out_slot, const_value});
	else if (func->function_name == "<>" || func->function_name == "!=")
		steps.push_back(ProjectStep{ProjectOp::STRING_NE_VAR_CONST, col->chunk_slot, const_len, out_slot, const_value});
	else
		return false;
	return true;
}

static bool
LowerProjectionStringLike(const BoundFunctionExpression *func,
						  std::vector<ProjectStep> &steps,
						  uint8_t &next_int64_slot,
						  const std::vector<ColumnRef> &cols,
						  const std::vector<ColumnSchema> &schema,
						  uint8_t &out_slot)
{
	if (func == nullptr || func->children.size() != 2 || func->function_name != "~~")
		return false;
	const auto *lhs = dynamic_cast<const BoundColumnRefExpression *>(func->children[0].get());
	const auto *rhs = dynamic_cast<const BoundConstantExpression *>(func->children[1].get());
	if (lhs == nullptr || rhs == nullptr)
		return false;
	ColumnRef ref{};
	const ColumnSchema *col = nullptr;
	if (!LookupBindingColumn(lhs->binding, cols, schema, ref, col) || col == nullptr || col->decode_kind != ColumnDecodeKind::STRING_REF)
		return false;
	bool prefix = false;
	std::string match;
	if (!TryExtractLikePattern(rhs, prefix, match) || !prefix || match.size() > 8 || next_int64_slot >= 16)
		return false;
	int64_t packed = 0;
	if (!match.empty())
		std::memcpy(&packed, match.data(), match.size());
	out_slot = next_int64_slot++;
	steps.push_back(ProjectStep{
		ProjectOp::STRING_PREFIX_LIKE,
		col->chunk_slot,
		static_cast<uint8_t>(match.size()),
		out_slot,
		packed});
	return true;
}

static bool
LowerOptimizerBoolExpr(const Expression *expr,
					   std::vector<ProjectStep> &steps,
					   uint8_t &next_int64_slot,
					   const std::vector<ColumnRef> &cols,
					   const std::vector<ColumnSchema> &schema,
					   uint8_t &out_slot)
{
	if (expr == nullptr)
		return false;
	if (expr->type == ExpressionType::BOUND_FUNCTION)
	{
		const auto *func = static_cast<const BoundFunctionExpression *>(expr);
		if (LowerProjectionStringLike(func, steps, next_int64_slot, cols, schema, out_slot) ||
			LowerProjectionStringCompare(func, steps, next_int64_slot, cols, schema, out_slot))
			return true;
	}
	if (expr->type != ExpressionType::BOUND_CONJUNCTION)
		return false;
	const auto *conj = static_cast<const BoundConjunctionExpression *>(expr);
	if (conj->children.empty())
		return false;
	if (conj->bool_expr_type == NOT_EXPR)
	{
		if (conj->children.size() != 1 || next_int64_slot >= 16)
			return false;
		uint8_t child_slot = 0;
		if (!LowerOptimizerBoolExpr(conj->children[0].get(), steps, next_int64_slot, cols, schema, child_slot))
			return false;
		out_slot = next_int64_slot++;
		steps.push_back(ProjectStep{ProjectOp::BOOL_NOT_VAR, child_slot, 0, out_slot, 0});
		return true;
	}
	uint8_t left_slot = 0;
	if (!LowerOptimizerBoolExpr(conj->children[0].get(), steps, next_int64_slot, cols, schema, left_slot))
		return false;
	for (size_t i = 1; i < conj->children.size(); ++i)
	{
		if (next_int64_slot >= 16)
			return false;
		uint8_t right_slot = 0;
		if (!LowerOptimizerBoolExpr(conj->children[i].get(), steps, next_int64_slot, cols, schema, right_slot))
			return false;
		out_slot = next_int64_slot++;
		steps.push_back(ProjectStep{
			conj->bool_expr_type == AND_EXPR ? ProjectOp::BOOL_AND_VAR_VAR : ProjectOp::BOOL_OR_VAR_VAR,
			left_slot,
			right_slot,
			out_slot,
			0});
		left_slot = out_slot;
	}
	out_slot = left_slot;
	return true;
}

static bool
LowerExtractYearExpr(const BoundFunctionExpression *func,
					 std::vector<ProjectStep> &steps,
					 uint8_t &next_int64_slot,
					 const std::vector<ColumnRef> &cols,
					 const std::vector<ColumnSchema> &schema,
					 int8_t &out_scale,
					 uint8_t &out_slot)
{
	if (func == nullptr || func->children.size() != 2 ||
		(func->function_name != "extract" && func->function_name != "date_part"))
		return false;
	const auto *field = dynamic_cast<const BoundConstantExpression *>(func->children[0].get());
	const auto *value = dynamic_cast<const BoundColumnRefExpression *>(func->children[1].get());
	if (field == nullptr || value == nullptr || pg_strcasecmp(field->value.c_str(), "year") != 0 || next_int64_slot >= 16)
		return false;
	ColumnRef ref{};
	const ColumnSchema *col = nullptr;
	if (!LookupBindingColumn(value->binding, cols, schema, ref, col) || col == nullptr || col->decode_kind != ColumnDecodeKind::INT32_DATE)
		return false;
	out_slot = next_int64_slot++;
	out_scale = 0;
	steps.push_back(ProjectStep{ProjectOp::EXTRACT_YEAR_FROM_DATE, col->chunk_slot, 0, out_slot, 0});
	return true;
}

static bool
LowerNumericBinaryExpr(const BoundFunctionExpression *func,
					   std::vector<ProjectStep> &steps,
					   uint8_t &next_int64_slot,
					   const std::vector<ColumnRef> &cols,
					   const std::vector<ColumnSchema> &schema,
					   const std::vector<MaterializedOptExpr> *cache,
					   int8_t &out_scale,
					   uint8_t &out_slot)
{
	if (func == nullptr || func->children.size() != 2)
		return false;
	const bool is_mul = func->function_name == "*";
	const bool is_add = func->function_name == "+";
	const bool is_sub = func->function_name == "-";
	const bool is_div = func->function_name == "/";
	if (!is_mul && !is_add && !is_sub && !is_div)
		return false;

	const Expression *lhs = func->children[0].get();
	const Expression *rhs = func->children[1].get();
	const auto *lhs_const = dynamic_cast<const BoundConstantExpression *>(lhs);
	const auto *rhs_const = dynamic_cast<const BoundConstantExpression *>(rhs);

	if (is_div)
	{
		if (lhs_const != nullptr || rhs_const != nullptr || next_int64_slot >= 16)
			return false;
		int8_t lhs_scale = 0;
		int8_t rhs_scale = 0;
		uint8_t lhs_slot = 0;
		uint8_t rhs_slot = 0;
		if (!LowerOptimizerExpr(lhs, steps, next_int64_slot, cols, schema, cache, lhs_scale, lhs_slot) ||
			!LowerOptimizerExpr(rhs, steps, next_int64_slot, cols, schema, cache, rhs_scale, rhs_slot))
			return false;
		int64_t factor = 0;
		out_scale = kProjectionDivisionScale;
		if (!Pow10Int64(static_cast<int>(out_scale) + static_cast<int>(rhs_scale) - static_cast<int>(lhs_scale), factor))
			return false;
		out_slot = next_int64_slot++;
		steps.push_back(ProjectStep{ProjectOp::NUMERIC_DIV_VAR_VAR, lhs_slot, rhs_slot, out_slot, factor});
		return true;
	}

	if (is_mul)
	{
		if (lhs_const != nullptr && rhs_const != nullptr)
			return false;
		if (lhs_const != nullptr || rhs_const != nullptr)
		{
			const Expression *var_expr = lhs_const != nullptr ? rhs : lhs;
			const auto *const_expr = lhs_const != nullptr ? lhs_const : rhs_const;
			int8_t var_scale = 0;
			uint8_t var_slot = 0;
			if (!LowerOptimizerExpr(var_expr, steps, next_int64_slot, cols, schema, cache, var_scale, var_slot))
				return false;
			int8_t const_scale = 0;
			int64_t const_value = 0;
			if (!ScaleNumericConstantToInt64(const_expr, const_scale, const_value) || next_int64_slot >= 16)
				return false;
			out_slot = next_int64_slot++;
			out_scale = static_cast<int8_t>(var_scale + const_scale);
			steps.push_back(ProjectStep{ProjectOp::NUMERIC_MUL_VAR_CONST, var_slot, 0, out_slot, const_value});
			return true;
		}
		int8_t lhs_scale = 0;
		int8_t rhs_scale = 0;
		uint8_t lhs_slot = 0;
		uint8_t rhs_slot = 0;
		if (!LowerOptimizerExpr(lhs, steps, next_int64_slot, cols, schema, cache, lhs_scale, lhs_slot) ||
			!LowerOptimizerExpr(rhs, steps, next_int64_slot, cols, schema, cache, rhs_scale, rhs_slot) ||
			next_int64_slot >= 16)
			return false;
		out_slot = next_int64_slot++;
		out_scale = static_cast<int8_t>(lhs_scale + rhs_scale);
		steps.push_back(ProjectStep{ProjectOp::NUMERIC_MUL_VAR_VAR, lhs_slot, rhs_slot, out_slot, 0});
		return true;
	}

	if (lhs_const != nullptr && rhs_const != nullptr)
		return false;
	if (lhs_const != nullptr)
	{
		int8_t rhs_scale = 0;
		uint8_t rhs_slot = 0;
		if (!LowerOptimizerExpr(rhs, steps, next_int64_slot, cols, schema, cache, rhs_scale, rhs_slot))
			return false;
		int8_t lhs_scale = 0;
		int64_t lhs_value = 0;
		if (!ScaleNumericConstantToInt64(lhs_const, lhs_scale, lhs_value))
			return false;
		out_scale = static_cast<int8_t>(Max(lhs_scale, rhs_scale));
		if (!RescaleInt64Constant(lhs_value, lhs_scale, out_scale, lhs_value) || next_int64_slot >= 16)
			return false;
		out_slot = next_int64_slot++;
		steps.push_back(ProjectStep{
			is_sub ? ProjectOp::NUMERIC_SUB_CONST_VAR : ProjectOp::NUMERIC_ADD_CONST_VAR,
			0,
			rhs_slot,
			out_slot,
			lhs_value});
		return true;
	}
	if (rhs_const != nullptr)
	{
		int8_t lhs_scale = 0;
		uint8_t lhs_slot = 0;
		if (!LowerOptimizerExpr(lhs, steps, next_int64_slot, cols, schema, cache, lhs_scale, lhs_slot))
			return false;
		int8_t rhs_scale = 0;
		int64_t rhs_value = 0;
		if (!ScaleNumericConstantToInt64(rhs_const, rhs_scale, rhs_value))
			return false;
		out_scale = static_cast<int8_t>(Max(lhs_scale, rhs_scale));
		uint8_t lhs_aligned = lhs_slot;
		if (!AppendScaleProjectStep(lhs_slot, lhs_scale, out_scale, steps, next_int64_slot, lhs_aligned) ||
			!RescaleInt64Constant(rhs_value, rhs_scale, out_scale, rhs_value) ||
			next_int64_slot >= 16)
			return false;
		out_slot = next_int64_slot++;
		steps.push_back(ProjectStep{
			is_sub ? ProjectOp::NUMERIC_SUB_VAR_CONST : ProjectOp::NUMERIC_ADD_VAR_CONST,
			lhs_aligned,
			0,
			out_slot,
			rhs_value});
		return true;
	}
	int8_t lhs_scale = 0;
	int8_t rhs_scale = 0;
	uint8_t lhs_slot = 0;
	uint8_t rhs_slot = 0;
	if (!LowerOptimizerExpr(lhs, steps, next_int64_slot, cols, schema, cache, lhs_scale, lhs_slot) ||
		!LowerOptimizerExpr(rhs, steps, next_int64_slot, cols, schema, cache, rhs_scale, rhs_slot))
		return false;
	out_scale = static_cast<int8_t>(Max(lhs_scale, rhs_scale));
	uint8_t lhs_aligned = lhs_slot;
	uint8_t rhs_aligned = rhs_slot;
	if (!AppendScaleProjectStep(lhs_slot, lhs_scale, out_scale, steps, next_int64_slot, lhs_aligned) ||
		!AppendScaleProjectStep(rhs_slot, rhs_scale, out_scale, steps, next_int64_slot, rhs_aligned) ||
		next_int64_slot >= 16)
		return false;
	out_slot = next_int64_slot++;
	steps.push_back(ProjectStep{
		is_sub ? ProjectOp::NUMERIC_SUB_VAR_VAR : ProjectOp::NUMERIC_ADD_VAR_VAR,
		lhs_aligned,
		rhs_aligned,
		out_slot,
		0});
	return true;
}

static bool
LowerCaseExpr(const BoundFunctionExpression *func,
			  std::vector<ProjectStep> &steps,
			  uint8_t &next_int64_slot,
			  const std::vector<ColumnRef> &cols,
			  const std::vector<ColumnSchema> &schema,
			  const std::vector<MaterializedOptExpr> *cache,
			  int8_t &out_scale,
			  uint8_t &out_slot)
{
	if (func == nullptr || func->function_name != "case" || func->children.size() < 2)
		return false;
	const auto *when_func = dynamic_cast<const BoundFunctionExpression *>(func->children[0].get());
	if (when_func == nullptr || when_func->function_name != "when" || when_func->children.size() != 2)
		return false;
	uint8_t cond_slot = 0;
	if (!LowerOptimizerBoolExpr(when_func->children[0].get(), steps, next_int64_slot, cols, schema, cond_slot))
		return false;
	int8_t then_scale = 0;
	uint8_t then_slot = 0;
	if (!LowerOptimizerExpr(when_func->children[1].get(), steps, next_int64_slot, cols, schema, cache, then_scale, then_slot))
		return false;
	const Expression *else_expr = func->children.back().get();
	const auto *else_const = dynamic_cast<const BoundConstantExpression *>(else_expr);
	if (else_const != nullptr)
	{
		int64_t else_value = 0;
		if (!ScaleConstantToTargetScale(else_const, then_scale, else_value) || next_int64_slot >= 16)
			return false;
		out_slot = next_int64_slot++;
		out_scale = then_scale;
		steps.push_back(ProjectStep{
			ProjectOp::NUMERIC_CASE_VAR_CONST,
			cond_slot,
			then_slot,
			out_slot,
			else_value});
		return true;
	}
	int8_t else_scale = 0;
	uint8_t else_slot = 0;
	if (!LowerOptimizerExpr(else_expr, steps, next_int64_slot, cols, schema, cache, else_scale, else_slot))
		return false;
	out_scale = static_cast<int8_t>(Max(then_scale, else_scale));
	uint8_t aligned_then = then_slot;
	uint8_t aligned_else = else_slot;
	if (!AppendScaleProjectStep(then_slot, then_scale, out_scale, steps, next_int64_slot, aligned_then) ||
		!AppendScaleProjectStep(else_slot, else_scale, out_scale, steps, next_int64_slot, aligned_else) ||
		next_int64_slot >= 16)
		return false;
	out_slot = next_int64_slot++;
	steps.push_back(ProjectStep{
		ProjectOp::NUMERIC_CASE_ELSE_VAR,
		cond_slot,
		aligned_then,
		out_slot,
		aligned_else});
	return true;
}

static bool
LowerOptimizerExpr(const Expression *expr,
				   std::vector<ProjectStep> &steps,
				   uint8_t &next_int64_slot,
				   const std::vector<ColumnRef> &cols,
				   const std::vector<ColumnSchema> &schema,
				   const std::vector<MaterializedOptExpr> *cache,
				   int8_t &out_scale,
				   uint8_t &out_slot)
{
	if (expr == nullptr)
		return false;
	if (LookupCachedOptimizerExpr(expr, cache, out_scale, out_slot))
		return true;
	if (expr->type == ExpressionType::BOUND_CONSTANT)
		return LowerProjectionConstant(static_cast<const BoundConstantExpression *>(expr), steps, next_int64_slot, out_scale, out_slot);
	if (expr->type == ExpressionType::BOUND_COLUMN_REF)
	{
		ColumnRef ref{};
		const ColumnSchema *col = nullptr;
		const auto *bound = static_cast<const BoundColumnRefExpression *>(expr);
		if (!LookupBindingColumn(bound->binding, cols, schema, ref, col) || col == nullptr || !ColumnNumericScale(*col, out_scale))
			return false;
		out_slot = col->chunk_slot;
		return true;
	}
	if (expr->type != ExpressionType::BOUND_FUNCTION)
		return false;
	const auto *func = static_cast<const BoundFunctionExpression *>(expr);
	if (LowerExtractYearExpr(func, steps, next_int64_slot, cols, schema, out_scale, out_slot) ||
		LowerCaseExpr(func, steps, next_int64_slot, cols, schema, cache, out_scale, out_slot) ||
		LowerNumericBinaryExpr(func, steps, next_int64_slot, cols, schema, cache, out_scale, out_slot))
		return true;
	return false;
}

static bool
InferProjectionExprSchema(const Expression *expr,
						  const std::vector<ColumnRef> &cols,
						  const std::vector<ColumnSchema> &schema,
						  Oid &out_type_oid,
						  int32 &out_typmod,
						  int8_t &out_scale)
{
	if (expr == nullptr)
		return false;
	if (expr->type == ExpressionType::BOUND_COLUMN_REF)
	{
		ColumnRef ref{};
		const ColumnSchema *col = nullptr;
		if (!LookupBindingColumn(static_cast<const BoundColumnRefExpression *>(expr)->binding, cols, schema, ref, col) || col == nullptr)
			return false;
		out_type_oid = col->type_oid;
		out_typmod = col->typmod;
		return ColumnNumericScale(*col, out_scale);
	}
	if (expr->type == ExpressionType::BOUND_CONSTANT)
	{
		const auto *constant = static_cast<const BoundConstantExpression *>(expr);
		if (pg_strcasecmp(constant->value.c_str(), "true") == 0 || pg_strcasecmp(constant->value.c_str(), "false") == 0)
		{
			out_type_oid = BOOLOID;
			out_typmod = -1;
			out_scale = 0;
			return true;
		}
		if (constant->value.find('.') != std::string::npos)
		{
			out_type_oid = NUMERICOID;
			out_typmod = -1;
			out_scale = static_cast<int8_t>(constant->value.size() - constant->value.find('.') - 1);
			return true;
		}
		out_type_oid = INT8OID;
		out_typmod = -1;
		out_scale = 0;
		return true;
	}
	if (expr->type != ExpressionType::BOUND_FUNCTION)
		return false;
	const auto *func = static_cast<const BoundFunctionExpression *>(expr);
	if (func->function_name == "extract" || func->function_name == "date_part" ||
		func->function_name == "+" || func->function_name == "-" ||
		func->function_name == "*" || func->function_name == "/" ||
		func->function_name == "case")
	{
		out_type_oid = NUMERICOID;
		out_typmod = -1;
		out_scale = 0;
		return true;
	}
	if (func->function_name == "=" || func->function_name == "<>" || func->function_name == "!=" ||
		func->function_name == "~~")
	{
		out_type_oid = BOOLOID;
		out_typmod = -1;
		out_scale = 0;
		return true;
	}
	return false;
}

static uint8_t
NextFreeInt64Slot(const std::vector<ColumnSchema> &schema)
{
	uint8_t next_slot = 0;
	for (const ColumnSchema &col : schema)
	{
		if (col.decode_kind != ColumnDecodeKind::INT64_INT8 &&
			col.decode_kind != ColumnDecodeKind::INT64_NUMERIC_SCALED)
			continue;
		const uint8_t candidate = static_cast<uint8_t>(col.chunk_slot + 1);
		if (candidate > next_slot)
			next_slot = candidate;
	}
	return next_slot;
}

static bool
ClassifyOptimizerAggregate(const BoundAggregateExpression *agg,
						   const std::vector<ColumnRef> &cols,
						   const std::vector<ColumnSchema> &schema,
						   std::vector<ProjectStep> &project_steps,
						   std::vector<ProjectExprDesc> &project_exprs,
						   std::vector<MaterializedOptExpr> &materialized_exprs,
						   uint8_t &next_int64_slot,
						   AggFuncDesc &out_desc,
						   TdcAggKind &out_kind,
						   int16_t &out_numeric_scale)
{
	if (agg == nullptr || agg->is_distinct)
		return false;
	out_desc = AggFuncDesc{static_cast<Oid>(agg->agg_oid), InvalidOid, InvalidOid, 0, 0};
	out_numeric_scale = 0;

	if (pg_strcasecmp(agg->function_name.c_str(), "count") == 0)
	{
		if (!agg->children.empty())
			return false;
		out_kind = TdcAggKind::COUNT_STAR;
		return true;
	}
	if (agg->children.size() != 1)
		return false;

	const Expression *arg = agg->children[0].get();
	ColumnRef bare_ref{};
	const ColumnSchema *bare_col = nullptr;
	bool is_bare_ref = false;
	if (arg != nullptr && arg->type == ExpressionType::BOUND_COLUMN_REF)
		is_bare_ref = LookupBindingColumn(static_cast<const BoundColumnRefExpression *>(arg)->binding, cols, schema, bare_ref, bare_col);

	if (pg_strcasecmp(agg->function_name.c_str(), "sum") == 0)
	{
		if (is_bare_ref && bare_col != nullptr &&
			(bare_col->decode_kind == ColumnDecodeKind::INT32_INT4 || bare_col->decode_kind == ColumnDecodeKind::INT64_INT8))
		{
			if (bare_col->decode_kind == ColumnDecodeKind::INT32_INT4)
			{
				if (next_int64_slot >= 16)
					return false;
				const uint16_t first = static_cast<uint16_t>(project_steps.size());
				const uint8_t cast_slot = next_int64_slot++;
				project_steps.push_back(ProjectStep{ProjectOp::INT32_TO_INT64_VAR, bare_col->chunk_slot, 0, cast_slot, 0});
				project_exprs.push_back(ProjectExprDesc{first, 1, cast_slot, 0, 0});
				materialized_exprs.push_back(MaterializedOptExpr{arg, 0, cast_slot});
				out_desc.input_col_idx = cast_slot;
			}
			else
				out_desc.input_col_idx = bare_col->chunk_slot;
			out_kind = TdcAggKind::SUM_INT64;
			return true;
		}
		out_kind = TdcAggKind::SUM_NUMERIC;
	}
	else if (pg_strcasecmp(agg->function_name.c_str(), "avg") == 0)
		out_kind = TdcAggKind::AVG_NUMERIC;
	else
		return false;

	if (is_bare_ref && bare_col != nullptr)
	{
		int8_t scale = 0;
		if (!ColumnNumericScale(*bare_col, scale))
			return false;
		out_desc.input_col_idx = bare_col->chunk_slot;
		out_numeric_scale = (out_kind == TdcAggKind::AVG_NUMERIC) ? static_cast<int16_t>(scale + kAvgNumericExtraScale) : scale;
		return true;
	}

	int8_t lowered_scale = 0;
	uint8_t lowered_slot = 0;
	if (LookupCachedOptimizerExpr(arg, &materialized_exprs, lowered_scale, lowered_slot))
	{
		out_desc.input_col_idx = lowered_slot;
		out_numeric_scale = (out_kind == TdcAggKind::AVG_NUMERIC) ? static_cast<int16_t>(lowered_scale + kAvgNumericExtraScale) : lowered_scale;
		return true;
	}

	const uint16_t first_step_idx = static_cast<uint16_t>(project_steps.size());
	if (!LowerOptimizerExpr(arg, project_steps, next_int64_slot, cols, schema, &materialized_exprs, lowered_scale, lowered_slot) ||
		project_steps.size() == first_step_idx)
		return false;
	project_exprs.push_back(ProjectExprDesc{
		first_step_idx,
		static_cast<uint16_t>(project_steps.size() - first_step_idx),
		lowered_slot,
		lowered_scale,
		0});
	materialized_exprs.push_back(MaterializedOptExpr{arg, lowered_scale, lowered_slot});
	out_desc.input_col_idx = lowered_slot;
	out_numeric_scale = (out_kind == TdcAggKind::AVG_NUMERIC) ? static_cast<int16_t>(lowered_scale + kAvgNumericExtraScale) : lowered_scale;
	return true;
}

static bool
BuildOptimizerAggOutput(const PhysicalHashAggregate &agg,
						const std::vector<ColumnRef> &input_cols,
						const std::vector<ColumnSchema> &input_schema,
						const AggBuildState &agg_state,
						std::vector<ColumnRef> &out_cols,
						std::vector<ColumnSchema> &out_schema)
{
	out_cols.clear();
	out_schema.clear();
	if (agg.groups.size() + agg.expressions.size() > 16)
		return false;

	for (size_t i = 0; i < agg.groups.size(); ++i)
	{
		const auto *col_expr = dynamic_cast<const BoundColumnRefExpression *>(agg.groups[i]);
		if (col_expr == nullptr)
			return false;
		ColumnRef ref{};
		const ColumnSchema *src = nullptr;
		if (!LookupBindingColumn(col_expr->binding, input_cols, input_schema, ref, src) || src == nullptr)
			return false;
		ColumnSchema cs = *src;
		cs.chunk_slot = src->chunk_slot;
		cs.src_attno = 0;
		out_schema.push_back(cs);
		out_cols.push_back(ColumnRef{
			static_cast<Index>(agg.group_index.index + 1),
			static_cast<AttrNumber>(i + 1)});
	}

	for (size_t i = 0; i < agg_state.agg_kinds.size(); ++i)
	{
		ColumnSchema cs{};
		cs.chunk_slot = static_cast<uint8_t>(agg.groups.size() + i);
		cs.src_attno = 0;
		cs._pad0 = 0;
		switch (agg_state.agg_kinds[i])
		{
			case TdcAggKind::COUNT_STAR:
			case TdcAggKind::SUM_INT64:
				cs.type_oid = INT8OID;
				cs.typmod = -1;
				cs.typlen = 8;
				cs.typbyval = true;
				cs.decode_kind = ColumnDecodeKind::INT64_INT8;
				break;
			case TdcAggKind::SUM_NUMERIC:
			case TdcAggKind::AVG_NUMERIC:
				cs.type_oid = NUMERICOID;
				cs.typmod = MakeNumericTypmod(18, agg_state.agg_numeric_scales[i]);
				cs.typlen = -1;
				cs.typbyval = false;
				cs.decode_kind = ColumnDecodeKind::INT64_NUMERIC_SCALED;
				break;
			default:
				return false;
		}
		out_schema.push_back(cs);
		out_cols.push_back(ColumnRef{
			static_cast<Index>(agg.aggregate_index.index + 1),
			static_cast<AttrNumber>(i + 1)});
	}

	return out_cols.size() == out_schema.size();
}

static bool
CollectJoinKeys(const Expression *expr,
				const std::vector<ColumnRef> &left_cols,
				const std::vector<ColumnSchema> &left_schema,
				const std::vector<ColumnRef> &right_cols,
				const std::vector<ColumnSchema> &right_schema,
				std::vector<ColumnRef> &left_keys,
				std::vector<ColumnRef> &right_keys,
				std::vector<Expression *> &residuals)
{
	if (expr == nullptr)
		return false;
	if (expr->type == ExpressionType::BOUND_CONJUNCTION)
	{
		const auto *conj = static_cast<const BoundConjunctionExpression *>(expr);
		if (conj->bool_expr_type != AND_EXPR)
		{
			residuals.push_back(const_cast<Expression *>(expr));
			return true;
		}
		for (const auto &child : conj->children)
		{
			if (!CollectJoinKeys(child.get(), left_cols, left_schema, right_cols, right_schema, left_keys, right_keys, residuals))
				return false;
		}
		return true;
	}
	if (expr->type != ExpressionType::BOUND_FUNCTION)
	{
		residuals.push_back(const_cast<Expression *>(expr));
		return true;
	}
	const auto *func = static_cast<const BoundFunctionExpression *>(expr);
	if (func->function_name != "=" || func->children.size() != 2)
	{
		residuals.push_back(const_cast<Expression *>(expr));
		return true;
	}
	const auto *lhs = dynamic_cast<const BoundColumnRefExpression *>(func->children[0].get());
	const auto *rhs = dynamic_cast<const BoundColumnRefExpression *>(func->children[1].get());
	if (lhs == nullptr || rhs == nullptr)
	{
		residuals.push_back(const_cast<Expression *>(expr));
		return true;
	}

	ColumnRef lhs_ref{};
	ColumnRef rhs_ref{};
	const ColumnSchema *lhs_schema = nullptr;
	const ColumnSchema *rhs_schema = nullptr;
	const bool lhs_left = LookupBindingColumn(lhs->binding, left_cols, left_schema, lhs_ref, lhs_schema);
	const bool lhs_right = LookupBindingColumn(lhs->binding, right_cols, right_schema, lhs_ref, lhs_schema);
	const bool rhs_left = LookupBindingColumn(rhs->binding, left_cols, left_schema, rhs_ref, rhs_schema);
	const bool rhs_right = LookupBindingColumn(rhs->binding, right_cols, right_schema, rhs_ref, rhs_schema);
	if (lhs_schema == nullptr || rhs_schema == nullptr || lhs_schema->decode_kind != rhs_schema->decode_kind)
	{
		residuals.push_back(const_cast<Expression *>(expr));
		return true;
	}
	if (lhs_left && rhs_right)
	{
		left_keys.push_back(lhs_ref);
		right_keys.push_back(rhs_ref);
		return true;
	}
	if (lhs_right && rhs_left)
	{
		left_keys.push_back(rhs_ref);
		right_keys.push_back(lhs_ref);
		return true;
	}
	residuals.push_back(const_cast<Expression *>(expr));
	return true;
}

static std::unique_ptr<PipelineOperator>
BuildOutputContract(OptimizerNodeTranslation &node,
					QueryDesc *queryDesc,
					PgYaapQueryState *state)
{
	if (node.op == nullptr || state == nullptr || state->runtime_dsa == nullptr || node.schema.empty())
		return nullptr;
	TupleDataLayout output_layout;
	if (!BuildColumnOnlyLayout(node.schema, output_layout))
		return nullptr;
	dsa_pointer output_schema_dp = BuildSchemaDescriptorFromColumns(node.schema, state->runtime_dsa);
	dsa_pointer output_layout_dp = SerializeTupleDataLayout(output_layout, state->runtime_dsa);
	if (!DsaPointerIsValid(output_schema_dp) || !DsaPointerIsValid(output_layout_dp))
		return nullptr;
	const uint32_t row_capacity = EstimateInitialResultRows(queryDesc, node.estimated_groups);
	const dsa_pointer output_payload_dp = BuildOutputTdc(state->runtime_dsa, output_layout_dp, output_layout, row_capacity);
	if (!DsaPointerIsValid(output_payload_dp))
		return nullptr;

	auto output_op = std::make_unique<OutputSink>(
		queryDesc->dest,
		queryDesc->tupDesc,
		static_cast<int>(queryDesc->operation),
		output_schema_dp,
		output_layout_dp,
		output_payload_dp,
		row_capacity,
		node.final_sort_keys,
		node.limit_count,
		nullptr);
	output_op->AddChild(std::move(node.op));
	return output_op;
}

static bool
TryBuildPureProjection(const PhysicalProjection &projection,
					   OptimizerNodeTranslation &child,
					   OptimizerNodeTranslation &out)
{
	std::vector<ColumnRef> new_cols;
	std::vector<ColumnSchema> new_schema;
	new_cols.reserve(projection.select_list.size());
	new_schema.reserve(projection.select_list.size());
	for (Expression *expr : projection.select_list)
	{
		const auto *col_expr = dynamic_cast<const BoundColumnRefExpression *>(expr);
		if (col_expr == nullptr)
			return false;
		ColumnRef ref{};
		const ColumnSchema *col = nullptr;
		if (!LookupBindingColumn(col_expr->binding, child.cols, child.schema, ref, col) || col == nullptr)
			return false;
		new_cols.push_back(ref);
		new_schema.push_back(*col);
	}
	out.op = std::move(child.op);
	out.cols = std::move(new_cols);
	out.schema = std::move(new_schema);
	out.final_sort_keys = child.final_sort_keys;
	out.limit_count = child.limit_count;
	out.estimated_groups = child.estimated_groups;
	return true;
}

static bool
TranslateProjectionNode(const PhysicalProjection &projection,
						QueryDesc *queryDesc,
						PgYaapQueryState *state,
						OptimizerNodeTranslation &out)
{
	if (projection.children.size() != 1 || projection.children[0] == nullptr)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer projection rejected: invalid child shape");
		return false;
	}
	OptimizerNodeTranslation child;
	std::vector<ColumnRef> child_required;
	for (Expression *expr : projection.select_list)
		CollectReferencedColumns(expr, child_required);
	if (!TranslateOptimizerNode(*projection.children[0], queryDesc, state, {}, &child_required, child) || child.op == nullptr)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer projection rejected: child translation failed");
		return false;
	}
	if (TryBuildPureProjection(projection, child, out))
		return true;

	std::vector<ProjectStep> steps;
	std::vector<ProjectExprDesc> expr_descs;
	std::vector<ColumnSchema> out_schema;
	uint8_t next_int64_slot = NextFreeInt64Slot(child.schema);
	for (Expression *expr : projection.select_list)
	{
		const uint16_t first_step_idx = static_cast<uint16_t>(steps.size());
		int8_t result_scale = 0;
		uint8_t result_slot = 0;
		Oid type_oid = InvalidOid;
		int32 typmod = -1;
		if (!InferProjectionExprSchema(expr, child.cols, child.schema, type_oid, typmod, result_scale) ||
			!LowerOptimizerExpr(expr, steps, next_int64_slot, child.cols, child.schema, nullptr, result_scale, result_slot))
		{
			if (pg_yaap_trace_hooks)
				elog(LOG, "pg_yaap: optimizer projection rejected: expr lowering failed");
			return false;
		}
		ColumnSchema mapped{};
		if (!MapProjectedExprSchema(type_oid, typmod, result_scale, result_slot, mapped))
		{
			if (pg_yaap_trace_hooks)
				elog(LOG, "pg_yaap: optimizer projection rejected: schema mapping failed type=%u slot=%u scale=%d",
					 type_oid, result_slot, result_scale);
			return false;
		}
		out_schema.push_back(mapped);
		expr_descs.push_back(ProjectExprDesc{
			first_step_idx,
			static_cast<uint16_t>(steps.size() - first_step_idx),
			result_slot,
			result_scale,
			0});
	}

	dsa_pointer input_schema_dp = BuildSchemaDescriptorFromColumns(child.schema, state->runtime_dsa);
	dsa_pointer output_schema_dp = BuildSchemaDescriptorFromColumns(out_schema, state->runtime_dsa);
	dsa_pointer expr_descs_dp = BuildFilterArray(state->runtime_dsa, expr_descs.data(), sizeof(ProjectExprDesc), expr_descs.size());
	dsa_pointer steps_dp = BuildFilterArray(state->runtime_dsa, steps.data(), sizeof(ProjectStep), steps.size());
	if (!DsaPointerIsValid(input_schema_dp) || !DsaPointerIsValid(output_schema_dp))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer projection rejected: schema DSA publish failed");
		return false;
	}

	PgVector<ProjectExprDesc> expr_descs_vec;
	expr_descs_vec.assign(expr_descs.begin(), expr_descs.end());
	PgVector<ProjectStep> steps_vec;
	steps_vec.assign(steps.begin(), steps.end());

	auto project_op = std::make_unique<PipelineProjection>(
		input_schema_dp,
		output_schema_dp,
		std::move(expr_descs_vec),
		std::move(steps_vec),
		expr_descs_dp,
		steps_dp,
		nullptr);
	project_op->AddChild(std::move(child.op));

	out.op = std::move(project_op);
	out.schema = std::move(out_schema);
	out.cols.clear();
	for (size_t i = 0; i < projection.select_list.size(); ++i)
		out.cols.push_back(ColumnRef{1, static_cast<AttrNumber>(i + 1)});
	out.final_sort_keys = std::move(child.final_sort_keys);
	out.limit_count = child.limit_count;
	out.estimated_groups = child.estimated_groups;
	return true;
}

static bool
TranslateTableScanNode(const PhysicalTableScan &scan,
					   PgYaapQueryState *state,
					   const std::vector<Expression *> &pending_filters,
					   const std::vector<ColumnRef> *required_output_cols,
					   OptimizerNodeTranslation &out)
{
	if (state == nullptr || state->runtime_dsa == nullptr)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer scan rejected: missing state/runtime_dsa");
		return false;
	}

	std::vector<ColumnRef> all_cols;
	if (!BuildAllTableColumnRefs(scan.relid, static_cast<Index>(scan.table_index.index + 1), all_cols))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer scan rejected: failed to enumerate table columns relid=%u", scan.relid);
		return false;
	}
	std::vector<ColumnSchema> ordered_cols;
	if (!BuildOrderedSeqScanColumns(scan.relid, all_cols, static_cast<Index>(scan.table_index.index + 1), ordered_cols))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer scan rejected: BuildOrderedSeqScanColumns failed relid=%u", scan.relid);
		return false;
	}

	std::vector<Expression *> filters = scan.filters;
	filters.insert(filters.end(), pending_filters.begin(), pending_filters.end());

	std::vector<FilterInputDesc> filter_inputs;
	std::vector<FilterExprDesc> filter_exprs;
	std::vector<FilterStep> filter_steps;
	std::vector<char> filter_string_consts;
	if (!LowerScanFilters(filters, all_cols, ordered_cols, filter_inputs, filter_exprs, filter_steps, filter_string_consts))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer scan rejected: filter lowering failed n_filters=%zu", filters.size());
		return false;
	}

	dsa_pointer output_schema_dp = BuildSchemaDescriptorFromColumns(ordered_cols, state->runtime_dsa);
	dsa_pointer filter_inputs_dp = BuildFilterArray(state->runtime_dsa, filter_inputs.data(), sizeof(FilterInputDesc), filter_inputs.size());
	dsa_pointer filter_exprs_dp = BuildFilterArray(state->runtime_dsa, filter_exprs.data(), sizeof(FilterExprDesc), filter_exprs.size());
	dsa_pointer filter_steps_dp = BuildFilterArray(state->runtime_dsa, filter_steps.data(), sizeof(FilterStep), filter_steps.size());
	dsa_pointer filter_string_consts_dp = BuildCharArray(state->runtime_dsa, filter_string_consts);
	if (!DsaPointerIsValid(output_schema_dp))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer scan rejected: output schema publish failed");
		return false;
	}

	out.op = std::make_unique<PipelineSeqScan>(
		scan.relid,
		InvalidDsaPointer,
		output_schema_dp,
		filter_inputs_dp,
		filter_exprs_dp,
		filter_steps_dp,
		filter_string_consts_dp,
		static_cast<uint16_t>(filter_inputs.size()),
		static_cast<uint16_t>(filter_exprs.size()),
		static_cast<uint16_t>(filter_steps.size()),
		static_cast<uint16_t>(std::min<size_t>(pg_yaap::pipeline::FILTER_MAX_BOOL_REGS, filter_exprs.size() ? filter_steps.back().out_bool_reg + 1 : 0)),
		static_cast<uint32_t>(filter_string_consts.size()),
		InvalidDsaPointer);
	out.cols = std::move(all_cols);
	out.schema = std::move(ordered_cols);
	out.final_sort_keys.clear();
	out.limit_count = 0;
	out.estimated_groups = 0;
	return true;
}

static bool
TranslateHashAggregateNode(const PhysicalHashAggregate &agg,
						   QueryDesc *queryDesc,
						   PgYaapQueryState *state,
						   OptimizerNodeTranslation &out)
{
	if (agg.children.size() != 1 || agg.children[0] == nullptr || state == nullptr || state->runtime_dsa == nullptr)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer agg rejected: invalid child/state");
		return false;
	}
	OptimizerNodeTranslation child;
	std::vector<ColumnRef> child_required;
	for (Expression *group_expr : agg.groups)
		CollectReferencedColumns(group_expr, child_required);
	for (Expression *agg_expr : agg.expressions)
		CollectReferencedColumns(agg_expr, child_required);
	if (!TranslateOptimizerNode(*agg.children[0], queryDesc, state, {}, &child_required, child) || child.op == nullptr)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer agg rejected: child translation failed");
		return false;
	}

	AggBuildState agg_state;
	TupleDataLayoutInit(&agg_state.hash_layout);
	uint8_t next_int64_slot = NextFreeInt64Slot(child.schema);
	for (Expression *expr : agg.expressions)
	{
		const auto *bound_agg = dynamic_cast<const BoundAggregateExpression *>(expr);
		if (bound_agg == nullptr)
			return false;
		AggFuncDesc desc{};
		TdcAggKind kind{};
		int16_t numeric_scale = 0;
		if (!ClassifyOptimizerAggregate(bound_agg,
										child.cols,
										child.schema,
										agg_state.project_steps,
										agg_state.project_exprs,
										agg_state.materialized_exprs,
										next_int64_slot,
										desc,
										kind,
										numeric_scale))
		{
			if (pg_yaap_trace_hooks)
				elog(LOG, "pg_yaap: optimizer agg rejected: aggregate classify failed fn=%s",
					 bound_agg != nullptr ? bound_agg->function_name.c_str() : "<null>");
			return false;
		}
		agg_state.agg_funcs.push_back(desc);
		agg_state.agg_kinds.push_back(kind);
		agg_state.agg_numeric_scales.push_back(numeric_scale);
	}

	std::vector<ColumnRef> group_cols;
	group_cols.reserve(agg.groups.size());
	for (Expression *group_expr : agg.groups)
	{
		const auto *group_col = dynamic_cast<const BoundColumnRefExpression *>(group_expr);
		if (group_col == nullptr)
		{
			if (pg_yaap_trace_hooks)
				elog(LOG, "pg_yaap: optimizer agg rejected: non-column group expr");
			return false;
		}
		group_cols.push_back(BindingToColumnRef(group_col->binding));
	}

	if (!BuildHashGroupLayout(group_cols,
							  child.cols,
							  child.schema,
							  agg_state.agg_funcs,
							  agg_state.agg_kinds,
							  agg_state.agg_numeric_scales,
							  agg_state.hash_layout))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer agg rejected: BuildHashGroupLayout failed groups=%zu aggs=%zu",
				 group_cols.size(), agg_state.agg_funcs.size());
		return false;
	}
	(void) pg_yaap::pipeline::translator_detail::TryBuildPerfectHashSpec(group_cols, child.cols, child.schema, agg_state.perfect_hash_capacity);

	std::unique_ptr<PipelineOperator> agg_child = std::move(child.op);
	if (!agg_state.project_exprs.empty())
	{
		PgVector<ProjectExprDesc> expr_descs_vec;
		expr_descs_vec.assign(agg_state.project_exprs.begin(), agg_state.project_exprs.end());
		PgVector<ProjectStep> steps_vec;
		steps_vec.assign(agg_state.project_steps.begin(), agg_state.project_steps.end());
		dsa_pointer input_schema_dp = BuildSchemaDescriptorFromColumns(child.schema, state->runtime_dsa);
		auto project_op = std::make_unique<PipelineProjection>(
			input_schema_dp,
			InvalidDsaPointer,
			std::move(expr_descs_vec),
			std::move(steps_vec));
		project_op->AddChild(std::move(agg_child));
		agg_child = std::move(project_op);
	}

	dsa_pointer hash_layout_dp = SerializeTupleDataLayout(agg_state.hash_layout, state->runtime_dsa);
	if (!DsaPointerIsValid(hash_layout_dp))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer agg rejected: hash layout publish failed");
		return false;
	}
	PgVector<uint16_t> group_keys;
	for (const ColumnRef &ref : group_cols)
	{
		const ColumnSchema *col = nullptr;
		if (!LookupRawColumn(ref, child.cols, child.schema, col) || col == nullptr)
		{
			if (pg_yaap_trace_hooks)
				elog(LOG, "pg_yaap: optimizer agg rejected: missing group key column");
			return false;
		}
		group_keys.push_back(col->chunk_slot);
	}
	PgVector<AggFuncDesc> agg_funcs_vec;
	agg_funcs_vec.assign(agg_state.agg_funcs.begin(), agg_state.agg_funcs.end());
	std::unique_ptr<PipelineOperator> hash_op;
	if (agg_state.perfect_hash_capacity > 0)
	{
		hash_op = std::make_unique<PipelinePerfectHashAggregate>(
			hash_layout_dp,
			std::move(group_keys),
			std::move(agg_funcs_vec),
			InvalidDsaPointer,
			std::max<uint32_t>(1024u, static_cast<uint32_t>(agg.estimated_cardinality)),
			agg_state.perfect_hash_capacity);
	}
	else
	{
		hash_op = std::make_unique<PipelineHashAggregate>(
			hash_layout_dp,
			std::move(group_keys),
			std::move(agg_funcs_vec),
			InvalidDsaPointer,
			std::max<uint32_t>(1024u, static_cast<uint32_t>(agg.estimated_cardinality)),
			0);
	}
	hash_op->AddChild(std::move(agg_child));

	out.op = std::move(hash_op);
	out.final_sort_keys = std::move(child.final_sort_keys);
	out.limit_count = child.limit_count;
	out.estimated_groups = static_cast<uint32_t>(std::max<size_t>(1, agg.estimated_cardinality));
	return BuildOptimizerAggOutput(agg, child.cols, child.schema, agg_state, out.cols, out.schema);
}

static bool
TranslateHashJoinNode(const PhysicalHashJoin &join,
					  QueryDesc *queryDesc,
					  PgYaapQueryState *state,
					  const std::vector<Expression *> &pending_filters,
					  const std::vector<ColumnRef> *required_output_cols,
					  OptimizerNodeTranslation &out)
{
	if (join.children.size() != 2 || join.children[0] == nullptr || join.children[1] == nullptr || state == nullptr || state->runtime_dsa == nullptr)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer hash join rejected: invalid children/state");
		return false;
	}
	if (join.join_type != yaap::JOIN_INNER)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer hash join rejected: join_type=%d", join.join_type);
		return false;
	}

	OptimizerNodeTranslation left_child;
	OptimizerNodeTranslation right_child;
	std::vector<ColumnRef> child_required;
	if (required_output_cols != nullptr)
		child_required = *required_output_cols;
	for (Expression *expr : join.conditions)
		CollectReferencedColumns(expr, child_required);
	for (Expression *expr : pending_filters)
		CollectReferencedColumns(expr, child_required);
	if (!TranslateOptimizerNode(*join.children[0], queryDesc, state, {}, &child_required, left_child) ||
		!TranslateOptimizerNode(*join.children[1], queryDesc, state, {}, &child_required, right_child) ||
		left_child.op == nullptr || right_child.op == nullptr)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer hash join rejected: child translation failed");
		return false;
	}

	std::vector<ColumnRef> left_keys;
	std::vector<ColumnRef> right_keys;
	std::vector<Expression *> residuals = pending_filters;
	for (Expression *expr : join.conditions)
	{
		if (!CollectJoinKeys(expr, left_child.cols, left_child.schema, right_child.cols, right_child.schema, left_keys, right_keys, residuals))
		{
			if (pg_yaap_trace_hooks)
				elog(LOG, "pg_yaap: optimizer hash join rejected: join key extraction failed");
			return false;
		}
	}
	if (left_keys.empty() || left_keys.size() != right_keys.size())
	{
		if (pg_yaap_trace_hooks)
			elog(LOG,
				 "pg_yaap: optimizer hash join rejected: no usable equi-join keys (left=%zu right=%zu residuals=%zu)",
				 left_keys.size(),
				 right_keys.size(),
				 residuals.size());
		return false;
	}

	const bool swap_sides = left_child.schema.size() < right_child.schema.size();
	const auto &probe_cols = swap_sides ? right_child.cols : left_child.cols;
	const auto &probe_schema = swap_sides ? right_child.schema : left_child.schema;
	const auto &build_cols = swap_sides ? left_child.cols : right_child.cols;
	const auto &build_schema = swap_sides ? left_child.schema : right_child.schema;
	const auto &probe_keys = swap_sides ? right_keys : left_keys;
	const auto &build_keys = swap_sides ? left_keys : right_keys;

	std::vector<ColumnRef> raw_output_cols = probe_cols;
	raw_output_cols.insert(raw_output_cols.end(), build_cols.begin(), build_cols.end());
	std::vector<ColumnRef> requested_output_cols;
	FilterRequestedColumns(raw_output_cols, required_output_cols, requested_output_cols);
	if (requested_output_cols.empty())
		requested_output_cols = raw_output_cols;
	std::vector<HashJoinOutputColumnDesc> output_mappings;
	std::vector<ColumnSchema> output_schema;
	TupleDataLayout probe_key_layout;
	TupleDataLayout build_key_layout;
	TupleDataLayout probe_payload_layout;
	TupleDataLayout build_payload_layout;
	if (!BuildColumnOnlyLayoutForRefs(probe_keys, probe_cols, probe_schema, probe_key_layout) ||
		!BuildColumnOnlyLayoutForRefs(build_keys, build_cols, build_schema, build_key_layout) ||
		!BuildColumnOnlyLayout(probe_schema, probe_payload_layout) ||
		!BuildColumnOnlyLayout(build_schema, build_payload_layout) ||
		!BuildHashJoinOutputMappings(requested_output_cols, probe_cols, probe_schema, build_cols, build_schema, output_mappings, output_schema))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer hash join rejected: join layout/output mapping build failed");
		return false;
	}

	std::vector<HashJoinFilterInputDesc> filter_inputs;
	std::vector<FilterExprDesc> filter_exprs;
	std::vector<FilterStep> filter_steps;
	std::vector<char> filter_string_consts;
	uint16_t filter_bool_regs = 0;
	if (!LowerJoinFilters(residuals,
						  probe_cols, probe_schema,
						  build_cols, build_schema,
						  filter_inputs, filter_exprs, filter_steps, filter_string_consts, filter_bool_regs))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG,
				 "pg_yaap: optimizer hash join rejected: residual join filter lowering failed (%zu residuals)",
				 residuals.size());
		return false;
	}

	dsa_pointer left_schema_dp = BuildSchemaDescriptorFromColumns(probe_schema, state->runtime_dsa);
	dsa_pointer right_schema_dp = BuildSchemaDescriptorFromColumns(build_schema, state->runtime_dsa);
	dsa_pointer output_schema_dp = BuildSchemaDescriptorFromColumns(output_schema, state->runtime_dsa);
	dsa_pointer left_key_layout_dp = SerializeTupleDataLayout(probe_key_layout, state->runtime_dsa);
	dsa_pointer right_key_layout_dp = SerializeTupleDataLayout(build_key_layout, state->runtime_dsa);
	dsa_pointer left_payload_layout_dp = SerializeTupleDataLayout(probe_payload_layout, state->runtime_dsa);
	dsa_pointer right_payload_layout_dp = SerializeTupleDataLayout(build_payload_layout, state->runtime_dsa);
	dsa_pointer output_columns_dp = BuildFilterArray(state->runtime_dsa, output_mappings.data(), sizeof(HashJoinOutputColumnDesc), output_mappings.size());
	dsa_pointer filter_inputs_dp = BuildFilterArray(state->runtime_dsa, filter_inputs.data(), sizeof(HashJoinFilterInputDesc), filter_inputs.size());
	dsa_pointer filter_exprs_dp = BuildFilterArray(state->runtime_dsa, filter_exprs.data(), sizeof(FilterExprDesc), filter_exprs.size());
	dsa_pointer filter_steps_dp = BuildFilterArray(state->runtime_dsa, filter_steps.data(), sizeof(FilterStep), filter_steps.size());
	dsa_pointer filter_string_consts_dp = BuildCharArray(state->runtime_dsa, filter_string_consts);
	if (!DsaPointerIsValid(left_schema_dp) || !DsaPointerIsValid(right_schema_dp) ||
		!DsaPointerIsValid(output_schema_dp) || !DsaPointerIsValid(left_key_layout_dp) ||
		!DsaPointerIsValid(right_key_layout_dp) || !DsaPointerIsValid(left_payload_layout_dp) ||
		!DsaPointerIsValid(right_payload_layout_dp) || !DsaPointerIsValid(output_columns_dp))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer hash join rejected: DSA publish failed");
		return false;
	}

	auto join_op = std::make_unique<pg_yaap::pipeline::PhysicalHashJoin>(
		left_schema_dp,
		right_schema_dp,
		output_schema_dp,
		left_key_layout_dp,
		right_key_layout_dp,
		left_payload_layout_dp,
		right_payload_layout_dp,
		output_columns_dp,
		static_cast<uint16_t>(output_mappings.size()),
		filter_inputs_dp,
		filter_exprs_dp,
		filter_steps_dp,
		filter_string_consts_dp,
		static_cast<uint16_t>(filter_inputs.size()),
		static_cast<uint16_t>(filter_exprs.size()),
		static_cast<uint16_t>(filter_steps.size()),
		filter_bool_regs,
		static_cast<uint32_t>(filter_string_consts.size()),
		InvalidDsaPointer,
		static_cast<uint16_t>(probe_keys.size()),
		static_cast<uint16_t>(build_keys.size()),
		EstimateHashJoinBuildRows(swap_sides ? left_child.schema.size() : right_child.schema.size()));
	if (swap_sides)
	{
		join_op->AddChild(std::move(right_child.op));
		join_op->AddChild(std::move(left_child.op));
	}
	else
	{
		join_op->AddChild(std::move(left_child.op));
		join_op->AddChild(std::move(right_child.op));
	}

	out.op = std::move(join_op);
	out.cols = std::move(requested_output_cols);
	out.schema = std::move(output_schema);
	out.final_sort_keys.clear();
	out.limit_count = 0;
	out.estimated_groups = 0;
	return true;
}

static bool
TranslateCrossProductNode(const PhysicalCrossProduct &join,
						 QueryDesc *queryDesc,
						 PgYaapQueryState *state,
						 const std::vector<Expression *> &pending_filters,
						 const std::vector<ColumnRef> *required_output_cols,
						 OptimizerNodeTranslation &out)
{
	if (join.children.size() != 2 || join.children[0] == nullptr || join.children[1] == nullptr || state == nullptr || state->runtime_dsa == nullptr)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer cross product rejected: invalid children/state");
		return false;
	}
	if (pending_filters.empty())
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer cross product rejected: pure cartesian product is unsupported");
		return false;
	}

	OptimizerNodeTranslation left_child;
	OptimizerNodeTranslation right_child;
	std::vector<ColumnRef> child_required;
	if (required_output_cols != nullptr)
		child_required = *required_output_cols;
	for (Expression *expr : pending_filters)
		CollectReferencedColumns(expr, child_required);
	if (!TranslateOptimizerNode(*join.children[0], queryDesc, state, {}, &child_required, left_child) ||
		!TranslateOptimizerNode(*join.children[1], queryDesc, state, {}, &child_required, right_child) ||
		left_child.op == nullptr || right_child.op == nullptr)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer cross product rejected: child translation failed");
		return false;
	}

	std::vector<ColumnRef> left_keys;
	std::vector<ColumnRef> right_keys;
	std::vector<Expression *> residuals;
	for (Expression *expr : pending_filters)
	{
		if (!CollectJoinKeys(expr, left_child.cols, left_child.schema, right_child.cols, right_child.schema, left_keys, right_keys, residuals))
		{
			if (pg_yaap_trace_hooks)
				elog(LOG, "pg_yaap: optimizer cross product rejected: join key extraction failed");
			return false;
		}
	}
	if (left_keys.empty() || left_keys.size() != right_keys.size())
	{
		if (pg_yaap_trace_hooks)
			elog(LOG,
				 "pg_yaap: optimizer cross product rejected: no usable equi-join keys (left=%zu right=%zu residuals=%zu)",
				 left_keys.size(),
				 right_keys.size(),
				 residuals.size());
		return false;
	}

	const bool swap_sides = left_child.schema.size() < right_child.schema.size();
	const auto &probe_cols = swap_sides ? right_child.cols : left_child.cols;
	const auto &probe_schema = swap_sides ? right_child.schema : left_child.schema;
	const auto &build_cols = swap_sides ? left_child.cols : right_child.cols;
	const auto &build_schema = swap_sides ? left_child.schema : right_child.schema;
	const auto &probe_keys = swap_sides ? right_keys : left_keys;
	const auto &build_keys = swap_sides ? left_keys : right_keys;

	std::vector<ColumnRef> raw_output_cols = probe_cols;
	raw_output_cols.insert(raw_output_cols.end(), build_cols.begin(), build_cols.end());
	std::vector<ColumnRef> requested_output_cols;
	FilterRequestedColumns(raw_output_cols, required_output_cols, requested_output_cols);
	if (requested_output_cols.empty())
		requested_output_cols = raw_output_cols;
	std::vector<HashJoinOutputColumnDesc> output_mappings;
	std::vector<ColumnSchema> output_schema;
	TupleDataLayout probe_key_layout;
	TupleDataLayout build_key_layout;
	TupleDataLayout probe_payload_layout;
	TupleDataLayout build_payload_layout;
	if (!BuildColumnOnlyLayoutForRefs(probe_keys, probe_cols, probe_schema, probe_key_layout) ||
		!BuildColumnOnlyLayoutForRefs(build_keys, build_cols, build_schema, build_key_layout) ||
		!BuildColumnOnlyLayout(probe_schema, probe_payload_layout) ||
		!BuildColumnOnlyLayout(build_schema, build_payload_layout) ||
		!BuildHashJoinOutputMappings(requested_output_cols, probe_cols, probe_schema, build_cols, build_schema, output_mappings, output_schema))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer cross product rejected: join layout/output mapping build failed");
		return false;
	}

	std::vector<HashJoinFilterInputDesc> filter_inputs;
	std::vector<FilterExprDesc> filter_exprs;
	std::vector<FilterStep> filter_steps;
	std::vector<char> filter_string_consts;
	uint16_t filter_bool_regs = 0;
	if (!LowerJoinFilters(residuals,
						  probe_cols, probe_schema,
						  build_cols, build_schema,
						  filter_inputs, filter_exprs, filter_steps, filter_string_consts, filter_bool_regs))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG,
				 "pg_yaap: optimizer cross product rejected: residual join filter lowering failed (%zu residuals)",
				 residuals.size());
		return false;
	}

	dsa_pointer left_schema_dp = BuildSchemaDescriptorFromColumns(probe_schema, state->runtime_dsa);
	dsa_pointer right_schema_dp = BuildSchemaDescriptorFromColumns(build_schema, state->runtime_dsa);
	dsa_pointer output_schema_dp = BuildSchemaDescriptorFromColumns(output_schema, state->runtime_dsa);
	dsa_pointer left_key_layout_dp = SerializeTupleDataLayout(probe_key_layout, state->runtime_dsa);
	dsa_pointer right_key_layout_dp = SerializeTupleDataLayout(build_key_layout, state->runtime_dsa);
	dsa_pointer left_payload_layout_dp = SerializeTupleDataLayout(probe_payload_layout, state->runtime_dsa);
	dsa_pointer right_payload_layout_dp = SerializeTupleDataLayout(build_payload_layout, state->runtime_dsa);
	dsa_pointer output_columns_dp = BuildFilterArray(state->runtime_dsa, output_mappings.data(), sizeof(HashJoinOutputColumnDesc), output_mappings.size());
	dsa_pointer filter_inputs_dp = BuildFilterArray(state->runtime_dsa, filter_inputs.data(), sizeof(HashJoinFilterInputDesc), filter_inputs.size());
	dsa_pointer filter_exprs_dp = BuildFilterArray(state->runtime_dsa, filter_exprs.data(), sizeof(FilterExprDesc), filter_exprs.size());
	dsa_pointer filter_steps_dp = BuildFilterArray(state->runtime_dsa, filter_steps.data(), sizeof(FilterStep), filter_steps.size());
	dsa_pointer filter_string_consts_dp = BuildCharArray(state->runtime_dsa, filter_string_consts);
	if (!DsaPointerIsValid(left_schema_dp) || !DsaPointerIsValid(right_schema_dp) ||
		!DsaPointerIsValid(output_schema_dp) || !DsaPointerIsValid(left_key_layout_dp) ||
		!DsaPointerIsValid(right_key_layout_dp) || !DsaPointerIsValid(left_payload_layout_dp) ||
		!DsaPointerIsValid(right_payload_layout_dp) || !DsaPointerIsValid(output_columns_dp))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer cross product rejected: DSA publish failed");
		return false;
	}

	auto join_op = std::make_unique<pg_yaap::pipeline::PhysicalHashJoin>(
		left_schema_dp,
		right_schema_dp,
		output_schema_dp,
		left_key_layout_dp,
		right_key_layout_dp,
		left_payload_layout_dp,
		right_payload_layout_dp,
		output_columns_dp,
		static_cast<uint16_t>(output_mappings.size()),
		filter_inputs_dp,
		filter_exprs_dp,
		filter_steps_dp,
		filter_string_consts_dp,
		static_cast<uint16_t>(filter_inputs.size()),
		static_cast<uint16_t>(filter_exprs.size()),
		static_cast<uint16_t>(filter_steps.size()),
		filter_bool_regs,
		static_cast<uint32_t>(filter_string_consts.size()),
		InvalidDsaPointer,
		static_cast<uint16_t>(probe_keys.size()),
		static_cast<uint16_t>(build_keys.size()),
		EstimateHashJoinBuildRows(swap_sides ? left_child.schema.size() : right_child.schema.size()));
	if (swap_sides)
	{
		join_op->AddChild(std::move(right_child.op));
		join_op->AddChild(std::move(left_child.op));
	}
	else
	{
		join_op->AddChild(std::move(left_child.op));
		join_op->AddChild(std::move(right_child.op));
	}

	out.op = std::move(join_op);
	out.cols = std::move(requested_output_cols);
	out.schema = std::move(output_schema);
	out.final_sort_keys.clear();
	out.limit_count = 0;
	out.estimated_groups = 0;
	return true;
}

static bool
BuildFinalSortKeys(const PhysicalOrderBy &order,
				   QueryDesc *queryDesc,
				   const std::vector<ColumnRef> &cols,
				   const std::vector<ColumnSchema> &schema,
				   std::vector<SortKeyDesc> &out)
{
	out.clear();
	const std::vector<bool> directions = ParseOrderDirections(queryDesc != nullptr ? queryDesc->sourceText : nullptr, order.orders.size());
	for (size_t i = 0; i < order.orders.size(); ++i)
	{
		const auto *col_expr = dynamic_cast<const BoundColumnRefExpression *>(order.orders[i]);
		uint16_t output_col_idx = UINT16_MAX;
		if (col_expr == nullptr)
		{
			if (pg_yaap_trace_hooks)
				elog(LOG, "pg_yaap: optimizer order rejected: order expr %zu is not a column ref", i);
			return false;
		}
		ColumnRef ref = BindingToColumnRef(col_expr->binding);
		const ColumnSchema *col = nullptr;
		for (size_t col_idx = 0; col_idx < cols.size() && col_idx < schema.size(); ++col_idx)
		{
			if (cols[col_idx] == ref)
			{
				col = &schema[col_idx];
				output_col_idx = static_cast<uint16_t>(col_idx);
				break;
			}
		}
		if ((col == nullptr || output_col_idx == UINT16_MAX) &&
			col_expr->binding.column_index.index < schema.size())
		{
			col = &schema[col_expr->binding.column_index.index];
			output_col_idx = static_cast<uint16_t>(col_expr->binding.column_index.index);
		}
		if (col == nullptr || output_col_idx == UINT16_MAX)
		{
			if (pg_yaap_trace_hooks)
				elog(LOG,
					 "pg_yaap: optimizer order rejected: order expr %zu binding=(%zu,%zu) not found in output cols=%zu schema=%zu",
					 i,
					 col_expr->binding.table_index.index,
					 col_expr->binding.column_index.index,
					 cols.size(),
					 schema.size());
			return false;
		}
		out.push_back(SortKeyDesc{InvalidOid, output_col_idx, directions[i], false, 0});
	}
	return true;
}

static bool
TranslateOptimizerNode(const PhysicalOperator &op,
					   QueryDesc *queryDesc,
					   PgYaapQueryState *state,
					   const std::vector<Expression *> &pending_filters,
					   const std::vector<ColumnRef> *required_output_cols,
					   OptimizerNodeTranslation &out)
{
	switch (op.type)
	{
		case PhysicalOperatorType::TABLE_SCAN:
			return TranslateTableScanNode(static_cast<const PhysicalTableScan &>(op), state, pending_filters, required_output_cols, out);

		case PhysicalOperatorType::FILTER:
		{
			const auto &filter = static_cast<const PhysicalFilter &>(op);
			if (filter.children.size() != 1 || filter.children[0] == nullptr)
				return false;
			std::vector<Expression *> next_filters = pending_filters;
			next_filters.insert(next_filters.end(), filter.expressions.begin(), filter.expressions.end());
			return TranslateOptimizerNode(*filter.children[0], queryDesc, state, next_filters, required_output_cols, out);
		}

		case PhysicalOperatorType::PROJECTION:
			if (!pending_filters.empty())
				return false;
			return TranslateProjectionNode(static_cast<const PhysicalProjection &>(op), queryDesc, state, out);

		case PhysicalOperatorType::HASH_GROUP_BY:
			if (!pending_filters.empty())
				return false;
			return TranslateHashAggregateNode(static_cast<const PhysicalHashAggregate &>(op), queryDesc, state, out);

		case PhysicalOperatorType::HASH_JOIN:
			return TranslateHashJoinNode(static_cast<const PhysicalHashJoin &>(op), queryDesc, state, pending_filters, required_output_cols, out);

		case PhysicalOperatorType::CROSS_PRODUCT:
			return TranslateCrossProductNode(static_cast<const PhysicalCrossProduct &>(op), queryDesc, state, pending_filters, required_output_cols, out);

		case PhysicalOperatorType::ORDER_BY:
		{
			if (!pending_filters.empty())
				return false;
			const auto &order = static_cast<const PhysicalOrderBy &>(op);
			if (order.children.size() != 1 || order.children[0] == nullptr)
				return false;
			if (!TranslateOptimizerNode(*order.children[0], queryDesc, state, {}, required_output_cols, out))
			{
				if (pg_yaap_trace_hooks)
					elog(LOG, "pg_yaap: optimizer order rejected: child translation failed");
				return false;
			}
			return BuildFinalSortKeys(order, queryDesc, out.cols, out.schema, out.final_sort_keys);
		}

		case PhysicalOperatorType::LIMIT:
		{
			if (!pending_filters.empty())
				return false;
			const auto &limit = static_cast<const PhysicalLimit &>(op);
			if (limit.children.size() != 1 || limit.children[0] == nullptr)
				return false;
			if (!TryParseLimitExpression(limit.limit_count, out.limit_count))
				return false;
			OptimizerNodeTranslation child;
			if (!TranslateOptimizerNode(*limit.children[0], queryDesc, state, {}, required_output_cols, child))
				return false;
			out = std::move(child);
			out.limit_count = out.limit_count == 0 ? 0 : out.limit_count;
			return true;
		}

		default:
			return false;
	}
}

static bool
BuildAllProjectionColumnRefs(const PhysicalProjection &projection,
							 const PhysicalTableScan &scan,
							 std::vector<ColumnRef> &out_cols)
{
	out_cols.clear();
	for (Expression *expr : projection.select_list)
	{
		const auto *colref = dynamic_cast<const BoundColumnRefExpression *>(expr);
		if (colref == nullptr || colref->binding.table_index.index != scan.table_index.index)
			return false;
		out_cols.push_back(BindingToColumnRef(colref->binding));
	}
	return !out_cols.empty();
}

static bool
ExtractScanShape(const PhysicalOperator &op,
				 const PhysicalTableScan *&out_scan,
				 std::vector<ColumnRef> &out_cols)
{
	out_scan = nullptr;
	switch (op.type)
	{
		case PhysicalOperatorType::TABLE_SCAN:
			out_scan = static_cast<const PhysicalTableScan *>(&op);
			return BuildAllTableColumnRefs(out_scan->relid, static_cast<Index>(out_scan->table_index.index + 1), out_cols);
		case PhysicalOperatorType::PROJECTION:
		{
			const auto &projection = static_cast<const PhysicalProjection &>(op);
			if (projection.children.size() != 1 || projection.children[0] == nullptr ||
				projection.children[0]->type != PhysicalOperatorType::TABLE_SCAN)
				return false;
			out_scan = static_cast<const PhysicalTableScan *>(projection.children[0].get());
			return BuildAllProjectionColumnRefs(projection, *out_scan, out_cols);
		}
		default:
			return false;
	}
}

}  // namespace

OptimizerPlanSupportStatus
AnalyzeOptimizerPlanSupport(const OptimizerPlanBundle &bundle)
{
	if (bundle.physical_plan == nullptr)
		return OptimizerPlanSupportStatus{false, "root", "optimizer physical plan is null"};

	SupportContext ctx;
	ctx.stack.push_back(std::string("root(") + OptimizerOpTypeName(bundle.physical_plan->type) + ")");
	return AnalyzeOptimizerPlanNode(*bundle.physical_plan, ctx);
}

std::string
DescribeOptimizerPlan(const OptimizerPlanBundle &bundle)
{
	if (bundle.physical_plan == nullptr)
		return "NULL";
	std::string out;
	AppendOptimizerPlanNode(*bundle.physical_plan, out);
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
	if (!TranslateOptimizerNode(*bundle.physical_plan, queryDesc, state, {}, nullptr, node) || node.op == nullptr)
		return nullptr;

	return BuildOutputContract(node, queryDesc, state);
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
