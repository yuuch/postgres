#include "parallel/pipeline/translator.hpp"

extern "C" {
#include "postgres.h"
#include "access/relation.h"
#include "access/tupdesc.h"
#include "catalog/pg_type_d.h"
#include "executor/execdesc.h"
#include "fmgr.h"
#include "nodes/nodes.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "parser/parsetree.h"
#include "storage/lockdefs.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/rel.h"

extern Datum numeric_mul(PG_FUNCTION_ARGS);
extern Datum numeric_int8(PG_FUNCTION_ARGS);
}

#include <algorithm>
#include <cstring>
#include <vector>

#include "parallel/pipeline/output_sink.hpp"
#include "parallel/pipeline/physical_hash_aggregate.hpp"
#include "parallel/pipeline/physical_order.hpp"
#include "parallel/pipeline/physical_projection.hpp"
#include "parallel/pipeline/physical_seq_scan.hpp"
#include "parallel/pipeline/pipeline_descriptor.hpp"
#include "parallel/pipeline/tuple_data_layout.hpp"

namespace pg_volvec {
namespace pipeline {

namespace {

struct MaterializedProjectExpr {
	Expr   *expr;
	int8_t  scale;
	uint8_t slot;
};

struct Q1ExtractedShape {
	SeqScan                    *scan;
	Agg                        *agg;
	Sort                       *sort;
	Oid                         relid;
	std::vector<AttrNumber>     group_attnos;
	std::vector<Aggref *>       aggrefs;
	QualDescriptor              qual;
	std::vector<SortKeyDesc>    sort_keys;
	std::vector<AttrNumber>     seq_scan_attnos;
	std::vector<ColumnSchema>   seq_scan_columns;
	TupleDataLayout             hash_layout;
	TupleDataLayout             sort_key_layout;
	TupleDataLayout             sort_payload_layout;
	dsa_pointer                 hash_layout_dp = InvalidDsaPointer;
	dsa_pointer                 sort_key_layout_dp = InvalidDsaPointer;
	dsa_pointer                 sort_payload_layout_dp = InvalidDsaPointer;
	std::vector<AggFuncDesc>    agg_funcs;
	std::vector<TdcAggKind>     agg_kinds;
	std::vector<int16_t>        agg_numeric_scales;
	std::vector<ProjectStep>    project_steps;
	std::vector<ProjectExprDesc> project_exprs;
	uint8_t                     next_int32_slot = 0;
	uint8_t                     next_int64_slot = 0;
	uint8_t                     next_double_slot = 0;
};

static Expr *
StripRelabels(Expr *expr)
{
	while (expr != nullptr && nodeTag(expr) == T_RelabelType)
		expr = ((RelabelType *) expr)->arg;
	return expr;
}

static int16_t
ExtractNumericTypmodScale(int32 typmod)
{
	if (typmod < VARHDRSZ)
		return 0;
	return static_cast<int16_t>((((typmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024);
}

static bool
Pow10Int64(int exp, int64_t &out)
{
	if (exp < 0 || exp > 18)
		return false;

	out = 1;
	for (int i = 0; i < exp; ++i)
		out *= 10;
	return true;
}

static bool
RescaleInt64Constant(int64_t value, int8_t from_scale, int8_t to_scale, int64_t &out)
{
	if (to_scale < from_scale)
		return false;

	int64_t factor = 1;
	if (!Pow10Int64(static_cast<int>(to_scale - from_scale), factor))
		return false;
	out = value * factor;
	return true;
}

static bool
LookupRawColumn(AttrNumber attno,
				const std::vector<AttrNumber> &raw_attnos,
				const std::vector<ColumnSchema> &raw_cols,
				const ColumnSchema *&out_col)
{
	for (size_t i = 0; i < raw_attnos.size(); ++i)
	{
		if (raw_attnos[i] == attno)
		{
			out_col = &raw_cols[i];
			return true;
		}
	}
	return false;
}

static bool
ColumnNumericScale(const ColumnSchema &col, int8_t &out)
{
	switch (col.decode_kind)
	{
		case ColumnDecodeKind::INT64_NUMERIC_SCALED:
			out = 2;
			return true;
		case ColumnDecodeKind::INT64_INT8:
		case ColumnDecodeKind::INT32_INT4:
		case ColumnDecodeKind::INT32_CHAR:
		case ColumnDecodeKind::INT32_DATE:
			out = 0;
			return true;
		case ColumnDecodeKind::DOUBLE_FLOAT8:
		case ColumnDecodeKind::NONE:
			return false;
	}
	return false;
}

static bool
ScaleNumericConstDatumToInt64(Const *c, int8_t &out_scale, int64_t &out_value)
{
	if (c == nullptr || c->constisnull || c->consttype != NUMERICOID)
		return false;

	out_scale = static_cast<int8_t>(ExtractNumericTypmodScale(c->consttypmod));
	int64_t factor = 1;
	if (!Pow10Int64(out_scale, factor))
		return false;

	Datum factor_numeric = NumericGetDatum(int64_to_numeric(factor));
	Datum scaled = DirectFunctionCall2(numeric_mul, c->constvalue, factor_numeric);
	out_value = DatumGetInt64(DirectFunctionCall1(numeric_int8, scaled));
	return true;
}

static bool
IsBareVarArg(Expr *arg, AttrNumber &out_attno)
{
	arg = StripRelabels(arg);
	if (arg == nullptr || nodeTag(arg) != T_Var)
		return false;
	out_attno = ((Var *) arg)->varattno;
	return out_attno > 0;
}

static bool LowerExprToStepsInternal(Expr *e,
					 std::vector<ProjectStep> &steps,
					 uint8_t &next_int64_slot,
					 const std::vector<AttrNumber> &raw_attnos,
					 const std::vector<ColumnSchema> &raw_cols,
					 const std::vector<MaterializedProjectExpr> *cache,
					 int8_t &out_result_scale,
					 uint8_t &out_result_slot);

static bool
LookupCachedExpr(Expr *expr,
				 const std::vector<MaterializedProjectExpr> *cache,
				 int8_t &out_scale,
				 uint8_t &out_slot)
{
	if (cache == nullptr)
		return false;

	for (const MaterializedProjectExpr &entry : *cache)
	{
		if (equal(entry.expr, expr))
		{
			out_scale = entry.scale;
			out_slot = entry.slot;
			return true;
		}
	}
	return false;
}

static bool
LowerNumericBinaryExpr(OpExpr *op,
				      Expr *lhs,
				      Expr *rhs,
				      const char *opname,
				      std::vector<ProjectStep> &steps,
				      uint8_t &next_int64_slot,
				      const std::vector<AttrNumber> &raw_attnos,
				      const std::vector<ColumnSchema> &raw_cols,
				      const std::vector<MaterializedProjectExpr> *cache,
				      int8_t &out_result_scale,
				      uint8_t &out_result_slot)
{
	(void) op;
	const bool is_mul = std::strcmp(opname, "*") == 0;
	const bool is_sub = std::strcmp(opname, "-") == 0;
	const bool is_add = std::strcmp(opname, "+") == 0;
	if (!is_mul && !is_sub && !is_add)
		return false;

	lhs = StripRelabels(lhs);
	rhs = StripRelabels(rhs);
	const bool lhs_const = lhs != nullptr && nodeTag(lhs) == T_Const;
	const bool rhs_const = rhs != nullptr && nodeTag(rhs) == T_Const;

	if (is_mul)
	{
		if (lhs_const && rhs_const)
			return false;

		if (lhs_const || rhs_const)
		{
			Expr *var_expr = lhs_const ? rhs : lhs;
			Const *const_expr = (Const *) (lhs_const ? lhs : rhs);
			int8_t var_scale = 0;
			uint8_t var_slot = 0;
			if (!LowerExprToStepsInternal(var_expr, steps, next_int64_slot,
					raw_attnos, raw_cols, cache, var_scale, var_slot))
				return false;

			int8_t const_scale = 0;
			int64_t const_value = 0;
			if (!ScaleNumericConstDatumToInt64(const_expr, const_scale, const_value))
				return false;

			if (next_int64_slot >= 16)
				return false;
			out_result_slot = next_int64_slot++;
			out_result_scale = static_cast<int8_t>(var_scale + const_scale);
			steps.push_back(ProjectStep{ProjectOp::NUMERIC_MUL_VAR_CONST,
				var_slot, 0, out_result_slot, const_value});
			return true;
		}

		int8_t lhs_scale = 0;
		int8_t rhs_scale = 0;
		uint8_t lhs_slot = 0;
		uint8_t rhs_slot = 0;
		if (!LowerExprToStepsInternal(lhs, steps, next_int64_slot,
				raw_attnos, raw_cols, cache, lhs_scale, lhs_slot))
			return false;
		if (!LowerExprToStepsInternal(rhs, steps, next_int64_slot,
				raw_attnos, raw_cols, cache, rhs_scale, rhs_slot))
			return false;

		if (next_int64_slot >= 16)
			return false;
		out_result_slot = next_int64_slot++;
		out_result_scale = static_cast<int8_t>(lhs_scale + rhs_scale);
		steps.push_back(ProjectStep{ProjectOp::NUMERIC_MUL_VAR_VAR,
			lhs_slot, rhs_slot, out_result_slot, 0});
		return true;
	}

	if (!lhs_const || rhs_const)
		return false;

	int8_t rhs_scale = 0;
	uint8_t rhs_slot = 0;
	if (!LowerExprToStepsInternal(rhs, steps, next_int64_slot,
			raw_attnos, raw_cols, cache, rhs_scale, rhs_slot))
		return false;

	int8_t lhs_scale = 0;
	int64_t lhs_value = 0;
	if (!ScaleNumericConstDatumToInt64((Const *) lhs, lhs_scale, lhs_value))
		return false;

	out_result_scale = static_cast<int8_t>(Max(lhs_scale, rhs_scale));
	if (!RescaleInt64Constant(lhs_value, lhs_scale, out_result_scale, lhs_value))
		return false;

	if (next_int64_slot >= 16)
		return false;
	out_result_slot = next_int64_slot++;
	steps.push_back(ProjectStep{is_sub ? ProjectOp::NUMERIC_SUB_CONST_VAR
						 : ProjectOp::NUMERIC_ADD_CONST_VAR,
		0, rhs_slot, out_result_slot, lhs_value});
	return true;
}

static bool
LowerExprToStepsInternal(Expr *e,
				 std::vector<ProjectStep> &steps,
				 uint8_t &next_int64_slot,
				 const std::vector<AttrNumber> &raw_attnos,
				 const std::vector<ColumnSchema> &raw_cols,
				 const std::vector<MaterializedProjectExpr> *cache,
				 int8_t &out_result_scale,
				 uint8_t &out_result_slot)
{
	e = StripRelabels(e);
	if (e == nullptr)
		return false;

	if (LookupCachedExpr(e, cache, out_result_scale, out_result_slot))
		return true;

	AttrNumber attno = InvalidAttrNumber;
	if (IsBareVarArg(e, attno))
	{
		const ColumnSchema *col = nullptr;
		if (!LookupRawColumn(attno, raw_attnos, raw_cols, col))
			return false;
		if (!ColumnNumericScale(*col, out_result_scale))
			return false;
		out_result_slot = col->chunk_slot;
		return true;
	}

	if (nodeTag(e) != T_OpExpr)
		return false;

	OpExpr *op = (OpExpr *) e;
	if (list_length(op->args) != 2)
		return false;

	Expr *lhs = (Expr *) linitial(op->args);
	Expr *rhs = (Expr *) lsecond(op->args);
	char *opname = get_opname(op->opno);
	if (opname == nullptr)
		return false;

	const bool ok = LowerNumericBinaryExpr(op, lhs, rhs, opname,
		steps, next_int64_slot, raw_attnos, raw_cols, cache,
		out_result_scale, out_result_slot);
	pfree(opname);
	return ok;
}

static bool
ColumnDecodeKindToTdc(ColumnDecodeKind dk, TdcColumnKind &out)
{
	switch (dk)
	{
		case ColumnDecodeKind::INT32_CHAR:
		case ColumnDecodeKind::INT32_DATE:
		case ColumnDecodeKind::INT32_INT4:
			out = TdcColumnKind::INT32;
			return true;
		case ColumnDecodeKind::INT64_INT8:
		case ColumnDecodeKind::INT64_NUMERIC_SCALED:
			out = TdcColumnKind::INT64;
			return true;
		case ColumnDecodeKind::DOUBLE_FLOAT8:
			out = TdcColumnKind::DOUBLE;
			return true;
		case ColumnDecodeKind::NONE:
			return false;
	}
	return false;
}

static bool
BuildHashGroupLayout(const std::vector<AttrNumber> &group_attnos,
				     const std::vector<AttrNumber> &seq_scan_attnos,
				     const std::vector<ColumnSchema> &seq_scan_columns,
				     const std::vector<AggFuncDesc> &agg_funcs,
				     const std::vector<TdcAggKind> &agg_kinds,
				     const std::vector<int16_t> &agg_numeric_scales,
				     TupleDataLayout &out)
{
	TupleDataLayoutInit(&out);

	for (AttrNumber group_attno : group_attnos)
	{
		uint16_t src_idx = 0;
		bool found = false;
		for (uint16_t i = 0; i < seq_scan_attnos.size(); ++i)
		{
			if (seq_scan_attnos[i] == group_attno)
			{
				src_idx = i;
				found = true;
				break;
			}
		}
		if (!found)
			return false;

		const ColumnSchema &cs = seq_scan_columns[src_idx];
		TdcColumnKind tdc_kind;
		if (!ColumnDecodeKindToTdc(cs.decode_kind, tdc_kind))
			return false;

		int8_t numeric_scale = 0;
		if (!ColumnNumericScale(cs, numeric_scale))
			return false;

		(void) TupleDataLayoutAppendColumn(&out, tdc_kind, cs.type_oid, numeric_scale);
	}

	if (agg_funcs.size() != agg_kinds.size() || agg_funcs.size() != agg_numeric_scales.size())
		return false;

	for (size_t i = 0; i < agg_funcs.size(); ++i)
	{
		(void) TupleDataLayoutAppendAggregate(&out,
			agg_kinds[i],
			agg_funcs[i].input_col_idx,
			agg_funcs[i].agg_oid,
			agg_numeric_scales[i]);
	}

	TupleDataLayoutSeal(&out);
	return true;
}

static bool
BuildSortLayouts(const std::vector<AttrNumber> &group_attnos,
				 const std::vector<AttrNumber> &seq_scan_attnos,
				 const std::vector<ColumnSchema> &seq_scan_columns,
				 const std::vector<AggFuncDesc> &agg_funcs,
				 const std::vector<TdcAggKind> &agg_kinds,
				 const std::vector<int16_t> &agg_numeric_scales,
				 const std::vector<SortKeyDesc> &sort_keys,
				 TupleDataLayout &out_key,
				 TupleDataLayout &out_payload)
{
	TupleDataLayoutInit(&out_payload);

	for (AttrNumber group_attno : group_attnos)
	{
		uint16_t src_idx = 0;
		bool found = false;
		for (uint16_t i = 0; i < seq_scan_attnos.size(); ++i)
		{
			if (seq_scan_attnos[i] == group_attno)
			{
				src_idx = i;
				found = true;
				break;
			}
		}
		if (!found)
			return false;

		const ColumnSchema &cs = seq_scan_columns[src_idx];
		TdcColumnKind tdc_kind;
		if (!ColumnDecodeKindToTdc(cs.decode_kind, tdc_kind))
			return false;

		int8_t numeric_scale = 0;
		if (!ColumnNumericScale(cs, numeric_scale))
			return false;

		(void) TupleDataLayoutAppendColumn(&out_payload, tdc_kind, cs.type_oid, numeric_scale);
	}

	if (agg_funcs.size() != agg_kinds.size() || agg_funcs.size() != agg_numeric_scales.size())
		return false;

	for (size_t i = 0; i < agg_funcs.size(); ++i)
	{
		(void) TupleDataLayoutAppendAggregate(&out_payload,
			agg_kinds[i],
			agg_funcs[i].input_col_idx,
			agg_funcs[i].agg_oid,
			agg_numeric_scales[i]);
	}

	TupleDataLayoutSeal(&out_payload);

	TupleDataLayoutInit(&out_key);
	for (size_t k = 0; k < sort_keys.size(); ++k)
	{
		if (sort_keys[k].col_idx >= group_attnos.size())
			return false;

		AttrNumber group_attno = group_attnos[sort_keys[k].col_idx];
		uint16_t src_idx = 0;
		bool found = false;
		for (uint16_t i = 0; i < seq_scan_attnos.size(); ++i)
		{
			if (seq_scan_attnos[i] == group_attno)
			{
				src_idx = i;
				found = true;
				break;
			}
		}
		if (!found)
			return false;

		const ColumnSchema &cs = seq_scan_columns[src_idx];
		TdcColumnKind tdc_kind;
		if (!ColumnDecodeKindToTdc(cs.decode_kind, tdc_kind))
			return false;

		int8_t numeric_scale = 0;
		if (!ColumnNumericScale(cs, numeric_scale))
			return false;

		(void) TupleDataLayoutAppendColumn(&out_key, tdc_kind, cs.type_oid, numeric_scale);
	}

	TupleDataLayoutSeal(&out_key);

	for (size_t k = 0; k < sort_keys.size(); ++k)
	{
		const TdcColumnDesc &key_col = out_key.columns[k];
		const TdcColumnDesc &payload_col = out_payload.columns[sort_keys[k].col_idx];
		if (key_col.kind != payload_col.kind ||
			key_col.offset != payload_col.offset ||
			key_col.width != payload_col.width)
		{
			elog(ERROR,
				 "pg_volvec: sort key/payload layout mismatch at key %u (key_off=%u, payload_off=%u, key_width=%u, payload_width=%u, key_kind=%u, payload_kind=%u)",
				 static_cast<unsigned>(k),
				 static_cast<unsigned>(key_col.offset),
				 static_cast<unsigned>(payload_col.offset),
				 static_cast<unsigned>(key_col.width),
				 static_cast<unsigned>(payload_col.width),
				 static_cast<unsigned>(key_col.kind),
				 static_cast<unsigned>(payload_col.kind));
		}
	}

	return true;
}

static bool
CollectVarLeavesFromExpr(Expr *expr, std::vector<AttrNumber> &out)
{
	if (expr == nullptr)
		return false;

	expr = StripRelabels(expr);
	if (expr == nullptr)
		return false;

	switch (nodeTag(expr))
	{
		case T_Var:
			out.push_back(((Var *) expr)->varattno);
			return true;
		case T_Const:
			return true;
		case T_OpExpr:
		{
			OpExpr *op = (OpExpr *) expr;
			ListCell *lc;
			foreach(lc, op->args)
			{
				if (!CollectVarLeavesFromExpr((Expr *) lfirst(lc), out))
					return false;
			}
			return true;
		}
		case T_FuncExpr:
		{
			FuncExpr *fe = (FuncExpr *) expr;
			ListCell *lc;
			foreach(lc, fe->args)
			{
				if (!CollectVarLeavesFromExpr((Expr *) lfirst(lc), out))
					return false;
			}
			return true;
		}
		default:
			return false;
	}
}

static bool
CollectAggrefArgAttnos(const std::vector<Aggref *> &aggrefs,
				   std::vector<AttrNumber> &out)
{
	for (Aggref *ag : aggrefs)
	{
		if (ag->aggorder != NIL || ag->aggdistinct != NIL || ag->aggfilter != nullptr)
			return false;
		if (ag->aggstar)
			continue;

		ListCell *lc;
		foreach(lc, ag->args)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);
			if (tle == nullptr || nodeTag(tle) != T_TargetEntry)
				return false;
			if (!CollectVarLeavesFromExpr(tle->expr, out))
				return false;
		}
	}
	return true;
}

static bool
BuildSeqScanColumns(Oid relid,
			    const std::vector<AttrNumber> &attnos,
			    std::vector<ColumnSchema> &out,
			    uint8_t &next_int32_slot,
			    uint8_t &next_int64_slot,
			    uint8_t &next_double_slot)
{
	Relation  rel = relation_open(relid, AccessShareLock);
	TupleDesc td = RelationGetDescr(rel);
	uint8_t   i32 = 0;
	uint8_t   i64 = 0;
	uint8_t   dbl = 0;

	out.clear();
	out.reserve(attnos.size());

	for (AttrNumber attno : attnos)
	{
		if (attno <= 0 || attno > td->natts)
		{
			relation_close(rel, AccessShareLock);
			return false;
		}
		Form_pg_attribute attr = TupleDescAttr(td, attno - 1);

		ColumnSchema cs{};
		cs.type_oid = attr->atttypid;
		cs.typlen = attr->attlen;
		cs.typbyval = attr->attbyval;
		cs.src_attno = attno;
		cs._pad0 = 0;

		switch (attr->atttypid)
		{
			case BPCHAROID:
			case CHAROID:
				cs.decode_kind = ColumnDecodeKind::INT32_CHAR;
				cs.chunk_slot = i32++;
				break;
			case DATEOID:
				cs.decode_kind = ColumnDecodeKind::INT32_DATE;
				cs.chunk_slot = i32++;
				break;
			case INT4OID:
				cs.decode_kind = ColumnDecodeKind::INT32_INT4;
				cs.chunk_slot = i32++;
				break;
			case INT8OID:
				cs.decode_kind = ColumnDecodeKind::INT64_INT8;
				cs.chunk_slot = i64++;
				break;
			case NUMERICOID:
				cs.decode_kind = ColumnDecodeKind::INT64_NUMERIC_SCALED;
				cs.chunk_slot = i64++;
				break;
			case FLOAT8OID:
				cs.decode_kind = ColumnDecodeKind::DOUBLE_FLOAT8;
				cs.chunk_slot = dbl++;
				break;
			default:
				relation_close(rel, AccessShareLock);
				return false;
		}

		if (cs.chunk_slot >= 16)
		{
			relation_close(rel, AccessShareLock);
			return false;
		}

		out.push_back(cs);
	}

	relation_close(rel, AccessShareLock);
	next_int32_slot = i32;
	next_int64_slot = i64;
	next_double_slot = dbl;
	return true;
}

/* Build the OutputSink input SchemaDescriptor in DSA. The output chunk is
 * Order's emitted PipelineChunk; per tuple_data_ops.cpp:84-122 Gather, the
 * group columns sit at chunk index i = 0..N-1 (storage bucket selected by
 * TdcColumnKind), and aggregate state columns sit at chunk index N+a in
 * int64_columns. So chunk_slot here is the LAYOUT column index, not the
 * per-storage-bucket slot. src_attno=0 (not from SeqScan). */
static dsa_pointer
BuildOutputSchemaDescriptor(const Q1ExtractedShape &shape, dsa_area *dsa)
{
	const uint16_t n_groups = static_cast<uint16_t>(shape.group_attnos.size());
	const uint16_t n_aggs   = static_cast<uint16_t>(shape.agg_kinds.size());
	const uint16_t n_cols   = n_groups + n_aggs;
	if (n_cols == 0 || n_cols > 16)
		return InvalidDsaPointer;

	const Size sz = offsetof(SchemaDescriptor, columns) +
	                static_cast<Size>(n_cols) * sizeof(ColumnSchema);
	dsa_pointer dp = dsa_allocate0(dsa, sz);
	if (!DsaPointerIsValid(dp))
		return InvalidDsaPointer;

	auto *schema = static_cast<SchemaDescriptor *>(dsa_get_address(dsa, dp));
	schema->n_columns = n_cols;
	schema->_pad0 = 0;
	schema->_pad1 = 0;

	/* Group key columns: copy type metadata from the SeqScan ColumnSchema
	 * (looked up by attno) so type_oid/typlen/typbyval/decode_kind match the
	 * physical Datum we produce in EncodeColumn. chunk_slot is the layout
	 * index (i), not the SeqScan per-storage slot. */
	for (uint16_t i = 0; i < n_groups; ++i)
	{
		const AttrNumber attno = shape.group_attnos[i];
		const ColumnSchema *src = nullptr;
		for (size_t k = 0; k < shape.seq_scan_attnos.size(); ++k)
		{
			if (shape.seq_scan_attnos[k] == attno)
			{
				src = &shape.seq_scan_columns[k];
				break;
			}
		}
		if (src == nullptr)
			return InvalidDsaPointer;

		ColumnSchema cs{};
		cs.type_oid    = src->type_oid;
		cs.typlen      = src->typlen;
		cs.typbyval    = src->typbyval;
		cs.chunk_slot  = static_cast<uint8_t>(i);
		cs.src_attno   = 0;
		cs.decode_kind = src->decode_kind;
		cs._pad0       = 0;
		schema->columns[i] = cs;
	}

	/* Aggregate output columns. Q1 finals: COUNT_STAR -> int8, SUM_INT64 ->
	 * int8, SUM_NUMERIC / AVG_NUMERIC -> numeric. AVG_NUMERIC carries a
	 * scaled int64 sum in Gather's chunk slot; EmitGlobalTdcToDest detects
	 * the kind from layout->aggregates[a] and reads count from row_ptr+8. */
	for (uint16_t a = 0; a < n_aggs; ++a)
	{
		const TdcAggKind kind = shape.agg_kinds[a];
		ColumnSchema cs{};
		cs.chunk_slot = static_cast<uint8_t>(n_groups + a);
		cs.src_attno  = 0;
		cs._pad0      = 0;

		switch (kind)
		{
			case TdcAggKind::COUNT_STAR:
			case TdcAggKind::SUM_INT64:
				cs.type_oid    = INT8OID;
				cs.typlen      = 8;
				cs.typbyval    = true;
				cs.decode_kind = ColumnDecodeKind::INT64_INT8;
				break;
			case TdcAggKind::SUM_NUMERIC:
			case TdcAggKind::AVG_NUMERIC:
				cs.type_oid    = NUMERICOID;
				cs.typlen      = -1;
				cs.typbyval    = false;
				cs.decode_kind = ColumnDecodeKind::INT64_NUMERIC_SCALED;
				break;
			default:
				return InvalidDsaPointer;
		}
		schema->columns[n_groups + a] = cs;
	}

	return dp;
}

static bool
MatchQ1Shape(Plan *root, Sort **out_sort, Agg **out_agg, SeqScan **out_scan)
{
	if (root == nullptr || nodeTag(root) != T_Sort)
		return false;
	Sort *sort = (Sort *) root;
	if (sort->numCols < 1 || sort->plan.lefttree == nullptr)
		return false;

	Plan *agg_plan = sort->plan.lefttree;
	if (nodeTag(agg_plan) != T_Agg)
		return false;
	Agg *agg = (Agg *) agg_plan;
	if (agg->aggstrategy != AGG_HASHED)
		return false;
	if (agg->aggsplit != AGGSPLIT_SIMPLE)
		return false;
	if (agg->groupingSets != NIL || agg->chain != NIL)
		return false;
	if (agg->numCols < 1 || agg->plan.lefttree == nullptr)
		return false;

	Plan *scan_plan = agg->plan.lefttree;
	if (nodeTag(scan_plan) != T_SeqScan)
		return false;
	SeqScan *scan = (SeqScan *) scan_plan;
	if (scan->scan.plan.lefttree != nullptr || scan->scan.plan.righttree != nullptr)
		return false;

	*out_sort = sort;
	*out_agg = agg;
	*out_scan = scan;
	return true;
}

static bool
ExtractGroupAttnos(Agg *agg, SeqScan *scan, std::vector<AttrNumber> &out)
{
	List *scan_tlist = scan->scan.plan.targetlist;
	if (scan_tlist == NIL)
		return false;

	out.clear();
	out.reserve(agg->numCols);
	for (int i = 0; i < agg->numCols; ++i)
	{
		AttrNumber resno = agg->grpColIdx[i];
		if (resno < 1 || resno > list_length(scan_tlist))
			return false;
		TargetEntry *tle = (TargetEntry *) list_nth(scan_tlist, resno - 1);
		if (tle == nullptr || tle->expr == nullptr || nodeTag(tle->expr) != T_Var)
			return false;
		Var *v = (Var *) tle->expr;
		if (v->varattno <= 0)
			return false;
		out.push_back(v->varattno);
	}
	return true;
}

static bool
ExtractAggrefs(Agg *agg,
		       std::vector<AttrNumber> const &group_attnos,
		       std::vector<Aggref *> &out)
{
	List *agg_tlist = agg->plan.targetlist;
	if (agg_tlist == NIL)
		return false;

	out.clear();
	ListCell *lc;
	foreach(lc, agg_tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		if (tle == nullptr || tle->expr == nullptr)
			return false;
		NodeTag tag = nodeTag(tle->expr);
		if (tag == T_Var)
		{
			Var *v = (Var *) tle->expr;
			bool is_group = false;
			for (AttrNumber a : group_attnos)
			{
				if (v->varattno == a)
				{
					is_group = true;
					break;
				}
			}
			if (!is_group)
				return false;
		}
		else if (tag == T_Aggref)
		{
			out.push_back((Aggref *) tle->expr);
		}
		else
		{
			return false;
		}
	}
	return true;
}

static bool
MapOpnoToQualOp(Oid opno, QualOp &out)
{
	char *name = get_opname(opno);
	if (name == nullptr)
		return false;
	bool ok = true;
	if (strcmp(name, "<=") == 0)      out = QualOp::LE;
	else if (strcmp(name, "<") == 0)  out = QualOp::LT;
	else if (strcmp(name, "=") == 0)  out = QualOp::EQ;
	else if (strcmp(name, ">=") == 0) out = QualOp::GE;
	else if (strcmp(name, ">") == 0)  out = QualOp::GT;
	else if (strcmp(name, "<>") == 0) out = QualOp::NE;
	else                               ok = false;
	pfree(name);
	return ok;
}

static bool
ExtractQual(SeqScan *scan, QualDescriptor &out)
{
	List *qual = scan->scan.plan.qual;
	if (qual == NIL)
	{
		out.kind = QualKind::NONE;
		out.op = QualOp::EQ;
		out.col_attno = 0;
		out._pad0 = 0;
		out.const_typoid = InvalidOid;
		out.const_value = 0;
		return true;
	}
	if (list_length(qual) != 1)
		return false;

	Node *clause = (Node *) linitial(qual);
	if (clause == nullptr || nodeTag(clause) != T_OpExpr)
		return false;
	OpExpr *op = (OpExpr *) clause;
	if (list_length(op->args) != 2)
		return false;

	Node *left = (Node *) linitial(op->args);
	Node *right = (Node *) lsecond(op->args);
	if (left == nullptr || right == nullptr)
		return false;
	if (nodeTag(left) != T_Var || nodeTag(right) != T_Const)
		return false;

	Var *v = (Var *) left;
	Const *c = (Const *) right;
	if (v->varattno <= 0)
		return false;
	if (c->constisnull || !c->constbyval)
		return false;
	if (c->consttype != DATEOID || v->vartype != DATEOID)
		return false;

	QualOp qop;
	if (!MapOpnoToQualOp(op->opno, qop))
		return false;

	out.kind = QualKind::COL_OP_CONST;
	out.op = qop;
	out.col_attno = (uint16_t) v->varattno;
	out._pad0 = 0;
	out.const_typoid = c->consttype;
	out.const_value = (uint64_t) c->constvalue;
	return true;
}

static bool
ExtractSortKeys(Sort *sort, Agg *agg, std::vector<SortKeyDesc> &out)
{
	List *agg_tlist = agg->plan.targetlist;
	if (agg_tlist == NIL)
		return false;
	int agg_tlist_len = list_length(agg_tlist);

	out.clear();
	out.reserve(sort->numCols);
	for (int i = 0; i < sort->numCols; ++i)
	{
		AttrNumber resno = sort->sortColIdx[i];
		if (resno < 1 || resno > agg_tlist_len)
			return false;
		SortKeyDesc k{};
		k.collation_oid = sort->collations[i];
		k.col_idx = (uint16_t) (resno - 1);
		k.asc = true;
		k.nulls_first = sort->nullsFirst[i];
		k._pad = 0;

		QualOp dummy;
		if (MapOpnoToQualOp(sort->sortOperators[i], dummy))
			k.asc = (dummy == QualOp::LT) || (dummy == QualOp::LE);

		out.push_back(k);
	}
	return true;
}

static bool
ExtractRelid(SeqScan *scan, QueryDesc *qd, Oid &out)
{
	if (qd->plannedstmt == nullptr || qd->plannedstmt->rtable == NIL)
		return false;
	Index rti = scan->scan.scanrelid;
	if (rti < 1 || rti > (Index) list_length(qd->plannedstmt->rtable))
		return false;
	RangeTblEntry *rte = rt_fetch(rti, qd->plannedstmt->rtable);
	if (rte == nullptr || rte->relid == InvalidOid)
		return false;
	out = rte->relid;
	return true;
}

static bool
ClassifyAggref(Aggref *ag,
		       const std::vector<AttrNumber> &raw_attnos,
		       const std::vector<ColumnSchema> &raw_cols,
		       std::vector<ProjectStep> &project_steps,
		       std::vector<ProjectExprDesc> &project_exprs,
		       std::vector<MaterializedProjectExpr> &materialized_exprs,
		       uint8_t &next_int64_slot,
		       AggFuncDesc &out_desc,
		       TdcAggKind &out_kind,
		       int16_t &out_numeric_scale)
{
	if (ag->aggorder != NIL || ag->aggdistinct != NIL || ag->aggfilter != nullptr)
		return false;

	out_desc = AggFuncDesc{};
	out_desc.agg_oid = ag->aggfnoid;
	out_desc.transtype = ag->aggtranstype;
	out_desc.finaltype = ag->aggtype;
	out_desc.input_col_idx = 0;
	out_desc._pad = 0;
	out_numeric_scale = 0;

	if (ag->aggfnoid == F_COUNT_)
	{
		if (!ag->aggstar)
			return false;
		out_kind = TdcAggKind::COUNT_STAR;
		return true;
	}

	if (ag->aggfnoid == F_COUNT_ANY)
		return false;

	if (list_length(ag->args) != 1)
		return false;
	TargetEntry *tle = (TargetEntry *) linitial(ag->args);
	if (tle == nullptr || tle->expr == nullptr)
		return false;
	Expr *arg = (Expr *) tle->expr;

	if (ag->aggfnoid == F_SUM_INT4)
	{
		AttrNumber attno = InvalidAttrNumber;
		if (!IsBareVarArg(arg, attno))
			return false;
		const ColumnSchema *col = nullptr;
		if (!LookupRawColumn(attno, raw_attnos, raw_cols, col))
			return false;
		out_kind = TdcAggKind::SUM_INT64;
		out_desc.input_col_idx = col->chunk_slot;
		return true;
	}

	if (ag->aggfnoid != F_SUM_NUMERIC && ag->aggfnoid != F_AVG_NUMERIC)
		return false;

	out_kind = (ag->aggfnoid == F_SUM_NUMERIC) ? TdcAggKind::SUM_NUMERIC
							      : TdcAggKind::AVG_NUMERIC;

	AttrNumber bare_attno = InvalidAttrNumber;
	if (IsBareVarArg(arg, bare_attno))
	{
		const ColumnSchema *col = nullptr;
		if (!LookupRawColumn(bare_attno, raw_attnos, raw_cols, col))
			return false;
		int8_t scale = 0;
		if (!ColumnNumericScale(*col, scale))
			return false;
		out_desc.input_col_idx = col->chunk_slot;
		out_numeric_scale = scale;
		return true;
	}

	int8_t lowered_scale = 0;
	uint8_t lowered_slot = 0;
	if (LookupCachedExpr(arg, &materialized_exprs, lowered_scale, lowered_slot))
	{
		out_desc.input_col_idx = lowered_slot;
		out_numeric_scale = lowered_scale;
		return true;
	}

	const uint16_t first_step_idx = static_cast<uint16_t>(project_steps.size());
	if (!LowerExprToStepsInternal(arg, project_steps, next_int64_slot,
			raw_attnos, raw_cols, &materialized_exprs,
			lowered_scale, lowered_slot))
		return false;
	if (project_steps.size() == first_step_idx)
		return false;

	const uint16_t n_steps = static_cast<uint16_t>(project_steps.size() - first_step_idx);
	project_exprs.push_back(ProjectExprDesc{first_step_idx,
		n_steps,
		lowered_slot,
		lowered_scale,
		0});
	materialized_exprs.push_back(MaterializedProjectExpr{arg, lowered_scale, lowered_slot});
	out_desc.input_col_idx = lowered_slot;
	out_numeric_scale = lowered_scale;
	return true;
}

static bool
ExtractQ1Shape(Sort *sort, Agg *agg, SeqScan *scan, QueryDesc *qd,
		   Q1ExtractedShape &out)
{
	out.scan = scan;
	out.agg = agg;
	out.sort = sort;

	if (!ExtractRelid(scan, qd, out.relid))
		return false;
	if (!ExtractGroupAttnos(agg, scan, out.group_attnos))
		return false;
	if (!ExtractAggrefs(agg, out.group_attnos, out.aggrefs))
		return false;
	if (!ExtractQual(scan, out.qual))
		return false;
	if (!ExtractSortKeys(sort, agg, out.sort_keys))
		return false;

	out.seq_scan_attnos = out.group_attnos;
	std::vector<AttrNumber> agg_arg_attnos;
	if (!CollectAggrefArgAttnos(out.aggrefs, agg_arg_attnos))
		return false;
	for (AttrNumber a : agg_arg_attnos)
	{
		bool seen = false;
		for (AttrNumber existing : out.seq_scan_attnos)
		{
			if (existing == a)
			{
				seen = true;
				break;
			}
		}
		if (!seen)
			out.seq_scan_attnos.push_back(a);
	}
	if (!BuildSeqScanColumns(out.relid,
			out.seq_scan_attnos,
			out.seq_scan_columns,
			out.next_int32_slot,
			out.next_int64_slot,
			out.next_double_slot))
		return false;

	std::vector<MaterializedProjectExpr> materialized_exprs;
	for (Aggref *aggref : out.aggrefs)
	{
		AggFuncDesc desc{};
		TdcAggKind kind = TdcAggKind::COUNT_STAR;
		int16_t numeric_scale = 0;
		if (!ClassifyAggref(aggref,
				out.seq_scan_attnos,
				out.seq_scan_columns,
				out.project_steps,
				out.project_exprs,
				materialized_exprs,
				out.next_int64_slot,
				desc,
				kind,
				numeric_scale))
			return false;
		out.agg_funcs.push_back(desc);
		out.agg_kinds.push_back(kind);
		out.agg_numeric_scales.push_back(numeric_scale);
	}

	if (!BuildHashGroupLayout(out.group_attnos,
			out.seq_scan_attnos,
			out.seq_scan_columns,
			out.agg_funcs,
			out.agg_kinds,
			out.agg_numeric_scales,
			out.hash_layout))
		return false;

	if (!BuildSortLayouts(out.group_attnos,
			out.seq_scan_attnos,
			out.seq_scan_columns,
			out.agg_funcs,
			out.agg_kinds,
			out.agg_numeric_scales,
			out.sort_keys,
			out.sort_key_layout,
			out.sort_payload_layout))
		return false;

	return true;
}

}  /* namespace */

std::unique_ptr<PhysicalOperator>
Translator::TranslatePlan(Plan *plan, QueryDesc *qd, PgVolVecQueryState *state)
{
	if (plan == nullptr || qd == nullptr || state == nullptr)
		return nullptr;

	Sort *sort = nullptr;
	Agg *agg = nullptr;
	SeqScan *scan = nullptr;
	if (!MatchQ1Shape(plan, &sort, &agg, &scan))
		return nullptr;

	Q1ExtractedShape shape{};
	if (!ExtractQ1Shape(sort, agg, scan, qd, shape))
		return nullptr;
	if (state->runtime_dsa == nullptr)
		return nullptr;

	shape.hash_layout_dp = SerializeTupleDataLayout(shape.hash_layout, state->runtime_dsa);
	shape.sort_key_layout_dp = SerializeTupleDataLayout(shape.sort_key_layout, state->runtime_dsa);
	shape.sort_payload_layout_dp = SerializeTupleDataLayout(shape.sort_payload_layout, state->runtime_dsa);
	if (shape.hash_layout_dp == InvalidDsaPointer ||
		shape.sort_key_layout_dp == InvalidDsaPointer ||
		shape.sort_payload_layout_dp == InvalidDsaPointer)
		return nullptr;

	/*
	 * Bug D fix — publish PhysicalSeqScan's output_schema and qual into DSA.
	 *
	 * SeqScan workers call ResolveSchemaDescriptor(output_schema_dp_) and
	 * ResolveQualDescriptor(qual_desc_dp_) inside GetData; if either ctor
	 * arg is InvalidDsaPointer (the previous behaviour) the worker either
	 * elog(ERROR)s ("output_schema_dp not published") or silently runs
	 * without any qual filter. Build them here at descriptor-build time and
	 * pass real dsa_pointers, mirroring how OutputSink consumes
	 * BuildOutputSchemaDescriptor above.
	 *
	 * Layout: output_schema mirrors shape.seq_scan_columns 1:1 (chunk_slot
	 * already populated by BuildSeqScanColumns). Qual: copy POD struct.
	 * input_schema_dp_ is currently unread by PhysicalSeqScan, leave Invalid.
	 */
	const uint16_t seq_n_cols = static_cast<uint16_t>(shape.seq_scan_columns.size());
	dsa_pointer seq_output_schema_dp = InvalidDsaPointer;
	if (seq_n_cols > 0 && seq_n_cols <= 16)
	{
		const Size seq_sz = offsetof(SchemaDescriptor, columns) +
			static_cast<Size>(seq_n_cols) * sizeof(ColumnSchema);
		seq_output_schema_dp = dsa_allocate0(state->runtime_dsa, seq_sz);
		if (!DsaPointerIsValid(seq_output_schema_dp))
			return nullptr;
		auto *seq_schema = static_cast<SchemaDescriptor *>(
			dsa_get_address(state->runtime_dsa, seq_output_schema_dp));
		seq_schema->n_columns = seq_n_cols;
		for (uint16_t i = 0; i < seq_n_cols; ++i)
			seq_schema->columns[i] = shape.seq_scan_columns[i];
	}

	dsa_pointer seq_qual_dp = InvalidDsaPointer;
	if (shape.qual.kind != QualKind::NONE)
	{
		seq_qual_dp = dsa_allocate0(state->runtime_dsa, sizeof(QualDescriptor));
		if (!DsaPointerIsValid(seq_qual_dp))
			return nullptr;
		*static_cast<QualDescriptor *>(
			dsa_get_address(state->runtime_dsa, seq_qual_dp)) = shape.qual;
	}

	auto seq_op = std::make_unique<PhysicalSeqScan>(
		shape.relid,
		InvalidDsaPointer,
		seq_output_schema_dp,
		seq_qual_dp,
		InvalidDsaPointer);

	std::unique_ptr<PhysicalOperator> hash_child = std::move(seq_op);
	if (!shape.project_exprs.empty())
	{
		PgVector<ProjectExprDesc> expr_descs;
		expr_descs.assign(shape.project_exprs.begin(), shape.project_exprs.end());
		PgVector<ProjectStep> steps;
		steps.assign(shape.project_steps.begin(), shape.project_steps.end());

		auto project_op = std::make_unique<PhysicalProjection>(
			InvalidDsaPointer,
			InvalidDsaPointer,
			std::move(expr_descs),
			std::move(steps));
		project_op->AddChild(std::move(hash_child));
		hash_child = std::move(project_op);
	}

	PgVector<uint16_t> group_keys;
	for (AttrNumber attno : shape.group_attnos)
	{
		const ColumnSchema *col = nullptr;
		if (!LookupRawColumn(attno, shape.seq_scan_attnos, shape.seq_scan_columns, col))
			return nullptr;
		group_keys.push_back(col->chunk_slot);
	}

	PgVector<AggFuncDesc> agg_funcs;
	agg_funcs.assign(shape.agg_funcs.begin(), shape.agg_funcs.end());
	if (agg_funcs.size() != shape.aggrefs.size())
		return nullptr;

	auto hash_op = std::make_unique<PhysicalHashAggregate>(
		shape.hash_layout_dp,
		std::move(group_keys),
		std::move(agg_funcs),
		InvalidDsaPointer);
	hash_op->AddChild(std::move(hash_child));

	auto order_op = std::make_unique<PhysicalOrder>(
		shape.sort_key_layout_dp,
		shape.sort_payload_layout_dp,
		InvalidDsaPointer);
	order_op->AddChild(std::move(hash_op));

	dsa_pointer output_input_schema_dp =
		BuildOutputSchemaDescriptor(shape, state->runtime_dsa);
	if (!DsaPointerIsValid(output_input_schema_dp))
		return nullptr;

	double plan_rows = 1024.0;
	if (qd->plannedstmt != nullptr && qd->plannedstmt->planTree != nullptr)
		plan_rows = qd->plannedstmt->planTree->plan_rows;
	const double cap_d = std::max(1024.0,
		std::min(static_cast<double>(1u << 20), plan_rows * 1.5));
	const uint32_t row_capacity = static_cast<uint32_t>(cap_d);

	/* Pre-alloc + Init the OutputSink global TDC at descriptor-build time and
	 * publish the dsa_pointer through the OutputSink ctor (mirrored to
	 * desc_->body.output.shared_payload by EmitOutput). REQUIRED because
	 * OutputSink runs in the OUTPUT pipeline whose RUN tasks are popped
	 * concurrently by every worker; no leader-first gate exists at attach
	 * time (unlike PhysicalHashAggregate, whose Combine is leader-only and
	 * thus self-allocates in GetGlobalSinkState — see physical_hash_aggregate.cpp:110-125).
	 * Without pre-alloc, the first worker to call OutputSink::GetGlobalSinkState
	 * loads an InvalidDsaPointer from the descriptor and ResolveTdc returns
	 * nullptr -> "output sink global TDC not initialized" ERROR. */
	dsa_pointer output_payload_dp = dsa_allocate0(state->runtime_dsa,
		TupleDataCollectionAllocSize(row_capacity, shape.sort_payload_layout.row_width));
	if (!DsaPointerIsValid(output_payload_dp))
		return nullptr;
	auto *output_tdc = static_cast<TupleDataCollection *>(
		dsa_get_address(state->runtime_dsa, output_payload_dp));
	TupleDataCollectionInit(output_tdc,
		row_capacity,
		shape.sort_payload_layout.row_width,
		shape.sort_payload_layout_dp);

	auto output_op = std::make_unique<OutputSink>(
		qd->dest,
		qd->tupDesc,
		output_input_schema_dp,
		shape.sort_payload_layout_dp,
		output_payload_dp,
		row_capacity,
		nullptr);
	output_op->AddChild(std::move(order_op));

	return output_op;
}

std::unique_ptr<PhysicalOperator>
Translator::Translate(QueryDesc *qd, PgVolVecQueryState *state)
{
	if (qd == nullptr || state == nullptr ||
		qd->plannedstmt == nullptr || qd->plannedstmt->planTree == nullptr)
		return nullptr;

	return TranslatePlan(qd->plannedstmt->planTree, qd, state);
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */
