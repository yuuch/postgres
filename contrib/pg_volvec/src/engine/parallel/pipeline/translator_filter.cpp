#include "parallel/pipeline/translator_internal.hpp"

extern "C" {
#include "access/relation.h"
#include "catalog/pg_type_d.h"
#include "optimizer/optimizer.h"
#include "datatype/timestamp.h"
#include "storage/lockdefs.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
}

#include <cstring>

namespace pg_volvec {
namespace pipeline {
namespace translator_detail {

static bool
MapOpnoToQualOp(Oid opno, QualOp &out)
{
	char *name = get_opname(opno);
	if (name == nullptr)
		return false;
	bool ok = true;
	if (strcmp(name, "<=") == 0) out = QualOp::LE;
	else if (strcmp(name, "<") == 0) out = QualOp::LT;
	else if (strcmp(name, "=") == 0) out = QualOp::EQ;
	else if (strcmp(name, ">=") == 0) out = QualOp::GE;
	else if (strcmp(name, ">") == 0) out = QualOp::GT;
	else if (strcmp(name, "<>") == 0) out = QualOp::NE;
	else ok = false;
	pfree(name);
	return ok;
}

static bool
ExtractDateQualConst(Const *c, QualOp &qop, uint64_t &out_value)
{
	DateADT date_const;
	if (c->consttype == DATEOID)
		date_const = DatumGetDateADT(c->constvalue);
	else if (c->consttype == TIMESTAMPOID)
	{
		Timestamp ts = DatumGetTimestamp(c->constvalue);
		int64 days = ts / USECS_PER_DAY;
		int64 rem = ts % USECS_PER_DAY;
		if (rem < 0) { days -= 1; rem += USECS_PER_DAY; }
		if (rem != 0)
		{
			switch (qop)
			{
				case QualOp::LT: qop = QualOp::LE; date_const = (DateADT) days; break;
				case QualOp::LE: date_const = (DateADT) days; break;
				case QualOp::GT: qop = QualOp::GE; date_const = (DateADT) (days + 1); break;
				case QualOp::GE: date_const = (DateADT) (days + 1); break;
				case QualOp::EQ:
				case QualOp::NE:
				default:
					return false;
			}
		}
		else
			date_const = (DateADT) days;
	}
	else
		return false;
	out_value = (uint64_t) DateADTGetDatum(date_const);
	return true;
}

static bool
LookupRelationAttr(Oid relid, AttrNumber attno, Oid &out_type_oid, int32 &out_typmod)
{
	if (relid == InvalidOid || attno <= 0)
		return false;
	Relation rel = relation_open(relid, AccessShareLock);
	TupleDesc td = RelationGetDescr(rel);
	if (attno > td->natts)
	{
		relation_close(rel, AccessShareLock);
		return false;
	}
	Form_pg_attribute attr = TupleDescAttr(td, attno - 1);
	out_type_oid = attr->atttypid;
	out_typmod = attr->atttypmod;
	relation_close(rel, AccessShareLock);
	return true;
}

static bool
LookupOrAddFilterInput(AttrNumber attno,
			      ColumnDecodeKind decode_kind,
			      std::vector<FilterInputDesc> &inputs,
			      uint16_t &out_dst_col)
{
	for (const FilterInputDesc &input : inputs)
	{
		if (input.attno == static_cast<uint16_t>(attno) && input.decode_kind == decode_kind)
		{
			out_dst_col = input.dst_col;
			return true;
		}
	}
	if (inputs.size() >= FILTER_MAX_INPUTS)
		return false;
	out_dst_col = static_cast<uint16_t>(inputs.size());
	inputs.push_back(FilterInputDesc{static_cast<uint16_t>(attno), static_cast<uint8_t>(out_dst_col), decode_kind, 0});
	return true;
}

static bool
StoreFilterStringConstBytes(const char *data,
			    uint32_t len,
			    std::vector<char> &pool,
			    uint32_t &out_offset,
			    uint64_t &out_value)
{
	out_value = 0;
	if (len > 0 && data != nullptr)
		std::memcpy(&out_value, data, len > 8 ? 8 : len);
	if (len <= 8)
	{
		out_offset = UINT32_MAX;
		return true;
	}
	out_offset = static_cast<uint32_t>(pool.size());
	pool.insert(pool.end(), data, data + len);
	return true;
}

static bool
ExtractCharFilterConst(Const *c, int32_t &out_value)
{
	if (c == nullptr || c->constisnull)
		return false;
	if (c->consttype == CHAROID)
	{
		out_value = static_cast<unsigned char>(DatumGetChar(c->constvalue));
		return true;
	}
	if (c->consttype != BPCHAROID && c->consttype != TEXTOID && c->consttype != VARCHAROID)
		return false;
	char *str = TextDatumGetCString(c->constvalue);
	const size_t len = std::strlen(str);
	if (len != 1)
	{
		pfree(str);
		return false;
	}
	out_value = static_cast<unsigned char>(str[0]);
	pfree(str);
	return true;
}

static bool
ExtractStringFilterConst(Const *c,
			 std::vector<char> &pool,
			 uint32_t &out_offset,
			 uint32_t &out_len,
			 uint64_t &out_value)
{
	if (c == nullptr || c->constisnull)
		return false;
	if (c->consttype != BPCHAROID && c->consttype != TEXTOID && c->consttype != VARCHAROID)
		return false;
	char *str = TextDatumGetCString(c->constvalue);
	out_len = static_cast<uint32_t>(std::strlen(str));
	const bool ok = StoreFilterStringConstBytes(str, out_len, pool, out_offset, out_value);
	pfree(str);
	return ok;
}

bool
ExtractStringLikePrefix(Const *c,
			 std::vector<char> &pool,
			 uint32_t &out_offset,
			 uint32_t &out_len,
			 uint64_t &out_value)
{
	if (c == nullptr || c->constisnull)
		return false;
	if (c->consttype != BPCHAROID && c->consttype != TEXTOID && c->consttype != VARCHAROID)
		return false;
	char *pattern = TextDatumGetCString(c->constvalue);
	const size_t len = std::strlen(pattern);
	if (len == 0 || pattern[len - 1] != '%')
	{
		pfree(pattern);
		return false;
	}
	for (size_t i = 0; i + 1 < len; ++i)
	{
		if (pattern[i] == '%' || pattern[i] == '_')
		{
			pfree(pattern);
			return false;
		}
	}
	out_len = static_cast<uint32_t>(len - 1);
	const bool ok = StoreFilterStringConstBytes(pattern, out_len, pool, out_offset, out_value);
	pfree(pattern);
	return ok;
}

static bool
AllocateFilterBoolReg(uint16_t &next_reg, uint16_t &out_reg)
{
	if (next_reg >= FILTER_MAX_BOOL_REGS)
		return false;
	out_reg = next_reg++;
	return true;
}

static bool
ExtractConstFilterExpr(Expr *expr, Const *&out_const)
{
	expr = StripRelabels(expr);
	if (expr == nullptr)
		return false;
	if (nodeTag(expr) == T_Const)
	{
		out_const = (Const *) expr;
		return true;
	}

	Node *folded = eval_const_expressions(nullptr, (Node *) expr);
	if (folded == nullptr)
		return false;
	folded = (Node *) StripRelabels((Expr *) folded);
	if (folded == nullptr || nodeTag(folded) != T_Const)
		return false;
	out_const = (Const *) folded;
	return true;
}

static bool
LowerFilterExprInternal(Expr *expr,
			Oid relid,
			std::vector<FilterInputDesc> &inputs,
			std::vector<FilterStep> &steps,
			std::vector<char> &string_consts,
			uint16_t &next_bool_reg,
			uint16_t &out_bool_reg)
{
	expr = StripRelabels(expr);
	if (expr == nullptr)
		return false;
	if (nodeTag(expr) == T_BoolExpr)
	{
		BoolExpr *bool_expr = (BoolExpr *) expr;
		if (bool_expr->boolop == NOT_EXPR)
		{
			if (list_length(bool_expr->args) != 1)
				return false;
			uint16_t child_reg = 0;
			if (!LowerFilterExprInternal((Expr *) linitial(bool_expr->args), relid, inputs, steps, string_consts, next_bool_reg, child_reg) ||
			    !AllocateFilterBoolReg(next_bool_reg, out_bool_reg))
				return false;
			steps.push_back(FilterStep{FilterStepOp::BOOL_NOT, QualOp::EQ, child_reg, 0, out_bool_reg, 0, UINT32_MAX, 0, 0});
			return true;
		}
		ListCell *lc = list_head(bool_expr->args);
		if (lc == nullptr)
			return false;
		uint16_t left_reg = 0;
		if (!LowerFilterExprInternal((Expr *) lfirst(lc), relid, inputs, steps, string_consts, next_bool_reg, left_reg))
			return false;
		for_each_from(lc, bool_expr->args, 1)
		{
			uint16_t right_reg = 0;
			uint16_t combined_reg = 0;
			if (!LowerFilterExprInternal((Expr *) lfirst(lc), relid, inputs, steps, string_consts, next_bool_reg, right_reg) ||
			    !AllocateFilterBoolReg(next_bool_reg, combined_reg))
				return false;
			steps.push_back(FilterStep{bool_expr->boolop == AND_EXPR ? FilterStepOp::BOOL_AND : FilterStepOp::BOOL_OR,
				QualOp::EQ, left_reg, right_reg, combined_reg, 0, UINT32_MAX, 0, 0});
			left_reg = combined_reg;
		}
		out_bool_reg = left_reg;
		return true;
	}
	if (nodeTag(expr) != T_OpExpr)
		return false;
	OpExpr *op = (OpExpr *) expr;
	if (list_length(op->args) != 2)
		return false;
	Expr *left = StripRelabels((Expr *) linitial(op->args));
	Const *c = nullptr;
	if (left == nullptr || nodeTag(left) != T_Var ||
		!ExtractConstFilterExpr((Expr *) lsecond(op->args), c))
		return false;
	Var *var = (Var *) left;
	if (var->varattno <= 0 || c->constisnull)
		return false;
	Oid rel_type_oid = InvalidOid;
	int32 rel_typmod = -1;
	if (!LookupRelationAttr(relid, var->varattno, rel_type_oid, rel_typmod) || rel_type_oid != var->vartype)
		return false;
	char *opname = get_opname(op->opno);
	if (opname == nullptr)
		return false;
	FilterStep step{};
	step.const_offset = UINT32_MAX;
	step.const_len = 0;
	step.const_value = 0;
	step._pad0 = 0;
	if (!AllocateFilterBoolReg(next_bool_reg, out_bool_reg))
	{
		pfree(opname);
		return false;
	}
	step.out_bool_reg = out_bool_reg;
	bool ok = false;
	if (var->vartype == DATEOID)
	{
		step.op = FilterStepOp::INT32_CMP_CONST;
		ok = MapOpnoToQualOp(op->opno, step.cmp_op) &&
			LookupOrAddFilterInput(var->varattno, ColumnDecodeKind::INT32_DATE, inputs, step.left_idx) &&
			ExtractDateQualConst(c, step.cmp_op, step.const_value);
	}
	else if (var->vartype == INT4OID)
	{
		step.op = FilterStepOp::INT32_CMP_CONST;
		ok = MapOpnoToQualOp(op->opno, step.cmp_op) && c->consttype == INT4OID && c->constbyval &&
			LookupOrAddFilterInput(var->varattno, ColumnDecodeKind::INT32_INT4, inputs, step.left_idx);
		if (ok)
			step.const_value = static_cast<uint64_t>(DatumGetInt32(c->constvalue));
	}
	else if (var->vartype == BPCHAROID || var->vartype == CHAROID)
	{
		step.op = FilterStepOp::INT32_CMP_CONST;
		int32_t ch = 0;
		ok = MapOpnoToQualOp(op->opno, step.cmp_op) &&
			LookupOrAddFilterInput(var->varattno, ColumnDecodeKind::INT32_CHAR, inputs, step.left_idx) &&
			ExtractCharFilterConst(c, ch);
		if (ok)
			step.const_value = static_cast<uint64_t>(static_cast<uint32_t>(ch));
	}
	else if (var->vartype == INT8OID)
	{
		step.op = FilterStepOp::INT64_CMP_CONST;
		ok = MapOpnoToQualOp(op->opno, step.cmp_op) && c->consttype == INT8OID && c->constbyval &&
			LookupOrAddFilterInput(var->varattno, ColumnDecodeKind::INT64_INT8, inputs, step.left_idx);
		if (ok)
			step.const_value = static_cast<uint64_t>(DatumGetInt64(c->constvalue));
	}
	else if (var->vartype == NUMERICOID)
	{
		int64_t const_value = 0;
		step.op = FilterStepOp::INT64_CMP_CONST;
		ok = MapOpnoToQualOp(op->opno, step.cmp_op) &&
			LookupOrAddFilterInput(var->varattno, ColumnDecodeKind::INT64_NUMERIC_SCALED, inputs, step.left_idx) &&
			ScaleNumericConstDatumToTargetScale(c, static_cast<int8_t>(ExtractNumericTypmodScale(rel_typmod)), const_value);
		if (ok)
			step.const_value = static_cast<uint64_t>(const_value);
	}
	else if (var->vartype == TEXTOID || var->vartype == VARCHAROID)
	{
		ok = LookupOrAddFilterInput(var->varattno, ColumnDecodeKind::STRING_REF, inputs, step.left_idx);
		if (ok && std::strcmp(opname, "=") == 0)
		{
			step.op = FilterStepOp::STRING_EQ_CONST;
			ok = ExtractStringFilterConst(c, string_consts, step.const_offset, step.const_len, step.const_value);
		}
		else if (ok && std::strcmp(opname, "<>") == 0)
		{
			step.op = FilterStepOp::STRING_NE_CONST;
			ok = ExtractStringFilterConst(c, string_consts, step.const_offset, step.const_len, step.const_value);
		}
		else if (ok && std::strcmp(opname, "~~") == 0)
		{
			step.op = FilterStepOp::STRING_PREFIX_LIKE;
			ok = ExtractStringLikePrefix(c, string_consts, step.const_offset, step.const_len, step.const_value);
		}
		else
			ok = false;
	}
	pfree(opname);
	if (!ok)
		return false;
	steps.push_back(step);
	return true;
}

bool
ExtractFilterQual(List *qual,
		      Oid relid,
		      std::vector<FilterInputDesc> &inputs,
		      std::vector<FilterExprDesc> &exprs,
		      std::vector<FilterStep> &steps,
		      std::vector<char> &string_consts,
		      uint16_t &next_bool_reg)
{
	inputs.clear();
	exprs.clear();
	steps.clear();
	string_consts.clear();
	next_bool_reg = 0;
	if (qual == NIL)
		return true;
	ListCell *lc;
	foreach(lc, qual)
	{
		const uint16_t first_step_idx = static_cast<uint16_t>(steps.size());
		uint16_t output_reg = 0;
		if (!LowerFilterExprInternal((Expr *) lfirst(lc), relid, inputs, steps, string_consts, next_bool_reg, output_reg))
			return false;
		if (steps.size() > FILTER_MAX_STEPS)
			return false;
		exprs.push_back(FilterExprDesc{first_step_idx,
			static_cast<uint16_t>(steps.size() - first_step_idx),
			output_reg,
			0});
	}
	return true;
}

bool
ExtractSortKeys(Sort *sort,
		Agg *agg,
		Plan *agg_plan,
		const std::vector<ColumnRef> &group_cols,
		const std::vector<Aggref *> &aggrefs,
		std::vector<SortKeyDesc> &out)
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
		TargetEntry *tle = (TargetEntry *) list_nth(agg_tlist, resno - 1);
		if (tle == nullptr || tle->expr == nullptr)
			return false;
		SortKeyDesc k{};
		k.collation_oid = sort->collations[i];
		k.asc = true;
		k.nulls_first = sort->nullsFirst[i];
		k._pad = 0;
		QualOp dummy;
		if (MapOpnoToQualOp(sort->sortOperators[i], dummy))
			k.asc = (dummy == QualOp::LT) || (dummy == QualOp::LE);

		Expr *expr = StripRelabels((Expr *) tle->expr);
		if (expr == nullptr)
			return false;
		if (nodeTag(expr) == T_Aggref)
		{
			uint16_t agg_idx = UINT16_MAX;
			for (uint16_t a = 0; a < aggrefs.size(); ++a)
			{
				if (aggrefs[a] == (Aggref *) expr)
				{
					agg_idx = a;
					break;
				}
			}
			if (agg_idx == UINT16_MAX)
				return false;
			k.col_idx = static_cast<uint16_t>(group_cols.size() + agg_idx);
		}
		else
		{
			ColumnRef ref{};
			if (!ResolvePlanExprToColumnRef(expr, agg_plan, ref))
				return false;
			uint16_t group_idx = UINT16_MAX;
			for (uint16_t g = 0; g < group_cols.size(); ++g)
			{
				if (group_cols[g] == ref)
				{
					group_idx = g;
					break;
				}
			}
			if (group_idx == UINT16_MAX)
				return false;
			k.col_idx = group_idx;
		}
		out.push_back(k);
	}
	return true;
}

bool
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

}  /* namespace translator_detail */
}  /* namespace pipeline */
}  /* namespace pg_volvec */
