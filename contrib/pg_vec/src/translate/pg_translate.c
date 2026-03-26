#include "postgres.h"

#include <ctype.h>

#include "catalog/pg_type_d.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "parser/parsetree.h"
#include "varatt.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"

#include "pg_translate.h"

typedef struct PgVecInputContext
{
	Index		rtindex;
	Oid			relid;
	uint8		input_id;
} PgVecInputContext;

typedef struct PgVecLowerContext
{
	int			ninputs;
	PgVecInputContext inputs[PG_VEC_MAX_INPUTS];
} PgVecLowerContext;

static bool pg_vec_try_translate_scan_filter_agg_plan(QueryDesc *queryDesc,
														  PgVecPlan *plan);
static bool pg_vec_try_translate_join_filter_agg_plan(QueryDesc *queryDesc,
														  PgVecPlan *plan);
static void pg_vec_plan_init(PgVecPlan *plan);
static void pg_vec_input_spec_init(PgVecInputSpec *input);
static void pg_vec_expr_program_init(PgVecExprProgram *program);
static void pg_vec_filter_spec_init(PgVecFilterSpec *filter);
static void pg_vec_output_expr_program_init(PgVecOutputExprProgram *program);
static bool pg_vec_add_scan_column(PgVecInputSpec *input,
								   uint8 input_id,
								   AttrNumber attno,
								   PgVecScalarKind scalar_kind);
static bool pg_vec_add_expr_node(PgVecExprProgram *program,
								 const PgVecExprNode *node,
								 int *node_idx);
static bool pg_vec_add_qual_node(PgVecFilterSpec *filter,
								 const PgVecQualNode *node,
								 int *node_idx);
static bool pg_vec_add_binary_qual(PgVecFilterSpec *filter,
								   PgVecQualKind kind,
								   int left_idx,
								   int right_idx,
								   int *node_idx);
static bool pg_vec_add_output_expr_node(PgVecOutputExprProgram *program,
										const PgVecOutputExprNode *node,
										int *node_idx);
static bool pg_vec_add_agg_call(PgVecAggSpec *agg,
								const PgVecAggCall *agg_call,
								int *agg_idx);
static bool pg_vec_add_group_key(PgVecAggSpec *agg,
								 PgVecColumnRef group_key,
								 int *group_idx);
static bool pg_vec_scalar_kind_from_pg_type(Oid type_oid,
											int32 typmod,
											PgVecScalarKind *scalar_kind);
static bool pg_vec_lower_const_value(Const *constnode,
									 PgVecScalarKind scalar_kind,
									 PgVecConstValue *constant);
static bool pg_vec_resolve_binary_expr_kind(PgVecExprKind expr_kind,
											PgVecScalarKind left_kind,
											PgVecScalarKind right_kind,
											PgVecScalarKind *result_kind);
static bool pg_vec_resolve_binary_output_kind(PgVecOutputExprKind expr_kind,
											  PgVecScalarKind left_kind,
											  PgVecScalarKind right_kind,
											  PgVecScalarKind *result_kind);
static Node *pg_vec_strip_implicit_casts(Node *node);
static TargetEntry *pg_vec_find_tle_by_resno(List *targetlist, AttrNumber resno);
static Node *pg_vec_resolve_var_through_plan(Node *node, Plan *plan);
static bool pg_vec_lower_var(Var *var,
							 const PgVecLowerContext *ctx,
							 PgVecPlan *plan,
							 PgVecColumnRef *column_ref);
static bool pg_vec_lower_expr_internal(Node *node,
									   Plan *source_plan,
									   const PgVecLowerContext *ctx,
									   PgVecPlan *plan,
									   PgVecExprProgram *program,
									   bool has_expected_kind,
									   PgVecScalarKind expected_kind,
									   int *expr_root);
static bool pg_vec_lower_expr(Node *node,
							  Plan *source_plan,
							  const PgVecLowerContext *ctx,
							  PgVecPlan *plan,
							  PgVecExprProgram *program,
							  int *expr_root);
static bool pg_vec_lower_compare_operands(Node *left,
										  Node *right,
										  Plan *source_plan,
										  const PgVecLowerContext *ctx,
										  PgVecPlan *plan,
										  PgVecFilterSpec *filter,
										  int *left_root,
										  int *right_root);
static bool pg_vec_try_parse_prefix_like(Const *constnode,
										 PgVecStringConst *prefix);
static bool pg_vec_lower_qual(Node *node,
							  Plan *source_plan,
							  const PgVecLowerContext *ctx,
							  PgVecPlan *plan,
							  PgVecFilterSpec *filter,
							  int *qual_root);
static bool pg_vec_lower_filter_quals(List *quals,
									  Plan *source_plan,
									  const PgVecLowerContext *ctx,
									  PgVecPlan *plan,
									  PgVecFilterSpec *filter);
static bool pg_vec_agg_kind_from_aggref(Aggref *aggref, PgVecAggKind *agg_kind);
static bool pg_vec_const_is_zero(Node *node);
static bool pg_vec_try_lower_conditional_agg(Aggref *aggref,
											 Plan *source_plan,
											 const PgVecLowerContext *ctx,
											 PgVecPlan *plan,
											 PgVecAggCall *agg_call);
static bool pg_vec_lower_agg_call(Aggref *aggref,
								  Plan *source_plan,
								  const PgVecLowerContext *ctx,
								  PgVecPlan *plan,
								  int *agg_idx);
static bool pg_vec_lower_output_expr(Node *node,
									 Plan *source_plan,
									 const PgVecLowerContext *ctx,
									 PgVecPlan *plan,
									 PgVecOutputExprProgram *program,
									 int *expr_root);
static bool pg_vec_lower_agg_targetlist(QueryDesc *queryDesc,
										List *targetlist,
										Plan *source_plan,
										const PgVecLowerContext *ctx,
										PgVecPlan *plan);
static bool pg_vec_make_single_input_context(SeqScan *seqscan,
											 PlannedStmt *plannedstmt,
											 PgVecLowerContext *ctx);
static bool pg_vec_make_join_input_context(SeqScan *left_scan,
										   SeqScan *right_scan,
										   PlannedStmt *plannedstmt,
										   PgVecLowerContext *ctx);
static bool pg_vec_lower_join_key_expr(Node *node,
									   Plan *join_plan,
									   const PgVecLowerContext *ctx,
									   PgVecPlan *plan,
									   PgVecColumnRef *column_ref);
static bool pg_vec_lower_join_keys_from_list(List *clauses,
											 Plan *join_plan,
											 const PgVecLowerContext *ctx,
											 PgVecPlan *plan);
static bool pg_vec_lower_join_spec(Plan *join_plan,
								   const PgVecLowerContext *ctx,
								   PgVecPlan *plan);
static bool pg_vec_numeric_to_scaled_int64(Datum value, int scale, int64 *out);
static bool pg_vec_parse_scaled_int64(const char *str, int scale, int64 *out);

bool
pg_vec_try_translate_plan(QueryDesc *queryDesc, int eflags, PgVecPlan *plan)
{
	pg_vec_plan_init(plan);

	if ((eflags & EXEC_FLAG_EXPLAIN_ONLY) != 0)
		return false;

	if (queryDesc->operation != CMD_SELECT || queryDesc->estate == NULL)
		return false;

	if (queryDesc->plannedstmt == NULL || queryDesc->plannedstmt->planTree == NULL)
		return false;

	if (pg_vec_try_translate_scan_filter_agg_plan(queryDesc, plan))
		return true;

	if (pg_vec_try_translate_join_filter_agg_plan(queryDesc, plan))
		return true;

	return false;
}

const char *
pg_vec_plan_kind_name(PgVecPlanKind kind)
{
	switch (kind)
	{
		case PG_VEC_PLAN_SCAN_FILTER_AGG:
			return "scan_filter_agg";
		case PG_VEC_PLAN_UNSUPPORTED:
		default:
			return "unsupported";
	}
}

static void
pg_vec_plan_init(PgVecPlan *plan)
{
	MemSet(plan, 0, sizeof(*plan));
	plan->kind = PG_VEC_PLAN_UNSUPPORTED;
	for (int input_id = 0; input_id < PG_VEC_MAX_INPUTS; input_id++)
		pg_vec_input_spec_init(&plan->inputs[input_id]);
}

static void
pg_vec_input_spec_init(PgVecInputSpec *input)
{
	MemSet(input, 0, sizeof(*input));
	pg_vec_filter_spec_init(&input->filter);
}

static void
pg_vec_expr_program_init(PgVecExprProgram *program)
{
	MemSet(program, 0, sizeof(*program));
	program->root = -1;
}

static void
pg_vec_filter_spec_init(PgVecFilterSpec *filter)
{
	MemSet(filter, 0, sizeof(*filter));
	filter->root = -1;
	pg_vec_expr_program_init(&filter->exprs);
}

static void
pg_vec_output_expr_program_init(PgVecOutputExprProgram *program)
{
	MemSet(program, 0, sizeof(*program));
	program->root = -1;
}

static bool
pg_vec_add_scan_column(PgVecInputSpec *input,
					   uint8 input_id,
					   AttrNumber attno,
					   PgVecScalarKind scalar_kind)
{
	int			i;

	for (i = 0; i < input->ncolumns; i++)
	{
		if (input->columns[i].attno != attno)
			continue;

		return input->columns[i].scalar_kind == scalar_kind;
	}

	if (input->ncolumns >= PG_VEC_MAX_SCAN_COLUMNS)
		return false;

	input->columns[input->ncolumns].input_id = input_id;
	input->columns[input->ncolumns].attno = attno;
	input->columns[input->ncolumns].scalar_kind = scalar_kind;
	input->ncolumns++;
	return true;
}

static bool
pg_vec_add_expr_node(PgVecExprProgram *program,
					 const PgVecExprNode *node,
					 int *node_idx)
{
	if (program->nnodes >= PG_VEC_MAX_EXPR_NODES)
		return false;

	program->nodes[program->nnodes] = *node;
	*node_idx = program->nnodes;
	program->nnodes++;
	return true;
}

static bool
pg_vec_add_qual_node(PgVecFilterSpec *filter,
					 const PgVecQualNode *node,
					 int *node_idx)
{
	if (filter->nnodes >= PG_VEC_MAX_FILTER_NODES)
		return false;

	filter->nodes[filter->nnodes] = *node;
	*node_idx = filter->nnodes;
	filter->nnodes++;
	return true;
}

static bool
pg_vec_add_binary_qual(PgVecFilterSpec *filter,
					   PgVecQualKind kind,
					   int left_idx,
					   int right_idx,
					   int *node_idx)
{
	PgVecQualNode node;

	MemSet(&node, 0, sizeof(node));
	node.kind = kind;
	node.left = left_idx;
	node.right = right_idx;
	node.lhs_expr = -1;
	node.rhs_expr = -1;
	return pg_vec_add_qual_node(filter, &node, node_idx);
}

static bool
pg_vec_add_output_expr_node(PgVecOutputExprProgram *program,
							const PgVecOutputExprNode *node,
							int *node_idx)
{
	if (program->nnodes >= PG_VEC_MAX_OUTPUT_EXPR_NODES)
		return false;

	program->nodes[program->nnodes] = *node;
	*node_idx = program->nnodes;
	program->nnodes++;
	return true;
}

static bool
pg_vec_add_agg_call(PgVecAggSpec *agg,
					const PgVecAggCall *agg_call,
					int *agg_idx)
{
	if (agg->naggs >= PG_VEC_MAX_AGG_CALLS)
		return false;

	agg->aggs[agg->naggs] = *agg_call;
	*agg_idx = agg->naggs;
	agg->naggs++;
	return true;
}

static bool
pg_vec_add_group_key(PgVecAggSpec *agg,
					 PgVecColumnRef group_key,
					 int *group_idx)
{
	for (int i = 0; i < agg->ngroup_keys; i++)
	{
		if (agg->group_keys[i].input_id != group_key.input_id ||
			agg->group_keys[i].attno != group_key.attno)
			continue;

		if (agg->group_keys[i].scalar_kind != group_key.scalar_kind)
			return false;

		*group_idx = i;
		return true;
	}

	if (agg->ngroup_keys >= PG_VEC_MAX_GROUP_KEYS)
		return false;

	agg->group_keys[agg->ngroup_keys] = group_key;
	*group_idx = agg->ngroup_keys;
	agg->ngroup_keys++;
	return true;
}

static bool
pg_vec_scalar_kind_from_pg_type(Oid type_oid,
								int32 typmod,
								PgVecScalarKind *scalar_kind)
{
	switch (type_oid)
	{
		case INT4OID:
			*scalar_kind = PG_VEC_SCALAR_INT32;
			return true;
		case DATEOID:
			*scalar_kind = PG_VEC_SCALAR_DATE32;
			return true;
		case NUMERICOID:
			*scalar_kind = PG_VEC_SCALAR_DECIMAL64_S2;
			return true;
		case BPCHAROID:
			if (typmod == (VARHDRSZ + 1))
				*scalar_kind = PG_VEC_SCALAR_CHAR1;
			else
				*scalar_kind = PG_VEC_SCALAR_STRING128;
			return true;
		case VARCHAROID:
		case TEXTOID:
			*scalar_kind = PG_VEC_SCALAR_STRING128;
			return true;
		default:
			return false;
	}
}

static bool
pg_vec_lower_const_value(Const *constnode,
						 PgVecScalarKind scalar_kind,
						 PgVecConstValue *constant)
{
	char	   *str;
	Size		strlen;

	if (constnode == NULL || constnode->constisnull)
		return false;

	switch (scalar_kind)
	{
		case PG_VEC_SCALAR_INT32:
			if (constnode->consttype != INT4OID)
				return false;
			constant->int32_value = DatumGetInt32(constnode->constvalue);
			return true;

		case PG_VEC_SCALAR_DATE32:
			if (constnode->consttype == DATEOID)
			{
				constant->date32 = DatumGetDateADT(constnode->constvalue);
				return true;
			}
			if (constnode->consttype == TIMESTAMPOID)
			{
				constant->date32 =
					DatumGetDateADT(DirectFunctionCall1(timestamp_date,
														 constnode->constvalue));
				return true;
			}
			return false;

		case PG_VEC_SCALAR_DECIMAL64_S2:
			if (constnode->consttype != NUMERICOID)
				return false;
			return pg_vec_numeric_to_scaled_int64(constnode->constvalue, 2,
												 &constant->decimal64_s2);

		case PG_VEC_SCALAR_CHAR1:
			if (constnode->consttype != BPCHAROID &&
				constnode->consttype != VARCHAROID &&
				constnode->consttype != TEXTOID)
				return false;
			if (VARSIZE_ANY_EXHDR(DatumGetPointer(constnode->constvalue)) < 1)
				return false;
			constant->char1 = *(VARDATA_ANY(DatumGetPointer(constnode->constvalue)));
			return true;

		case PG_VEC_SCALAR_STRING128:
			if (constnode->consttype != BPCHAROID &&
				constnode->consttype != VARCHAROID &&
				constnode->consttype != TEXTOID)
				return false;

			str = VARDATA_ANY(DatumGetPointer(constnode->constvalue));
			strlen = VARSIZE_ANY_EXHDR(DatumGetPointer(constnode->constvalue));
			if (strlen >= PG_VEC_INLINE_STRING_MAX)
				return false;

			constant->string128.len = (uint16) strlen;
			if (strlen > 0)
				memcpy(constant->string128.bytes, str, strlen);
			memset(constant->string128.bytes + strlen,
				   0,
				   PG_VEC_INLINE_STRING_MAX - strlen);
			return true;

		case PG_VEC_SCALAR_DECIMAL128_S6:
		case PG_VEC_SCALAR_DECIMAL128_S4:
		case PG_VEC_SCALAR_INVALID:
		default:
			return false;
	}
}

static bool
pg_vec_resolve_binary_expr_kind(PgVecExprKind expr_kind,
								PgVecScalarKind left_kind,
								PgVecScalarKind right_kind,
								PgVecScalarKind *result_kind)
{
	switch (expr_kind)
	{
		case PG_VEC_EXPR_ADD:
		case PG_VEC_EXPR_SUB:
			if (left_kind != right_kind)
				return false;
			switch (left_kind)
			{
				case PG_VEC_SCALAR_INT32:
				case PG_VEC_SCALAR_DECIMAL64_S2:
				case PG_VEC_SCALAR_DECIMAL128_S4:
				case PG_VEC_SCALAR_DECIMAL128_S6:
					*result_kind = left_kind;
					return true;
				case PG_VEC_SCALAR_DATE32:
				case PG_VEC_SCALAR_CHAR1:
				case PG_VEC_SCALAR_STRING128:
				case PG_VEC_SCALAR_INVALID:
				default:
					return false;
			}

		case PG_VEC_EXPR_MUL:
			if (left_kind == PG_VEC_SCALAR_DECIMAL64_S2 &&
				right_kind == PG_VEC_SCALAR_DECIMAL64_S2)
			{
				*result_kind = PG_VEC_SCALAR_DECIMAL128_S4;
				return true;
			}
			if ((left_kind == PG_VEC_SCALAR_DECIMAL128_S4 &&
				 right_kind == PG_VEC_SCALAR_DECIMAL64_S2) ||
				(left_kind == PG_VEC_SCALAR_DECIMAL64_S2 &&
				 right_kind == PG_VEC_SCALAR_DECIMAL128_S4))
			{
				*result_kind = PG_VEC_SCALAR_DECIMAL128_S6;
				return true;
			}
			return false;

		case PG_VEC_EXPR_COLUMN:
		case PG_VEC_EXPR_CONST:
		case PG_VEC_EXPR_INVALID:
		default:
			return false;
	}
}

static bool
pg_vec_resolve_binary_output_kind(PgVecOutputExprKind expr_kind,
								  PgVecScalarKind left_kind,
								  PgVecScalarKind right_kind,
								  PgVecScalarKind *result_kind)
{
	if (left_kind != right_kind &&
		!(left_kind == PG_VEC_SCALAR_DECIMAL64_S2 &&
		  right_kind == PG_VEC_SCALAR_DECIMAL128_S4) &&
		!(left_kind == PG_VEC_SCALAR_DECIMAL128_S4 &&
		  right_kind == PG_VEC_SCALAR_DECIMAL64_S2))
		return false;

	switch (expr_kind)
	{
		case PG_VEC_OUTPUT_EXPR_ADD:
		case PG_VEC_OUTPUT_EXPR_SUB:
			*result_kind = (left_kind == right_kind) ? left_kind : PG_VEC_SCALAR_DECIMAL128_S6;
			return true;
		case PG_VEC_OUTPUT_EXPR_MUL:
		case PG_VEC_OUTPUT_EXPR_DIV:
			*result_kind = PG_VEC_SCALAR_DECIMAL128_S6;
			return true;
		case PG_VEC_OUTPUT_EXPR_GROUP_KEY:
		case PG_VEC_OUTPUT_EXPR_AGGREF:
		case PG_VEC_OUTPUT_EXPR_CONST:
		case PG_VEC_OUTPUT_EXPR_INVALID:
		default:
			return false;
	}
}

static Node *
pg_vec_strip_implicit_casts(Node *node)
{
	while (node != NULL)
	{
		if (IsA(node, RelabelType))
			node = (Node *) castNode(RelabelType, node)->arg;
		else if (IsA(node, CoerceViaIO))
			node = (Node *) castNode(CoerceViaIO, node)->arg;
		else if (IsA(node, CoerceToDomain))
			node = (Node *) castNode(CoerceToDomain, node)->arg;
		else
			break;
	}

	return node;
}

static TargetEntry *
pg_vec_find_tle_by_resno(List *targetlist, AttrNumber resno)
{
	ListCell   *lc;

	foreach(lc, targetlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);

		if (tle->resjunk)
			continue;
		if (tle->resno == resno)
			return tle;
	}

	return NULL;
}

static Node *
pg_vec_resolve_var_through_plan(Node *node, Plan *plan)
{
	Var		   *var;
	Plan	   *source_plan;
	TargetEntry *tle;

	node = pg_vec_strip_implicit_casts(node);
	if (node == NULL || !IsA(node, Var))
		return node;

	var = castNode(Var, node);
	if (var->varno != OUTER_VAR && var->varno != INNER_VAR)
		return node;

	if (plan == NULL)
		return node;

	if (var->varno == OUTER_VAR)
		source_plan = plan->lefttree;
	else
		source_plan = plan->righttree;

	if (source_plan == NULL)
		return node;

	tle = pg_vec_find_tle_by_resno(source_plan->targetlist, var->varattno);
	if (tle == NULL)
		return node;

	return pg_vec_resolve_var_through_plan((Node *) tle->expr, source_plan);
}

static bool
pg_vec_lower_var(Var *var,
				 const PgVecLowerContext *ctx,
				 PgVecPlan *plan,
				 PgVecColumnRef *column_ref)
{
	PgVecScalarKind scalar_kind;

	if (var == NULL || var->varattno <= 0)
		return false;

	if (var->varno == OUTER_VAR || var->varno == INNER_VAR)
		return false;

	if (!pg_vec_scalar_kind_from_pg_type(var->vartype, var->vartypmod, &scalar_kind))
		return false;

	for (int i = 0; i < ctx->ninputs; i++)
	{
		if (ctx->inputs[i].rtindex != var->varno)
			continue;

		if (!pg_vec_add_scan_column(&plan->inputs[ctx->inputs[i].input_id],
									ctx->inputs[i].input_id,
									var->varattno,
									scalar_kind))
			return false;

		column_ref->input_id = ctx->inputs[i].input_id;
		column_ref->attno = var->varattno;
		column_ref->scalar_kind = scalar_kind;
		return true;
	}

	return false;
}

static bool
pg_vec_lower_expr_internal(Node *node,
						   Plan *source_plan,
						   const PgVecLowerContext *ctx,
						   PgVecPlan *plan,
						   PgVecExprProgram *program,
						   bool has_expected_kind,
						   PgVecScalarKind expected_kind,
						   int *expr_root)
{
	PgVecExprNode expr_node;
	OpExpr	   *opexpr;
	char	   *op_name;
	Node	   *left;
	Node	   *right;
	int			left_root;
	int			right_root;
	PgVecScalarKind left_kind;
	PgVecScalarKind right_kind;
	PgVecScalarKind result_kind;
	Const	   *constnode;

	node = pg_vec_resolve_var_through_plan(node, source_plan);
	node = pg_vec_strip_implicit_casts(node);
	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		MemSet(&expr_node, 0, sizeof(expr_node));
		expr_node.kind = PG_VEC_EXPR_COLUMN;
		if (!pg_vec_lower_var(castNode(Var, node), ctx, plan, &expr_node.column))
			return false;
		expr_node.scalar_kind = expr_node.column.scalar_kind;
		expr_node.left = -1;
		expr_node.right = -1;
		return pg_vec_add_expr_node(program, &expr_node, expr_root);
	}

	if (IsA(node, Const))
	{
		constnode = castNode(Const, node);
		if (has_expected_kind)
			result_kind = expected_kind;
		else if (!pg_vec_scalar_kind_from_pg_type(constnode->consttype,
												  constnode->consttypmod,
												  &result_kind))
			return false;

		MemSet(&expr_node, 0, sizeof(expr_node));
		expr_node.kind = PG_VEC_EXPR_CONST;
		expr_node.scalar_kind = result_kind;
		expr_node.left = -1;
		expr_node.right = -1;
		if (!pg_vec_lower_const_value(constnode,
									  result_kind,
									  &expr_node.constant))
			return false;
		return pg_vec_add_expr_node(program, &expr_node, expr_root);
	}

	if (!IsA(node, OpExpr))
		return false;

	opexpr = castNode(OpExpr, node);
	if (list_length(opexpr->args) != 2)
		return false;

	op_name = get_opname(opexpr->opno);
	if (op_name == NULL)
		return false;

	left = linitial(opexpr->args);
	right = lsecond(opexpr->args);
	if (!pg_vec_lower_expr(left, source_plan, ctx, plan, program, &left_root) ||
		!pg_vec_lower_expr(right, source_plan, ctx, plan, program, &right_root))
		return false;

	left_kind = program->nodes[left_root].scalar_kind;
	right_kind = program->nodes[right_root].scalar_kind;

	MemSet(&expr_node, 0, sizeof(expr_node));
	if (strcmp(op_name, "+") == 0)
		expr_node.kind = PG_VEC_EXPR_ADD;
	else if (strcmp(op_name, "-") == 0)
		expr_node.kind = PG_VEC_EXPR_SUB;
	else if (strcmp(op_name, "*") == 0)
		expr_node.kind = PG_VEC_EXPR_MUL;
	else
		return false;

	if (!pg_vec_resolve_binary_expr_kind(expr_node.kind,
										 left_kind,
										 right_kind,
										 &result_kind))
		return false;

	expr_node.scalar_kind = result_kind;
	expr_node.left = left_root;
	expr_node.right = right_root;
	return pg_vec_add_expr_node(program, &expr_node, expr_root);
}

static bool
pg_vec_lower_expr(Node *node,
				  Plan *source_plan,
				  const PgVecLowerContext *ctx,
				  PgVecPlan *plan,
				  PgVecExprProgram *program,
				  int *expr_root)
{
	return pg_vec_lower_expr_internal(node,
									  source_plan,
									  ctx,
									  plan,
									  program,
									  false,
									  PG_VEC_SCALAR_INVALID,
									  expr_root);
}

static bool
pg_vec_lower_compare_operands(Node *left,
							  Node *right,
							  Plan *source_plan,
							  const PgVecLowerContext *ctx,
							  PgVecPlan *plan,
							  PgVecFilterSpec *filter,
							  int *left_root,
							  int *right_root)
{
	left = pg_vec_resolve_var_through_plan(left, source_plan);
	right = pg_vec_resolve_var_through_plan(right, source_plan);
	left = pg_vec_strip_implicit_casts(left);
	right = pg_vec_strip_implicit_casts(right);

	if (IsA(left, Const) && !IsA(right, Const))
	{
		PgVecExprProgram tmp_program = filter->exprs;

		if (!pg_vec_lower_expr(right,
							   source_plan,
							   ctx,
							   plan,
							   &filter->exprs,
							   right_root))
			return false;
		if (!pg_vec_lower_expr_internal(left,
										source_plan,
										ctx,
										plan,
										&filter->exprs,
										true,
										filter->exprs.nodes[*right_root].scalar_kind,
										left_root))
		{
			filter->exprs = tmp_program;
			return false;
		}
		return true;
	}

	if (!pg_vec_lower_expr(left,
						   source_plan,
						   ctx,
						   plan,
						   &filter->exprs,
						   left_root))
		return false;

	if (IsA(right, Const))
	{
		if (!pg_vec_lower_expr_internal(right,
										source_plan,
										ctx,
										plan,
										&filter->exprs,
										true,
										filter->exprs.nodes[*left_root].scalar_kind,
										right_root))
			return false;
	}
	else if (!pg_vec_lower_expr(right,
								source_plan,
								ctx,
								plan,
								&filter->exprs,
								right_root))
		return false;

	return true;
}

static bool
pg_vec_try_parse_prefix_like(Const *constnode, PgVecStringConst *prefix)
{
	const char *payload;
	Size		payload_size;

	if (constnode == NULL || constnode->constisnull)
		return false;
	if (constnode->consttype != TEXTOID &&
		constnode->consttype != VARCHAROID &&
		constnode->consttype != BPCHAROID)
		return false;

	payload = VARDATA_ANY(DatumGetPointer(constnode->constvalue));
	payload_size = VARSIZE_ANY_EXHDR(DatumGetPointer(constnode->constvalue));
	if (payload_size < 1)
		return false;
	if (payload[payload_size - 1] != '%')
		return false;
	for (Size i = 0; i + 1 < payload_size; i++)
	{
		if (payload[i] == '%' || payload[i] == '_')
			return false;
	}
	if (payload_size - 1 >= PG_VEC_INLINE_STRING_MAX)
		return false;

	prefix->len = (uint16) (payload_size - 1);
	if (prefix->len > 0)
		memcpy(prefix->bytes, payload, prefix->len);
	memset(prefix->bytes + prefix->len,
		   0,
		   PG_VEC_INLINE_STRING_MAX - prefix->len);
	return true;
}

static bool
pg_vec_lower_qual(Node *node,
				  Plan *source_plan,
				  const PgVecLowerContext *ctx,
				  PgVecPlan *plan,
				  PgVecFilterSpec *filter,
				  int *qual_root)
{
	BoolExpr   *bool_expr;
	OpExpr	   *opexpr;
	PgVecQualNode qual_node;
	Node	   *left;
	Node	   *right;
	char	   *op_name;
	int			left_root;
	int			right_root;
	PgVecFilterOp filter_op;
	Const	   *prefix_const;
	PgVecStringConst prefix_value;

	node = pg_vec_resolve_var_through_plan(node, source_plan);
	node = pg_vec_strip_implicit_casts(node);
	if (node == NULL)
		return false;

	if (IsA(node, BoolExpr))
	{
		ListCell   *lc;
		int			current_root = -1;
		PgVecQualKind kind;

		bool_expr = castNode(BoolExpr, node);
		if (bool_expr->boolop == NOT_EXPR)
			return false;
		if (bool_expr->args == NIL)
			return false;

		kind = (bool_expr->boolop == AND_EXPR) ? PG_VEC_QUAL_AND : PG_VEC_QUAL_OR;

		foreach(lc, bool_expr->args)
		{
			int child_root;

			if (!pg_vec_lower_qual(lfirst(lc), source_plan, ctx, plan, filter, &child_root))
				return false;

			if (current_root < 0)
				current_root = child_root;
			else if (!pg_vec_add_binary_qual(filter,
											 kind,
											 current_root,
											 child_root,
											 &current_root))
				return false;
		}

		*qual_root = current_root;
		return true;
	}

	if (!IsA(node, OpExpr))
		return false;

	opexpr = castNode(OpExpr, node);
	if (list_length(opexpr->args) != 2)
		return false;

	op_name = get_opname(opexpr->opno);
	if (op_name == NULL)
		return false;

	left = linitial(opexpr->args);
	right = lsecond(opexpr->args);

	if (strcmp(op_name, "~~") == 0)
	{
		PgVecExprNode rhs_node;

		prefix_const = castNode(Const, pg_vec_strip_implicit_casts(right));
		if (!IsA(pg_vec_strip_implicit_casts(right), Const) ||
			!pg_vec_try_parse_prefix_like(prefix_const, &prefix_value))
			return false;

		if (!pg_vec_lower_expr(left,
							   source_plan,
							   ctx,
							   plan,
							   &filter->exprs,
							   &left_root))
			return false;

		MemSet(&rhs_node, 0, sizeof(rhs_node));
		rhs_node.kind = PG_VEC_EXPR_CONST;
		rhs_node.scalar_kind = PG_VEC_SCALAR_STRING128;
		rhs_node.left = -1;
		rhs_node.right = -1;
		rhs_node.constant.string128 = prefix_value;
		if (!pg_vec_add_expr_node(&filter->exprs, &rhs_node, &right_root))
			return false;

		if (filter->exprs.nodes[left_root].scalar_kind != PG_VEC_SCALAR_STRING128)
			return false;

		MemSet(&qual_node, 0, sizeof(qual_node));
		qual_node.kind = PG_VEC_QUAL_COMPARE;
		qual_node.left = -1;
		qual_node.right = -1;
		qual_node.lhs_expr = left_root;
		qual_node.rhs_expr = right_root;
		qual_node.op = PG_VEC_OP_PREFIX_LIKE;
		return pg_vec_add_qual_node(filter, &qual_node, qual_root);
	}

	if (strcmp(op_name, "=") == 0)
		filter_op = PG_VEC_OP_EQ;
	else if (strcmp(op_name, "<") == 0)
		filter_op = PG_VEC_OP_LT;
	else if (strcmp(op_name, "<=") == 0)
		filter_op = PG_VEC_OP_LE;
	else if (strcmp(op_name, ">") == 0)
		filter_op = PG_VEC_OP_GT;
	else if (strcmp(op_name, ">=") == 0)
		filter_op = PG_VEC_OP_GE;
	else
		return false;

	if (!pg_vec_lower_compare_operands(left,
									   right,
									   source_plan,
									   ctx,
									   plan,
									   filter,
									   &left_root,
									   &right_root))
		return false;

	if (filter->exprs.nodes[left_root].scalar_kind !=
		filter->exprs.nodes[right_root].scalar_kind)
		return false;

	MemSet(&qual_node, 0, sizeof(qual_node));
	qual_node.kind = PG_VEC_QUAL_COMPARE;
	qual_node.left = -1;
	qual_node.right = -1;
	qual_node.lhs_expr = left_root;
	qual_node.rhs_expr = right_root;
	qual_node.op = filter_op;
	return pg_vec_add_qual_node(filter, &qual_node, qual_root);
}

static bool
pg_vec_lower_filter_quals(List *quals,
						  Plan *source_plan,
						  const PgVecLowerContext *ctx,
						  PgVecPlan *plan,
						  PgVecFilterSpec *filter)
{
	ListCell   *lc;
	int			root = -1;

	pg_vec_filter_spec_init(filter);

	foreach(lc, quals)
	{
		int			qual_root;

		if (!pg_vec_lower_qual(lfirst(lc), source_plan, ctx, plan, filter, &qual_root))
			return false;

		if (root < 0)
			root = qual_root;
		else if (!pg_vec_add_binary_qual(filter,
										 PG_VEC_QUAL_AND,
										 root,
										 qual_root,
										 &root))
			return false;
	}

	filter->root = root;
	return true;
}

static bool
pg_vec_agg_kind_from_aggref(Aggref *aggref, PgVecAggKind *agg_kind)
{
	char	   *func_name;

	func_name = get_func_name(aggref->aggfnoid);
	if (func_name == NULL)
		return false;
	if (strcmp(func_name, "sum") == 0)
		*agg_kind = PG_VEC_AGG_SUM;
	else if (strcmp(func_name, "avg") == 0)
		*agg_kind = PG_VEC_AGG_AVG;
	else if (strcmp(func_name, "count") == 0)
		*agg_kind = PG_VEC_AGG_COUNT;
	else if (strcmp(func_name, "min") == 0)
		*agg_kind = PG_VEC_AGG_MIN;
	else if (strcmp(func_name, "max") == 0)
		*agg_kind = PG_VEC_AGG_MAX;
	else
		return false;

	return true;
}

static bool
pg_vec_const_is_zero(Node *node)
{
	Const	   *constnode;
	int64		scaled;

	node = pg_vec_strip_implicit_casts(node);
	if (!IsA(node, Const))
		return false;

	constnode = castNode(Const, node);
	if (constnode->constisnull)
		return false;

	if (constnode->consttype == INT4OID)
		return DatumGetInt32(constnode->constvalue) == 0;
	if (constnode->consttype == NUMERICOID &&
		pg_vec_numeric_to_scaled_int64(constnode->constvalue, 2, &scaled))
		return scaled == 0;

	return false;
}

static bool
pg_vec_try_lower_conditional_agg(Aggref *aggref,
								 Plan *source_plan,
								 const PgVecLowerContext *ctx,
								 PgVecPlan *plan,
								 PgVecAggCall *agg_call)
{
	TargetEntry *arg_tle;
	CaseExpr   *case_expr;
	CaseWhen   *case_when;

	if (agg_call->kind != PG_VEC_AGG_SUM)
		return false;
	if (list_length(aggref->args) != 1)
		return false;

	arg_tle = linitial_node(TargetEntry, aggref->args);
	if (arg_tle->resjunk)
		return false;

	if (!IsA(pg_vec_strip_implicit_casts(arg_tle->expr), CaseExpr))
		return false;

	case_expr = castNode(CaseExpr, pg_vec_strip_implicit_casts(arg_tle->expr));
	if (list_length(case_expr->args) != 1)
		return false;
	if (!pg_vec_const_is_zero(case_expr->defresult))
		return false;

	case_when = linitial_node(CaseWhen, case_expr->args);
	if (!pg_vec_lower_expr(case_when->result,
						   source_plan,
						   ctx,
						   plan,
						   &agg_call->expr,
						   &agg_call->expr.root))
		return false;
	if (!pg_vec_lower_filter_quals(list_make1(case_when->expr),
								   source_plan,
								   ctx,
								   plan,
								   &agg_call->filter))
		return false;

	agg_call->has_filter = true;
	agg_call->zero_if_empty = true;
	return true;
}

static bool
pg_vec_lower_agg_call(Aggref *aggref,
					  Plan *source_plan,
					  const PgVecLowerContext *ctx,
					  PgVecPlan *plan,
					  int *agg_idx)
{
	PgVecAggCall agg_call;
	TargetEntry *arg_tle;

	if (aggref->aggdistinct != NIL ||
		aggref->aggorder != NIL ||
		aggref->aggfilter != NULL)
		return false;

	MemSet(&agg_call, 0, sizeof(agg_call));
	pg_vec_expr_program_init(&agg_call.expr);
	pg_vec_filter_spec_init(&agg_call.filter);
	if (!pg_vec_agg_kind_from_aggref(aggref, &agg_call.kind))
		return false;

	if (aggref->aggstar)
	{
		if (agg_call.kind != PG_VEC_AGG_COUNT)
			return false;
		agg_call.star_arg = true;
		agg_call.expr.root = -1;
	}
	else if (!pg_vec_try_lower_conditional_agg(aggref,
												 source_plan,
												 ctx,
												 plan,
												 &agg_call))
	{
		if (list_length(aggref->args) != 1)
			return false;

		arg_tle = linitial_node(TargetEntry, aggref->args);
		if (arg_tle->resjunk)
			return false;

		if (!pg_vec_lower_expr(arg_tle->expr,
							   source_plan,
							   ctx,
							   plan,
							   &agg_call.expr,
							   &agg_call.expr.root))
			return false;
	}

	return pg_vec_add_agg_call(&plan->agg, &agg_call, agg_idx);
}

static bool
pg_vec_lower_output_expr(Node *node,
						 Plan *source_plan,
						 const PgVecLowerContext *ctx,
						 PgVecPlan *plan,
						 PgVecOutputExprProgram *program,
						 int *expr_root)
{
	PgVecOutputExprNode expr_node;
	Node	   *resolved_node;
	OpExpr	   *opexpr;
	char	   *op_name;
	int			left_root;
	int			right_root;
	PgVecScalarKind left_kind;
	PgVecScalarKind right_kind;
	PgVecScalarKind result_kind;
	PgVecColumnRef group_key;
	Aggref	   *aggref;
	int			ref_idx;
	Const	   *constnode;

	resolved_node = pg_vec_resolve_var_through_plan(node, source_plan);
	resolved_node = pg_vec_strip_implicit_casts(resolved_node);
	if (resolved_node == NULL)
		return false;

	if (IsA(resolved_node, Var))
	{
		if (!pg_vec_lower_var(castNode(Var, resolved_node), ctx, plan, &group_key))
			return false;
		if (!pg_vec_add_group_key(&plan->agg, group_key, &ref_idx))
			return false;

		MemSet(&expr_node, 0, sizeof(expr_node));
		expr_node.kind = PG_VEC_OUTPUT_EXPR_GROUP_KEY;
		expr_node.scalar_kind = group_key.scalar_kind;
		expr_node.index = ref_idx;
		expr_node.left = -1;
		expr_node.right = -1;
		return pg_vec_add_output_expr_node(program, &expr_node, expr_root);
	}

	if (IsA(resolved_node, Aggref))
	{
		aggref = castNode(Aggref, resolved_node);
		if (!pg_vec_lower_agg_call(aggref, source_plan, ctx, plan, &ref_idx))
			return false;

		MemSet(&expr_node, 0, sizeof(expr_node));
		expr_node.kind = PG_VEC_OUTPUT_EXPR_AGGREF;
		if (plan->agg.aggs[ref_idx].star_arg)
			expr_node.scalar_kind = PG_VEC_SCALAR_INT32;
		else
			expr_node.scalar_kind =
				plan->agg.aggs[ref_idx].expr.nodes[plan->agg.aggs[ref_idx].expr.root].scalar_kind;
		expr_node.index = ref_idx;
		expr_node.left = -1;
		expr_node.right = -1;
		return pg_vec_add_output_expr_node(program, &expr_node, expr_root);
	}

	if (IsA(resolved_node, Const))
	{
		constnode = castNode(Const, resolved_node);
		if (!pg_vec_scalar_kind_from_pg_type(constnode->consttype,
											 constnode->consttypmod,
											 &result_kind))
			return false;

		MemSet(&expr_node, 0, sizeof(expr_node));
		expr_node.kind = PG_VEC_OUTPUT_EXPR_CONST;
		expr_node.scalar_kind = result_kind;
		expr_node.left = -1;
		expr_node.right = -1;
		if (!pg_vec_lower_const_value(constnode, result_kind, &expr_node.constant))
			return false;
		return pg_vec_add_output_expr_node(program, &expr_node, expr_root);
	}

	if (!IsA(resolved_node, OpExpr))
		return false;

	opexpr = castNode(OpExpr, resolved_node);
	if (list_length(opexpr->args) != 2)
		return false;

	op_name = get_opname(opexpr->opno);
	if (op_name == NULL)
		return false;

	if (!pg_vec_lower_output_expr(linitial(opexpr->args),
								  source_plan,
								  ctx,
								  plan,
								  program,
								  &left_root) ||
		!pg_vec_lower_output_expr(lsecond(opexpr->args),
								  source_plan,
								  ctx,
								  plan,
								  program,
								  &right_root))
		return false;

	left_kind = program->nodes[left_root].scalar_kind;
	right_kind = program->nodes[right_root].scalar_kind;

	MemSet(&expr_node, 0, sizeof(expr_node));
	if (strcmp(op_name, "+") == 0)
		expr_node.kind = PG_VEC_OUTPUT_EXPR_ADD;
	else if (strcmp(op_name, "-") == 0)
		expr_node.kind = PG_VEC_OUTPUT_EXPR_SUB;
	else if (strcmp(op_name, "*") == 0)
		expr_node.kind = PG_VEC_OUTPUT_EXPR_MUL;
	else if (strcmp(op_name, "/") == 0)
		expr_node.kind = PG_VEC_OUTPUT_EXPR_DIV;
	else
		return false;

	if (!pg_vec_resolve_binary_output_kind(expr_node.kind,
										   left_kind,
										   right_kind,
										   &result_kind))
		return false;

	expr_node.scalar_kind = result_kind;
	expr_node.left = left_root;
	expr_node.right = right_root;
	return pg_vec_add_output_expr_node(program, &expr_node, expr_root);
}

static bool
pg_vec_lower_agg_targetlist(QueryDesc *queryDesc,
							List *targetlist,
							Plan *source_plan,
							const PgVecLowerContext *ctx,
							PgVecPlan *plan)
{
	ListCell   *lc;

	if (queryDesc->tupDesc == NULL)
		return false;

	foreach(lc, targetlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		PgVecOutputExprProgram *output_expr;

		if (tle->resjunk)
			continue;
		if (plan->agg.noutputs >= PG_VEC_MAX_OUTPUT_COLUMNS)
			return false;

		output_expr = &plan->agg.outputs[plan->agg.noutputs];
		pg_vec_output_expr_program_init(output_expr);
		if (!pg_vec_lower_output_expr(tle->expr,
									  source_plan,
									  ctx,
									  plan,
									  output_expr,
									  &output_expr->root))
			return false;

		plan->agg.noutputs++;
	}

	return plan->agg.naggs > 0 && plan->agg.noutputs > 0;
}

static bool
pg_vec_make_single_input_context(SeqScan *seqscan,
								 PlannedStmt *plannedstmt,
								 PgVecLowerContext *ctx)
{
	ctx->ninputs = 1;
	ctx->inputs[0].rtindex = seqscan->scan.scanrelid;
	ctx->inputs[0].input_id = 0;
	ctx->inputs[0].relid = rt_fetch(seqscan->scan.scanrelid, plannedstmt->rtable)->relid;
	return true;
}

static bool
pg_vec_make_join_input_context(SeqScan *left_scan,
							   SeqScan *right_scan,
							   PlannedStmt *plannedstmt,
							   PgVecLowerContext *ctx)
{
	ctx->ninputs = 2;

	ctx->inputs[0].rtindex = left_scan->scan.scanrelid;
	ctx->inputs[0].input_id = 0;
	ctx->inputs[0].relid = rt_fetch(left_scan->scan.scanrelid, plannedstmt->rtable)->relid;

	ctx->inputs[1].rtindex = right_scan->scan.scanrelid;
	ctx->inputs[1].input_id = 1;
	ctx->inputs[1].relid = rt_fetch(right_scan->scan.scanrelid, plannedstmt->rtable)->relid;
	return true;
}

static bool
pg_vec_lower_join_key_expr(Node *node,
						   Plan *join_plan,
						   const PgVecLowerContext *ctx,
						   PgVecPlan *plan,
						   PgVecColumnRef *column_ref)
{
	Node	   *resolved = pg_vec_resolve_var_through_plan(node, join_plan);

	resolved = pg_vec_strip_implicit_casts(resolved);
	if (!IsA(resolved, Var))
		return false;

	return pg_vec_lower_var(castNode(Var, resolved), ctx, plan, column_ref);
}

static bool
pg_vec_lower_join_keys_from_list(List *clauses,
								 Plan *join_plan,
								 const PgVecLowerContext *ctx,
								 PgVecPlan *plan)
{
	ListCell   *lc;

	foreach(lc, clauses)
	{
		OpExpr	   *opexpr;
		Node	   *left;
		Node	   *right;
		char	   *op_name;
		PgVecColumnRef left_ref;
		PgVecColumnRef right_ref;
		PgVecJoinKey key;

		if (!IsA(lfirst(lc), OpExpr))
			return false;

		opexpr = castNode(OpExpr, lfirst(lc));
		if (list_length(opexpr->args) != 2)
			return false;
		op_name = get_opname(opexpr->opno);
		if (op_name == NULL || strcmp(op_name, "=") != 0)
			return false;

		left = linitial(opexpr->args);
		right = lsecond(opexpr->args);
		if (!pg_vec_lower_join_key_expr(left, join_plan, ctx, plan, &left_ref) ||
			!pg_vec_lower_join_key_expr(right, join_plan, ctx, plan, &right_ref))
			return false;
		if (left_ref.input_id == right_ref.input_id)
			return false;
		if (left_ref.scalar_kind != PG_VEC_SCALAR_INT32 ||
			right_ref.scalar_kind != PG_VEC_SCALAR_INT32)
			return false;

		if (left_ref.input_id != plan->join.left_input)
		{
			PgVecColumnRef tmp = left_ref;

			left_ref = right_ref;
			right_ref = tmp;
		}

		if (plan->join.nkeys >= PG_VEC_MAX_JOIN_KEYS)
			return false;

		key.left = left_ref;
		key.right = right_ref;
		plan->join.keys[plan->join.nkeys++] = key;
	}

	return plan->join.nkeys > 0;
}

static bool
pg_vec_lower_join_spec(Plan *join_plan,
					   const PgVecLowerContext *ctx,
					   PgVecPlan *plan)
{
	List	   *clauses = NIL;

	plan->join.enabled = true;
	plan->join.kind = PG_VEC_JOIN_INNER;
	plan->join.left_input = 0;
	plan->join.right_input = 1;
	plan->join.nkeys = 0;

	if (IsA(join_plan, HashJoin))
		clauses = castNode(HashJoin, join_plan)->hashclauses;
	else if (IsA(join_plan, MergeJoin))
		clauses = castNode(MergeJoin, join_plan)->mergeclauses;
	else if (IsA(join_plan, NestLoop))
		clauses = join_plan->qual;
	else
		return false;

	return pg_vec_lower_join_keys_from_list(clauses, join_plan, ctx, plan);
}

static bool
pg_vec_try_translate_scan_filter_agg_plan(QueryDesc *queryDesc, PgVecPlan *plan)
{
	PlannedStmt *plannedstmt;
	Plan	   *plantree;
	Agg		   *agg;
	SeqScan    *seqscan;
	RangeTblEntry *rte;
	PgVecLowerContext ctx;

	plannedstmt = queryDesc->plannedstmt;
	plantree = plannedstmt->planTree;

	if (IsA(plantree, Sort))
	{
		Sort	   *sort = castNode(Sort, plantree);

		if (sort->plan.righttree != NULL || !IsA(sort->plan.lefttree, Agg))
			return false;
		plantree = sort->plan.lefttree;
	}

	if (!IsA(plantree, Agg))
		return false;

	agg = castNode(Agg, plantree);
	if (agg->plan.righttree != NULL || !IsA(agg->plan.lefttree, SeqScan))
		return false;

	seqscan = castNode(SeqScan, agg->plan.lefttree);
	rte = rt_fetch(seqscan->scan.scanrelid, plannedstmt->rtable);
	if (rte == NULL || rte->rtekind != RTE_RELATION)
		return false;

	plan->kind = PG_VEC_PLAN_SCAN_FILTER_AGG;
	plan->ninputs = 1;
	plan->inputs[0].relid = rte->relid;
	plan->agg.grouped = (agg->numCols > 0);

	if (!pg_vec_make_single_input_context(seqscan, plannedstmt, &ctx))
		return false;
	if (!pg_vec_lower_agg_targetlist(queryDesc,
									 agg->plan.targetlist,
									 agg->plan.lefttree,
									 &ctx,
									 plan))
		return false;
	if (!pg_vec_lower_filter_quals(seqscan->scan.plan.qual,
								   &seqscan->scan.plan,
								   &ctx,
								   plan,
								   &plan->inputs[0].filter))
		return false;

	return true;
}

static bool
pg_vec_try_translate_join_filter_agg_plan(QueryDesc *queryDesc, PgVecPlan *plan)
{
	PlannedStmt *plannedstmt;
	Plan	   *plantree;
	Agg		   *agg;
	Plan	   *join_plan;
	SeqScan    *left_scan;
	SeqScan    *right_scan;
	RangeTblEntry *left_rte;
	RangeTblEntry *right_rte;
	PgVecLowerContext ctx;

	plannedstmt = queryDesc->plannedstmt;
	plantree = plannedstmt->planTree;

	if (IsA(plantree, Sort))
	{
		Sort	   *sort = castNode(Sort, plantree);

		if (sort->plan.righttree != NULL || !IsA(sort->plan.lefttree, Agg))
			return false;
		plantree = sort->plan.lefttree;
	}

	if (!IsA(plantree, Agg))
		return false;

	agg = castNode(Agg, plantree);
	join_plan = agg->plan.lefttree;
	if (join_plan == NULL ||
		(!IsA(join_plan, HashJoin) &&
		 !IsA(join_plan, MergeJoin) &&
		 !IsA(join_plan, NestLoop)))
		return false;

	if (join_plan->lefttree == NULL || join_plan->righttree == NULL ||
		!IsA(join_plan->lefttree, SeqScan) ||
		!IsA(join_plan->righttree, SeqScan))
		return false;

	left_scan = castNode(SeqScan, join_plan->lefttree);
	right_scan = castNode(SeqScan, join_plan->righttree);
	left_rte = rt_fetch(left_scan->scan.scanrelid, plannedstmt->rtable);
	right_rte = rt_fetch(right_scan->scan.scanrelid, plannedstmt->rtable);
	if (left_rte == NULL || right_rte == NULL ||
		left_rte->rtekind != RTE_RELATION ||
		right_rte->rtekind != RTE_RELATION)
		return false;

	plan->kind = PG_VEC_PLAN_SCAN_FILTER_AGG;
	plan->ninputs = 2;
	plan->inputs[0].relid = left_rte->relid;
	plan->inputs[1].relid = right_rte->relid;
	plan->agg.grouped = (agg->numCols > 0);

	if (!pg_vec_make_join_input_context(left_scan, right_scan, plannedstmt, &ctx))
		return false;
	if (!pg_vec_lower_join_spec(join_plan, &ctx, plan))
		return false;
	if (!pg_vec_lower_filter_quals(left_scan->scan.plan.qual,
								   &left_scan->scan.plan,
								   &ctx,
								   plan,
								   &plan->inputs[0].filter))
		return false;
	if (!pg_vec_lower_filter_quals(right_scan->scan.plan.qual,
								   &right_scan->scan.plan,
								   &ctx,
								   plan,
								   &plan->inputs[1].filter))
		return false;
	if (!pg_vec_lower_agg_targetlist(queryDesc,
									 agg->plan.targetlist,
									 join_plan,
									 &ctx,
									 plan))
		return false;

	return true;
}

static bool
pg_vec_numeric_to_scaled_int64(Datum value, int scale, int64 *out)
{
	char	   *str;
	bool		ok;

	str = DatumGetCString(DirectFunctionCall1(numeric_out, value));
	ok = pg_vec_parse_scaled_int64(str, scale, out);
	pfree(str);

	return ok;
}

static bool
pg_vec_parse_scaled_int64(const char *str, int scale, int64 *out)
{
	const char *ptr = str;
	bool		negative = false;
	int64		int_part = 0;
	int64		frac_part = 0;
	int			frac_digits = 0;
	int64		scale_factor = 1;

	for (int i = 0; i < scale; i++)
		scale_factor *= 10;

	while (*ptr != '\0' && isspace((unsigned char) *ptr))
		ptr++;

	if (*ptr == '-')
	{
		negative = true;
		ptr++;
	}
	else if (*ptr == '+')
		ptr++;

	if (!isdigit((unsigned char) *ptr))
		return false;

	while (isdigit((unsigned char) *ptr))
	{
		int_part = int_part * 10 + (*ptr - '0');
		ptr++;
	}

	if (*ptr == '.')
	{
		ptr++;
		while (isdigit((unsigned char) *ptr))
		{
			if (frac_digits < scale)
			{
				frac_part = frac_part * 10 + (*ptr - '0');
				frac_digits++;
			}
			else if (*ptr != '0')
				return false;
			ptr++;
		}
	}

	while (frac_digits < scale)
	{
		frac_part *= 10;
		frac_digits++;
	}

	while (*ptr != '\0' && isspace((unsigned char) *ptr))
		ptr++;

	if (*ptr != '\0')
		return false;

	*out = int_part * scale_factor + frac_part;
	if (negative)
		*out = -*out;

	return true;
}
