#include "postgres.h"

#include <ctype.h>

#include "executor/executor.h"
#include "executor/nodeSubplan.h"
#include "access/stratnum.h"
#include "catalog/pg_type_d.h"
#include "nodes/makefuncs.h"
#include "nodes/params.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "parser/parsetree.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "varatt.h"

#include "pg_translate.h"

extern Datum numeric_scale(PG_FUNCTION_ARGS);

typedef struct PgVecInputContext {
  Index rtindex;
  Oid relid;
  uint8 input_id;
} PgVecInputContext;

typedef struct PgVecLowerContext {
  QueryDesc *queryDesc;
  int ninputs;
  PgVecInputContext inputs[PG_VEC_MAX_INPUTS];
} PgVecLowerContext;

typedef struct PgVecJoinTreeInfo {
  int first_input;
  int ninputs;
  int njoins;
  PgVecJoinSpec joins[PG_VEC_MAX_JOINS];
} PgVecJoinTreeInfo;

static bool
pg_vec_try_translate_scan_filter_agg_plan(QueryDesc *queryDesc, PgVecPlan *plan,
                                          const char **failure_reason);
static bool
pg_vec_try_translate_join_filter_agg_plan(QueryDesc *queryDesc, PgVecPlan *plan,
                                          const char **failure_reason);
static void pg_vec_plan_init(PgVecPlan *plan);
static void pg_vec_input_spec_init(PgVecInputSpec *input);
static void pg_vec_expr_program_init(PgVecExprProgram *program);
static void pg_vec_filter_spec_init(PgVecFilterSpec *filter);
static void pg_vec_output_expr_program_init(PgVecOutputExprProgram *program);
static bool pg_vec_add_scan_column(PgVecInputSpec *input, uint8 input_id,
                                   AttrNumber attno,
                                   PgVecScalarKind scalar_kind);
static bool pg_vec_add_expr_node(PgVecExprProgram *program,
                                 const PgVecExprNode *node, int *node_idx);
static bool pg_vec_add_qual_node(PgVecFilterSpec *filter,
                                 const PgVecQualNode *node, int *node_idx);
static bool pg_vec_add_binary_qual(PgVecFilterSpec *filter, PgVecQualKind kind,
                                   int left_idx, int right_idx, int *node_idx);
static bool pg_vec_add_output_expr_node(PgVecOutputExprProgram *program,
                                        const PgVecOutputExprNode *node,
                                        int *node_idx);
static bool pg_vec_add_agg_call(PgVecAggSpec *agg, const PgVecAggCall *agg_call,
                                int *agg_idx);
static bool pg_vec_add_group_key(PgVecAggSpec *agg,
                                 const PgVecExprProgram *group_expr,
                                 PgVecScalarKind scalar_kind,
                                 int *group_idx);
static bool pg_vec_scalar_kind_from_pg_type(Oid type_oid, int32 typmod,
                                            PgVecScalarKind *scalar_kind);
static bool pg_vec_lower_const_value(Const *constnode,
                                     PgVecScalarKind scalar_kind,
                                     PgVecConstValue *constant);
static bool pg_vec_scalar_kind_from_const(Const *constnode,
                                          bool has_expected_kind,
                                          PgVecScalarKind expected_kind,
                                          PgVecScalarKind *scalar_kind);
static bool pg_vec_resolve_constlike_node(Node *node,
                                          const PgVecLowerContext *ctx,
                                          Const *runtime_const,
                                          Const **constnode);
static bool pg_vec_resolve_binary_expr_kind(PgVecExprKind expr_kind,
                                            PgVecScalarKind left_kind,
                                            PgVecScalarKind right_kind,
                                            PgVecScalarKind *result_kind);
static bool pg_vec_resolve_binary_output_kind(PgVecOutputExprKind expr_kind,
                                              PgVecScalarKind left_kind,
                                              PgVecScalarKind right_kind,
                                              PgVecScalarKind *result_kind);
static Node *pg_vec_strip_implicit_casts(Node *node);
static TargetEntry *pg_vec_find_tle_by_resno(List *targetlist,
                                             AttrNumber resno);
static Node *pg_vec_resolve_var_through_plan_with_source(Node *node, Plan *plan,
                                                         Plan **resolved_plan);
static Node *pg_vec_resolve_var_through_plan(Node *node, Plan *plan);
static bool pg_vec_lower_var(Var *var, const PgVecLowerContext *ctx,
                             PgVecPlan *plan, PgVecColumnRef *column_ref);
static bool
pg_vec_lower_expr_internal(Node *node, Plan *source_plan,
                           const PgVecLowerContext *ctx, PgVecPlan *plan,
                           PgVecExprProgram *program, bool has_expected_kind,
                           PgVecScalarKind expected_kind, int *expr_root);
static bool pg_vec_try_lower_extract_year_expr(FuncExpr *func,
                                               Plan *source_plan,
                                               const PgVecLowerContext *ctx,
                                               PgVecPlan *plan,
                                               PgVecExprProgram *program,
                                               int *expr_root);
static bool pg_vec_lower_expr(Node *node, Plan *source_plan,
                              const PgVecLowerContext *ctx, PgVecPlan *plan,
                              PgVecExprProgram *program, int *expr_root);
static bool pg_vec_lower_compare_operands(
    Node *left, Node *right, Plan *source_plan, const PgVecLowerContext *ctx,
    PgVecPlan *plan, PgVecFilterSpec *filter, int *left_root, int *right_root);
static bool pg_vec_try_parse_prefix_like(Const *constnode,
                                         PgVecStringConst *prefix);
static bool pg_vec_try_parse_contains_like(Const *constnode,
                                           PgVecStringConst *needle);
static bool pg_vec_filter_op_from_name(const char *op_name,
                                       PgVecFilterOp *filter_op);
static bool pg_vec_lower_scalar_array_qual(ScalarArrayOpExpr *scalar_array,
                                           Plan *source_plan,
                                           const PgVecLowerContext *ctx,
                                           PgVecPlan *plan,
                                           PgVecFilterSpec *filter,
                                           int *qual_root);
static bool pg_vec_lower_qual(Node *node, Plan *source_plan,
                              const PgVecLowerContext *ctx, PgVecPlan *plan,
                              PgVecFilterSpec *filter, int *qual_root);
static bool pg_vec_append_filter_quals(List *quals, Plan *source_plan,
                                       const PgVecLowerContext *ctx,
                                       PgVecPlan *plan, PgVecFilterSpec *filter);
static bool pg_vec_lower_filter_quals(List *quals, Plan *source_plan,
                                      const PgVecLowerContext *ctx,
                                      PgVecPlan *plan, PgVecFilterSpec *filter);
static bool pg_vec_agg_kind_from_aggref(Aggref *aggref, PgVecAggKind *agg_kind);
static bool pg_vec_const_is_zero(Node *node);
static Aggref *pg_vec_resolve_logical_aggref(Aggref *aggref, Plan *source_plan,
                                             Plan **logical_source_plan);
static bool pg_vec_try_lower_conditional_agg(Aggref *aggref, Plan *source_plan,
                                             const PgVecLowerContext *ctx,
                                             PgVecPlan *plan,
                                             PgVecAggCall *agg_call);
static bool pg_vec_lower_agg_call(Aggref *aggref, Plan *source_plan,
                                  const PgVecLowerContext *ctx, PgVecPlan *plan,
                                  int *agg_idx);
static void pg_vec_post_agg_filter_init(PgVecPostAggFilterSpec *filter);
static bool pg_vec_add_post_agg_qual_node(PgVecPostAggFilterSpec *filter,
                                          const PgVecQualNode *node,
                                          int *node_idx);
static bool pg_vec_add_post_agg_binary_qual(PgVecPostAggFilterSpec *filter,
                                            PgVecQualKind kind,
                                            int left_idx,
                                            int right_idx,
                                            int *node_idx);
static bool pg_vec_lower_output_expr_internal(Node *node, Plan *source_plan,
                                              const PgVecLowerContext *ctx,
                                              PgVecPlan *plan,
                                              PgVecOutputExprProgram *program,
                                              bool has_expected_kind,
                                              PgVecScalarKind expected_kind,
                                              int *expr_root);
static bool pg_vec_lower_output_expr(Node *node, Plan *source_plan,
                                     const PgVecLowerContext *ctx,
                                     PgVecPlan *plan,
                                     PgVecOutputExprProgram *program,
                                     int *expr_root);
static bool pg_vec_lower_post_agg_compare_operands(Node *left, Node *right,
                                                   Plan *source_plan,
                                                   const PgVecLowerContext *ctx,
                                                   PgVecPlan *plan,
                                                   PgVecPostAggFilterSpec *filter,
                                                   int *left_root,
                                                   int *right_root);
static bool pg_vec_lower_post_agg_qual(Node *node, Plan *source_plan,
                                       const PgVecLowerContext *ctx,
                                       PgVecPlan *plan,
                                       PgVecPostAggFilterSpec *filter,
                                       int *qual_root);
static bool pg_vec_append_post_agg_filter_quals(List *quals, Plan *source_plan,
                                                const PgVecLowerContext *ctx,
                                                PgVecPlan *plan,
                                                PgVecPostAggFilterSpec *filter);
static bool pg_vec_lower_agg_targetlist(QueryDesc *queryDesc, List *targetlist,
                                        Plan *source_plan,
                                        const PgVecLowerContext *ctx,
                                        PgVecPlan *plan);
static bool pg_vec_make_single_input_context(SeqScan *seqscan,
                                             PlannedStmt *plannedstmt,
                                             PgVecLowerContext *ctx);
static bool pg_vec_add_input_context(SeqScan *seqscan,
                                     PlannedStmt *plannedstmt,
                                     PgVecLowerContext *ctx,
                                     PgVecPlan *plan,
                                     int *input_id);
static bool pg_vec_lower_join_key_expr(Node *node, Plan *join_plan,
                                       const PgVecLowerContext *ctx,
                                       PgVecPlan *plan,
                                       PgVecColumnRef *column_ref);
static bool pg_vec_lower_join_keys_from_list(List *clauses, Plan *join_plan,
                                             const PgVecLowerContext *ctx,
                                             PgVecPlan *plan,
                                             PgVecJoinSpec *join_spec);
static bool pg_vec_lower_join_spec(Plan *join_plan,
                                   PgVecJoinKind join_kind,
                                   const PgVecLowerContext *ctx,
                                   PgVecPlan *plan,
                                   PgVecJoinSpec *join_spec);
static List *pg_vec_join_quals(Plan *join_plan);
static Plan *pg_vec_strip_plan_wrappers(Plan *plan);
static bool pg_vec_top_agg_split_supported(const Agg *agg);
static bool pg_vec_join_tree_agg_elidable(const Agg *agg);
static bool pg_vec_output_index_for_resno(List *targetlist, AttrNumber resno,
                                          int *output_idx);
static bool pg_vec_sort_key_descending(Oid sort_operator, bool *descending);
static bool pg_vec_lower_topn_spec(Limit *limit, Sort *sort, List *targetlist,
                                   PgVecPlan *plan);
static bool pg_vec_lower_grouped_input_sort_spec(const Agg *agg, Sort *sort,
                                                 PgVecPlan *plan);
static bool pg_vec_lower_join_tree(Plan *subplan, PlannedStmt *plannedstmt,
                                   PgVecLowerContext *ctx, PgVecPlan *plan,
                                   PgVecJoinTreeInfo *info,
                                   const char **failure_reason);
static bool pg_vec_numeric_to_scaled_int64(Datum value, int scale, int64 *out);
static bool pg_vec_parse_scaled_int64(const char *str, int scale, int64 *out);
static void pg_vec_set_failure_reason(const char **failure_reason,
                                      const char *reason);

bool pg_vec_try_translate_plan(QueryDesc *queryDesc, int eflags,
                               PgVecPlan *plan, const char **failure_reason) {
  const char *scan_reason = NULL;
  const char *join_reason = NULL;

  pg_vec_plan_init(plan);
  pg_vec_set_failure_reason(failure_reason, NULL);

  if ((eflags & EXEC_FLAG_EXPLAIN_ONLY) != 0) {
    pg_vec_set_failure_reason(
        failure_reason, "EXEC_FLAG_EXPLAIN_ONLY queries are not supported");
    return false;
  }

  if (queryDesc->operation != CMD_SELECT || queryDesc->estate == NULL) {
    pg_vec_set_failure_reason(
        failure_reason,
        "only SELECT queries with an executor state are supported");
    return false;
  }

  if (queryDesc->plannedstmt == NULL ||
      queryDesc->plannedstmt->planTree == NULL) {
    pg_vec_set_failure_reason(failure_reason,
                              "missing PlannedStmt or planTree");
    return false;
  }

  if (pg_vec_try_translate_scan_filter_agg_plan(queryDesc, plan, &scan_reason))
    return true;

  if (pg_vec_try_translate_join_filter_agg_plan(queryDesc, plan, &join_reason))
    return true;

  if (scan_reason != NULL && join_reason != NULL)
    pg_vec_set_failure_reason(
        failure_reason, psprintf("scan path failed: %s; join path failed: %s",
                                 scan_reason, join_reason));
  else if (scan_reason != NULL)
    pg_vec_set_failure_reason(failure_reason, scan_reason);
  else if (join_reason != NULL)
    pg_vec_set_failure_reason(failure_reason, join_reason);
  else
    pg_vec_set_failure_reason(
        failure_reason,
        "query shape is not supported by the current pg_vec translator");

  return false;
}

const char *pg_vec_plan_kind_name(PgVecPlanKind kind) {
  switch (kind) {
  case PG_VEC_PLAN_SCAN_FILTER_AGG:
    return "scan_filter_agg";
  case PG_VEC_PLAN_UNSUPPORTED:
  default:
    return "unsupported";
  }
}

static void pg_vec_set_failure_reason(const char **failure_reason,
                                      const char *reason) {
  if (failure_reason != NULL)
    *failure_reason = reason;
}

static void pg_vec_plan_init(PgVecPlan *plan) {
  MemSet(plan, 0, sizeof(*plan));
  plan->kind = PG_VEC_PLAN_UNSUPPORTED;
  pg_vec_post_agg_filter_init(&plan->agg.having);
  for (int input_id = 0; input_id < PG_VEC_MAX_INPUTS; input_id++)
    pg_vec_input_spec_init(&plan->inputs[input_id]);
}

static void pg_vec_input_spec_init(PgVecInputSpec *input) {
  MemSet(input, 0, sizeof(*input));
  pg_vec_filter_spec_init(&input->filter);
}

static void pg_vec_expr_program_init(PgVecExprProgram *program) {
  MemSet(program, 0, sizeof(*program));
  program->root = -1;
}

static void pg_vec_filter_spec_init(PgVecFilterSpec *filter) {
  MemSet(filter, 0, sizeof(*filter));
  filter->root = -1;
  pg_vec_expr_program_init(&filter->exprs);
}

static void pg_vec_output_expr_program_init(PgVecOutputExprProgram *program) {
  MemSet(program, 0, sizeof(*program));
  program->root = -1;
}

static void
pg_vec_post_agg_filter_init(PgVecPostAggFilterSpec *filter) {
  MemSet(filter, 0, sizeof(*filter));
  filter->root = -1;
  pg_vec_output_expr_program_init(&filter->exprs);
}

static bool pg_vec_add_scan_column(PgVecInputSpec *input, uint8 input_id,
                                   AttrNumber attno,
                                   PgVecScalarKind scalar_kind) {
  int i;

  for (i = 0; i < input->ncolumns; i++) {
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

static bool pg_vec_add_expr_node(PgVecExprProgram *program,
                                 const PgVecExprNode *node, int *node_idx) {
  if (program->nnodes >= PG_VEC_MAX_EXPR_NODES)
    return false;

  program->nodes[program->nnodes] = *node;
  *node_idx = program->nnodes;
  program->nnodes++;
  return true;
}

static bool pg_vec_add_qual_node(PgVecFilterSpec *filter,
                                 const PgVecQualNode *node, int *node_idx) {
  if (filter->nnodes >= PG_VEC_MAX_FILTER_NODES)
    return false;

  filter->nodes[filter->nnodes] = *node;
  *node_idx = filter->nnodes;
  filter->nnodes++;
  return true;
}

static bool pg_vec_add_binary_qual(PgVecFilterSpec *filter, PgVecQualKind kind,
                                   int left_idx, int right_idx, int *node_idx) {
  PgVecQualNode node;

  MemSet(&node, 0, sizeof(node));
  node.kind = kind;
  node.left = left_idx;
  node.right = right_idx;
  node.lhs_expr = -1;
  node.rhs_expr = -1;
  return pg_vec_add_qual_node(filter, &node, node_idx);
}

static bool pg_vec_add_output_expr_node(PgVecOutputExprProgram *program,
                                        const PgVecOutputExprNode *node,
                                        int *node_idx) {
  if (program->nnodes >= PG_VEC_MAX_OUTPUT_EXPR_NODES)
    return false;

  program->nodes[program->nnodes] = *node;
  *node_idx = program->nnodes;
  program->nnodes++;
  return true;
}

static bool pg_vec_add_agg_call(PgVecAggSpec *agg, const PgVecAggCall *agg_call,
                                int *agg_idx) {
  if (agg->naggs >= PG_VEC_MAX_AGG_CALLS)
    return false;

  agg->aggs[agg->naggs] = *agg_call;
  *agg_idx = agg->naggs;
  agg->naggs++;
  return true;
}

static bool
pg_vec_expr_program_equal(const PgVecExprProgram *lhs,
                          const PgVecExprProgram *rhs)
{
  if (lhs->root != rhs->root || lhs->nnodes != rhs->nnodes)
    return false;
  if (lhs->nnodes == 0)
    return true;
  return memcmp(lhs->nodes,
                rhs->nodes,
                sizeof(PgVecExprNode) * lhs->nnodes) == 0;
}

static bool pg_vec_add_group_key(PgVecAggSpec *agg,
                                 const PgVecExprProgram *group_expr,
                                 PgVecScalarKind scalar_kind,
                                 int *group_idx) {
  for (int i = 0; i < agg->ngroup_keys; i++) {
    if (agg->group_keys[i].scalar_kind != scalar_kind)
      continue;
    if (!pg_vec_expr_program_equal(&agg->group_keys[i].expr, group_expr))
      continue;

    *group_idx = i;
    return true;
  }

  if (agg->ngroup_keys >= PG_VEC_MAX_GROUP_KEYS)
    return false;

  agg->group_keys[agg->ngroup_keys].scalar_kind = scalar_kind;
  agg->group_keys[agg->ngroup_keys].expr = *group_expr;
  *group_idx = agg->ngroup_keys;
  agg->ngroup_keys++;
  return true;
}

static bool pg_vec_scalar_kind_from_pg_type(Oid type_oid, int32 typmod,
                                            PgVecScalarKind *scalar_kind) {
  switch (type_oid) {
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
pg_vec_scalar_kind_from_const(Const *constnode, bool has_expected_kind,
                              PgVecScalarKind expected_kind,
                              PgVecScalarKind *scalar_kind) {
  int32 dscale;

  if (constnode == NULL || scalar_kind == NULL || constnode->constisnull)
    return false;
  if (has_expected_kind) {
    *scalar_kind = expected_kind;
    return true;
  }
  if (constnode->consttype != NUMERICOID)
    return pg_vec_scalar_kind_from_pg_type(constnode->consttype,
                                           constnode->consttypmod,
                                           scalar_kind);

  dscale = DatumGetInt32(DirectFunctionCall1(numeric_scale,
                                             constnode->constvalue));
  if (dscale <= 2)
    *scalar_kind = PG_VEC_SCALAR_DECIMAL128_S2;
  else if (dscale <= 4)
    *scalar_kind = PG_VEC_SCALAR_DECIMAL128_S4;
  else if (dscale <= 6)
    *scalar_kind = PG_VEC_SCALAR_DECIMAL128_S6;
  else
    return false;

  return true;
}

static bool pg_vec_lower_const_value(Const *constnode,
                                     PgVecScalarKind scalar_kind,
                                     PgVecConstValue *constant) {
  char *str;
  Size strlen;

  if (constnode == NULL || constnode->constisnull)
    return false;

  switch (scalar_kind) {
  case PG_VEC_SCALAR_INT32:
    if (constnode->consttype != INT4OID)
      return false;
    constant->int32_value = DatumGetInt32(constnode->constvalue);
    return true;

  case PG_VEC_SCALAR_DATE32:
    if (constnode->consttype == DATEOID) {
      constant->date32 = DatumGetDateADT(constnode->constvalue);
      return true;
    }
    if (constnode->consttype == TIMESTAMPOID) {
      constant->date32 = DatumGetDateADT(
          DirectFunctionCall1(timestamp_date, constnode->constvalue));
      return true;
    }
    return false;

  case PG_VEC_SCALAR_DECIMAL64_S2:
  case PG_VEC_SCALAR_DECIMAL128_S2:
    if (constnode->consttype != NUMERICOID)
      return false;
    return pg_vec_numeric_to_scaled_int64(constnode->constvalue, 2,
                                          &constant->decimal64_s2);
  case PG_VEC_SCALAR_DECIMAL128_S4:
    if (constnode->consttype != NUMERICOID)
      return false;
    return pg_vec_numeric_to_scaled_int64(constnode->constvalue, 4,
                                          &constant->decimal64_s2);
  case PG_VEC_SCALAR_DECIMAL128_S6:
    if (constnode->consttype != NUMERICOID)
      return false;
    return pg_vec_numeric_to_scaled_int64(constnode->constvalue, 6,
                                          &constant->decimal64_s2);

  case PG_VEC_SCALAR_CHAR1:
    if (constnode->consttype != BPCHAROID &&
        constnode->consttype != VARCHAROID && constnode->consttype != TEXTOID)
      return false;
    if (VARSIZE_ANY_EXHDR(DatumGetPointer(constnode->constvalue)) < 1)
      return false;
    constant->char1 = *(VARDATA_ANY(DatumGetPointer(constnode->constvalue)));
    return true;

  case PG_VEC_SCALAR_STRING128:
    if (constnode->consttype != BPCHAROID &&
        constnode->consttype != VARCHAROID && constnode->consttype != TEXTOID)
      return false;

    str = VARDATA_ANY(DatumGetPointer(constnode->constvalue));
    strlen = VARSIZE_ANY_EXHDR(DatumGetPointer(constnode->constvalue));
    if (strlen >= PG_VEC_INLINE_STRING_MAX)
      return false;

    constant->string128.len = (uint16)strlen;
    if (strlen > 0)
      memcpy(constant->string128.bytes, str, strlen);
    memset(constant->string128.bytes + strlen, 0,
           PG_VEC_INLINE_STRING_MAX - strlen);
    return true;

  case PG_VEC_SCALAR_INVALID:
  default:
    return false;
  }
}

static bool
pg_vec_resolve_constlike_node(Node *node,
                              const PgVecLowerContext *ctx,
                              Const *runtime_const,
                              Const **constnode)
{
  Param *param;
  EState *estate;
  ExprContext *econtext;
  ParamExecData *prm;

  if (constnode != NULL)
    *constnode = NULL;
  if (node == NULL)
    return false;
  if (IsA(node, Const)) {
    if (constnode != NULL)
      *constnode = castNode(Const, node);
    return true;
  }
  if (!IsA(node, Param))
    return false;

  param = castNode(Param, node);
  if (param->paramkind != PARAM_EXEC || ctx == NULL || ctx->queryDesc == NULL ||
      ctx->queryDesc->estate == NULL || runtime_const == NULL)
    return false;
  if (param->paramid < 0 ||
      param->paramid >= list_length(ctx->queryDesc->plannedstmt->paramExecTypes))
    return false;

  estate = ctx->queryDesc->estate;
  econtext = GetPerTupleExprContext(estate);
  prm = &estate->es_param_exec_vals[param->paramid];
  if (prm->execPlan != NULL)
    ExecSetParamPlan((SubPlanState *)prm->execPlan, econtext);

  MemSet(runtime_const, 0, sizeof(*runtime_const));
  runtime_const->xpr.type = T_Const;
  runtime_const->consttype = param->paramtype;
  runtime_const->consttypmod = param->paramtypmod;
  runtime_const->constcollid = param->paramcollid;
  runtime_const->constisnull = prm->isnull;
  runtime_const->constvalue = prm->value;
  runtime_const->location = -1;

  if (constnode != NULL)
    *constnode = runtime_const;
  return true;
}

static bool pg_vec_resolve_binary_expr_kind(PgVecExprKind expr_kind,
                                            PgVecScalarKind left_kind,
                                            PgVecScalarKind right_kind,
                                            PgVecScalarKind *result_kind) {
  switch (expr_kind) {
  case PG_VEC_EXPR_ADD:
  case PG_VEC_EXPR_SUB:
    if (left_kind != right_kind)
      return false;
    switch (left_kind) {
    case PG_VEC_SCALAR_INT32:
    case PG_VEC_SCALAR_DECIMAL64_S2:
    case PG_VEC_SCALAR_DECIMAL128_S2:
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
    if ((left_kind == PG_VEC_SCALAR_DECIMAL64_S2 &&
         right_kind == PG_VEC_SCALAR_INT32) ||
        (left_kind == PG_VEC_SCALAR_INT32 &&
         right_kind == PG_VEC_SCALAR_DECIMAL64_S2)) {
      *result_kind = PG_VEC_SCALAR_DECIMAL128_S2;
      return true;
    }
    if (left_kind == PG_VEC_SCALAR_DECIMAL64_S2 &&
        right_kind == PG_VEC_SCALAR_DECIMAL64_S2) {
      *result_kind = PG_VEC_SCALAR_DECIMAL128_S4;
      return true;
    }
    if ((left_kind == PG_VEC_SCALAR_DECIMAL128_S4 &&
         right_kind == PG_VEC_SCALAR_DECIMAL64_S2) ||
        (left_kind == PG_VEC_SCALAR_DECIMAL64_S2 &&
         right_kind == PG_VEC_SCALAR_DECIMAL128_S4)) {
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

static bool pg_vec_resolve_binary_output_kind(PgVecOutputExprKind expr_kind,
                                              PgVecScalarKind left_kind,
                                              PgVecScalarKind right_kind,
                                              PgVecScalarKind *result_kind) {
  if (left_kind != right_kind &&
      !(left_kind == PG_VEC_SCALAR_DECIMAL128_S2 &&
        right_kind == PG_VEC_SCALAR_DECIMAL128_S4) &&
      !(left_kind == PG_VEC_SCALAR_DECIMAL128_S4 &&
        right_kind == PG_VEC_SCALAR_DECIMAL128_S2) &&
      !(left_kind == PG_VEC_SCALAR_DECIMAL128_S2 &&
        right_kind == PG_VEC_SCALAR_DECIMAL128_S6) &&
      !(left_kind == PG_VEC_SCALAR_DECIMAL128_S6 &&
        right_kind == PG_VEC_SCALAR_DECIMAL128_S2) &&
      !(left_kind == PG_VEC_SCALAR_DECIMAL64_S2 &&
        right_kind == PG_VEC_SCALAR_DECIMAL128_S4) &&
      !(left_kind == PG_VEC_SCALAR_DECIMAL128_S4 &&
        right_kind == PG_VEC_SCALAR_DECIMAL64_S2) &&
      !(left_kind == PG_VEC_SCALAR_DECIMAL128_S6 &&
        right_kind == PG_VEC_SCALAR_DECIMAL128_S4) &&
      !(left_kind == PG_VEC_SCALAR_DECIMAL128_S4 &&
        right_kind == PG_VEC_SCALAR_DECIMAL128_S6))
    return false;

  switch (expr_kind) {
  case PG_VEC_OUTPUT_EXPR_ADD:
  case PG_VEC_OUTPUT_EXPR_SUB:
    if (left_kind == right_kind)
      *result_kind = left_kind;
    else if ((left_kind == PG_VEC_SCALAR_DECIMAL128_S2 &&
              right_kind == PG_VEC_SCALAR_DECIMAL128_S4) ||
             (left_kind == PG_VEC_SCALAR_DECIMAL128_S4 &&
              right_kind == PG_VEC_SCALAR_DECIMAL128_S2))
      *result_kind = PG_VEC_SCALAR_DECIMAL128_S4;
    else
      *result_kind = PG_VEC_SCALAR_DECIMAL128_S6;
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

static Node *pg_vec_strip_implicit_casts(Node *node) {
  while (node != NULL) {
    if (IsA(node, FuncExpr)) {
      FuncExpr *func = castNode(FuncExpr, node);
      const char *func_name = get_func_name(func->funcid);

      if (func->funcformat == COERCE_IMPLICIT_CAST &&
          list_length(func->args) == 1)
        node = linitial(func->args);
      else if (list_length(func->args) == 1 &&
               func->funcresulttype == NUMERICOID &&
               func_name != NULL &&
               (strcmp(func_name, "int2_numeric") == 0 ||
                strcmp(func_name, "int4_numeric") == 0 ||
                strcmp(func_name, "int8_numeric") == 0))
        node = linitial(func->args);
      else
        break;
    } else if (IsA(node, RelabelType))
      node = (Node *)castNode(RelabelType, node)->arg;
    else if (IsA(node, CoerceViaIO))
      node = (Node *)castNode(CoerceViaIO, node)->arg;
    else if (IsA(node, CoerceToDomain))
      node = (Node *)castNode(CoerceToDomain, node)->arg;
    else
      break;
  }

  return node;
}

static TargetEntry *pg_vec_find_tle_by_resno(List *targetlist,
                                             AttrNumber resno) {
  ListCell *lc;

  foreach (lc, targetlist) {
    TargetEntry *tle = lfirst_node(TargetEntry, lc);

    if (tle->resjunk)
      continue;
    if (tle->resno == resno)
      return tle;
  }

  return NULL;
}

static Node *pg_vec_resolve_var_through_plan_with_source(Node *node, Plan *plan,
                                                         Plan **resolved_plan) {
  Var *var;
  Plan *source_plan;
  TargetEntry *tle;

  node = pg_vec_strip_implicit_casts(node);
  if (node == NULL || !IsA(node, Var)) {
    if (resolved_plan != NULL)
      *resolved_plan = plan;
    return node;
  }

  var = castNode(Var, node);
  if (var->varno != OUTER_VAR && var->varno != INNER_VAR) {
    if (resolved_plan != NULL)
      *resolved_plan = plan;
    return node;
  }

  if (plan == NULL) {
    if (resolved_plan != NULL)
      *resolved_plan = NULL;
    return node;
  }

  if (var->varno == OUTER_VAR)
    source_plan = plan->lefttree;
  else
    source_plan = plan->righttree;

  if (source_plan == NULL) {
    if (resolved_plan != NULL)
      *resolved_plan = NULL;
    return node;
  }

  while (source_plan != NULL &&
         source_plan->targetlist == NIL &&
         source_plan->lefttree != NULL)
    source_plan = source_plan->lefttree;

  tle = pg_vec_find_tle_by_resno(source_plan->targetlist, var->varattno);
  if (tle == NULL) {
    if (resolved_plan != NULL)
      *resolved_plan = source_plan;
    return node;
  }

  return pg_vec_resolve_var_through_plan_with_source((Node *) tle->expr,
                                                     source_plan,
                                                     resolved_plan);
}

static Node *
pg_vec_resolve_var_through_plan(Node *node, Plan *plan)
{
  return pg_vec_resolve_var_through_plan_with_source(node, plan, NULL);
}

static bool pg_vec_lower_var(Var *var, const PgVecLowerContext *ctx,
                             PgVecPlan *plan, PgVecColumnRef *column_ref) {
  PgVecScalarKind scalar_kind;

  if (var == NULL || var->varattno <= 0)
    return false;

  if (var->varno == OUTER_VAR || var->varno == INNER_VAR)
    return false;

  if (!pg_vec_scalar_kind_from_pg_type(var->vartype, var->vartypmod,
                                       &scalar_kind))
    return false;

  for (int i = 0; i < ctx->ninputs; i++) {
    if (ctx->inputs[i].rtindex != var->varno)
      continue;

    if (!pg_vec_add_scan_column(&plan->inputs[ctx->inputs[i].input_id],
                                ctx->inputs[i].input_id, var->varattno,
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
pg_vec_try_lower_extract_year_expr(FuncExpr *func,
                                   Plan *source_plan,
                                   const PgVecLowerContext *ctx,
                                   PgVecPlan *plan,
                                   PgVecExprProgram *program,
                                   int *expr_root)
{
  const char *func_name;
  Node *field_node;
  Const *field_const;
  char *field_name;
  PgVecExprNode expr_node;
  int arg_root;

  if (func == NULL || list_length(func->args) != 2)
    return false;

  func_name = get_func_name(func->funcid);
  if (func_name == NULL || strcmp(func_name, "extract") != 0)
    return false;

  field_node = pg_vec_strip_implicit_casts(linitial(func->args));
  if (!IsA(field_node, Const))
    return false;
  field_const = castNode(Const, field_node);
  if (field_const->constisnull || field_const->consttype != TEXTOID)
    return false;

  field_name = TextDatumGetCString(field_const->constvalue);
  if (pg_strcasecmp(field_name, "year") != 0)
    return false;

  if (!pg_vec_lower_expr(lsecond(func->args), source_plan, ctx, plan, program,
                         &arg_root))
    return false;
  if (program->nodes[arg_root].scalar_kind != PG_VEC_SCALAR_DATE32)
    return false;

  MemSet(&expr_node, 0, sizeof(expr_node));
  expr_node.kind = PG_VEC_EXPR_EXTRACT_YEAR;
  expr_node.scalar_kind = PG_VEC_SCALAR_INT32;
  expr_node.left = arg_root;
  expr_node.right = -1;
  return pg_vec_add_expr_node(program, &expr_node, expr_root);
}

static bool
pg_vec_lower_expr_internal(Node *node, Plan *source_plan,
                           const PgVecLowerContext *ctx, PgVecPlan *plan,
                           PgVecExprProgram *program, bool has_expected_kind,
                           PgVecScalarKind expected_kind, int *expr_root) {
  PgVecExprNode expr_node;
  FuncExpr *funcexpr;
  OpExpr *opexpr;
  char *op_name;
  Node *left;
  Node *right;
  Plan *expr_source_plan = source_plan;
  int left_root;
  int right_root;
  PgVecScalarKind left_kind;
  PgVecScalarKind right_kind;
  PgVecScalarKind result_kind;
  Const runtime_const;
  Const *constnode;

  node = pg_vec_resolve_var_through_plan_with_source(node, source_plan,
                                                     &expr_source_plan);
  node = pg_vec_strip_implicit_casts(node);
  if (node == NULL)
    return false;

  if (IsA(node, Var)) {
    MemSet(&expr_node, 0, sizeof(expr_node));
    expr_node.kind = PG_VEC_EXPR_COLUMN;
    if (!pg_vec_lower_var(castNode(Var, node), ctx, plan, &expr_node.column))
      return false;
    expr_node.scalar_kind = expr_node.column.scalar_kind;
    expr_node.left = -1;
    expr_node.right = -1;
    return pg_vec_add_expr_node(program, &expr_node, expr_root);
  }

  if (pg_vec_resolve_constlike_node(node, ctx, &runtime_const, &constnode)) {
    if (!pg_vec_scalar_kind_from_const(constnode, has_expected_kind,
                                       expected_kind, &result_kind))
      return false;

    MemSet(&expr_node, 0, sizeof(expr_node));
    expr_node.kind = PG_VEC_EXPR_CONST;
    expr_node.scalar_kind = result_kind;
    expr_node.left = -1;
    expr_node.right = -1;
    if (!pg_vec_lower_const_value(constnode, result_kind, &expr_node.constant))
      return false;
    return pg_vec_add_expr_node(program, &expr_node, expr_root);
  }

  if (IsA(node, FuncExpr)) {
    funcexpr = castNode(FuncExpr, node);
    if (pg_vec_try_lower_extract_year_expr(funcexpr, expr_source_plan, ctx, plan,
                                           program, expr_root))
      return true;
    return false;
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
  if (!pg_vec_lower_expr(left, expr_source_plan, ctx, plan, program, &left_root) ||
      !pg_vec_lower_expr(right, expr_source_plan, ctx, plan, program, &right_root))
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

  if (!pg_vec_resolve_binary_expr_kind(expr_node.kind, left_kind, right_kind,
                                       &result_kind))
    return false;

  expr_node.scalar_kind = result_kind;
  expr_node.left = left_root;
  expr_node.right = right_root;
  return pg_vec_add_expr_node(program, &expr_node, expr_root);
}

static bool pg_vec_lower_expr(Node *node, Plan *source_plan,
                              const PgVecLowerContext *ctx, PgVecPlan *plan,
                              PgVecExprProgram *program, int *expr_root) {
  return pg_vec_lower_expr_internal(node, source_plan, ctx, plan, program,
                                    false, PG_VEC_SCALAR_INVALID, expr_root);
}

static bool pg_vec_lower_compare_operands(
    Node *left, Node *right, Plan *source_plan, const PgVecLowerContext *ctx,
    PgVecPlan *plan, PgVecFilterSpec *filter, int *left_root, int *right_root) {
  Const left_runtime_const;
  Const right_runtime_const;
  Const *left_const = NULL;
  Const *right_const = NULL;

  left = pg_vec_resolve_var_through_plan(left, source_plan);
  right = pg_vec_resolve_var_through_plan(right, source_plan);
  left = pg_vec_strip_implicit_casts(left);
  right = pg_vec_strip_implicit_casts(right);

  if (pg_vec_resolve_constlike_node(left, ctx, &left_runtime_const, &left_const) &&
      !pg_vec_resolve_constlike_node(right, ctx, &right_runtime_const,
                                     &right_const)) {
    PgVecExprProgram tmp_program = filter->exprs;

    if (!pg_vec_lower_expr(right, source_plan, ctx, plan, &filter->exprs,
                           right_root))
      return false;
    if (!pg_vec_lower_expr_internal(
            (Node *) left_const, source_plan, ctx, plan, &filter->exprs, true,
            filter->exprs.nodes[*right_root].scalar_kind, left_root)) {
      filter->exprs = tmp_program;
      return false;
    }
    return true;
  }

  if (!pg_vec_lower_expr(left, source_plan, ctx, plan, &filter->exprs,
                         left_root))
    return false;

  if (pg_vec_resolve_constlike_node(right, ctx, &right_runtime_const, &right_const)) {
    if (!pg_vec_lower_expr_internal(
            (Node *) right_const, source_plan, ctx, plan, &filter->exprs, true,
            filter->exprs.nodes[*left_root].scalar_kind, right_root))
      return false;
  } else if (!pg_vec_lower_expr(right, source_plan, ctx, plan, &filter->exprs,
                                right_root))
    return false;

  return true;
}

static bool pg_vec_try_parse_prefix_like(Const *constnode,
                                         PgVecStringConst *prefix) {
  const char *payload;
  Size payload_size;

  if (constnode == NULL || constnode->constisnull)
    return false;
  if (constnode->consttype != TEXTOID && constnode->consttype != VARCHAROID &&
      constnode->consttype != BPCHAROID)
    return false;

  payload = VARDATA_ANY(DatumGetPointer(constnode->constvalue));
  payload_size = VARSIZE_ANY_EXHDR(DatumGetPointer(constnode->constvalue));
  if (payload_size < 1)
    return false;
  if (payload[payload_size - 1] != '%')
    return false;
  for (Size i = 0; i + 1 < payload_size; i++) {
    if (payload[i] == '%' || payload[i] == '_')
      return false;
  }
  if (payload_size - 1 >= PG_VEC_INLINE_STRING_MAX)
    return false;

  prefix->len = (uint16)(payload_size - 1);
  if (prefix->len > 0)
    memcpy(prefix->bytes, payload, prefix->len);
  memset(prefix->bytes + prefix->len, 0,
         PG_VEC_INLINE_STRING_MAX - prefix->len);
  return true;
}

static bool
pg_vec_try_parse_contains_like(Const *constnode,
                               PgVecStringConst *needle)
{
  const char *payload;
  Size payload_size;

  if (constnode == NULL || constnode->constisnull)
    return false;
  if (constnode->consttype != TEXTOID && constnode->consttype != VARCHAROID &&
      constnode->consttype != BPCHAROID)
    return false;

  payload = VARDATA_ANY(DatumGetPointer(constnode->constvalue));
  payload_size = VARSIZE_ANY_EXHDR(DatumGetPointer(constnode->constvalue));
  if (payload_size < 3)
    return false;
  if (payload[0] != '%' || payload[payload_size - 1] != '%')
    return false;
  for (Size i = 1; i + 1 < payload_size; i++) {
    if (payload[i] == '%' || payload[i] == '_')
      return false;
  }
  if (payload_size - 2 >= PG_VEC_INLINE_STRING_MAX)
    return false;

  needle->len = (uint16) (payload_size - 2);
  if (needle->len > 0)
    memcpy(needle->bytes, payload + 1, needle->len);
  memset(needle->bytes + needle->len, 0,
         PG_VEC_INLINE_STRING_MAX - needle->len);
  return true;
}

static bool
pg_vec_filter_op_from_name(const char *op_name, PgVecFilterOp *filter_op)
{
  if (op_name == NULL)
    return false;

  if (strcmp(op_name, "=") == 0)
    *filter_op = PG_VEC_OP_EQ;
  else if (strcmp(op_name, "<>") == 0 || strcmp(op_name, "!=") == 0)
    *filter_op = PG_VEC_OP_NE;
  else if (strcmp(op_name, "<") == 0)
    *filter_op = PG_VEC_OP_LT;
  else if (strcmp(op_name, "<=") == 0)
    *filter_op = PG_VEC_OP_LE;
  else if (strcmp(op_name, ">") == 0)
    *filter_op = PG_VEC_OP_GT;
  else if (strcmp(op_name, ">=") == 0)
    *filter_op = PG_VEC_OP_GE;
  else
    return false;

  return true;
}

static bool
pg_vec_lower_scalar_array_qual(ScalarArrayOpExpr *scalar_array,
                               Plan *source_plan,
                               const PgVecLowerContext *ctx,
                               PgVecPlan *plan,
                               PgVecFilterSpec *filter,
                               int *qual_root)
{
  const char *op_name;
  PgVecFilterOp filter_op;
  Node *left;
  Node *right;
  Const *array_const;
  ArrayType *array_value;
  Oid element_type;
  int16 typlen;
  bool typbyval;
  char typalign;
  Datum *elements = NULL;
  bool *nulls = NULL;
  int nelems = 0;
  int current_root = -1;

  if (scalar_array == NULL || list_length(scalar_array->args) != 2)
    return false;

  op_name = get_opname(scalar_array->opno);
  if (!pg_vec_filter_op_from_name(op_name, &filter_op))
    return false;

  left = linitial(scalar_array->args);
  right = pg_vec_strip_implicit_casts(lsecond(scalar_array->args));
  if (!IsA(right, Const))
    return false;

  array_const = castNode(Const, right);
  if (array_const->constisnull)
    return false;

  array_value = DatumGetArrayTypeP(array_const->constvalue);
  if (ARR_NDIM(array_value) != 1 || ARR_HASNULL(array_value))
    return false;

  element_type = ARR_ELEMTYPE(array_value);
  get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);
  deconstruct_array(array_value,
                    element_type,
                    typlen,
                    typbyval,
                    typalign,
                    &elements,
                    &nulls,
                    &nelems);
  if (nelems <= 0)
    return false;

  for (int i = 0; i < nelems; i++)
  {
    Const *element_const;
    PgVecQualNode qual_node;
    int left_root;
    int right_root;
    int compare_root;

    if (nulls != NULL && nulls[i])
      return false;

    element_const = makeConst(element_type,
                              -1,
                              InvalidOid,
                              typlen,
                              elements[i],
                              false,
                              typbyval);

    if (!pg_vec_lower_compare_operands(left,
                                       (Node *) element_const,
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
    if (!pg_vec_add_qual_node(filter, &qual_node, &compare_root))
      return false;

    if (current_root < 0)
      current_root = compare_root;
    else if (!pg_vec_add_binary_qual(filter,
                                     scalar_array->useOr ? PG_VEC_QUAL_OR
                                                         : PG_VEC_QUAL_AND,
                                     current_root,
                                     compare_root,
                                     &current_root))
      return false;
  }

  *qual_root = current_root;
  return true;
}

static bool pg_vec_lower_qual(Node *node, Plan *source_plan,
                              const PgVecLowerContext *ctx, PgVecPlan *plan,
                              PgVecFilterSpec *filter, int *qual_root) {
  BoolExpr *bool_expr;
  OpExpr *opexpr;
  PgVecQualNode qual_node;
  Node *left;
  Node *right;
  char *op_name;
  int left_root;
  int right_root;
  PgVecFilterOp filter_op;
  Const *prefix_const;
  PgVecStringConst prefix_value;

  node = pg_vec_resolve_var_through_plan(node, source_plan);
  node = pg_vec_strip_implicit_casts(node);
  if (node == NULL)
    return false;

  if (IsA(node, BoolExpr)) {
    ListCell *lc;
    int current_root = -1;
    PgVecQualKind kind;

    bool_expr = castNode(BoolExpr, node);
    if (bool_expr->boolop == NOT_EXPR)
      return false;
    if (bool_expr->args == NIL)
      return false;

    kind = (bool_expr->boolop == AND_EXPR) ? PG_VEC_QUAL_AND : PG_VEC_QUAL_OR;

    foreach (lc, bool_expr->args) {
      int child_root;

      if (!pg_vec_lower_qual(lfirst(lc), source_plan, ctx, plan, filter,
                             &child_root))
        return false;

      if (current_root < 0)
        current_root = child_root;
      else if (!pg_vec_add_binary_qual(filter, kind, current_root, child_root,
                                       &current_root))
        return false;
    }

    *qual_root = current_root;
    return true;
  }

  if (IsA(node, ScalarArrayOpExpr))
    return pg_vec_lower_scalar_array_qual(castNode(ScalarArrayOpExpr, node),
                                          source_plan,
                                          ctx,
                                          plan,
                                          filter,
                                          qual_root);

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

  if (strcmp(op_name, "~~") == 0) {
    PgVecExprNode rhs_node;
    PgVecFilterOp like_op;

    prefix_const = castNode(Const, pg_vec_strip_implicit_casts(right));
    if (!IsA(pg_vec_strip_implicit_casts(right), Const))
      return false;

    if (pg_vec_try_parse_prefix_like(prefix_const, &prefix_value))
      like_op = PG_VEC_OP_PREFIX_LIKE;
    else if (pg_vec_try_parse_contains_like(prefix_const, &prefix_value))
      like_op = PG_VEC_OP_CONTAINS_LIKE;
    else
      return false;

    if (!pg_vec_lower_expr(left, source_plan, ctx, plan, &filter->exprs,
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
    qual_node.op = like_op;
    return pg_vec_add_qual_node(filter, &qual_node, qual_root);
  }

  if (!pg_vec_filter_op_from_name(op_name, &filter_op))
    return false;

  if (!pg_vec_lower_compare_operands(left, right, source_plan, ctx, plan,
                                     filter, &left_root, &right_root))
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

static bool pg_vec_lower_filter_quals(List *quals, Plan *source_plan,
                                      const PgVecLowerContext *ctx,
                                      PgVecPlan *plan,
                                      PgVecFilterSpec *filter) {
  pg_vec_filter_spec_init(filter);
  return pg_vec_append_filter_quals(quals, source_plan, ctx, plan, filter);
}

static bool pg_vec_append_filter_quals(List *quals, Plan *source_plan,
                                       const PgVecLowerContext *ctx,
                                       PgVecPlan *plan,
                                       PgVecFilterSpec *filter) {
  ListCell *lc;
  int root = filter->root;

  foreach (lc, quals) {
    int qual_root;

    if (!pg_vec_lower_qual(lfirst(lc), source_plan, ctx, plan, filter,
                           &qual_root))
      return false;

    if (root < 0)
      root = qual_root;
    else if (!pg_vec_add_binary_qual(filter, PG_VEC_QUAL_AND, root, qual_root,
                                     &root))
      return false;
  }

  filter->root = root;
  return true;
}

static bool pg_vec_agg_kind_from_aggref(Aggref *aggref,
                                        PgVecAggKind *agg_kind) {
  char *func_name;

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

static bool pg_vec_const_is_zero(Node *node) {
  Const *constnode;
  int64 scaled;

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

static Aggref *
pg_vec_resolve_logical_aggref(Aggref *aggref, Plan *source_plan,
                              Plan **logical_source_plan)
{
  TargetEntry *arg_tle;
  Node *resolved_arg;

  if (aggref == NULL)
    return NULL;

  if (logical_source_plan != NULL)
    *logical_source_plan = source_plan;

  if (aggref->aggsplit != AGGSPLIT_FINAL_DESERIAL)
    return aggref;
  if (list_length(aggref->args) != 1)
    return aggref;

  arg_tle = linitial_node(TargetEntry, aggref->args);
  if (arg_tle->resjunk)
    return aggref;

  resolved_arg = pg_vec_resolve_var_through_plan_with_source((Node *) arg_tle->expr,
                                                             source_plan,
                                                             logical_source_plan);
  resolved_arg = pg_vec_strip_implicit_casts(resolved_arg);
  if (resolved_arg != NULL && IsA(resolved_arg, Aggref))
    return castNode(Aggref, resolved_arg);

  if (logical_source_plan != NULL)
    *logical_source_plan = source_plan;

  return aggref;
}

static bool pg_vec_try_lower_conditional_agg(Aggref *aggref, Plan *source_plan,
                                             const PgVecLowerContext *ctx,
                                             PgVecPlan *plan,
                                             PgVecAggCall *agg_call) {
  TargetEntry *arg_tle;
  CaseExpr *case_expr;
  CaseWhen *case_when;

  if (agg_call->kind != PG_VEC_AGG_SUM)
    return false;
  if (list_length(aggref->args) != 1)
    return false;

  arg_tle = linitial_node(TargetEntry, aggref->args);
  if (arg_tle->resjunk)
    return false;

  if (!IsA(pg_vec_strip_implicit_casts((Node *)arg_tle->expr), CaseExpr))
    return false;

  case_expr = castNode(CaseExpr, pg_vec_strip_implicit_casts((Node *)arg_tle->expr));
  if (list_length(case_expr->args) != 1)
    return false;
  if (!pg_vec_const_is_zero((Node *)case_expr->defresult))
    return false;

  case_when = linitial_node(CaseWhen, case_expr->args);
  if (!pg_vec_lower_expr((Node *)case_when->result, source_plan, ctx, plan,
                         &agg_call->expr, &agg_call->expr.root))
    return false;
  if (!pg_vec_lower_filter_quals(list_make1(case_when->expr), source_plan, ctx,
                                 plan, &agg_call->filter))
    return false;

  agg_call->has_filter = true;
  agg_call->zero_if_empty = true;
  return true;
}

static bool pg_vec_lower_agg_call(Aggref *aggref, Plan *source_plan,
                                  const PgVecLowerContext *ctx, PgVecPlan *plan,
                                  int *agg_idx) {
  PgVecAggCall agg_call;
  Aggref *logical_aggref;
  Plan *logical_source_plan;
  TargetEntry *arg_tle;

  logical_aggref =
      pg_vec_resolve_logical_aggref(aggref, source_plan, &logical_source_plan);
  if (logical_aggref == NULL)
    return false;

  if (logical_aggref->aggdistinct != NIL || logical_aggref->aggorder != NIL ||
      logical_aggref->aggfilter != NULL)
    return false;

  MemSet(&agg_call, 0, sizeof(agg_call));
  pg_vec_expr_program_init(&agg_call.expr);
  pg_vec_filter_spec_init(&agg_call.filter);
  if (!pg_vec_agg_kind_from_aggref(logical_aggref, &agg_call.kind))
    return false;

  if (logical_aggref->aggstar) {
    if (agg_call.kind != PG_VEC_AGG_COUNT)
      return false;
    agg_call.star_arg = true;
    agg_call.expr.root = -1;
  } else if (!pg_vec_try_lower_conditional_agg(logical_aggref,
                                               logical_source_plan,
                                               ctx,
                                               plan,
                                               &agg_call)) {
    if (list_length(logical_aggref->args) != 1)
      return false;

    arg_tle = linitial_node(TargetEntry, logical_aggref->args);
    if (arg_tle->resjunk)
      return false;

    if (!pg_vec_lower_expr((Node *)arg_tle->expr, logical_source_plan, ctx, plan,
                           &agg_call.expr, &agg_call.expr.root))
      return false;
  }

  return pg_vec_add_agg_call(&plan->agg, &agg_call, agg_idx);
}

static bool pg_vec_lower_output_expr_internal(Node *node, Plan *source_plan,
                                              const PgVecLowerContext *ctx,
                                              PgVecPlan *plan,
                                              PgVecOutputExprProgram *program,
                                              bool has_expected_kind,
                                              PgVecScalarKind expected_kind,
                                              int *expr_root) {
  PgVecOutputExprNode expr_node;
  Node *resolved_node;
  OpExpr *opexpr;
  char *op_name;
  int left_root;
  int right_root;
  PgVecScalarKind left_kind;
  PgVecScalarKind right_kind;
  PgVecScalarKind result_kind;
  Aggref *aggref;
  int ref_idx;
  Const runtime_const;
  Const *constnode;
  PgVecExprProgram group_key_expr;
  Plan *resolved_source_plan = source_plan;

  resolved_node = pg_vec_resolve_var_through_plan_with_source(node, source_plan,
                                                              &resolved_source_plan);
  resolved_node = pg_vec_strip_implicit_casts(resolved_node);
  if (resolved_node == NULL)
    return false;

  if (!IsA(resolved_node, Aggref) &&
      !pg_vec_resolve_constlike_node(resolved_node, ctx, &runtime_const, &constnode)) {
    pg_vec_expr_program_init(&group_key_expr);
    if (pg_vec_lower_expr(resolved_node, resolved_source_plan, ctx, plan,
                          &group_key_expr, &group_key_expr.root)) {
      result_kind = group_key_expr.nodes[group_key_expr.root].scalar_kind;
      if (!pg_vec_add_group_key(&plan->agg, &group_key_expr, result_kind,
                                &ref_idx))
        return false;

      MemSet(&expr_node, 0, sizeof(expr_node));
      expr_node.kind = PG_VEC_OUTPUT_EXPR_GROUP_KEY;
      expr_node.scalar_kind = result_kind;
      expr_node.index = ref_idx;
      expr_node.left = -1;
      expr_node.right = -1;
      return pg_vec_add_output_expr_node(program, &expr_node, expr_root);
    }
  }

  if (IsA(resolved_node, Aggref)) {
    aggref = castNode(Aggref, resolved_node);
    if (!pg_vec_lower_agg_call(aggref, resolved_source_plan, ctx, plan, &ref_idx))
      return false;

    MemSet(&expr_node, 0, sizeof(expr_node));
    expr_node.kind = PG_VEC_OUTPUT_EXPR_AGGREF;
    if (plan->agg.aggs[ref_idx].star_arg)
      expr_node.scalar_kind = PG_VEC_SCALAR_INT32;
    else
      expr_node.scalar_kind = plan->agg.aggs[ref_idx]
                                  .expr.nodes[plan->agg.aggs[ref_idx].expr.root]
                                  .scalar_kind;
    expr_node.index = ref_idx;
    expr_node.left = -1;
    expr_node.right = -1;
    return pg_vec_add_output_expr_node(program, &expr_node, expr_root);
  }

  if (pg_vec_resolve_constlike_node(resolved_node, ctx, &runtime_const, &constnode)) {
    if (!pg_vec_scalar_kind_from_const(constnode, has_expected_kind,
                                       expected_kind, &result_kind))
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

  if (!pg_vec_lower_output_expr(linitial(opexpr->args), resolved_source_plan, ctx, plan,
                                program, &left_root) ||
      !pg_vec_lower_output_expr(lsecond(opexpr->args), resolved_source_plan, ctx, plan,
                                program, &right_root))
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

  if (!pg_vec_resolve_binary_output_kind(expr_node.kind, left_kind, right_kind,
                                         &result_kind))
    return false;

  expr_node.scalar_kind = result_kind;
  expr_node.left = left_root;
  expr_node.right = right_root;
  return pg_vec_add_output_expr_node(program, &expr_node, expr_root);
}

static bool
pg_vec_lower_output_expr(Node *node, Plan *source_plan,
                         const PgVecLowerContext *ctx,
                         PgVecPlan *plan,
                         PgVecOutputExprProgram *program,
                         int *expr_root) {
  return pg_vec_lower_output_expr_internal(node, source_plan, ctx, plan,
                                           program, false,
                                           PG_VEC_SCALAR_INVALID, expr_root);
}

static bool
pg_vec_add_post_agg_qual_node(PgVecPostAggFilterSpec *filter,
                              const PgVecQualNode *node, int *node_idx)
{
  if (filter->nnodes >= PG_VEC_MAX_FILTER_NODES)
    return false;

  filter->nodes[filter->nnodes] = *node;
  *node_idx = filter->nnodes++;
  return true;
}

static bool
pg_vec_add_post_agg_binary_qual(PgVecPostAggFilterSpec *filter,
                                PgVecQualKind kind,
                                int left_idx,
                                int right_idx,
                                int *node_idx)
{
  PgVecQualNode qual_node;

  MemSet(&qual_node, 0, sizeof(qual_node));
  qual_node.kind = kind;
  qual_node.left = left_idx;
  qual_node.right = right_idx;
  qual_node.lhs_expr = -1;
  qual_node.rhs_expr = -1;
  qual_node.op = PG_VEC_OP_INVALID;
  return pg_vec_add_post_agg_qual_node(filter, &qual_node, node_idx);
}

static bool
pg_vec_scalar_kinds_numeric_compatible(PgVecScalarKind left_kind,
                                       PgVecScalarKind right_kind)
{
  switch (left_kind) {
    case PG_VEC_SCALAR_DECIMAL64_S2:
    case PG_VEC_SCALAR_DECIMAL128_S2:
    case PG_VEC_SCALAR_DECIMAL128_S4:
    case PG_VEC_SCALAR_DECIMAL128_S6:
      switch (right_kind) {
        case PG_VEC_SCALAR_DECIMAL64_S2:
        case PG_VEC_SCALAR_DECIMAL128_S2:
        case PG_VEC_SCALAR_DECIMAL128_S4:
        case PG_VEC_SCALAR_DECIMAL128_S6:
          return true;
        default:
          return false;
      }
    default:
      return false;
  }
}

static bool
pg_vec_lower_post_agg_compare_operands(Node *left, Node *right,
                                       Plan *source_plan,
                                       const PgVecLowerContext *ctx,
                                       PgVecPlan *plan,
                                       PgVecPostAggFilterSpec *filter,
                                       int *left_root,
                                       int *right_root)
{
  Node *left_resolved = pg_vec_resolve_var_through_plan(left, source_plan);
  Node *right_resolved = pg_vec_resolve_var_through_plan(right, source_plan);
  Const left_runtime_const;
  Const right_runtime_const;
  Const *left_const = NULL;
  Const *right_const = NULL;

  left_resolved = pg_vec_strip_implicit_casts(left_resolved);
  right_resolved = pg_vec_strip_implicit_casts(right_resolved);

  if (pg_vec_resolve_constlike_node(left_resolved, ctx, &left_runtime_const,
                                    &left_const) &&
      !pg_vec_resolve_constlike_node(right_resolved, ctx, &right_runtime_const,
                                     &right_const)) {
    PgVecOutputExprProgram tmp_program = filter->exprs;

    if (!pg_vec_lower_output_expr(right_resolved, source_plan, ctx, plan,
                                  &filter->exprs, right_root))
      return false;
    if (!pg_vec_lower_output_expr_internal((Node *) left_const,
                                           source_plan,
                                           ctx,
                                           plan,
                                           &filter->exprs,
                                           true,
                                           filter->exprs.nodes[*right_root].scalar_kind,
                                           left_root)) {
      filter->exprs = tmp_program;
      if (!pg_vec_lower_output_expr_internal((Node *) left_const,
                                             source_plan,
                                             ctx,
                                             plan,
                                             &filter->exprs,
                                             false,
                                             PG_VEC_SCALAR_INVALID,
                                             left_root))
        return false;
    }
    return filter->exprs.nodes[*left_root].scalar_kind ==
               filter->exprs.nodes[*right_root].scalar_kind ||
           pg_vec_scalar_kinds_numeric_compatible(
               filter->exprs.nodes[*left_root].scalar_kind,
               filter->exprs.nodes[*right_root].scalar_kind);
  }

  if (!pg_vec_lower_output_expr(left_resolved, source_plan, ctx, plan,
                                &filter->exprs, left_root))
    return false;

  if (pg_vec_resolve_constlike_node(right_resolved, ctx, &right_runtime_const,
                                    &right_const)) {
    if (!pg_vec_lower_output_expr_internal((Node *) right_const,
                                           source_plan,
                                           ctx,
                                           plan,
                                           &filter->exprs,
                                           true,
                                           filter->exprs.nodes[*left_root].scalar_kind,
                                           right_root))
    {
      if (!pg_vec_lower_output_expr_internal((Node *) right_const,
                                             source_plan,
                                             ctx,
                                             plan,
                                             &filter->exprs,
                                             false,
                                             PG_VEC_SCALAR_INVALID,
                                             right_root))
        return false;
    }
  } else if (!pg_vec_lower_output_expr(right_resolved, source_plan, ctx, plan,
                                       &filter->exprs, right_root))
    return false;

  return filter->exprs.nodes[*left_root].scalar_kind ==
             filter->exprs.nodes[*right_root].scalar_kind ||
         pg_vec_scalar_kinds_numeric_compatible(
             filter->exprs.nodes[*left_root].scalar_kind,
             filter->exprs.nodes[*right_root].scalar_kind);
}

static bool
pg_vec_lower_post_agg_qual(Node *node, Plan *source_plan,
                           const PgVecLowerContext *ctx,
                           PgVecPlan *plan,
                           PgVecPostAggFilterSpec *filter,
                           int *qual_root)
{
  PgVecQualNode qual_node;
  BoolExpr *bool_expr;
  OpExpr *opexpr;
  char *op_name;
  int left_root;
  int right_root;

  if (node == NULL)
    return false;

  node = pg_vec_strip_implicit_casts(node);
  if (IsA(node, BoolExpr)) {
    ListCell *lc;
    int combined_root = -1;

    bool_expr = castNode(BoolExpr, node);
    if (bool_expr->boolop != AND_EXPR && bool_expr->boolop != OR_EXPR)
      return false;

    foreach (lc, bool_expr->args) {
      int child_root;

      if (!pg_vec_lower_post_agg_qual((Node *) lfirst(lc),
                                      source_plan,
                                      ctx,
                                      plan,
                                      filter,
                                      &child_root))
        return false;
      if (combined_root < 0)
        combined_root = child_root;
      else if (!pg_vec_add_post_agg_binary_qual(filter,
                                                bool_expr->boolop == AND_EXPR
                                                    ? PG_VEC_QUAL_AND
                                                    : PG_VEC_QUAL_OR,
                                                combined_root,
                                                child_root,
                                                &combined_root))
        return false;
    }

    *qual_root = combined_root;
    return combined_root >= 0;
  }

  if (!IsA(node, OpExpr))
    return false;

  opexpr = castNode(OpExpr, node);
  if (list_length(opexpr->args) != 2)
    return false;
  op_name = get_opname(opexpr->opno);
  if (op_name == NULL || !pg_vec_filter_op_from_name(op_name, &qual_node.op))
    return false;

  if (!pg_vec_lower_post_agg_compare_operands(linitial(opexpr->args),
                                              lsecond(opexpr->args),
                                              source_plan,
                                              ctx,
                                              plan,
                                              filter,
                                              &left_root,
                                              &right_root))
    return false;

  MemSet(&qual_node, 0, sizeof(qual_node));
  qual_node.kind = PG_VEC_QUAL_COMPARE;
  qual_node.left = -1;
  qual_node.right = -1;
  qual_node.lhs_expr = left_root;
  qual_node.rhs_expr = right_root;
  if (!pg_vec_filter_op_from_name(op_name, &qual_node.op))
    return false;

  return pg_vec_add_post_agg_qual_node(filter, &qual_node, qual_root);
}

static bool
pg_vec_append_post_agg_filter_quals(List *quals, Plan *source_plan,
                                    const PgVecLowerContext *ctx,
                                    PgVecPlan *plan,
                                    PgVecPostAggFilterSpec *filter)
{
  ListCell *lc;

  foreach (lc, quals) {
    int qual_root;

    if (!pg_vec_lower_post_agg_qual((Node *) lfirst(lc),
                                    source_plan,
                                    ctx,
                                    plan,
                                    filter,
                                    &qual_root))
      return false;
    if (filter->root < 0)
      filter->root = qual_root;
    else if (!pg_vec_add_post_agg_binary_qual(filter,
                                              PG_VEC_QUAL_AND,
                                              filter->root,
                                              qual_root,
                                              &filter->root))
      return false;
  }

  return true;
}

static bool pg_vec_lower_agg_targetlist(QueryDesc *queryDesc, List *targetlist,
                                        Plan *source_plan,
                                        const PgVecLowerContext *ctx,
                                        PgVecPlan *plan) {
  ListCell *lc;

  if (queryDesc->tupDesc == NULL)
    return false;

  foreach (lc, targetlist) {
    TargetEntry *tle = lfirst_node(TargetEntry, lc);
    PgVecOutputExprProgram *output_expr;

    if (tle->resjunk)
      continue;
    if (plan->agg.noutputs >= PG_VEC_MAX_OUTPUT_COLUMNS)
      return false;

    output_expr = &plan->agg.outputs[plan->agg.noutputs];
    pg_vec_output_expr_program_init(output_expr);
    if (!pg_vec_lower_output_expr((Node *)tle->expr, source_plan, ctx, plan,
                                  output_expr, &output_expr->root))
    {
      ereport(WARNING,
              errmsg("pg_vec debug: failed to lower agg output expr: %s",
                     nodeToString(tle->expr)));
      return false;
    }

    plan->agg.noutputs++;
  }

  return plan->agg.naggs > 0 && plan->agg.noutputs > 0;
}

static bool pg_vec_make_single_input_context(SeqScan *seqscan,
                                             PlannedStmt *plannedstmt,
                                             PgVecLowerContext *ctx) {
  ctx->ninputs = 1;
  ctx->inputs[0].rtindex = seqscan->scan.scanrelid;
  ctx->inputs[0].input_id = 0;
  ctx->inputs[0].relid =
      rt_fetch(seqscan->scan.scanrelid, plannedstmt->rtable)->relid;
  return true;
}

static bool pg_vec_add_input_context(SeqScan *seqscan,
                                     PlannedStmt *plannedstmt,
                                     PgVecLowerContext *ctx, PgVecPlan *plan,
                                     int *input_id) {
  RangeTblEntry *rte;

  if (seqscan == NULL || ctx->ninputs >= PG_VEC_MAX_INPUTS)
    return false;

  rte = rt_fetch(seqscan->scan.scanrelid, plannedstmt->rtable);
  if (rte == NULL || rte->rtekind != RTE_RELATION)
    return false;

  *input_id = ctx->ninputs;
  ctx->inputs[*input_id].rtindex = seqscan->scan.scanrelid;
  ctx->inputs[*input_id].input_id = *input_id;
  ctx->inputs[*input_id].relid = rte->relid;
  plan->inputs[*input_id].relid = rte->relid;
  ctx->ninputs++;
  plan->ninputs = ctx->ninputs;
  return true;
}

static bool pg_vec_lower_join_key_expr(Node *node, Plan *join_plan,
                                       const PgVecLowerContext *ctx,
                                       PgVecPlan *plan,
                                       PgVecColumnRef *column_ref) {
  Node *resolved = pg_vec_resolve_var_through_plan(node, join_plan);

  resolved = pg_vec_strip_implicit_casts(resolved);
  if (!IsA(resolved, Var))
    return false;

  return pg_vec_lower_var(castNode(Var, resolved), ctx, plan, column_ref);
}

static bool pg_vec_lower_join_keys_from_list(List *clauses, Plan *join_plan,
                                             const PgVecLowerContext *ctx,
                                             PgVecPlan *plan,
                                             PgVecJoinSpec *join_spec) {
  ListCell *lc;

  foreach (lc, clauses) {
    OpExpr *opexpr;
    Node *left;
    Node *right;
    char *op_name;
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

    if (left_ref.input_id == join_spec->right_input) {
      PgVecColumnRef tmp = left_ref;

      left_ref = right_ref;
      right_ref = tmp;
    }

    if (right_ref.input_id != join_spec->right_input ||
        left_ref.input_id == join_spec->right_input)
      return false;

    if (join_spec->nkeys >= PG_VEC_MAX_JOIN_KEYS)
      return false;

    key.left = left_ref;
    key.right = right_ref;
    join_spec->keys[join_spec->nkeys++] = key;
  }

  return join_spec->nkeys > 0;
}

static bool pg_vec_lower_join_spec(Plan *join_plan,
                                   PgVecJoinKind join_kind,
                                   const PgVecLowerContext *ctx,
                                   PgVecPlan *plan,
                                   PgVecJoinSpec *join_spec) {
  List *clauses = NIL;
  int left_input = join_spec->left_input;
  int right_input = join_spec->right_input;

  MemSet(join_spec, 0, sizeof(*join_spec));
  join_spec->kind = join_kind;
  join_spec->left_input = left_input;
  join_spec->right_input = right_input;
  pg_vec_filter_spec_init(&join_spec->filter);

  if (IsA(join_plan, HashJoin))
    clauses = castNode(HashJoin, join_plan)->hashclauses;
  else if (IsA(join_plan, MergeJoin))
    clauses = castNode(MergeJoin, join_plan)->mergeclauses;
  else if (IsA(join_plan, NestLoop))
    clauses = pg_vec_join_quals(join_plan);
  else
    return false;

  return pg_vec_lower_join_keys_from_list(clauses, join_plan, ctx, plan,
                                          join_spec);
}

static List *
pg_vec_join_quals(Plan *join_plan)
{
  if (join_plan == NULL)
    return NIL;

  if (IsA(join_plan, HashJoin))
    return castNode(HashJoin, join_plan)->join.joinqual;
  if (IsA(join_plan, MergeJoin))
    return castNode(MergeJoin, join_plan)->join.joinqual;
  if (IsA(join_plan, NestLoop))
    return castNode(NestLoop, join_plan)->join.joinqual;

  return NIL;
}

static Plan *
pg_vec_strip_plan_wrappers(Plan *plan)
{
  while (plan != NULL) {
    if (IsA(plan, Hash) || IsA(plan, Material) || IsA(plan, Sort)) {
      plan = plan->lefttree;
      continue;
    }

    if (IsA(plan, Agg) && pg_vec_join_tree_agg_elidable(castNode(Agg, plan))) {
      plan = plan->lefttree;
      continue;
    }

    break;
  }

  return plan;
}

static bool
pg_vec_top_agg_split_supported(const Agg *agg)
{
  if (agg == NULL)
    return false;

  return (agg->aggsplit == AGGSPLIT_SIMPLE ||
          agg->aggsplit == AGGSPLIT_FINAL_DESERIAL);
}

static bool
pg_vec_join_tree_agg_elidable(const Agg *agg)
{
  if (agg == NULL)
    return false;

  if (agg->plan.righttree != NULL || agg->plan.qual != NIL)
    return false;
  if (agg->groupingSets != NIL)
    return false;

  return (agg->aggsplit == AGGSPLIT_SIMPLE ||
          agg->aggsplit == AGGSPLIT_INITIAL_SERIAL);
}

static bool
pg_vec_output_index_for_resno(List *targetlist, AttrNumber resno,
                              int *output_idx)
{
  int current_idx = 0;
  ListCell *lc;

  foreach (lc, targetlist) {
    TargetEntry *tle = lfirst_node(TargetEntry, lc);

    if (tle->resjunk)
      continue;
    if (tle->resno == resno) {
      *output_idx = current_idx;
      return true;
    }
    current_idx++;
  }

  return false;
}

static bool
pg_vec_output_index_for_group_key(const PgVecPlan *plan, int group_key_idx,
                                  int *output_idx)
{
  for (int idx = 0; idx < plan->agg.noutputs; idx++) {
    const PgVecOutputExprProgram *program = &plan->agg.outputs[idx];
    const PgVecOutputExprNode *root;

    if (program->root < 0 || program->root >= program->nnodes)
      continue;
    root = &program->nodes[program->root];
    if (root->kind == PG_VEC_OUTPUT_EXPR_GROUP_KEY && root->index == group_key_idx) {
      *output_idx = idx;
      return true;
    }
  }

  return false;
}

static bool
pg_vec_sort_key_descending(Oid sort_operator, bool *descending)
{
  Oid opfamily;
  Oid opcintype;
  CompareType strategy;

  if (!get_ordering_op_properties(sort_operator, &opfamily, &opcintype,
                                  &strategy))
    return false;
  if (strategy == BTLessStrategyNumber) {
    *descending = false;
    return true;
  }
  if (strategy == BTGreaterStrategyNumber) {
    *descending = true;
    return true;
  }

  return false;
}

static bool
pg_vec_lower_topn_spec(Limit *limit, Sort *sort, List *targetlist,
                       PgVecPlan *plan)
{
  plan->topn.enabled = false;
  plan->topn.has_limit = false;
  plan->topn.limit_count = 0;
  plan->topn.nsortkeys = 0;

  if (sort != NULL) {
    for (int key_idx = 0; key_idx < sort->numCols; key_idx++) {
      int output_idx;
      bool descending;
      PgVecSortKey *sort_key;
      PgVecOutputExprProgram *program;

      if (plan->topn.nsortkeys >= PG_VEC_MAX_SORT_KEYS)
        return false;
      if (!pg_vec_output_index_for_resno(targetlist,
                                         sort->sortColIdx[key_idx],
                                         &output_idx))
        return false;
      if (!pg_vec_sort_key_descending(sort->sortOperators[key_idx],
                                      &descending))
        return false;

      program = &plan->agg.outputs[output_idx];
      if (program->root < 0 || program->root >= program->nnodes)
        return false;
      if (program->nodes[program->root].kind != PG_VEC_OUTPUT_EXPR_GROUP_KEY &&
          program->nodes[program->root].kind != PG_VEC_OUTPUT_EXPR_AGGREF)
        return false;

      sort_key = &plan->topn.sort_keys[plan->topn.nsortkeys++];
      sort_key->output_idx = output_idx;
      sort_key->descending = descending;
      sort_key->nulls_first = sort->nullsFirst[key_idx];
    }
  }

  if (limit != NULL) {
    Const *limit_const =
        castNode(Const, pg_vec_strip_implicit_casts(limit->limitCount));

    if (limit->limitOffset != NULL && !pg_vec_const_is_zero(limit->limitOffset))
      return false;
    if (limit->limitCount == NULL || !IsA(limit_const, Const) ||
        limit_const->constisnull)
      return false;

    if (limit_const->consttype == INT8OID)
      plan->topn.limit_count = DatumGetInt64(limit_const->constvalue);
    else if (limit_const->consttype == INT4OID)
      plan->topn.limit_count = DatumGetInt32(limit_const->constvalue);
    else
      return false;

    if (plan->topn.limit_count < 0)
      return false;
    plan->topn.has_limit = true;
  }

  plan->topn.enabled = (plan->topn.nsortkeys > 0 || plan->topn.has_limit);
  return true;
}

static bool
pg_vec_lower_grouped_input_sort_spec(const Agg *agg, Sort *sort, PgVecPlan *plan)
{
  if (agg == NULL || sort == NULL)
    return true;
  if (!plan->agg.grouped)
    return true;
  if (plan->topn.enabled)
    return true;

  plan->topn.enabled = false;
  plan->topn.has_limit = false;
  plan->topn.limit_count = 0;
  plan->topn.nsortkeys = 0;

  for (int key_idx = 0; key_idx < sort->numCols; key_idx++) {
    int group_key_idx = -1;
    int output_idx;
    bool descending;
    PgVecSortKey *sort_key;

    for (int grp_idx = 0; grp_idx < agg->numCols; grp_idx++) {
      if (agg->grpColIdx[grp_idx] == sort->sortColIdx[key_idx]) {
        group_key_idx = grp_idx;
        break;
      }
    }

    if (group_key_idx < 0)
      return false;
    if (!pg_vec_output_index_for_group_key(plan, group_key_idx, &output_idx))
      return false;
    if (!pg_vec_sort_key_descending(sort->sortOperators[key_idx], &descending))
      return false;
    if (plan->topn.nsortkeys >= PG_VEC_MAX_SORT_KEYS)
      return false;

    sort_key = &plan->topn.sort_keys[plan->topn.nsortkeys++];
    sort_key->output_idx = output_idx;
    sort_key->descending = descending;
    sort_key->nulls_first = sort->nullsFirst[key_idx];
  }

  plan->topn.enabled = (plan->topn.nsortkeys > 0);
  return true;
}

static bool
pg_vec_lower_join_tree(Plan *subplan, PlannedStmt *plannedstmt,
                       PgVecLowerContext *ctx, PgVecPlan *plan,
                       PgVecJoinTreeInfo *info,
                       const char **failure_reason)
{
  Join *join_node;
  Plan *stripped = pg_vec_strip_plan_wrappers(subplan);

  if (stripped == NULL) {
    pg_vec_set_failure_reason(failure_reason, "missing join subplan");
    return false;
  }

  if (IsA(stripped, SeqScan)) {
    SeqScan *seqscan = castNode(SeqScan, stripped);
    int input_id;

    if (!pg_vec_add_input_context(seqscan, plannedstmt, ctx, plan, &input_id) ||
        !pg_vec_lower_filter_quals(seqscan->scan.plan.qual,
                                   &seqscan->scan.plan,
                                   ctx,
                                   plan,
                                   &plan->inputs[input_id].filter)) {
      pg_vec_set_failure_reason(failure_reason,
                                "failed to lower SeqScan input in join tree");
      return false;
    }

    info->first_input = input_id;
    info->ninputs = 1;
    info->njoins = 0;
    return true;
  }

  if (!IsA(stripped, HashJoin) && !IsA(stripped, MergeJoin) &&
      !IsA(stripped, NestLoop)) {
    pg_vec_set_failure_reason(
        failure_reason,
        "join path currently requires a left-deep tree of base-relation joins");
    return false;
  }

  join_node = (Join *) stripped;

  {
    PgVecJoinTreeInfo phys_left_info;
    PgVecJoinTreeInfo phys_right_info;
    const PgVecJoinTreeInfo *ordered_left_info[2];
    const PgVecJoinTreeInfo *ordered_right_info[2];
    const char *lower_failure = NULL;

    if (!pg_vec_lower_join_tree(join_node->plan.lefttree,
                                plannedstmt,
                                ctx,
                                plan,
                                &phys_left_info,
                                failure_reason))
      return false;
    if (!pg_vec_lower_join_tree(join_node->plan.righttree,
                                plannedstmt,
                                ctx,
                                plan,
                                &phys_right_info,
                                failure_reason))
      return false;
    ordered_left_info[0] = &phys_left_info;
    ordered_right_info[0] = &phys_right_info;
    ordered_left_info[1] = &phys_right_info;
    ordered_right_info[1] = &phys_left_info;

    for (int attempt = 0; attempt < 2; attempt++) {
      const PgVecJoinTreeInfo *left_info = ordered_left_info[attempt];
      const PgVecJoinTreeInfo *right_info = ordered_right_info[attempt];
      PgVecJoinSpec join_spec;
      PgVecJoinKind join_kind = PG_VEC_JOIN_INVALID;
      int total_joins = left_info->njoins + 1 + right_info->njoins;

      if (total_joins > PG_VEC_MAX_JOINS) {
        lower_failure = "too many joins for current pg_vec IR";
        continue;
      }

      switch (join_node->jointype) {
        case JOIN_INNER:
          join_kind = PG_VEC_JOIN_INNER;
          break;
        case JOIN_SEMI:
          if (left_info != &phys_left_info || right_info != &phys_right_info)
            continue;
          join_kind = PG_VEC_JOIN_SEMI;
          break;
        case JOIN_RIGHT_SEMI:
          if (left_info != &phys_right_info || right_info != &phys_left_info)
            continue;
          join_kind = PG_VEC_JOIN_SEMI;
          break;
        default:
          lower_failure = "join tree currently supports only inner and semi joins";
          continue;
      }

      MemSet(&join_spec, 0, sizeof(join_spec));
      join_spec.left_input = left_info->first_input;
      join_spec.right_input = right_info->first_input;
      if (!pg_vec_lower_join_spec(stripped, join_kind, ctx, plan, &join_spec) ||
          !pg_vec_lower_filter_quals(pg_vec_join_quals(stripped),
                                     stripped,
                                     ctx,
                                     plan,
                                     &join_spec.filter) ||
          !pg_vec_append_filter_quals(stripped->qual,
                                      stripped,
                                      ctx,
                                      plan,
                                      &join_spec.filter)) {
        lower_failure = "failed to lower join keys or residual join filter";
        continue;
      }

      info->first_input = left_info->first_input;
      info->ninputs = left_info->ninputs + right_info->ninputs;
      info->njoins = 0;

      for (int join_idx = 0; join_idx < left_info->njoins; join_idx++)
        info->joins[info->njoins++] = left_info->joins[join_idx];
      info->joins[info->njoins++] = join_spec;
      for (int join_idx = 0; join_idx < right_info->njoins; join_idx++)
        info->joins[info->njoins++] = right_info->joins[join_idx];

      return true;
    }

    pg_vec_set_failure_reason(
        failure_reason,
        lower_failure != NULL ? lower_failure
                              : "failed to lower join keys or residual join filter");
    return false;
  }
}

static bool
pg_vec_try_translate_scan_filter_agg_plan(QueryDesc *queryDesc, PgVecPlan *plan,
                                          const char **failure_reason) {
  PlannedStmt *plannedstmt;
  Plan *plantree;
  Limit *limit = NULL;
  Sort *sort = NULL;
  Sort *group_sort = NULL;
  Agg *agg;
  Plan *scan_plan;
  SeqScan *seqscan;
  RangeTblEntry *rte;
  PgVecLowerContext ctx;

  plannedstmt = queryDesc->plannedstmt;
  plantree = plannedstmt->planTree;

  MemSet(&ctx, 0, sizeof(ctx));
  ctx.queryDesc = queryDesc;

  if (IsA(plantree, Limit)) {
    limit = castNode(Limit, plantree);
    if (limit->plan.righttree != NULL) {
      pg_vec_set_failure_reason(failure_reason,
                                "top-level Limit must have a single child");
      return false;
    }
    plantree = limit->plan.lefttree;
  }

  if (IsA(plantree, Sort)) {
    sort = castNode(Sort, plantree);

    if (sort->plan.righttree != NULL || !IsA(sort->plan.lefttree, Agg)) {
      pg_vec_set_failure_reason(failure_reason,
                                "top-level Sort must have a single Agg child");
      return false;
    }
    plantree = sort->plan.lefttree;
  }

  if (!IsA(plantree, Agg)) {
    pg_vec_set_failure_reason(
        failure_reason, "single-input path expects Agg at the top of the plan");
    return false;
  }

  agg = castNode(Agg, plantree);
  if (!pg_vec_top_agg_split_supported(agg)) {
    pg_vec_set_failure_reason(
        failure_reason,
        "single-input path only supports SIMPLE or FINAL aggregates");
    return false;
  }
  scan_plan = agg->plan.lefttree;
  if (sort == NULL && IsA(scan_plan, Sort)) {
    group_sort = castNode(Sort, scan_plan);
    if (group_sort->plan.righttree != NULL) {
      pg_vec_set_failure_reason(failure_reason,
                                "input Sort below Agg must have a single child");
      return false;
    }
    scan_plan = group_sort->plan.lefttree;
  }

  if (agg->plan.righttree != NULL || !IsA(scan_plan, SeqScan)) {
    pg_vec_set_failure_reason(failure_reason,
                              "single-input path expects Agg over SeqScan");
    return false;
  }

  seqscan = castNode(SeqScan, scan_plan);
  rte = rt_fetch(seqscan->scan.scanrelid, plannedstmt->rtable);
  if (rte == NULL || rte->rtekind != RTE_RELATION) {
    pg_vec_set_failure_reason(
        failure_reason, "single-input path requires a base-relation SeqScan");
    return false;
  }

  plan->kind = PG_VEC_PLAN_SCAN_FILTER_AGG;
  plan->ninputs = 1;
  plan->inputs[0].relid = rte->relid;
  plan->agg.grouped = (agg->numCols > 0);

  if (!pg_vec_make_single_input_context(seqscan, plannedstmt, &ctx)) {
    pg_vec_set_failure_reason(
        failure_reason,
        "failed to map SeqScan relation into pg_vec input context");
    return false;
  }
  if (!pg_vec_lower_agg_targetlist(queryDesc, agg->plan.targetlist, &agg->plan,
                                   &ctx, plan)) {
    pg_vec_set_failure_reason(
        failure_reason,
        "failed to lower aggregate targetlist for single-input path");
    return false;
  }
  if (!pg_vec_append_post_agg_filter_quals(agg->plan.qual,
                                           &agg->plan,
                                           &ctx,
                                           plan,
                                           &plan->agg.having)) {
    pg_vec_set_failure_reason(
        failure_reason,
        "failed to lower aggregate HAVING quals for single-input path");
    return false;
  }
  if (!pg_vec_lower_topn_spec(limit, sort, agg->plan.targetlist, plan)) {
    pg_vec_set_failure_reason(
        failure_reason,
        "failed to lower top-level sort or limit for single-input path");
    return false;
  }
  if (!pg_vec_lower_grouped_input_sort_spec(agg, group_sort, plan)) {
    pg_vec_set_failure_reason(
        failure_reason,
        "failed to lower grouped input sort order for single-input path");
    return false;
  }
  if (!pg_vec_lower_filter_quals(seqscan->scan.plan.qual, &seqscan->scan.plan,
                                 &ctx, plan, &plan->inputs[0].filter)) {
    pg_vec_set_failure_reason(
        failure_reason,
        "failed to lower SeqScan filter quals for single-input path");
    return false;
  }

  return true;
}

static bool
pg_vec_try_translate_join_filter_agg_plan(QueryDesc *queryDesc, PgVecPlan *plan,
                                          const char **failure_reason) {
  PlannedStmt *plannedstmt;
  Plan *plantree;
  Limit *limit = NULL;
  Sort *sort = NULL;
  Sort *group_sort = NULL;
  Agg *agg;
  Plan *join_plan;
  PgVecLowerContext ctx;
  PgVecJoinTreeInfo join_info;

  plannedstmt = queryDesc->plannedstmt;
  plantree = plannedstmt->planTree;

  MemSet(&ctx, 0, sizeof(ctx));
  MemSet(&join_info, 0, sizeof(join_info));
  ctx.queryDesc = queryDesc;

  if (IsA(plantree, Limit)) {
    limit = castNode(Limit, plantree);
    if (limit->plan.righttree != NULL) {
      pg_vec_set_failure_reason(failure_reason,
                                "top-level Limit must have a single child");
      return false;
    }
    plantree = limit->plan.lefttree;
  }

  if (IsA(plantree, Sort)) {
    sort = castNode(Sort, plantree);

    if (sort->plan.righttree != NULL || !IsA(sort->plan.lefttree, Agg)) {
      pg_vec_set_failure_reason(failure_reason,
                                "top-level Sort must have a single Agg child");
      return false;
    }
    plantree = sort->plan.lefttree;
  }

  if (!IsA(plantree, Agg)) {
    pg_vec_set_failure_reason(failure_reason,
                              "join path expects Agg at the top of the plan");
    return false;
  }

  agg = castNode(Agg, plantree);
  if (!pg_vec_top_agg_split_supported(agg)) {
    pg_vec_set_failure_reason(
        failure_reason,
        "join path only supports SIMPLE or FINAL aggregates");
    return false;
  }
  join_plan = agg->plan.lefttree;
  if (sort == NULL && IsA(join_plan, Sort)) {
    group_sort = castNode(Sort, join_plan);
    if (group_sort->plan.righttree != NULL) {
      pg_vec_set_failure_reason(
          failure_reason,
          "input Sort below Agg must have a single child in join path");
      return false;
    }
    join_plan = group_sort->plan.lefttree;
  }
  if (join_plan == NULL) {
    pg_vec_set_failure_reason(
        failure_reason,
        "join path expects Agg over a left-deep join subtree");
    return false;
  }

  plan->kind = PG_VEC_PLAN_SCAN_FILTER_AGG;
  plan->agg.grouped = (agg->numCols > 0);

  if (!pg_vec_lower_join_tree(join_plan,
                              plannedstmt,
                              &ctx,
                              plan,
                              &join_info,
                              failure_reason)) {
    if (failure_reason != NULL && *failure_reason == NULL)
      pg_vec_set_failure_reason(failure_reason,
                                "failed to lower join tree");
    return false;
  }
  plan->njoins = join_info.njoins;
  for (int join_idx = 0; join_idx < join_info.njoins; join_idx++)
    plan->joins[join_idx] = join_info.joins[join_idx];
  if (plan->ninputs < 2 || plan->njoins < 1) {
    pg_vec_set_failure_reason(
        failure_reason,
        "join path requires at least two base-relation inputs");
    return false;
  }
  if (join_info.ninputs != plan->ninputs) {
    pg_vec_set_failure_reason(
        failure_reason,
        "join path failed to preserve a consistent input count");
    return false;
  }
  if (!pg_vec_lower_agg_targetlist(queryDesc, agg->plan.targetlist, &agg->plan,
                                   &ctx, plan)) {
    pg_vec_set_failure_reason(
        failure_reason, "failed to lower aggregate targetlist for join path");
    return false;
  }
  if (!pg_vec_append_post_agg_filter_quals(agg->plan.qual,
                                           &agg->plan,
                                           &ctx,
                                           plan,
                                           &plan->agg.having)) {
    pg_vec_set_failure_reason(
        failure_reason,
        "failed to lower aggregate HAVING quals for join path");
    return false;
  }
  if (!pg_vec_lower_topn_spec(limit, sort, agg->plan.targetlist, plan)) {
    pg_vec_set_failure_reason(
        failure_reason,
        "failed to lower top-level sort or limit for join path");
    return false;
  }
  if (!pg_vec_lower_grouped_input_sort_spec(agg, group_sort, plan)) {
    pg_vec_set_failure_reason(
        failure_reason,
        "failed to lower grouped input sort order for join path");
    return false;
  }

  return true;
}

static bool pg_vec_numeric_to_scaled_int64(Datum value, int scale, int64 *out) {
  char *str;
  bool ok;

  str = DatumGetCString(DirectFunctionCall1(numeric_out, value));
  ok = pg_vec_parse_scaled_int64(str, scale, out);
  pfree(str);

  return ok;
}

static bool pg_vec_parse_scaled_int64(const char *str, int scale, int64 *out) {
  const char *ptr = str;
  bool negative = false;
  int64 int_part = 0;
  int64 frac_part = 0;
  int frac_digits = 0;
  int64 scale_factor = 1;

  for (int i = 0; i < scale; i++)
    scale_factor *= 10;

  while (*ptr != '\0' && isspace((unsigned char)*ptr))
    ptr++;

  if (*ptr == '-') {
    negative = true;
    ptr++;
  } else if (*ptr == '+')
    ptr++;

  if (!isdigit((unsigned char)*ptr))
    return false;

  while (isdigit((unsigned char)*ptr)) {
    int_part = int_part * 10 + (*ptr - '0');
    ptr++;
  }

  if (*ptr == '.') {
    ptr++;
    while (isdigit((unsigned char)*ptr)) {
      if (frac_digits < scale) {
        frac_part = frac_part * 10 + (*ptr - '0');
        frac_digits++;
      } else if (*ptr != '0')
        return false;
      ptr++;
    }
  }

  while (frac_digits < scale) {
    frac_part *= 10;
    frac_digits++;
  }

  while (*ptr != '\0' && isspace((unsigned char)*ptr))
    ptr++;

  if (*ptr != '\0')
    return false;

  *out = int_part * scale_factor + frac_part;
  if (negative)
    *out = -*out;

  return true;
}
