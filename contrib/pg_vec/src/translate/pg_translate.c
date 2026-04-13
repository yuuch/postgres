#include "postgres.h"

#include <ctype.h>

#include "executor/executor.h"
#include "executor/nodeSubplan.h"
#include "access/stratnum.h"
#include "catalog/pg_type_d.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/params.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "parser/parsetree.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "varatt.h"

#include "pg_translate.h"
#include "../bridge/state.h"

extern Datum numeric_scale(PG_FUNCTION_ARGS);
extern Datum numeric_round(PG_FUNCTION_ARGS);

typedef struct PgVecInputContext {
  Index rtindex;
  Oid relid;
  uint8 input_id;
  Plan *boundary_plan;
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

typedef struct PgVecFlattenedJoinLeaf
{
  Plan *plan;
  int input_id;
} PgVecFlattenedJoinLeaf;

typedef struct PgVecFlattenedJoinQual
{
  Node *qual;
  Plan *owner_plan;
} PgVecFlattenedJoinQual;

static bool
pg_vec_try_translate_scan_filter_agg_plan(QueryDesc *queryDesc, PgVecPlan *plan,
                                          const char **failure_reason);
static bool
pg_vec_try_translate_join_filter_agg_plan(QueryDesc *queryDesc, PgVecPlan *plan,
                                          const char **failure_reason);
static bool
pg_vec_try_translate_join_project_plan(QueryDesc *queryDesc, PgVecPlan *plan,
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
static Plan *pg_vec_find_ctx_boundary_plan(const PgVecLowerContext *ctx,
                                           Plan *plan,
                                           uint8 *input_id);
static TargetEntry *pg_vec_find_tle_by_resno(List *targetlist,
                                             AttrNumber resno);
static bool pg_vec_ctx_contains_rtindex(const PgVecLowerContext *ctx, Index rtindex);
static Node *pg_vec_make_input_boundary_var(Var *template_var,
                                            Index rtindex,
                                            AttrNumber attno);
static Node *pg_vec_resolve_var_through_plan_with_source(Node *node, Plan *plan,
                                                         const PgVecLowerContext *ctx,
                                                         Plan **resolved_plan);
static Node *pg_vec_resolve_var_through_plan(Node *node, Plan *plan,
                                             const PgVecLowerContext *ctx);
static bool pg_vec_lower_var_with_source(Var *var, const PgVecLowerContext *ctx,
                                         PgVecPlan *plan, Plan *source_plan,
                                         PgVecColumnRef *column_ref);
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
static bool pg_vec_try_lower_substring_prefix2_expr(FuncExpr *func,
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
static bool pg_vec_try_make_boundary_agg_var(Var *var,
                                             const PgVecLowerContext *ctx,
                                             Node **rewritten);
static Node *pg_vec_prepare_agg_arg_mutator(Node *node, void *context);
static Node *pg_vec_prepare_agg_arg_node(Node *node,
                                         const PgVecLowerContext *ctx);
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
static bool pg_vec_filter_has_qual_kind(const PgVecFilterSpec *filter,
                                        PgVecQualKind kind);
static bool pg_vec_post_agg_filter_has_param(
    const PgVecPostAggFilterSpec *filter);
static bool pg_vec_plan_has_derived_grouped_input(const PgVecPlan *plan);
static bool pg_vec_plan_tree_has_outer_join(Plan *plan);
static bool pg_vec_plan_tree_contains_subqueryscan(Plan *plan);
static bool pg_vec_join_subtree_has_nonelidable_agg_or_subquery(Plan *plan);
static bool pg_vec_append_direct_derived_post_agg_filter_quals(
    List *quals,
    Plan *source_plan,
    const PgVecLowerContext *ctx,
    PgVecPlan *plan,
    PgVecPostAggFilterSpec *filter);
static bool pg_vec_lower_agg_targetlist(QueryDesc *queryDesc, List *targetlist,
                                        Plan *source_plan,
                                        const PgVecLowerContext *ctx,
                                        PgVecPlan *plan);
static bool pg_vec_lower_direct_derived_agg_targetlist(List *targetlist,
                                                       Plan *agg_plan,
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
static bool pg_vec_add_derived_input_context(SubqueryScan *subqueryscan,
                                             PlannedStmt *plannedstmt,
                                             PgVecLowerContext *ctx,
                                             PgVecPlan *plan,
                                             int *input_id);
static bool pg_vec_add_derived_agg_input_context(Plan *subplan,
                                                 PlannedStmt *plannedstmt,
                                                 PgVecLowerContext *ctx,
                                                 PgVecPlan *plan,
                                                 int *input_id);
static bool pg_vec_populate_derived_grouped_agg_input_from_subplan(
    Plan *subplan,
    PlannedStmt *plannedstmt,
    QueryDesc *queryDesc,
    PgVecInputSpec *input,
    Index *boundary_rtindex);
static bool pg_vec_lower_join_key_expr(Node *node, Plan *join_plan,
                                       const PgVecLowerContext *ctx,
                                       PgVecPlan *plan,
                                       PgVecColumnRef *column_ref);
static bool pg_vec_lower_join_keys_from_list(List *clauses, Plan *join_plan,
                                             const PgVecLowerContext *ctx,
                                             PgVecPlan *plan,
                                             PgVecJoinSpec *join_spec);
static bool pg_vec_try_extract_hashed_any_join_qual(Node *node,
                                                    PlannedStmt *plannedstmt,
                                                    PgVecJoinKind *join_kind,
                                                    Var **outer_join_var,
                                                    SeqScan **inner_seqscan,
                                                    Var **inner_join_var);
static bool pg_vec_is_subplan_compare_candidate(Node *node);
static bool pg_vec_split_seqscan_quals_for_hashed_any_join(List *quals,
                                                           PlannedStmt *plannedstmt,
                                                           List **base_quals,
                                                           PgVecJoinKind *join_kind,
                                                           Var **outer_join_var,
                                                           SeqScan **inner_seqscan,
                                                           Var **inner_join_var);
static bool pg_vec_make_simple_column_expr(PgVecExprProgram *program,
                                           PgVecColumnRef column_ref);
static bool pg_vec_make_simple_output_group_key(PgVecOutputExprProgram *program,
                                                PgVecScalarKind scalar_kind,
                                                int group_idx);
static bool pg_vec_make_simple_output_aggref(PgVecOutputExprProgram *program,
                                             PgVecScalarKind scalar_kind,
                                             int agg_idx);
static bool pg_vec_try_translate_count_distinct_join_plan(QueryDesc *queryDesc,
                                                          Limit *limit,
                                                          Sort *sort,
                                                          Agg *agg,
                                                          Plan *join_plan,
                                                          PgVecPlan *plan,
                                                          const char **failure_reason);
static bool pg_vec_lower_join_spec(Plan *join_plan,
                                   PgVecJoinKind join_kind,
                                   const PgVecLowerContext *ctx,
                                   PgVecPlan *plan,
                                   PgVecJoinSpec *join_spec);
static bool
pg_vec_collect_flattened_inner_join_tree(Plan *subplan,
                                         List **leaves,
                                         List **join_quals);
static bool
pg_vec_try_extract_flattened_join_key(Node *qual,
                                      Plan *join_plan,
                                      const PgVecLowerContext *ctx,
                                      PgVecPlan *plan,
                                      int candidate_input,
                                      const bool *bound_inputs,
                                      PgVecJoinKey *key);
static bool
pg_vec_try_lower_flattened_inner_join_tree(Plan *subplan,
                                           PlannedStmt *plannedstmt,
                                           PgVecLowerContext *ctx,
                                           PgVecPlan *plan,
                                           PgVecJoinTreeInfo *info,
                                           const char **failure_reason);
static bool pg_vec_scalar_kind_is_numeric(PgVecScalarKind scalar_kind);
static bool pg_vec_scalar_kinds_compare_compatible(PgVecScalarKind left_kind,
                                                   PgVecScalarKind right_kind);
static bool
pg_vec_collect_correlated_scan_qual_node(Node *node,
                                         int param_id,
                                         Var **inner_join_var,
                                         List **remaining_quals);
static bool
pg_vec_collect_multi_correlated_scan_qual_node(Node *node,
                                               List *param_ids,
                                               Var **inner_join_vars,
                                               List **remaining_quals);
static bool
pg_vec_collect_correlated_scan_quals(List *quals,
                                     int param_id,
                                     Var **inner_join_var,
                                     List **remaining_quals);
static bool
pg_vec_collect_multi_correlated_scan_quals(List *quals,
                                           List *param_ids,
                                           Var **inner_join_vars,
                                           List **remaining_quals);
static bool
pg_vec_append_flattened_and_quals(List **flat_quals,
                                  Node *node);
static List *
pg_vec_flatten_and_quals(List *quals);
static bool
pg_vec_strip_correlated_param_qual_from_plan(Plan *plan,
                                             int param_id,
                                             Var **inner_join_var);
static Aggref *pg_vec_make_q17_sum_aggref(const Aggref *template_aggref,
                                          const Var *arg_var);
static Aggref *pg_vec_make_q17_count_star_aggref(const Aggref *template_aggref);
static bool
pg_vec_try_rewrite_q17_correlated_joinqual(List *quals,
                                           Plan *join_plan,
                                           PlannedStmt *plannedstmt,
                                           PgVecLowerContext *ctx,
                                           PgVecPlan *plan,
                                           int leftmost_input,
                                           PgVecJoinSpec *extra_join);
static bool
pg_vec_try_rewrite_correlated_seqscan_qual(Node *qual,
                                           SeqScan *outer_seqscan,
                                           PlannedStmt *plannedstmt,
                                           PgVecLowerContext *ctx,
                                           PgVecPlan *plan,
                                           int left_input_id,
                                           PgVecJoinSpec *extra_join);
static bool
pg_vec_try_rewrite_q2_correlated_joinqual(Node *qual,
                                          Plan *join_plan,
                                          PlannedStmt *plannedstmt,
                                          PgVecLowerContext *ctx,
                                          PgVecPlan *plan,
                                          PgVecJoinSpec *extra_join);
static List *pg_vec_join_quals(Plan *join_plan);
static Plan *pg_vec_strip_plan_wrappers(Plan *plan);
static bool pg_vec_top_agg_split_supported(const Agg *agg);
static bool pg_vec_join_tree_agg_elidable(const Agg *agg);
static bool pg_vec_output_index_for_resno(List *targetlist, AttrNumber resno,
                                          int *output_idx);
static bool pg_vec_build_derived_output_map(List *targetlist,
                                            List *source_targetlist,
                                            int *output_map);
static bool pg_vec_reorder_outputs_to_targetlist(List *targetlist,
                                                 List *source_targetlist,
                                                 PgVecAggSpec *agg);
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
  const char *project_reason = NULL;

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

  if (pg_vec_try_translate_join_project_plan(queryDesc, plan, &project_reason))
    return true;

  if (scan_reason != NULL && join_reason != NULL && project_reason != NULL)
    pg_vec_set_failure_reason(
        failure_reason,
        psprintf("scan path failed: %s; join path failed: %s; project path failed: %s",
                 scan_reason,
                 join_reason,
                 project_reason));
  else if (scan_reason != NULL && join_reason != NULL)
    pg_vec_set_failure_reason(
        failure_reason, psprintf("scan path failed: %s; join path failed: %s",
                                 scan_reason, join_reason));
  else if (scan_reason != NULL)
    pg_vec_set_failure_reason(failure_reason, scan_reason);
  else if (join_reason != NULL)
    pg_vec_set_failure_reason(failure_reason, join_reason);
  else if (project_reason != NULL)
    pg_vec_set_failure_reason(failure_reason, project_reason);
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
  input->kind = PG_VEC_INPUT_RELATION;
  pg_vec_filter_spec_init(&input->filter);
  pg_vec_filter_spec_init(&input->derived.base_filter);
  pg_vec_post_agg_filter_init(&input->derived.agg.having);
  for (int idx = 0; idx < PG_VEC_MAX_OUTPUT_COLUMNS; idx++)
    input->derived.output_map[idx] = -1;
}

static bool
pg_vec_ctx_contains_rtindex(const PgVecLowerContext *ctx, Index rtindex)
{
  if (ctx == NULL || rtindex <= 0)
    return false;

  for (int input_idx = 0; input_idx < ctx->ninputs; input_idx++)
  {
    if (ctx->inputs[input_idx].rtindex == rtindex)
      return true;
  }

  return false;
}

static Node *
pg_vec_make_input_boundary_var(Var *template_var, Index rtindex, AttrNumber attno)
{
 Var *boundary_var;

  if (template_var == NULL || rtindex <= 0 || attno <= 0)
    return NULL;

  boundary_var = palloc(sizeof(Var));
  memcpy(boundary_var, template_var, sizeof(Var));
  boundary_var->varno = rtindex;
  boundary_var->varattno = attno;
  boundary_var->varnosyn = rtindex;
  boundary_var->varattnosyn = attno;
  return (Node *) boundary_var;
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

static bool
pg_vec_make_simple_column_expr(PgVecExprProgram *program, PgVecColumnRef column_ref)
{
  PgVecExprNode expr_node;

  pg_vec_expr_program_init(program);
  MemSet(&expr_node, 0, sizeof(expr_node));
  expr_node.kind = PG_VEC_EXPR_COLUMN;
  expr_node.scalar_kind = column_ref.scalar_kind;
  expr_node.left = -1;
  expr_node.right = -1;
  expr_node.column = column_ref;
  return pg_vec_add_expr_node(program, &expr_node, &program->root);
}

static bool
pg_vec_make_simple_output_group_key(PgVecOutputExprProgram *program,
                                    PgVecScalarKind scalar_kind,
                                    int group_idx)
{
  PgVecOutputExprNode expr_node;

  pg_vec_output_expr_program_init(program);
  MemSet(&expr_node, 0, sizeof(expr_node));
  expr_node.kind = PG_VEC_OUTPUT_EXPR_GROUP_KEY;
  expr_node.scalar_kind = scalar_kind;
  expr_node.left = -1;
  expr_node.right = -1;
  expr_node.index = group_idx;
  return pg_vec_add_output_expr_node(program, &expr_node, &program->root);
}

static bool
pg_vec_make_simple_output_aggref(PgVecOutputExprProgram *program,
                                 PgVecScalarKind scalar_kind,
                                 int agg_idx)
{
  PgVecOutputExprNode expr_node;

  pg_vec_output_expr_program_init(program);
  MemSet(&expr_node, 0, sizeof(expr_node));
  expr_node.kind = PG_VEC_OUTPUT_EXPR_AGGREF;
  expr_node.scalar_kind = scalar_kind;
  expr_node.left = -1;
  expr_node.right = -1;
  expr_node.index = agg_idx;
  return pg_vec_add_output_expr_node(program, &expr_node, &program->root);
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
  else
    *scalar_kind = PG_VEC_SCALAR_DECIMAL128_S6;

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
    {
      int64 scaled;

      if (!pg_vec_numeric_to_scaled_int64(constnode->constvalue, 4, &scaled))
        return false;
      constant->decimal128 = scaled;
      return true;
    }
  case PG_VEC_SCALAR_DECIMAL128_S6:
    if (constnode->consttype != NUMERICOID)
      return false;
    {
      int64 scaled;

      if (!pg_vec_numeric_to_scaled_int64(constnode->constvalue, 6, &scaled))
        return false;
      constant->decimal128 = scaled;
      return true;
    }

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
  prm = &estate->es_param_exec_vals[param->paramid];
  if (prm->execPlan != NULL)
  {
    ExprContext *econtext = GetPerTupleExprContext(estate);

    PG_TRY();
    {
      pg_vec_push_translation_guard();
      ExecSetParamPlan((SubPlanState *) prm->execPlan, econtext);
      pg_vec_pop_translation_guard();
    }
    PG_CATCH();
    {
      pg_vec_pop_translation_guard();
      PG_RE_THROW();
    }
    PG_END_TRY();
  }

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
  int left_scale = -1;
  int right_scale = -1;

  switch (left_kind)
  {
    case PG_VEC_SCALAR_DECIMAL64_S2:
    case PG_VEC_SCALAR_DECIMAL128_S2:
      left_scale = 2;
      break;
    case PG_VEC_SCALAR_DECIMAL128_S4:
      left_scale = 4;
      break;
    case PG_VEC_SCALAR_DECIMAL128_S6:
      left_scale = 6;
      break;
    default:
      break;
  }

  switch (right_kind)
  {
    case PG_VEC_SCALAR_DECIMAL64_S2:
    case PG_VEC_SCALAR_DECIMAL128_S2:
      right_scale = 2;
      break;
    case PG_VEC_SCALAR_DECIMAL128_S4:
      right_scale = 4;
      break;
    case PG_VEC_SCALAR_DECIMAL128_S6:
      right_scale = 6;
      break;
    default:
      break;
  }

  switch (expr_kind) {
  case PG_VEC_EXPR_ADD:
  case PG_VEC_EXPR_SUB:
    if (left_scale > 0 && right_scale > 0) {
      if (left_scale == 2 && right_scale == 2) {
        *result_kind = (left_kind == PG_VEC_SCALAR_DECIMAL64_S2 &&
                        right_kind == PG_VEC_SCALAR_DECIMAL64_S2)
                           ? PG_VEC_SCALAR_DECIMAL64_S2
                           : PG_VEC_SCALAR_DECIMAL128_S2;
        return true;
      }
      if (left_scale <= 4 && right_scale <= 4) {
        *result_kind = PG_VEC_SCALAR_DECIMAL128_S4;
        return true;
      }
      if (left_scale <= 6 && right_scale <= 6) {
        *result_kind = PG_VEC_SCALAR_DECIMAL128_S6;
        return true;
      }
      return false;
    }
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
    if ((left_kind == PG_VEC_SCALAR_INT32 && right_scale > 0) ||
        (right_kind == PG_VEC_SCALAR_INT32 && left_scale > 0)) {
      int scale = left_scale > 0 ? left_scale : right_scale;

      if (scale == 2)
        *result_kind = PG_VEC_SCALAR_DECIMAL128_S2;
      else if (scale == 4)
        *result_kind = PG_VEC_SCALAR_DECIMAL128_S4;
      else if (scale == 6)
        *result_kind = PG_VEC_SCALAR_DECIMAL128_S6;
      else
        return false;
      return true;
    }
    if (left_scale > 0 && right_scale > 0) {
      int result_scale = left_scale + right_scale;

      if (result_scale <= 2)
        *result_kind = PG_VEC_SCALAR_DECIMAL128_S2;
      else if (result_scale <= 4)
        *result_kind = PG_VEC_SCALAR_DECIMAL128_S4;
      else if (result_scale <= 6)
        *result_kind = PG_VEC_SCALAR_DECIMAL128_S6;
      else
        return false;
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
  int left_scale = -1;
  int right_scale = -1;
  int result_scale = -1;

  switch (left_kind)
  {
    case PG_VEC_SCALAR_INT32:
      left_scale = 0;
      break;
    case PG_VEC_SCALAR_DECIMAL64_S2:
    case PG_VEC_SCALAR_DECIMAL128_S2:
      left_scale = 2;
      break;
    case PG_VEC_SCALAR_DECIMAL128_S4:
      left_scale = 4;
      break;
    case PG_VEC_SCALAR_DECIMAL128_S6:
      left_scale = 6;
      break;
    default:
      break;
  }

  switch (right_kind)
  {
    case PG_VEC_SCALAR_INT32:
      right_scale = 0;
      break;
    case PG_VEC_SCALAR_DECIMAL64_S2:
    case PG_VEC_SCALAR_DECIMAL128_S2:
      right_scale = 2;
      break;
    case PG_VEC_SCALAR_DECIMAL128_S4:
      right_scale = 4;
      break;
    case PG_VEC_SCALAR_DECIMAL128_S6:
      right_scale = 6;
      break;
    default:
      break;
  }

  if (left_scale < 0 || right_scale < 0)
    return false;

  switch (expr_kind) {
  case PG_VEC_OUTPUT_EXPR_ADD:
  case PG_VEC_OUTPUT_EXPR_SUB:
    result_scale = Max(left_scale, right_scale);
    break;
  case PG_VEC_OUTPUT_EXPR_MUL:
    result_scale = left_scale + right_scale;
    if (result_scale > 6)
      result_scale = 6;
    break;
  case PG_VEC_OUTPUT_EXPR_DIV:
    result_scale = 6;
    break;
  case PG_VEC_OUTPUT_EXPR_GROUP_KEY:
  case PG_VEC_OUTPUT_EXPR_AGGREF:
  case PG_VEC_OUTPUT_EXPR_CONST:
  case PG_VEC_OUTPUT_EXPR_INVALID:
  default:
    return false;
  }

  if (result_scale <= 2)
    *result_kind = PG_VEC_SCALAR_DECIMAL128_S2;
  else if (result_scale <= 4)
    *result_kind = PG_VEC_SCALAR_DECIMAL128_S4;
  else
    *result_kind = PG_VEC_SCALAR_DECIMAL128_S6;

  return true;
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

static Plan *
pg_vec_find_ctx_boundary_plan(const PgVecLowerContext *ctx,
                              Plan *plan,
                              uint8 *input_id)
{
  if (ctx == NULL || plan == NULL)
    return NULL;

  for (int idx = 0; idx < ctx->ninputs; idx++)
  {
    if (ctx->inputs[idx].boundary_plan == plan)
    {
      if (input_id != NULL)
        *input_id = ctx->inputs[idx].input_id;
      return plan;
    }
  }

  return NULL;
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
                                                         const PgVecLowerContext *ctx,
                                                         Plan **resolved_plan) {
  static int resolve_depth = 0;
  Var *var;
  Plan *source_plan;
  TargetEntry *tle;
  bool invalid_ctx_boundary_var = false;

  resolve_depth++;
  if (resolve_depth > 256)
  {
    if (resolved_plan != NULL)
      *resolved_plan = plan;
    resolve_depth--;
    return node;
  }
  node = pg_vec_strip_implicit_casts(node);
  if (node == NULL || !IsA(node, Var)) {
    if (resolved_plan != NULL)
      *resolved_plan = plan;
    resolve_depth--;
    return node;
  }

  var = castNode(Var, node);
  if (var->varno != OUTER_VAR &&
      var->varno != INNER_VAR &&
      pg_vec_ctx_contains_rtindex(ctx, var->varno))
  {
    bool keep_boundary_var = true;

    for (int input_idx = 0; input_idx < ctx->ninputs; input_idx++)
    {
      if (ctx->inputs[input_idx].rtindex != var->varno)
        continue;

      if (ctx->inputs[input_idx].relid != InvalidOid)
      {
        Relation rel = RelationIdGetRelation(ctx->inputs[input_idx].relid);
        int natts = 0;

        if (rel != NULL)
        {
          natts = RelationGetNumberOfAttributes(rel);
          RelationClose(rel);
        }
        if (natts > 0 && var->varattno > natts)
        {
          keep_boundary_var = false;
          invalid_ctx_boundary_var = true;
        }
      }
      break;
    }

    if (!keep_boundary_var)
    {
      /* Keep resolving through the current plan; this Var is not a real
       * base-table boundary attribute but an upper-slot reference. */
    }
    else
    {
      if (resolved_plan != NULL)
        *resolved_plan = plan;
      resolve_depth--;
      return node;
    }
  }

  if (invalid_ctx_boundary_var && plan != NULL)
  {
    Plan *lookup_plan = plan;

    while (lookup_plan != NULL)
    {
      if (lookup_plan->targetlist != NIL)
      {
        tle = pg_vec_find_tle_by_resno(lookup_plan->targetlist, var->varattno);
        if (tle != NULL)
        {
          node = pg_vec_resolve_var_through_plan_with_source((Node *) tle->expr,
                                                             lookup_plan,
                                                             ctx,
                                                             resolved_plan);
          resolve_depth--;
          return node;
        }
      }

      if (lookup_plan->righttree == NULL && lookup_plan->lefttree != NULL &&
          (IsA(lookup_plan, Agg) ||
           IsA(lookup_plan, Sort) ||
           IsA(lookup_plan, Material) ||
           IsA(lookup_plan, Hash)))
      {
        lookup_plan = lookup_plan->lefttree;
        continue;
      }

      break;
    }
  }

  if (plan != NULL && IsA(plan, SubqueryScan) &&
      var->varno == castNode(SubqueryScan, plan)->scan.scanrelid)
  {
    SubqueryScan *subqueryscan = castNode(SubqueryScan, plan);

    tle = pg_vec_find_tle_by_resno(plan->targetlist, var->varattno);
    if (tle == NULL) {
      if (resolved_plan != NULL)
        *resolved_plan = subqueryscan->subplan;
      resolve_depth--;
      return node;
    }

    node = pg_vec_resolve_var_through_plan_with_source((Node *) tle->expr,
                                                       subqueryscan->subplan,
                                                       ctx,
                                                       resolved_plan);
    resolve_depth--;
    return node;
  }

  if (plan != NULL &&
      var->varno != OUTER_VAR &&
      var->varno != INNER_VAR &&
      !pg_vec_ctx_contains_rtindex(ctx, var->varno) &&
      plan->righttree == NULL &&
      plan->lefttree != NULL)
  {
    source_plan = pg_vec_strip_plan_wrappers(plan->lefttree);

    while (source_plan != NULL &&
           source_plan->targetlist == NIL &&
           source_plan->lefttree != NULL)
      source_plan = source_plan->lefttree;

    if (source_plan != NULL)
    {
      tle = pg_vec_find_tle_by_resno(source_plan->targetlist, var->varattno);
      if (tle != NULL)
      {
        node = pg_vec_resolve_var_through_plan_with_source((Node *) tle->expr,
                                                           source_plan,
                                                           ctx,
                                                           resolved_plan);
        resolve_depth--;
        return node;
      }
    }
  }

  if (var->varno != OUTER_VAR && var->varno != INNER_VAR) {
    if (resolved_plan != NULL)
      *resolved_plan = plan;
    resolve_depth--;
    return node;
  }

  if (plan == NULL) {
    if (resolved_plan != NULL)
      *resolved_plan = NULL;
    resolve_depth--;
    return node;
  }

  if (var->varno == OUTER_VAR)
    source_plan = plan->lefttree;
  else
    source_plan = plan->righttree;

  if (source_plan == NULL) {
    if (resolved_plan != NULL)
      *resolved_plan = NULL;
    resolve_depth--;
    return node;
  }

  {
    Plan *boundary_plan = source_plan;
    uint8 boundary_input_id = 0;

    while (boundary_plan != NULL &&
           (IsA(boundary_plan, Hash) ||
            IsA(boundary_plan, Material) ||
            IsA(boundary_plan, Sort)))
      boundary_plan = boundary_plan->lefttree;

    if (boundary_plan != NULL &&
        (IsA(boundary_plan, Agg) || IsA(boundary_plan, SubqueryScan)) &&
        pg_vec_find_ctx_boundary_plan(ctx, boundary_plan, &boundary_input_id) != NULL)
    {
      node = pg_vec_make_input_boundary_var(var,
                                            ctx->inputs[boundary_input_id].rtindex,
                                            var->varattno);
      if (resolved_plan != NULL)
        *resolved_plan = boundary_plan;
      resolve_depth--;
      return node;
    }
  }

  {
    Plan *lookup_plan = source_plan;

    while (lookup_plan != NULL)
    {
      if (lookup_plan->targetlist != NIL)
      {
        tle = pg_vec_find_tle_by_resno(lookup_plan->targetlist, var->varattno);
        if (tle != NULL)
        {
          node = pg_vec_resolve_var_through_plan_with_source((Node *) tle->expr,
                                                             lookup_plan,
                                                             ctx,
                                                             resolved_plan);
          resolve_depth--;
          return node;
        }
      }

      if ((IsA(lookup_plan, Sort) ||
           IsA(lookup_plan, Material) ||
           IsA(lookup_plan, Hash)) &&
          lookup_plan->lefttree != NULL)
      {
        lookup_plan = lookup_plan->lefttree;
        continue;
      }

      break;
    }
  }

  source_plan = pg_vec_strip_plan_wrappers(source_plan);

  if (source_plan == NULL) {
    if (resolved_plan != NULL)
      *resolved_plan = NULL;
    resolve_depth--;
    return node;
  }

  while (source_plan != NULL &&
         source_plan->targetlist == NIL &&
         source_plan->lefttree != NULL)
    source_plan = source_plan->lefttree;

  if (source_plan != NULL)
  {
    Index boundary_rtindex = 0;

    if (IsA(source_plan, SeqScan))
      boundary_rtindex = castNode(SeqScan, source_plan)->scan.scanrelid;
    else if (IsA(source_plan, SubqueryScan))
      boundary_rtindex = castNode(SubqueryScan, source_plan)->scan.scanrelid;

    if (pg_vec_ctx_contains_rtindex(ctx, boundary_rtindex))
    {
      Var *boundary_var = var;
      AttrNumber boundary_attno = var->varattno;
      TargetEntry *boundary_tle =
        pg_vec_find_tle_by_resno(source_plan->targetlist, var->varattno);
      Node *boundary_expr = NULL;

      if (boundary_tle != NULL)
        boundary_expr = pg_vec_strip_implicit_casts((Node *) boundary_tle->expr);
      if (boundary_expr != NULL && IsA(boundary_expr, Var))
      {
        boundary_var = castNode(Var, boundary_expr);
        boundary_attno = boundary_var->varattno;
      }
      else if (var->varnosyn == boundary_rtindex &&
               var->varattnosyn > 0)
      {
        boundary_attno = var->varattnosyn;
      }

      if (resolved_plan != NULL)
        *resolved_plan = source_plan;
      node = pg_vec_make_input_boundary_var(boundary_var,
                                            boundary_rtindex,
                                            boundary_attno);
      resolve_depth--;
      return node;
    }
  }

  if (var->varnosyn != OUTER_VAR &&
      var->varnosyn != INNER_VAR &&
      var->varnosyn > 0 &&
      var->varattnosyn > 0 &&
      pg_vec_ctx_contains_rtindex(ctx, var->varnosyn))
  {
    if (resolved_plan != NULL)
      *resolved_plan = source_plan;
    node = pg_vec_make_input_boundary_var(var,
                                          var->varnosyn,
                                          var->varattnosyn);
    resolve_depth--;
    return node;
  }

  tle = pg_vec_find_tle_by_resno(source_plan->targetlist, var->varattno);
  if (tle == NULL) {
    if (resolved_plan != NULL)
      *resolved_plan = source_plan;
    resolve_depth--;
    return node;
  }

  node = pg_vec_resolve_var_through_plan_with_source((Node *) tle->expr,
                                                     source_plan,
                                                     ctx,
                                                     resolved_plan);
  resolve_depth--;
  return node;
}

static Node *
pg_vec_resolve_var_through_plan(Node *node, Plan *plan,
                                const PgVecLowerContext *ctx)
{
  return pg_vec_resolve_var_through_plan_with_source(node, plan, ctx, NULL);
}

static bool
pg_vec_lower_var_with_source(Var *var, const PgVecLowerContext *ctx,
                             PgVecPlan *plan, Plan *source_plan,
                             PgVecColumnRef *column_ref) {
  PgVecScalarKind scalar_kind = PG_VEC_SCALAR_INVALID;

  if (var == NULL || var->varattno <= 0)
    return false;

  if (var->varno == OUTER_VAR || var->varno == INNER_VAR)
    return false;

  for (int i = 0; i < ctx->ninputs; i++) {
    if (ctx->inputs[i].rtindex != var->varno)
      continue;

    if (plan->inputs[ctx->inputs[i].input_id].kind ==
        PG_VEC_INPUT_DERIVED_GROUPED_AGG) {
      int output_slot = var->varattno - 1;
      int output_idx;
      PgVecDerivedAggInputSpec *derived =
          &plan->inputs[ctx->inputs[i].input_id].derived;

      if (output_slot < 0 || output_slot >= PG_VEC_MAX_OUTPUT_COLUMNS)
        return false;
      output_idx = derived->output_map[output_slot];
      if (output_idx < 0 || output_idx >= derived->agg.noutputs)
        return false;
      if (derived->agg.outputs[output_idx].root < 0 ||
          derived->agg.outputs[output_idx].root >=
              derived->agg.outputs[output_idx].nnodes)
        return false;

      scalar_kind =
          derived->agg.outputs[output_idx]
              .nodes[derived->agg.outputs[output_idx].root]
              .scalar_kind;
    } else if (!pg_vec_scalar_kind_from_pg_type(var->vartype, var->vartypmod,
                                                &scalar_kind))
      return false;

    if (plan->inputs[ctx->inputs[i].input_id].relid != InvalidOid)
    {
      Relation rel = RelationIdGetRelation(plan->inputs[ctx->inputs[i].input_id].relid);
      int natts = 0;

      if (rel != NULL)
      {
        natts = RelationGetNumberOfAttributes(rel);
        RelationClose(rel);
      }
      if (natts > 0 && var->varattno > natts)
      {
        Plan *lookup_plan = source_plan;

        while (lookup_plan != NULL)
        {
          TargetEntry *boundary_tle = NULL;

          if (lookup_plan->targetlist != NIL)
            boundary_tle =
                pg_vec_find_tle_by_resno(lookup_plan->targetlist, var->varattno);
          if (boundary_tle != NULL)
          {
            Node *boundary_expr =
                pg_vec_strip_implicit_casts((Node *) boundary_tle->expr);

            if (boundary_expr != NULL &&
                IsA(boundary_expr, Var) &&
                !equal(boundary_expr, var))
              return pg_vec_lower_var_with_source(castNode(Var, boundary_expr),
                                                  ctx,
                                                  plan,
                                                  lookup_plan,
                                                  column_ref);
          }

          if (lookup_plan->righttree == NULL &&
              lookup_plan->lefttree != NULL &&
              (IsA(lookup_plan, Agg) ||
               IsA(lookup_plan, Sort) ||
               IsA(lookup_plan, Material) ||
               IsA(lookup_plan, Hash)))
          {
            lookup_plan = lookup_plan->lefttree;
            continue;
          }
          break;
        }

      }
    }

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
pg_vec_lower_var(Var *var, const PgVecLowerContext *ctx,
                 PgVecPlan *plan, PgVecColumnRef *column_ref)
{
  return pg_vec_lower_var_with_source(var, ctx, plan, NULL, column_ref);
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
pg_vec_try_lower_substring_prefix2_expr(FuncExpr *func,
                                        Plan *source_plan,
                                        const PgVecLowerContext *ctx,
                                        PgVecPlan *plan,
                                        PgVecExprProgram *program,
                                        int *expr_root)
{
  const char *func_name;
  Node *arg_node;
  Const *start_const;
  Const *len_const;
  PgVecExprNode expr_node;
  int arg_root;

  if (func == NULL || list_length(func->args) != 3)
    return false;

  func_name = get_func_name(func->funcid);
  if (func_name == NULL ||
      (strcmp(func_name, "text_substr") != 0 &&
       strcmp(func_name, "bpchar_substr") != 0 &&
       strcmp(func_name, "substring") != 0 &&
       strcmp(func_name, "substr") != 0))
    return false;

  start_const = castNode(Const, pg_vec_strip_implicit_casts(lsecond(func->args)));
  len_const = castNode(Const, pg_vec_strip_implicit_casts(lthird(func->args)));
  if (!IsA(pg_vec_strip_implicit_casts(lsecond(func->args)), Const) ||
      !IsA(pg_vec_strip_implicit_casts(lthird(func->args)), Const))
    return false;
  if (start_const->constisnull || len_const->constisnull ||
      start_const->consttype != INT4OID || len_const->consttype != INT4OID)
    return false;
  if (DatumGetInt32(start_const->constvalue) != 1 ||
      DatumGetInt32(len_const->constvalue) != 2)
    return false;

  arg_node = linitial(func->args);
  if (!pg_vec_lower_expr(arg_node, source_plan, ctx, plan, program, &arg_root))
    return false;
  if (program->nodes[arg_root].scalar_kind != PG_VEC_SCALAR_STRING128)
    return false;

  MemSet(&expr_node, 0, sizeof(expr_node));
  expr_node.kind = PG_VEC_EXPR_SUBSTRING_PREFIX2;
  expr_node.scalar_kind = PG_VEC_SCALAR_STRING128;
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

  node = pg_vec_resolve_var_through_plan_with_source(node, source_plan, ctx,
                                                     &expr_source_plan);
  node = pg_vec_strip_implicit_casts(node);
  if (node == NULL)
    return false;

  if (IsA(node, Var)) {
    Var *resolved_var = castNode(Var, node);

    if (resolved_var->varno != OUTER_VAR &&
        resolved_var->varno != INNER_VAR)
    {
      for (int input_idx = 0; input_idx < ctx->ninputs; input_idx++)
      {
        if (ctx->inputs[input_idx].rtindex != resolved_var->varno ||
            ctx->inputs[input_idx].relid == InvalidOid)
          continue;

        {
          Relation rel = RelationIdGetRelation(ctx->inputs[input_idx].relid);
          int natts = 0;

          if (rel != NULL)
          {
            natts = RelationGetNumberOfAttributes(rel);
            RelationClose(rel);
          }
          (void) natts;
        }
        break;
      }
    }

    MemSet(&expr_node, 0, sizeof(expr_node));
    expr_node.kind = PG_VEC_EXPR_COLUMN;
    if (!pg_vec_lower_var_with_source(resolved_var,
                                      ctx,
                                      plan,
                                      expr_source_plan,
                                      &expr_node.column))
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
    if (pg_vec_try_lower_substring_prefix2_expr(funcexpr,
                                                expr_source_plan,
                                                ctx,
                                                plan,
                                                program,
                                                expr_root))
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
  PgVecExprProgram saved_program;
  PgVecScalarKind const_kind = PG_VEC_SCALAR_INVALID;
  PgVecScalarKind other_kind = PG_VEC_SCALAR_INVALID;

  left = pg_vec_resolve_var_through_plan(left, source_plan, ctx);
  right = pg_vec_resolve_var_through_plan(right, source_plan, ctx);
  left = pg_vec_strip_implicit_casts(left);
  right = pg_vec_strip_implicit_casts(right);

  if (pg_vec_resolve_constlike_node(left, ctx, &left_runtime_const, &left_const) &&
      !pg_vec_resolve_constlike_node(right, ctx, &right_runtime_const,
                                     &right_const)) {
    if (!pg_vec_lower_expr(right, source_plan, ctx, plan, &filter->exprs,
                           right_root))
      return false;
    other_kind = filter->exprs.nodes[*right_root].scalar_kind;
    saved_program = filter->exprs;
    if (left_const->consttype == NUMERICOID &&
        pg_vec_scalar_kind_is_numeric(other_kind))
    {
      bool got_kind;
      bool kinds_ok;
      bool lowered;

      got_kind = pg_vec_scalar_kind_from_const(left_const,
                                               false,
                                               PG_VEC_SCALAR_INVALID,
                                               &const_kind);
      kinds_ok = got_kind &&
                 pg_vec_scalar_kinds_compare_compatible(const_kind, other_kind);
      lowered = kinds_ok &&
                pg_vec_lower_expr_internal((Node *) left_const,
                                           source_plan,
                                           ctx,
                                           plan,
                                           &filter->exprs,
                                           false,
                                           PG_VEC_SCALAR_INVALID,
                                           left_root);
      if (!lowered)
      {
        filter->exprs = saved_program;
        if (!pg_vec_lower_expr_internal((Node *) left_const,
                                        source_plan,
                                        ctx,
                                        plan,
                                        &filter->exprs,
                                        true,
                                        other_kind,
                                        left_root))
          return false;
      }
    }
    else if (!pg_vec_lower_expr_internal(
                 (Node *) left_const, source_plan, ctx, plan, &filter->exprs,
                 true, other_kind, left_root))
    {
      filter->exprs = saved_program;
      if (!pg_vec_scalar_kind_from_const(left_const,
                                         false,
                                         PG_VEC_SCALAR_INVALID,
                                         &const_kind) ||
          !pg_vec_scalar_kinds_compare_compatible(const_kind, other_kind) ||
          !pg_vec_lower_expr_internal((Node *) left_const,
                                      source_plan,
                                      ctx,
                                      plan,
                                      &filter->exprs,
                                      false,
                                      PG_VEC_SCALAR_INVALID,
                                      left_root))
      {
        filter->exprs = saved_program;
        return false;
      }
    }
    if (!pg_vec_scalar_kinds_compare_compatible(filter->exprs.nodes[*left_root].scalar_kind,
                                                filter->exprs.nodes[*right_root].scalar_kind)) {
      filter->exprs = saved_program;
      return false;
    }
    return true;
  }

  if (!pg_vec_lower_expr(left, source_plan, ctx, plan, &filter->exprs,
                         left_root))
    return false;

  if (pg_vec_resolve_constlike_node(right, ctx, &right_runtime_const, &right_const)) {
    other_kind = filter->exprs.nodes[*left_root].scalar_kind;
    saved_program = filter->exprs;
    if (right_const->consttype == NUMERICOID &&
        pg_vec_scalar_kind_is_numeric(other_kind))
    {
      bool got_kind;
      bool kinds_ok;
      bool lowered;

      got_kind = pg_vec_scalar_kind_from_const(right_const,
                                               false,
                                               PG_VEC_SCALAR_INVALID,
                                               &const_kind);
      kinds_ok = got_kind &&
                 pg_vec_scalar_kinds_compare_compatible(other_kind, const_kind);
      lowered = kinds_ok &&
                pg_vec_lower_expr_internal((Node *) right_const,
                                           source_plan,
                                           ctx,
                                           plan,
                                           &filter->exprs,
                                           false,
                                           PG_VEC_SCALAR_INVALID,
                                           right_root);
      if (!lowered)
      {
        filter->exprs = saved_program;
        if (!pg_vec_lower_expr_internal((Node *) right_const,
                                        source_plan,
                                        ctx,
                                        plan,
                                        &filter->exprs,
                                        true,
                                        other_kind,
                                        right_root))
          return false;
      }
    }
    else if (!pg_vec_lower_expr_internal((Node *) right_const,
                                         source_plan,
                                         ctx,
                                         plan,
                                         &filter->exprs,
                                         true,
                                         other_kind,
                                         right_root))
    {
      filter->exprs = saved_program;
      if (!pg_vec_scalar_kind_from_const(right_const,
                                         false,
                                         PG_VEC_SCALAR_INVALID,
                                         &const_kind) ||
          !pg_vec_scalar_kinds_compare_compatible(other_kind, const_kind) ||
          !pg_vec_lower_expr_internal((Node *) right_const,
                                      source_plan,
                                      ctx,
                                      plan,
                                      &filter->exprs,
                                      false,
                                      PG_VEC_SCALAR_INVALID,
                                      right_root))
        return false;
    }
  } else if (!pg_vec_lower_expr(right, source_plan, ctx, plan, &filter->exprs,
                                right_root))
    return false;

  if (!pg_vec_scalar_kinds_compare_compatible(filter->exprs.nodes[*left_root].scalar_kind,
                                              filter->exprs.nodes[*right_root].scalar_kind))
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
    if (!pg_vec_scalar_kinds_compare_compatible(
            filter->exprs.nodes[left_root].scalar_kind,
            filter->exprs.nodes[right_root].scalar_kind))
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

  node = pg_vec_resolve_var_through_plan(node, source_plan, ctx);
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
      like_op = PG_VEC_OP_SQL_LIKE;

    if (!pg_vec_lower_expr(left, source_plan, ctx, plan, &filter->exprs,
                           &left_root))
      return false;

    MemSet(&rhs_node, 0, sizeof(rhs_node));
    rhs_node.kind = PG_VEC_EXPR_CONST;
    rhs_node.scalar_kind = PG_VEC_SCALAR_STRING128;
    rhs_node.left = -1;
    rhs_node.right = -1;
    if (like_op == PG_VEC_OP_SQL_LIKE)
    {
      if (!pg_vec_lower_const_value(prefix_const,
                                    PG_VEC_SCALAR_STRING128,
                                    &rhs_node.constant))
        return false;
    }
    else
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

  if (strcmp(op_name, "!~~") == 0) {
    PgVecExprNode rhs_node;
    PgVecFilterOp like_op;

    prefix_const = castNode(Const, pg_vec_strip_implicit_casts(right));
    if (!IsA(pg_vec_strip_implicit_casts(right), Const))
      return false;

    if (pg_vec_try_parse_prefix_like(prefix_const, &prefix_value))
      like_op = PG_VEC_OP_NOT_PREFIX_LIKE;
    else if (pg_vec_try_parse_contains_like(prefix_const, &prefix_value))
      like_op = PG_VEC_OP_NOT_CONTAINS_LIKE;
    else
      like_op = PG_VEC_OP_NOT_SQL_LIKE;

    if (!pg_vec_lower_expr(left, source_plan, ctx, plan, &filter->exprs,
                           &left_root))
      return false;

    MemSet(&rhs_node, 0, sizeof(rhs_node));
    rhs_node.kind = PG_VEC_EXPR_CONST;
    rhs_node.scalar_kind = PG_VEC_SCALAR_STRING128;
    rhs_node.left = -1;
    rhs_node.right = -1;
    if (like_op == PG_VEC_OP_NOT_SQL_LIKE)
    {
      if (!pg_vec_lower_const_value(prefix_const,
                                    PG_VEC_SCALAR_STRING128,
                                    &rhs_node.constant))
        return false;
    }
    else
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

  if (!pg_vec_scalar_kinds_compare_compatible(
          filter->exprs.nodes[left_root].scalar_kind,
          filter->exprs.nodes[right_root].scalar_kind))
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
                                                             NULL,
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
  Node *case_result;

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
  case_result = pg_vec_prepare_agg_arg_node((Node *) case_when->result, ctx);
  if (!pg_vec_lower_expr(case_result, source_plan, ctx, plan,
                         &agg_call->expr, &agg_call->expr.root))
    return false;
  if (!pg_vec_lower_filter_quals(list_make1(case_when->expr), source_plan, ctx,
                                 plan, &agg_call->filter))
    return false;

  agg_call->has_filter = true;
  agg_call->zero_if_empty = true;
  return true;
}

static Node *
pg_vec_prepare_agg_arg_node(Node *node, const PgVecLowerContext *ctx)
{
  if (node == NULL || ctx == NULL)
    return node;

  return expression_tree_mutator(node,
                                 pg_vec_prepare_agg_arg_mutator,
                                 (void *) ctx);
}

static bool
pg_vec_try_make_boundary_agg_var(Var *var,
                                 const PgVecLowerContext *ctx,
                                 Node **rewritten)
{
  if (rewritten != NULL)
    *rewritten = NULL;
  if (var == NULL || ctx == NULL)
    return false;
  if ((var->varno != OUTER_VAR && var->varno != INNER_VAR) ||
      var->varnosyn <= 0 ||
      var->varattnosyn <= 0 ||
      !pg_vec_ctx_contains_rtindex(ctx, var->varnosyn))
    return false;

  for (int input_idx = 0; input_idx < ctx->ninputs; input_idx++)
  {
    if (ctx->inputs[input_idx].rtindex != var->varnosyn)
      continue;

    if (ctx->inputs[input_idx].relid != InvalidOid)
    {
      Relation rel = RelationIdGetRelation(ctx->inputs[input_idx].relid);
      int natts = 0;

      if (rel != NULL)
      {
        natts = RelationGetNumberOfAttributes(rel);
        RelationClose(rel);
      }
      if (natts > 0 && var->varattnosyn <= natts)
      {
        if (rewritten != NULL)
          *rewritten = pg_vec_make_input_boundary_var(var,
                                                      var->varnosyn,
                                                      var->varattnosyn);
        return rewritten == NULL || *rewritten != NULL;
      }
    }
    break;
  }

  return false;
}

static Node *
pg_vec_prepare_agg_arg_mutator(Node *node, void *context)
{
  const PgVecLowerContext *ctx = (const PgVecLowerContext *) context;
  Node *rewritten = NULL;

  if (node == NULL)
    return NULL;

  if (IsA(node, Var) &&
      pg_vec_try_make_boundary_agg_var(castNode(Var, node),
                                       ctx,
                                       &rewritten))
    return rewritten;

  return expression_tree_mutator(node,
                                 pg_vec_prepare_agg_arg_mutator,
                                 context);
}

static bool pg_vec_lower_agg_call(Aggref *aggref, Plan *source_plan,
                                  const PgVecLowerContext *ctx, PgVecPlan *plan,
                                  int *agg_idx) {
  PgVecAggCall *agg_call;
  Aggref *logical_aggref;
  Plan *logical_source_plan;
  Plan *agg_input_plan;
  TargetEntry *arg_tle;
  Node *agg_arg;

  logical_aggref =
      pg_vec_resolve_logical_aggref(aggref, source_plan, &logical_source_plan);
  if (logical_aggref == NULL)
    return false;

  if (logical_aggref->aggorder != NIL || logical_aggref->aggfilter != NULL)
    return false;

  agg_call = palloc0(sizeof(*agg_call));
  pg_vec_expr_program_init(&agg_call->expr);
  pg_vec_filter_spec_init(&agg_call->filter);
  if (!pg_vec_agg_kind_from_aggref(logical_aggref, &agg_call->kind))
    return false;

  /*
   * Aggregate arguments are evaluated against the input below the logical Agg
   * node, not against the Agg node's own targetlist. Keeping the Agg plan here
   * can recurse back into the same aggregate output when a derived grouped
   * input exposes Aggref target entries, as in Q15.
   */
  agg_input_plan = logical_source_plan;
  if (agg_input_plan != NULL &&
      IsA(agg_input_plan, Agg) &&
      castNode(Agg, agg_input_plan)->plan.lefttree != NULL)
    agg_input_plan = castNode(Agg, agg_input_plan)->plan.lefttree;

  if (logical_aggref->aggstar) {
    if (logical_aggref->aggdistinct != NIL)
      return false;
    if (agg_call->kind != PG_VEC_AGG_COUNT)
      return false;
    agg_call->star_arg = true;
    agg_call->expr.root = -1;
  } else if (logical_aggref->aggdistinct != NIL) {
    if (agg_call->kind != PG_VEC_AGG_COUNT)
      return false;
    if (list_length(logical_aggref->args) != 1)
      return false;

    arg_tle = linitial_node(TargetEntry, logical_aggref->args);
    if (arg_tle->resjunk)
      return false;
    agg_arg = pg_vec_prepare_agg_arg_node((Node *) arg_tle->expr, ctx);
    if (!pg_vec_lower_expr(agg_arg,
                           agg_input_plan,
                           ctx,
                           plan,
                           &agg_call->expr,
                           &agg_call->expr.root))
    {
      if (logical_source_plan == agg_input_plan)
        return false;
      pg_vec_expr_program_init(&agg_call->expr);
      if (!pg_vec_lower_expr(agg_arg,
                             logical_source_plan,
                             ctx,
                             plan,
                             &agg_call->expr,
                             &agg_call->expr.root))
        return false;
    }
    if (agg_call->expr.root < 0 ||
        agg_call->expr.nodes[agg_call->expr.root].scalar_kind != PG_VEC_SCALAR_INT32)
      return false;
    agg_call->distinct = true;
  } else if (!pg_vec_try_lower_conditional_agg(logical_aggref,
                                               agg_input_plan,
                                               ctx,
                                               plan,
                                               agg_call) &&
             !(logical_source_plan != agg_input_plan &&
               (pg_vec_expr_program_init(&agg_call->expr),
                pg_vec_filter_spec_init(&agg_call->filter),
                pg_vec_try_lower_conditional_agg(logical_aggref,
                                                logical_source_plan,
                                                ctx,
                                                plan,
                                                agg_call)))) {
    if (list_length(logical_aggref->args) != 1)
      return false;

    arg_tle = linitial_node(TargetEntry, logical_aggref->args);
    if (arg_tle->resjunk)
      return false;
    agg_arg = pg_vec_prepare_agg_arg_node((Node *) arg_tle->expr, ctx);
    if (!pg_vec_lower_expr(agg_arg, agg_input_plan, ctx, plan,
                           &agg_call->expr, &agg_call->expr.root))
    {
      if (logical_source_plan == agg_input_plan)
        return false;
      pg_vec_expr_program_init(&agg_call->expr);
      if (!pg_vec_lower_expr(agg_arg,
                             logical_source_plan,
                             ctx,
                             plan,
                             &agg_call->expr,
                             &agg_call->expr.root))
        return false;
    }
  }

  return pg_vec_add_agg_call(&plan->agg, agg_call, agg_idx);
}

static bool pg_vec_lower_output_expr_internal(Node *node, Plan *source_plan,
                                              const PgVecLowerContext *ctx,
                                              PgVecPlan *plan,
                                              PgVecOutputExprProgram *program,
                                              bool has_expected_kind,
                                              PgVecScalarKind expected_kind,
                                              int *expr_root) {
  static int output_depth = 0;
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

  output_depth++;
  if (output_depth > 128)
  {
    output_depth--;
    return false;
  }
  resolved_node = pg_vec_resolve_var_through_plan_with_source(node,
                                                              source_plan,
                                                              ctx,
                                                              &resolved_source_plan);
  resolved_node = pg_vec_strip_implicit_casts(resolved_node);
  if (resolved_node == NULL)
  {
    output_depth--;
    return false;
  }

  if (IsA(resolved_node, Param))
  {
    Param *param = castNode(Param, resolved_node);

    if (param->paramkind != PARAM_EXEC)
    {
      output_depth--;
      return false;
    }

    MemSet(&expr_node, 0, sizeof(expr_node));
    expr_node.kind = PG_VEC_OUTPUT_EXPR_PARAM;
    expr_node.left = -1;
    expr_node.right = -1;
    expr_node.index = param->paramid;
    if (has_expected_kind)
      expr_node.scalar_kind = expected_kind;
    else if (!pg_vec_scalar_kind_from_pg_type(param->paramtype,
                                              param->paramtypmod,
                                              &expr_node.scalar_kind))
    {
      output_depth--;
      return false;
    }

    {
      bool ok = pg_vec_add_output_expr_node(program, &expr_node, expr_root);
      output_depth--;
      return ok;
    }
  }

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
      bool ok = pg_vec_add_output_expr_node(program, &expr_node, expr_root);
      output_depth--;
      return ok;
    }
  }

  if (IsA(resolved_node, Aggref)) {
    aggref = castNode(Aggref, resolved_node);
    if (!pg_vec_lower_agg_call(aggref, resolved_source_plan, ctx, plan, &ref_idx))
    {
      output_depth--;
      return false;
    }

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
    bool ok = pg_vec_add_output_expr_node(program, &expr_node, expr_root);
    output_depth--;
    return ok;
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
    bool ok = pg_vec_add_output_expr_node(program, &expr_node, expr_root);
    output_depth--;
    return ok;
  }

  if (!IsA(resolved_node, OpExpr))
  {
    output_depth--;
    return false;
  }

  opexpr = castNode(OpExpr, resolved_node);
  if (list_length(opexpr->args) != 2)
    return false;

  op_name = get_opname(opexpr->opno);
  if (op_name == NULL)
  {
    output_depth--;
    return false;
  }

  if (!pg_vec_lower_output_expr(linitial(opexpr->args), resolved_source_plan, ctx, plan,
                                program, &left_root) ||
      !pg_vec_lower_output_expr(lsecond(opexpr->args), resolved_source_plan, ctx, plan,
                                program, &right_root))
  {
    output_depth--;
    return false;
  }

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
  {
    output_depth--;
    return false;
  }

  if (!pg_vec_resolve_binary_output_kind(expr_node.kind, left_kind, right_kind,
                                         &result_kind))
  {
    output_depth--;
    return false;
  }

  expr_node.scalar_kind = result_kind;
  expr_node.left = left_root;
  expr_node.right = right_root;
  bool ok = pg_vec_add_output_expr_node(program, &expr_node, expr_root);
  output_depth--;
  return ok;
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
pg_vec_scalar_kind_is_numeric(PgVecScalarKind scalar_kind)
{
  switch (scalar_kind) {
    case PG_VEC_SCALAR_DECIMAL64_S2:
    case PG_VEC_SCALAR_DECIMAL128_S2:
    case PG_VEC_SCALAR_DECIMAL128_S4:
    case PG_VEC_SCALAR_DECIMAL128_S6:
      return true;
    default:
      return false;
  }
}

static bool
pg_vec_scalar_kinds_compare_compatible(PgVecScalarKind left_kind,
                                       PgVecScalarKind right_kind)
{
  if (left_kind == right_kind)
    return true;
  if (pg_vec_scalar_kind_is_numeric(left_kind) &&
      pg_vec_scalar_kind_is_numeric(right_kind))
    return true;

  switch (left_kind) {
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
  Node *left_resolved = pg_vec_resolve_var_through_plan(left, source_plan, ctx);
  Node *right_resolved = pg_vec_resolve_var_through_plan(right, source_plan, ctx);
  Const left_runtime_const;
  Const right_runtime_const;
  Const *left_const = NULL;
  Const *right_const = NULL;

  left_resolved = pg_vec_strip_implicit_casts(left_resolved);
  right_resolved = pg_vec_strip_implicit_casts(right_resolved);

  if (IsA(left_resolved, Param) &&
      castNode(Param, left_resolved)->paramkind == PARAM_EXEC &&
      !IsA(right_resolved, Param))
  {
    PgVecOutputExprProgram tmp_program = filter->exprs;

    if (!pg_vec_lower_output_expr(right_resolved, source_plan, ctx, plan,
                                  &filter->exprs, right_root))
      return false;
    if (!pg_vec_lower_output_expr_internal(left_resolved,
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
    return filter->exprs.nodes[*left_root].scalar_kind ==
               filter->exprs.nodes[*right_root].scalar_kind ||
           pg_vec_scalar_kinds_numeric_compatible(
               filter->exprs.nodes[*left_root].scalar_kind,
               filter->exprs.nodes[*right_root].scalar_kind);
  }

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

  if (IsA(right_resolved, Param) &&
      castNode(Param, right_resolved)->paramkind == PARAM_EXEC)
  {
    if (!pg_vec_lower_output_expr_internal(right_resolved,
                                           source_plan,
                                           ctx,
                                           plan,
                                           &filter->exprs,
                                           true,
                                           filter->exprs.nodes[*left_root].scalar_kind,
                                           right_root))
      return false;
    return true;
  }

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
pg_vec_filter_has_qual_kind(const PgVecFilterSpec *filter, PgVecQualKind kind)
{
  if (filter == NULL)
    return false;

  for (int qual_idx = 0; qual_idx < filter->nnodes; qual_idx++)
  {
    if (filter->nodes[qual_idx].kind == kind)
      return true;
  }

  return false;
}

static bool
pg_vec_post_agg_filter_has_param(const PgVecPostAggFilterSpec *filter)
{
  if (filter == NULL)
    return false;

  for (int expr_idx = 0; expr_idx < filter->exprs.nnodes; expr_idx++)
  {
    if (filter->exprs.nodes[expr_idx].kind == PG_VEC_OUTPUT_EXPR_PARAM)
      return true;
  }

  return false;
}

static bool
pg_vec_plan_has_derived_grouped_input(const PgVecPlan *plan)
{
  if (plan == NULL)
    return false;

  for (int input_idx = 0; input_idx < plan->ninputs; input_idx++)
  {
    if (plan->inputs[input_idx].kind == PG_VEC_INPUT_DERIVED_GROUPED_AGG)
      return true;
  }

  return false;
}

static bool
pg_vec_plan_tree_has_outer_join(Plan *plan)
{
  Plan *stripped;

  if (plan == NULL)
    return false;

  stripped = pg_vec_strip_plan_wrappers(plan);
  if (stripped == NULL)
    return false;

  if (IsA(stripped, HashJoin) ||
      IsA(stripped, MergeJoin) ||
      IsA(stripped, NestLoop))
  {
    Join *join_node = castNode(Join, stripped);

    if (join_node->jointype == JOIN_LEFT ||
        join_node->jointype == JOIN_RIGHT ||
        join_node->jointype == JOIN_FULL)
      return true;
  }

  return pg_vec_plan_tree_has_outer_join(stripped->lefttree) ||
         pg_vec_plan_tree_has_outer_join(stripped->righttree);
}

static bool
pg_vec_plan_tree_contains_subqueryscan(Plan *plan)
{
  if (plan == NULL)
    return false;
  if (IsA(plan, SubqueryScan))
    return true;

  return pg_vec_plan_tree_contains_subqueryscan(plan->lefttree) ||
         pg_vec_plan_tree_contains_subqueryscan(plan->righttree);
}

static bool
pg_vec_join_subtree_has_nonelidable_agg_or_subquery(Plan *plan)
{
  plan = pg_vec_strip_plan_wrappers(plan);
  if (plan == NULL)
    return false;
  if (IsA(plan, SubqueryScan) || IsA(plan, Agg))
    return true;

  return pg_vec_join_subtree_has_nonelidable_agg_or_subquery(plan->lefttree) ||
         pg_vec_join_subtree_has_nonelidable_agg_or_subquery(plan->righttree);
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

static bool
pg_vec_lower_direct_derived_post_agg_operand(Node *node,
                                             Plan *source_plan,
                                             const PgVecLowerContext *ctx,
                                             PgVecPlan *plan,
                                             PgVecPostAggFilterSpec *filter,
                                             bool has_expected_kind,
                                             PgVecScalarKind expected_kind,
                                             int *expr_root)
{
  Node *resolved_node;
  Plan *resolved_source_plan = source_plan;
  PgVecOutputExprNode expr_node;
  int ref_idx;

  resolved_node = pg_vec_resolve_var_through_plan_with_source(node,
                                                              source_plan,
                                                              ctx,
                                                              &resolved_source_plan);
  resolved_node = pg_vec_strip_implicit_casts(resolved_node);
  if (resolved_node == NULL)
    return false;

  MemSet(&expr_node, 0, sizeof(expr_node));
  expr_node.left = -1;
  expr_node.right = -1;

  if (IsA(resolved_node, Aggref))
  {
    Aggref *aggref = castNode(Aggref, resolved_node);

    if (!pg_vec_lower_agg_call(aggref,
                               resolved_source_plan,
                               ctx,
                               plan,
                               &ref_idx))
      return false;
    expr_node.kind = PG_VEC_OUTPUT_EXPR_AGGREF;
    expr_node.index = ref_idx;
    if (plan->agg.aggs[ref_idx].star_arg)
      expr_node.scalar_kind = PG_VEC_SCALAR_INT32;
    else
      expr_node.scalar_kind =
          plan->agg.aggs[ref_idx]
              .expr.nodes[plan->agg.aggs[ref_idx].expr.root]
              .scalar_kind;
    return pg_vec_add_output_expr_node(&filter->exprs, &expr_node, expr_root);
  }

  {
    Const runtime_const;
    Const *constnode;
    PgVecExprProgram group_key_expr;
    PgVecScalarKind result_kind;

    if (pg_vec_resolve_constlike_node(resolved_node,
                                      ctx,
                                      &runtime_const,
                                      &constnode))
    {
      if (!pg_vec_scalar_kind_from_const(constnode,
                                         has_expected_kind,
                                         expected_kind,
                                         &result_kind))
        return false;
      expr_node.kind = PG_VEC_OUTPUT_EXPR_CONST;
      expr_node.scalar_kind = result_kind;
      if (!pg_vec_lower_const_value(constnode,
                                    result_kind,
                                    &expr_node.constant))
        return false;
      return pg_vec_add_output_expr_node(&filter->exprs, &expr_node, expr_root);
    }

    pg_vec_expr_program_init(&group_key_expr);
    if (!pg_vec_lower_expr(resolved_node,
                           resolved_source_plan,
                           ctx,
                           plan,
                           &group_key_expr,
                           &group_key_expr.root))
      return false;
    result_kind = group_key_expr.nodes[group_key_expr.root].scalar_kind;
    if (!pg_vec_add_group_key(&plan->agg, &group_key_expr, result_kind, &ref_idx))
      return false;
    expr_node.kind = PG_VEC_OUTPUT_EXPR_GROUP_KEY;
    expr_node.scalar_kind = result_kind;
    expr_node.index = ref_idx;
    return pg_vec_add_output_expr_node(&filter->exprs, &expr_node, expr_root);
  }
}

static bool
pg_vec_lower_direct_derived_post_agg_qual(Node *node,
                                          Plan *source_plan,
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

  node = pg_vec_strip_implicit_casts(node);
  if (node == NULL)
    return false;

  if (IsA(node, BoolExpr))
  {
    ListCell *lc;
    int combined_root = -1;

    bool_expr = castNode(BoolExpr, node);
    if (bool_expr->boolop != AND_EXPR)
      return false;

    foreach (lc, bool_expr->args)
    {
      int child_root;

      if (!pg_vec_lower_direct_derived_post_agg_qual((Node *) lfirst(lc),
                                                     source_plan,
                                                     ctx,
                                                     plan,
                                                     filter,
                                                     &child_root))
        return false;
      if (combined_root < 0)
        combined_root = child_root;
      else if (!pg_vec_add_post_agg_binary_qual(filter,
                                                PG_VEC_QUAL_AND,
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

  if (!pg_vec_lower_direct_derived_post_agg_operand(linitial(opexpr->args),
                                                    source_plan,
                                                    ctx,
                                                    plan,
                                                    filter,
                                                    false,
                                                    PG_VEC_SCALAR_INVALID,
                                                    &left_root))
    return false;
  if (!pg_vec_lower_direct_derived_post_agg_operand(lsecond(opexpr->args),
                                                    source_plan,
                                                    ctx,
                                                    plan,
                                                    filter,
                                                    true,
                                                    filter->exprs.nodes[left_root].scalar_kind,
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
pg_vec_append_direct_derived_post_agg_filter_quals(List *quals,
                                                   Plan *source_plan,
                                                   const PgVecLowerContext *ctx,
                                                   PgVecPlan *plan,
                                                   PgVecPostAggFilterSpec *filter)
{
  ListCell *lc;

  foreach (lc, quals)
  {
    int qual_root;

    if (!pg_vec_lower_direct_derived_post_agg_qual((Node *) lfirst(lc),
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
  int output_idx = 0;

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
      return false;

    plan->agg.noutputs++;
    output_idx++;
  }

  return plan->agg.noutputs > 0;
}

static bool
pg_vec_lower_direct_derived_agg_targetlist(List *targetlist,
                                           Plan *agg_plan,
                                           const PgVecLowerContext *ctx,
                                           PgVecPlan *plan)
{
  ListCell *lc;

  foreach (lc, targetlist)
  {
    TargetEntry *tle = lfirst_node(TargetEntry, lc);
    PgVecOutputExprProgram *output_expr;
    PgVecOutputExprNode expr_node;
    Node *resolved_node;
    Plan *resolved_source_plan = agg_plan;
    Aggref *aggref;
    int ref_idx;

    if (tle->resjunk)
      continue;
    if (plan->agg.noutputs >= PG_VEC_MAX_OUTPUT_COLUMNS)
      return false;

    output_expr = &plan->agg.outputs[plan->agg.noutputs];
    pg_vec_output_expr_program_init(output_expr);
    resolved_node = pg_vec_resolve_var_through_plan_with_source((Node *) tle->expr,
                                                                agg_plan,
                                                                ctx,
                                                                &resolved_source_plan);
    resolved_node = pg_vec_strip_implicit_casts(resolved_node);
    if (resolved_node == NULL)
      return false;

    MemSet(&expr_node, 0, sizeof(expr_node));
    expr_node.left = -1;
    expr_node.right = -1;

    if (IsA(resolved_node, Aggref))
    {
      aggref = castNode(Aggref, resolved_node);
      if (!pg_vec_lower_agg_call(aggref,
                                 resolved_source_plan,
                                 ctx,
                                 plan,
                                 &ref_idx))
        return false;
      expr_node.kind = PG_VEC_OUTPUT_EXPR_AGGREF;
      expr_node.index = ref_idx;
      if (plan->agg.aggs[ref_idx].star_arg)
        expr_node.scalar_kind = PG_VEC_SCALAR_INT32;
      else
        expr_node.scalar_kind =
            plan->agg.aggs[ref_idx]
                .expr.nodes[plan->agg.aggs[ref_idx].expr.root]
                .scalar_kind;
    }
    else
    {
      Const runtime_const;
      Const *constnode;
      PgVecExprProgram group_key_expr;
      PgVecScalarKind result_kind;

      if (pg_vec_resolve_constlike_node(resolved_node,
                                        ctx,
                                        &runtime_const,
                                        &constnode))
      {
        if (!pg_vec_scalar_kind_from_const(constnode,
                                           false,
                                           PG_VEC_SCALAR_INVALID,
                                           &result_kind))
          return false;
        expr_node.kind = PG_VEC_OUTPUT_EXPR_CONST;
        expr_node.scalar_kind = result_kind;
        if (!pg_vec_lower_const_value(constnode,
                                      result_kind,
                                      &expr_node.constant))
          return false;
      }
      else
      {
        pg_vec_expr_program_init(&group_key_expr);
        if (!pg_vec_lower_expr(resolved_node,
                               resolved_source_plan,
                               ctx,
                               plan,
                               &group_key_expr,
                               &group_key_expr.root))
          return false;
        result_kind = group_key_expr.nodes[group_key_expr.root].scalar_kind;
        if (!pg_vec_add_group_key(&plan->agg,
                                  &group_key_expr,
                                  result_kind,
                                  &ref_idx))
          return false;
        expr_node.kind = PG_VEC_OUTPUT_EXPR_GROUP_KEY;
        expr_node.scalar_kind = result_kind;
        expr_node.index = ref_idx;
      }
    }

    if (!pg_vec_add_output_expr_node(output_expr, &expr_node, &output_expr->root))
      return false;
    plan->agg.noutputs++;
  }

  return plan->agg.noutputs > 0;
}

static bool pg_vec_make_single_input_context(SeqScan *seqscan,
                                             PlannedStmt *plannedstmt,
                                             PgVecLowerContext *ctx) {
  ctx->ninputs = 1;
  ctx->inputs[0].rtindex = seqscan->scan.scanrelid;
  ctx->inputs[0].input_id = 0;
  ctx->inputs[0].relid =
      rt_fetch(seqscan->scan.scanrelid, plannedstmt->rtable)->relid;
  ctx->inputs[0].boundary_plan = &seqscan->scan.plan;
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
  ctx->inputs[*input_id].boundary_plan = &seqscan->scan.plan;
  plan->inputs[*input_id].relid = rte->relid;
  ctx->ninputs++;
  plan->ninputs = ctx->ninputs;
  return true;
}

static bool
pg_vec_populate_derived_grouped_agg_input_from_subplan(Plan *subplan,
                                                       PlannedStmt *plannedstmt,
                                                       QueryDesc *queryDesc,
                                                       PgVecInputSpec *input,
                                                       Index *boundary_rtindex)
{
  Agg *agg;
  Sort *group_sort = NULL;
  Plan *scan_plan;
  PgVecLowerContext subctx;
  PgVecPlan *derived_plan;
  PgVecPlan *nested_plan = NULL;
  PgVecJoinTreeInfo join_info;
  const char *failure_reason = NULL;
  SeqScan *seqscan;
  RangeTblEntry *rte;

  if (subplan == NULL || plannedstmt == NULL || queryDesc == NULL ||
      input == NULL)
    return false;

  if (IsA(subplan, Sort))
  {
    group_sort = castNode(Sort, subplan);
    if (group_sort->plan.righttree != NULL)
      return false;
    subplan = group_sort->plan.lefttree;
  }
  if (!IsA(subplan, Agg))
    return false;

  agg = castNode(Agg, subplan);
  if (!pg_vec_top_agg_split_supported(agg))
    return false;
  scan_plan = agg->plan.lefttree;
  if (scan_plan != NULL && IsA(scan_plan, Sort))
  {
    group_sort = castNode(Sort, scan_plan);
    if (group_sort->plan.righttree != NULL)
      return false;
    scan_plan = group_sort->plan.lefttree;
  }
  if (agg->plan.righttree != NULL)
    return false;

  MemSet(&subctx, 0, sizeof(subctx));
  MemSet(&join_info, 0, sizeof(join_info));
  subctx.queryDesc = queryDesc;
  derived_plan = palloc0(sizeof(*derived_plan));
  pg_vec_plan_init(derived_plan);
  derived_plan->kind = PG_VEC_PLAN_SCAN_FILTER_AGG;
  derived_plan->agg.grouped = (agg->numCols > 0);
  input->derived.subplan = NULL;
  for (int idx = 0; idx < PG_VEC_MAX_OUTPUT_COLUMNS; idx++)
    input->derived.output_map[idx] = -1;

  if (scan_plan == NULL)
    return false;

  if (IsA(scan_plan, SeqScan))
  {
    seqscan = castNode(SeqScan, scan_plan);
    rte = rt_fetch(seqscan->scan.scanrelid, plannedstmt->rtable);
    if (rte == NULL || rte->rtekind != RTE_RELATION)
      return false;

    derived_plan->ninputs = 1;
    derived_plan->inputs[0].kind = PG_VEC_INPUT_RELATION;
    derived_plan->inputs[0].relid = rte->relid;
    if (!pg_vec_make_single_input_context(seqscan, plannedstmt, &subctx))
      return false;
  }
  else
  {
    MemoryContext oldcxt;

    oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
    nested_plan = palloc0(sizeof(*nested_plan));
    MemoryContextSwitchTo(oldcxt);

    pg_vec_plan_init(nested_plan);
    nested_plan->kind = PG_VEC_PLAN_SCAN_FILTER_AGG;
    nested_plan->agg.grouped = (agg->numCols > 0);
    if (!pg_vec_lower_join_tree(scan_plan,
                                plannedstmt,
                                &subctx,
                                nested_plan,
                                &join_info,
                                &failure_reason))
      return false;
    nested_plan->njoins = join_info.njoins;
    for (int join_idx = 0; join_idx < join_info.njoins; join_idx++)
      nested_plan->joins[join_idx] = join_info.joins[join_idx];
  }

  /*
   * A derived grouped input exposes the aggregate boundary itself to the outer
   * plan. Lower its targetlist/having through the direct-derived path even for
   * simple single-input Agg nodes; that avoids re-entering the generic output
   * lowering path against the same aggregate node and keeps derived outputs in
   * the "group key / aggref / const" domain we actually materialize.
   */
  if (!pg_vec_lower_direct_derived_agg_targetlist(agg->plan.targetlist,
                                                  &agg->plan,
                                                  &subctx,
                                                  nested_plan != NULL ? nested_plan : derived_plan))
    return false;
  for (int idx = 0; idx < (nested_plan != NULL ? nested_plan->agg.noutputs
                                               : derived_plan->agg.noutputs) &&
                    idx < PG_VEC_MAX_OUTPUT_COLUMNS;
       idx++)
    input->derived.output_map[idx] = idx;
  {
    PgVecPostAggFilterSpec *having =
        (nested_plan != NULL) ? &nested_plan->agg.having : &derived_plan->agg.having;

    if (!pg_vec_append_post_agg_filter_quals(agg->plan.qual,
                                             &agg->plan,
                                             &subctx,
                                             nested_plan != NULL ? nested_plan : derived_plan,
                                             having))
      return false;
  }
  if (!pg_vec_lower_grouped_input_sort_spec(agg,
                                            group_sort,
                                            nested_plan != NULL ? nested_plan : derived_plan))
    return false;
  if (nested_plan == NULL)
  {
    if (!pg_vec_lower_filter_quals(seqscan->scan.plan.qual,
                                   &seqscan->scan.plan,
                                   &subctx,
                                   derived_plan,
                                   &derived_plan->inputs[0].filter))
      return false;
  }

  input->kind = PG_VEC_INPUT_DERIVED_GROUPED_AGG;
  input->relid = InvalidOid;
  if (nested_plan != NULL)
  {
    input->derived.relid = InvalidOid;
    input->derived.nbase_columns = 0;
    pg_vec_filter_spec_init(&input->derived.base_filter);
    input->derived.agg = nested_plan->agg;
    input->derived.subplan = nested_plan;
    if (boundary_rtindex != NULL)
      *boundary_rtindex = (join_info.ninputs > 0)
                              ? subctx.inputs[join_info.first_input].rtindex
                              : InvalidOid;
    return true;
  }

  input->derived.relid = rte->relid;
  input->derived.nbase_columns = derived_plan->inputs[0].ncolumns;
  memcpy(input->derived.base_columns,
         derived_plan->inputs[0].columns,
         sizeof(PgVecColumnRef) * derived_plan->inputs[0].ncolumns);
  input->derived.base_filter = derived_plan->inputs[0].filter;
  input->derived.agg = derived_plan->agg;
  if (boundary_rtindex != NULL)
    *boundary_rtindex = seqscan->scan.scanrelid;
  return true;
}

static bool
pg_vec_add_derived_input_context(SubqueryScan *subqueryscan,
                                 PlannedStmt *plannedstmt,
                                 PgVecLowerContext *ctx,
                                 PgVecPlan *plan,
                                 int *input_id)
{
  RangeTblEntry *rte;

  if (subqueryscan == NULL || ctx->ninputs >= PG_VEC_MAX_INPUTS)
    return false;

  rte = rt_fetch(subqueryscan->scan.scanrelid, plannedstmt->rtable);
  if (rte == NULL ||
      (rte->rtekind != RTE_SUBQUERY && rte->rtekind != RTE_RELATION))
    return false;

  *input_id = ctx->ninputs;
  ctx->inputs[*input_id].rtindex = subqueryscan->scan.scanrelid;
  ctx->inputs[*input_id].input_id = *input_id;
  ctx->inputs[*input_id].relid = InvalidOid;
  ctx->inputs[*input_id].boundary_plan = &subqueryscan->scan.plan;
  if (!pg_vec_populate_derived_grouped_agg_input_from_subplan(subqueryscan->subplan,
                                                              plannedstmt,
                                                              ctx->queryDesc,
                                                              &plan->inputs[*input_id],
                                                              NULL))
    return false;

  if (!pg_vec_build_derived_output_map(subqueryscan->scan.plan.targetlist,
                                       subqueryscan->subplan->targetlist,
                                       plan->inputs[*input_id].derived.output_map))
    return false;

  ctx->ninputs++;
  plan->ninputs = ctx->ninputs;
  return true;
}

static bool
pg_vec_add_derived_agg_input_context(Plan *subplan,
                                     PlannedStmt *plannedstmt,
                                     PgVecLowerContext *ctx,
                                     PgVecPlan *plan,
                                     int *input_id)
{
  Index boundary_rtindex = InvalidOid;
  Plan *boundary_plan = subplan;

  if (subplan == NULL || ctx->ninputs >= PG_VEC_MAX_INPUTS)
    return false;

  *input_id = ctx->ninputs;
  ctx->inputs[*input_id].input_id = *input_id;
  ctx->inputs[*input_id].relid = InvalidOid;
  if (!pg_vec_populate_derived_grouped_agg_input_from_subplan(subplan,
                                                              plannedstmt,
                                                              ctx->queryDesc,
                                                              &plan->inputs[*input_id],
                                                              &boundary_rtindex))
    return false;
  if (boundary_rtindex == InvalidOid)
    return false;
  ctx->inputs[*input_id].rtindex = boundary_rtindex;
  while (boundary_plan != NULL &&
         (IsA(boundary_plan, Hash) ||
          IsA(boundary_plan, Material) ||
          IsA(boundary_plan, Sort)))
    boundary_plan = boundary_plan->lefttree;
  ctx->inputs[*input_id].boundary_plan = boundary_plan;

  ctx->ninputs++;
  plan->ninputs = ctx->ninputs;
  return true;
}

static bool
pg_vec_collect_correlated_scan_qual_node(Node *node,
                                         int param_id,
                                         Var **inner_join_var,
                                         List **remaining_quals)
{
  if (node == NULL)
    return true;

  node = pg_vec_strip_implicit_casts(node);
  if (node == NULL)
    return true;

  if (IsA(node, BoolExpr))
  {
    BoolExpr *bool_expr = castNode(BoolExpr, node);
    ListCell *lc;

    if (bool_expr->boolop != AND_EXPR)
      return false;

    foreach (lc, bool_expr->args)
    {
      if (!pg_vec_collect_correlated_scan_qual_node((Node *) lfirst(lc),
                                                    param_id,
                                                    inner_join_var,
                                                    remaining_quals))
        return false;
    }
    return true;
  }

  if (IsA(node, OpExpr))
  {
    OpExpr *opexpr = castNode(OpExpr, node);
    Node *left;
    Node *right;
    char *op_name;

    if (list_length(opexpr->args) == 2)
    {
      left = pg_vec_strip_implicit_casts(linitial(opexpr->args));
      right = pg_vec_strip_implicit_casts(lsecond(opexpr->args));
      op_name = get_opname(opexpr->opno);
      if (op_name != NULL && strcmp(op_name, "=") == 0)
      {
        if (IsA(left, Var) && IsA(right, Param) &&
            castNode(Param, right)->paramkind == PARAM_EXEC &&
            castNode(Param, right)->paramid == param_id)
        {
          if (*inner_join_var != NULL)
            return false;
          *inner_join_var = castNode(Var, left);
          return true;
        }
        if (IsA(right, Var) && IsA(left, Param) &&
            castNode(Param, left)->paramkind == PARAM_EXEC &&
            castNode(Param, left)->paramid == param_id)
        {
          if (*inner_join_var != NULL)
            return false;
          *inner_join_var = castNode(Var, right);
          return true;
        }
      }
    }
  }

  *remaining_quals = lappend(*remaining_quals, node);
  return true;
}

static bool
pg_vec_collect_correlated_scan_quals(List *quals,
                                     int param_id,
                                     Var **inner_join_var,
                                     List **remaining_quals)
{
  ListCell *lc;

  foreach (lc, quals)
  {
    if (!pg_vec_collect_correlated_scan_qual_node((Node *) lfirst(lc),
                                                  param_id,
                                                  inner_join_var,
                                                  remaining_quals))
      return false;
  }

  return true;
}

static bool
pg_vec_collect_multi_correlated_scan_qual_node(Node *node,
                                               List *param_ids,
                                               Var **inner_join_vars,
                                               List **remaining_quals)
{
  if (node == NULL)
    return true;

  node = pg_vec_strip_implicit_casts(node);
  if (node == NULL)
    return true;

  if (IsA(node, BoolExpr))
  {
    BoolExpr *bool_expr = castNode(BoolExpr, node);
    ListCell *lc;

    if (bool_expr->boolop != AND_EXPR)
      return false;

    foreach (lc, bool_expr->args)
    {
      if (!pg_vec_collect_multi_correlated_scan_qual_node((Node *) lfirst(lc),
                                                          param_ids,
                                                          inner_join_vars,
                                                          remaining_quals))
        return false;
    }
    return true;
  }

  if (IsA(node, OpExpr))
  {
    OpExpr *opexpr = castNode(OpExpr, node);
    Node *left;
    Node *right;
    char *op_name;

    if (list_length(opexpr->args) == 2)
    {
      left = pg_vec_strip_implicit_casts(linitial(opexpr->args));
      right = pg_vec_strip_implicit_casts(lsecond(opexpr->args));
      op_name = get_opname(opexpr->opno);
      if (op_name != NULL && strcmp(op_name, "=") == 0)
      {
        int param_pos = 0;
        ListCell *lc;

        foreach (lc, param_ids)
        {
          int param_id = lfirst_int(lc);

          if (IsA(left, Var) && IsA(right, Param) &&
              castNode(Param, right)->paramkind == PARAM_EXEC &&
              castNode(Param, right)->paramid == param_id)
          {
            if (inner_join_vars[param_pos] != NULL)
              return false;
            inner_join_vars[param_pos] = castNode(Var, left);
            return true;
          }
          if (IsA(right, Var) && IsA(left, Param) &&
              castNode(Param, left)->paramkind == PARAM_EXEC &&
              castNode(Param, left)->paramid == param_id)
          {
            if (inner_join_vars[param_pos] != NULL)
              return false;
            inner_join_vars[param_pos] = castNode(Var, right);
            return true;
          }
          param_pos++;
        }
      }
    }
  }

  *remaining_quals = lappend(*remaining_quals, node);
  return true;
}

static bool
pg_vec_collect_multi_correlated_scan_quals(List *quals,
                                           List *param_ids,
                                           Var **inner_join_vars,
                                           List **remaining_quals)
{
  ListCell *lc;

  foreach (lc, quals)
  {
    if (!pg_vec_collect_multi_correlated_scan_qual_node((Node *) lfirst(lc),
                                                        param_ids,
                                                        inner_join_vars,
                                                        remaining_quals))
      return false;
  }

  return true;
}

static bool
pg_vec_append_flattened_and_quals(List **flat_quals,
                                  Node *node)
{
  ListCell *lc;

  if (node == NULL)
    return true;

  node = pg_vec_strip_implicit_casts(node);
  if (node == NULL)
    return true;

  if (IsA(node, BoolExpr))
  {
    BoolExpr *bool_expr = castNode(BoolExpr, node);

    if (bool_expr->boolop != AND_EXPR)
    {
      *flat_quals = lappend(*flat_quals, node);
      return true;
    }

    foreach (lc, bool_expr->args)
    {
      if (!pg_vec_append_flattened_and_quals(flat_quals,
                                             (Node *) lfirst(lc)))
        return false;
    }
    return true;
  }

  *flat_quals = lappend(*flat_quals, node);
  return true;
}

static List *
pg_vec_flatten_and_quals(List *quals)
{
  List *flat_quals = NIL;
  ListCell *lc;

  foreach (lc, quals)
  {
    if (!pg_vec_append_flattened_and_quals(&flat_quals,
                                           (Node *) lfirst(lc)))
      return NIL;
  }

  return flat_quals;
}

static bool
pg_vec_strip_correlated_param_qual_from_plan(Plan *plan,
                                             int param_id,
                                             Var **inner_join_var)
{
  if (plan == NULL)
    return true;

  if (IsA(plan, SeqScan))
  {
    SeqScan *seqscan = castNode(SeqScan, plan);
    List *remaining_quals = NIL;

    if (!pg_vec_collect_correlated_scan_quals(seqscan->scan.plan.qual,
                                              param_id,
                                              inner_join_var,
                                              &remaining_quals))
      return false;
    seqscan->scan.plan.qual = remaining_quals;
    return true;
  }

  if (IsA(plan, HashJoin) || IsA(plan, MergeJoin) || IsA(plan, NestLoop))
  {
    List *remaining_joinquals = NIL;
    List *remaining_planquals = NIL;

    if (!pg_vec_collect_correlated_scan_quals(pg_vec_join_quals(plan),
                                              param_id,
                                              inner_join_var,
                                              &remaining_joinquals))
      return false;
    if (!pg_vec_collect_correlated_scan_quals(plan->qual,
                                              param_id,
                                              inner_join_var,
                                              &remaining_planquals))
      return false;

    if (IsA(plan, HashJoin))
      castNode(HashJoin, plan)->join.joinqual = remaining_joinquals;
    else if (IsA(plan, MergeJoin))
      castNode(MergeJoin, plan)->join.joinqual = remaining_joinquals;
    else
      castNode(NestLoop, plan)->join.joinqual = remaining_joinquals;
    plan->qual = remaining_planquals;

    return pg_vec_strip_correlated_param_qual_from_plan(plan->lefttree,
                                                        param_id,
                                                        inner_join_var) &&
           pg_vec_strip_correlated_param_qual_from_plan(plan->righttree,
                                                        param_id,
                                                        inner_join_var);
  }

  if (IsA(plan, Sort) || IsA(plan, Material) || IsA(plan, Hash) || IsA(plan, Agg))
    return pg_vec_strip_correlated_param_qual_from_plan(plan->lefttree,
                                                        param_id,
                                                        inner_join_var);

  return pg_vec_strip_correlated_param_qual_from_plan(plan->lefttree,
                                                      param_id,
                                                      inner_join_var) &&
         pg_vec_strip_correlated_param_qual_from_plan(plan->righttree,
                                                      param_id,
                                                      inner_join_var);
}

static Aggref *
pg_vec_make_q17_sum_aggref(const Aggref *template_aggref,
                           const Var *arg_var)
{
  Aggref *aggref;
  TargetEntry *arg_tle;

  if (template_aggref == NULL || arg_var == NULL)
    return NULL;

  aggref = makeNode(Aggref);
  aggref->aggfnoid = F_SUM_NUMERIC;
  aggref->aggtype = NUMERICOID;
  aggref->aggcollid = template_aggref->aggcollid;
  aggref->inputcollid = template_aggref->inputcollid;
  aggref->aggtranstype = template_aggref->aggtranstype;
  aggref->aggargtypes = list_make1_oid(arg_var->vartype);
  arg_tle = makeTargetEntry((Expr *) copyObject((Node *) arg_var), 1, NULL, false);
  aggref->args = list_make1(arg_tle);
  aggref->aggdirectargs = NIL;
  aggref->aggstar = false;
  aggref->aggvariadic = false;
  aggref->aggkind = 'n';
  aggref->aggfilter = NULL;
  aggref->aggorder = NIL;
  aggref->aggdistinct = NIL;
  aggref->aggpresorted = false;
  aggref->agglevelsup = 0;
  aggref->aggsplit = AGGSPLIT_SIMPLE;
  aggref->aggno = -1;
  aggref->aggtransno = -1;
  aggref->location = -1;
  return aggref;
}

static Aggref *
pg_vec_make_q17_count_star_aggref(const Aggref *template_aggref)
{
  Aggref *aggref;

  if (template_aggref == NULL)
    return NULL;

  aggref = makeNode(Aggref);
  aggref->aggfnoid = F_COUNT_;
  aggref->aggtype = INT8OID;
  aggref->aggcollid = InvalidOid;
  aggref->inputcollid = InvalidOid;
  aggref->aggtranstype = INT8OID;
  aggref->aggargtypes = NIL;
  aggref->aggdirectargs = NIL;
  aggref->args = NIL;
  aggref->aggorder = NIL;
  aggref->aggdistinct = NIL;
  aggref->aggfilter = NULL;
  aggref->aggstar = true;
  aggref->aggvariadic = false;
  aggref->aggkind = 'n';
  aggref->aggpresorted = false;
  aggref->agglevelsup = 0;
  aggref->aggsplit = AGGSPLIT_SIMPLE;
  aggref->aggno = -1;
  aggref->aggtransno = -1;
  aggref->location = -1;
  return aggref;
}

static bool
pg_vec_try_rewrite_q17_correlated_joinqual(List *quals,
                                           Plan *join_plan,
                                           PlannedStmt *plannedstmt,
                                           PgVecLowerContext *ctx,
                                           PgVecPlan *plan,
                                           int leftmost_input,
                                           PgVecJoinSpec *extra_join)
{
  OpExpr *compare_expr;
  Node *outer_expr;
  SubPlan *subplan_expr;
  Plan *subplan_plan;
  Agg *agg;
  Plan *scan_plan;
  SeqScan *seqscan;
  RangeTblEntry *rte;
  TargetEntry *target_tle;
  Node *target_expr;
  OpExpr *target_mul;
  Node *target_left;
  Node *target_right;
  Const *scale_const;
  Aggref *avg_aggref;
  Aggref *sum_aggref;
  Aggref *count_aggref;
  Var *avg_arg_var;
  Var *inner_join_var = NULL;
  Node *outer_join_arg;
  int param_id;
  List *remaining_quals = NIL;
  PgVecLowerContext subctx;
  PgVecPlan *derived_plan;
  PgVecInputSpec *input;
  int input_id;
  int noutputs;
  Node *derived_key_var;
  Node *derived_sum_var;
  Node *derived_count_var;
  Node *five_const;
  OpExpr *mul_outer_count;
  OpExpr *mul_outer_count_five;
  OpExpr *rewritten_compare;
  TargetEntry *group_tle;
  TargetEntry *sum_tle;
  TargetEntry *count_tle;
  PgVecColumnRef left_ref;
  PgVecColumnRef right_ref;
  ListCell *lc;
  char *compare_op_name;
  char *mul_op_name;

  if (quals == NIL || list_length(quals) != 1 || join_plan == NULL ||
      plannedstmt == NULL || ctx == NULL || plan == NULL || extra_join == NULL)
    return false;

  compare_expr = (OpExpr *) pg_vec_strip_implicit_casts((Node *) linitial(quals));
  if (!IsA(compare_expr, OpExpr) || list_length(compare_expr->args) != 2)
    return false;
  compare_op_name = get_opname(compare_expr->opno);
  if (compare_op_name == NULL || strcmp(compare_op_name, "<") != 0)
    return false;

  outer_expr = pg_vec_strip_implicit_casts(linitial(compare_expr->args));
  subplan_expr = (SubPlan *) pg_vec_strip_implicit_casts(lsecond(compare_expr->args));
  if (!IsA(subplan_expr, SubPlan))
    return false;
  if (subplan_expr->subLinkType != EXPR_SUBLINK || subplan_expr->isInitPlan ||
      subplan_expr->plan_id <= 0 || list_length(subplan_expr->parParam) != 1 ||
      list_length(subplan_expr->args) != 1)
    return false;

  param_id = linitial_int(subplan_expr->parParam);
  outer_join_arg = linitial(subplan_expr->args);
  subplan_plan = list_nth_node(Plan, plannedstmt->subplans, subplan_expr->plan_id - 1);
  while (subplan_plan != NULL &&
         !IsA(subplan_plan, Agg) &&
         (IsA(subplan_plan, Hash) || IsA(subplan_plan, Material) || IsA(subplan_plan, Sort)))
    subplan_plan = subplan_plan->lefttree;
  if (subplan_plan == NULL || !IsA(subplan_plan, Agg))
    return false;
  agg = castNode(Agg, subplan_plan);
  if (!pg_vec_top_agg_split_supported(agg) || agg->numCols != 0 ||
      agg->plan.qual != NIL || agg->plan.righttree != NULL)
    return false;

  scan_plan = agg->plan.lefttree;
  if (scan_plan != NULL && IsA(scan_plan, Sort))
    scan_plan = castNode(Sort, scan_plan)->plan.lefttree;
  scan_plan = pg_vec_strip_plan_wrappers(scan_plan);
  if (scan_plan == NULL || !IsA(scan_plan, SeqScan))
    return false;
  seqscan = castNode(SeqScan, scan_plan);
  rte = rt_fetch(seqscan->scan.scanrelid, plannedstmt->rtable);
  if (rte == NULL || rte->rtekind != RTE_RELATION)
    return false;

  target_tle = NULL;
  foreach (lc, agg->plan.targetlist)
  {
    TargetEntry *tle = lfirst_node(TargetEntry, lc);

    if (!tle->resjunk)
    {
      if (target_tle != NULL)
        return false;
      target_tle = tle;
    }
  }
  if (target_tle == NULL)
    return false;

  target_expr = pg_vec_strip_implicit_casts((Node *) target_tle->expr);
  if (!IsA(target_expr, OpExpr))
    return false;
  target_mul = castNode(OpExpr, target_expr);
  mul_op_name = get_opname(target_mul->opno);
  if (list_length(target_mul->args) != 2 || mul_op_name == NULL ||
      strcmp(mul_op_name, "*") != 0)
    return false;

  target_left = pg_vec_strip_implicit_casts(linitial(target_mul->args));
  target_right = pg_vec_strip_implicit_casts(lsecond(target_mul->args));
  if (IsA(target_left, Const) && IsA(target_right, Aggref))
  {
    scale_const = castNode(Const, target_left);
    avg_aggref = castNode(Aggref, target_right);
  }
  else if (IsA(target_right, Const) && IsA(target_left, Aggref))
  {
    scale_const = castNode(Const, target_right);
    avg_aggref = castNode(Aggref, target_left);
  }
  else
    return false;

  if (scale_const->constisnull || scale_const->consttype != NUMERICOID)
    return false;
  {
    int64 scaled_factor = 0;

    if (!pg_vec_numeric_to_scaled_int64(scale_const->constvalue, 1, &scaled_factor) ||
        scaled_factor != 2)
      return false;
  }
  if (avg_aggref->aggstar || avg_aggref->aggdistinct != NIL ||
      avg_aggref->aggorder != NIL || avg_aggref->aggfilter != NULL)
    return false;
  if (get_func_name(avg_aggref->aggfnoid) == NULL ||
      strcmp(get_func_name(avg_aggref->aggfnoid), "avg") != 0)
    return false;
  if (list_length(avg_aggref->args) != 1)
    return false;
  avg_arg_var = (Var *) pg_vec_strip_implicit_casts(
      (Node *) linitial_node(TargetEntry, avg_aggref->args)->expr);
  if (!IsA(avg_arg_var, Var))
    return false;

  if (!pg_vec_collect_correlated_scan_quals(seqscan->scan.plan.qual,
                                            param_id,
                                            &inner_join_var,
                                            &remaining_quals))
    return false;
  if (inner_join_var == NULL)
    return false;

  input_id = ctx->ninputs;
  if (input_id >= PG_VEC_MAX_INPUTS)
    return false;
  pg_vec_input_spec_init(&plan->inputs[input_id]);
  input = &plan->inputs[input_id];

  MemSet(&subctx, 0, sizeof(subctx));
  subctx.queryDesc = ctx->queryDesc;
  derived_plan = palloc0(sizeof(*derived_plan));
  pg_vec_plan_init(derived_plan);
  derived_plan->kind = PG_VEC_PLAN_SCAN_FILTER_AGG;
  derived_plan->ninputs = 1;
  derived_plan->inputs[0].kind = PG_VEC_INPUT_RELATION;
  derived_plan->inputs[0].relid = rte->relid;
  derived_plan->agg.grouped = true;
  if (!pg_vec_make_single_input_context(seqscan, plannedstmt, &subctx))
    return false;

  sum_aggref = pg_vec_make_q17_sum_aggref(avg_aggref, avg_arg_var);
  count_aggref = pg_vec_make_q17_count_star_aggref(avg_aggref);
  if (sum_aggref == NULL || count_aggref == NULL)
    return false;
  group_tle = makeTargetEntry((Expr *) copyObject(inner_join_var), 1, NULL, false);
  sum_tle = makeTargetEntry((Expr *) sum_aggref, 2, NULL, false);
  count_tle = makeTargetEntry((Expr *) count_aggref, 3, NULL, false);
  if (!pg_vec_lower_direct_derived_agg_targetlist(list_make3(group_tle, sum_tle, count_tle),
                                                  &agg->plan,
                                                  &subctx,
                                                  derived_plan))
    return false;
  if (!pg_vec_lower_filter_quals(remaining_quals,
                                 &seqscan->scan.plan,
                                 &subctx,
                                 derived_plan,
                                 &derived_plan->inputs[0].filter))
    return false;

  input->kind = PG_VEC_INPUT_DERIVED_GROUPED_AGG;
  input->relid = InvalidOid;
  input->derived.relid = rte->relid;
  input->derived.nbase_columns = derived_plan->inputs[0].ncolumns;
  memcpy(input->derived.base_columns,
         derived_plan->inputs[0].columns,
         sizeof(PgVecColumnRef) * derived_plan->inputs[0].ncolumns);
  input->derived.base_filter = derived_plan->inputs[0].filter;
  input->derived.agg = derived_plan->agg;
  for (int idx = 0; idx < PG_VEC_MAX_OUTPUT_COLUMNS; idx++)
    input->derived.output_map[idx] = -1;

  noutputs = input->derived.agg.noutputs;
  if (noutputs < 3)
    return false;
  for (int idx = 0; idx < noutputs; idx++)
    input->derived.output_map[idx] = idx;
  if (!pg_vec_add_scan_column(input,
                              input_id,
                              1,
                              input->derived.agg.outputs[0]
                                  .nodes[input->derived.agg.outputs[0].root]
                                  .scalar_kind) ||
      !pg_vec_add_scan_column(input,
                              input_id,
                              2,
                              input->derived.agg.outputs[1]
                                  .nodes[input->derived.agg.outputs[1].root]
                                  .scalar_kind) ||
      !pg_vec_add_scan_column(input,
                              input_id,
                              3,
                              input->derived.agg.outputs[2]
                                  .nodes[input->derived.agg.outputs[2].root]
                                  .scalar_kind))
    return false;

  ctx->inputs[input_id].rtindex = seqscan->scan.scanrelid;
  ctx->inputs[input_id].input_id = input_id;
  ctx->inputs[input_id].relid = InvalidOid;
  ctx->inputs[input_id].boundary_plan = &seqscan->scan.plan;
  ctx->ninputs++;
  plan->ninputs = ctx->ninputs;

  MemSet(extra_join, 0, sizeof(*extra_join));
  extra_join->kind = PG_VEC_JOIN_INNER;
  extra_join->left_input = leftmost_input;
  extra_join->right_input = input_id;
  pg_vec_filter_spec_init(&extra_join->filter);

  if (!pg_vec_lower_join_key_expr(outer_join_arg,
                                  join_plan,
                                  ctx,
                                  plan,
                                  &left_ref))
    return false;
  derived_key_var = pg_vec_make_input_boundary_var(inner_join_var,
                                                   seqscan->scan.scanrelid,
                                                   1);
  if (derived_key_var == NULL ||
      !pg_vec_lower_join_key_expr(derived_key_var,
                                  join_plan,
                                  ctx,
                                  plan,
                                  &right_ref))
    return false;
  if (left_ref.input_id == extra_join->right_input)
  {
    PgVecColumnRef tmp = left_ref;

    left_ref = right_ref;
    right_ref = tmp;
  }
  if (left_ref.scalar_kind != PG_VEC_SCALAR_INT32 ||
      right_ref.scalar_kind != PG_VEC_SCALAR_INT32 ||
      right_ref.input_id != extra_join->right_input)
    return false;
  extra_join->keys[0].left = left_ref;
  extra_join->keys[0].right = right_ref;
  extra_join->nkeys = 1;

  derived_sum_var = (Node *) makeVar(seqscan->scan.scanrelid,
                                     2,
                                     NUMERICOID,
                                     -1,
                                     InvalidOid,
                                     0);
  derived_count_var = (Node *) makeVar(seqscan->scan.scanrelid,
                                       3,
                                       INT4OID,
                                       -1,
                                       InvalidOid,
                                       0);
  five_const = (Node *) makeConst(INT4OID,
                                  -1,
                                  InvalidOid,
                                  sizeof(int32),
                                  Int32GetDatum(5),
                                  false,
                                  true);
  mul_outer_count = copyObject(target_mul);
  mul_outer_count->args = list_make2(copyObject(outer_expr),
                                     copyObject(derived_count_var));
  mul_outer_count_five = copyObject(target_mul);
  mul_outer_count_five->args = list_make2((Node *) mul_outer_count,
                                          five_const);
  rewritten_compare = copyObject(compare_expr);
  rewritten_compare->args = list_make2((Node *) mul_outer_count_five,
                                       derived_sum_var);

  if (!pg_vec_lower_filter_quals(list_make1(rewritten_compare),
                                 join_plan,
                                 ctx,
                                 plan,
                                 &extra_join->filter))
    return false;
  if (extra_join->filter.root < 0)
    return false;
  return true;
}

static bool
pg_vec_try_rewrite_correlated_seqscan_qual(Node *qual,
                                           SeqScan *outer_seqscan,
                                           PlannedStmt *plannedstmt,
                                           PgVecLowerContext *ctx,
                                           PgVecPlan *plan,
                                           int left_input_id,
                                           PgVecJoinSpec *extra_join)
{
  OpExpr *compare_expr;
  Node *outer_expr;
  SubPlan *subplan_expr;
  Plan *subplan_plan;
  Agg *agg;
  Plan *scan_plan;
  SeqScan *seqscan;
  RangeTblEntry *rte;
  TargetEntry *target_tle = NULL;
  Node *target_expr;
  OpExpr *target_mul;
  Node *target_left;
  Node *target_right;
  Const *scale_const;
  Aggref *sum_aggref;
  Var *sum_arg_var;
  List *remaining_quals = NIL;
  PgVecLowerContext subctx;
  PgVecPlan *derived_plan;
  PgVecInputSpec *input;
  int input_id;
  int nparams;
  Var *inner_join_vars[PG_VEC_MAX_JOIN_KEYS];
  Node *derived_key_vars[PG_VEC_MAX_JOIN_KEYS];
  PgVecColumnRef left_ref;
  PgVecColumnRef right_ref;
  TargetEntry *group_tles[PG_VEC_MAX_JOIN_KEYS];
  TargetEntry *sum_tle;
  List *derived_tlist = NIL;
  ListCell *lc;
  Node *derived_sum_var;
  Const *two_const;
  OpExpr *mul_outer_two;
  OpExpr *rewritten_compare;
  Datum two_numeric;

  if (qual == NULL || outer_seqscan == NULL || plannedstmt == NULL ||
      ctx == NULL || plan == NULL || extra_join == NULL)
    return false;

  compare_expr = (OpExpr *) pg_vec_strip_implicit_casts(qual);
  if (!IsA(compare_expr, OpExpr) || list_length(compare_expr->args) != 2)
    return false;
  if (get_opname(compare_expr->opno) == NULL ||
      strcmp(get_opname(compare_expr->opno), ">") != 0)
    return false;

  outer_expr = pg_vec_strip_implicit_casts(linitial(compare_expr->args));
  subplan_expr = (SubPlan *) pg_vec_strip_implicit_casts(lsecond(compare_expr->args));
  if (!IsA(subplan_expr, SubPlan))
    return false;
  if (subplan_expr->subLinkType != EXPR_SUBLINK ||
      subplan_expr->isInitPlan ||
      subplan_expr->plan_id <= 0 ||
      list_length(subplan_expr->parParam) < 1 ||
      list_length(subplan_expr->parParam) != list_length(subplan_expr->args) ||
      list_length(subplan_expr->parParam) > PG_VEC_MAX_JOIN_KEYS)
    return false;

  subplan_plan = list_nth_node(Plan, plannedstmt->subplans, subplan_expr->plan_id - 1);
  while (subplan_plan != NULL &&
         !IsA(subplan_plan, Agg) &&
         (IsA(subplan_plan, Hash) || IsA(subplan_plan, Material) || IsA(subplan_plan, Sort)))
    subplan_plan = subplan_plan->lefttree;
  if (subplan_plan == NULL || !IsA(subplan_plan, Agg))
    return false;

  agg = castNode(Agg, subplan_plan);
  if (!pg_vec_top_agg_split_supported(agg) ||
      agg->numCols != 0 ||
      agg->plan.qual != NIL ||
      agg->plan.righttree != NULL)
    return false;

  scan_plan = agg->plan.lefttree;
  if (scan_plan != NULL && IsA(scan_plan, Sort))
    scan_plan = castNode(Sort, scan_plan)->plan.lefttree;
  scan_plan = pg_vec_strip_plan_wrappers(scan_plan);
  if (scan_plan == NULL || !IsA(scan_plan, SeqScan))
    return false;
  seqscan = castNode(SeqScan, scan_plan);
  rte = rt_fetch(seqscan->scan.scanrelid, plannedstmt->rtable);
  if (rte == NULL || rte->rtekind != RTE_RELATION)
    return false;

  foreach (lc, agg->plan.targetlist)
  {
    TargetEntry *tle = lfirst_node(TargetEntry, lc);

    if (!tle->resjunk)
    {
      if (target_tle != NULL)
        return false;
      target_tle = tle;
    }
  }
  if (target_tle == NULL)
    return false;

  target_expr = pg_vec_strip_implicit_casts((Node *) target_tle->expr);
  if (!IsA(target_expr, OpExpr))
    return false;
  target_mul = castNode(OpExpr, target_expr);
  if (list_length(target_mul->args) != 2 ||
      get_opname(target_mul->opno) == NULL ||
      strcmp(get_opname(target_mul->opno), "*") != 0)
    return false;

  target_left = pg_vec_strip_implicit_casts(linitial(target_mul->args));
  target_right = pg_vec_strip_implicit_casts(lsecond(target_mul->args));
  if (IsA(target_left, Const) && IsA(target_right, Aggref))
  {
    scale_const = castNode(Const, target_left);
    sum_aggref = castNode(Aggref, target_right);
  }
  else if (IsA(target_right, Const) && IsA(target_left, Aggref))
  {
    scale_const = castNode(Const, target_right);
    sum_aggref = castNode(Aggref, target_left);
  }
  else
    return false;

  if (scale_const->constisnull || scale_const->consttype != NUMERICOID)
    return false;
  {
    int64 scaled_factor = 0;

    if (!pg_vec_numeric_to_scaled_int64(scale_const->constvalue, 1, &scaled_factor) ||
        scaled_factor != 5)
      return false;
  }
  if (sum_aggref->aggstar ||
      sum_aggref->aggdistinct != NIL ||
      sum_aggref->aggorder != NIL ||
      sum_aggref->aggfilter != NULL ||
      list_length(sum_aggref->args) != 1 ||
      get_func_name(sum_aggref->aggfnoid) == NULL ||
      strcmp(get_func_name(sum_aggref->aggfnoid), "sum") != 0)
    return false;

  sum_arg_var = (Var *) pg_vec_strip_implicit_casts(
      (Node *) linitial_node(TargetEntry, sum_aggref->args)->expr);
  if (!IsA(sum_arg_var, Var))
    return false;

  nparams = list_length(subplan_expr->parParam);
  for (int idx = 0; idx < PG_VEC_MAX_JOIN_KEYS; idx++)
    inner_join_vars[idx] = NULL;
  if (!pg_vec_collect_multi_correlated_scan_quals(seqscan->scan.plan.qual,
                                                  subplan_expr->parParam,
                                                  inner_join_vars,
                                                  &remaining_quals))
    return false;
  for (int idx = 0; idx < nparams; idx++)
  {
    if (inner_join_vars[idx] == NULL)
      return false;
  }

  input_id = ctx->ninputs;
  if (input_id >= PG_VEC_MAX_INPUTS)
    return false;
  pg_vec_input_spec_init(&plan->inputs[input_id]);
  input = &plan->inputs[input_id];

  MemSet(&subctx, 0, sizeof(subctx));
  subctx.queryDesc = ctx->queryDesc;
  derived_plan = palloc0(sizeof(*derived_plan));
  pg_vec_plan_init(derived_plan);
  derived_plan->kind = PG_VEC_PLAN_SCAN_FILTER_AGG;
  derived_plan->ninputs = 1;
  derived_plan->inputs[0].kind = PG_VEC_INPUT_RELATION;
  derived_plan->inputs[0].relid = rte->relid;
  derived_plan->agg.grouped = true;
  if (!pg_vec_make_single_input_context(seqscan, plannedstmt, &subctx))
    return false;

  for (int idx = 0; idx < nparams; idx++)
  {
    group_tles[idx] = makeTargetEntry((Expr *) copyObject((Node *) inner_join_vars[idx]),
                                      idx + 1,
                                      NULL,
                                      false);
    derived_tlist = lappend(derived_tlist, group_tles[idx]);
  }
  sum_tle = makeTargetEntry((Expr *) copyObject((Node *) sum_aggref),
                            nparams + 1,
                            NULL,
                            false);
  derived_tlist = lappend(derived_tlist, sum_tle);

  if (!pg_vec_lower_direct_derived_agg_targetlist(derived_tlist,
                                                  &agg->plan,
                                                  &subctx,
                                                  derived_plan))
    return false;
  if (!pg_vec_lower_filter_quals(remaining_quals,
                                 &seqscan->scan.plan,
                                 &subctx,
                                 derived_plan,
                                 &derived_plan->inputs[0].filter))
    return false;

  input->kind = PG_VEC_INPUT_DERIVED_GROUPED_AGG;
  input->relid = InvalidOid;
  input->derived.relid = rte->relid;
  input->derived.nbase_columns = derived_plan->inputs[0].ncolumns;
  memcpy(input->derived.base_columns,
         derived_plan->inputs[0].columns,
         sizeof(PgVecColumnRef) * derived_plan->inputs[0].ncolumns);
  input->derived.base_filter = derived_plan->inputs[0].filter;
  input->derived.agg = derived_plan->agg;
  for (int idx = 0; idx < PG_VEC_MAX_OUTPUT_COLUMNS; idx++)
    input->derived.output_map[idx] = -1;
  for (int idx = 0; idx < input->derived.agg.noutputs; idx++)
    input->derived.output_map[idx] = idx;
  for (int idx = 0; idx < nparams + 1; idx++)
  {
    if (!pg_vec_add_scan_column(input,
                                input_id,
                                idx + 1,
                                input->derived.agg.outputs[idx]
                                    .nodes[input->derived.agg.outputs[idx].root]
                                    .scalar_kind))
      return false;
  }

  ctx->inputs[input_id].rtindex = seqscan->scan.scanrelid;
  ctx->inputs[input_id].input_id = input_id;
  ctx->inputs[input_id].relid = InvalidOid;
  ctx->inputs[input_id].boundary_plan = &seqscan->scan.plan;
  ctx->ninputs++;
  plan->ninputs = ctx->ninputs;

  MemSet(extra_join, 0, sizeof(*extra_join));
  extra_join->kind = PG_VEC_JOIN_INNER;
  extra_join->left_input = left_input_id;
  extra_join->right_input = input_id;
  pg_vec_filter_spec_init(&extra_join->filter);
  extra_join->nkeys = nparams;

  for (int idx = 0; idx < nparams; idx++)
  {
    Node *outer_join_arg = (Node *) list_nth(subplan_expr->args, idx);

    if (!pg_vec_lower_join_key_expr(outer_join_arg,
                                    &outer_seqscan->scan.plan,
                                    ctx,
                                    plan,
                                    &left_ref))
      return false;
    derived_key_vars[idx] = pg_vec_make_input_boundary_var(inner_join_vars[idx],
                                                           seqscan->scan.scanrelid,
                                                           idx + 1);
    if (derived_key_vars[idx] == NULL ||
        !pg_vec_lower_join_key_expr(derived_key_vars[idx],
                                    &outer_seqscan->scan.plan,
                                    ctx,
                                    plan,
                                    &right_ref))
      return false;
    if (left_ref.input_id == extra_join->right_input)
    {
      PgVecColumnRef tmp = left_ref;

      left_ref = right_ref;
      right_ref = tmp;
    }
    if (left_ref.scalar_kind != PG_VEC_SCALAR_INT32 ||
        right_ref.scalar_kind != PG_VEC_SCALAR_INT32 ||
        right_ref.input_id != extra_join->right_input)
      return false;
    extra_join->keys[idx].left = left_ref;
    extra_join->keys[idx].right = right_ref;
  }

  two_numeric = DirectFunctionCall3(numeric_in,
                                    CStringGetDatum("2.0"),
                                    ObjectIdGetDatum(InvalidOid),
                                    Int32GetDatum(-1));
  two_const = makeConst(NUMERICOID,
                        -1,
                        InvalidOid,
                        -1,
                        two_numeric,
                        false,
                        false);
  mul_outer_two = copyObject(target_mul);
  mul_outer_two->args = list_make2(copyObject(outer_expr), (Node *) two_const);
  derived_sum_var = (Node *) makeVar(seqscan->scan.scanrelid,
                                     nparams + 1,
                                     NUMERICOID,
                                     -1,
                                     InvalidOid,
                                     0);
  rewritten_compare = copyObject(compare_expr);
  rewritten_compare->args = list_make2((Node *) mul_outer_two,
                                       copyObject(derived_sum_var));

  if (!pg_vec_lower_filter_quals(list_make1(rewritten_compare),
                                 &outer_seqscan->scan.plan,
                                 ctx,
                                 plan,
                                 &extra_join->filter))
    return false;
  return extra_join->filter.root >= 0;
}

static bool
pg_vec_try_rewrite_q2_correlated_joinqual(Node *qual,
                                          Plan *join_plan,
                                          PlannedStmt *plannedstmt,
                                          PgVecLowerContext *ctx,
                                          PgVecPlan *plan,
                                          PgVecJoinSpec *extra_join)
{
  OpExpr *compare_expr;
  Node *left_arg;
  Node *right_arg;
  Node *outer_expr;
  SubPlan *subplan_expr;
  Plan *subplan_plan;
  Agg *agg;
  Plan *scan_plan;
  Plan *copied_scan_plan;
  TargetEntry *target_tle = NULL;
  Aggref *min_aggref;
  Var *min_arg_var;
  Var *canonical_min_arg_var = NULL;
  Aggref *canonical_min_aggref;
  int param_id;
  Node *outer_join_arg;
  Var *inner_join_var = NULL;
  PgVecLowerContext subctx;
  PgVecPlan *nested_plan;
  PgVecJoinTreeInfo join_info;
  PgVecInputSpec *input;
  int input_id;
  Node *derived_key_var;
  PgVecColumnRef left_ref;
  PgVecColumnRef right_ref;
  ListCell *lc;
  const char *failure_reason = NULL;
  char *compare_op_name;
  Index derived_rtindex;
  PgVecExprProgram derived_group_expr;
  PgVecExprProgram derived_min_expr;
  PgVecFilterSpec derived_min_filter;
  int derived_group_idx;
  int derived_min_idx;
  int derived_group_root;
  int derived_min_root;
  PgVecAggCall derived_min_call;
  PgVecExprNode compare_expr_node;
  PgVecQualNode compare_qual_node;
  PgVecScalarKind compare_kind;
  int derived_value_root;
  int outer_value_root;

  if (qual == NULL || join_plan == NULL || plannedstmt == NULL ||
      ctx == NULL || plan == NULL || extra_join == NULL)
    return false;

  compare_expr = (OpExpr *) pg_vec_strip_implicit_casts(qual);
  if (!IsA(compare_expr, OpExpr) || list_length(compare_expr->args) != 2)
    return false;
  compare_op_name = get_opname(compare_expr->opno);
  if (compare_op_name == NULL || strcmp(compare_op_name, "=") != 0)
    return false;

  left_arg = pg_vec_strip_implicit_casts(linitial(compare_expr->args));
  right_arg = pg_vec_strip_implicit_casts(lsecond(compare_expr->args));
  if (IsA(left_arg, SubPlan))
  {
    subplan_expr = castNode(SubPlan, left_arg);
    outer_expr = right_arg;
  }
  else if (IsA(right_arg, SubPlan))
  {
    subplan_expr = castNode(SubPlan, right_arg);
    outer_expr = left_arg;
  }
  else
    return false;

  if (subplan_expr->subLinkType != EXPR_SUBLINK ||
      subplan_expr->isInitPlan ||
      subplan_expr->plan_id <= 0 ||
      list_length(subplan_expr->parParam) != 1 ||
      list_length(subplan_expr->args) != 1)
    return false;

  param_id = linitial_int(subplan_expr->parParam);
  outer_join_arg = linitial(subplan_expr->args);
  subplan_plan = list_nth_node(Plan, plannedstmt->subplans, subplan_expr->plan_id - 1);
  while (subplan_plan != NULL &&
         !IsA(subplan_plan, Agg) &&
         (IsA(subplan_plan, Hash) || IsA(subplan_plan, Material) || IsA(subplan_plan, Sort)))
    subplan_plan = subplan_plan->lefttree;
  if (subplan_plan == NULL || !IsA(subplan_plan, Agg))
    return false;

  agg = castNode(Agg, subplan_plan);
  if (!pg_vec_top_agg_split_supported(agg) ||
      agg->numCols != 0 ||
      agg->plan.qual != NIL ||
      agg->plan.righttree != NULL)
    return false;

  scan_plan = agg->plan.lefttree;
  if (scan_plan != NULL && IsA(scan_plan, Sort))
    scan_plan = castNode(Sort, scan_plan)->plan.lefttree;
  scan_plan = pg_vec_strip_plan_wrappers(scan_plan);
  if (scan_plan == NULL)
    return false;

  foreach (lc, agg->plan.targetlist)
  {
    TargetEntry *tle = lfirst_node(TargetEntry, lc);

    if (!tle->resjunk)
    {
      if (target_tle != NULL)
        return false;
      target_tle = tle;
    }
  }
  if (target_tle == NULL)
    return false;

  min_aggref = (Aggref *) pg_vec_strip_implicit_casts((Node *) target_tle->expr);
  if (!IsA(min_aggref, Aggref))
    return false;
  if (min_aggref->aggstar || min_aggref->aggdistinct != NIL ||
      min_aggref->aggorder != NIL || min_aggref->aggfilter != NULL)
    return false;
  if (get_func_name(min_aggref->aggfnoid) == NULL ||
      strcmp(get_func_name(min_aggref->aggfnoid), "min") != 0)
    return false;
  if (list_length(min_aggref->args) != 1)
    return false;
  min_arg_var = (Var *) pg_vec_strip_implicit_casts(
      (Node *) linitial_node(TargetEntry, min_aggref->args)->expr);
  if (!IsA(min_arg_var, Var))
    return false;
  canonical_min_aggref = copyObject(min_aggref);
  if (min_arg_var->varnosyn > 0 && min_arg_var->varattnosyn > 0)
  {
    canonical_min_arg_var =
        castNode(Var,
                 pg_vec_make_input_boundary_var(min_arg_var,
                                                min_arg_var->varnosyn,
                                                min_arg_var->varattnosyn));
  }
  if (canonical_min_arg_var != NULL)
  {
    TargetEntry *canonical_arg_tle =
        linitial_node(TargetEntry, canonical_min_aggref->args);

    canonical_arg_tle->expr = (Expr *) canonical_min_arg_var;
  }

  copied_scan_plan = copyObject(scan_plan);
  if (!pg_vec_strip_correlated_param_qual_from_plan(copied_scan_plan,
                                                    param_id,
                                                    &inner_join_var))
  {
    ereport(WARNING,
            errmsg("pg_vec: q2 rewrite failed stripping correlated param qual"));
    return false;
  }
  if (inner_join_var == NULL)
  {
    ereport(WARNING,
            errmsg("pg_vec: q2 rewrite failed to identify inner join var"));
    return false;
  }

  input_id = ctx->ninputs;
  if (input_id >= PG_VEC_MAX_INPUTS)
    return false;

  MemSet(&subctx, 0, sizeof(subctx));
  MemSet(&join_info, 0, sizeof(join_info));
  subctx.queryDesc = ctx->queryDesc;
  nested_plan = palloc0(sizeof(*nested_plan));
  pg_vec_plan_init(nested_plan);
  nested_plan->kind = PG_VEC_PLAN_SCAN_FILTER_AGG;
  nested_plan->agg.grouped = true;

  if (!pg_vec_lower_join_tree(copied_scan_plan,
                              plannedstmt,
                              &subctx,
                              nested_plan,
                              &join_info,
                              &failure_reason))
  {
    ereport(WARNING,
            errmsg("pg_vec: q2 rewrite failed lowering copied subplan join tree: %s",
                   failure_reason != NULL ? failure_reason : "(no reason)"));
    return false;
  }
  nested_plan->njoins = join_info.njoins;
  for (int join_idx = 0; join_idx < join_info.njoins; join_idx++)
    nested_plan->joins[join_idx] = join_info.joins[join_idx];

  pg_vec_expr_program_init(&derived_group_expr);
  if (!pg_vec_lower_expr((Node *) inner_join_var,
                         copied_scan_plan,
                         &subctx,
                         nested_plan,
                         &derived_group_expr,
                         &derived_group_root))
  {
    ereport(WARNING,
            errmsg("pg_vec: q2 rewrite failed lowering derived group key"));
    return false;
  }
  derived_group_expr.root = derived_group_root;
  if (
      !pg_vec_add_group_key(&nested_plan->agg,
                            &derived_group_expr,
                            derived_group_expr.nodes[derived_group_root].scalar_kind,
                            &derived_group_idx))
  {
    ereport(WARNING,
            errmsg("pg_vec: q2 rewrite failed adding derived group key"));
    return false;
  }

  MemSet(&derived_min_call, 0, sizeof(derived_min_call));
  pg_vec_expr_program_init(&derived_min_expr);
  pg_vec_filter_spec_init(&derived_min_filter);
  derived_min_call.kind = PG_VEC_AGG_MIN;
  derived_min_call.expr = derived_min_expr;
  derived_min_call.filter = derived_min_filter;
  if (!pg_vec_lower_expr((Node *) canonical_min_arg_var != NULL ?
                             (Node *) canonical_min_arg_var :
                             (Node *) min_arg_var,
                         copied_scan_plan,
                         &subctx,
                         nested_plan,
                         &derived_min_call.expr,
                         &derived_min_root))
  {
    ereport(WARNING,
            errmsg("pg_vec: q2 rewrite failed lowering derived min arg"));
    return false;
  }
  derived_min_call.expr.root = derived_min_root;
  if (!pg_vec_add_agg_call(&nested_plan->agg, &derived_min_call, &derived_min_idx))
  {
    ereport(WARNING,
            errmsg("pg_vec: q2 rewrite failed adding derived min agg"));
    return false;
  }

  nested_plan->agg.noutputs = 0;
  if (!pg_vec_make_simple_output_group_key(&nested_plan->agg.outputs[nested_plan->agg.noutputs++],
                                           nested_plan->agg.group_keys[derived_group_idx].scalar_kind,
                                           derived_group_idx) ||
      !pg_vec_make_simple_output_aggref(&nested_plan->agg.outputs[nested_plan->agg.noutputs++],
                                        derived_min_call.expr.nodes[derived_min_call.expr.root].scalar_kind,
                                        derived_min_idx))
  {
    ereport(WARNING,
            errmsg("pg_vec: q2 rewrite failed building derived outputs"));
    return false;
  }

  input = &plan->inputs[input_id];
  pg_vec_input_spec_init(input);
  input->kind = PG_VEC_INPUT_DERIVED_GROUPED_AGG;
  input->relid = InvalidOid;
  input->derived.relid = InvalidOid;
  input->derived.nbase_columns = 0;
  pg_vec_filter_spec_init(&input->derived.base_filter);
  input->derived.agg = nested_plan->agg;
  input->derived.subplan = nested_plan;
  for (int idx = 0; idx < PG_VEC_MAX_OUTPUT_COLUMNS; idx++)
    input->derived.output_map[idx] = -1;
  input->derived.output_map[0] = 0;
  input->derived.output_map[1] = 1;

  if (!pg_vec_add_scan_column(input,
                              input_id,
                              1,
                              input->derived.agg.outputs[0]
                                  .nodes[input->derived.agg.outputs[0].root]
                                  .scalar_kind) ||
      !pg_vec_add_scan_column(input,
                              input_id,
                              2,
                              input->derived.agg.outputs[1]
                                  .nodes[input->derived.agg.outputs[1].root]
                                  .scalar_kind))
  {
    ereport(WARNING,
            errmsg("pg_vec: q2 rewrite failed adding derived scan columns"));
    return false;
  }

  derived_rtindex = list_length(plannedstmt->rtable) + ctx->ninputs + 1;
  while (pg_vec_ctx_contains_rtindex(ctx, derived_rtindex))
    derived_rtindex++;

  ctx->inputs[input_id].rtindex = derived_rtindex;
  ctx->inputs[input_id].input_id = input_id;
  ctx->inputs[input_id].relid = InvalidOid;
  ctx->inputs[input_id].boundary_plan = copied_scan_plan;
  ctx->ninputs++;
  plan->ninputs = ctx->ninputs;

  MemSet(extra_join, 0, sizeof(*extra_join));
  extra_join->kind = PG_VEC_JOIN_INNER;
  pg_vec_filter_spec_init(&extra_join->filter);

  if (!pg_vec_lower_join_key_expr(outer_join_arg,
                                  join_plan,
                                  ctx,
                                  plan,
                                  &left_ref))
  {
    ereport(WARNING,
            errmsg("pg_vec: q2 rewrite failed lowering outer join key"));
    return false;
  }
  derived_key_var = pg_vec_make_input_boundary_var(inner_join_var,
                                                   derived_rtindex,
                                                   1);
  if (derived_key_var == NULL ||
      !pg_vec_lower_join_key_expr(derived_key_var,
                                  join_plan,
                                  ctx,
                                  plan,
                                  &right_ref))
  {
    ereport(WARNING,
            errmsg("pg_vec: q2 rewrite failed lowering derived join key"));
    return false;
  }

  extra_join->left_input = left_ref.input_id;
  extra_join->right_input = input_id;
  if (left_ref.input_id == extra_join->right_input)
  {
    PgVecColumnRef tmp = left_ref;

    left_ref = right_ref;
    right_ref = tmp;
    extra_join->left_input = left_ref.input_id;
  }
  if (left_ref.scalar_kind != PG_VEC_SCALAR_INT32 ||
      right_ref.scalar_kind != PG_VEC_SCALAR_INT32 ||
      right_ref.input_id != extra_join->right_input)
  {
    ereport(WARNING,
            errmsg("pg_vec: q2 rewrite derived join key shape mismatch left_input=%d right_input=%d expected_right=%d left_kind=%d right_kind=%d",
                   left_ref.input_id,
                   right_ref.input_id,
                   extra_join->right_input,
                   (int) left_ref.scalar_kind,
                   (int) right_ref.scalar_kind));
    return false;
  }
  extra_join->nkeys = 1;
  extra_join->keys[0].left = left_ref;
  extra_join->keys[0].right = right_ref;

  if (!pg_vec_scalar_kind_from_pg_type(min_aggref->aggtype,
                                       min_arg_var->vartypmod,
                                       &compare_kind))
    return false;

  pg_vec_filter_spec_init(&extra_join->filter);
  MemSet(&compare_expr_node, 0, sizeof(compare_expr_node));
  compare_expr_node.kind = PG_VEC_EXPR_COLUMN;
  compare_expr_node.scalar_kind = compare_kind;
  compare_expr_node.left = -1;
  compare_expr_node.right = -1;
  compare_expr_node.column.input_id = input_id;
  compare_expr_node.column.attno = 2;
  compare_expr_node.column.scalar_kind = compare_kind;
  if (!pg_vec_add_expr_node(&extra_join->filter.exprs,
                            &compare_expr_node,
                            &derived_value_root))
    return false;

  if (!pg_vec_lower_expr_internal(copyObject(outer_expr),
                                  join_plan,
                                  ctx,
                                  plan,
                                  &extra_join->filter.exprs,
                                  true,
                                  compare_kind,
                                  &outer_value_root))
    return false;

  MemSet(&compare_qual_node, 0, sizeof(compare_qual_node));
  compare_qual_node.kind = PG_VEC_QUAL_COMPARE;
  compare_qual_node.left = -1;
  compare_qual_node.right = -1;
  compare_qual_node.op = PG_VEC_OP_EQ;
  if (IsA(left_arg, SubPlan))
  {
    compare_qual_node.lhs_expr = derived_value_root;
    compare_qual_node.rhs_expr = outer_value_root;
  }
  else
  {
    compare_qual_node.lhs_expr = outer_value_root;
    compare_qual_node.rhs_expr = derived_value_root;
  }
  if (!pg_vec_add_qual_node(&extra_join->filter,
                            &compare_qual_node,
                            &extra_join->filter.root))
    return false;

  return extra_join->filter.root >= 0;
}

static bool pg_vec_lower_join_key_expr(Node *node, Plan *join_plan,
                                       const PgVecLowerContext *ctx,
                                       PgVecPlan *plan,
                                       PgVecColumnRef *column_ref) {
  Node *stripped = pg_vec_strip_implicit_casts(node);
  Node *resolved;

  /*
   * If the join qual already references an input boundary Var (for example a
   * SubqueryScan output representing a derived grouped input), keep that
   * boundary instead of resolving back into the subplan. Over-resolving here
   * loses the derived-input identity and can make otherwise valid join keys
   * fail to lower, as in Q15.
   */
  if (IsA(stripped, Var))
  {
    Var *var = castNode(Var, stripped);
    Node *boundary_var;

    if (var->varno != OUTER_VAR &&
        var->varno != INNER_VAR &&
        pg_vec_ctx_contains_rtindex(ctx, var->varno))
      return pg_vec_lower_var_with_source(var, ctx, plan, join_plan, column_ref);

    if ((var->varno == OUTER_VAR || var->varno == INNER_VAR) &&
        var->varnosyn > 0 &&
        var->varattnosyn > 0 &&
        pg_vec_ctx_contains_rtindex(ctx, var->varnosyn))
    {
      Plan *source_plan = (join_plan != NULL &&
                           var->varno == OUTER_VAR) ?
                              join_plan->lefttree :
                              (join_plan != NULL ? join_plan->righttree : NULL);
      Plan *boundary_plan = source_plan;
      uint8 boundary_input_id = 0;
      AttrNumber boundary_attno = var->varattnosyn;
      Var *template_var = var;

      while (boundary_plan != NULL &&
             (IsA(boundary_plan, Hash) ||
              IsA(boundary_plan, Material) ||
              IsA(boundary_plan, Sort)))
        boundary_plan = boundary_plan->lefttree;

      if (boundary_plan != NULL &&
          pg_vec_find_ctx_boundary_plan(ctx, boundary_plan, &boundary_input_id) != NULL)
      {
        PgVecInputSpec *boundary_input =
            &plan->inputs[ctx->inputs[boundary_input_id].input_id];

        if (boundary_input->kind == PG_VEC_INPUT_DERIVED_GROUPED_AGG)
        {
          boundary_attno = var->varattno;
          TargetEntry *boundary_tle =
              pg_vec_find_tle_by_resno(boundary_plan->targetlist, var->varattno);

          if (boundary_tle != NULL)
          {
            Node *boundary_expr =
                pg_vec_strip_implicit_casts((Node *) boundary_tle->expr);

            if (boundary_expr != NULL && IsA(boundary_expr, Var))
            {
              template_var = castNode(Var, boundary_expr);
            }
          }

          boundary_var = pg_vec_make_input_boundary_var(template_var,
                                                        var->varnosyn,
                                                        boundary_attno);
          if (boundary_var != NULL && IsA(boundary_var, Var))
            return pg_vec_lower_var_with_source(castNode(Var, boundary_var),
                                                ctx,
                                                plan,
                                                join_plan,
                                                column_ref);
        }
      }
    }
  }

  resolved = pg_vec_resolve_var_through_plan(node, join_plan, ctx);

  resolved = pg_vec_strip_implicit_casts(resolved);
  if (IsA(resolved, Var))
  {
    if (pg_vec_lower_var_with_source(castNode(Var, resolved),
                                     ctx,
                                     plan,
                                     join_plan,
                                     column_ref))
      return true;
  }

  if (IsA(stripped, Var))
  {
    Var *var = castNode(Var, stripped);
    Node *boundary_var;

    if ((var->varno == OUTER_VAR || var->varno == INNER_VAR) &&
        var->varnosyn > 0 &&
        var->varattnosyn > 0 &&
        pg_vec_ctx_contains_rtindex(ctx, var->varnosyn))
    {
      AttrNumber boundary_attno = var->varattnosyn;

      boundary_var = pg_vec_make_input_boundary_var(var,
                                                    var->varnosyn,
                                                    boundary_attno);
      if (boundary_var != NULL && IsA(boundary_var, Var))
        return pg_vec_lower_var_with_source(castNode(Var, boundary_var),
                                            ctx,
                                            plan,
                                            join_plan,
                                            column_ref);
    }
  }
  return false;
}

static bool
pg_vec_lower_nestloop_join_key_expr(Node *node,
                                    Plan *join_plan,
                                    const PgVecLowerContext *ctx,
                                    PgVecPlan *plan,
                                    PgVecColumnRef *column_ref)
{
  Node *stripped = pg_vec_strip_implicit_casts(node);
  Var *var;
  Node *boundary_var;

  if (pg_vec_lower_join_key_expr(node, join_plan, ctx, plan, column_ref))
    return true;

  if (!IsA(stripped, Var))
    return false;
  var = castNode(Var, stripped);
  if (var->varno != OUTER_VAR && var->varno != INNER_VAR)
    return false;
  if (var->varnosyn <= 0 || var->varattnosyn <= 0)
    return false;
  if (!pg_vec_ctx_contains_rtindex(ctx, var->varnosyn))
    return false;

  boundary_var = pg_vec_make_input_boundary_var(var,
                                                var->varnosyn,
                                                var->varattnosyn);
  if (boundary_var == NULL || !IsA(boundary_var, Var))
    return false;
  return pg_vec_lower_var_with_source(castNode(Var, boundary_var),
                                      ctx,
                                      plan,
                                      join_plan,
                                      column_ref);
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

static bool
pg_vec_lower_nestloop_join_keys_from_list(List *clauses,
                                          Plan *join_plan,
                                          const PgVecLowerContext *ctx,
                                          PgVecPlan *plan,
                                          PgVecJoinSpec *join_spec)
{
  ListCell *lc;

  foreach (lc, clauses)
  {
    OpExpr *opexpr;
    Node *left;
    Node *right;
    char *op_name;
    PgVecColumnRef left_ref;
    PgVecColumnRef right_ref;
    PgVecJoinKey key;

    if (!IsA(lfirst(lc), OpExpr))
      continue;

    opexpr = castNode(OpExpr, lfirst(lc));
    if (list_length(opexpr->args) != 2)
      continue;
    op_name = get_opname(opexpr->opno);
    if (op_name == NULL || strcmp(op_name, "=") != 0)
      continue;

    left = linitial(opexpr->args);
    right = lsecond(opexpr->args);
    if (!pg_vec_lower_nestloop_join_key_expr(left, join_plan, ctx, plan, &left_ref) ||
        !pg_vec_lower_nestloop_join_key_expr(right, join_plan, ctx, plan, &right_ref))
      continue;
    if (left_ref.input_id == right_ref.input_id)
      continue;
    if (left_ref.scalar_kind != PG_VEC_SCALAR_INT32 ||
        right_ref.scalar_kind != PG_VEC_SCALAR_INT32)
      continue;

    if (left_ref.input_id == join_spec->right_input)
    {
      PgVecColumnRef tmp = left_ref;

      left_ref = right_ref;
      right_ref = tmp;
    }

    if (right_ref.input_id != join_spec->right_input ||
        left_ref.input_id == join_spec->right_input)
      continue;

    if (join_spec->nkeys >= PG_VEC_MAX_JOIN_KEYS)
      return false;

    key.left = left_ref;
    key.right = right_ref;
    join_spec->keys[join_spec->nkeys++] = key;
  }

  return join_spec->nkeys > 0;
}

static bool
pg_vec_try_extract_hashed_any_join_qual(Node *node,
                                        PlannedStmt *plannedstmt,
                                        PgVecJoinKind *join_kind,
                                        Var **outer_join_var,
                                        SeqScan **inner_seqscan,
                                        Var **inner_join_var)
{
  BoolExpr *bool_expr;
  SubPlan *subplan;
  Node *testexpr;
  OpExpr *test_opexpr;
  Node *left;
  Node *right;
  Plan *subplan_plan;
  TargetEntry *inner_tle = NULL;
  ListCell *lc;

  if (join_kind != NULL)
    *join_kind = PG_VEC_JOIN_INVALID;
  if (outer_join_var != NULL)
    *outer_join_var = NULL;
  if (inner_seqscan != NULL)
    *inner_seqscan = NULL;
  if (inner_join_var != NULL)
    *inner_join_var = NULL;

  if (node == NULL || plannedstmt == NULL)
    return false;

  node = pg_vec_strip_implicit_casts(node);
  if (IsA(node, BoolExpr))
  {
    bool_expr = castNode(BoolExpr, node);
    if (bool_expr->boolop == NOT_EXPR && list_length(bool_expr->args) == 1)
    {
      node = pg_vec_strip_implicit_casts(linitial(bool_expr->args));
      if (join_kind != NULL)
        *join_kind = PG_VEC_JOIN_ANTI;
    }
  }

  if (!IsA(node, SubPlan))
    return false;

  subplan = castNode(SubPlan, node);
  if (join_kind != NULL && *join_kind == PG_VEC_JOIN_INVALID)
    *join_kind = PG_VEC_JOIN_SEMI;
  if (subplan->subLinkType != ANY_SUBLINK || !subplan->useHashTable ||
      subplan->isInitPlan || subplan->plan_id <= 0 || subplan->parParam != NIL)
    return false;

  testexpr = pg_vec_strip_implicit_casts(subplan->testexpr);
  if (!IsA(testexpr, OpExpr))
    return false;
  test_opexpr = castNode(OpExpr, testexpr);
  if (list_length(test_opexpr->args) != 2)
    return false;
  if (get_opname(test_opexpr->opno) == NULL ||
      strcmp(get_opname(test_opexpr->opno), "=") != 0)
    return false;

  left = pg_vec_strip_implicit_casts(linitial(test_opexpr->args));
  right = pg_vec_strip_implicit_casts(lsecond(test_opexpr->args));
  if (IsA(left, Var) && IsA(right, Param))
  {
    if (outer_join_var != NULL)
      *outer_join_var = castNode(Var, left);
  }
  else if (IsA(right, Var) && IsA(left, Param))
  {
    if (outer_join_var != NULL)
      *outer_join_var = castNode(Var, right);
  }
  else
    return false;

  subplan_plan = list_nth_node(Plan, plannedstmt->subplans, subplan->plan_id - 1);
  subplan_plan = pg_vec_strip_plan_wrappers(subplan_plan);
  if (subplan_plan == NULL || !IsA(subplan_plan, SeqScan))
    return false;

  foreach (lc, subplan_plan->targetlist)
  {
    TargetEntry *tle = lfirst_node(TargetEntry, lc);

    if (!tle->resjunk)
    {
      inner_tle = tle;
      break;
    }
  }
  if (inner_tle == NULL)
    return false;
  if (!IsA(pg_vec_strip_implicit_casts((Node *) inner_tle->expr), Var))
    return false;

  if (inner_seqscan != NULL)
    *inner_seqscan = castNode(SeqScan, subplan_plan);
  if (inner_join_var != NULL)
    *inner_join_var =
        castNode(Var, pg_vec_strip_implicit_casts((Node *) inner_tle->expr));
  return true;
}

static bool
pg_vec_is_subplan_compare_candidate(Node *node)
{
  OpExpr *opexpr;
  Node *left;
  Node *right;

  node = pg_vec_strip_implicit_casts(node);
  if (node == NULL || !IsA(node, OpExpr))
    return false;

  opexpr = castNode(OpExpr, node);
  if (list_length(opexpr->args) != 2)
    return false;

  left = pg_vec_strip_implicit_casts(linitial(opexpr->args));
  right = pg_vec_strip_implicit_casts(lsecond(opexpr->args));
  return IsA(left, SubPlan) || IsA(right, SubPlan);
}

static bool
pg_vec_split_seqscan_quals_for_hashed_any_join(List *quals,
                                               PlannedStmt *plannedstmt,
                                               List **base_quals,
                                               PgVecJoinKind *join_kind,
                                               Var **outer_join_var,
                                               SeqScan **inner_seqscan,
                                               Var **inner_join_var)
{
  List *remaining = NIL;
  bool found = false;
  ListCell *lc;

  if (base_quals != NULL)
    *base_quals = NIL;
  if (join_kind != NULL)
    *join_kind = PG_VEC_JOIN_INVALID;
  if (outer_join_var != NULL)
    *outer_join_var = NULL;
  if (inner_seqscan != NULL)
    *inner_seqscan = NULL;
  if (inner_join_var != NULL)
    *inner_join_var = NULL;

  foreach (lc, quals)
  {
    Node *qual = (Node *) lfirst(lc);
    PgVecJoinKind extracted_kind = PG_VEC_JOIN_INVALID;
    Var *extracted_outer = NULL;
    SeqScan *extracted_seqscan = NULL;
    Var *extracted_inner = NULL;

    if (!found &&
        pg_vec_try_extract_hashed_any_join_qual(qual,
                                                plannedstmt,
                                                &extracted_kind,
                                                &extracted_outer,
                                                &extracted_seqscan,
                                                &extracted_inner))
    {
      found = true;
      if (join_kind != NULL)
        *join_kind = extracted_kind;
      if (outer_join_var != NULL)
        *outer_join_var = extracted_outer;
      if (inner_seqscan != NULL)
        *inner_seqscan = extracted_seqscan;
      if (inner_join_var != NULL)
        *inner_join_var = extracted_inner;
      continue;
    }

    remaining = lappend(remaining, qual);
  }

  if (base_quals != NULL)
    *base_quals = remaining;
  return found;
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
  {
    List *join_quals = pg_vec_flatten_and_quals(pg_vec_join_quals(join_plan));
    List *plan_quals = pg_vec_flatten_and_quals(join_plan->qual);

    clauses = list_concat_copy(join_quals, plan_quals);
  }
  else
    return false;

  if (IsA(join_plan, NestLoop))
    return pg_vec_lower_nestloop_join_keys_from_list(clauses,
                                                     join_plan,
                                                     ctx,
                                                     plan,
                                                     join_spec);

  return pg_vec_lower_join_keys_from_list(clauses, join_plan, ctx, plan, join_spec);
}

static bool
pg_vec_collect_flattened_inner_join_tree(Plan *subplan,
                                         List **leaves,
                                         List **join_quals)
{
  Plan *stripped = pg_vec_strip_plan_wrappers(subplan);
  Join *join_node;
  List *flat_quals;

  if (stripped == NULL || leaves == NULL || join_quals == NULL)
    return false;

  if (IsA(stripped, HashJoin) || IsA(stripped, MergeJoin) || IsA(stripped, NestLoop))
  {
    join_node = (Join *) stripped;
    if (join_node->jointype != JOIN_INNER)
      return false;

    if (!pg_vec_collect_flattened_inner_join_tree(join_node->plan.lefttree,
                                                  leaves,
                                                  join_quals) ||
        !pg_vec_collect_flattened_inner_join_tree(join_node->plan.righttree,
                                                  leaves,
                                                  join_quals))
      return false;

    flat_quals = pg_vec_flatten_and_quals(pg_vec_join_quals(stripped));
    if (flat_quals != NIL)
    {
      ListCell *lc;

      foreach (lc, flat_quals)
      {
        PgVecFlattenedJoinQual *qual_info = palloc(sizeof(*qual_info));

        qual_info->qual = (Node *) lfirst(lc);
        qual_info->owner_plan = stripped;
        *join_quals = lappend(*join_quals, qual_info);
      }
    }
    flat_quals = pg_vec_flatten_and_quals(stripped->qual);
    if (flat_quals != NIL)
    {
      ListCell *lc;

      foreach (lc, flat_quals)
      {
        PgVecFlattenedJoinQual *qual_info = palloc(sizeof(*qual_info));

        qual_info->qual = (Node *) lfirst(lc);
        qual_info->owner_plan = stripped;
        *join_quals = lappend(*join_quals, qual_info);
      }
    }
    return true;
  }

  if (IsA(stripped, SeqScan) || IsA(stripped, SubqueryScan) || IsA(stripped, Agg))
  {
    *leaves = lappend(*leaves, stripped);
    return true;
  }

  return false;
}

static bool
pg_vec_try_extract_flattened_join_key(Node *qual,
                                      Plan *join_plan,
                                      const PgVecLowerContext *ctx,
                                      PgVecPlan *plan,
                                      int candidate_input,
                                      const bool *bound_inputs,
                                      PgVecJoinKey *key)
{
  OpExpr *opexpr;
  Node *left;
  Node *right;
  char *op_name;
  PgVecColumnRef left_ref;
  PgVecColumnRef right_ref;

  if (qual == NULL || join_plan == NULL || ctx == NULL || plan == NULL ||
      bound_inputs == NULL || key == NULL)
    return false;
  if (!IsA(qual, OpExpr))
    return false;

  opexpr = castNode(OpExpr, qual);
  if (list_length(opexpr->args) != 2)
    return false;
  op_name = get_opname(opexpr->opno);
  if (op_name == NULL || strcmp(op_name, "=") != 0)
    return false;

  left = linitial(opexpr->args);
  right = lsecond(opexpr->args);
  if (!pg_vec_lower_nestloop_join_key_expr(left, join_plan, ctx, plan, &left_ref) ||
      !pg_vec_lower_nestloop_join_key_expr(right, join_plan, ctx, plan, &right_ref))
    return false;
  if (left_ref.input_id == right_ref.input_id ||
      left_ref.scalar_kind != PG_VEC_SCALAR_INT32 ||
      right_ref.scalar_kind != PG_VEC_SCALAR_INT32)
    return false;

  if (left_ref.input_id == candidate_input &&
      right_ref.input_id >= 0 &&
      right_ref.input_id < PG_VEC_MAX_INPUTS &&
      bound_inputs[right_ref.input_id])
  {
    key->left = right_ref;
    key->right = left_ref;
    return true;
  }
  if (right_ref.input_id == candidate_input &&
      left_ref.input_id >= 0 &&
      left_ref.input_id < PG_VEC_MAX_INPUTS &&
      bound_inputs[left_ref.input_id])
  {
    key->left = left_ref;
    key->right = right_ref;
    return true;
  }

  return false;
}

static bool
pg_vec_try_lower_flattened_inner_join_tree(Plan *subplan,
                                           PlannedStmt *plannedstmt,
                                           PgVecLowerContext *ctx,
                                           PgVecPlan *plan,
                                           PgVecJoinTreeInfo *info,
                                           const char **failure_reason)
{
  List *leaves = NIL;
  List *join_quals = NIL;
  PgVecLowerContext *saved_ctx;
  PgVecPlan *saved_plan;
  PgVecLowerContext *attempt_ctx;
  PgVecPlan *attempt_plan;
  PgVecFlattenedJoinLeaf leaf_infos[PG_VEC_MAX_INPUTS];
  bool bound_inputs[PG_VEC_MAX_INPUTS];
  List *remaining_inputs = NIL;
  List *remaining_quals = NIL;
  Node *correlated_join_qual = NULL;
  List *residual_join_quals = NIL;
  PgVecJoinTreeInfo local_info;
  int nleaves = 0;
  int stream_input;
  PgVecJoinSpec extra_join;

  if (subplan == NULL || plannedstmt == NULL || ctx == NULL || plan == NULL ||
      info == NULL)
    return false;

  if (!pg_vec_collect_flattened_inner_join_tree(subplan, &leaves, &join_quals))
    return false;
  if (list_length(leaves) < 2)
    return false;

  saved_ctx = palloc(sizeof(*saved_ctx));
  saved_plan = palloc(sizeof(*saved_plan));
  attempt_ctx = palloc(sizeof(*attempt_ctx));
  attempt_plan = palloc(sizeof(*attempt_plan));
  *saved_ctx = *ctx;
  *saved_plan = *plan;
  *attempt_ctx = *ctx;
  *attempt_plan = *plan;
  MemSet(&local_info, 0, sizeof(local_info));
  MemSet(bound_inputs, 0, sizeof(bound_inputs));

  {
    ListCell *lc;

    foreach (lc, leaves)
    {
      Plan *leaf_plan = castNode(Plan, lfirst(lc));
      PgVecJoinTreeInfo leaf_info;

      if (nleaves >= PG_VEC_MAX_INPUTS)
      {
        *ctx = *saved_ctx;
        *plan = *saved_plan;
        return false;
      }

      MemSet(&leaf_info, 0, sizeof(leaf_info));
      if (!pg_vec_lower_join_tree(leaf_plan,
                                  plannedstmt,
                                  attempt_ctx,
                                  attempt_plan,
                                  &leaf_info,
                                  failure_reason))
      {
        *ctx = *saved_ctx;
        *plan = *saved_plan;
        return false;
      }
      if (leaf_info.ninputs != 1 || leaf_info.njoins != 0)
      {
        *ctx = *saved_ctx;
        *plan = *saved_plan;
        return false;
      }

      leaf_infos[nleaves].plan = leaf_plan;
      leaf_infos[nleaves].input_id = leaf_info.first_input;
      nleaves++;
    }
  }

  stream_input = leaf_infos[0].input_id;
  local_info.first_input = stream_input;
  local_info.ninputs = nleaves;
  local_info.njoins = 0;
  bound_inputs[stream_input] = true;
  for (int leaf_idx = 1; leaf_idx < nleaves; leaf_idx++)
    remaining_inputs = lappend_int(remaining_inputs, leaf_infos[leaf_idx].input_id);
  remaining_quals = list_copy(join_quals);

  while (remaining_inputs != NIL)
  {
    ListCell *lc_input;
    bool found = false;

    foreach (lc_input, remaining_inputs)
    {
      int candidate_input = lfirst_int(lc_input);
      PgVecJoinSpec join_spec;
      List *consumed_quals = NIL;
      ListCell *lc_qual;

      if (local_info.njoins >= PG_VEC_MAX_JOINS)
      {
        *ctx = *saved_ctx;
        *plan = *saved_plan;
        pg_vec_set_failure_reason(failure_reason,
                                  "too many joins for current pg_vec IR");
        return false;
      }

      MemSet(&join_spec, 0, sizeof(join_spec));
      join_spec.kind = PG_VEC_JOIN_INNER;
      join_spec.left_input = stream_input;
      join_spec.right_input = candidate_input;
      pg_vec_filter_spec_init(&join_spec.filter);

      foreach (lc_qual, remaining_quals)
      {
        PgVecFlattenedJoinQual *qual_info =
            (PgVecFlattenedJoinQual *) lfirst(lc_qual);
        PgVecJoinKey key;

        if (pg_vec_is_subplan_compare_candidate(qual_info->qual))
          continue;
        if (!pg_vec_try_extract_flattened_join_key(qual_info->qual,
                                                   qual_info->owner_plan,
                                                   attempt_ctx,
                                                   attempt_plan,
                                                   candidate_input,
                                                   bound_inputs,
                                                   &key))
          continue;
        if (join_spec.nkeys >= PG_VEC_MAX_JOIN_KEYS)
        {
          *ctx = *saved_ctx;
          *plan = *saved_plan;
          return false;
        }
        join_spec.keys[join_spec.nkeys++] = key;
        consumed_quals = lappend(consumed_quals, qual_info);
      }

      if (join_spec.nkeys <= 0)
        continue;

      local_info.joins[local_info.njoins++] = join_spec;
      bound_inputs[candidate_input] = true;
      remaining_inputs = list_delete_int(remaining_inputs, candidate_input);

      {
        ListCell *lc_consumed;

        foreach (lc_consumed, consumed_quals)
          remaining_quals = list_delete_ptr(remaining_quals, lfirst(lc_consumed));
      }

      found = true;
      break;
    }

    if (!found)
    {
      *ctx = *saved_ctx;
      *plan = *saved_plan;
      pg_vec_set_failure_reason(
          failure_reason,
          "join path currently requires a left-deep tree of base-relation joins");
      return false;
    }
  }

  {
    ListCell *lc;

    foreach (lc, remaining_quals)
    {
      PgVecFlattenedJoinQual *qual_info =
          (PgVecFlattenedJoinQual *) lfirst(lc);

      if (correlated_join_qual == NULL &&
          pg_vec_is_subplan_compare_candidate(qual_info->qual))
        correlated_join_qual = (Node *) qual_info;
      else
        residual_join_quals = lappend(residual_join_quals, qual_info);
    }
  }

  if (residual_join_quals != NIL)
  {
    if (local_info.njoins <= 0)
    {
      *ctx = *saved_ctx;
      *plan = *saved_plan;
      pg_vec_set_failure_reason(failure_reason, "failed to lower joinqual");
      return false;
    }

    {
      ListCell *lc;

      pg_vec_filter_spec_init(&local_info.joins[local_info.njoins - 1].filter);
      foreach (lc, residual_join_quals)
      {
        PgVecFlattenedJoinQual *qual_info =
            (PgVecFlattenedJoinQual *) lfirst(lc);

        if (!pg_vec_append_filter_quals(list_make1(qual_info->qual),
                                        subplan,
                                        attempt_ctx,
                                        attempt_plan,
                                        &local_info.joins[local_info.njoins - 1].filter))
        {
          *ctx = *saved_ctx;
          *plan = *saved_plan;
          pg_vec_set_failure_reason(failure_reason, "failed to lower joinqual");
          return false;
        }
      }
    }
  }

  if (correlated_join_qual != NULL)
  {
    PgVecFlattenedJoinQual *qual_info = (PgVecFlattenedJoinQual *) correlated_join_qual;
    bool rewrote =
        pg_vec_try_rewrite_q17_correlated_joinqual(list_make1(qual_info->qual),
                                                   qual_info->owner_plan,
                                                   plannedstmt,
                                                   attempt_ctx,
                                                   attempt_plan,
                                                   stream_input,
                                                   &extra_join) ||
        pg_vec_try_rewrite_q2_correlated_joinqual(qual_info->qual,
                                                  qual_info->owner_plan,
                                                  plannedstmt,
                                                  attempt_ctx,
                                                  attempt_plan,
                                                  &extra_join);

    if (!rewrote || local_info.njoins >= PG_VEC_MAX_JOINS)
    {
      *ctx = *saved_ctx;
      *plan = *saved_plan;
      pg_vec_set_failure_reason(failure_reason, "failed to lower joinqual");
      return false;
    }

    local_info.joins[local_info.njoins++] = extra_join;
    local_info.ninputs += 1;
  }

  *ctx = *attempt_ctx;
  *plan = *attempt_plan;
  *info = local_info;
  return true;
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
pg_vec_build_derived_output_map(List *targetlist,
                                List *source_targetlist,
                                int *output_map)
{
  ListCell *lc;

  if (output_map == NULL)
    return false;

  for (int idx = 0; idx < PG_VEC_MAX_OUTPUT_COLUMNS; idx++)
    output_map[idx] = -1;

  foreach (lc, targetlist) {
    TargetEntry *tle = lfirst_node(TargetEntry, lc);
    Node *expr;
    Var *var;
    int source_idx;

    if (tle->resjunk)
      continue;
    if (tle->resno <= 0 || tle->resno > PG_VEC_MAX_OUTPUT_COLUMNS)
      return false;

    expr = pg_vec_strip_implicit_casts((Node *) tle->expr);
    if (!IsA(expr, Var))
      return false;
    var = castNode(Var, expr);
    if (!pg_vec_output_index_for_resno(source_targetlist,
                                       var->varattno,
                                       &source_idx))
      return false;
    output_map[tle->resno - 1] = source_idx;
  }

  return true;
}

static bool
pg_vec_reorder_outputs_to_targetlist(List *targetlist,
                                     List *source_targetlist,
                                     PgVecAggSpec *agg)
{
  PgVecOutputExprProgram reordered[PG_VEC_MAX_OUTPUT_COLUMNS];
  int noutputs = 0;
  ListCell *lc;

  if (agg == NULL)
    return false;

  foreach (lc, targetlist) {
    TargetEntry *tle = lfirst_node(TargetEntry, lc);
    Node *expr;
    Var *var;
    int source_idx;

    if (tle->resjunk)
      continue;
    expr = pg_vec_strip_implicit_casts((Node *) tle->expr);
    if (!IsA(expr, Var))
      return false;
    var = castNode(Var, expr);
    if (!pg_vec_output_index_for_resno(source_targetlist,
                                       var->varattno,
                                       &source_idx))
      return false;
    if (source_idx < 0 || source_idx >= agg->noutputs ||
        noutputs >= PG_VEC_MAX_OUTPUT_COLUMNS)
      return false;
    reordered[noutputs++] = agg->outputs[source_idx];
  }

  memcpy(agg->outputs,
         reordered,
         sizeof(PgVecOutputExprProgram) * noutputs);
  agg->noutputs = noutputs;
  return true;
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
  if (plan->topn.nsortkeys > 0)
    return true;
  plan->topn.nsortkeys = 0;

  for (int key_idx = 0; key_idx < sort->numCols; key_idx++) {
    int output_idx;
    bool descending;
    PgVecSortKey *sort_key;
    PgVecOutputExprProgram *program;

    if (!pg_vec_output_index_for_resno(agg->plan.targetlist,
                                       sort->sortColIdx[key_idx],
                                       &output_idx))
      return false;
    if (!pg_vec_sort_key_descending(sort->sortOperators[key_idx], &descending))
      return false;
    if (plan->topn.nsortkeys >= PG_VEC_MAX_SORT_KEYS)
      return false;
    program = &plan->agg.outputs[output_idx];
    if (program->root < 0 || program->root >= program->nnodes)
      return false;
    if (program->nodes[program->root].kind != PG_VEC_OUTPUT_EXPR_GROUP_KEY)
      return false;

    sort_key = &plan->topn.sort_keys[plan->topn.nsortkeys++];
    sort_key->output_idx = output_idx;
    sort_key->descending = descending;
    sort_key->nulls_first = sort->nullsFirst[key_idx];
  }

  plan->topn.enabled = (plan->topn.nsortkeys > 0 || plan->topn.has_limit);
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
    List *base_quals = seqscan->scan.plan.qual;
    List *residual_quals = NIL;
    ListCell *lc;
    PgVecJoinKind extracted_join_kind = PG_VEC_JOIN_INVALID;
    Var *outer_join_var = NULL;
    SeqScan *inner_seqscan = NULL;
    Var *inner_join_var = NULL;
    Node *correlated_qual = NULL;
    PgVecJoinSpec correlated_join;
    int input_id;

    (void) pg_vec_split_seqscan_quals_for_hashed_any_join(seqscan->scan.plan.qual,
                                                          plannedstmt,
                                                          &base_quals,
                                                          &extracted_join_kind,
                                                          &outer_join_var,
                                                          &inner_seqscan,
                                                          &inner_join_var);

    foreach (lc, base_quals)
    {
      Node *qual = (Node *) lfirst(lc);

      if (correlated_qual == NULL && pg_vec_is_subplan_compare_candidate(qual))
        correlated_qual = qual;
      else
        residual_quals = lappend(residual_quals, qual);
    }

    if (!pg_vec_add_input_context(seqscan, plannedstmt, ctx, plan, &input_id) ||
        !pg_vec_lower_filter_quals(residual_quals,
                                   &seqscan->scan.plan,
                                   ctx,
                                   plan,
                                   &plan->inputs[input_id].filter)) {
      pg_vec_set_failure_reason(failure_reason,
                                "failed to lower SeqScan input in join tree");
      return false;
    }

    if (extracted_join_kind != PG_VEC_JOIN_INVALID)
    {
      PgVecJoinSpec join_spec;
      int anti_input_id;
      PgVecColumnRef left_ref;
      PgVecColumnRef right_ref;

      if (inner_seqscan == NULL || outer_join_var == NULL || inner_join_var == NULL ||
          !pg_vec_add_input_context(inner_seqscan,
                                    plannedstmt,
                                    ctx,
                                    plan,
                                    &anti_input_id) ||
          !pg_vec_lower_filter_quals(inner_seqscan->scan.plan.qual,
                                     &inner_seqscan->scan.plan,
                                     ctx,
                                     plan,
                                     &plan->inputs[anti_input_id].filter))
      {
        pg_vec_set_failure_reason(
            failure_reason,
            "failed to lower hashed ANY subplan into join input");
        return false;
      }

      MemSet(&join_spec, 0, sizeof(join_spec));
      join_spec.kind = extracted_join_kind;
      join_spec.left_input = input_id;
      join_spec.right_input = anti_input_id;
      pg_vec_filter_spec_init(&join_spec.filter);
      if (!pg_vec_lower_var(outer_join_var, ctx, plan, &left_ref) ||
          !pg_vec_lower_var(inner_join_var, ctx, plan, &right_ref))
      {
        pg_vec_set_failure_reason(
            failure_reason,
            "failed to lower hashed ANY subplan join keys");
        return false;
      }
      if (left_ref.input_id == join_spec.right_input)
      {
        PgVecColumnRef tmp = left_ref;

        left_ref = right_ref;
        right_ref = tmp;
      }
      if (left_ref.input_id != join_spec.left_input ||
          right_ref.input_id != join_spec.right_input ||
          left_ref.scalar_kind != PG_VEC_SCALAR_INT32 ||
          right_ref.scalar_kind != PG_VEC_SCALAR_INT32)
      {
        pg_vec_set_failure_reason(
            failure_reason,
            "hashed ANY subplan join keys must lower to int32 inputs");
        return false;
      }
      join_spec.nkeys = 1;
      join_spec.keys[0].left = left_ref;
      join_spec.keys[0].right = right_ref;

      info->first_input = input_id;
      info->ninputs = 2;
      info->njoins = 1;
      info->joins[0] = join_spec;
      if (correlated_qual != NULL)
      {
        if (!pg_vec_try_rewrite_correlated_seqscan_qual(correlated_qual,
                                                        seqscan,
                                                        plannedstmt,
                                                        ctx,
                                                        plan,
                                                        input_id,
                                                        &correlated_join))
        {
          pg_vec_set_failure_reason(
              failure_reason,
              "failed to rewrite correlated scalar subquery on scan input");
          return false;
        }
        info->ninputs += 1;
        info->joins[info->njoins++] = correlated_join;
      }
      return true;
    }

    info->first_input = input_id;
    info->ninputs = 1;
    info->njoins = 0;
    if (correlated_qual != NULL)
    {
      if (!pg_vec_try_rewrite_correlated_seqscan_qual(correlated_qual,
                                                      seqscan,
                                                      plannedstmt,
                                                      ctx,
                                                      plan,
                                                      input_id,
                                                      &correlated_join))
      {
        pg_vec_set_failure_reason(
            failure_reason,
            "failed to rewrite correlated scalar subquery on scan input");
        return false;
      }
      info->ninputs += 1;
      info->joins[info->njoins++] = correlated_join;
    }
    return true;
  }

  if (IsA(stripped, SubqueryScan)) {
    SubqueryScan *subqueryscan = castNode(SubqueryScan, stripped);
    int input_id;

    if (!pg_vec_add_derived_input_context(subqueryscan,
                                          plannedstmt,
                                          ctx,
                                          plan,
                                          &input_id) ||
        !pg_vec_lower_filter_quals(subqueryscan->scan.plan.qual,
                                   &subqueryscan->scan.plan,
                                   ctx,
                                   plan,
                                   &plan->inputs[input_id].filter)) {
      pg_vec_set_failure_reason(
          failure_reason,
          "failed to lower grouped derived input in join tree");
      return false;
    }

    info->first_input = input_id;
    info->ninputs = 1;
    info->njoins = 0;
    return true;
  }

  if (IsA(stripped, Agg)) {
    int input_id;

    if (!pg_vec_add_derived_agg_input_context(stripped,
                                              plannedstmt,
                                              ctx,
                                              plan,
                                              &input_id)) {
      pg_vec_set_failure_reason(
          failure_reason,
          "failed to lower grouped aggregate input in join tree");
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

  if (pg_vec_try_lower_flattened_inner_join_tree(stripped,
                                                 plannedstmt,
                                                 ctx,
                                                 plan,
                                                 info,
                                                 failure_reason))
    return true;

  join_node = (Join *) stripped;

  {
    PgVecJoinTreeInfo phys_left_info;
    PgVecJoinTreeInfo phys_right_info;
    const PgVecJoinTreeInfo *ordered_left_info[2];
    const PgVecJoinTreeInfo *ordered_right_info[2];
    const char *lower_failure = NULL;
    PgVecLowerContext *saved_ctx;
    PgVecPlan *saved_plan;

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
    saved_ctx = palloc(sizeof(*saved_ctx));
    saved_plan = palloc(sizeof(*saved_plan));
    *saved_ctx = *ctx;
    *saved_plan = *plan;

    for (int attempt = 0; attempt < 2; attempt++) {
      const PgVecJoinTreeInfo *left_info = ordered_left_info[attempt];
      const PgVecJoinTreeInfo *right_info = ordered_right_info[attempt];
      PgVecLowerContext *attempt_ctx = palloc(sizeof(*attempt_ctx));
      PgVecPlan *attempt_plan = palloc(sizeof(*attempt_plan));
      PgVecJoinSpec join_spec;
      PgVecJoinSpec extra_join;
      PgVecJoinKind join_kind = PG_VEC_JOIN_INVALID;
      int total_joins = left_info->njoins + 1 + right_info->njoins;
      bool rewrote_joinqual = false;

      *attempt_ctx = *saved_ctx;
      *attempt_plan = *saved_plan;

      if (total_joins > PG_VEC_MAX_JOINS) {
        lower_failure = "too many joins for current pg_vec IR";
        continue;
      }

      switch (join_node->jointype) {
        case JOIN_INNER:
          join_kind = PG_VEC_JOIN_INNER;
          break;
        case JOIN_LEFT:
          if (total_joins != 1 ||
              left_info != &phys_left_info ||
              right_info != &phys_right_info)
            continue;
          join_kind = PG_VEC_JOIN_LEFT;
          break;
        case JOIN_RIGHT:
          if (total_joins != 1 ||
              left_info != &phys_right_info ||
              right_info != &phys_left_info)
            continue;
          join_kind = PG_VEC_JOIN_LEFT;
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
        case JOIN_ANTI:
          if (left_info != &phys_left_info || right_info != &phys_right_info)
            continue;
          join_kind = PG_VEC_JOIN_ANTI;
          break;
        case JOIN_RIGHT_ANTI:
          if (left_info != &phys_right_info || right_info != &phys_left_info)
            continue;
          join_kind = PG_VEC_JOIN_ANTI;
          break;
        default:
          lower_failure = "join tree currently supports only inner, left, semi, and anti joins";
          continue;
      }

      MemSet(&join_spec, 0, sizeof(join_spec));
      join_spec.left_input = left_info->first_input;
      join_spec.right_input = right_info->first_input;
      if (!pg_vec_lower_join_spec(stripped,
                                  join_kind,
                                  attempt_ctx,
                                  attempt_plan,
                                  &join_spec)) {
        lower_failure = "failed to lower join keys";
        continue;
      }
      {
        List *join_quals = pg_vec_flatten_and_quals(pg_vec_join_quals(stripped));
        List *residual_join_quals = NIL;
        Node *correlated_join_qual = NULL;
        ListCell *lc;

        foreach (lc, join_quals)
        {
          Node *qual = (Node *) lfirst(lc);

          if (correlated_join_qual == NULL &&
              pg_vec_is_subplan_compare_candidate(qual))
            correlated_join_qual = qual;
          else
            residual_join_quals = lappend(residual_join_quals, qual);
        }

        if (!pg_vec_lower_filter_quals(residual_join_quals,
                                       stripped,
                                       attempt_ctx,
                                       attempt_plan,
                                       &join_spec.filter)) {
          lower_failure = "failed to lower joinqual";
          continue;
        }

        if (correlated_join_qual != NULL)
        {
          if (pg_vec_try_rewrite_q17_correlated_joinqual(list_make1(correlated_join_qual),
                                                         stripped,
                                                         plannedstmt,
                                                         attempt_ctx,
                                                         attempt_plan,
                                                         left_info->first_input,
                                                         &extra_join) ||
              pg_vec_try_rewrite_q2_correlated_joinqual(correlated_join_qual,
                                                        stripped,
                                                        plannedstmt,
                                                        attempt_ctx,
                                                        attempt_plan,
                                                        &extra_join))
            rewrote_joinqual = true;
          else
          {
            lower_failure = "failed to lower joinqual";
            continue;
          }
        }
      }
      if (!pg_vec_append_filter_quals(stripped->qual,
                                      stripped,
                                      attempt_ctx,
                                      attempt_plan,
                                      &join_spec.filter)) {
        lower_failure = "failed to lower join plan qual";
        continue;
      }
      if (rewrote_joinqual && total_joins + 1 > PG_VEC_MAX_JOINS) {
        lower_failure = "too many joins for current pg_vec IR";
        continue;
      }

      info->first_input = left_info->first_input;
      info->ninputs = left_info->ninputs + right_info->ninputs +
                      (rewrote_joinqual ? 1 : 0);
      info->njoins = 0;

      for (int join_idx = 0; join_idx < left_info->njoins; join_idx++)
        info->joins[info->njoins++] = left_info->joins[join_idx];
      info->joins[info->njoins++] = join_spec;
      for (int join_idx = 0; join_idx < right_info->njoins; join_idx++)
        info->joins[info->njoins++] = right_info->joins[join_idx];
      if (rewrote_joinqual)
        info->joins[info->njoins++] = extra_join;

      *ctx = *attempt_ctx;
      *plan = *attempt_plan;

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
  int input_id = -1;

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

  if (agg->plan.righttree != NULL) {
    pg_vec_set_failure_reason(failure_reason,
                              "single-input path expects Agg over a single child");
    return false;
  }

  if (IsA(scan_plan, Agg))
  {
    plan->kind = PG_VEC_PLAN_SCAN_FILTER_AGG;
    plan->ninputs = 0;
    plan->agg.grouped = (agg->numCols > 0);
    if (!pg_vec_add_derived_agg_input_context(scan_plan,
                                              plannedstmt,
                                              &ctx,
                                              plan,
                                              &input_id))
    {
      pg_vec_set_failure_reason(
          failure_reason,
          "failed to lower derived grouped aggregate input for single-input path");
      return false;
    }
    if (!pg_vec_lower_agg_targetlist(queryDesc,
                                     agg->plan.targetlist,
                                     &agg->plan,
                                     &ctx,
                                     plan)) {
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
    return true;
  }

  if (!IsA(scan_plan, SeqScan)) {
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
pg_vec_try_translate_count_distinct_join_plan(QueryDesc *queryDesc,
                                              Limit *limit,
                                              Sort *sort,
                                              Agg *agg,
                                              Plan *join_plan,
                                              PgVecPlan *plan,
                                              const char **failure_reason)
{
  PlannedStmt *plannedstmt;
  PgVecLowerContext inner_ctx;
  PgVecJoinTreeInfo join_info;
  PgVecPlan inner_plan;
  PgVecPlan *nested_plan;
  MemoryContext oldcxt;
  Aggref *distinct_aggref = NULL;
  Aggref *logical_aggref;
  Plan *logical_source_plan;
  TargetEntry *arg_tle;
  int group_slots[PG_VEC_MAX_GROUP_KEYS];
  int outer_group_count = 0;
  int distinct_group_idx;
  int output_idx = 0;
  ListCell *lc;
  PgVecInputSpec *input;
  PgVecAggCall outer_count;

  plannedstmt = queryDesc->plannedstmt;
  if (agg == NULL || join_plan == NULL || queryDesc == NULL ||
      queryDesc->estate == NULL)
    return false;
  if (agg->plan.qual != NIL)
    return false;
  if (agg->numCols <= 0 || agg->numCols + 1 > PG_VEC_MAX_GROUP_KEYS)
    return false;

  foreach (lc, agg->plan.targetlist)
  {
    TargetEntry *tle = lfirst_node(TargetEntry, lc);
    Node *expr;

    if (tle->resjunk)
      continue;
    expr = pg_vec_strip_implicit_casts((Node *) tle->expr);
    if (IsA(expr, Aggref))
    {
      if (distinct_aggref != NULL)
        return false;
      distinct_aggref = castNode(Aggref, expr);
      continue;
    }
  }

  if (distinct_aggref == NULL)
    return false;

  logical_aggref =
      pg_vec_resolve_logical_aggref(distinct_aggref, &agg->plan, &logical_source_plan);
  if (logical_aggref == NULL || logical_aggref->aggfilter != NULL ||
      logical_aggref->aggorder != NIL || !logical_aggref->aggdistinct ||
      logical_aggref->aggstar || list_length(logical_aggref->args) != 1)
    return false;
  if (get_func_name(logical_aggref->aggfnoid) == NULL ||
      strcmp(get_func_name(logical_aggref->aggfnoid), "count") != 0)
    return false;

  arg_tle = linitial_node(TargetEntry, logical_aggref->args);
  if (arg_tle->resjunk)
    return false;

  MemSet(&inner_ctx, 0, sizeof(inner_ctx));
  MemSet(&join_info, 0, sizeof(join_info));
  inner_ctx.queryDesc = queryDesc;
  pg_vec_plan_init(&inner_plan);
  inner_plan.kind = PG_VEC_PLAN_SCAN_FILTER_AGG;
  inner_plan.agg.grouped = true;

  if (!pg_vec_lower_join_tree(join_plan,
                              plannedstmt,
                              &inner_ctx,
                              &inner_plan,
                              &join_info,
                              failure_reason))
    return false;
  inner_plan.njoins = join_info.njoins;
  for (int join_idx = 0; join_idx < join_info.njoins; join_idx++)
    inner_plan.joins[join_idx] = join_info.joins[join_idx];

  for (int key_idx = 0; key_idx < agg->numCols; key_idx++)
  {
    TargetEntry *group_tle;
    PgVecExprProgram group_expr;
    int group_idx;
    PgVecScalarKind scalar_kind;

    group_tle = pg_vec_find_tle_by_resno(agg->plan.targetlist, agg->grpColIdx[key_idx]);
    if (group_tle == NULL || group_tle->resjunk)
      return false;
    pg_vec_expr_program_init(&group_expr);
    if (!pg_vec_lower_expr((Node *) group_tle->expr,
                           &agg->plan,
                           &inner_ctx,
                           &inner_plan,
                           &group_expr,
                           &group_expr.root))
      return false;
    scalar_kind = group_expr.nodes[group_expr.root].scalar_kind;
    if (!pg_vec_add_group_key(&inner_plan.agg, &group_expr, scalar_kind, &group_idx))
      return false;
    group_slots[key_idx] = group_idx;
    outer_group_count++;
  }

  {
    PgVecExprProgram distinct_expr;
    PgVecScalarKind scalar_kind;

    pg_vec_expr_program_init(&distinct_expr);
    if (!pg_vec_lower_expr((Node *) arg_tle->expr,
                           logical_source_plan,
                           &inner_ctx,
                           &inner_plan,
                           &distinct_expr,
                           &distinct_expr.root))
      return false;
    scalar_kind = distinct_expr.nodes[distinct_expr.root].scalar_kind;
    if (scalar_kind != PG_VEC_SCALAR_INT32)
      return false;
    if (!pg_vec_add_group_key(&inner_plan.agg,
                              &distinct_expr,
                              scalar_kind,
                              &distinct_group_idx))
      return false;
  }

  if (distinct_group_idx != outer_group_count)
    return false;

  inner_plan.agg.noutputs = 0;
  for (int key_idx = 0; key_idx < outer_group_count; key_idx++)
  {
    PgVecScalarKind scalar_kind =
        inner_plan.agg.group_keys[group_slots[key_idx]].scalar_kind;

    if (!pg_vec_make_simple_output_group_key(
            &inner_plan.agg.outputs[inner_plan.agg.noutputs++],
            scalar_kind,
            group_slots[key_idx]))
      return false;
  }
  if (!pg_vec_make_simple_output_group_key(
          &inner_plan.agg.outputs[inner_plan.agg.noutputs++],
          inner_plan.agg.group_keys[distinct_group_idx].scalar_kind,
          distinct_group_idx))
    return false;

  oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
  nested_plan = palloc0(sizeof(*nested_plan));
  MemoryContextSwitchTo(oldcxt);
  *nested_plan = inner_plan;

  pg_vec_plan_init(plan);
  plan->kind = PG_VEC_PLAN_SCAN_FILTER_AGG;
  plan->ninputs = 1;
  plan->agg.grouped = true;
  input = &plan->inputs[0];
  input->kind = PG_VEC_INPUT_DERIVED_GROUPED_AGG;
  input->relid = InvalidOid;
  input->derived.relid = InvalidOid;
  input->derived.nbase_columns = 0;
  pg_vec_filter_spec_init(&input->derived.base_filter);
  input->derived.agg = nested_plan->agg;
  input->derived.subplan = nested_plan;
  for (int idx = 0; idx < PG_VEC_MAX_OUTPUT_COLUMNS; idx++)
    input->derived.output_map[idx] = -1;

  for (int key_idx = 0; key_idx < outer_group_count; key_idx++)
  {
    PgVecColumnRef column_ref;
    PgVecExprProgram group_expr;
    int group_idx;

    input->derived.output_map[key_idx] = key_idx;
    column_ref.input_id = 0;
    column_ref.attno = key_idx + 1;
    column_ref.scalar_kind = nested_plan->agg.outputs[key_idx]
                                 .nodes[nested_plan->agg.outputs[key_idx].root]
                                 .scalar_kind;
    if (!pg_vec_add_scan_column(input,
                                0,
                                column_ref.attno,
                                column_ref.scalar_kind))
      return false;
    if (!pg_vec_make_simple_column_expr(&group_expr, column_ref) ||
        !pg_vec_add_group_key(&plan->agg,
                              &group_expr,
                              column_ref.scalar_kind,
                              &group_idx))
      return false;
  }

  MemSet(&outer_count, 0, sizeof(outer_count));
  pg_vec_expr_program_init(&outer_count.expr);
  pg_vec_filter_spec_init(&outer_count.filter);
  outer_count.kind = PG_VEC_AGG_COUNT;
  outer_count.star_arg = true;
  outer_count.expr.root = -1;
  if (!pg_vec_add_agg_call(&plan->agg, &outer_count, &distinct_group_idx))
    return false;

  foreach (lc, agg->plan.targetlist)
  {
    TargetEntry *tle = lfirst_node(TargetEntry, lc);
    Node *expr;

    if (tle->resjunk)
      continue;
    expr = pg_vec_strip_implicit_casts((Node *) tle->expr);
    if (IsA(expr, Aggref))
    {
      if (!pg_vec_make_simple_output_aggref(&plan->agg.outputs[output_idx++],
                                            PG_VEC_SCALAR_INT32,
                                            0))
        return false;
      continue;
    }

    bool matched_group = false;
    for (int key_idx = 0; key_idx < agg->numCols; key_idx++)
    {
      if (agg->grpColIdx[key_idx] != tle->resno)
        continue;
      if (!pg_vec_make_simple_output_group_key(
              &plan->agg.outputs[output_idx++],
              plan->agg.group_keys[key_idx].scalar_kind,
              key_idx))
        return false;
      matched_group = true;
      break;
    }
    if (!matched_group)
      return false;
  }
  plan->agg.noutputs = output_idx;
  if (plan->agg.noutputs <= 0)
    return false;

  if (!pg_vec_lower_topn_spec(limit, sort, agg->plan.targetlist, plan))
  {
    pg_vec_set_failure_reason(
        failure_reason,
        "failed to lower top-level sort or limit for count-distinct join path");
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

static bool
pg_vec_try_translate_join_project_plan(QueryDesc *queryDesc,
                                       PgVecPlan *plan,
                                       const char **failure_reason)
{
  PlannedStmt *plannedstmt;
  Plan *plantree;
  Limit *limit = NULL;
  Sort *sort = NULL;
  PgVecLowerContext ctx;
  PgVecJoinTreeInfo join_info;

  plannedstmt = queryDesc->plannedstmt;
  plantree = plannedstmt->planTree;

  MemSet(&ctx, 0, sizeof(ctx));
  MemSet(&join_info, 0, sizeof(join_info));
  ctx.queryDesc = queryDesc;

  if (IsA(plantree, Limit))
  {
    limit = castNode(Limit, plantree);
    if (limit->plan.righttree != NULL)
    {
      pg_vec_set_failure_reason(failure_reason,
                                "top-level Limit must have a single child");
      return false;
    }
    plantree = limit->plan.lefttree;
  }

  if (IsA(plantree, Sort))
  {
    sort = castNode(Sort, plantree);
    if (sort->plan.righttree != NULL)
    {
      pg_vec_set_failure_reason(failure_reason,
                                "top-level Sort must have a single child");
      return false;
    }
    plantree = sort->plan.lefttree;
  }

  if (plantree == NULL ||
      (!IsA(plantree, HashJoin) && !IsA(plantree, MergeJoin) &&
       !IsA(plantree, NestLoop)))
  {
    pg_vec_set_failure_reason(
        failure_reason,
        "project path expects a top-level join tree");
    return false;
  }

  plan->kind = PG_VEC_PLAN_SCAN_FILTER_AGG;
  plan->agg.grouped = true;

  if (!pg_vec_lower_join_tree(plantree,
                              plannedstmt,
                              &ctx,
                              plan,
                              &join_info,
                              failure_reason))
  {
    if (failure_reason != NULL && *failure_reason == NULL)
      pg_vec_set_failure_reason(failure_reason,
                                "failed to lower join tree for project path");
    return false;
  }

  plan->njoins = join_info.njoins;
  for (int join_idx = 0; join_idx < join_info.njoins; join_idx++)
    plan->joins[join_idx] = join_info.joins[join_idx];
  if (plan->ninputs < 2 || plan->njoins < 1)
  {
    pg_vec_set_failure_reason(
        failure_reason,
        "project path requires at least two lowered inputs");
    return false;
  }
  if (!pg_vec_lower_agg_targetlist(queryDesc,
                                   plantree->targetlist,
                                   plantree,
                                   &ctx,
                                   plan))
  {
    pg_vec_set_failure_reason(
        failure_reason,
        "failed to lower project targetlist");
    return false;
  }
  if (!pg_vec_lower_topn_spec(limit, sort, plantree->targetlist, plan))
  {
    pg_vec_set_failure_reason(
        failure_reason,
        "failed to lower project sort or limit");
    return false;
  }
  return true;
}

static bool pg_vec_numeric_to_scaled_int64(Datum value, int scale, int64 *out) {
  char *str;
  bool ok;
  Datum rounded;

  rounded = DirectFunctionCall2(numeric_round, value, Int32GetDatum(scale));
  str = DatumGetCString(DirectFunctionCall1(numeric_out, rounded));
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
