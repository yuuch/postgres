#pragma once

#include "core/types.hpp"
#include "core/memory.hpp"
#include "core/hash_table_defs.hpp"
#include "core/data_chunk.hpp"
#include "core/data_chunk_deform.hpp"
#include "expr/expr.hpp"
#include "exec/plan_state.hpp"
#include "exec/seq_scan.hpp"
#include "exec/agg.hpp"
#include "exec/filter.hpp"
#include "exec/lookup.hpp"
#include "exec/project.hpp"
#include "exec/limit.hpp"
#include "exec/sort.hpp"
#include "exec/query_state.hpp"
#include "parallel/pipeline/worker_context.hpp"

#include <algorithm>
#include <cstring>

extern "C" {
#include "utils/lsyscache.h"
#include "access/tableam.h"
#include "access/visibilitymap.h"
#include "access/stratnum.h"
#include "executor/nodeSubplan.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "storage/bufmgr.h"

extern bool pg_volvec_jit_deform;
extern bool pg_volvec_trace_hooks;
extern bool pg_volvec_disable_jit_for_parallel_worker;
}

namespace pg_volvec {

struct CorrelatedLookupProjectSpec
{
	std::unique_ptr<VecPlanState> lookup_state;
	Expr *rewritten_expr = nullptr;
	int num_keys = 0;
	uint16_t input_key_cols[kMaxLookupKeys] = {0, 0, 0, 0};
	VecOutputColMeta input_key_metas[kMaxLookupKeys];
	uint16_t lookup_key_cols[kMaxLookupKeys] = {0, 0, 0, 0};
	VecOutputColMeta lookup_key_metas[kMaxLookupKeys];
	uint16_t lookup_value_col = 0;
	int output_resno = 0;
	VecOutputColMeta output_meta;
};

struct LookupMembershipFilterSpec
{
	std::unique_ptr<VecPlanState> lookup_state;
	Expr *residual_expr = nullptr;
	uint16_t input_key_col = 0;
	VecOutputColMeta input_key_meta;
	uint16_t lookup_key_col = 0;
	VecOutputColMeta lookup_key_meta;
	bool negate = false;
};

constexpr uint32 VOLVEC_GROUPED_PARTIAL_FILE_MAGIC = 0x56564750;
constexpr uint32 VOLVEC_GROUPED_PARTIAL_FILE_VERSION = 2;

struct GroupedPartialFileHeader
{
	uint32 magic = VOLVEC_GROUPED_PARTIAL_FILE_MAGIC;
	uint32 version = VOLVEC_GROUPED_PARTIAL_FILE_VERSION;
	uint32 naggs = 0;
	uint32 reserved = 0;
};

bool IsRewriteExprNode(Node *node);
Expr *StripImplicitNodesLocal(Expr *expr);
bool IsInt64LikeTypeLocal(Oid type);
bool ShouldUseExactNumericAgg(Oid arg_type);
VecOutputStorageKind DefaultOutputStorageKindForType(Oid typid);
uint64_t EncodeFloat8SortKey(double value);
uint32_t TrimBpcharLengthLocal(const char *data, uint32_t len);
uint64_t HashBytes64(const char *data, uint32_t len);
uint64_t HashStringRefForGroupKey(const DataChunk<DEFAULT_CHUNK_SIZE> &chunk,
								  const VecStringRef &ref,
								  Oid sql_type,
								  uint32_t *trimmed_len_out);
uint64_t HashStringBytesForGroupKey(const char *ptr, uint32_t len, Oid sql_type,
									uint32_t *len_out);
Expr *RewriteExprAgainstTargetList(Expr *expr, List *targetlist);
VecStringRef CopyStringRefToChunk(DataChunk<DEFAULT_CHUNK_SIZE> &dst,
								  const DataChunk<DEFAULT_CHUNK_SIZE> &src,
								  const VecStringRef &ref);
bool BufFileWriteAllLocal(BufFile *file, const void *ptr, size_t size);
bool BufFileReadAllLocal(BufFile *file, void *ptr, size_t size, bool eof_ok,
						 bool *eof_reached = nullptr);
void AdjustProgramVarScales(VecExprProgram *program, VecPlanState *input_state);
void CollectAttrNosFromExpr(Node *node, Bitmapset **attrs);
bool MatchStringPrefixExpr(Expr *expr, uint16_t *input_col, uint32_t *prefix_len);
int CountVisibleTargetEntries(List *targetlist);
bool ResolveAggPassThroughExpr(Agg *node, Expr *expr, int *input_col, int *group_key_pos);
void CollectRequiredAttrsForPlan(Plan *plan, Bitmapset **attrs);
bool IsProcessParallelLocalContext(const ParallelWorkerContext *parallel_worker_context);
bool ShouldSuppressPartialAggQual(const ParallelWorkerContext *parallel_worker_context,
								  Agg *agg);
bool HasProcessParallelTargetAggSubtree(const ParallelWorkerContext *parallel_worker_context,
										VecPlanState *plan_state);
Expr *BuildCombinedQualExpr(List *joinqual, List *planqual);
Oid FindPlanBaseRelid(Plan *plan, EState *estate);
bool PlanContainsNodeId(Plan *plan, int target_plan_node_id);
void BuildPrunedDeformProgram(Bitmapset *attrs, TupleDesc desc, DeformProgram *program);

bool CanBuildDirectVarProjectTargetList(List *targetlist);
std::unique_ptr<VecPlanState> BuildDirectVarProject(std::unique_ptr<VecPlanState> left,
													List *targetlist);

std::unique_ptr<VecPlanState> BuildAggWithOptionalProject(std::unique_ptr<VecPlanState> left,
														  Agg *node,
														  EState *estate,
														  bool suppress_qual_filter = false);
bool LookupPlanOutputMeta(Plan *plan,
						  VecPlanState *state,
						  int target_resno,
						  uint16_t *source_col,
						  VecOutputColMeta *meta);
bool TryBuildLookupMembershipFilterSpec(Expr *expr,
										VecPlanState *input_state,
										EState *estate,
										LookupMembershipFilterSpec *spec_out);
bool TryBuildPlanCorrelatedLookupProjectSpec(Expr *expr,
											 VecPlanState *input_state,
											 EState *estate,
											 CorrelatedLookupProjectSpec *spec_out);

std::unique_ptr<VecPlanState> ExecInitVecPlanInternal(Plan *plan,
													  EState *estate,
													  Bitmapset *required_attrs,
													  bool force_full_deform,
													  const ParallelWorkerContext *parallel_worker_context);

} /* namespace pg_volvec */
