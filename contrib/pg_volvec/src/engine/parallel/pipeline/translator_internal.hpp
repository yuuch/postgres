#pragma once

extern "C" {
#include "postgres.h"
#include "executor/execdesc.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"
#include "utils/dsa.h"
}

#include <vector>

#include "parallel/pipeline/pipeline_descriptor.hpp"
#include "parallel/pipeline/tuple_data_layout.hpp"

namespace pg_volvec {
namespace pipeline {
namespace translator_detail {

struct MaterializedProjectExpr {
	Expr   *expr;
	int8_t  scale;
	uint8_t slot;
};

struct ColumnRef {
	Index      varno = 0;
	AttrNumber attno = InvalidAttrNumber;

	bool operator==(const ColumnRef &other) const
	{
		return varno == other.varno && attno == other.attno;
	}
};

struct SupportedPlanShape {
	SeqScan                     *scan = nullptr;
	HashJoin                    *hash_join = nullptr;
	SeqScan                     *hash_join_left_scan = nullptr;
	SeqScan                     *hash_join_right_scan = nullptr;
	Agg                         *agg = nullptr;
	Sort                        *sort = nullptr;
	uint32_t                     estimated_groups = 256;
	uint32_t                     perfect_hash_capacity = 0;
	Index                        scan_varno = 0;
	Index                        hash_join_left_varno = 0;
	Index                        hash_join_right_varno = 0;
	Oid                          relid = InvalidOid;
	Oid                          hash_join_left_relid = InvalidOid;
	Oid                          hash_join_right_relid = InvalidOid;
	std::vector<ColumnRef>       group_cols;
	std::vector<ColumnRef>       hash_join_left_keys;
	std::vector<ColumnRef>       hash_join_right_keys;
	std::vector<ColumnRef>       hash_join_output_cols;
	std::vector<ColumnRef>       hash_join_left_output_cols;
	std::vector<ColumnRef>       hash_join_right_output_cols;
	std::vector<HashJoinOutputColumnDesc> hash_join_output_mappings;
	std::vector<ColumnSchema>    hash_join_output_schema_columns;
	std::vector<Aggref *>        aggrefs;
	std::vector<FilterInputDesc> filter_inputs;
	std::vector<FilterExprDesc>  filter_exprs;
	std::vector<FilterStep>      filter_steps;
	std::vector<char>            filter_string_consts;
	std::vector<FilterInputDesc> hash_join_left_filter_inputs;
	std::vector<FilterExprDesc>  hash_join_left_filter_exprs;
	std::vector<FilterStep>      hash_join_left_filter_steps;
	std::vector<char>            hash_join_left_filter_string_consts;
	std::vector<FilterInputDesc> hash_join_right_filter_inputs;
	std::vector<FilterExprDesc>  hash_join_right_filter_exprs;
	std::vector<FilterStep>      hash_join_right_filter_steps;
	std::vector<char>            hash_join_right_filter_string_consts;
	std::vector<SortKeyDesc>     sort_keys;
	std::vector<ColumnRef>       input_cols;
	std::vector<ColumnSchema>    input_columns;
	std::vector<ColumnSchema>    hash_join_left_columns;
	std::vector<ColumnSchema>    hash_join_right_columns;
	TupleDataLayout              hash_layout;
	TupleDataLayout              hash_join_left_key_layout;
	TupleDataLayout              hash_join_right_key_layout;
	TupleDataLayout              hash_join_left_payload_layout;
	TupleDataLayout              hash_join_right_payload_layout;
	TupleDataLayout              hash_join_output_layout;
	TupleDataLayout              sort_key_layout;
	TupleDataLayout              sort_payload_layout;
	dsa_pointer                  hash_layout_dp = InvalidDsaPointer;
	dsa_pointer                  hash_join_left_key_layout_dp = InvalidDsaPointer;
	dsa_pointer                  hash_join_right_key_layout_dp = InvalidDsaPointer;
	dsa_pointer                  hash_join_left_payload_layout_dp = InvalidDsaPointer;
	dsa_pointer                  hash_join_right_payload_layout_dp = InvalidDsaPointer;
	dsa_pointer                  hash_join_output_layout_dp = InvalidDsaPointer;
	dsa_pointer                  sort_key_layout_dp = InvalidDsaPointer;
	dsa_pointer                  sort_payload_layout_dp = InvalidDsaPointer;
	std::vector<AggFuncDesc>     agg_funcs;
	std::vector<TdcAggKind>      agg_kinds;
	std::vector<int16_t>         agg_numeric_scales;
	std::vector<ProjectStep>     project_steps;
	std::vector<ProjectExprDesc> project_exprs;
	std::vector<ProjectStep>     final_project_steps;
	std::vector<ProjectExprDesc> final_project_exprs;
	std::vector<ColumnSchema>    final_project_schema;
	TupleDataLayout              final_project_layout;
	bool                         has_final_project = false;
	uint16_t                     next_filter_bool_reg = 0;
	uint16_t                     hash_join_left_next_filter_bool_reg = 0;
	uint16_t                     hash_join_right_next_filter_bool_reg = 0;
	uint8_t                      next_int32_slot = 0;
	uint8_t                      next_int64_slot = 0;
	uint8_t                      next_double_slot = 0;
};

Expr *StripRelabels(Expr *expr);
bool Pow10Int64(int exp, int64_t &out);
bool RescaleInt64Constant(int64_t value, int8_t from_scale, int8_t to_scale, int64_t &out);
bool LookupRawColumn(const ColumnRef &ref,
			     const std::vector<ColumnRef> &raw_cols_ref,
			     const std::vector<ColumnSchema> &raw_cols,
			     const ColumnSchema *&out_col);
bool ColumnNumericScale(const ColumnSchema &col, int8_t &out);
int16_t ExtractNumericTypmodScale(int32 typmod);
bool ScaleNumericConstDatumToInt64(Const *c, int8_t &out_scale, int64_t &out_value);
bool ScaleNumericConstDatumToTargetScale(Const *c, int8_t target_scale, int64_t &out_value);
bool ExtractStringLikePrefix(Const *c,
				    std::vector<char> &pool,
				    uint32_t &out_offset,
				    uint32_t &out_len,
				    uint64_t &out_value);
bool ResolvePlanVarToColumnRef(Var *var, Plan *context_plan, ColumnRef &out_ref);
bool ResolvePlanExprToColumnRef(Expr *expr, Plan *context_plan, ColumnRef &out_ref);
bool IsBareVarArg(Expr *arg, Plan *context_plan, ColumnRef &out_ref);
bool CollectAggrefArgCols(const std::vector<Aggref *> &aggrefs,
			  Plan *context_plan,
			  std::vector<ColumnRef> &out);
bool CollectExprVarCols(Expr *expr,
			 Plan *context_plan,
			 std::vector<ColumnRef> &out);
bool ClassifyAggref(Aggref *ag,
		    const std::vector<ColumnRef> &raw_cols_ref,
		    const std::vector<ColumnSchema> &raw_cols,
		    Plan *context_plan,
		    std::vector<ProjectStep> &project_steps,
		    std::vector<ProjectExprDesc> &project_exprs,
		    std::vector<MaterializedProjectExpr> &materialized_exprs,
		    uint8_t &next_int64_slot,
		    AggFuncDesc &out_desc,
		    TdcAggKind &out_kind,
		    int16_t &out_numeric_scale);
bool LowerProjectionExpr(Expr *expr,
				std::vector<ProjectStep> &steps,
				uint8_t &next_int64_slot,
				const std::vector<ColumnRef> &raw_cols_ref,
				const std::vector<ColumnSchema> &raw_cols,
				Plan *context_plan,
				const std::vector<MaterializedProjectExpr> *cache,
				int8_t &out_result_scale,
				uint8_t &out_result_slot);

bool TryBuildPerfectHashSpec(const std::vector<ColumnRef> &group_cols,
			     const std::vector<ColumnRef> &input_cols,
			     const std::vector<ColumnSchema> &input_columns,
			     uint32_t &out_capacity);
bool BuildOrderedSeqScanColumns(Oid relid,
				 const std::vector<ColumnRef> &cols,
				 Index expected_varno,
				 std::vector<ColumnSchema> &out);
bool BuildSeqScanColumns(Oid relid,
			 const std::vector<ColumnRef> &cols,
			 Index expected_varno,
			 std::vector<ColumnSchema> &out,
			 uint8_t &next_int32_slot,
			 uint8_t &next_int64_slot,
			 uint8_t &next_double_slot);
dsa_pointer BuildSchemaDescriptorFromColumns(const std::vector<ColumnSchema> &columns, dsa_area *dsa);
dsa_pointer BuildAggOutputSchemaDescriptor(const std::vector<ColumnRef> &group_cols,
					 const std::vector<ColumnRef> &available_cols,
					 const std::vector<ColumnSchema> &available_schema,
					 const std::vector<TdcAggKind> &agg_kinds,
					 dsa_area *dsa);
dsa_pointer BuildOutputSchemaDescriptor(const SupportedPlanShape &shape, dsa_area *dsa);
bool BuildColumnOnlyLayout(const std::vector<ColumnSchema> &columns, TupleDataLayout &out);
bool BuildColumnOnlyLayoutForRefs(const std::vector<ColumnRef> &refs,
				  const std::vector<ColumnRef> &available_cols,
				  const std::vector<ColumnSchema> &available_schema,
				  TupleDataLayout &out);
bool BuildHashJoinOutputMappings(const std::vector<ColumnRef> &output_cols,
				 const std::vector<ColumnRef> &left_cols,
				 const std::vector<ColumnSchema> &left_schema,
				 const std::vector<ColumnRef> &right_cols,
				 const std::vector<ColumnSchema> &right_schema,
				 std::vector<HashJoinOutputColumnDesc> &out_mappings,
				 std::vector<ColumnSchema> &out_schema);
bool BuildHashGroupLayout(const std::vector<ColumnRef> &group_cols,
			  const std::vector<ColumnRef> &input_cols,
			  const std::vector<ColumnSchema> &input_columns,
			  const std::vector<AggFuncDesc> &agg_funcs,
			  const std::vector<TdcAggKind> &agg_kinds,
			  const std::vector<int16_t> &agg_numeric_scales,
			  TupleDataLayout &out);
bool MapProjectedExprSchema(Oid type_oid,
			 int32 typmod,
			 int8_t numeric_scale,
			 uint8_t slot,
			 ColumnSchema &out);
bool BuildAggFinalOutput(const Agg *agg,
			 const std::vector<ColumnRef> &group_cols,
			 const std::vector<ColumnRef> &available_cols,
			 const std::vector<ColumnSchema> &available_schema,
			 const std::vector<Aggref *> &aggrefs,
			 const std::vector<TdcAggKind> &agg_kinds,
			 const std::vector<int16_t> &agg_numeric_scales,
			 std::vector<ColumnSchema> &out_schema,
			 TupleDataLayout &out_layout);
bool BuildSortLayouts(const std::vector<ColumnRef> &group_cols,
			      const std::vector<ColumnRef> &input_cols,
			      const std::vector<ColumnSchema> &input_columns,
			      const std::vector<AggFuncDesc> &agg_funcs,
			      const std::vector<TdcAggKind> &agg_kinds,
			      const std::vector<int16_t> &agg_numeric_scales,
			      const std::vector<SortKeyDesc> &sort_keys,
			      TupleDataLayout &out_key,
			      TupleDataLayout &out_payload);

bool ExtractFilterQual(List *qual,
		       Oid relid,
		       std::vector<FilterInputDesc> &inputs,
		       std::vector<FilterExprDesc> &exprs,
		       std::vector<FilterStep> &steps,
		       std::vector<char> &string_consts,
		       uint16_t &next_bool_reg);
bool ExtractHashJoinFilterQual(List *qual,
			      Plan *context_plan,
			      const std::vector<ColumnRef> &left_cols,
			      const std::vector<ColumnSchema> &left_schema,
			      const std::vector<ColumnRef> &right_cols,
			      const std::vector<ColumnSchema> &right_schema,
			      std::vector<HashJoinFilterInputDesc> &inputs,
			      std::vector<FilterExprDesc> &exprs,
			      std::vector<FilterStep> &steps,
			      std::vector<char> &string_consts,
			      uint16_t &next_bool_reg);
bool ExtractSortKeys(Sort *sort,
		     Agg *agg,
		     Plan *agg_plan,
		     const std::vector<ColumnRef> &group_cols,
		     const std::vector<Aggref *> &aggrefs,
		     std::vector<SortKeyDesc> &out);
bool ExtractRelid(SeqScan *scan, QueryDesc *qd, Oid &out);
bool ExtractHashJoinClauseKeys(HashJoin *hash_join,
			       std::vector<ColumnRef> &left_keys,
			       std::vector<ColumnRef> &right_keys);
bool ExtractHashJoinOutputCols(HashJoin *hash_join,
			      std::vector<ColumnRef> &out);
bool CollectPlanTargetInputCols(Plan *plan,
			       std::vector<ColumnRef> &out);
bool ExtractGroupCols(Agg *agg, Plan *agg_input_plan, std::vector<ColumnRef> &out);
bool ResolveAggGroupVarToColumnRef(Var *var,
			   Agg *agg,
			   const std::vector<ColumnRef> &group_cols,
			   ColumnRef &out);
bool ExtractAggrefs(Agg *agg,
		   Plan *agg_plan,
		   const std::vector<ColumnRef> &group_cols,
		   std::vector<Aggref *> &out);
uint32_t EstimateHashAggGroups(Agg *agg);
uint32_t EstimateHashAggInitialGroups(Agg *agg);
bool AnalyzePlanOutput(Plan *plan,
		       QueryDesc *qd,
		       std::vector<ColumnRef> &out_cols,
		       std::vector<ColumnSchema> &out_schema);

bool ExtractSupportedPlanShape(Plan *plan, QueryDesc *qd, SupportedPlanShape &out);

}  /* namespace translator_detail */
}  /* namespace pipeline */
}  /* namespace pg_volvec */
