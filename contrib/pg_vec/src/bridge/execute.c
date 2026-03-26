#include "postgres.h"

#include "catalog/pg_type_d.h"
#include "executor/executor.h"
#include "utils/builtins.h"
#include "utils/numeric.h"

#include "execute.h"
#include "../engine/vec_exec_api.h"

extern Datum numeric_add(PG_FUNCTION_ARGS);
extern Datum numeric_sub(PG_FUNCTION_ARGS);
extern Datum numeric_mul(PG_FUNCTION_ARGS);
extern Datum numeric_div(PG_FUNCTION_ARGS);

static bool pg_vec_execute_scan_filter_agg(QueryDesc *queryDesc,
										   PgVecQueryState *state,
										   ScanDirection direction,
										   uint64 count);
static int pg_vec_scalar_scale(PgVecScalarKind scalar_kind);
static Datum pg_vec_numeric_from_scaled_int128(__int128 value, int scale);
static Datum pg_vec_avg_from_state(const PgVecAggExecState *agg_state);
static Datum pg_vec_bpchar_from_char1(char value, int32 typmod);
static Datum pg_vec_string128_to_datum(const PgVecStringConst *value,
									   Form_pg_attribute attr);
static Datum pg_vec_const_value_to_datum(PgVecScalarKind scalar_kind,
										 const PgVecConstValue *value,
										 Form_pg_attribute attr);
static Datum pg_vec_int128_value_to_datum(PgVecScalarKind scalar_kind,
										  __int128 value,
										  Form_pg_attribute attr);
static bool pg_vec_eval_agg_ref(const PgVecPlan *plan,
								const PgVecExecRow *row,
								int agg_idx,
								Form_pg_attribute attr,
								Datum *value,
								bool *isnull);
static bool pg_vec_eval_output_expr(const PgVecPlan *plan,
									const PgVecExecRow *row,
									const PgVecOutputExprProgram *program,
									int node_idx,
									Form_pg_attribute attr,
									Datum *value,
									bool *isnull);
static void pg_vec_format_scaled_int128(__int128 value,
										int scale,
										char *buf,
										size_t buflen);

bool
pg_vec_execute_query(QueryDesc *queryDesc,
					 PgVecQueryState *state,
					 ScanDirection direction,
					 uint64 count)
{
	switch (state->plan.kind)
	{
		case PG_VEC_PLAN_SCAN_FILTER_AGG:
			return pg_vec_execute_scan_filter_agg(queryDesc, state, direction, count);
		case PG_VEC_PLAN_UNSUPPORTED:
		default:
			return false;
	}
}

static bool
pg_vec_execute_scan_filter_agg(QueryDesc *queryDesc,
							   PgVecQueryState *state,
							   ScanDirection direction,
							   uint64 count)
{
	EState	   *estate = queryDesc->estate;
	DestReceiver *dest = queryDesc->dest;
	MemoryContext oldcxt;
	PgVecScanFilterAggExecParams exec_params;
	PgVecScanFilterAggExecResult exec_result;
	PgVecPlan   *plan = &state->plan;
	int			row_idx;
	int			attrno;

	if (state->completed)
	{
		estate->es_processed = 0;
		return true;
	}

	if (ScanDirectionIsNoMovement(direction))
	{
		estate->es_processed = 0;
		return true;
	}

	if (!ScanDirectionIsForward(direction))
		return false;

	if (queryDesc->tupDesc == NULL || queryDesc->tupDesc->natts != plan->agg.noutputs)
		return false;

	oldcxt = MemoryContextSwitchTo(estate->es_query_cxt);

	estate->es_processed = 0;
	dest->rStartup(dest, queryDesc->operation, queryDesc->tupDesc);

	MemSet(&exec_params, 0, sizeof(exec_params));
	exec_params.ninputs = plan->ninputs;
	exec_params.snapshot = estate->es_snapshot;
	exec_params.join = plan->join;
	exec_params.agg = plan->agg;
	for (int input_id = 0; input_id < plan->ninputs; input_id++)
	{
		exec_params.rels[input_id] = state->rels[input_id];
		exec_params.inputs[input_id] = plan->inputs[input_id];
	}

	pg_vec_execute_scan_filter_agg_datachunk(&exec_params, &exec_result);

	for (row_idx = 0; row_idx < exec_result.nrows; row_idx++)
	{
		const PgVecExecRow *row = &exec_result.rows[row_idx];

		ExecClearTuple(state->result_slot);
		for (attrno = 0; attrno < plan->agg.noutputs; attrno++)
		{
			Datum		value = (Datum) 0;
			bool		isnull = true;
			Form_pg_attribute attr = TupleDescAttr(queryDesc->tupDesc, attrno);

			if (!pg_vec_eval_output_expr(plan,
										 row,
										 &plan->agg.outputs[attrno],
										 plan->agg.outputs[attrno].root,
										 attr,
										 &value,
										 &isnull))
			{
				dest->rShutdown(dest);
				MemoryContextSwitchTo(oldcxt);
				return false;
			}

			state->result_slot->tts_values[attrno] = value;
			state->result_slot->tts_isnull[attrno] = isnull;
		}
		ExecStoreVirtualTuple(state->result_slot);
		(void) dest->receiveSlot(state->result_slot, dest);
		ExecClearTuple(state->result_slot);
	}

	dest->rShutdown(dest);

	estate->es_processed = exec_result.nrows;
	estate->es_total_processed += estate->es_processed;
	queryDesc->already_executed = true;
	state->completed = true;

	MemoryContextSwitchTo(oldcxt);
	(void) count;
	return true;
}

static int
pg_vec_scalar_scale(PgVecScalarKind scalar_kind)
{
	switch (scalar_kind)
	{
		case PG_VEC_SCALAR_DECIMAL64_S2:
			return 2;
		case PG_VEC_SCALAR_DECIMAL128_S4:
			return 4;
		case PG_VEC_SCALAR_DECIMAL128_S6:
			return 6;
		case PG_VEC_SCALAR_INT32:
		case PG_VEC_SCALAR_DATE32:
		case PG_VEC_SCALAR_CHAR1:
		case PG_VEC_SCALAR_STRING128:
		case PG_VEC_SCALAR_INVALID:
		default:
			return -1;
	}
}

static Datum
pg_vec_numeric_from_scaled_int128(__int128 value, int scale)
{
	char		buf[128];

	pg_vec_format_scaled_int128(value, scale, buf, sizeof(buf));
	return DirectFunctionCall3(numeric_in,
							   CStringGetDatum(buf),
							   ObjectIdGetDatum(InvalidOid),
							   Int32GetDatum(-1));
}

static Datum
pg_vec_avg_from_state(const PgVecAggExecState *agg_state)
{
	Datum		sum_numeric;
	Datum		count_numeric;
	int			scale;

	scale = pg_vec_scalar_scale(agg_state->scalar_kind);
	if (scale < 0)
		elog(ERROR, "pg_vec: unsupported avg scalar kind %d",
			 (int) agg_state->scalar_kind);

	sum_numeric = pg_vec_numeric_from_scaled_int128(agg_state->value, scale);
	count_numeric = DirectFunctionCall1(int8_numeric,
										Int64GetDatum(agg_state->count));
	return DirectFunctionCall2(numeric_div, sum_numeric, count_numeric);
}

static Datum
pg_vec_bpchar_from_char1(char value, int32 typmod)
{
	char		buf[2];

	buf[0] = value;
	buf[1] = '\0';
	return DirectFunctionCall3(bpcharin,
							   CStringGetDatum(buf),
							   ObjectIdGetDatum(InvalidOid),
							   Int32GetDatum(typmod));
}

static Datum
pg_vec_string128_to_datum(const PgVecStringConst *value, Form_pg_attribute attr)
{
	char		buf[PG_VEC_INLINE_STRING_MAX];

	memcpy(buf, value->bytes, value->len);
	buf[value->len] = '\0';

	switch (attr->atttypid)
	{
		case TEXTOID:
			return CStringGetTextDatum(buf);
		case VARCHAROID:
			return DirectFunctionCall3(varcharin,
									   CStringGetDatum(buf),
									   ObjectIdGetDatum(InvalidOid),
									   Int32GetDatum(attr->atttypmod));
		case BPCHAROID:
			return DirectFunctionCall3(bpcharin,
									   CStringGetDatum(buf),
									   ObjectIdGetDatum(InvalidOid),
									   Int32GetDatum(attr->atttypmod));
		default:
			return CStringGetTextDatum(buf);
	}
}

static Datum
pg_vec_const_value_to_datum(PgVecScalarKind scalar_kind,
							 const PgVecConstValue *value,
							 Form_pg_attribute attr)
{
	switch (scalar_kind)
	{
		case PG_VEC_SCALAR_INT32:
			return Int32GetDatum(value->int32_value);
		case PG_VEC_SCALAR_DATE32:
			return DateADTGetDatum(value->date32);
		case PG_VEC_SCALAR_DECIMAL64_S2:
			return pg_vec_numeric_from_scaled_int128(value->decimal64_s2, 2);
		case PG_VEC_SCALAR_CHAR1:
			return pg_vec_bpchar_from_char1(value->char1, attr->atttypmod);
		case PG_VEC_SCALAR_STRING128:
			return pg_vec_string128_to_datum(&value->string128, attr);
		case PG_VEC_SCALAR_DECIMAL128_S4:
		case PG_VEC_SCALAR_DECIMAL128_S6:
		case PG_VEC_SCALAR_INVALID:
		default:
			elog(ERROR, "pg_vec: unsupported const scalar kind %d",
				 (int) scalar_kind);
	}
}

static Datum
pg_vec_int128_value_to_datum(PgVecScalarKind scalar_kind,
							  __int128 value,
							  Form_pg_attribute attr)
{
	int			scale;

	switch (scalar_kind)
	{
		case PG_VEC_SCALAR_INT32:
			return Int32GetDatum((int32) value);
		case PG_VEC_SCALAR_DATE32:
			return DateADTGetDatum((DateADT) value);
		case PG_VEC_SCALAR_CHAR1:
			return pg_vec_bpchar_from_char1((char) value, attr->atttypmod);
		case PG_VEC_SCALAR_DECIMAL64_S2:
		case PG_VEC_SCALAR_DECIMAL128_S4:
		case PG_VEC_SCALAR_DECIMAL128_S6:
			scale = pg_vec_scalar_scale(scalar_kind);
			if (scale < 0)
				elog(ERROR, "pg_vec: unsupported numeric scalar kind %d",
					 (int) scalar_kind);
			return pg_vec_numeric_from_scaled_int128(value, scale);
		case PG_VEC_SCALAR_STRING128:
		case PG_VEC_SCALAR_INVALID:
		default:
			elog(ERROR, "pg_vec: unsupported scalar kind %d",
				 (int) scalar_kind);
	}
}

static bool
pg_vec_eval_agg_ref(const PgVecPlan *plan,
					const PgVecExecRow *row,
					int agg_idx,
					Form_pg_attribute attr,
					Datum *value,
					bool *isnull)
{
	const PgVecAggCall *agg_call;
	const PgVecAggExecState *agg_state;

	if (agg_idx < 0 || agg_idx >= plan->agg.naggs)
		return false;

	agg_call = &plan->agg.aggs[agg_idx];
	agg_state = &row->aggs[agg_idx];

	switch (agg_call->kind)
	{
		case PG_VEC_AGG_COUNT:
			*value = Int64GetDatum(agg_state->count);
			*isnull = false;
			return true;

		case PG_VEC_AGG_AVG:
			if (agg_state->isnull || agg_state->count == 0)
			{
				*value = (Datum) 0;
				*isnull = true;
				return true;
			}
			*value = pg_vec_avg_from_state(agg_state);
			*isnull = false;
			return true;

		case PG_VEC_AGG_SUM:
			if (agg_state->isnull && agg_call->zero_if_empty)
			{
				*value = pg_vec_int128_value_to_datum(agg_state->scalar_kind,
													  0,
													  attr);
				*isnull = false;
				return true;
			}
			/* fall through */
		case PG_VEC_AGG_MIN:
		case PG_VEC_AGG_MAX:
			if (agg_state->isnull)
			{
				*value = (Datum) 0;
				*isnull = true;
				return true;
			}
			*value = pg_vec_int128_value_to_datum(agg_state->scalar_kind,
												  agg_state->value,
												  attr);
			*isnull = false;
			return true;

		case PG_VEC_AGG_INVALID:
		default:
			return false;
	}
}

static bool
pg_vec_eval_output_expr(const PgVecPlan *plan,
						const PgVecExecRow *row,
						const PgVecOutputExprProgram *program,
						int node_idx,
						Form_pg_attribute attr,
						Datum *value,
						bool *isnull)
{
	Datum		left_value;
	Datum		right_value;
	bool		left_null;
	bool		right_null;
	const PgVecOutputExprNode *node;

	if (node_idx < 0 || node_idx >= program->nnodes)
		return false;

	node = &program->nodes[node_idx];
	switch (node->kind)
	{
		case PG_VEC_OUTPUT_EXPR_GROUP_KEY:
			if (node->index < 0 || node->index >= plan->agg.ngroup_keys)
				return false;
			*value = pg_vec_const_value_to_datum(plan->agg.group_keys[node->index].scalar_kind,
												 &row->group_keys[node->index],
												 attr);
			*isnull = false;
			return true;

		case PG_VEC_OUTPUT_EXPR_AGGREF:
			return pg_vec_eval_agg_ref(plan, row, node->index, attr, value, isnull);

		case PG_VEC_OUTPUT_EXPR_CONST:
			*value = pg_vec_const_value_to_datum(node->scalar_kind,
												 &node->constant,
												 attr);
			*isnull = false;
			return true;

		case PG_VEC_OUTPUT_EXPR_ADD:
		case PG_VEC_OUTPUT_EXPR_SUB:
		case PG_VEC_OUTPUT_EXPR_MUL:
		case PG_VEC_OUTPUT_EXPR_DIV:
			if (!pg_vec_eval_output_expr(plan,
										 row,
										 program,
										 node->left,
										 attr,
										 &left_value,
										 &left_null) ||
				!pg_vec_eval_output_expr(plan,
										 row,
										 program,
										 node->right,
										 attr,
										 &right_value,
										 &right_null))
				return false;
			if (left_null || right_null)
			{
				*value = (Datum) 0;
				*isnull = true;
				return true;
			}

			switch (node->kind)
			{
				case PG_VEC_OUTPUT_EXPR_ADD:
					*value = DirectFunctionCall2(numeric_add, left_value, right_value);
					break;
				case PG_VEC_OUTPUT_EXPR_SUB:
					*value = DirectFunctionCall2(numeric_sub, left_value, right_value);
					break;
				case PG_VEC_OUTPUT_EXPR_MUL:
					*value = DirectFunctionCall2(numeric_mul, left_value, right_value);
					break;
				case PG_VEC_OUTPUT_EXPR_DIV:
					*value = DirectFunctionCall2(numeric_div, left_value, right_value);
					break;
				case PG_VEC_OUTPUT_EXPR_GROUP_KEY:
				case PG_VEC_OUTPUT_EXPR_AGGREF:
				case PG_VEC_OUTPUT_EXPR_CONST:
				case PG_VEC_OUTPUT_EXPR_INVALID:
				default:
					return false;
			}
			*isnull = false;
			return true;

		case PG_VEC_OUTPUT_EXPR_INVALID:
		default:
			return false;
	}
}

static void
pg_vec_format_scaled_int128(__int128 value, int scale, char *buf, size_t buflen)
{
	unsigned __int128 abs_value;
	unsigned __int128 scale_factor = 1;
	unsigned __int128 int_part;
	unsigned __int128 frac_part;
	char		intbuf[128];
	char		fracbuf[32];
	int			intpos = 0;
	int			frac_idx;
	char	   *dst = buf;

	for (int i = 0; i < scale; i++)
		scale_factor *= 10;

	if (value < 0)
	{
		abs_value = (unsigned __int128) (-value);
		*dst++ = '-';
	}
	else
		abs_value = (unsigned __int128) value;

	int_part = scale == 0 ? abs_value : abs_value / scale_factor;
	frac_part = scale == 0 ? 0 : abs_value % scale_factor;

	if (int_part == 0)
		intbuf[intpos++] = '0';
	else
	{
		while (int_part > 0)
		{
			intbuf[intpos++] = '0' + (char) (int_part % 10);
			int_part /= 10;
		}
	}

	while (intpos > 0)
		*dst++ = intbuf[--intpos];

	if (scale > 0)
	{
		*dst++ = '.';
		for (frac_idx = scale - 1; frac_idx >= 0; frac_idx--)
		{
			fracbuf[frac_idx] = '0' + (char) (frac_part % 10);
			frac_part /= 10;
		}
		for (frac_idx = 0; frac_idx < scale; frac_idx++)
			*dst++ = fracbuf[frac_idx];
	}

	*dst = '\0';
	if ((size_t) (dst - buf) >= buflen)
		elog(ERROR, "pg_vec: formatted numeric overflow");
}
