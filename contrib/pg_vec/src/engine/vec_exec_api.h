#ifndef PG_VEC_EXEC_API_H
#define PG_VEC_EXEC_API_H

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

#include "utils/rel.h"
#include "utils/snapshot.h"

#include "../ir/vec_ir.h"

typedef struct PgVecScanFilterAggExecParams
{
	int			ninputs;
	Relation	rels[PG_VEC_MAX_INPUTS];
	Snapshot	snapshot;
	PgVecInputSpec inputs[PG_VEC_MAX_INPUTS];
	PgVecJoinSpec join;
	PgVecAggSpec agg;
} PgVecScanFilterAggExecParams;

typedef struct PgVecAggExecState
{
	bool		isnull;
	PgVecScalarKind scalar_kind;
	__int128	value;
	int64		count;
} PgVecAggExecState;

typedef struct PgVecExecRow
{
	PgVecConstValue group_keys[PG_VEC_MAX_GROUP_KEYS];
	PgVecAggExecState aggs[PG_VEC_MAX_AGG_CALLS];
} PgVecExecRow;

typedef struct PgVecScanFilterAggExecResult
{
	int			nrows;
	PgVecExecRow *rows;
	uint64		rows_scanned;
	uint64		rows_selected;
	uint64		chunks_scanned;
} PgVecScanFilterAggExecResult;

void pg_vec_execute_scan_filter_agg_datachunk(
	const PgVecScanFilterAggExecParams *params,
	PgVecScanFilterAggExecResult *result);

#ifdef __cplusplus
}
#endif

#endif
