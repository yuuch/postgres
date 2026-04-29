/*
 * pipeline/tuple_data_layout.cpp
 *
 * Implementation of TupleDataLayout helpers. See tuple_data_layout.hpp for
 * the design contract. Spec: §3.1, §10 step 1 of
 * .sisyphus/plans/3g2-tuple-data-collection-design.md.
 *
 * Width policy (v1): all columns and aggregates pad up to 8 bytes in the
 * row layout. We carry the actual byte-width on TdcColumnDesc::width for
 * the scatter/gather code, but row offsets always advance by 8. This keeps
 * the row 8-byte aligned without per-row pad bookkeeping and matches what
 * duckdb::TupleDataLayout does for its column section before the validity
 * mask is added. When variable-width support lands (TODO(Q3+)) the row
 * builder will switch to per-column actual-width plus an explicit pad.
 */

#include "tuple_data_layout.hpp"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

namespace pg_volvec {
namespace pipeline {

uint16_t
TupleDataLayoutColumnWidth(TdcColumnKind kind)
{
	switch (kind)
	{
		case TdcColumnKind::INT32:  return 4;
		case TdcColumnKind::INT64:  return 8;
		case TdcColumnKind::DOUBLE: return 8;
	}
	elog(ERROR, "pg_volvec: TupleDataLayoutColumnWidth: unknown TdcColumnKind %u",
		 static_cast<unsigned>(kind));
	return 0;
}

uint16_t
TupleDataLayoutAggregateWidth(TdcAggKind kind)
{
	switch (kind)
	{
		case TdcAggKind::SUM_INT64:    return 8;
		case TdcAggKind::COUNT_STAR:   return 8;
		case TdcAggKind::SUM_NUMERIC:  return 8;
		case TdcAggKind::AVG_NUMERIC:  return 16;
	}
	elog(ERROR, "pg_volvec: TupleDataLayoutAggregateWidth: unknown TdcAggKind %u",
		 static_cast<unsigned>(kind));
	return 0;
}

void
TupleDataLayoutInit(TupleDataLayout *layout)
{
	Assert(layout != nullptr);
	layout->column_count    = 0;
	layout->aggregate_count = 0;
	layout->row_width       = 0;
	layout->validity_width  = 0;
	layout->_pad            = 0;
	for (uint16_t i = 0; i < TUPLE_DATA_MAX_COLUMNS; ++i)
	{
		layout->columns[i]    = TdcColumnDesc{};
		layout->aggregates[i] = TdcAggregateDesc{};
	}
}

uint16_t
TupleDataLayoutAppendColumn(TupleDataLayout *layout,
							TdcColumnKind kind,
							Oid pg_type_oid,
							int16_t numeric_scale)
{
	Assert(layout != nullptr);
	if (layout->column_count >= TUPLE_DATA_MAX_COLUMNS)
		elog(ERROR, "pg_volvec: TupleDataLayout column overflow (max %u)",
			 static_cast<unsigned>(TUPLE_DATA_MAX_COLUMNS));

	const uint16_t idx    = layout->column_count;
	const uint16_t width  = TupleDataLayoutColumnWidth(kind);
	TdcColumnDesc &col    = layout->columns[idx];
	col.kind          = kind;
	col.offset        = layout->row_width;
	col.width         = width;
	col.numeric_scale = numeric_scale;
	col.pg_type_oid   = pg_type_oid;

	/* v1: advance row_width by 8 regardless of width (see file header). */
	layout->row_width   = static_cast<uint16_t>(layout->row_width + 8);
	layout->column_count = static_cast<uint16_t>(idx + 1);
	return idx;
}

uint16_t
TupleDataLayoutAppendAggregate(TupleDataLayout *layout,
							   TdcAggKind kind,
							   uint16_t src_col_idx,
							   Oid pg_agg_oid,
							   int16_t numeric_scale)
{
	Assert(layout != nullptr);
	if (layout->aggregate_count >= TUPLE_DATA_MAX_COLUMNS)
		elog(ERROR, "pg_volvec: TupleDataLayout aggregate overflow (max %u)",
			 static_cast<unsigned>(TUPLE_DATA_MAX_COLUMNS));

	const uint16_t idx     = layout->aggregate_count;
	const uint16_t width   = TupleDataLayoutAggregateWidth(kind);
	const uint16_t advance = static_cast<uint16_t>((width + 7u) & ~7u);
	TdcAggregateDesc &agg  = layout->aggregates[idx];
	agg.kind          = kind;
	agg.offset        = layout->row_width;
	agg.width         = width;
	agg.src_col_idx   = src_col_idx;
	agg.numeric_scale = numeric_scale;
	agg._pad1         = 0;
	agg.pg_agg_oid    = pg_agg_oid;

	layout->row_width       = static_cast<uint16_t>(layout->row_width + advance);
	layout->aggregate_count = static_cast<uint16_t>(idx + 1);
	return idx;
}

void
TupleDataLayoutSeal(TupleDataLayout *layout)
{
	Assert(layout != nullptr);
	const uint16_t rem = static_cast<uint16_t>(layout->row_width % 8);
	if (rem != 0)
		layout->row_width = static_cast<uint16_t>(layout->row_width + (8 - rem));
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */
