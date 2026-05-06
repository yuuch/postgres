// Reserved for M-Q1-PERF B.2 (batched columnar predicate path); not wired in B.1.
// B.1 evaluates the qual inline per-tuple inside PhysicalSeqScan; this batched
// API is kept so B.2 can flip on bitmap/dense selvec without re-introducing the
// type-dispatch table.

#include "parallel/pipeline/columnar_filter.hpp"

extern "C" {
#include "postgres.h"
#include "utils/date.h"
#include "utils/elog.h"
}

namespace pg_volvec {
namespace pipeline {

uint16_t
EvalColumnarPredicate(const QualDescriptor                            *qual,
                      const DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE>    &qual_chunk,
                      uint16_t                                         qual_dst_col,
                      uint16_t                                         count,
                      uint16_t                                        *selvec_out)
{
	if (qual == nullptr || qual->kind == QualKind::NONE)
	{
		for (uint16_t i = 0; i < count; ++i)
			selvec_out[i] = i;
		return count;
	}

	const uint8_t *nulls = qual_chunk.nulls[qual_dst_col];
	uint16_t       s     = 0;

	switch (qual->const_typoid)
	{
		case DATEOID:
		{
			const int32_t  *col = qual_chunk.int32_columns[qual_dst_col];
			const int32_t   r   = (int32_t) DatumGetDateADT((Datum) qual->const_value);
			switch (qual->op)
			{
				case QualOp::LE:
					for (uint16_t i = 0; i < count; ++i)
					{ selvec_out[s] = i; s += (uint16_t)(!nulls[i] && col[i] <= r); }
					break;
				case QualOp::LT:
					for (uint16_t i = 0; i < count; ++i)
					{ selvec_out[s] = i; s += (uint16_t)(!nulls[i] && col[i] <  r); }
					break;
				case QualOp::EQ:
					for (uint16_t i = 0; i < count; ++i)
					{ selvec_out[s] = i; s += (uint16_t)(!nulls[i] && col[i] == r); }
					break;
				case QualOp::GE:
					for (uint16_t i = 0; i < count; ++i)
					{ selvec_out[s] = i; s += (uint16_t)(!nulls[i] && col[i] >= r); }
					break;
				case QualOp::GT:
					for (uint16_t i = 0; i < count; ++i)
					{ selvec_out[s] = i; s += (uint16_t)(!nulls[i] && col[i] >  r); }
					break;
				case QualOp::NE:
					for (uint16_t i = 0; i < count; ++i)
					{ selvec_out[s] = i; s += (uint16_t)(!nulls[i] && col[i] != r); }
					break;
			}
			return s;
		}
		case INT4OID:
		{
			const int32_t  *col = qual_chunk.int32_columns[qual_dst_col];
			const int32_t   r   = DatumGetInt32((Datum) qual->const_value);
			switch (qual->op)
			{
				case QualOp::LE:
					for (uint16_t i = 0; i < count; ++i)
					{ selvec_out[s] = i; s += (uint16_t)(!nulls[i] && col[i] <= r); }
					break;
				case QualOp::LT:
					for (uint16_t i = 0; i < count; ++i)
					{ selvec_out[s] = i; s += (uint16_t)(!nulls[i] && col[i] <  r); }
					break;
				case QualOp::EQ:
					for (uint16_t i = 0; i < count; ++i)
					{ selvec_out[s] = i; s += (uint16_t)(!nulls[i] && col[i] == r); }
					break;
				case QualOp::GE:
					for (uint16_t i = 0; i < count; ++i)
					{ selvec_out[s] = i; s += (uint16_t)(!nulls[i] && col[i] >= r); }
					break;
				case QualOp::GT:
					for (uint16_t i = 0; i < count; ++i)
					{ selvec_out[s] = i; s += (uint16_t)(!nulls[i] && col[i] >  r); }
					break;
				case QualOp::NE:
					for (uint16_t i = 0; i < count; ++i)
					{ selvec_out[s] = i; s += (uint16_t)(!nulls[i] && col[i] != r); }
					break;
			}
			return s;
		}
		case INT8OID:
		{
			const int64_t  *col = qual_chunk.int64_columns[qual_dst_col];
			const int64_t   r   = DatumGetInt64((Datum) qual->const_value);
			switch (qual->op)
			{
				case QualOp::LE:
					for (uint16_t i = 0; i < count; ++i)
					{ selvec_out[s] = i; s += (uint16_t)(!nulls[i] && col[i] <= r); }
					break;
				case QualOp::LT:
					for (uint16_t i = 0; i < count; ++i)
					{ selvec_out[s] = i; s += (uint16_t)(!nulls[i] && col[i] <  r); }
					break;
				case QualOp::EQ:
					for (uint16_t i = 0; i < count; ++i)
					{ selvec_out[s] = i; s += (uint16_t)(!nulls[i] && col[i] == r); }
					break;
				case QualOp::GE:
					for (uint16_t i = 0; i < count; ++i)
					{ selvec_out[s] = i; s += (uint16_t)(!nulls[i] && col[i] >= r); }
					break;
				case QualOp::GT:
					for (uint16_t i = 0; i < count; ++i)
					{ selvec_out[s] = i; s += (uint16_t)(!nulls[i] && col[i] >  r); }
					break;
				case QualOp::NE:
					for (uint16_t i = 0; i < count; ++i)
					{ selvec_out[s] = i; s += (uint16_t)(!nulls[i] && col[i] != r); }
					break;
			}
			return s;
		}
		default:
			elog(ERROR, "pg_volvec: EvalColumnarPredicate const_typoid=%u unsupported (by-value only)",
			     qual->const_typoid);
	}
	return 0;
}

}
}
