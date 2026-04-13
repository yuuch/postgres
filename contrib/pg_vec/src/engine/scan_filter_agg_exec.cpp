extern "C" {
#include "postgres.h"

#include "access/heapam.h"
#include "access/table.h"
#include "access/tableam.h"
#include "executor/nodeSubplan.h"
#ifdef USE_LLVM
#include "jit/llvmjit.h"
#endif
#include "lib/stringinfo.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "utils/datetime.h"
}

#include "vec_exec_api.h"
#include "data_chunk.hpp"
#include "data_chunk_deform.hpp"
#include "llvmjit_deform_datachunk.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace pg_vec
{

static constexpr std::uint16_t kExecChunkCapacity = 2048;
static constexpr std::uint32_t kNoTupleCopyOffset = UINT32_MAX;
static constexpr std::uint16_t kLateTupleCopyThreshold = 128;

struct TupleCopyRef
{
	std::uint32_t offset;
	std::uint32_t len;
};

struct ExecDataChunk : public DataChunkHeader<kExecChunkCapacity>
{
	int			ncolumns;
	bool		deform_filter_applied;
	PgVecScalarKind column_kinds[PG_VEC_MAX_SCAN_COLUMNS];
	ItemPointerData tids[kExecChunkCapacity];
	TupleCopyRef tuple_copies[kExecChunkCapacity];
	std::int32_t int32_values[PG_VEC_MAX_SCAN_COLUMNS][kExecChunkCapacity];
	std::int32_t date32_values[PG_VEC_MAX_SCAN_COLUMNS][kExecChunkCapacity];
	std::int64_t decimal64_values[PG_VEC_MAX_SCAN_COLUMNS][kExecChunkCapacity];
	__int128	decimal128_values[PG_VEC_MAX_SCAN_COLUMNS][kExecChunkCapacity];
	char		char1_values[PG_VEC_MAX_SCAN_COLUMNS][kExecChunkCapacity];
	PgVecStringRef string_refs[PG_VEC_MAX_SCAN_COLUMNS][kExecChunkCapacity];
	PgVecStringArena string_arenas[PG_VEC_MAX_SCAN_COLUMNS];
	PgVecStringArena tuple_arena;

	void
	reset()
	{
		DataChunkHeader<kExecChunkCapacity>::reset();
		deform_filter_applied = false;
		string_arena_reset(&tuple_arena);
		for (int col_idx = 0; col_idx < ncolumns; col_idx++)
		{
			if (column_kinds[col_idx] == PG_VEC_SCALAR_STRING128)
				string_arena_reset(&string_arenas[col_idx]);
		}
	}
};

struct JoinKey
{
	int			nkeys;
	std::int32_t values[PG_VEC_MAX_JOIN_KEYS];

	bool operator==(const JoinKey &other) const
	{
		if (nkeys != other.nkeys)
			return false;
		for (int i = 0; i < nkeys; i++)
		{
			if (values[i] != other.values[i])
				return false;
		}
		return true;
	}
};

struct JoinKeyHash
{
	std::size_t operator()(const JoinKey &key) const
	{
		std::size_t hash = static_cast<std::size_t>(key.nkeys);

		for (int i = 0; i < key.nkeys; i++)
			hash ^= static_cast<std::size_t>(key.values[i]) +
				0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);

		return hash;
	}
};

using JoinMatchTable = std::unordered_map<JoinKey, std::vector<std::size_t>, JoinKeyHash>;
using Int32JoinMatchTable = std::unordered_map<std::int32_t, std::vector<std::size_t>>;

struct GroupKey
{
	int			nkeys;
	PgVecScalarKind kinds[PG_VEC_MAX_GROUP_KEYS];
	PgVecConstValue values[PG_VEC_MAX_GROUP_KEYS];

	bool operator==(const GroupKey &other) const;
};

struct GroupKeyHash
{
	std::size_t operator()(const GroupKey &key) const;
};

struct DistinctAggState
{
	bool enabled;
	std::unordered_set<std::int32_t> plain_values;
	std::vector<std::unordered_set<std::int32_t>> grouped_values;
};

struct MaterializedInput
{
	const PgVecInputSpec *spec;
	uint32_t	late_mask;
	std::vector<std::int32_t> int32_columns[PG_VEC_MAX_SCAN_COLUMNS];
	std::vector<std::int32_t> date32_columns[PG_VEC_MAX_SCAN_COLUMNS];
	std::vector<std::int64_t> decimal64_columns[PG_VEC_MAX_SCAN_COLUMNS];
	std::vector<__int128> decimal128_columns[PG_VEC_MAX_SCAN_COLUMNS];
	std::vector<char> char1_columns[PG_VEC_MAX_SCAN_COLUMNS];
	std::vector<PgVecStringRef> string_columns[PG_VEC_MAX_SCAN_COLUMNS];
	std::vector<uint8_t> late_cached[PG_VEC_MAX_SCAN_COLUMNS];
	std::vector<ItemPointerData> tids;
	PgVecStringArena string_arenas[PG_VEC_MAX_SCAN_COLUMNS];
	std::size_t row_count;

	explicit MaterializedInput(const PgVecInputSpec *input_spec,
							   uint32_t input_late_mask = 0) :
		spec(input_spec),
		late_mask(input_late_mask),
		row_count(0)
	{
		for (int col_idx = 0; col_idx < PG_VEC_MAX_SCAN_COLUMNS; col_idx++)
		{
			string_arenas[col_idx].data = nullptr;
			string_arenas[col_idx].size = 0;
			string_arenas[col_idx].capacity = 0;
		}
	}

	void reserve_rows(std::size_t nrows);
	void append_row(const ExecDataChunk &chunk, std::uint16_t row);
};

struct ExecInputCursor
{
	const PgVecInputSpec *spec;
	Relation	rel;
	Snapshot	snapshot;
	uint32_t	late_mask;
	bool		null_row;
	const ExecDataChunk *chunk;
	const MaterializedInput *materialized;
	std::uint16_t chunk_row;
	std::size_t materialized_row;
	Buffer		late_buffer;
	HeapTupleData late_tuple;
	uint32_t	late_cached_mask;
	std::int32_t late_int32_values[PG_VEC_MAX_SCAN_COLUMNS];
	std::int32_t late_date32_values[PG_VEC_MAX_SCAN_COLUMNS];
	std::int64_t late_decimal64_values[PG_VEC_MAX_SCAN_COLUMNS];
	__int128	late_decimal128_values[PG_VEC_MAX_SCAN_COLUMNS];
	char		late_char1_values[PG_VEC_MAX_SCAN_COLUMNS];
	PgVecStringRef late_string_values[PG_VEC_MAX_SCAN_COLUMNS];
	PgVecStringArena late_string_arenas[PG_VEC_MAX_SCAN_COLUMNS];
};

struct EvalContext
{
	ExecInputCursor inputs[PG_VEC_MAX_INPUTS];
};

using BoundFilterClause = DeformFilterClause;
using BoundFilterProgram = DeformFilterProgram;

enum class BoundJoinQualKind : uint8_t
{
	kInvalid = 0,
	kClause,
	kAnd,
	kOr
};

struct BoundJoinFilterClause
{
	uint8 input_id;
	uint8 column_idx;
	PgVecScalarKind scalar_kind;
	PgVecFilterOp op;
	PgVecConstValue constant;
};

struct BoundJoinFilterNode
{
	BoundJoinQualKind kind;
	int left;
	int right;
	int clause_idx;
};

struct BoundJoinFilterProgram
{
	bool valid;
	int root;
	int nnodes;
	int nclauses;
	uint64_t input_masks[PG_VEC_MAX_INPUTS];
	BoundJoinFilterClause clauses[PG_VEC_MAX_FILTER_NODES];
	BoundJoinFilterNode nodes[PG_VEC_MAX_FILTER_NODES];
};

static ExecDataChunk *allocate_exec_chunk(const PgVecInputSpec *spec);
static bool eval_expr_to_int128(const PgVecExprProgram &program,
								   int node_idx,
								   const EvalContext &ctx,
								   __int128 *out);
static bool copy_string_ref_between_arenas(const PgVecStringRef &src,
											 const PgVecStringArena *src_arena,
											 PgVecStringArena *dst_arena,
											 PgVecStringRef *dst);
static bool copy_string_const_into_arena(const PgVecStringConst &src,
										  PgVecStringArena *dst_arena,
										  PgVecStringRef *dst);

bool
GroupKey::operator==(const GroupKey &other) const
{
	if (nkeys != other.nkeys)
		return false;

	for (int i = 0; i < nkeys; i++)
	{
		if (kinds[i] != other.kinds[i])
			return false;

		switch (kinds[i])
		{
			case PG_VEC_SCALAR_INT32:
				if (values[i].int32_value != other.values[i].int32_value)
					return false;
				break;
			case PG_VEC_SCALAR_DATE32:
				if (values[i].date32 != other.values[i].date32)
					return false;
				break;
			case PG_VEC_SCALAR_DECIMAL64_S2:
				if (values[i].decimal64_s2 != other.values[i].decimal64_s2)
					return false;
				break;
			case PG_VEC_SCALAR_DECIMAL128_S2:
			case PG_VEC_SCALAR_DECIMAL128_S4:
			case PG_VEC_SCALAR_DECIMAL128_S6:
				if (values[i].decimal128 != other.values[i].decimal128)
					return false;
				break;
			case PG_VEC_SCALAR_CHAR1:
				if (values[i].char1 != other.values[i].char1)
					return false;
				break;
			case PG_VEC_SCALAR_STRING128:
				if (string_const_compare(values[i].string128, other.values[i].string128) != 0)
					return false;
				break;
			case PG_VEC_SCALAR_INVALID:
			default:
				return false;
		}
	}

	return true;
}

std::size_t
GroupKeyHash::operator()(const GroupKey &key) const
{
	std::size_t hash = static_cast<std::size_t>(key.nkeys);

	for (int i = 0; i < key.nkeys; i++)
	{
		hash ^= static_cast<std::size_t>(key.kinds[i]) +
			0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
		switch (key.kinds[i])
		{
			case PG_VEC_SCALAR_INT32:
				hash ^= static_cast<std::size_t>(key.values[i].int32_value);
				break;
			case PG_VEC_SCALAR_DATE32:
				hash ^= static_cast<std::size_t>(key.values[i].date32);
				break;
			case PG_VEC_SCALAR_DECIMAL64_S2:
				hash ^= static_cast<std::size_t>(key.values[i].decimal64_s2);
				break;
			case PG_VEC_SCALAR_DECIMAL128_S2:
			case PG_VEC_SCALAR_DECIMAL128_S4:
			case PG_VEC_SCALAR_DECIMAL128_S6:
			{
				unsigned __int128 value =
					static_cast<unsigned __int128>(key.values[i].decimal128);
				hash ^= static_cast<std::size_t>(value);
				hash ^= static_cast<std::size_t>(value >> 64);
				break;
			}
			case PG_VEC_SCALAR_CHAR1:
				hash ^= static_cast<unsigned char>(key.values[i].char1);
				break;
			case PG_VEC_SCALAR_STRING128:
			{
				uint16 len = key.values[i].string128.len;

				hash ^= static_cast<std::size_t>(len) +
					0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
				for (uint16 j = 0; j < len; j++)
					hash ^= static_cast<unsigned char>(key.values[i].string128.bytes[j]) +
						0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
				break;
			}
			case PG_VEC_SCALAR_INVALID:
			default:
				break;
		}
	}

	return hash;
}

static bool
copy_string_ref_between_arenas(const PgVecStringRef &src,
								 const PgVecStringArena *src_arena,
								 PgVecStringArena *dst_arena,
								 PgVecStringRef *dst)
{
	*dst = src;
	if (src.len <= PG_VEC_STRING_PREFIX_BYTES)
	{
		dst->tail_offset = kStringNoTailOffset;
		return true;
	}

	return string_arena_append(dst_arena,
							   string_ref_tail_ptr(src, src_arena),
							   src.len - PG_VEC_STRING_PREFIX_BYTES,
							   &dst->tail_offset);
}

static bool
copy_string_const_into_arena(const PgVecStringConst &src,
							 PgVecStringArena *dst_arena,
							 PgVecStringRef *dst)
{
	uint64 prefix = 0;
	std::size_t prefix_len =
		std::min<std::size_t>(src.len, PG_VEC_STRING_PREFIX_BYTES);

	std::memcpy(&prefix, src.bytes, prefix_len);
	dst->len = src.len;
	dst->flags = 0;
	dst->prefix = prefix;
	if (src.len <= PG_VEC_STRING_PREFIX_BYTES)
	{
		dst->tail_offset = kStringNoTailOffset;
		return true;
	}

	return string_arena_append(dst_arena,
							   src.bytes + PG_VEC_STRING_PREFIX_BYTES,
							   src.len - PG_VEC_STRING_PREFIX_BYTES,
							   &dst->tail_offset);
}

void
MaterializedInput::append_row(const ExecDataChunk &chunk, std::uint16_t row)
{
	for (int col_idx = 0; col_idx < spec->ncolumns; col_idx++)
	{
		if ((late_mask & (static_cast<uint32_t>(1) << col_idx)) != 0)
		{
			late_cached[col_idx].push_back(0);
			continue;
		}

		switch (spec->columns[col_idx].scalar_kind)
		{
			case PG_VEC_SCALAR_INT32:
				int32_columns[col_idx].push_back(chunk.int32_values[col_idx][row]);
				break;
			case PG_VEC_SCALAR_DATE32:
				date32_columns[col_idx].push_back(chunk.date32_values[col_idx][row]);
				break;
			case PG_VEC_SCALAR_DECIMAL64_S2:
				decimal64_columns[col_idx].push_back(chunk.decimal64_values[col_idx][row]);
				break;
			case PG_VEC_SCALAR_DECIMAL128_S2:
			case PG_VEC_SCALAR_DECIMAL128_S4:
			case PG_VEC_SCALAR_DECIMAL128_S6:
				decimal128_columns[col_idx].push_back(chunk.decimal128_values[col_idx][row]);
				break;
			case PG_VEC_SCALAR_CHAR1:
				char1_columns[col_idx].push_back(chunk.char1_values[col_idx][row]);
				break;
			case PG_VEC_SCALAR_STRING128:
			{
				PgVecStringRef copied;

				if (!copy_string_ref_between_arenas(chunk.string_refs[col_idx][row],
													 &chunk.string_arenas[col_idx],
													 &string_arenas[col_idx],
													 &copied))
					elog(ERROR, "pg_vec: failed to copy string ref into materialized input");
				string_columns[col_idx].push_back(copied);
				break;
			}
			case PG_VEC_SCALAR_INVALID:
			default:
				elog(ERROR, "pg_vec: unsupported materialized scalar kind %d",
					 (int) spec->columns[col_idx].scalar_kind);
		}
	}

	tids.push_back(chunk.tids[row]);
	row_count++;
}

void
MaterializedInput::reserve_rows(std::size_t nrows)
{
	tids.reserve(nrows);
	for (int col_idx = 0; col_idx < spec->ncolumns; col_idx++)
	{
		if ((late_mask & (static_cast<uint32_t>(1) << col_idx)) != 0)
		{
			late_cached[col_idx].reserve(nrows);
			continue;
		}

		switch (spec->columns[col_idx].scalar_kind)
		{
			case PG_VEC_SCALAR_INT32:
				int32_columns[col_idx].reserve(nrows);
				break;
			case PG_VEC_SCALAR_DATE32:
				date32_columns[col_idx].reserve(nrows);
				break;
			case PG_VEC_SCALAR_DECIMAL64_S2:
				decimal64_columns[col_idx].reserve(nrows);
				break;
			case PG_VEC_SCALAR_DECIMAL128_S2:
			case PG_VEC_SCALAR_DECIMAL128_S4:
			case PG_VEC_SCALAR_DECIMAL128_S6:
				decimal128_columns[col_idx].reserve(nrows);
				break;
			case PG_VEC_SCALAR_CHAR1:
				char1_columns[col_idx].reserve(nrows);
				break;
			case PG_VEC_SCALAR_STRING128:
				string_columns[col_idx].reserve(nrows);
				break;
			case PG_VEC_SCALAR_INVALID:
			default:
				elog(ERROR, "pg_vec: unsupported materialized scalar kind %d",
					 (int) spec->columns[col_idx].scalar_kind);
		}
	}
}

static void
init_materialized_eval_context(const PgVecScanFilterAggExecParams *params,
							   const std::vector<MaterializedInput> &inputs,
							   EvalContext *ctx)
{
	for (int input_id = 0; input_id < params->ninputs; input_id++)
	{
		ctx->inputs[input_id].spec = &params->inputs[input_id];
		ctx->inputs[input_id].rel = params->rels[input_id];
		ctx->inputs[input_id].snapshot = params->snapshot;
	ctx->inputs[input_id].late_mask = inputs[input_id].late_mask;
		ctx->inputs[input_id].null_row = false;
		ctx->inputs[input_id].chunk = nullptr;
		ctx->inputs[input_id].materialized = &inputs[input_id];
		ctx->inputs[input_id].chunk_row = 0;
		ctx->inputs[input_id].materialized_row = 0;
		ctx->inputs[input_id].late_buffer = InvalidBuffer;
		ctx->inputs[input_id].late_tuple.t_data = nullptr;
		ctx->inputs[input_id].late_cached_mask = 0;
		for (int col_idx = 0; col_idx < PG_VEC_MAX_SCAN_COLUMNS; col_idx++)
		{
			ctx->inputs[input_id].late_string_arenas[col_idx].data = nullptr;
			ctx->inputs[input_id].late_string_arenas[col_idx].size = 0;
			ctx->inputs[input_id].late_string_arenas[col_idx].capacity = 0;
		}
	}
}

static void
cursor_reset_late_state(ExecInputCursor *cursor)
{
	if (BufferIsValid(cursor->late_buffer))
	{
		ReleaseBuffer(cursor->late_buffer);
		cursor->late_buffer = InvalidBuffer;
	}
	cursor->late_tuple.t_data = nullptr;
	cursor->late_cached_mask = 0;
	for (int col_idx = 0; col_idx < PG_VEC_MAX_SCAN_COLUMNS; col_idx++)
		string_arena_reset(&cursor->late_string_arenas[col_idx]);
}

static void
cleanup_eval_context(EvalContext *ctx, int ninputs)
{
	for (int input_id = 0; input_id < ninputs && input_id < PG_VEC_MAX_INPUTS; input_id++)
		cursor_reset_late_state(&ctx->inputs[input_id]);
}

static void
cursor_bind_chunk(ExecInputCursor *cursor,
					 const PgVecInputSpec *spec,
					 Relation rel,
					 Snapshot snapshot,
					 uint32_t late_mask,
					 const ExecDataChunk *chunk)
{
	cursor->spec = spec;
	cursor->rel = rel;
	cursor->snapshot = snapshot;
	cursor->late_mask = late_mask;
	cursor->null_row = false;
	cursor->chunk = chunk;
	cursor->materialized = nullptr;
	cursor->materialized_row = 0;
	cursor_reset_late_state(cursor);
}

static void
cursor_bind_materialized(ExecInputCursor *cursor,
						   const PgVecInputSpec *spec,
						   Relation rel,
						   Snapshot snapshot,
						   uint32_t late_mask,
						   const MaterializedInput *materialized)
{
	cursor->spec = spec;
	cursor->rel = rel;
	cursor->snapshot = snapshot;
	cursor->late_mask = late_mask;
	cursor->null_row = false;
	cursor->chunk = nullptr;
	cursor->materialized = materialized;
	cursor->chunk_row = 0;
	cursor_reset_late_state(cursor);
}

static void
cursor_set_chunk_row(ExecInputCursor *cursor, std::uint16_t row)
{
	cursor_reset_late_state(cursor);
	cursor->null_row = false;
	cursor->chunk_row = row;
}

static void
cursor_set_materialized_row(ExecInputCursor *cursor, std::size_t row)
{
	cursor_reset_late_state(cursor);
	cursor->null_row = false;
	cursor->materialized_row = row;
}

static void
cursor_set_null_row(ExecInputCursor *cursor)
{
	cursor_reset_late_state(cursor);
	cursor->null_row = true;
}

static bool
cursor_current_tid(const ExecInputCursor &cursor, ItemPointerData *tid)
{
	if (cursor.materialized != nullptr)
	{
		if (cursor.null_row)
			return false;
		if (cursor.materialized_row >= cursor.materialized->tids.size())
			return false;
		*tid = cursor.materialized->tids[cursor.materialized_row];
		return true;
	}
	if (cursor.chunk == nullptr || cursor.chunk_row >= cursor.chunk->count)
		return false;
	*tid = cursor.chunk->tids[cursor.chunk_row];
	return true;
}

static bool
cursor_prepare_late_tuple(ExecInputCursor *cursor)
{
	ItemPointerData tid;

	if (cursor->late_tuple.t_data != nullptr)
		return true;
	if (cursor->chunk != nullptr &&
		cursor->chunk_row < cursor->chunk->count &&
		cursor->chunk->tuple_copies[cursor->chunk_row].offset != kNoTupleCopyOffset)
	{
		const TupleCopyRef &copy_ref = cursor->chunk->tuple_copies[cursor->chunk_row];

		cursor->late_tuple.t_self = cursor->chunk->tids[cursor->chunk_row];
		cursor->late_tuple.t_len = copy_ref.len;
		cursor->late_tuple.t_data =
			reinterpret_cast<HeapTupleHeader>(cursor->chunk->tuple_arena.data + copy_ref.offset);
		return true;
	}
	if (cursor->rel == NULL || cursor->spec == nullptr ||
		!cursor_current_tid(*cursor, &tid))
		return false;

	cursor->late_tuple.t_self = tid;
	if (!heap_fetch(cursor->rel,
					cursor->snapshot,
					&cursor->late_tuple,
					&cursor->late_buffer,
					false))
		return false;
	return true;
}

static bool
cursor_fetch_late_column(ExecInputCursor *cursor,
						   int column_idx)
{
	Datum value;
	bool isnull = false;
	AttrNumber attno;
	PgVecScalarKind scalar_kind;
	Form_pg_attribute attr;
	std::size_t row_idx = 0;

	if (cursor->spec == nullptr ||
		column_idx < 0 ||
		column_idx >= cursor->spec->ncolumns)
		return false;
	if ((cursor->late_mask & (static_cast<uint32_t>(1) << column_idx)) == 0)
		return false;
	if (cursor->materialized != nullptr)
		row_idx = cursor->materialized_row;
	if ((cursor->late_cached_mask & (static_cast<uint32_t>(1) << column_idx)) != 0)
		return true;
	if (cursor->materialized != nullptr &&
		row_idx < cursor->materialized->late_cached[column_idx].size() &&
		cursor->materialized->late_cached[column_idx][row_idx] != 0)
	{
		cursor->late_cached_mask |= (static_cast<uint32_t>(1) << column_idx);
		return true;
	}
	if (!cursor_prepare_late_tuple(cursor))
		return false;

	attno = cursor->spec->columns[column_idx].attno;
	scalar_kind = cursor->spec->columns[column_idx].scalar_kind;
	attr = TupleDescAttr(RelationGetDescr(cursor->rel), attno - 1);
	value = heap_getattr(&cursor->late_tuple, attno, RelationGetDescr(cursor->rel), &isnull);
	if (isnull)
		return false;

	switch (scalar_kind)
	{
		case PG_VEC_SCALAR_INT32:
			if (cursor->materialized != nullptr)
			{
				MaterializedInput *mat = const_cast<MaterializedInput *>(cursor->materialized);

				if (mat->int32_columns[column_idx].size() < mat->row_count)
					mat->int32_columns[column_idx].resize(mat->row_count);
				mat->int32_columns[column_idx][row_idx] = DatumGetInt32(value);
				mat->late_cached[column_idx][row_idx] = 1;
			}
			else
			{
				cursor->late_int32_values[column_idx] = DatumGetInt32(value);
			}
			break;
		case PG_VEC_SCALAR_DATE32:
			if (cursor->materialized != nullptr)
			{
				MaterializedInput *mat = const_cast<MaterializedInput *>(cursor->materialized);

				if (mat->date32_columns[column_idx].size() < mat->row_count)
					mat->date32_columns[column_idx].resize(mat->row_count);
				mat->date32_columns[column_idx][row_idx] = DatumGetDateADT(value);
				mat->late_cached[column_idx][row_idx] = 1;
			}
			else
			{
				cursor->late_date32_values[column_idx] = DatumGetDateADT(value);
			}
			break;
		case PG_VEC_SCALAR_DECIMAL64_S2:
			if (cursor->materialized != nullptr)
			{
				MaterializedInput *mat = const_cast<MaterializedInput *>(cursor->materialized);

				if (mat->decimal64_columns[column_idx].size() < mat->row_count)
					mat->decimal64_columns[column_idx].resize(mat->row_count);
				if (!numeric_varlena_to_scaled_int64(DatumGetPointer(value),
													 kDecimalScale2,
													 &mat->decimal64_columns[column_idx][row_idx]))
					return false;
				mat->late_cached[column_idx][row_idx] = 1;
			}
			else
			{
				if (!numeric_varlena_to_scaled_int64(DatumGetPointer(value),
													 kDecimalScale2,
													 &cursor->late_decimal64_values[column_idx]))
					return false;
			}
			break;
		case PG_VEC_SCALAR_DECIMAL128_S2:
		case PG_VEC_SCALAR_DECIMAL128_S4:
		case PG_VEC_SCALAR_DECIMAL128_S6:
		{
			int scale = 2;
			int64 scaled = 0;

			if (scalar_kind == PG_VEC_SCALAR_DECIMAL128_S4)
				scale = 4;
			else if (scalar_kind == PG_VEC_SCALAR_DECIMAL128_S6)
				scale = 6;

			if (!numeric_varlena_to_scaled_int64(DatumGetPointer(value),
												 scale,
												 &scaled))
				return false;
			if (cursor->materialized != nullptr)
			{
				MaterializedInput *mat = const_cast<MaterializedInput *>(cursor->materialized);

				if (mat->decimal128_columns[column_idx].size() < mat->row_count)
					mat->decimal128_columns[column_idx].resize(mat->row_count);
				mat->decimal128_columns[column_idx][row_idx] = scaled;
				mat->late_cached[column_idx][row_idx] = 1;
			}
			else
			{
				cursor->late_decimal128_values[column_idx] = scaled;
			}
			break;
		}
		case PG_VEC_SCALAR_CHAR1:
			if (cursor->materialized != nullptr)
			{
				MaterializedInput *mat = const_cast<MaterializedInput *>(cursor->materialized);

				if (mat->char1_columns[column_idx].size() < mat->row_count)
					mat->char1_columns[column_idx].resize(mat->row_count);
				if (!bpchar_varlena_to_char1(DatumGetPointer(value),
											 &mat->char1_columns[column_idx][row_idx]))
					return false;
				mat->late_cached[column_idx][row_idx] = 1;
			}
			else
			{
				if (!bpchar_varlena_to_char1(DatumGetPointer(value),
											 &cursor->late_char1_values[column_idx]))
					return false;
			}
			break;
		case PG_VEC_SCALAR_STRING128:
		{
			bool trim_trailing_spaces = (attr->atttypid == BPCHAROID);

			if (cursor->materialized != nullptr)
			{
				MaterializedInput *mat = const_cast<MaterializedInput *>(cursor->materialized);

				if (mat->string_columns[column_idx].size() < mat->row_count)
					mat->string_columns[column_idx].resize(mat->row_count);
				if (!varlena_payload_to_string_ref(DatumGetPointer(value),
												   trim_trailing_spaces,
												   &mat->string_arenas[column_idx],
												   &mat->string_columns[column_idx][row_idx]))
					return false;
				mat->late_cached[column_idx][row_idx] = 1;
			}
			else
			{
				if (!varlena_payload_to_string_ref(DatumGetPointer(value),
												   trim_trailing_spaces,
												   &cursor->late_string_arenas[column_idx],
												   &cursor->late_string_values[column_idx]))
					return false;
			}
			break;
		}
		case PG_VEC_SCALAR_INVALID:
		default:
			return false;
	}

	cursor->late_cached_mask |= (static_cast<uint32_t>(1) << column_idx);
	return true;
}

static ExecDataChunk *
allocate_exec_chunk(const PgVecInputSpec *spec)
{
	ExecDataChunk *chunk;

	chunk = reinterpret_cast<ExecDataChunk *>(palloc0(sizeof(ExecDataChunk)));
	chunk->ncolumns = spec->ncolumns;
	chunk->tuple_arena.data = nullptr;
	chunk->tuple_arena.size = 0;
	chunk->tuple_arena.capacity = 0;
	for (int i = 0; i < chunk->ncolumns; i++)
	{
		chunk->column_kinds[i] = spec->columns[i].scalar_kind;
		chunk->string_arenas[i].data = nullptr;
		chunk->string_arenas[i].size = 0;
		chunk->string_arenas[i].capacity = 0;
	}

	return chunk;
}

static DeformDecodeKind
decode_kind_for_scalar(PgVecScalarKind scalar_kind)
{
	switch (scalar_kind)
	{
		case PG_VEC_SCALAR_INT32:
			return DeformDecodeKind::kInt32;
		case PG_VEC_SCALAR_DATE32:
			return DeformDecodeKind::kDate32;
		case PG_VEC_SCALAR_DECIMAL64_S2:
			return DeformDecodeKind::kDecimal64Scale2;
		case PG_VEC_SCALAR_CHAR1:
			return DeformDecodeKind::kBpChar1;
		case PG_VEC_SCALAR_STRING128:
			return DeformDecodeKind::kStringRef;
		case PG_VEC_SCALAR_DECIMAL128_S6:
		case PG_VEC_SCALAR_DECIMAL128_S4:
		case PG_VEC_SCALAR_INVALID:
		default:
			elog(ERROR, "pg_vec: unsupported scan scalar kind %d",
				 (int) scalar_kind);
	}
}

static int
scan_column_index(const PgVecInputSpec *input, AttrNumber attno)
{
	for (int i = 0; i < input->ncolumns; i++)
	{
		if (input->columns[i].attno == attno)
			return i;
	}

	return -1;
}

static DeformProgram
build_deform_program(const PgVecInputSpec *input, uint32_t late_mask = 0)
{
	DeformProgram program;

	program.reset();

	for (int i = 0; i < input->ncolumns; i++)
	{
		if ((late_mask & (static_cast<uint32_t>(1) << i)) != 0)
			continue;
		if (!program.add_target(input->columns[i].attno - 1,
								 i,
								 decode_kind_for_scalar(input->columns[i].scalar_kind)))
			elog(ERROR, "pg_vec: failed to build deform program");
	}

	program.finalize();
	return program;
}

enum ColumnUsageFlags : uint8_t
{
	kUsageNone = 0,
	kUsageInputFilter = 1 << 0,
	kUsageJoinKey = 1 << 1,
	kUsageJoinFilter = 1 << 2,
	kUsageAggPayload = 1 << 3
};

static void
mark_column_usage(const PgVecInputSpec *input,
					 AttrNumber attno,
					 uint8 usage,
					 uint8 usage_flags[PG_VEC_MAX_SCAN_COLUMNS])
{
	int column_idx = scan_column_index(input, attno);

	if (column_idx >= 0)
		usage_flags[column_idx] |= usage;
}

static void
mark_expr_usage(const PgVecInputSpec inputs[PG_VEC_MAX_INPUTS],
				  const PgVecExprProgram &expr,
				  int node_idx,
				  uint8 usage,
				  uint8 usage_maps[PG_VEC_MAX_INPUTS][PG_VEC_MAX_SCAN_COLUMNS])
{
	if (node_idx < 0 || node_idx >= expr.nnodes)
		return;

	const PgVecExprNode &node = expr.nodes[node_idx];

	switch (node.kind)
	{
		case PG_VEC_EXPR_COLUMN:
			if (node.column.input_id < PG_VEC_MAX_INPUTS)
				mark_column_usage(&inputs[node.column.input_id],
								 node.column.attno,
								 usage,
								 usage_maps[node.column.input_id]);
			return;
		case PG_VEC_EXPR_EXTRACT_YEAR:
		case PG_VEC_EXPR_SUBSTRING_PREFIX2:
			mark_expr_usage(inputs, expr, node.left, usage, usage_maps);
			return;
		case PG_VEC_EXPR_ADD:
		case PG_VEC_EXPR_SUB:
		case PG_VEC_EXPR_MUL:
			mark_expr_usage(inputs, expr, node.left, usage, usage_maps);
			mark_expr_usage(inputs, expr, node.right, usage, usage_maps);
			return;
		case PG_VEC_EXPR_CONST:
		case PG_VEC_EXPR_INVALID:
		default:
			return;
	}
}

static void
mark_filter_usage(const PgVecInputSpec inputs[PG_VEC_MAX_INPUTS],
				   const PgVecFilterSpec &filter,
				   int qual_idx,
				   uint8 usage,
				   uint8 usage_maps[PG_VEC_MAX_INPUTS][PG_VEC_MAX_SCAN_COLUMNS])
{
	if (qual_idx < 0 || qual_idx >= filter.nnodes)
		return;

	const PgVecQualNode &qual = filter.nodes[qual_idx];

	switch (qual.kind)
	{
		case PG_VEC_QUAL_COMPARE:
			mark_expr_usage(inputs, filter.exprs, qual.lhs_expr, usage, usage_maps);
			mark_expr_usage(inputs, filter.exprs, qual.rhs_expr, usage, usage_maps);
			return;
		case PG_VEC_QUAL_AND:
		case PG_VEC_QUAL_OR:
			mark_filter_usage(inputs, filter, qual.left, usage, usage_maps);
			mark_filter_usage(inputs, filter, qual.right, usage, usage_maps);
			return;
		case PG_VEC_QUAL_INVALID:
		default:
			return;
	}
}

static void
build_single_join_late_masks(const PgVecScanFilterAggExecParams *params,
							 uint32_t late_masks[PG_VEC_MAX_INPUTS])
{
	uint8 usage_maps[PG_VEC_MAX_INPUTS][PG_VEC_MAX_SCAN_COLUMNS];

	std::memset(usage_maps, 0, sizeof(usage_maps));
	std::memset(late_masks, 0, sizeof(uint32_t) * PG_VEC_MAX_INPUTS);

	if (params->njoins != 1)
		return;

	for (int input_id = 0; input_id < params->ninputs; input_id++)
	{
		const PgVecInputSpec &input = params->inputs[input_id];

		if (input.filter.root >= 0)
			mark_filter_usage(params->inputs,
							  input.filter,
							  input.filter.root,
							  kUsageInputFilter,
							  usage_maps);
	}

	const PgVecJoinSpec &join = params->joins[0];
	for (int key_idx = 0; key_idx < join.nkeys; key_idx++)
	{
		mark_column_usage(&params->inputs[join.left_input],
						 join.keys[key_idx].left.attno,
						 kUsageJoinKey,
						 usage_maps[join.left_input]);
		mark_column_usage(&params->inputs[join.right_input],
						 join.keys[key_idx].right.attno,
						 kUsageJoinKey,
						 usage_maps[join.right_input]);
	}
	if (join.filter.root >= 0)
		mark_filter_usage(params->inputs,
						  join.filter,
						  join.filter.root,
						  kUsageJoinFilter,
						  usage_maps);

	for (int key_idx = 0; key_idx < params->agg.ngroup_keys; key_idx++)
		mark_expr_usage(params->inputs,
						params->agg.group_keys[key_idx].expr,
						params->agg.group_keys[key_idx].expr.root,
						kUsageAggPayload,
						usage_maps);

	for (int agg_idx = 0; agg_idx < params->agg.naggs; agg_idx++)
	{
		const PgVecAggCall &agg = params->agg.aggs[agg_idx];

		if (!agg.star_arg && agg.expr.root >= 0)
			mark_expr_usage(params->inputs,
							agg.expr,
							agg.expr.root,
							kUsageAggPayload,
							usage_maps);
		if (agg.has_filter && agg.filter.root >= 0)
			mark_filter_usage(params->inputs,
							  agg.filter,
							  agg.filter.root,
							  kUsageAggPayload,
							  usage_maps);
	}

	for (int input_id = 0; input_id < params->ninputs; input_id++)
	{
		const PgVecInputSpec &input = params->inputs[input_id];

		if (input.kind == PG_VEC_INPUT_DERIVED_GROUPED_AGG)
			continue;

		for (int col_idx = 0; col_idx < input.ncolumns; col_idx++)
		{
			uint8 flags = usage_maps[input_id][col_idx];

			if ((flags & kUsageAggPayload) != 0 &&
				(flags & (kUsageInputFilter | kUsageJoinKey | kUsageJoinFilter)) == 0)
				late_masks[input_id] |= (static_cast<uint32_t>(1) << col_idx);
		}
	}
}

static PgVecFilterOp
reverse_filter_op(PgVecFilterOp op)
{
	switch (op)
	{
		case PG_VEC_OP_EQ:
			return PG_VEC_OP_EQ;
		case PG_VEC_OP_NE:
			return PG_VEC_OP_NE;
		case PG_VEC_OP_LT:
			return PG_VEC_OP_GT;
		case PG_VEC_OP_LE:
			return PG_VEC_OP_GE;
		case PG_VEC_OP_GT:
			return PG_VEC_OP_LT;
		case PG_VEC_OP_GE:
			return PG_VEC_OP_LE;
		case PG_VEC_OP_PREFIX_LIKE:
		case PG_VEC_OP_CONTAINS_LIKE:
		case PG_VEC_OP_INVALID:
		default:
			return PG_VEC_OP_INVALID;
	}
}

static bool
bound_filter_supports_scalar(PgVecScalarKind scalar_kind)
{
	switch (scalar_kind)
	{
		case PG_VEC_SCALAR_INT32:
		case PG_VEC_SCALAR_DATE32:
		case PG_VEC_SCALAR_DECIMAL64_S2:
		case PG_VEC_SCALAR_CHAR1:
		case PG_VEC_SCALAR_STRING128:
			return true;
		case PG_VEC_SCALAR_DECIMAL128_S4:
		case PG_VEC_SCALAR_DECIMAL128_S6:
		case PG_VEC_SCALAR_INVALID:
		default:
			return false;
	}
}

static bool
append_bound_filter_clause(BoundFilterProgram *program,
						   int att_index,
						   std::uint16_t dst_col,
						   PgVecScalarKind scalar_kind,
						   PgVecFilterOp op,
						   const PgVecConstValue &constant,
						   int *clause_idx)
{
	if (program->nclauses >= PG_VEC_MAX_FILTER_NODES)
		return false;

	program->clauses[program->nclauses].att_index = att_index;
	program->clauses[program->nclauses].dst_col = dst_col;
	program->clauses[program->nclauses].scalar_kind = scalar_kind;
	program->clauses[program->nclauses].op = op;
	program->clauses[program->nclauses].constant = constant;
	if (att_index > program->last_att_index)
		program->last_att_index = att_index;
	*clause_idx = program->nclauses;
	program->nclauses++;
	return true;
}

static bool
add_bound_filter_node(BoundFilterProgram *program,
					  const DeformFilterNode &node,
					  int *node_idx)
{
	if (program->nnodes >= PG_VEC_MAX_FILTER_NODES)
		return false;

	program->nodes[program->nnodes] = node;
	*node_idx = program->nnodes++;
	return true;
}

static bool
bind_compare_clause(const PgVecInputSpec *input,
					uint8 input_id,
					const PgVecFilterSpec &filter,
					int qual_idx,
					BoundFilterProgram *program,
					int *node_idx)
{
	const PgVecExprNode *column_expr;
	const PgVecExprNode *const_expr;
	PgVecFilterOp op;
	int			column_idx;
	int			clause_idx;
	DeformFilterNode node{};

	if (qual_idx < 0 || qual_idx >= filter.nnodes)
		return false;

	const PgVecQualNode &qual = filter.nodes[qual_idx];

	if (qual.kind != PG_VEC_QUAL_COMPARE ||
		qual.lhs_expr < 0 || qual.lhs_expr >= filter.exprs.nnodes ||
		qual.rhs_expr < 0 || qual.rhs_expr >= filter.exprs.nnodes)
		return false;

	const PgVecExprNode &lhs_expr = filter.exprs.nodes[qual.lhs_expr];
	const PgVecExprNode &rhs_expr = filter.exprs.nodes[qual.rhs_expr];

	op = qual.op;
	if (lhs_expr.kind == PG_VEC_EXPR_COLUMN && rhs_expr.kind == PG_VEC_EXPR_CONST)
	{
		column_expr = &lhs_expr;
		const_expr = &rhs_expr;
	}
	else if (lhs_expr.kind == PG_VEC_EXPR_CONST &&
			 rhs_expr.kind == PG_VEC_EXPR_COLUMN &&
			 qual.op != PG_VEC_OP_PREFIX_LIKE &&
			 qual.op != PG_VEC_OP_CONTAINS_LIKE &&
			 qual.op != PG_VEC_OP_NOT_PREFIX_LIKE &&
			 qual.op != PG_VEC_OP_NOT_CONTAINS_LIKE &&
			 qual.op != PG_VEC_OP_SQL_LIKE &&
			 qual.op != PG_VEC_OP_NOT_SQL_LIKE)
	{
		op = reverse_filter_op(qual.op);
		if (op == PG_VEC_OP_INVALID)
			return false;
		column_expr = &rhs_expr;
		const_expr = &lhs_expr;
	}
	else
	{
		return false;
	}

	if (column_expr->column.input_id != input_id ||
		column_expr->column.scalar_kind != const_expr->scalar_kind ||
		!bound_filter_supports_scalar(column_expr->column.scalar_kind))
		return false;

	if ((op == PG_VEC_OP_PREFIX_LIKE ||
		 op == PG_VEC_OP_CONTAINS_LIKE ||
		 op == PG_VEC_OP_NOT_PREFIX_LIKE ||
		 op == PG_VEC_OP_NOT_CONTAINS_LIKE ||
		 op == PG_VEC_OP_SQL_LIKE ||
		 op == PG_VEC_OP_NOT_SQL_LIKE) &&
		column_expr->column.scalar_kind != PG_VEC_SCALAR_STRING128)
		return false;

	column_idx = scan_column_index(input, column_expr->column.attno);
	if (column_idx < 0)
		return false;

	if (!append_bound_filter_clause(program,
									  column_expr->column.attno - 1,
									  column_idx,
									  column_expr->column.scalar_kind,
									  op,
									  const_expr->constant,
									  &clause_idx))
		return false;

	node.kind = DeformFilterNodeKind::kClause;
	node.left = -1;
	node.right = -1;
	node.clause_idx = clause_idx;
	return add_bound_filter_node(program, node, node_idx);
}

static bool
bind_filter_node(const PgVecInputSpec *input,
					uint8 input_id,
					const PgVecFilterSpec &filter,
					int qual_idx,
					BoundFilterProgram *program,
					int *node_idx)
{
	DeformFilterNode node{};
	int left_idx;
	int right_idx;

	if (qual_idx < 0 || qual_idx >= filter.nnodes)
		return false;

	const PgVecQualNode &qual = filter.nodes[qual_idx];

	switch (qual.kind)
	{
		case PG_VEC_QUAL_COMPARE:
			return bind_compare_clause(input, input_id, filter, qual_idx, program, node_idx);
		case PG_VEC_QUAL_AND:
		case PG_VEC_QUAL_OR:
			if (qual.kind == PG_VEC_QUAL_OR)
				program->deform_safe = false;
			if (!bind_filter_node(input, input_id, filter, qual.left, program, &left_idx) ||
				!bind_filter_node(input, input_id, filter, qual.right, program, &right_idx))
				return false;
			node.kind = (qual.kind == PG_VEC_QUAL_AND) ?
				DeformFilterNodeKind::kAnd :
				DeformFilterNodeKind::kOr;
			node.left = left_idx;
			node.right = right_idx;
			node.clause_idx = -1;
			return add_bound_filter_node(program, node, node_idx);
		case PG_VEC_QUAL_INVALID:
		default:
			return false;
	}
}

struct DnfBuildResult
{
	int nbranches;
	uint64_t branch_masks[PG_VEC_MAX_FILTER_NODES];
};

static bool
build_filter_dnf(const BoundFilterProgram *program,
				 int node_idx,
				 DnfBuildResult *out)
{
	const DeformFilterNode &node = program->nodes[node_idx];

	switch (node.kind)
	{
		case DeformFilterNodeKind::kClause:
			out->nbranches = 1;
			out->branch_masks[0] = (UINT64CONST(1) << node.clause_idx);
			return true;
		case DeformFilterNodeKind::kOr:
		{
			DnfBuildResult left{};
			DnfBuildResult right{};

			if (!build_filter_dnf(program, node.left, &left) ||
				!build_filter_dnf(program, node.right, &right) ||
				left.nbranches + right.nbranches > PG_VEC_MAX_FILTER_NODES)
				return false;

			out->nbranches = 0;
			for (int i = 0; i < left.nbranches; i++)
				out->branch_masks[out->nbranches++] = left.branch_masks[i];
			for (int i = 0; i < right.nbranches; i++)
				out->branch_masks[out->nbranches++] = right.branch_masks[i];
			return true;
		}
		case DeformFilterNodeKind::kAnd:
		{
			DnfBuildResult left{};
			DnfBuildResult right{};

			if (!build_filter_dnf(program, node.left, &left) ||
				!build_filter_dnf(program, node.right, &right) ||
				left.nbranches * right.nbranches > PG_VEC_MAX_FILTER_NODES)
				return false;

			out->nbranches = 0;
			for (int i = 0; i < left.nbranches; i++)
			{
				for (int j = 0; j < right.nbranches; j++)
					out->branch_masks[out->nbranches++] =
						left.branch_masks[i] | right.branch_masks[j];
			}
			return true;
		}
		case DeformFilterNodeKind::kInvalid:
		default:
			return false;
	}
}

static void
finalize_bound_input_filter(BoundFilterProgram *program)
{
	DnfBuildResult dnf{};

	program->max_clause_att_index = -1;
	for (int att_idx = 0; att_idx < PG_VEC_MAX_FILTER_NODES; att_idx++)
		program->att_clause_heads[att_idx] = -1;
	for (int clause_idx = 0; clause_idx < program->nclauses; clause_idx++)
	{
		int att_index = program->clauses[clause_idx].att_index;

		if (att_index >= 0 && att_index < PG_VEC_MAX_FILTER_NODES)
		{
			program->clause_next[clause_idx] = program->att_clause_heads[att_index];
			program->att_clause_heads[att_index] = clause_idx;
			if (att_index > program->max_clause_att_index)
				program->max_clause_att_index = att_index;
		}
		else
		{
			program->valid = false;
			program->deform_safe = false;
			return;
		}
	}

	program->dnf_valid = build_filter_dnf(program, program->root, &dnf);
	program->ndnf_branches = 0;
	program->all_branch_mask = 0;
	std::memset(program->dnf_branch_masks, 0, sizeof(program->dnf_branch_masks));
	std::memset(program->clause_branch_masks, 0, sizeof(program->clause_branch_masks));

	if (program->dnf_valid)
	{
		program->ndnf_branches = dnf.nbranches;
		for (int branch_idx = 0; branch_idx < dnf.nbranches; branch_idx++)
		{
			uint64_t branch_mask = dnf.branch_masks[branch_idx];

			program->dnf_branch_masks[branch_idx] = branch_mask;
			program->all_branch_mask |= (UINT64CONST(1) << branch_idx);
			for (int clause_idx = 0; clause_idx < program->nclauses; clause_idx++)
			{
				if ((branch_mask & (UINT64CONST(1) << clause_idx)) != 0)
					program->clause_branch_masks[clause_idx] |=
						(UINT64CONST(1) << branch_idx);
			}
		}
		if (program->ndnf_branches > 0)
			program->deform_safe = true;
	}
}

static BoundFilterProgram
build_bound_input_filter(const PgVecInputSpec *input, uint8 input_id)
{
	BoundFilterProgram program;

	std::memset(&program, 0, sizeof(program));
	program.deform_safe = true;
	program.root = -1;
	program.last_att_index = -1;

	if (input->filter.nnodes == 0)
	{
		program.valid = true;
		return program;
	}

	if (input->filter.root < 0)
		return program;

	program.valid = bind_filter_node(input,
									 input_id,
									 input->filter,
									 input->filter.root,
									 &program,
									 &program.root);
	if (!program.valid)
	{
		program.nclauses = 0;
		program.nnodes = 0;
		program.root = -1;
		program.last_att_index = -1;
	}
	else
	{
		finalize_bound_input_filter(&program);
		if (!program.valid)
		{
			program.nclauses = 0;
			program.nnodes = 0;
			program.root = -1;
			program.last_att_index = -1;
		}
	}

	return program;
}

static bool
add_bound_join_node(BoundJoinFilterProgram *program,
					  const BoundJoinFilterNode &node,
					  int *node_idx)
{
	if (program->nnodes >= PG_VEC_MAX_FILTER_NODES)
		return false;

	program->nodes[program->nnodes] = node;
	*node_idx = program->nnodes++;
	return true;
}

static bool
append_bound_join_clause(BoundJoinFilterProgram *program,
						   uint8 input_id,
						   uint8 column_idx,
						   PgVecScalarKind scalar_kind,
						   PgVecFilterOp op,
						   const PgVecConstValue &constant,
						   int *clause_idx)
{
	if (program->nclauses >= PG_VEC_MAX_FILTER_NODES)
		return false;

	program->clauses[program->nclauses].input_id = input_id;
	program->clauses[program->nclauses].column_idx = column_idx;
	program->clauses[program->nclauses].scalar_kind = scalar_kind;
	program->clauses[program->nclauses].op = op;
	program->clauses[program->nclauses].constant = constant;
	*clause_idx = program->nclauses;
	program->input_masks[input_id] |= (UINT64CONST(1) << program->nclauses);
	program->nclauses++;
	return true;
}

static bool
bind_join_compare_clause(const PgVecScanFilterAggExecParams *params,
						   const PgVecFilterSpec &filter,
						   int qual_idx,
						   BoundJoinFilterProgram *program,
						   int *node_idx)
{
	const PgVecExprNode *column_expr;
	const PgVecExprNode *const_expr;
	const PgVecInputSpec *input;
	PgVecFilterOp op;
	int column_idx;
	int clause_idx;
	BoundJoinFilterNode node{};

	if (qual_idx < 0 || qual_idx >= filter.nnodes)
		return false;

	const PgVecQualNode &qual = filter.nodes[qual_idx];

	if (qual.kind != PG_VEC_QUAL_COMPARE ||
		qual.lhs_expr < 0 || qual.lhs_expr >= filter.exprs.nnodes ||
		qual.rhs_expr < 0 || qual.rhs_expr >= filter.exprs.nnodes)
		return false;

	const PgVecExprNode &lhs_expr = filter.exprs.nodes[qual.lhs_expr];
	const PgVecExprNode &rhs_expr = filter.exprs.nodes[qual.rhs_expr];

	op = qual.op;
	if (lhs_expr.kind == PG_VEC_EXPR_COLUMN && rhs_expr.kind == PG_VEC_EXPR_CONST)
	{
		column_expr = &lhs_expr;
		const_expr = &rhs_expr;
	}
	else if (lhs_expr.kind == PG_VEC_EXPR_CONST &&
			 rhs_expr.kind == PG_VEC_EXPR_COLUMN &&
			 qual.op != PG_VEC_OP_PREFIX_LIKE &&
			 qual.op != PG_VEC_OP_CONTAINS_LIKE &&
			 qual.op != PG_VEC_OP_NOT_PREFIX_LIKE &&
			 qual.op != PG_VEC_OP_NOT_CONTAINS_LIKE &&
			 qual.op != PG_VEC_OP_SQL_LIKE &&
			 qual.op != PG_VEC_OP_NOT_SQL_LIKE)
	{
		op = reverse_filter_op(qual.op);
		if (op == PG_VEC_OP_INVALID)
			return false;
		column_expr = &rhs_expr;
		const_expr = &lhs_expr;
	}
	else
	{
		return false;
	}

	if (column_expr->column.input_id >= params->ninputs ||
		column_expr->column.scalar_kind != const_expr->scalar_kind ||
		!bound_filter_supports_scalar(column_expr->column.scalar_kind))
		return false;

	if ((op == PG_VEC_OP_PREFIX_LIKE ||
		 op == PG_VEC_OP_CONTAINS_LIKE ||
		 op == PG_VEC_OP_NOT_PREFIX_LIKE ||
		 op == PG_VEC_OP_NOT_CONTAINS_LIKE ||
		 op == PG_VEC_OP_SQL_LIKE ||
		 op == PG_VEC_OP_NOT_SQL_LIKE) &&
		column_expr->column.scalar_kind != PG_VEC_SCALAR_STRING128)
		return false;

	input = &params->inputs[column_expr->column.input_id];
	column_idx = scan_column_index(input, column_expr->column.attno);
	if (column_idx < 0)
		return false;

	if (!append_bound_join_clause(program,
									column_expr->column.input_id,
									(uint8) column_idx,
									column_expr->column.scalar_kind,
									op,
									const_expr->constant,
									&clause_idx))
		return false;

	node.kind = BoundJoinQualKind::kClause;
	node.left = -1;
	node.right = -1;
	node.clause_idx = clause_idx;
	return add_bound_join_node(program, node, node_idx);
}

static bool
bind_join_filter_node(const PgVecScanFilterAggExecParams *params,
					   const PgVecFilterSpec &filter,
					   int qual_idx,
					   BoundJoinFilterProgram *program,
					   int *node_idx)
{
	const PgVecQualNode &qual = filter.nodes[qual_idx];
	BoundJoinFilterNode node{};
	int left_idx;
	int right_idx;

	if (qual_idx < 0 || qual_idx >= filter.nnodes)
		return false;

	switch (qual.kind)
	{
		case PG_VEC_QUAL_COMPARE:
			return bind_join_compare_clause(params, filter, qual_idx, program, node_idx);
		case PG_VEC_QUAL_AND:
		case PG_VEC_QUAL_OR:
			if (!bind_join_filter_node(params, filter, qual.left, program, &left_idx) ||
				!bind_join_filter_node(params, filter, qual.right, program, &right_idx))
				return false;
			node.kind = (qual.kind == PG_VEC_QUAL_AND) ?
				BoundJoinQualKind::kAnd :
				BoundJoinQualKind::kOr;
			node.left = left_idx;
			node.right = right_idx;
			node.clause_idx = -1;
			return add_bound_join_node(program, node, node_idx);
		case PG_VEC_QUAL_INVALID:
		default:
			return false;
	}
}

static BoundJoinFilterProgram
build_bound_join_filter(const PgVecScanFilterAggExecParams *params,
						  const PgVecFilterSpec &filter)
{
	BoundJoinFilterProgram program;

	std::memset(&program, 0, sizeof(program));
	program.root = -1;

	if (filter.nnodes == 0 || filter.root < 0)
	{
		program.valid = true;
		return program;
	}

	program.valid = bind_join_filter_node(params,
											filter,
											filter.root,
											&program,
											&program.root);
	if (!program.valid)
	{
		program.nnodes = 0;
		program.nclauses = 0;
		program.root = -1;
	}

	return program;
}


struct ExecChunkBindings
{
	DeformColumnBinding columns[PG_VEC_MAX_SCAN_COLUMNS];
	DeformBindings	bindings;

	explicit ExecChunkBindings(ExecDataChunk &chunk)
	{
		for (int i = 0; i < chunk.ncolumns; i++)
		{
			switch (chunk.column_kinds[i])
			{
				case PG_VEC_SCALAR_INT32:
					columns[i].data = chunk.int32_values[i];
					break;
				case PG_VEC_SCALAR_DATE32:
					columns[i].data = chunk.date32_values[i];
					break;
				case PG_VEC_SCALAR_DECIMAL64_S2:
					columns[i].data = chunk.decimal64_values[i];
					break;
				case PG_VEC_SCALAR_CHAR1:
					columns[i].data = chunk.char1_values[i];
					break;
				case PG_VEC_SCALAR_STRING128:
					columns[i].data = chunk.string_refs[i];
					columns[i].aux = &chunk.string_arenas[i];
					break;
				case PG_VEC_SCALAR_DECIMAL128_S6:
				case PG_VEC_SCALAR_DECIMAL128_S4:
				case PG_VEC_SCALAR_INVALID:
				default:
						elog(ERROR, "pg_vec: unsupported chunk binding scalar kind %d",
							 (int) chunk.column_kinds[i]);
			}

			if (chunk.column_kinds[i] != PG_VEC_SCALAR_STRING128)
				columns[i].aux = nullptr;
		}

		bindings.columns = columns;
		bindings.ncolumns = chunk.ncolumns;
		for (int i = 0; i < chunk.ncolumns; i++)
		{
			bindings.columns_data[i] = columns[i].data;
			bindings.columns_nulls[i] = nullptr;
		}
	}
};

class HeapDataChunkScanner
{
public:
	HeapDataChunkScanner(Relation rel,
						 Snapshot snapshot,
						 const PgVecInputSpec *input,
						 const BoundFilterProgram *bound_filter,
						 uint32_t late_mask = 0,
						 bool enable_jit_deform = false) :
		rel_(rel),
		snapshot_(snapshot),
		input_(input),
		late_mask_(late_mask),
		bound_filter_(bound_filter != nullptr && bound_filter->deform_safe ?
					  bound_filter :
					  nullptr),
		desc_(RelationGetDescr(rel)),
		program_(build_deform_program(input_, late_mask_)),
		deformer_(desc_, &program_),
		filter_applied_in_deformer_(bound_filter_ != nullptr),
		jit_context_(nullptr)
	{
		uint32		flags = SO_TYPE_SEQSCAN |
			SO_ALLOW_STRAT |
			SO_ALLOW_SYNC |
			SO_ALLOW_PAGEMODE;

		scan_ = (HeapScanDesc) heap_beginscan(rel_,
											 snapshot_,
											 0,
											 NULL,
											 NULL,
											 flags);
		page_visible_index_ = 0;

		if (enable_jit_deform && late_mask_ == 0)
		{
			JitDeformFunc jit_func = nullptr;
			JitContext *jit_context = nullptr;
			const char *failure_reason = nullptr;

			if (pg_vec_try_compile_jit_deform_to_datachunk(desc_,
														   &program_,
														   &jit_func,
														   &jit_context,
														   &failure_reason))
				{
					deformer_.set_jit_func(jit_func);
					filter_applied_in_deformer_ = false;
					jit_context_ = jit_context;
				}
			}
		}

	~HeapDataChunkScanner()
	{
#ifdef USE_LLVM
		if (jit_context_ != nullptr)
			llvm_release_context_direct((LLVMJitContext *) jit_context_);
#endif
		if (scan_ != NULL)
			heap_endscan((TableScanDesc) scan_);
	}

	bool
	next_chunk(ExecDataChunk &chunk)
	{
		ExecChunkBindings bindings(chunk);

		chunk.reset();

		while (chunk.count < kExecChunkCapacity)
		{
			if (!ensure_page_loaded())
				break;

			if (!append_visible_tuple(chunk, bindings.bindings))
				continue;

			chunk.count++;
		}

		return chunk.count > 0;
	}

private:
	bool
	ensure_page_loaded()
	{
		while (true)
		{
			if (BufferIsValid(scan_->rs_cbuf) &&
				page_visible_index_ < scan_->rs_ntuples)
				return true;

			release_current_page();
			load_next_page();
			if (!BufferIsValid(scan_->rs_cbuf))
				return false;
			if (scan_->rs_ntuples > 0)
				return true;
		}
	}

	void
	load_next_page()
	{
		if (BufferIsValid(scan_->rs_cbuf))
		{
			ReleaseBuffer(scan_->rs_cbuf);
			scan_->rs_cbuf = InvalidBuffer;
		}

		CHECK_FOR_INTERRUPTS();

		if (unlikely(scan_->rs_dir != ForwardScanDirection))
		{
			scan_->rs_prefetch_block = scan_->rs_cblock;
			read_stream_reset(scan_->rs_read_stream);
		}

		scan_->rs_dir = ForwardScanDirection;
		scan_->rs_cbuf = read_stream_next_buffer(scan_->rs_read_stream, NULL);
		if (!BufferIsValid(scan_->rs_cbuf))
		{
			scan_->rs_cblock = InvalidBlockNumber;
			scan_->rs_ntuples = 0;
			page_visible_index_ = 0;
			return;
		}

		scan_->rs_cblock = BufferGetBlockNumber(scan_->rs_cbuf);
		heap_prepare_pagescan((TableScanDesc) scan_);
		page_visible_index_ = 0;
	}

	void
	release_current_page()
	{
		if (BufferIsValid(scan_->rs_cbuf))
		{
			ReleaseBuffer(scan_->rs_cbuf);
			scan_->rs_cbuf = InvalidBuffer;
		}
		scan_->rs_cblock = InvalidBlockNumber;
		scan_->rs_ntuples = 0;
		page_visible_index_ = 0;
	}

	bool
	append_visible_tuple(ExecDataChunk &chunk, const DeformBindings &bindings)
	{
		OffsetNumber offnum = scan_->rs_vistuples[page_visible_index_++];
		Page		page = BufferGetPage(scan_->rs_cbuf);
		ItemId		lpp = PageGetItemId(page, offnum);
		HeapTupleData tuple;

		tuple.t_data = (HeapTupleHeader) PageGetItem(page, lpp);
		tuple.t_len = ItemIdGetLength(lpp);
		tuple.t_tableOid = RelationGetRelid(scan_->rs_base.rs_rd);
		ItemPointerSet(&(tuple.t_self), scan_->rs_cblock, offnum);

		switch (deformer_.append_tuple(&tuple, chunk.count, bindings, bound_filter_))
		{
			case AppendTupleResult::kStored:
				chunk.deform_filter_applied = filter_applied_in_deformer_;
				chunk.tids[chunk.count] = tuple.t_self;
				chunk.tuple_copies[chunk.count].offset = kNoTupleCopyOffset;
				chunk.tuple_copies[chunk.count].len = 0;
				if (late_mask_ != 0 && chunk.count < kLateTupleCopyThreshold)
				{
					if (!string_arena_append(&chunk.tuple_arena,
											 reinterpret_cast<const char *>(tuple.t_data),
											 tuple.t_len,
											 &chunk.tuple_copies[chunk.count].offset))
						elog(ERROR, "pg_vec: failed to copy tuple bytes for late materialization");
					chunk.tuple_copies[chunk.count].len = tuple.t_len;
				}
				return true;
			case AppendTupleResult::kFilteredOut:
				return false;
			case AppendTupleResult::kError:
			default:
			{
				int attno0 = (input_ != nullptr && input_->ncolumns > 0) ? input_->columns[0].attno : -1;
				int attno1 = (input_ != nullptr && input_->ncolumns > 1) ? input_->columns[1].attno : -1;
				int attno2 = (input_ != nullptr && input_->ncolumns > 2) ? input_->columns[2].attno : -1;
				int attno3 = (input_ != nullptr && input_->ncolumns > 3) ? input_->columns[3].attno : -1;

				release_current_page();
				elog(ERROR,
					 "pg_vec: failed to deform tuple into DataChunk for relation \"%s\" (natts=%d, last_att_index=%d, ntargets=%d, ncolumns=%d, late_mask=%u, attnos=[%d,%d,%d,%d])",
					 RelationGetRelationName(rel_),
					 (int) HeapTupleHeaderGetNatts(tuple.t_data),
					 program_.last_att_index,
					 program_.ntargets,
					 input_ != nullptr ? input_->ncolumns : -1,
					 late_mask_,
					 attno0,
					 attno1,
					 attno2,
					 attno3);
			}
		}
	}

	Relation	rel_;
	Snapshot	snapshot_;
	const PgVecInputSpec *input_;
	uint32_t	late_mask_;
	const BoundFilterProgram *bound_filter_;
	HeapScanDesc scan_;
	TupleDesc	desc_;
	DeformProgram program_;
	DataChunkDeformer deformer_;
	bool		filter_applied_in_deformer_;
	JitContext  *jit_context_;
	uint32		page_visible_index_;
};

static int
compare_string(const PgVecStringConst &lhs, const PgVecStringConst &rhs)
{
	return string_const_compare(lhs, rhs);
}

static int
scalar_numeric_scale(PgVecScalarKind scalar_kind)
{
	switch (scalar_kind)
	{
		case PG_VEC_SCALAR_DECIMAL64_S2:
		case PG_VEC_SCALAR_DECIMAL128_S2:
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

static __int128
scale_numeric_value(__int128 value, int scale_delta)
{
	while (scale_delta > 0)
	{
		value *= 10;
		scale_delta--;
	}
	return value;
}

template <typename T>
static bool
compare_value(T lhs, T rhs, PgVecFilterOp op)
{
	switch (op)
	{
		case PG_VEC_OP_EQ:
			return lhs == rhs;
		case PG_VEC_OP_NE:
			return lhs != rhs;
		case PG_VEC_OP_LT:
			return lhs < rhs;
		case PG_VEC_OP_LE:
			return lhs <= rhs;
		case PG_VEC_OP_GT:
			return lhs > rhs;
		case PG_VEC_OP_GE:
			return lhs >= rhs;
		case PG_VEC_OP_PREFIX_LIKE:
		case PG_VEC_OP_CONTAINS_LIKE:
		case PG_VEC_OP_NOT_PREFIX_LIKE:
		case PG_VEC_OP_NOT_CONTAINS_LIKE:
		case PG_VEC_OP_SQL_LIKE:
		case PG_VEC_OP_NOT_SQL_LIKE:
		case PG_VEC_OP_INVALID:
		default:
			return false;
	}
}

static bool
compare_numeric_expr_values(const PgVecExprProgram &exprs,
							   int lhs_expr,
							   int rhs_expr,
							   const EvalContext &ctx,
							   PgVecFilterOp op)
{
	__int128 lhs_value;
	__int128 rhs_value;
	int lhs_scale;
	int rhs_scale;

	if (!eval_expr_to_int128(exprs, lhs_expr, ctx, &lhs_value) ||
		!eval_expr_to_int128(exprs, rhs_expr, ctx, &rhs_value))
		return false;

	lhs_scale = scalar_numeric_scale(exprs.nodes[lhs_expr].scalar_kind);
	rhs_scale = scalar_numeric_scale(exprs.nodes[rhs_expr].scalar_kind);
	if (lhs_scale >= 0 && rhs_scale >= 0 && lhs_scale != rhs_scale)
	{
		if (lhs_scale < rhs_scale)
			lhs_value = scale_numeric_value(lhs_value, rhs_scale - lhs_scale);
		else
			rhs_value = scale_numeric_value(rhs_value, lhs_scale - rhs_scale);
	}

	return compare_value(lhs_value, rhs_value, op);
}

struct StringCursorValue
{
	const PgVecStringRef *ref;
	const PgVecStringArena *arena;
};

static bool
cursor_get_column_index(const ExecInputCursor &cursor, AttrNumber attno, int *column_idx)
{
	if (cursor.spec == nullptr)
		return false;
	*column_idx = scan_column_index(cursor.spec, attno);
	return *column_idx >= 0;
}

static bool
cursor_get_int32(const ExecInputCursor &cursor, AttrNumber attno, std::int32_t *out)
{
	int			column_idx;

	if (cursor.null_row)
		return false;
	if (!cursor_get_column_index(cursor, attno, &column_idx))
		return false;

	if ((cursor.late_mask & (static_cast<uint32_t>(1) << column_idx)) != 0)
	{
		if (!cursor_fetch_late_column(const_cast<ExecInputCursor *>(&cursor), column_idx))
			return false;
		if (cursor.materialized != nullptr)
			*out = cursor.materialized->int32_columns[column_idx][cursor.materialized_row];
		else
			*out = cursor.late_int32_values[column_idx];
		return true;
	}

	if (cursor.materialized != nullptr)
		*out = cursor.materialized->int32_columns[column_idx][cursor.materialized_row];
	else
		*out = cursor.chunk->int32_values[column_idx][cursor.chunk_row];
	return true;
}

static bool
cursor_get_date32(const ExecInputCursor &cursor, AttrNumber attno, std::int32_t *out)
{
	int			column_idx;

	if (cursor.null_row)
		return false;
	if (!cursor_get_column_index(cursor, attno, &column_idx))
		return false;

	if ((cursor.late_mask & (static_cast<uint32_t>(1) << column_idx)) != 0)
	{
		if (!cursor_fetch_late_column(const_cast<ExecInputCursor *>(&cursor), column_idx))
			return false;
		if (cursor.materialized != nullptr)
			*out = cursor.materialized->date32_columns[column_idx][cursor.materialized_row];
		else
			*out = cursor.late_date32_values[column_idx];
		return true;
	}

	if (cursor.materialized != nullptr)
		*out = cursor.materialized->date32_columns[column_idx][cursor.materialized_row];
	else
		*out = cursor.chunk->date32_values[column_idx][cursor.chunk_row];
	return true;
}

static bool
cursor_get_decimal64(const ExecInputCursor &cursor, AttrNumber attno, std::int64_t *out)
{
	int			column_idx;

	if (cursor.null_row)
		return false;
	if (!cursor_get_column_index(cursor, attno, &column_idx))
		return false;

	if ((cursor.late_mask & (static_cast<uint32_t>(1) << column_idx)) != 0)
	{
		if (!cursor_fetch_late_column(const_cast<ExecInputCursor *>(&cursor), column_idx))
			return false;
		if (cursor.materialized != nullptr)
			*out = cursor.materialized->decimal64_columns[column_idx][cursor.materialized_row];
		else
			*out = cursor.late_decimal64_values[column_idx];
		return true;
	}

	if (cursor.materialized != nullptr)
		*out = cursor.materialized->decimal64_columns[column_idx][cursor.materialized_row];
	else
		*out = cursor.chunk->decimal64_values[column_idx][cursor.chunk_row];
	return true;
}

static bool
cursor_get_decimal128(const ExecInputCursor &cursor, AttrNumber attno, __int128 *out)
{
	int			column_idx;

	if (cursor.null_row)
		return false;
	if (!cursor_get_column_index(cursor, attno, &column_idx))
		return false;

	if ((cursor.late_mask & (static_cast<uint32_t>(1) << column_idx)) != 0)
	{
		if (!cursor_fetch_late_column(const_cast<ExecInputCursor *>(&cursor), column_idx))
			return false;
		if (cursor.materialized != nullptr)
			*out = cursor.materialized->decimal128_columns[column_idx][cursor.materialized_row];
		else
			*out = cursor.late_decimal128_values[column_idx];
		return true;
	}

	if (cursor.materialized != nullptr)
		*out = cursor.materialized->decimal128_columns[column_idx][cursor.materialized_row];
	else
		*out = cursor.chunk->decimal128_values[column_idx][cursor.chunk_row];
	return true;
}

static bool
cursor_get_char1(const ExecInputCursor &cursor, AttrNumber attno, char *out)
{
	int			column_idx;

	if (cursor.null_row)
		return false;
	if (!cursor_get_column_index(cursor, attno, &column_idx))
		return false;

	if ((cursor.late_mask & (static_cast<uint32_t>(1) << column_idx)) != 0)
	{
		if (!cursor_fetch_late_column(const_cast<ExecInputCursor *>(&cursor), column_idx))
			return false;
		if (cursor.materialized != nullptr)
			*out = cursor.materialized->char1_columns[column_idx][cursor.materialized_row];
		else
			*out = cursor.late_char1_values[column_idx];
		return true;
	}

	if (cursor.materialized != nullptr)
		*out = cursor.materialized->char1_columns[column_idx][cursor.materialized_row];
	else
		*out = cursor.chunk->char1_values[column_idx][cursor.chunk_row];
	return true;
}

static bool
cursor_get_string_ref(const ExecInputCursor &cursor,
					   AttrNumber attno,
					   StringCursorValue *out)
{
	int			column_idx;

	if (cursor.null_row)
		return false;
	if (!cursor_get_column_index(cursor, attno, &column_idx))
		return false;

	if ((cursor.late_mask & (static_cast<uint32_t>(1) << column_idx)) != 0)
	{
		if (!cursor_fetch_late_column(const_cast<ExecInputCursor *>(&cursor), column_idx))
			return false;
		if (cursor.materialized != nullptr)
		{
			out->ref = &cursor.materialized->string_columns[column_idx][cursor.materialized_row];
			out->arena = &cursor.materialized->string_arenas[column_idx];
		}
		else
		{
			out->ref = &cursor.late_string_values[column_idx];
			out->arena = &cursor.late_string_arenas[column_idx];
		}
		return true;
	}

	if (cursor.materialized != nullptr)
	{
		out->ref = &cursor.materialized->string_columns[column_idx][cursor.materialized_row];
		out->arena = &cursor.materialized->string_arenas[column_idx];
	}
	else
	{
		out->ref = &cursor.chunk->string_refs[column_idx][cursor.chunk_row];
		out->arena = &cursor.chunk->string_arenas[column_idx];
	}
	return true;
}

static bool
cursor_get_int32_by_index(const ExecInputCursor &cursor,
						   int column_idx,
						   std::int32_t *out)
{
	if (cursor.null_row)
		return false;
	if ((cursor.late_mask & (static_cast<uint32_t>(1) << column_idx)) != 0)
	{
		if (!cursor_fetch_late_column(const_cast<ExecInputCursor *>(&cursor), column_idx))
			return false;
		if (cursor.materialized != nullptr)
			*out = cursor.materialized->int32_columns[column_idx][cursor.materialized_row];
		else
			*out = cursor.late_int32_values[column_idx];
		return true;
	}
	if (cursor.materialized != nullptr)
		*out = cursor.materialized->int32_columns[column_idx][cursor.materialized_row];
	else
		*out = cursor.chunk->int32_values[column_idx][cursor.chunk_row];
	return true;
}

static bool
cursor_get_date32_by_index(const ExecInputCursor &cursor,
						   int column_idx,
						   std::int32_t *out)
{
	if (cursor.null_row)
		return false;
	if ((cursor.late_mask & (static_cast<uint32_t>(1) << column_idx)) != 0)
	{
		if (!cursor_fetch_late_column(const_cast<ExecInputCursor *>(&cursor), column_idx))
			return false;
		if (cursor.materialized != nullptr)
			*out = cursor.materialized->date32_columns[column_idx][cursor.materialized_row];
		else
			*out = cursor.late_date32_values[column_idx];
		return true;
	}
	if (cursor.materialized != nullptr)
		*out = cursor.materialized->date32_columns[column_idx][cursor.materialized_row];
	else
		*out = cursor.chunk->date32_values[column_idx][cursor.chunk_row];
	return true;
}

static bool
cursor_get_decimal64_by_index(const ExecInputCursor &cursor,
							  int column_idx,
							  std::int64_t *out)
{
	if (cursor.null_row)
		return false;
	if ((cursor.late_mask & (static_cast<uint32_t>(1) << column_idx)) != 0)
	{
		if (!cursor_fetch_late_column(const_cast<ExecInputCursor *>(&cursor), column_idx))
			return false;
		if (cursor.materialized != nullptr)
			*out = cursor.materialized->decimal64_columns[column_idx][cursor.materialized_row];
		else
			*out = cursor.late_decimal64_values[column_idx];
		return true;
	}
	if (cursor.materialized != nullptr)
		*out = cursor.materialized->decimal64_columns[column_idx][cursor.materialized_row];
	else
		*out = cursor.chunk->decimal64_values[column_idx][cursor.chunk_row];
	return true;
}

static bool
cursor_get_decimal128_by_index(const ExecInputCursor &cursor,
							   int column_idx,
							   __int128 *out)
{
	if (cursor.null_row)
		return false;
	if ((cursor.late_mask & (static_cast<uint32_t>(1) << column_idx)) != 0)
	{
		if (!cursor_fetch_late_column(const_cast<ExecInputCursor *>(&cursor), column_idx))
			return false;
		if (cursor.materialized != nullptr)
			*out = cursor.materialized->decimal128_columns[column_idx][cursor.materialized_row];
		else
			*out = cursor.late_decimal128_values[column_idx];
		return true;
	}
	if (cursor.materialized != nullptr)
		*out = cursor.materialized->decimal128_columns[column_idx][cursor.materialized_row];
	else
		*out = cursor.chunk->decimal128_values[column_idx][cursor.chunk_row];
	return true;
}

static bool
cursor_get_char1_by_index(const ExecInputCursor &cursor,
						  int column_idx,
						  char *out)
{
	if (cursor.null_row)
		return false;
	if ((cursor.late_mask & (static_cast<uint32_t>(1) << column_idx)) != 0)
	{
		if (!cursor_fetch_late_column(const_cast<ExecInputCursor *>(&cursor), column_idx))
			return false;
		if (cursor.materialized != nullptr)
			*out = cursor.materialized->char1_columns[column_idx][cursor.materialized_row];
		else
			*out = cursor.late_char1_values[column_idx];
		return true;
	}
	if (cursor.materialized != nullptr)
		*out = cursor.materialized->char1_columns[column_idx][cursor.materialized_row];
	else
		*out = cursor.chunk->char1_values[column_idx][cursor.chunk_row];
	return true;
}

static bool
cursor_get_string_ref_by_index(const ExecInputCursor &cursor,
								  int column_idx,
								  StringCursorValue *out)
{
	if (cursor.null_row)
		return false;
	if ((cursor.late_mask & (static_cast<uint32_t>(1) << column_idx)) != 0)
	{
		if (!cursor_fetch_late_column(const_cast<ExecInputCursor *>(&cursor), column_idx))
			return false;
		if (cursor.materialized != nullptr)
		{
			out->ref = &cursor.materialized->string_columns[column_idx][cursor.materialized_row];
			out->arena = &cursor.materialized->string_arenas[column_idx];
		}
		else
		{
			out->ref = &cursor.late_string_values[column_idx];
			out->arena = &cursor.late_string_arenas[column_idx];
		}
		return true;
	}
	if (cursor.materialized != nullptr)
	{
		out->ref = &cursor.materialized->string_columns[column_idx][cursor.materialized_row];
		out->arena = &cursor.materialized->string_arenas[column_idx];
	}
	else
	{
		out->ref = &cursor.chunk->string_refs[column_idx][cursor.chunk_row];
		out->arena = &cursor.chunk->string_arenas[column_idx];
	}
	return true;
}

static bool
column_value_to_int128(const EvalContext &ctx,
					   const PgVecColumnRef &column,
					   __int128 *out)
{
	const ExecInputCursor &cursor = ctx.inputs[column.input_id];

	switch (column.scalar_kind)
	{
		case PG_VEC_SCALAR_INT32:
		{
			std::int32_t value;

			if (!cursor_get_int32(cursor, column.attno, &value))
				return false;
			*out = value;
			return true;
		}
		case PG_VEC_SCALAR_DATE32:
		{
			std::int32_t value;

			if (!cursor_get_date32(cursor, column.attno, &value))
				return false;
			*out = value;
			return true;
		}
		case PG_VEC_SCALAR_DECIMAL64_S2:
		{
			std::int64_t value;

			if (!cursor_get_decimal64(cursor, column.attno, &value))
				return false;
			*out = value;
			return true;
		}
		case PG_VEC_SCALAR_DECIMAL128_S2:
		case PG_VEC_SCALAR_DECIMAL128_S4:
		case PG_VEC_SCALAR_DECIMAL128_S6:
		{
			__int128 value;

			if (!cursor_get_decimal128(cursor, column.attno, &value))
				return false;
			*out = value;
			return true;
		}
		case PG_VEC_SCALAR_CHAR1:
		{
			char		value;

			if (!cursor_get_char1(cursor, column.attno, &value))
				return false;
			*out = static_cast<unsigned char>(value);
			return true;
		}
		case PG_VEC_SCALAR_STRING128:
		case PG_VEC_SCALAR_INVALID:
		default:
			return false;
	}
}

static bool
eval_bound_join_clause(const BoundJoinFilterClause &clause,
						  const EvalContext &ctx)
{
	const ExecInputCursor &cursor = ctx.inputs[clause.input_id];

	switch (clause.scalar_kind)
	{
		case PG_VEC_SCALAR_INT32:
		{
			std::int32_t value;

			if (!cursor_get_int32_by_index(cursor, clause.column_idx, &value))
				return false;
			return compare_value(value, clause.constant.int32_value, clause.op);
		}
		case PG_VEC_SCALAR_DATE32:
		{
			std::int32_t value;

			if (!cursor_get_date32_by_index(cursor, clause.column_idx, &value))
				return false;
			return compare_value(value, clause.constant.date32, clause.op);
		}
		case PG_VEC_SCALAR_DECIMAL64_S2:
		{
			std::int64_t value;

			if (!cursor_get_decimal64_by_index(cursor, clause.column_idx, &value))
				return false;
			return compare_value(value, clause.constant.decimal64_s2, clause.op);
		}
		case PG_VEC_SCALAR_DECIMAL128_S2:
		case PG_VEC_SCALAR_DECIMAL128_S4:
		case PG_VEC_SCALAR_DECIMAL128_S6:
		{
			__int128 value;

			if (!cursor_get_decimal128_by_index(cursor, clause.column_idx, &value))
				return false;
			return compare_value(value, clause.constant.decimal128, clause.op);
		}
		case PG_VEC_SCALAR_CHAR1:
		{
			char value;

			if (!cursor_get_char1_by_index(cursor, clause.column_idx, &value))
				return false;
			return compare_value(value, clause.constant.char1, clause.op);
		}
		case PG_VEC_SCALAR_STRING128:
		{
			StringCursorValue value;

			if (!cursor_get_string_ref_by_index(cursor, clause.column_idx, &value))
				return false;
			if (clause.op == PG_VEC_OP_PREFIX_LIKE)
				return string_ref_starts_with_const(*value.ref,
													 value.arena,
													 clause.constant.string128);
			if (clause.op == PG_VEC_OP_CONTAINS_LIKE)
				return string_ref_contains_const(*value.ref,
												  value.arena,
												  clause.constant.string128);
			if (clause.op == PG_VEC_OP_NOT_PREFIX_LIKE)
				return !string_ref_starts_with_const(*value.ref,
													  value.arena,
													  clause.constant.string128);
			if (clause.op == PG_VEC_OP_NOT_CONTAINS_LIKE)
				return !string_ref_contains_const(*value.ref,
												   value.arena,
												   clause.constant.string128);
			if (clause.op == PG_VEC_OP_SQL_LIKE)
				return string_ref_matches_like_const(*value.ref,
													 value.arena,
													 clause.constant.string128);
			if (clause.op == PG_VEC_OP_NOT_SQL_LIKE)
				return !string_ref_matches_like_const(*value.ref,
													  value.arena,
													  clause.constant.string128);
			return string_ref_compare_value(*value.ref,
											 value.arena,
											 clause.constant.string128,
											 clause.op);
		}
		case PG_VEC_SCALAR_INVALID:
		default:
			return false;
	}
}

static uint64_t
compute_bound_join_clause_bits(const BoundJoinFilterProgram &program,
							   uint8 input_id,
							   const EvalContext &ctx)
{
	uint64_t bits = 0;

	for (int clause_idx = 0; clause_idx < program.nclauses; clause_idx++)
	{
		if (program.clauses[clause_idx].input_id != input_id)
			continue;
		if (eval_bound_join_clause(program.clauses[clause_idx], ctx))
			bits |= (UINT64CONST(1) << clause_idx);
	}

	return bits;
}

static bool
eval_bound_join_filter_node(const BoundJoinFilterProgram &program,
							   int node_idx,
							   uint64_t clause_bits)
{
	const BoundJoinFilterNode &node = program.nodes[node_idx];

	switch (node.kind)
	{
		case BoundJoinQualKind::kClause:
			return (clause_bits & (UINT64CONST(1) << node.clause_idx)) != 0;
		case BoundJoinQualKind::kAnd:
			return eval_bound_join_filter_node(program, node.left, clause_bits) &&
				eval_bound_join_filter_node(program, node.right, clause_bits);
		case BoundJoinQualKind::kOr:
			return eval_bound_join_filter_node(program, node.left, clause_bits) ||
				eval_bound_join_filter_node(program, node.right, clause_bits);
		case BoundJoinQualKind::kInvalid:
		default:
			return false;
	}
}

static bool
const_value_to_int128(const PgVecConstValue &constant,
					  PgVecScalarKind scalar_kind,
					  __int128 *out)
{
	switch (scalar_kind)
	{
		case PG_VEC_SCALAR_INT32:
			*out = constant.int32_value;
			return true;
		case PG_VEC_SCALAR_DATE32:
			*out = constant.date32;
			return true;
		case PG_VEC_SCALAR_DECIMAL64_S2:
		case PG_VEC_SCALAR_DECIMAL128_S2:
			*out = constant.decimal64_s2;
			return true;
		case PG_VEC_SCALAR_DECIMAL128_S4:
		case PG_VEC_SCALAR_DECIMAL128_S6:
			*out = constant.decimal128;
			return true;
		case PG_VEC_SCALAR_CHAR1:
			*out = static_cast<unsigned char>(constant.char1);
			return true;
		case PG_VEC_SCALAR_STRING128:
		case PG_VEC_SCALAR_INVALID:
		default:
			return false;
	}
}

static bool
fetch_param_exec_to_int128(const PgVecScanFilterAggExecParams *params,
						   int param_id,
						   PgVecScalarKind scalar_kind,
						   __int128 *out,
						   bool *isnull)
{
	ParamExecData *prm;
	ExprContext *econtext;

	if (params == nullptr || params->estate == nullptr ||
		param_id < 0 ||
		param_id >= list_length(params->estate->es_plannedstmt->paramExecTypes))
		return false;

	prm = &params->estate->es_param_exec_vals[param_id];
	if (prm->execPlan != nullptr)
	{
		econtext = GetPerTupleExprContext(params->estate);
		ExecSetParamPlan((SubPlanState *) prm->execPlan, econtext);
	}

	*isnull = prm->isnull;
	if (*isnull)
		return true;

	switch (scalar_kind)
	{
		case PG_VEC_SCALAR_INT32:
			*out = DatumGetInt32(prm->value);
			return true;
		case PG_VEC_SCALAR_DATE32:
			*out = DatumGetDateADT(prm->value);
			return true;
		case PG_VEC_SCALAR_DECIMAL64_S2:
		case PG_VEC_SCALAR_DECIMAL128_S2:
		{
			int64 scaled;

			if (!numeric_varlena_to_scaled_int64(DatumGetPointer(prm->value), 2, &scaled))
				return false;
			*out = scaled;
			return true;
		}
		case PG_VEC_SCALAR_DECIMAL128_S4:
		{
			int64 scaled;

			if (!numeric_varlena_to_scaled_int64(DatumGetPointer(prm->value), 4, &scaled))
				return false;
			*out = scaled;
			return true;
		}
		case PG_VEC_SCALAR_DECIMAL128_S6:
		{
			int64 scaled;

			if (!numeric_varlena_to_scaled_int64(DatumGetPointer(prm->value), 6, &scaled))
				return false;
			*out = scaled;
			return true;
		}
		case PG_VEC_SCALAR_CHAR1:
		{
			text *txt = DatumGetTextPP(prm->value);

			if (VARSIZE_ANY_EXHDR(txt) < 1)
				return false;
			*out = static_cast<unsigned char>(*VARDATA_ANY(txt));
			return true;
		}
		case PG_VEC_SCALAR_STRING128:
		case PG_VEC_SCALAR_INVALID:
		default:
			return false;
	}
}

static bool
eval_expr_to_int128(const PgVecExprProgram &program,
					int node_idx,
					const EvalContext &ctx,
					__int128 *out)
{
	__int128	left_value;
	__int128	right_value;

	if (node_idx < 0 || node_idx >= program.nnodes)
		return false;

	const PgVecExprNode &expr = program.nodes[node_idx];

	switch (expr.kind)
	{
		case PG_VEC_EXPR_COLUMN:
			return column_value_to_int128(ctx, expr.column, out);

		case PG_VEC_EXPR_CONST:
			return const_value_to_int128(expr.constant, expr.scalar_kind, out);

		case PG_VEC_EXPR_EXTRACT_YEAR:
		{
			int year;
			int month;
			int day;

			if (!eval_expr_to_int128(program, expr.left, ctx, &left_value))
				return false;
			j2date(static_cast<int>(left_value) + POSTGRES_EPOCH_JDATE,
				   &year,
				   &month,
				   &day);
			*out = year;
			return true;
		}

		case PG_VEC_EXPR_ADD:
			if (!eval_expr_to_int128(program, expr.left, ctx, &left_value) ||
				!eval_expr_to_int128(program, expr.right, ctx, &right_value))
				return false;
			*out = left_value + right_value;
			return true;

		case PG_VEC_EXPR_SUB:
			if (!eval_expr_to_int128(program, expr.left, ctx, &left_value) ||
				!eval_expr_to_int128(program, expr.right, ctx, &right_value))
				return false;
			*out = left_value - right_value;
			return true;

		case PG_VEC_EXPR_MUL:
			if (!eval_expr_to_int128(program, expr.left, ctx, &left_value) ||
				!eval_expr_to_int128(program, expr.right, ctx, &right_value))
				return false;
			*out = left_value * right_value;
			return true;

		case PG_VEC_EXPR_INVALID:
		default:
			return false;
	}
}

struct StringEvalValue
{
	bool is_const;
	const PgVecStringConst *const_value;
	StringCursorValue ref_value;
	PgVecStringConst owned_value;
};

static bool
eval_expr_to_string_value(const PgVecExprProgram &program,
						  int node_idx,
						  const EvalContext &ctx,
						  StringEvalValue *out)
{
	if (node_idx < 0 || node_idx >= program.nnodes)
		return false;

	const PgVecExprNode &expr = program.nodes[node_idx];

	switch (expr.kind)
	{
		case PG_VEC_EXPR_COLUMN:
			if (expr.column.scalar_kind != PG_VEC_SCALAR_STRING128)
				return false;
			out->is_const = false;
			out->const_value = nullptr;
			return cursor_get_string_ref(ctx.inputs[expr.column.input_id],
										 expr.column.attno,
										 &out->ref_value);

		case PG_VEC_EXPR_CONST:
			if (expr.scalar_kind != PG_VEC_SCALAR_STRING128)
				return false;
			out->is_const = true;
			out->const_value = &expr.constant.string128;
			return true;

		case PG_VEC_EXPR_SUBSTRING_PREFIX2:
		{
			StringEvalValue base_value{};

			if (!eval_expr_to_string_value(program, expr.left, ctx, &base_value))
				return false;

			out->is_const = true;
			out->const_value = &out->owned_value;
			if (base_value.is_const)
			{
				out->owned_value.len =
					base_value.const_value->len < 2 ? base_value.const_value->len : 2;
				if (out->owned_value.len > 0)
					memcpy(out->owned_value.bytes,
						   base_value.const_value->bytes,
						   out->owned_value.len);
			}
			else
			{
				out->owned_value.len =
					base_value.ref_value.ref->len < 2 ? base_value.ref_value.ref->len : 2;
				for (uint16_t idx = 0; idx < out->owned_value.len; idx++)
					out->owned_value.bytes[idx] =
						string_ref_byte_at(*base_value.ref_value.ref,
										   base_value.ref_value.arena,
										   idx);
			}
			memset(out->owned_value.bytes + out->owned_value.len,
				   0,
				   PG_VEC_INLINE_STRING_MAX - out->owned_value.len);
			return true;
		}

		case PG_VEC_EXPR_EXTRACT_YEAR:
		case PG_VEC_EXPR_ADD:
		case PG_VEC_EXPR_SUB:
		case PG_VEC_EXPR_MUL:
		case PG_VEC_EXPR_INVALID:
		default:
			return false;
	}
}

static int
compare_string_eval_values(const StringEvalValue &lhs,
							 const StringEvalValue &rhs)
{
	if (lhs.is_const && rhs.is_const)
		return string_const_compare(*lhs.const_value, *rhs.const_value);
	if (!lhs.is_const && rhs.is_const)
		return string_ref_compare_const(*lhs.ref_value.ref,
										  lhs.ref_value.arena,
										  *rhs.const_value);
	if (lhs.is_const && !rhs.is_const)
		return -string_ref_compare_const(*rhs.ref_value.ref,
										   rhs.ref_value.arena,
										   *lhs.const_value);
	return string_ref_compare_ref(*lhs.ref_value.ref,
								   lhs.ref_value.arena,
								   *rhs.ref_value.ref,
								   rhs.ref_value.arena);
}

static bool
string_eval_starts_with(const StringEvalValue &value,
						  const StringEvalValue &prefix)
{
	if (!prefix.is_const)
		return false;
	if (value.is_const)
		return string_const_starts_with(*value.const_value, *prefix.const_value);
	return string_ref_starts_with_const(*value.ref_value.ref,
										 value.ref_value.arena,
										 *prefix.const_value);
}

static bool
string_eval_contains(const StringEvalValue &value,
						const StringEvalValue &needle)
{
	if (!needle.is_const)
		return false;
	if (value.is_const)
		return string_const_contains(*value.const_value, *needle.const_value);
	return string_ref_contains_const(*value.ref_value.ref,
									  value.ref_value.arena,
									  *needle.const_value);
}

static bool
eval_qual(const PgVecFilterSpec &filter,
		  int qual_idx,
		  const EvalContext &ctx)
	{
		StringEvalValue lhs_string{};
		StringEvalValue rhs_string{};

	if (qual_idx < 0 || qual_idx >= filter.nnodes)
		return false;

	const PgVecQualNode &qual = filter.nodes[qual_idx];

	switch (qual.kind)
	{
			case PG_VEC_QUAL_COMPARE:
				if (qual.op == PG_VEC_OP_PREFIX_LIKE)
				{
					if (!eval_expr_to_string_value(filter.exprs, qual.lhs_expr, ctx, &lhs_string) ||
						!eval_expr_to_string_value(filter.exprs, qual.rhs_expr, ctx, &rhs_string))
						return false;
					return string_eval_starts_with(lhs_string, rhs_string);
				}
				if (qual.op == PG_VEC_OP_CONTAINS_LIKE)
				{
					if (!eval_expr_to_string_value(filter.exprs, qual.lhs_expr, ctx, &lhs_string) ||
						!eval_expr_to_string_value(filter.exprs, qual.rhs_expr, ctx, &rhs_string))
						return false;
					return string_eval_contains(lhs_string, rhs_string);
				}
				if (qual.op == PG_VEC_OP_NOT_PREFIX_LIKE)
				{
					if (!eval_expr_to_string_value(filter.exprs, qual.lhs_expr, ctx, &lhs_string) ||
						!eval_expr_to_string_value(filter.exprs, qual.rhs_expr, ctx, &rhs_string))
						return false;
					return !string_eval_starts_with(lhs_string, rhs_string);
				}
				if (qual.op == PG_VEC_OP_NOT_CONTAINS_LIKE)
				{
					if (!eval_expr_to_string_value(filter.exprs, qual.lhs_expr, ctx, &lhs_string) ||
						!eval_expr_to_string_value(filter.exprs, qual.rhs_expr, ctx, &rhs_string))
						return false;
					return !string_eval_contains(lhs_string, rhs_string);
				}
				if (qual.op == PG_VEC_OP_SQL_LIKE || qual.op == PG_VEC_OP_NOT_SQL_LIKE)
				{
					PgVecStringConst lhs_tmp;
					bool matched;

					if (!eval_expr_to_string_value(filter.exprs, qual.lhs_expr, ctx, &lhs_string) ||
						!eval_expr_to_string_value(filter.exprs, qual.rhs_expr, ctx, &rhs_string) ||
						!rhs_string.is_const)
						return false;

					if (lhs_string.is_const)
						matched = payload_matches_like_pattern(lhs_string.const_value->bytes,
															   lhs_string.const_value->len,
															   rhs_string.const_value->bytes,
															   rhs_string.const_value->len);
					else if (string_ref_copy_to_const(*lhs_string.ref_value.ref,
													  lhs_string.ref_value.arena,
													  &lhs_tmp))
						matched = payload_matches_like_pattern(lhs_tmp.bytes,
															   lhs_tmp.len,
															   rhs_string.const_value->bytes,
															   rhs_string.const_value->len);
					else
						return false;

					return (qual.op == PG_VEC_OP_SQL_LIKE) ? matched : !matched;
				}

				if (filter.exprs.nodes[qual.lhs_expr].scalar_kind == PG_VEC_SCALAR_STRING128)
				{
					int cmp;

					if (!eval_expr_to_string_value(filter.exprs, qual.lhs_expr, ctx, &lhs_string) ||
						!eval_expr_to_string_value(filter.exprs, qual.rhs_expr, ctx, &rhs_string))
						return false;

					cmp = compare_string_eval_values(lhs_string, rhs_string);
					switch (qual.op)
					{
						case PG_VEC_OP_EQ:
							return cmp == 0;
						case PG_VEC_OP_NE:
							return cmp != 0;
						case PG_VEC_OP_LT:
							return cmp < 0;
						case PG_VEC_OP_LE:
							return cmp <= 0;
						case PG_VEC_OP_GT:
							return cmp > 0;
						case PG_VEC_OP_GE:
							return cmp >= 0;
						case PG_VEC_OP_PREFIX_LIKE:
						case PG_VEC_OP_CONTAINS_LIKE:
						case PG_VEC_OP_NOT_PREFIX_LIKE:
						case PG_VEC_OP_NOT_CONTAINS_LIKE:
						case PG_VEC_OP_SQL_LIKE:
						case PG_VEC_OP_NOT_SQL_LIKE:
						case PG_VEC_OP_INVALID:
						default:
							return false;
					}
				}

			return compare_numeric_expr_values(filter.exprs,
											 qual.lhs_expr,
											 qual.rhs_expr,
											 ctx,
											 qual.op);

		case PG_VEC_QUAL_AND:
			return eval_qual(filter, qual.left, ctx) &&
				eval_qual(filter, qual.right, ctx);

		case PG_VEC_QUAL_OR:
			return eval_qual(filter, qual.left, ctx) ||
				eval_qual(filter, qual.right, ctx);

		case PG_VEC_QUAL_INVALID:
		default:
			return false;
	}
}

static void
select_all_rows(ExecDataChunk &chunk)
{
	chunk.sel.count = chunk.count;
	for (std::uint16_t row = 0; row < chunk.count; row++)
		chunk.sel.row_ids[row] = row;
}

static bool
eval_bound_filter_clause_row(const BoundFilterClause &clause,
							 const ExecDataChunk &chunk,
							 std::uint16_t row)
{
	switch (clause.scalar_kind)
	{
		case PG_VEC_SCALAR_INT32:
			return compare_value(chunk.int32_values[clause.dst_col][row],
								 clause.constant.int32_value,
								 clause.op);
		case PG_VEC_SCALAR_DATE32:
			return compare_value(chunk.date32_values[clause.dst_col][row],
								 clause.constant.date32,
								 clause.op);
		case PG_VEC_SCALAR_DECIMAL64_S2:
			return compare_value(chunk.decimal64_values[clause.dst_col][row],
								 clause.constant.decimal64_s2,
								 clause.op);
		case PG_VEC_SCALAR_CHAR1:
			return compare_value(chunk.char1_values[clause.dst_col][row],
								 clause.constant.char1,
								 clause.op);
		case PG_VEC_SCALAR_STRING128:
			if (clause.op == PG_VEC_OP_PREFIX_LIKE)
				return string_ref_starts_with_const(chunk.string_refs[clause.dst_col][row],
													 &chunk.string_arenas[clause.dst_col],
													 clause.constant.string128);
			if (clause.op == PG_VEC_OP_CONTAINS_LIKE)
				return string_ref_contains_const(chunk.string_refs[clause.dst_col][row],
												  &chunk.string_arenas[clause.dst_col],
												  clause.constant.string128);
			if (clause.op == PG_VEC_OP_NOT_PREFIX_LIKE)
				return !string_ref_starts_with_const(chunk.string_refs[clause.dst_col][row],
													  &chunk.string_arenas[clause.dst_col],
													  clause.constant.string128);
			if (clause.op == PG_VEC_OP_NOT_CONTAINS_LIKE)
				return !string_ref_contains_const(chunk.string_refs[clause.dst_col][row],
												   &chunk.string_arenas[clause.dst_col],
												   clause.constant.string128);
			if (clause.op == PG_VEC_OP_SQL_LIKE)
				return string_ref_matches_like_const(chunk.string_refs[clause.dst_col][row],
													 &chunk.string_arenas[clause.dst_col],
													 clause.constant.string128);
			if (clause.op == PG_VEC_OP_NOT_SQL_LIKE)
				return !string_ref_matches_like_const(chunk.string_refs[clause.dst_col][row],
													  &chunk.string_arenas[clause.dst_col],
													  clause.constant.string128);
			return string_ref_compare_value(chunk.string_refs[clause.dst_col][row],
											 &chunk.string_arenas[clause.dst_col],
											 clause.constant.string128,
											 clause.op);
		case PG_VEC_SCALAR_DECIMAL128_S4:
		case PG_VEC_SCALAR_DECIMAL128_S6:
		case PG_VEC_SCALAR_INVALID:
		default:
			return false;
	}
}

static bool
eval_bound_filter_node(const BoundFilterProgram &program,
					   int node_idx,
					   uint64_t clause_bits)
{
	const DeformFilterNode &node = program.nodes[node_idx];

	switch (node.kind)
	{
		case DeformFilterNodeKind::kClause:
			return (clause_bits & (UINT64CONST(1) << node.clause_idx)) != 0;
		case DeformFilterNodeKind::kAnd:
			return eval_bound_filter_node(program, node.left, clause_bits) &&
				eval_bound_filter_node(program, node.right, clause_bits);
		case DeformFilterNodeKind::kOr:
			return eval_bound_filter_node(program, node.left, clause_bits) ||
				eval_bound_filter_node(program, node.right, clause_bits);
		case DeformFilterNodeKind::kInvalid:
		default:
			return false;
	}
}

static void
filter_chunk_with_bound_program(const BoundFilterProgram *bound_filter,
								  ExecDataChunk &chunk)
{
	chunk.sel.clear();

	if (bound_filter == nullptr || !bound_filter->valid || bound_filter->root < 0)
	{
		select_all_rows(chunk);
		return;
	}

	for (std::uint16_t row = 0; row < chunk.count; row++)
	{
		uint64_t clause_bits = 0;

		for (int clause_idx = 0; clause_idx < bound_filter->nclauses; clause_idx++)
		{
			if (eval_bound_filter_clause_row(bound_filter->clauses[clause_idx],
											 chunk,
											 row))
				clause_bits |= (UINT64CONST(1) << clause_idx);
		}

		if (eval_bound_filter_node(*bound_filter, bound_filter->root, clause_bits))
			chunk.sel.append(row);
	}
}

static void
filter_chunk(uint8 input_id,
			 const PgVecInputSpec *input,
			 const BoundFilterProgram *bound_filter,
			 ExecDataChunk &chunk)
{
	EvalContext eval_ctx{};

	if (input->filter.nnodes == 0)
	{
		select_all_rows(chunk);
		return;
	}

	if (bound_filter != nullptr && bound_filter->valid)
	{
		if (bound_filter->deform_safe && chunk.deform_filter_applied)
			select_all_rows(chunk);
		else
			filter_chunk_with_bound_program(bound_filter, chunk);
		return;
	}

	chunk.sel.clear();
	eval_ctx.inputs[input_id].spec = input;
	eval_ctx.inputs[input_id].chunk = &chunk;
	eval_ctx.inputs[input_id].materialized = nullptr;

	for (std::uint16_t row = 0; row < chunk.count; row++)
	{
		bool		match = true;

		eval_ctx.inputs[input_id].chunk_row = row;
		if (input->filter.nnodes > 0)
			match = eval_qual(input->filter, input->filter.root, eval_ctx);

		if (match)
			chunk.sel.append(row);
	}
}

static PgVecScalarKind
agg_input_scalar_kind(const PgVecAggCall &agg)
{
	if (agg.star_arg || agg.expr.root < 0 || agg.expr.root >= agg.expr.nnodes)
		return PG_VEC_SCALAR_INVALID;

	return agg.expr.nodes[agg.expr.root].scalar_kind;
}

static bool
agg_has_distinct(const PgVecAggSpec *agg_spec)
{
	for (int agg_idx = 0; agg_idx < agg_spec->naggs; agg_idx++)
	{
		if (agg_spec->aggs[agg_idx].distinct)
			return true;
	}

	return false;
}

static void
init_distinct_states(const PgVecAggSpec *agg_spec,
					 std::vector<DistinctAggState> *distinct_states)
{
	distinct_states->clear();
	distinct_states->resize(agg_spec->naggs);
	for (int agg_idx = 0; agg_idx < agg_spec->naggs; agg_idx++)
		(*distinct_states)[agg_idx].enabled = agg_spec->aggs[agg_idx].distinct;
}

static void
init_exec_row(const PgVecAggSpec *agg_spec, PgVecExecRow *row)
{
	std::memset(row, 0, sizeof(*row));

	for (int agg_idx = 0; agg_idx < agg_spec->naggs; agg_idx++)
	{
		PgVecAggExecState &state = row->aggs[agg_idx];

		state.scalar_kind = agg_input_scalar_kind(agg_spec->aggs[agg_idx]);
		state.isnull = (agg_spec->aggs[agg_idx].kind != PG_VEC_AGG_COUNT);
	}
}

static bool
agg_filter_matches(const PgVecAggCall &agg, const EvalContext &ctx)
{
	if (!agg.has_filter || agg.filter.nnodes == 0)
		return true;

	return eval_qual(agg.filter, agg.filter.root, ctx);
}

static void
advance_agg_state(const PgVecAggCall &agg,
				  const EvalContext &ctx,
				  PgVecAggExecState *state)
{
	__int128	expr_value = 0;

	if (!agg_filter_matches(agg, ctx))
		return;

	switch (agg.kind)
	{
		case PG_VEC_AGG_COUNT:
			if (!agg.star_arg)
			{
				if (agg.expr.root < 0)
					return;
				if (!eval_expr_to_int128(agg.expr, agg.expr.root, ctx, &expr_value))
					return;
			}
			state->count++;
			state->isnull = false;
			return;

		case PG_VEC_AGG_SUM:
		case PG_VEC_AGG_AVG:
		case PG_VEC_AGG_MIN:
		case PG_VEC_AGG_MAX:
			if (agg.star_arg || agg.expr.root < 0 ||
				!eval_expr_to_int128(agg.expr, agg.expr.root, ctx, &expr_value))
				elog(ERROR, "pg_vec: failed to evaluate aggregate expression");
			break;

		case PG_VEC_AGG_INVALID:
		default:
			elog(ERROR, "pg_vec: unsupported aggregate kind %d", (int) agg.kind);
	}

	switch (agg.kind)
	{
		case PG_VEC_AGG_SUM:
			if (state->isnull)
			{
				state->isnull = false;
				state->value = 0;
			}
			state->value += expr_value;
			return;

		case PG_VEC_AGG_AVG:
			if (state->isnull)
			{
				state->isnull = false;
				state->value = 0;
			}
			state->value += expr_value;
			state->count++;
			return;

		case PG_VEC_AGG_MIN:
			if (state->isnull || expr_value < state->value)
			{
				state->value = expr_value;
				state->isnull = false;
			}
			return;

		case PG_VEC_AGG_MAX:
			if (state->isnull || expr_value > state->value)
			{
				state->value = expr_value;
				state->isnull = false;
			}
			return;

		case PG_VEC_AGG_COUNT:
		case PG_VEC_AGG_INVALID:
		default:
			return;
	}
}

static void
accumulate_row_aggs(const PgVecScanFilterAggExecParams *params,
					const EvalContext &ctx,
					PgVecExecRow *agg_row,
					std::vector<DistinctAggState> *distinct_states,
					std::size_t group_pos,
					bool grouped)
{
	for (int agg_idx = 0; agg_idx < params->agg.naggs; agg_idx++)
	{
		const PgVecAggCall &agg_call = params->agg.aggs[agg_idx];

		if (agg_call.distinct)
		{
			__int128 expr_value = 0;
			DistinctAggState &distinct_state = (*distinct_states)[agg_idx];

			if (!agg_filter_matches(agg_call, ctx))
				continue;
			if (!eval_expr_to_int128(agg_call.expr,
									 agg_call.expr.root,
									 ctx,
									 &expr_value))
				continue;

			if (grouped)
			{
				if (group_pos >= distinct_state.grouped_values.size())
					distinct_state.grouped_values.resize(group_pos + 1);
				if (!distinct_state.grouped_values[group_pos]
						 .insert((std::int32_t) expr_value)
						 .second)
					continue;
			}
			else
			{
				if (!distinct_state.plain_values.insert((std::int32_t) expr_value).second)
					continue;
			}
		}

		advance_agg_state(agg_call, ctx, &agg_row->aggs[agg_idx]);
	}
}

static int
compare_group_key_values(const PgVecConstValue &lhs,
						 const PgVecConstValue &rhs,
						 PgVecScalarKind scalar_kind)
{
	switch (scalar_kind)
	{
		case PG_VEC_SCALAR_INT32:
			return (lhs.int32_value > rhs.int32_value) - (lhs.int32_value < rhs.int32_value);
		case PG_VEC_SCALAR_DATE32:
			return (lhs.date32 > rhs.date32) - (lhs.date32 < rhs.date32);
		case PG_VEC_SCALAR_DECIMAL64_S2:
			return (lhs.decimal64_s2 > rhs.decimal64_s2) -
				(lhs.decimal64_s2 < rhs.decimal64_s2);
		case PG_VEC_SCALAR_DECIMAL128_S2:
		case PG_VEC_SCALAR_DECIMAL128_S4:
		case PG_VEC_SCALAR_DECIMAL128_S6:
			return (lhs.decimal128 > rhs.decimal128) - (lhs.decimal128 < rhs.decimal128);
		case PG_VEC_SCALAR_CHAR1:
		{
			unsigned char lhs_char = static_cast<unsigned char>(lhs.char1);
			unsigned char rhs_char = static_cast<unsigned char>(rhs.char1);

			return (lhs_char > rhs_char) - (lhs_char < rhs_char);
		}
		case PG_VEC_SCALAR_STRING128:
			return compare_string(lhs.string128, rhs.string128);
		case PG_VEC_SCALAR_INVALID:
		default:
			elog(ERROR, "pg_vec: unsupported group key scalar kind %d",
				 (int) scalar_kind);
	}
}

static bool
group_row_less(const PgVecExecRow &lhs,
			   const PgVecExecRow &rhs,
			   const PgVecAggSpec &agg_spec)
{
	for (int key_idx = 0; key_idx < agg_spec.ngroup_keys; key_idx++)
	{
		int			cmp;

		cmp = compare_group_key_values(lhs.group_keys[key_idx],
									   rhs.group_keys[key_idx],
									   agg_spec.group_keys[key_idx].scalar_kind);
		if (cmp < 0)
			return true;
		if (cmp > 0)
			return false;
	}

	return false;
}

static int
compare_agg_sort_value(const PgVecAggCall &agg_call,
					   const PgVecAggExecState &lhs,
					   const PgVecAggExecState &rhs,
					   bool *lhs_isnull,
					   bool *rhs_isnull)
{
	if (agg_call.kind == PG_VEC_AGG_COUNT)
	{
		*lhs_isnull = false;
		*rhs_isnull = false;
		return (lhs.count > rhs.count) - (lhs.count < rhs.count);
	}

	if (agg_call.kind == PG_VEC_AGG_AVG)
		elog(ERROR, "pg_vec: top sort on AVG outputs is not supported yet");

	*lhs_isnull = lhs.isnull;
	*rhs_isnull = rhs.isnull;
	if (agg_call.zero_if_empty)
	{
		*lhs_isnull = false;
		*rhs_isnull = false;
	}

	if (*lhs_isnull || *rhs_isnull)
		return 0;

	return (lhs.value > rhs.value) - (lhs.value < rhs.value);
}

static int
compare_result_sort_key(const PgVecScanFilterAggExecParams *params,
						const PgVecSortKey &sort_key,
						const PgVecExecRow &lhs,
						const PgVecExecRow &rhs)
{
	const PgVecOutputExprProgram &program = params->agg.outputs[sort_key.output_idx];
	const PgVecOutputExprNode &root = program.nodes[program.root];
	int cmp = 0;
	bool lhs_isnull = false;
	bool rhs_isnull = false;

	switch (root.kind)
	{
		case PG_VEC_OUTPUT_EXPR_GROUP_KEY:
			cmp = compare_group_key_values(lhs.group_keys[root.index],
										  rhs.group_keys[root.index],
										  params->agg.group_keys[root.index].scalar_kind);
			break;

		case PG_VEC_OUTPUT_EXPR_AGGREF:
			cmp = compare_agg_sort_value(params->agg.aggs[root.index],
										 lhs.aggs[root.index],
										 rhs.aggs[root.index],
										 &lhs_isnull,
										 &rhs_isnull);
			if (lhs_isnull || rhs_isnull)
			{
				if (lhs_isnull && rhs_isnull)
					cmp = 0;
				else if (lhs_isnull)
					cmp = sort_key.nulls_first ? -1 : 1;
				else
					cmp = sort_key.nulls_first ? 1 : -1;
			}
			break;

		case PG_VEC_OUTPUT_EXPR_CONST:
			cmp = 0;
			break;

		case PG_VEC_OUTPUT_EXPR_ADD:
		case PG_VEC_OUTPUT_EXPR_SUB:
		case PG_VEC_OUTPUT_EXPR_MUL:
		case PG_VEC_OUTPUT_EXPR_DIV:
		case PG_VEC_OUTPUT_EXPR_INVALID:
		default:
			elog(ERROR, "pg_vec: unsupported top sort output expression kind %d",
				 (int) root.kind);
	}

	if (cmp == 0)
		return 0;
	return sort_key.descending ? -cmp : cmp;
}

static bool
result_row_less(const PgVecScanFilterAggExecParams *params,
				const PgVecExecRow &lhs,
				const PgVecExecRow &rhs)
{
	for (int sort_idx = 0; sort_idx < params->topn.nsortkeys; sort_idx++)
	{
		int cmp = compare_result_sort_key(params,
										 params->topn.sort_keys[sort_idx],
										 lhs,
										 rhs);

		if (cmp < 0)
			return true;
		if (cmp > 0)
			return false;
	}

	return false;
}

static bool
eval_post_agg_output_expr_to_int128(const PgVecScanFilterAggExecParams *params,
									const PgVecExecRow &row,
									const PgVecOutputExprProgram &program,
									int node_idx,
									__int128 *out,
									bool *isnull)
{
	const PgVecOutputExprNode *node;
	const PgVecAggCall *agg_call;
	const PgVecAggExecState *agg_state;
	__int128 left_value;
	__int128 right_value;
	bool left_null = false;
	bool right_null = false;

	if (node_idx < 0 || node_idx >= program.nnodes)
		return false;

	node = &program.nodes[node_idx];
	switch (node->kind)
	{
		case PG_VEC_OUTPUT_EXPR_GROUP_KEY:
			*isnull = false;
			return const_value_to_int128(row.group_keys[node->index],
										 params->agg.group_keys[node->index].scalar_kind,
										 out);

		case PG_VEC_OUTPUT_EXPR_AGGREF:
			agg_call = &params->agg.aggs[node->index];
			agg_state = &row.aggs[node->index];
			if (agg_call->kind == PG_VEC_AGG_COUNT)
			{
				*out = agg_state->count;
				*isnull = false;
				return true;
			}
			if (agg_call->kind == PG_VEC_AGG_AVG)
				return false;
			if (agg_state->isnull)
			{
				if (agg_call->zero_if_empty)
				{
					*out = 0;
					*isnull = false;
					return true;
				}
				*isnull = true;
				return true;
			}
			*out = agg_state->value;
			*isnull = false;
			return true;

		case PG_VEC_OUTPUT_EXPR_CONST:
			*isnull = false;
			return const_value_to_int128(node->constant, node->scalar_kind, out);

		case PG_VEC_OUTPUT_EXPR_PARAM:
			return fetch_param_exec_to_int128(params,
											  node->index,
											  node->scalar_kind,
											  out,
											  isnull);

		case PG_VEC_OUTPUT_EXPR_ADD:
		case PG_VEC_OUTPUT_EXPR_SUB:
			if (!eval_post_agg_output_expr_to_int128(params,
													 row,
													 program,
													 node->left,
													 &left_value,
													 &left_null) ||
				!eval_post_agg_output_expr_to_int128(params,
													 row,
													 program,
													 node->right,
													 &right_value,
													 &right_null))
				return false;
			if (left_null || right_null)
			{
				*isnull = true;
				return true;
			}
			*out = (node->kind == PG_VEC_OUTPUT_EXPR_ADD)
					   ? (left_value + right_value)
					   : (left_value - right_value);
			*isnull = false;
			return true;

		case PG_VEC_OUTPUT_EXPR_MUL:
		case PG_VEC_OUTPUT_EXPR_DIV:
		case PG_VEC_OUTPUT_EXPR_INVALID:
		default:
			return false;
	}
}

static bool
eval_post_agg_output_expr_to_string(const PgVecScanFilterAggExecParams *params,
									const PgVecExecRow &row,
									const PgVecOutputExprProgram &program,
									int node_idx,
									const PgVecStringConst **out,
									bool *isnull)
{
	const PgVecOutputExprNode *node;

	if (node_idx < 0 || node_idx >= program.nnodes)
		return false;

	node = &program.nodes[node_idx];
	switch (node->kind)
	{
		case PG_VEC_OUTPUT_EXPR_GROUP_KEY:
			if (params->agg.group_keys[node->index].scalar_kind != PG_VEC_SCALAR_STRING128)
				return false;
			*out = &row.group_keys[node->index].string128;
			*isnull = false;
			return true;

		case PG_VEC_OUTPUT_EXPR_CONST:
			if (node->scalar_kind != PG_VEC_SCALAR_STRING128)
				return false;
			*out = &node->constant.string128;
			*isnull = false;
			return true;

		case PG_VEC_OUTPUT_EXPR_PARAM:
		{
			static PgVecStringConst param_string;
			ParamExecData *prm;
			text *txt;

			if (params == nullptr || params->estate == nullptr ||
				node->index < 0 ||
				node->index >= list_length(params->estate->es_plannedstmt->paramExecTypes))
				return false;

			prm = &params->estate->es_param_exec_vals[node->index];
			if (prm->execPlan != nullptr)
			{
				ExprContext *econtext = GetPerTupleExprContext(params->estate);
				ExecSetParamPlan((SubPlanState *) prm->execPlan, econtext);
			}
			if (prm->isnull)
			{
				*isnull = true;
				return true;
			}
			txt = DatumGetTextPP(prm->value);
			param_string.len =
				Min((int) VARSIZE_ANY_EXHDR(txt), PG_VEC_INLINE_STRING_MAX - 1);
			memcpy(param_string.bytes, VARDATA_ANY(txt), param_string.len);
			memset(param_string.bytes + param_string.len,
				   0,
				   PG_VEC_INLINE_STRING_MAX - param_string.len);
			*out = &param_string;
			*isnull = false;
			return true;
		}

		case PG_VEC_OUTPUT_EXPR_AGGREF:
		case PG_VEC_OUTPUT_EXPR_ADD:
		case PG_VEC_OUTPUT_EXPR_SUB:
		case PG_VEC_OUTPUT_EXPR_MUL:
		case PG_VEC_OUTPUT_EXPR_DIV:
		case PG_VEC_OUTPUT_EXPR_INVALID:
		default:
			return false;
	}
}

static bool
compare_post_agg_values(PgVecFilterOp op, int cmp)
{
	switch (op)
	{
		case PG_VEC_OP_EQ:
			return cmp == 0;
		case PG_VEC_OP_NE:
			return cmp != 0;
		case PG_VEC_OP_LT:
			return cmp < 0;
		case PG_VEC_OP_LE:
			return cmp <= 0;
		case PG_VEC_OP_GT:
			return cmp > 0;
		case PG_VEC_OP_GE:
			return cmp >= 0;
		case PG_VEC_OP_PREFIX_LIKE:
		case PG_VEC_OP_CONTAINS_LIKE:
		case PG_VEC_OP_NOT_PREFIX_LIKE:
		case PG_VEC_OP_NOT_CONTAINS_LIKE:
		case PG_VEC_OP_SQL_LIKE:
		case PG_VEC_OP_NOT_SQL_LIKE:
		case PG_VEC_OP_INVALID:
		default:
			return false;
	}
}

static int
numeric_scalar_scale(PgVecScalarKind scalar_kind)
{
	switch (scalar_kind)
	{
		case PG_VEC_SCALAR_DECIMAL64_S2:
		case PG_VEC_SCALAR_DECIMAL128_S2:
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

static bool
scalar_kinds_numeric_compatible(PgVecScalarKind left_kind,
								  PgVecScalarKind right_kind)
{
	return numeric_scalar_scale(left_kind) >= 0 &&
		   numeric_scalar_scale(right_kind) >= 0;
}

static bool
eval_post_agg_qual_node(const PgVecScanFilterAggExecParams *params,
						const PgVecExecRow &row,
						const PgVecPostAggFilterSpec &filter,
						int node_idx)
{
	const PgVecQualNode &node = filter.nodes[node_idx];

	switch (node.kind)
	{
		case PG_VEC_QUAL_COMPARE:
		{
			PgVecScalarKind left_kind = filter.exprs.nodes[node.lhs_expr].scalar_kind;
			PgVecScalarKind right_kind = filter.exprs.nodes[node.rhs_expr].scalar_kind;

			if (left_kind == PG_VEC_SCALAR_STRING128 ||
				right_kind == PG_VEC_SCALAR_STRING128)
			{
				const PgVecStringConst *lhs_string;
				const PgVecStringConst *rhs_string;
				bool lhs_null = false;
				bool rhs_null = false;

				if (!eval_post_agg_output_expr_to_string(params,
														 row,
														 filter.exprs,
														 node.lhs_expr,
														 &lhs_string,
														 &lhs_null) ||
					!eval_post_agg_output_expr_to_string(params,
														 row,
														 filter.exprs,
														 node.rhs_expr,
														 &rhs_string,
														 &rhs_null))
					return false;
				if (lhs_null || rhs_null)
					return false;
				return compare_post_agg_values(node.op,
											 compare_string(*lhs_string, *rhs_string));
			}
			else
			{
				__int128 lhs_value;
				__int128 rhs_value;
				bool lhs_null = false;
				bool rhs_null = false;
				int left_scale;
				int right_scale;
				int scale_delta;
				__int128 scale_factor = 1;
				int cmp;

				if (!eval_post_agg_output_expr_to_int128(params,
														 row,
														 filter.exprs,
														 node.lhs_expr,
														 &lhs_value,
														 &lhs_null) ||
					!eval_post_agg_output_expr_to_int128(params,
														 row,
														 filter.exprs,
														 node.rhs_expr,
														 &rhs_value,
														 &rhs_null))
					return false;
				if (lhs_null || rhs_null)
					return false;
				left_scale = numeric_scalar_scale(left_kind);
				right_scale = numeric_scalar_scale(right_kind);
				if (left_kind != right_kind)
				{
					if (!scalar_kinds_numeric_compatible(left_kind, right_kind))
						return false;
					if (left_scale < right_scale)
					{
						scale_delta = right_scale - left_scale;
						for (int i = 0; i < scale_delta; i++)
							scale_factor *= 10;
						lhs_value *= scale_factor;
					}
					else if (right_scale < left_scale)
					{
						scale_delta = left_scale - right_scale;
						for (int i = 0; i < scale_delta; i++)
							scale_factor *= 10;
						rhs_value *= scale_factor;
					}
				}
				cmp = (lhs_value > rhs_value) - (lhs_value < rhs_value);
				return compare_post_agg_values(node.op, cmp);
			}
		}

		case PG_VEC_QUAL_AND:
			return eval_post_agg_qual_node(params, row, filter, node.left) &&
				   eval_post_agg_qual_node(params, row, filter, node.right);

		case PG_VEC_QUAL_OR:
			return eval_post_agg_qual_node(params, row, filter, node.left) ||
				   eval_post_agg_qual_node(params, row, filter, node.right);

		case PG_VEC_QUAL_INVALID:
		default:
			return false;
	}
}

static void
apply_post_agg_filter(const PgVecScanFilterAggExecParams *params,
					  std::vector<PgVecExecRow> *rows)
{
	const PgVecPostAggFilterSpec &filter = params->agg.having;
	std::vector<PgVecExecRow> filtered_rows;

	if (filter.root < 0 || filter.nnodes == 0)
		return;

	filtered_rows.reserve(rows->size());
	for (const PgVecExecRow &row : *rows)
	{
		if (eval_post_agg_qual_node(params, row, filter, filter.root))
			filtered_rows.push_back(row);
	}

	rows->swap(filtered_rows);
}

static bool
eval_expr_to_const(const PgVecExprProgram &expr,
				   int node_idx,
				   const EvalContext &ctx,
				   PgVecConstValue *out)
{
	PgVecScalarKind scalar_kind;
	__int128 value;
	StringEvalValue string_value{};

	if (node_idx < 0 || node_idx >= expr.nnodes)
		return false;

	scalar_kind = expr.nodes[node_idx].scalar_kind;
	switch (scalar_kind)
	{
		case PG_VEC_SCALAR_INT32:
			if (!eval_expr_to_int128(expr, node_idx, ctx, &value))
				return false;
			out->int32_value = static_cast<std::int32_t>(value);
			return true;
		case PG_VEC_SCALAR_DATE32:
			if (!eval_expr_to_int128(expr, node_idx, ctx, &value))
				return false;
			out->date32 = static_cast<DateADT>(value);
			return true;
		case PG_VEC_SCALAR_DECIMAL64_S2:
			if (!eval_expr_to_int128(expr, node_idx, ctx, &value))
				return false;
			out->decimal64_s2 = static_cast<std::int64_t>(value);
			return true;
		case PG_VEC_SCALAR_DECIMAL128_S2:
		case PG_VEC_SCALAR_DECIMAL128_S4:
		case PG_VEC_SCALAR_DECIMAL128_S6:
			if (!eval_expr_to_int128(expr, node_idx, ctx, &value))
				return false;
			out->decimal128 = value;
			return true;
		case PG_VEC_SCALAR_CHAR1:
			if (!eval_expr_to_int128(expr, node_idx, ctx, &value))
				return false;
			out->char1 = static_cast<char>(value);
			return true;
		case PG_VEC_SCALAR_STRING128:
			if (!eval_expr_to_string_value(expr, node_idx, ctx, &string_value))
				return false;
			if (string_value.is_const)
			{
				out->string128 = *string_value.const_value;
				return true;
			}
			return string_ref_copy_to_const(*string_value.ref_value.ref,
											string_value.ref_value.arena,
											&out->string128);
		case PG_VEC_SCALAR_INVALID:
		default:
			return false;
	}
}

static bool
build_group_key(const PgVecAggSpec &agg_spec,
				const EvalContext &ctx,
				GroupKey *group_key,
				PgVecConstValue *typed_values)
{
	group_key->nkeys = agg_spec.ngroup_keys;

	for (int key_idx = 0; key_idx < agg_spec.ngroup_keys; key_idx++)
	{
		group_key->kinds[key_idx] = agg_spec.group_keys[key_idx].scalar_kind;
		if (!eval_expr_to_const(agg_spec.group_keys[key_idx].expr,
								agg_spec.group_keys[key_idx].expr.root,
								ctx,
								&group_key->values[key_idx]))
			return false;
		typed_values[key_idx] = group_key->values[key_idx];
	}

	return true;
}

static void
accumulate_grouped_eval(const PgVecScanFilterAggExecParams *params,
						const EvalContext &ctx,
						std::vector<PgVecExecRow> *groups,
						std::unordered_map<GroupKey, std::size_t, GroupKeyHash> *group_index,
						std::vector<DistinctAggState> *distinct_states)
{
	GroupKey	group_key{};
	PgVecConstValue typed_keys[PG_VEC_MAX_GROUP_KEYS];
	std::size_t	group_pos;
	auto		it = group_index->end();
	bool		inserted = false;

	if (!build_group_key(params->agg, ctx, &group_key, typed_keys))
		elog(ERROR, "pg_vec: failed to decode grouped key");

	it = group_index->find(group_key);
	if (it == group_index->end())
	{
		PgVecExecRow	group_row;

		init_exec_row(&params->agg, &group_row);
		for (int key_idx = 0; key_idx < params->agg.ngroup_keys; key_idx++)
			group_row.group_keys[key_idx] = typed_keys[key_idx];

		group_pos = groups->size();
		groups->push_back(group_row);
		group_index->emplace(group_key, group_pos);
		inserted = true;
	}

	if (!inserted)
		group_pos = it->second;

	accumulate_row_aggs(params,
						ctx,
						&(*groups)[group_pos],
						distinct_states,
						group_pos,
						true);
}

static void
materialize_result_rows(const std::vector<PgVecExecRow> &source_rows,
						PgVecScanFilterAggExecResult *result)
{
	if (source_rows.empty())
	{
		result->nrows = 0;
		result->rows = NULL;
		return;
	}

	result->nrows = (int) source_rows.size();
	result->rows = (PgVecExecRow *) palloc0(sizeof(PgVecExecRow) * source_rows.size());
	for (std::size_t row_idx = 0; row_idx < source_rows.size(); row_idx++)
		result->rows[row_idx] = source_rows[row_idx];
}

static bool
eval_output_program_to_const(const PgVecScanFilterAggExecParams *params,
							 const PgVecExecRow &row,
							 const PgVecOutputExprProgram &program,
							 PgVecConstValue *out)
{
	PgVecScalarKind scalar_kind;
	bool isnull = false;

	if (params == nullptr)
		return false;
	if (program.root < 0 || program.root >= program.nnodes)
		return false;

	scalar_kind = program.nodes[program.root].scalar_kind;
	switch (scalar_kind)
	{
		case PG_VEC_SCALAR_INT32:
		{
			__int128 value = 0;

			if (!eval_post_agg_output_expr_to_int128(params,
													 row,
													 program,
													 program.root,
													 &value,
													 &isnull) ||
				isnull)
				return false;
			out->int32_value = static_cast<std::int32_t>(value);
			return true;
		}
		case PG_VEC_SCALAR_DATE32:
		{
			__int128 value = 0;

			if (!eval_post_agg_output_expr_to_int128(params,
													 row,
													 program,
													 program.root,
													 &value,
													 &isnull) ||
				isnull)
				return false;
			out->date32 = static_cast<DateADT>(value);
			return true;
		}
		case PG_VEC_SCALAR_DECIMAL64_S2:
		case PG_VEC_SCALAR_DECIMAL128_S2:
		{
			__int128 value = 0;

			if (!eval_post_agg_output_expr_to_int128(params,
													 row,
													 program,
													 program.root,
													 &value,
													 &isnull) ||
				isnull)
				return false;
			out->decimal64_s2 = static_cast<std::int64_t>(value);
			return true;
		}
		case PG_VEC_SCALAR_DECIMAL128_S4:
		case PG_VEC_SCALAR_DECIMAL128_S6:
		{
			__int128 value = 0;

			if (!eval_post_agg_output_expr_to_int128(params,
													 row,
													 program,
													 program.root,
													 &value,
													 &isnull) ||
				isnull)
				return false;
			out->decimal128 = value;
			return true;
		}
		case PG_VEC_SCALAR_CHAR1:
		{
			__int128 value = 0;

			if (!eval_post_agg_output_expr_to_int128(params,
													 row,
													 program,
													 program.root,
													 &value,
													 &isnull) ||
				isnull)
				return false;
			out->char1 = static_cast<char>(value);
			return true;
		}
		case PG_VEC_SCALAR_STRING128:
		{
			const PgVecStringConst *value = nullptr;

			if (!eval_post_agg_output_expr_to_string(params,
													 row,
													 program,
													 program.root,
													 &value,
													 &isnull) ||
				isnull || value == nullptr)
				return false;
			*out = {};
			out->string128 = *value;
			return true;
		}
		case PG_VEC_SCALAR_INVALID:
		default:
			return false;
	}
}

static void
append_derived_materialized_row(const PgVecScanFilterAggExecParams *params,
								const PgVecInputSpec *input,
								const PgVecExecRow &source_row,
								MaterializedInput *materialized)
{
	ItemPointerData tid;

	ItemPointerSetInvalid(&tid);
	for (int col_idx = 0; col_idx < input->ncolumns; col_idx++)
	{
		int output_slot = input->columns[col_idx].attno - 1;
		int output_idx;
		PgVecConstValue value{};

		if (output_slot < 0 || output_slot >= PG_VEC_MAX_OUTPUT_COLUMNS)
			elog(ERROR, "pg_vec: invalid derived output slot %d", output_slot);
		output_idx = input->derived.output_map[output_slot];
		if (output_idx < 0 || output_idx >= input->derived.agg.noutputs)
			elog(ERROR, "pg_vec: invalid derived output mapping slot=%d idx=%d",
				 output_slot,
				 output_idx);
		if (!eval_output_program_to_const(params,
										 source_row,
										 input->derived.agg.outputs[output_idx],
										 &value))
			elog(ERROR, "pg_vec: failed to evaluate derived output %d",
				 output_idx);

		switch (input->columns[col_idx].scalar_kind)
		{
			case PG_VEC_SCALAR_INT32:
				materialized->int32_columns[col_idx].push_back(value.int32_value);
				break;
			case PG_VEC_SCALAR_DATE32:
				materialized->date32_columns[col_idx].push_back(value.date32);
				break;
			case PG_VEC_SCALAR_DECIMAL64_S2:
				materialized->decimal64_columns[col_idx].push_back(value.decimal64_s2);
				break;
			case PG_VEC_SCALAR_DECIMAL128_S2:
			case PG_VEC_SCALAR_DECIMAL128_S4:
			case PG_VEC_SCALAR_DECIMAL128_S6:
				materialized->decimal128_columns[col_idx].push_back(value.decimal128);
				break;
			case PG_VEC_SCALAR_CHAR1:
				materialized->char1_columns[col_idx].push_back(value.char1);
				break;
			case PG_VEC_SCALAR_STRING128:
			{
				PgVecStringRef ref;

				if (!copy_string_const_into_arena(value.string128,
												  &materialized->string_arenas[col_idx],
												  &ref))
					elog(ERROR, "pg_vec: failed to copy derived string output");
				materialized->string_columns[col_idx].push_back(ref);
				break;
			}
			case PG_VEC_SCALAR_INVALID:
			default:
				elog(ERROR, "pg_vec: unsupported derived output scalar kind %d",
					 (int) input->columns[col_idx].scalar_kind);
		}
	}
	materialized->tids.push_back(tid);
	materialized->row_count++;
}

static void
materialize_derived_input(uint8 input_id,
						  const PgVecScanFilterAggExecParams *params,
						  MaterializedInput *materialized,
						  PgVecScanFilterAggExecResult *result)
{
	const PgVecInputSpec *input = &params->inputs[input_id];
	PgVecScanFilterAggExecParams *derived_params;
	PgVecScanFilterAggExecResult derived_result{};
	auto init_exec_params_from_plan =
		[&](const PgVecPlan *plan_spec,
			PgVecScanFilterAggExecParams *dst) -> void
	{
		dst->ninputs = plan_spec->ninputs;
		dst->snapshot = params->snapshot;
		dst->estate = params->estate;
		dst->enable_jit_deform = params->enable_jit_deform;
		memcpy(dst->inputs,
			   plan_spec->inputs,
			   sizeof(PgVecInputSpec) * plan_spec->ninputs);
		dst->njoins = plan_spec->njoins;
		memcpy(dst->joins,
			   plan_spec->joins,
			   sizeof(PgVecJoinSpec) * plan_spec->njoins);
		dst->agg = plan_spec->agg;
		dst->topn = plan_spec->topn;
		for (int rel_idx = 0; rel_idx < plan_spec->ninputs; rel_idx++)
		{
			if (plan_spec->inputs[rel_idx].kind == PG_VEC_INPUT_RELATION)
				dst->rels[rel_idx] = table_open(plan_spec->inputs[rel_idx].relid, NoLock);
		}
	};
	auto close_exec_params_rels =
		[](PgVecScanFilterAggExecParams *dst) -> void
	{
		for (int rel_idx = 0; rel_idx < dst->ninputs; rel_idx++)
		{
			if (dst->rels[rel_idx] != nullptr)
				table_close(dst->rels[rel_idx], NoLock);
		}
	};

	derived_params = reinterpret_cast<PgVecScanFilterAggExecParams *>(
		palloc0(sizeof(PgVecScanFilterAggExecParams)));
	if (input->derived.subplan != nullptr)
	{
		init_exec_params_from_plan(input->derived.subplan, derived_params);
	}
	else
	{
		Relation base_rel = table_open(input->derived.relid, NoLock);

		derived_params->ninputs = 1;
		derived_params->rels[0] = base_rel;
		derived_params->snapshot = params->snapshot;
		derived_params->estate = params->estate;
		derived_params->enable_jit_deform = params->enable_jit_deform;
		derived_params->inputs[0].kind = PG_VEC_INPUT_RELATION;
		derived_params->inputs[0].relid = input->derived.relid;
		derived_params->inputs[0].ncolumns = input->derived.nbase_columns;
		memcpy(derived_params->inputs[0].columns,
			   input->derived.base_columns,
			   sizeof(PgVecColumnRef) * input->derived.nbase_columns);
		derived_params->inputs[0].filter = input->derived.base_filter;
		derived_params->njoins = 0;
		derived_params->agg = input->derived.agg;
	}
	pg_vec_execute_scan_filter_agg_datachunk(derived_params, &derived_result);
	close_exec_params_rels(derived_params);

	materialized->reserve_rows(derived_result.nrows);
	for (int row_idx = 0; row_idx < derived_result.nrows; row_idx++)
		append_derived_materialized_row(derived_params,
										input,
										derived_result.rows[row_idx],
										materialized);
	pfree(derived_params);

	result->rows_scanned += derived_result.rows_scanned;
	result->rows_selected += derived_result.rows_selected;
	result->chunks_scanned += derived_result.chunks_scanned;
}

static bool
extract_join_key(const EvalContext &ctx,
				 const PgVecJoinSpec &join,
				 bool use_left_side,
				 JoinKey *key)
{
	key->nkeys = join.nkeys;
	for (int key_idx = 0; key_idx < join.nkeys; key_idx++)
	{
		const PgVecColumnRef &column =
			use_left_side ? join.keys[key_idx].left : join.keys[key_idx].right;
		__int128	value;
		const ExecInputCursor &cursor = ctx.inputs[column.input_id];

		if (!column_value_to_int128(ctx, column, &value))
		{
			StringInfoData spec_desc;

			initStringInfo(&spec_desc);
			if (cursor.spec == nullptr)
			{
				appendStringInfoString(&spec_desc, "<null>");
			}
			else
			{
				appendStringInfo(&spec_desc,
								 "ncolumns=%d [",
								 cursor.spec->ncolumns);
				for (int i = 0; i < cursor.spec->ncolumns; i++)
				{
					if (i > 0)
						appendStringInfoString(&spec_desc, ", ");
					appendStringInfo(&spec_desc,
									 "%d:%d",
									 (int) cursor.spec->columns[i].attno,
									 (int) cursor.spec->columns[i].scalar_kind);
				}
				appendStringInfoChar(&spec_desc, ']');
			}

			elog(ERROR,
				 "pg_vec: failed to extract join %s key %d: input_id=%d attno=%d kind=%d spec=%s chunk=%p materialized=%p",
				 use_left_side ? "probe" : "build",
				 key_idx,
				 (int) column.input_id,
				 (int) column.attno,
				 (int) column.scalar_kind,
				 spec_desc.data,
				 cursor.chunk,
				 cursor.materialized);
		}
		key->values[key_idx] = (std::int32_t) value;
	}

	return true;
}

static void
materialize_filtered_input(uint8 input_id,
							const PgVecScanFilterAggExecParams *params,
							MaterializedInput *materialized,
							PgVecScanFilterAggExecResult *result,
							uint32_t late_mask = 0)
{
	if (params->inputs[input_id].kind == PG_VEC_INPUT_DERIVED_GROUPED_AGG)
	{
		if (late_mask != 0)
			elog(ERROR, "pg_vec: late materialization is not supported for derived inputs");
		materialize_derived_input(input_id, params, materialized, result);
		return;
	}

	BoundFilterProgram bound_filter = build_bound_input_filter(&params->inputs[input_id],
																  input_id);
	HeapDataChunkScanner scanner(params->rels[input_id],
								 params->snapshot,
								 &params->inputs[input_id],
								 &bound_filter,
								 late_mask,
								 params->enable_jit_deform);
	ExecDataChunk *chunk = allocate_exec_chunk(&params->inputs[input_id]);

	if (params->inputs[input_id].filter.nnodes == 0 &&
		params->rels[input_id] != NULL &&
		params->rels[input_id]->rd_rel != NULL &&
		params->rels[input_id]->rd_rel->reltuples > 0)
	{
		std::size_t reserve_rows =
			static_cast<std::size_t>(params->rels[input_id]->rd_rel->reltuples);

		materialized->reserve_rows(reserve_rows);
	}

	while (scanner.next_chunk(*chunk))
	{
		filter_chunk(input_id, &params->inputs[input_id], &bound_filter, *chunk);
		result->rows_scanned += chunk->count;
		result->rows_selected += chunk->sel.count;
		for (std::uint16_t i = 0; i < chunk->sel.count; i++)
			materialized->append_row(*chunk, (*chunk).sel[i]);
		result->chunks_scanned++;
	}
}

static bool
join_supports_single_int32_fast_path(const PgVecJoinSpec &join)
{
	return (join.kind == PG_VEC_JOIN_INNER ||
			join.kind == PG_VEC_JOIN_LEFT ||
			join.kind == PG_VEC_JOIN_SEMI ||
			join.kind == PG_VEC_JOIN_ANTI) &&
		join.nkeys == 1 &&
		join.keys[0].left.scalar_kind == PG_VEC_SCALAR_INT32 &&
		join.keys[0].right.scalar_kind == PG_VEC_SCALAR_INT32;
}

static void
materialize_filtered_input_with_int32_hash(uint8 input_id,
											 AttrNumber key_attno,
											 const PgVecScanFilterAggExecParams *params,
											 MaterializedInput *materialized,
											 Int32JoinMatchTable *right_hash,
											 PgVecScanFilterAggExecResult *result,
											 uint32_t late_mask = 0)
{
	if (params->inputs[input_id].kind == PG_VEC_INPUT_DERIVED_GROUPED_AGG)
	{
		int key_col_idx;

		if (late_mask != 0)
			elog(ERROR, "pg_vec: late materialization is not supported for derived inputs");
		materialize_derived_input(input_id, params, materialized, result);
		key_col_idx = scan_column_index(&params->inputs[input_id], key_attno);
		if (key_col_idx < 0)
			elog(ERROR, "pg_vec: failed to locate derived int32 join key attno %d in input %d",
				 (int) key_attno,
				 (int) input_id);
		right_hash->reserve(materialized->row_count);
		for (std::size_t row_idx = 0; row_idx < materialized->row_count; row_idx++)
		{
			std::int32_t key = materialized->int32_columns[key_col_idx][row_idx];

			(*right_hash)[key].push_back(row_idx);
		}
		return;
	}

	BoundFilterProgram bound_filter = build_bound_input_filter(&params->inputs[input_id],
																  input_id);
	HeapDataChunkScanner scanner(params->rels[input_id],
								 params->snapshot,
								 &params->inputs[input_id],
								 &bound_filter,
								 late_mask,
								 params->enable_jit_deform);
	ExecDataChunk *chunk = allocate_exec_chunk(&params->inputs[input_id]);
	int key_col_idx = scan_column_index(&params->inputs[input_id], key_attno);

	if (key_col_idx < 0)
		elog(ERROR, "pg_vec: failed to locate int32 join key attno %d in input %d",
			 (int) key_attno,
			 (int) input_id);

	if (params->inputs[input_id].filter.nnodes == 0 &&
		params->rels[input_id] != NULL &&
		params->rels[input_id]->rd_rel != NULL &&
		params->rels[input_id]->rd_rel->reltuples > 0)
	{
		std::size_t reserve_rows =
			static_cast<std::size_t>(params->rels[input_id]->rd_rel->reltuples);

		materialized->reserve_rows(reserve_rows);
		right_hash->reserve(reserve_rows);
	}

	while (scanner.next_chunk(*chunk))
	{
		filter_chunk(input_id, &params->inputs[input_id], &bound_filter, *chunk);
		result->rows_scanned += chunk->count;
		result->rows_selected += chunk->sel.count;
		for (std::uint16_t i = 0; i < chunk->sel.count; i++)
		{
			std::uint16_t row = (*chunk).sel[i];
			std::size_t row_idx = materialized->row_count;
			std::int32_t key = chunk->int32_values[key_col_idx][row];

			materialized->append_row(*chunk, row);
			(*right_hash)[key].push_back(row_idx);
		}
		result->chunks_scanned++;
	}
}

static void
accumulate_single_input(const PgVecScanFilterAggExecParams *params,
						uint8 input_id,
						PgVecExecRow *plain_row,
						std::vector<PgVecExecRow> *groups,
						std::unordered_map<GroupKey, std::size_t, GroupKeyHash> *group_index,
						std::vector<DistinctAggState> *distinct_states,
						PgVecScanFilterAggExecResult *result)
{
	if (params->inputs[input_id].kind == PG_VEC_INPUT_DERIVED_GROUPED_AGG)
	{
		MaterializedInput materialized(&params->inputs[input_id], 0);
		EvalContext eval_ctx{};

		materialize_derived_input(input_id, params, &materialized, result);
		cursor_bind_materialized(&eval_ctx.inputs[input_id],
								 &params->inputs[input_id],
								 params->rels[input_id],
								 params->snapshot,
								 0,
								 &materialized);
		for (std::size_t row_idx = 0; row_idx < materialized.row_count; row_idx++)
		{
			cursor_set_materialized_row(&eval_ctx.inputs[input_id], row_idx);
			if (params->agg.grouped)
				accumulate_grouped_eval(params, eval_ctx, groups, group_index, distinct_states);
			else
				accumulate_row_aggs(params, eval_ctx, plain_row, distinct_states, 0, false);
		}
		cleanup_eval_context(&eval_ctx, params->ninputs);
		return;
	}

	BoundFilterProgram bound_filter = build_bound_input_filter(&params->inputs[input_id],
																  input_id);
	HeapDataChunkScanner scanner(params->rels[input_id],
								 params->snapshot,
								 &params->inputs[input_id],
								 &bound_filter,
								 0,
								 params->enable_jit_deform);
	ExecDataChunk *chunk = allocate_exec_chunk(&params->inputs[input_id]);
	EvalContext eval_ctx{};

	eval_ctx.inputs[input_id].spec = &params->inputs[input_id];
	eval_ctx.inputs[input_id].chunk = chunk;
	eval_ctx.inputs[input_id].materialized = nullptr;

	while (scanner.next_chunk(*chunk))
	{
		filter_chunk(input_id, &params->inputs[input_id], &bound_filter, *chunk);
		result->rows_scanned += chunk->count;
		result->rows_selected += chunk->sel.count;
		for (std::uint16_t i = 0; i < chunk->sel.count; i++)
		{
			eval_ctx.inputs[input_id].chunk_row = (*chunk).sel[i];
			if (params->agg.grouped)
				accumulate_grouped_eval(params, eval_ctx, groups, group_index, distinct_states);
			else
				accumulate_row_aggs(params, eval_ctx, plain_row, distinct_states, 0, false);
		}
		result->chunks_scanned++;
	}
}

static void
probe_join_chain(const PgVecScanFilterAggExecParams *params,
				 int join_idx,
				 const std::vector<MaterializedInput> &inputs,
				 const std::vector<JoinMatchTable> &right_hashes,
				 EvalContext *eval_ctx,
				 PgVecExecRow *plain_row,
				 std::vector<PgVecExecRow> *groups,
				 std::unordered_map<GroupKey, std::size_t, GroupKeyHash> *group_index,
				 std::vector<DistinctAggState> *distinct_states)
{
	if (join_idx >= params->njoins)
	{
		if (params->agg.grouped)
			accumulate_grouped_eval(params, *eval_ctx, groups, group_index, distinct_states);
		else
			accumulate_row_aggs(params, *eval_ctx, plain_row, distinct_states, 0, false);
		return;
	}

	{
		const PgVecJoinSpec &join = params->joins[join_idx];
		JoinKey key{};
		auto it = right_hashes[join_idx].end();

		if (!extract_join_key(*eval_ctx, join, true, &key))
			elog(ERROR, "pg_vec: failed to extract join probe key");

		it = right_hashes[join_idx].find(key);
		if (it == right_hashes[join_idx].end())
		{
			if (join.kind == PG_VEC_JOIN_ANTI || join.kind == PG_VEC_JOIN_LEFT)
			{
				cursor_set_null_row(&eval_ctx->inputs[join.right_input]);
				probe_join_chain(params,
								 join_idx + 1,
								 inputs,
								 right_hashes,
								 eval_ctx,
								 plain_row,
								 groups,
								 group_index,
								 distinct_states);
			}
			return;
		}

		bool anti_matched = false;
		bool left_matched = false;

		for (std::size_t right_row_idx : it->second)
		{
			bool matched = true;

			cursor_bind_materialized(&eval_ctx->inputs[join.right_input],
									 &params->inputs[join.right_input],
									 params->rels[join.right_input],
									 params->snapshot,
									 inputs[join.right_input].late_mask,
									 &inputs[join.right_input]);
			cursor_set_materialized_row(&eval_ctx->inputs[join.right_input],
										right_row_idx);

			if (join.filter.nnodes > 0)
			{
				matched = eval_qual(join.filter, join.filter.root, *eval_ctx);
				if (!matched)
					continue;
			}

			if (join.kind == PG_VEC_JOIN_ANTI)
			{
				anti_matched = true;
				break;
			}

			left_matched = true;
			probe_join_chain(params,
							 join_idx + 1,
							 inputs,
							 right_hashes,
							 eval_ctx,
							 plain_row,
							 groups,
							 group_index,
							 distinct_states);

			if (join.kind == PG_VEC_JOIN_SEMI)
				break;
		}

		if (join.kind == PG_VEC_JOIN_ANTI && !anti_matched)
		{
			cursor_set_null_row(&eval_ctx->inputs[join.right_input]);
			probe_join_chain(params,
							 join_idx + 1,
							 inputs,
							 right_hashes,
							 eval_ctx,
							 plain_row,
							 groups,
							 group_index,
							 distinct_states);
		}
		else if (join.kind == PG_VEC_JOIN_LEFT && !left_matched)
		{
			cursor_set_null_row(&eval_ctx->inputs[join.right_input]);
			probe_join_chain(params,
							 join_idx + 1,
							 inputs,
							 right_hashes,
							 eval_ctx,
							 plain_row,
							 groups,
							 group_index,
							 distinct_states);
		}
	}
}

static void
accumulate_join_chain(const PgVecScanFilterAggExecParams *params,
					  PgVecExecRow *plain_row,
					  std::vector<PgVecExecRow> *groups,
					  std::unordered_map<GroupKey, std::size_t, GroupKeyHash> *group_index,
					  std::vector<DistinctAggState> *distinct_states,
					  PgVecScanFilterAggExecResult *result)
{
	std::vector<MaterializedInput> inputs;
	std::vector<JoinMatchTable> right_hashes;
	EvalContext eval_ctx{};
	int stream_input;

	if (params->ninputs == 0 || params->njoins <= 0)
		return;
	stream_input = params->joins[0].left_input;
	if (stream_input < 0 || stream_input >= params->ninputs)
		elog(ERROR, "pg_vec: invalid leftmost input id %d for join chain",
			 stream_input);

	inputs.clear();
	inputs.reserve(params->ninputs);
	for (int input_id = 0; input_id < params->ninputs; input_id++)
		inputs.emplace_back(&params->inputs[input_id]);

	for (int input_id = 0; input_id < params->ninputs; input_id++)
	{
		if (input_id == stream_input)
			continue;
		materialize_filtered_input(input_id, params, &inputs[input_id], result);
	}

	init_materialized_eval_context(params, inputs, &eval_ctx);
	right_hashes.resize(params->njoins);

	for (int join_idx = 0; join_idx < params->njoins; join_idx++)
	{
		const PgVecJoinSpec &join = params->joins[join_idx];

		for (std::size_t right_row_idx = 0;
			 right_row_idx < inputs[join.right_input].row_count;
			 right_row_idx++)
		{
			JoinKey key{};

			cursor_bind_materialized(&eval_ctx.inputs[join.right_input],
									 &params->inputs[join.right_input],
									 params->rels[join.right_input],
									 params->snapshot,
									 inputs[join.right_input].late_mask,
									 &inputs[join.right_input]);
			cursor_set_materialized_row(&eval_ctx.inputs[join.right_input],
										right_row_idx);
			if (!extract_join_key(eval_ctx, join, false, &key))
				elog(ERROR, "pg_vec: failed to extract join build key");
			right_hashes[join_idx][key].push_back(right_row_idx);
		}

		cursor_reset_late_state(&eval_ctx.inputs[join.right_input]);
	}

	{
		BoundFilterProgram left_filter =
			build_bound_input_filter(&params->inputs[stream_input], stream_input);
		HeapDataChunkScanner left_scanner(params->rels[stream_input],
										  params->snapshot,
										  &params->inputs[stream_input],
										  &left_filter);
		ExecDataChunk *left_chunk = allocate_exec_chunk(&params->inputs[stream_input]);

		cursor_bind_chunk(&eval_ctx.inputs[stream_input],
						  &params->inputs[stream_input],
						  params->rels[stream_input],
						  params->snapshot,
						  0,
						  left_chunk);

		while (left_scanner.next_chunk(*left_chunk))
		{
			filter_chunk(stream_input,
						 &params->inputs[stream_input],
						 &left_filter,
						 *left_chunk);
			result->rows_scanned += left_chunk->count;
			result->rows_selected += left_chunk->sel.count;

			for (std::uint16_t i = 0; i < left_chunk->sel.count; i++)
			{
				cursor_set_chunk_row(&eval_ctx.inputs[stream_input],
									 (*left_chunk).sel[i]);
				probe_join_chain(params,
								 0,
								 inputs,
								 right_hashes,
								 &eval_ctx,
								 plain_row,
								 groups,
								 group_index,
								 distinct_states);
			}

			result->chunks_scanned++;
		}
	}

	cleanup_eval_context(&eval_ctx, params->ninputs);
}

static void
accumulate_joined(const PgVecScanFilterAggExecParams *params,
				  PgVecExecRow *plain_row,
				  std::vector<PgVecExecRow> *groups,
				  std::unordered_map<GroupKey, std::size_t, GroupKeyHash> *group_index,
				  std::vector<DistinctAggState> *distinct_states,
				  PgVecScanFilterAggExecResult *result)
{
	const PgVecJoinSpec &join = params->joins[0];
	uint32_t late_masks[PG_VEC_MAX_INPUTS];
	BoundJoinFilterProgram bound_join_filter =
		build_bound_join_filter(params, join.filter);
	std::unordered_map<JoinKey, std::vector<std::size_t>, JoinKeyHash> right_hash;
	Int32JoinMatchTable right_hash_i32;
	std::vector<uint64_t> right_clause_bits;
	BoundFilterProgram left_filter = build_bound_input_filter(
		&params->inputs[join.left_input],
		join.left_input);
	EvalContext eval_ctx{};
	int left_key_col_idx = -1;
	bool use_i32_fast_path = join_supports_single_int32_fast_path(join);
	build_single_join_late_masks(params, late_masks);

	MaterializedInput right_input(&params->inputs[join.right_input],
								  late_masks[join.right_input]);
	HeapDataChunkScanner left_scanner(params->rels[join.left_input],
									  params->snapshot,
									  &params->inputs[join.left_input],
									  &left_filter,
									  late_masks[join.left_input]);
	ExecDataChunk *left_chunk = allocate_exec_chunk(&params->inputs[join.left_input]);

	if (use_i32_fast_path)
	{
		left_key_col_idx = scan_column_index(&params->inputs[join.left_input],
											 join.keys[0].left.attno);
		if (left_key_col_idx < 0)
			elog(ERROR, "pg_vec: failed to locate int32 join key attno %d in left input %d",
				 (int) join.keys[0].left.attno,
				 (int) join.left_input);
		materialize_filtered_input_with_int32_hash(join.right_input,
												   join.keys[0].right.attno,
												   params,
												   &right_input,
												   &right_hash_i32,
												   result,
												   late_masks[join.right_input]);
	}
	else
	{
		EvalContext right_ctx{};

		cursor_bind_materialized(&right_ctx.inputs[join.right_input],
								 &params->inputs[join.right_input],
								 params->rels[join.right_input],
								 params->snapshot,
								 late_masks[join.right_input],
								 &right_input);

		materialize_filtered_input(join.right_input,
								   params,
								   &right_input,
								   result,
								   late_masks[join.right_input]);

		for (std::size_t row_idx = 0; row_idx < right_input.row_count; row_idx++)
		{
			JoinKey		key{};
			cursor_set_materialized_row(&right_ctx.inputs[join.right_input], row_idx);
			if (!extract_join_key(right_ctx, join, false, &key))
				elog(ERROR, "pg_vec: failed to extract join build key");
			right_hash[key].push_back(row_idx);
		}
		cursor_reset_late_state(&right_ctx.inputs[join.right_input]);
	}

	if (bound_join_filter.valid &&
		bound_join_filter.nclauses > 0 &&
		bound_join_filter.input_masks[join.right_input] != 0)
	{
		EvalContext right_ctx{};

		cursor_bind_materialized(&right_ctx.inputs[join.right_input],
								 &params->inputs[join.right_input],
								 params->rels[join.right_input],
								 params->snapshot,
								 late_masks[join.right_input],
								 &right_input);
		right_clause_bits.resize(right_input.row_count);

		for (std::size_t row_idx = 0; row_idx < right_input.row_count; row_idx++)
		{
			cursor_set_materialized_row(&right_ctx.inputs[join.right_input], row_idx);
			right_clause_bits[row_idx] =
				compute_bound_join_clause_bits(bound_join_filter,
											 join.right_input,
											 right_ctx);
		}

		cursor_reset_late_state(&right_ctx.inputs[join.right_input]);
	}

	cursor_bind_chunk(&eval_ctx.inputs[join.left_input],
					  &params->inputs[join.left_input],
					  params->rels[join.left_input],
					  params->snapshot,
					  late_masks[join.left_input],
					  left_chunk);
	cursor_bind_materialized(&eval_ctx.inputs[join.right_input],
							 &params->inputs[join.right_input],
							 params->rels[join.right_input],
							 params->snapshot,
							 late_masks[join.right_input],
							 &right_input);

	while (left_scanner.next_chunk(*left_chunk))
	{
		filter_chunk(join.left_input,
					 &params->inputs[join.left_input],
					 &left_filter,
					 *left_chunk);
		result->rows_scanned += left_chunk->count;
		result->rows_selected += left_chunk->sel.count;

		for (std::uint16_t i = 0; i < left_chunk->sel.count; i++)
		{
			JoinKey		key{};
			auto		it = right_hash.end();
			auto		it_i32 = right_hash_i32.end();
			const std::vector<std::size_t> *matches = nullptr;
			uint64_t	left_clause_bits = 0;

			cursor_set_chunk_row(&eval_ctx.inputs[join.left_input],
								 (*left_chunk).sel[i]);
			if (bound_join_filter.valid &&
				bound_join_filter.nclauses > 0 &&
				bound_join_filter.input_masks[join.left_input] != 0)
			{
				left_clause_bits =
					compute_bound_join_clause_bits(bound_join_filter,
												 join.left_input,
												 eval_ctx);
			}

			if (use_i32_fast_path)
			{
				std::int32_t key_i32 =
					left_chunk->int32_values[left_key_col_idx][(*left_chunk).sel[i]];

				it_i32 = right_hash_i32.find(key_i32);
				if (it_i32 == right_hash_i32.end())
				{
					if (join.kind == PG_VEC_JOIN_ANTI ||
						join.kind == PG_VEC_JOIN_LEFT)
					{
						if (join.kind == PG_VEC_JOIN_LEFT)
							cursor_set_null_row(&eval_ctx.inputs[join.right_input]);
						if (params->agg.grouped)
							accumulate_grouped_eval(params, eval_ctx, groups, group_index, distinct_states);
						else
							accumulate_row_aggs(params, eval_ctx, plain_row, distinct_states, 0, false);
					}
					continue;
				}
				matches = &it_i32->second;
			}
			else
			{
				if (!extract_join_key(eval_ctx, join, true, &key))
					elog(ERROR, "pg_vec: failed to extract join probe key");

				it = right_hash.find(key);
				if (it == right_hash.end())
				{
					if (join.kind == PG_VEC_JOIN_ANTI ||
						join.kind == PG_VEC_JOIN_LEFT)
					{
						if (join.kind == PG_VEC_JOIN_LEFT)
							cursor_set_null_row(&eval_ctx.inputs[join.right_input]);
						if (params->agg.grouped)
							accumulate_grouped_eval(params, eval_ctx, groups, group_index, distinct_states);
						else
							accumulate_row_aggs(params, eval_ctx, plain_row, distinct_states, 0, false);
					}
					continue;
				}
				matches = &it->second;
			}

			bool anti_matched = false;
			bool left_matched = false;
			for (std::size_t right_row_idx : *matches)
			{
				bool matched = true;

				cursor_set_materialized_row(&eval_ctx.inputs[join.right_input],
											right_row_idx);
				if (join.filter.nnodes > 0)
				{
					if (bound_join_filter.valid && bound_join_filter.root >= 0)
					{
						uint64_t clause_bits = left_clause_bits;

						if (!right_clause_bits.empty())
							clause_bits |= right_clause_bits[right_row_idx];
						matched = eval_bound_join_filter_node(bound_join_filter,
															  bound_join_filter.root,
															  clause_bits);
					}
					else
					{
						matched = eval_qual(join.filter, join.filter.root, eval_ctx);
					}

					if (!matched)
						continue;
				}

				if (join.kind == PG_VEC_JOIN_ANTI)
				{
					anti_matched = true;
					break;
				}
				left_matched = true;
				if (params->agg.grouped)
					accumulate_grouped_eval(params, eval_ctx, groups, group_index, distinct_states);
				else
					accumulate_row_aggs(params, eval_ctx, plain_row, distinct_states, 0, false);

				if (join.kind == PG_VEC_JOIN_SEMI)
					break;
			}

			if (join.kind == PG_VEC_JOIN_ANTI && !anti_matched)
			{
				if (params->agg.grouped)
					accumulate_grouped_eval(params, eval_ctx, groups, group_index, distinct_states);
				else
					accumulate_row_aggs(params, eval_ctx, plain_row, distinct_states, 0, false);
			}
			else if (join.kind == PG_VEC_JOIN_LEFT && !left_matched)
			{
				cursor_set_null_row(&eval_ctx.inputs[join.right_input]);
				if (params->agg.grouped)
					accumulate_grouped_eval(params, eval_ctx, groups, group_index, distinct_states);
				else
					accumulate_row_aggs(params, eval_ctx, plain_row, distinct_states, 0, false);
			}
		}

		result->chunks_scanned++;
	}

	cleanup_eval_context(&eval_ctx, params->ninputs);
}

} /* namespace pg_vec */

extern "C" void
pg_vec_execute_scan_filter_agg_datachunk(
	const PgVecScanFilterAggExecParams *params,
	PgVecScanFilterAggExecResult *result)
{
	PgVecExecRow plain_row;
	std::vector<PgVecExecRow> grouped_rows;
	std::unordered_map<pg_vec::GroupKey, std::size_t, pg_vec::GroupKeyHash> group_index;
	std::vector<pg_vec::DistinctAggState> distinct_states;

	std::memset(result, 0, sizeof(*result));
	pg_vec::init_exec_row(&params->agg, &plain_row);
	if (pg_vec::agg_has_distinct(&params->agg))
		pg_vec::init_distinct_states(&params->agg, &distinct_states);

	if (params->njoins > 1)
	{
		pg_vec::accumulate_join_chain(params,
									  &plain_row,
									  &grouped_rows,
									  &group_index,
									  &distinct_states,
									  result);
	}
	else if (params->njoins == 1)
	{
		pg_vec::accumulate_joined(params,
								  &plain_row,
								  &grouped_rows,
								  &group_index,
								  &distinct_states,
								  result);
	}
	else
	{
		pg_vec::accumulate_single_input(params,
										0,
										&plain_row,
										&grouped_rows,
										&group_index,
										&distinct_states,
										result);
	}

	if (params->agg.grouped)
	{
		pg_vec::apply_post_agg_filter(params, &grouped_rows);
		if (params->topn.enabled && params->topn.nsortkeys > 0)
		{
			std::sort(grouped_rows.begin(),
					  grouped_rows.end(),
					  [&](const PgVecExecRow &lhs, const PgVecExecRow &rhs)
					  {
						  return pg_vec::result_row_less(params, lhs, rhs);
					  });
		}
		else
		{
			std::sort(grouped_rows.begin(),
					  grouped_rows.end(),
					  [&](const PgVecExecRow &lhs, const PgVecExecRow &rhs)
					  {
						  return pg_vec::group_row_less(lhs, rhs, params->agg);
					  });
		}
		if (params->topn.enabled && params->topn.has_limit &&
			params->topn.limit_count < (int64) grouped_rows.size())
			grouped_rows.resize((std::size_t) params->topn.limit_count);
		pg_vec::materialize_result_rows(grouped_rows, result);
	}
	else
	{
		if (params->agg.having.root >= 0 && params->agg.having.nnodes > 0 &&
			!pg_vec::eval_post_agg_qual_node(params,
											 plain_row,
											 params->agg.having,
											 params->agg.having.root))
		{
			result->nrows = 0;
			result->rows = NULL;
		}
		else
		{
			result->nrows = 1;
			result->rows = (PgVecExecRow *) palloc0(sizeof(PgVecExecRow));
			result->rows[0] = plain_row;
		}
	}
}
