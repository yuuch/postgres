extern "C" {
#include "postgres.h"

#include "access/heapam.h"
#include "access/tableam.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
}

#include "vec_exec_api.h"
#include "data_chunk.hpp"
#include "data_chunk_deform.hpp"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <unordered_map>
#include <vector>

namespace pg_vec
{

static constexpr std::uint16_t kExecChunkCapacity = 2048;

struct ExecDataChunk : public DataChunkHeader<kExecChunkCapacity>
{
	int			ncolumns;
	PgVecScalarKind column_kinds[PG_VEC_MAX_SCAN_COLUMNS];
	std::int32_t int32_values[PG_VEC_MAX_SCAN_COLUMNS][kExecChunkCapacity];
	std::int32_t date32_values[PG_VEC_MAX_SCAN_COLUMNS][kExecChunkCapacity];
	std::int64_t decimal64_values[PG_VEC_MAX_SCAN_COLUMNS][kExecChunkCapacity];
	char		char1_values[PG_VEC_MAX_SCAN_COLUMNS][kExecChunkCapacity];
	PgVecStringConst string128_values[PG_VEC_MAX_SCAN_COLUMNS][kExecChunkCapacity];
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

struct MaterializedInput
{
	const PgVecInputSpec *spec;
	std::vector<std::int32_t> int32_columns[PG_VEC_MAX_SCAN_COLUMNS];
	std::vector<std::int32_t> date32_columns[PG_VEC_MAX_SCAN_COLUMNS];
	std::vector<std::int64_t> decimal64_columns[PG_VEC_MAX_SCAN_COLUMNS];
	std::vector<char> char1_columns[PG_VEC_MAX_SCAN_COLUMNS];
	std::vector<PgVecStringConst> string128_columns[PG_VEC_MAX_SCAN_COLUMNS];
	std::size_t row_count;

	explicit MaterializedInput(const PgVecInputSpec *input_spec) :
		spec(input_spec),
		row_count(0)
	{
	}

	void append_row(const ExecDataChunk &chunk, std::uint16_t row);
};

struct ExecInputCursor
{
	const PgVecInputSpec *spec;
	const ExecDataChunk *chunk;
	const MaterializedInput *materialized;
	std::uint16_t chunk_row;
	std::size_t materialized_row;
};

struct EvalContext
{
	ExecInputCursor inputs[PG_VEC_MAX_INPUTS];
};

static bool string_equals(const PgVecStringConst &lhs, const PgVecStringConst &rhs);
static int compare_string(const PgVecStringConst &lhs, const PgVecStringConst &rhs);
static bool compare_string_value(const PgVecStringConst &lhs,
								   const PgVecStringConst &rhs,
								   PgVecFilterOp op);
static bool string_starts_with(const PgVecStringConst &value,
								  const PgVecStringConst &prefix);

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
			case PG_VEC_SCALAR_CHAR1:
				if (values[i].char1 != other.values[i].char1)
					return false;
				break;
			case PG_VEC_SCALAR_STRING128:
				if (!string_equals(values[i].string128, other.values[i].string128))
					return false;
				break;
			case PG_VEC_SCALAR_DECIMAL128_S4:
			case PG_VEC_SCALAR_DECIMAL128_S6:
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
			case PG_VEC_SCALAR_CHAR1:
				hash ^= static_cast<unsigned char>(key.values[i].char1);
				break;
			case PG_VEC_SCALAR_STRING128:
				for (uint16 j = 0; j < key.values[i].string128.len; j++)
					hash ^= static_cast<unsigned char>(key.values[i].string128.bytes[j]) +
						0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
				break;
			case PG_VEC_SCALAR_DECIMAL128_S4:
			case PG_VEC_SCALAR_DECIMAL128_S6:
			case PG_VEC_SCALAR_INVALID:
			default:
				break;
		}
	}

	return hash;
}

void
MaterializedInput::append_row(const ExecDataChunk &chunk, std::uint16_t row)
{
	for (int col_idx = 0; col_idx < spec->ncolumns; col_idx++)
	{
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
			case PG_VEC_SCALAR_CHAR1:
				char1_columns[col_idx].push_back(chunk.char1_values[col_idx][row]);
				break;
			case PG_VEC_SCALAR_STRING128:
				string128_columns[col_idx].push_back(chunk.string128_values[col_idx][row]);
				break;
			case PG_VEC_SCALAR_DECIMAL128_S4:
			case PG_VEC_SCALAR_DECIMAL128_S6:
			case PG_VEC_SCALAR_INVALID:
			default:
				elog(ERROR, "pg_vec: unsupported materialized scalar kind %d",
					 (int) spec->columns[col_idx].scalar_kind);
		}
	}

	row_count++;
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
			return DeformDecodeKind::kString128;
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
build_deform_program(const PgVecInputSpec *input)
{
	DeformProgram program;

	program.reset();

	for (int i = 0; i < input->ncolumns; i++)
	{
		if (!program.add_target(input->columns[i].attno - 1,
								 i,
								 decode_kind_for_scalar(input->columns[i].scalar_kind)))
			elog(ERROR, "pg_vec: failed to build deform program");
	}

	program.finalize();
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
					columns[i].data = chunk.string128_values[i];
					break;
				case PG_VEC_SCALAR_DECIMAL128_S6:
				case PG_VEC_SCALAR_DECIMAL128_S4:
				case PG_VEC_SCALAR_INVALID:
				default:
					elog(ERROR, "pg_vec: unsupported chunk binding scalar kind %d",
						 (int) chunk.column_kinds[i]);
			}
		}

		bindings.columns = columns;
		bindings.ncolumns = chunk.ncolumns;
	}
};

class HeapDataChunkScanner
{
public:
	HeapDataChunkScanner(Relation rel,
						 Snapshot snapshot,
						 const PgVecInputSpec *input) :
		rel_(rel),
		snapshot_(snapshot),
		input_(input),
		desc_(RelationGetDescr(rel)),
		program_(build_deform_program(input_)),
		deformer_(desc_, &program_)
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
		total_blocks_ = scan_->rs_nblocks;
		blocks_scanned_ = 0;
		next_block_ = (total_blocks_ == 0) ? InvalidBlockNumber : scan_->rs_startblock;
		page_visible_index_ = 0;
	}

	~HeapDataChunkScanner()
	{
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

			if (blocks_scanned_ >= total_blocks_ || next_block_ == InvalidBlockNumber)
				return false;

			load_next_page();
			if (scan_->rs_ntuples > 0)
				return true;
		}
	}

	void
	load_next_page()
	{
		Buffer		buffer;

		CHECK_FOR_INTERRUPTS();

		buffer = ReadBufferExtended(scan_->rs_base.rs_rd,
									MAIN_FORKNUM,
									next_block_,
									RBM_NORMAL,
									scan_->rs_strategy);
		scan_->rs_cbuf = buffer;
		scan_->rs_cblock = next_block_;
		heap_prepare_pagescan((TableScanDesc) scan_);
		page_visible_index_ = 0;

		blocks_scanned_++;
		if (blocks_scanned_ >= total_blocks_)
			next_block_ = InvalidBlockNumber;
		else
			next_block_ = (next_block_ + 1) % total_blocks_;
	}

	void
	release_current_page()
	{
		if (BufferIsValid(scan_->rs_cbuf))
		{
			ReleaseBuffer(scan_->rs_cbuf);
			scan_->rs_cbuf = InvalidBuffer;
		}
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

		return deformer_.append_tuple(&tuple, chunk.count, bindings);
	}

	Relation	rel_;
	Snapshot	snapshot_;
	const PgVecInputSpec *input_;
	HeapScanDesc scan_;
	TupleDesc	desc_;
	DeformProgram program_;
	DataChunkDeformer deformer_;
	BlockNumber next_block_;
	BlockNumber total_blocks_;
	BlockNumber blocks_scanned_;
	uint32		page_visible_index_;
};

static bool
string_equals(const PgVecStringConst &lhs, const PgVecStringConst &rhs)
{
	if (lhs.len != rhs.len)
		return false;
	if (lhs.len == 0)
		return true;
	return std::memcmp(lhs.bytes, rhs.bytes, lhs.len) == 0;
}

static int
compare_string(const PgVecStringConst &lhs, const PgVecStringConst &rhs)
{
	int			cmp;
	uint16		min_len = (lhs.len < rhs.len) ? lhs.len : rhs.len;

	if (min_len > 0)
	{
		cmp = std::memcmp(lhs.bytes, rhs.bytes, min_len);
		if (cmp != 0)
			return cmp;
	}

	if (lhs.len < rhs.len)
		return -1;
	if (lhs.len > rhs.len)
		return 1;
	return 0;
}

template <typename T>
static bool
compare_value(T lhs, T rhs, PgVecFilterOp op)
{
	switch (op)
	{
		case PG_VEC_OP_EQ:
			return lhs == rhs;
		case PG_VEC_OP_LT:
			return lhs < rhs;
		case PG_VEC_OP_LE:
			return lhs <= rhs;
		case PG_VEC_OP_GT:
			return lhs > rhs;
		case PG_VEC_OP_GE:
			return lhs >= rhs;
		case PG_VEC_OP_PREFIX_LIKE:
		case PG_VEC_OP_INVALID:
		default:
			return false;
	}
}

static bool
compare_string_value(const PgVecStringConst &lhs,
					   const PgVecStringConst &rhs,
					   PgVecFilterOp op)
{
	int			cmp = compare_string(lhs, rhs);

	switch (op)
	{
		case PG_VEC_OP_EQ:
			return cmp == 0;
		case PG_VEC_OP_LT:
			return cmp < 0;
		case PG_VEC_OP_LE:
			return cmp <= 0;
		case PG_VEC_OP_GT:
			return cmp > 0;
		case PG_VEC_OP_GE:
			return cmp >= 0;
		case PG_VEC_OP_PREFIX_LIKE:
		case PG_VEC_OP_INVALID:
		default:
			return false;
	}
}

static bool
string_starts_with(const PgVecStringConst &value,
					  const PgVecStringConst &prefix)
{
	if (value.len < prefix.len)
		return false;
	if (prefix.len == 0)
		return true;
	return std::memcmp(value.bytes, prefix.bytes, prefix.len) == 0;
}

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

	if (!cursor_get_column_index(cursor, attno, &column_idx))
		return false;

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

	if (!cursor_get_column_index(cursor, attno, &column_idx))
		return false;

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

	if (!cursor_get_column_index(cursor, attno, &column_idx))
		return false;

	if (cursor.materialized != nullptr)
		*out = cursor.materialized->decimal64_columns[column_idx][cursor.materialized_row];
	else
		*out = cursor.chunk->decimal64_values[column_idx][cursor.chunk_row];
	return true;
}

static bool
cursor_get_char1(const ExecInputCursor &cursor, AttrNumber attno, char *out)
{
	int			column_idx;

	if (!cursor_get_column_index(cursor, attno, &column_idx))
		return false;

	if (cursor.materialized != nullptr)
		*out = cursor.materialized->char1_columns[column_idx][cursor.materialized_row];
	else
		*out = cursor.chunk->char1_values[column_idx][cursor.chunk_row];
	return true;
}

static bool
cursor_get_string128(const ExecInputCursor &cursor,
					 AttrNumber attno,
					 const PgVecStringConst **out)
{
	int			column_idx;

	if (!cursor_get_column_index(cursor, attno, &column_idx))
		return false;

	if (cursor.materialized != nullptr)
		*out = &cursor.materialized->string128_columns[column_idx][cursor.materialized_row];
	else
		*out = &cursor.chunk->string128_values[column_idx][cursor.chunk_row];
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
		case PG_VEC_SCALAR_CHAR1:
		{
			char		value;

			if (!cursor_get_char1(cursor, column.attno, &value))
				return false;
			*out = static_cast<unsigned char>(value);
			return true;
		}
		case PG_VEC_SCALAR_STRING128:
		case PG_VEC_SCALAR_DECIMAL128_S6:
		case PG_VEC_SCALAR_DECIMAL128_S4:
		case PG_VEC_SCALAR_INVALID:
		default:
			return false;
	}
}

static bool
column_value_to_const(const EvalContext &ctx,
					   const PgVecColumnRef &column,
					   PgVecConstValue *out)
{
	const ExecInputCursor &cursor = ctx.inputs[column.input_id];

	switch (column.scalar_kind)
	{
		case PG_VEC_SCALAR_INT32:
			return cursor_get_int32(cursor, column.attno, &out->int32_value);
		case PG_VEC_SCALAR_DATE32:
			return cursor_get_date32(cursor, column.attno, &out->date32);
		case PG_VEC_SCALAR_DECIMAL64_S2:
			return cursor_get_decimal64(cursor, column.attno, &out->decimal64_s2);
		case PG_VEC_SCALAR_CHAR1:
			return cursor_get_char1(cursor, column.attno, &out->char1);
		case PG_VEC_SCALAR_STRING128:
		{
			const PgVecStringConst *value;

			if (!cursor_get_string128(cursor, column.attno, &value))
				return false;
			out->string128 = *value;
			return true;
		}
		case PG_VEC_SCALAR_DECIMAL128_S6:
		case PG_VEC_SCALAR_DECIMAL128_S4:
		case PG_VEC_SCALAR_INVALID:
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
			*out = constant.decimal64_s2;
			return true;
		case PG_VEC_SCALAR_CHAR1:
			*out = static_cast<unsigned char>(constant.char1);
			return true;
		case PG_VEC_SCALAR_STRING128:
		case PG_VEC_SCALAR_DECIMAL128_S6:
		case PG_VEC_SCALAR_DECIMAL128_S4:
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

static bool
eval_expr_to_string128(const PgVecExprProgram &program,
					   int node_idx,
					   const EvalContext &ctx,
					   const PgVecStringConst **out)
{
	if (node_idx < 0 || node_idx >= program.nnodes)
		return false;

	const PgVecExprNode &expr = program.nodes[node_idx];

	switch (expr.kind)
	{
		case PG_VEC_EXPR_COLUMN:
			if (expr.column.scalar_kind != PG_VEC_SCALAR_STRING128)
				return false;
			return cursor_get_string128(ctx.inputs[expr.column.input_id],
										 expr.column.attno,
										 out);

		case PG_VEC_EXPR_CONST:
			if (expr.scalar_kind != PG_VEC_SCALAR_STRING128)
				return false;
			*out = &expr.constant.string128;
			return true;

		case PG_VEC_EXPR_ADD:
		case PG_VEC_EXPR_SUB:
		case PG_VEC_EXPR_MUL:
		case PG_VEC_EXPR_INVALID:
		default:
			return false;
	}
}

static bool
eval_qual(const PgVecFilterSpec &filter,
		  int qual_idx,
		  const EvalContext &ctx)
{
	__int128	lhs_value;
	__int128	rhs_value;
	const PgVecStringConst *lhs_string;
	const PgVecStringConst *rhs_string;

	if (qual_idx < 0 || qual_idx >= filter.nnodes)
		return false;

	const PgVecQualNode &qual = filter.nodes[qual_idx];

	switch (qual.kind)
	{
		case PG_VEC_QUAL_COMPARE:
			if (qual.op == PG_VEC_OP_PREFIX_LIKE)
			{
				if (!eval_expr_to_string128(filter.exprs, qual.lhs_expr, ctx, &lhs_string) ||
					!eval_expr_to_string128(filter.exprs, qual.rhs_expr, ctx, &rhs_string))
					return false;
				return string_starts_with(*lhs_string, *rhs_string);
			}

			if (filter.exprs.nodes[qual.lhs_expr].scalar_kind == PG_VEC_SCALAR_STRING128)
			{
				if (!eval_expr_to_string128(filter.exprs, qual.lhs_expr, ctx, &lhs_string) ||
					!eval_expr_to_string128(filter.exprs, qual.rhs_expr, ctx, &rhs_string))
					return false;
				return compare_string_value(*lhs_string, *rhs_string, qual.op);
			}

			if (!eval_expr_to_int128(filter.exprs, qual.lhs_expr, ctx, &lhs_value) ||
				!eval_expr_to_int128(filter.exprs, qual.rhs_expr, ctx, &rhs_value))
				return false;
			return compare_value(lhs_value, rhs_value, qual.op);

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
filter_chunk(uint8 input_id, const PgVecInputSpec *input, ExecDataChunk &chunk)
{
	EvalContext eval_ctx{};

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
					PgVecExecRow *agg_row)
{
	for (int agg_idx = 0; agg_idx < params->agg.naggs; agg_idx++)
		advance_agg_state(params->agg.aggs[agg_idx], ctx, &agg_row->aggs[agg_idx]);
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
		case PG_VEC_SCALAR_CHAR1:
		{
			unsigned char lhs_char = static_cast<unsigned char>(lhs.char1);
			unsigned char rhs_char = static_cast<unsigned char>(rhs.char1);

			return (lhs_char > rhs_char) - (lhs_char < rhs_char);
		}
		case PG_VEC_SCALAR_STRING128:
			return compare_string(lhs.string128, rhs.string128);
		case PG_VEC_SCALAR_DECIMAL128_S6:
		case PG_VEC_SCALAR_DECIMAL128_S4:
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
		if (!column_value_to_const(ctx, agg_spec.group_keys[key_idx], &group_key->values[key_idx]))
			return false;
		typed_values[key_idx] = group_key->values[key_idx];
	}

	return true;
}

static void
accumulate_grouped_eval(const PgVecScanFilterAggExecParams *params,
						const EvalContext &ctx,
						std::vector<PgVecExecRow> *groups,
						std::unordered_map<GroupKey, std::size_t, GroupKeyHash> *group_index)
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

	accumulate_row_aggs(params, ctx, &(*groups)[group_pos]);
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
extract_join_key(const EvalContext &ctx,
				 const PgVecJoinSpec &join,
				 bool use_left_side,
				 JoinKey *key)
{
	key->nkeys = join.nkeys;
	for (int key_idx = 0; key_idx < join.nkeys; key_idx++)
	{
		__int128	value;

		if (!column_value_to_int128(ctx,
									use_left_side ? join.keys[key_idx].left
												  : join.keys[key_idx].right,
									&value))
			return false;
		key->values[key_idx] = (std::int32_t) value;
	}

	return true;
}

static void
materialize_filtered_input(uint8 input_id,
							const PgVecScanFilterAggExecParams *params,
							MaterializedInput *materialized,
							PgVecScanFilterAggExecResult *result)
{
	HeapDataChunkScanner scanner(params->rels[input_id],
								 params->snapshot,
								 &params->inputs[input_id]);
	ExecDataChunk chunk;

	std::memset(&chunk, 0, sizeof(chunk));
	chunk.ncolumns = params->inputs[input_id].ncolumns;
	for (int i = 0; i < chunk.ncolumns; i++)
		chunk.column_kinds[i] = params->inputs[input_id].columns[i].scalar_kind;

	while (scanner.next_chunk(chunk))
	{
		filter_chunk(input_id, &params->inputs[input_id], chunk);
		result->rows_scanned += chunk.count;
		result->rows_selected += chunk.sel.count;
		for (std::uint16_t i = 0; i < chunk.sel.count; i++)
			materialized->append_row(chunk, chunk.sel[i]);
		result->chunks_scanned++;
	}
}

static void
accumulate_single_input(const PgVecScanFilterAggExecParams *params,
						uint8 input_id,
						PgVecExecRow *plain_row,
						std::vector<PgVecExecRow> *groups,
						std::unordered_map<GroupKey, std::size_t, GroupKeyHash> *group_index,
						PgVecScanFilterAggExecResult *result)
{
	HeapDataChunkScanner scanner(params->rels[input_id],
								 params->snapshot,
								 &params->inputs[input_id]);
	ExecDataChunk chunk;
	EvalContext eval_ctx{};

	std::memset(&chunk, 0, sizeof(chunk));
	chunk.ncolumns = params->inputs[input_id].ncolumns;
	for (int i = 0; i < chunk.ncolumns; i++)
		chunk.column_kinds[i] = params->inputs[input_id].columns[i].scalar_kind;

	eval_ctx.inputs[input_id].spec = &params->inputs[input_id];
	eval_ctx.inputs[input_id].chunk = &chunk;
	eval_ctx.inputs[input_id].materialized = nullptr;

	while (scanner.next_chunk(chunk))
	{
		filter_chunk(input_id, &params->inputs[input_id], chunk);
		result->rows_scanned += chunk.count;
		result->rows_selected += chunk.sel.count;
		for (std::uint16_t i = 0; i < chunk.sel.count; i++)
		{
			eval_ctx.inputs[input_id].chunk_row = chunk.sel[i];
			if (params->agg.grouped)
				accumulate_grouped_eval(params, eval_ctx, groups, group_index);
			else
				accumulate_row_aggs(params, eval_ctx, plain_row);
		}
		result->chunks_scanned++;
	}
}

static void
accumulate_joined(const PgVecScanFilterAggExecParams *params,
				  PgVecExecRow *plain_row,
				  std::vector<PgVecExecRow> *groups,
				  std::unordered_map<GroupKey, std::size_t, GroupKeyHash> *group_index,
				  PgVecScanFilterAggExecResult *result)
{
	MaterializedInput right_input(&params->inputs[params->join.right_input]);
	std::unordered_map<JoinKey, std::vector<std::size_t>, JoinKeyHash> right_hash;
	HeapDataChunkScanner left_scanner(params->rels[params->join.left_input],
									  params->snapshot,
									  &params->inputs[params->join.left_input]);
	ExecDataChunk left_chunk;
	EvalContext eval_ctx{};

	materialize_filtered_input(params->join.right_input, params, &right_input, result);

	for (std::size_t row_idx = 0; row_idx < right_input.row_count; row_idx++)
	{
		JoinKey		key{};
		EvalContext right_ctx{};

		right_ctx.inputs[params->join.right_input].spec = &params->inputs[params->join.right_input];
		right_ctx.inputs[params->join.right_input].materialized = &right_input;
		right_ctx.inputs[params->join.right_input].materialized_row = row_idx;
		if (!extract_join_key(right_ctx, params->join, false, &key))
			elog(ERROR, "pg_vec: failed to extract join build key");
		right_hash[key].push_back(row_idx);
	}

	std::memset(&left_chunk, 0, sizeof(left_chunk));
	left_chunk.ncolumns = params->inputs[params->join.left_input].ncolumns;
	for (int i = 0; i < left_chunk.ncolumns; i++)
		left_chunk.column_kinds[i] = params->inputs[params->join.left_input].columns[i].scalar_kind;

	eval_ctx.inputs[params->join.left_input].spec = &params->inputs[params->join.left_input];
	eval_ctx.inputs[params->join.left_input].chunk = &left_chunk;
	eval_ctx.inputs[params->join.left_input].materialized = nullptr;
	eval_ctx.inputs[params->join.right_input].spec = &params->inputs[params->join.right_input];
	eval_ctx.inputs[params->join.right_input].materialized = &right_input;

	while (left_scanner.next_chunk(left_chunk))
	{
		filter_chunk(params->join.left_input,
					 &params->inputs[params->join.left_input],
					 left_chunk);
		result->rows_scanned += left_chunk.count;
		result->rows_selected += left_chunk.sel.count;

		for (std::uint16_t i = 0; i < left_chunk.sel.count; i++)
		{
			JoinKey		key{};
			auto		it = right_hash.end();

			eval_ctx.inputs[params->join.left_input].chunk_row = left_chunk.sel[i];
			if (!extract_join_key(eval_ctx, params->join, true, &key))
				elog(ERROR, "pg_vec: failed to extract join probe key");

			it = right_hash.find(key);
			if (it == right_hash.end())
				continue;

			for (std::size_t right_row_idx : it->second)
			{
				eval_ctx.inputs[params->join.right_input].materialized_row = right_row_idx;
				if (params->agg.grouped)
					accumulate_grouped_eval(params, eval_ctx, groups, group_index);
				else
					accumulate_row_aggs(params, eval_ctx, plain_row);
			}
		}

		result->chunks_scanned++;
	}
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

	std::memset(result, 0, sizeof(*result));
	pg_vec::init_exec_row(&params->agg, &plain_row);

	if (params->join.enabled)
	{
		pg_vec::accumulate_joined(params,
								  &plain_row,
								  &grouped_rows,
								  &group_index,
								  result);
	}
	else
	{
		pg_vec::accumulate_single_input(params,
										0,
										&plain_row,
										&grouped_rows,
										&group_index,
										result);
	}

	if (params->agg.grouped)
	{
		std::sort(grouped_rows.begin(),
				  grouped_rows.end(),
				  [&](const PgVecExecRow &lhs, const PgVecExecRow &rhs)
				  {
					  return pg_vec::group_row_less(lhs, rhs, params->agg);
				  });
		pg_vec::materialize_result_rows(grouped_rows, result);
	}
	else
	{
		result->nrows = 1;
		result->rows = (PgVecExecRow *) palloc0(sizeof(PgVecExecRow));
		result->rows[0] = plain_row;
	}
}
