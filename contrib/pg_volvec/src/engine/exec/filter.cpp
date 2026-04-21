#include "exec/internal.hpp"

namespace pg_volvec {

bool VecFilterState::get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) {
	while (left_->get_next_batch(chunk)) { 
		program_->evaluate(chunk); 
		if (chunk.sel.count > 0) return true; 
	}
	return false;
}
VecLookupFilterState::VecLookupFilterState(std::unique_ptr<VecPlanState> left,
											 std::unique_ptr<VecPlanState> lookup_source,
											 uint16_t input_key_col,
											 VecOutputColMeta input_key_meta,
											 uint16_t lookup_key_col,
											 VecOutputColMeta lookup_key_meta,
											 bool negate)
	: left_(std::move(left)),
	  lookup_source_(std::move(lookup_source)),
	  memory_context_(CurrentMemoryContext),
	  lookup_table_(memory_context_),
	  input_key_col_(input_key_col),
	  input_key_meta_(input_key_meta),
	  lookup_key_col_(lookup_key_col),
	  lookup_key_meta_(lookup_key_meta),
	  negate_(negate),
	  lookup_built_(false),
	  lookup_has_null_(false)
{
}

bool
VecLookupFilterState::extract_lookup_key(const DataChunk<DEFAULT_CHUNK_SIZE> &chunk,
										 int row,
										 uint16_t col,
										 const VecOutputColMeta &meta,
										 VecLookupScalarKey *key) const
{
	if (key == nullptr || col >= 16)
		return false;

	key->is_null = chunk.nulls[col][row] ? 1 : 0;
	key->value = 0;
	if (key->is_null)
		return true;

	switch (meta.storage_kind)
	{
		case VecOutputStorageKind::Int32:
			key->value = (int64_t) chunk.int32_columns[col][row];
			return true;
		case VecOutputStorageKind::Int64:
		case VecOutputStorageKind::NumericScaledInt64:
			key->value = chunk.int64_columns[col][row];
			return true;
		default:
			return false;
	}
}

bool
VecLookupFilterState::build_lookup()
{
	if (lookup_built_)
		return true;
	if (lookup_source_ == nullptr)
		return false;

	while (lookup_source_->get_next_batch(lookup_chunk_))
	{
		int active_count = lookup_chunk_.has_selection ? lookup_chunk_.sel.count : lookup_chunk_.count;

		for (int s = 0; s < active_count; s++)
		{
			int row = lookup_chunk_.has_selection ? lookup_chunk_.sel.row_ids[s] : s;
			VecLookupScalarKey key;
			VecLookupScalarValue value;

			if (!extract_lookup_key(lookup_chunk_, row, lookup_key_col_, lookup_key_meta_, &key))
				return false;
			if (key.is_null)
		{
			lookup_has_null_ = true;
			continue;
		}
		value.is_null = 0;
		auto [slot, inserted] = lookup_table_.insert(key);
		slot->val = value;
	}
	}

	lookup_built_ = true;
	if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: built lookup filter table (rows=%zu, has_null=%d, negate=%d)",
			 lookup_table_.size(),
			 lookup_has_null_ ? 1 : 0,
			 negate_ ? 1 : 0);
	return true;
}

bool
VecLookupFilterState::get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk)
{
	if (!build_lookup())
		return false;

	while (left_->get_next_batch(chunk))
	{
		bool source_has_selection = chunk.has_selection;
		int active_count = source_has_selection ? chunk.sel.count : chunk.count;

		chunk.sel.count = 0;
		chunk.has_selection = true;
		for (int s = 0; s < active_count; s++)
		{
			int row = source_has_selection ? chunk.sel.row_ids[s] : s;
			VecLookupScalarKey key;
			bool matched = false;
		bool pass;

		if (!extract_lookup_key(chunk, row, input_key_col_, input_key_meta_, &key))
			return false;
		if (!key.is_null)
			matched = (lookup_table_.find(key) != nullptr);
		if (negate_)
			pass = !key.is_null && !matched && !lookup_has_null_;
		else
			pass = !key.is_null && matched;
			if (pass)
				chunk.sel.row_ids[chunk.sel.count++] = (uint16_t) row;
		}

		if (chunk.sel.count == chunk.count && !source_has_selection)
			chunk.has_selection = false;
		if (chunk.sel.count > 0 || (!chunk.has_selection && chunk.count > 0))
			return true;
	}

	return false;
}

} /* namespace pg_volvec */
