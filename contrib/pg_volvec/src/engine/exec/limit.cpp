#include "exec/internal.hpp"

namespace pg_volvec {

bool
VecLimitState::get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk)
{
	if (done_ || left_ == nullptr)
		return false;

	while (left_->get_next_batch(chunk))
	{
		uint64_t remaining;
		int active_count;

		if (emitted_ >= limit_count_)
		{
			done_ = true;
			chunk.reset();
			return false;
		}

		active_count = chunk.has_selection ? chunk.sel.count : chunk.count;
		if (active_count <= 0)
			continue;

		remaining = limit_count_ - emitted_;
		if ((uint64_t) active_count > remaining)
		{
			if (chunk.has_selection)
				chunk.sel.count = (uint16_t) remaining;
			else
				chunk.count = (int) remaining;
			active_count = (int) remaining;
			done_ = true;
		}
		emitted_ += (uint64_t) active_count;
		return active_count > 0;
	}

	done_ = true;
	return false;
}

} /* namespace pg_volvec */
