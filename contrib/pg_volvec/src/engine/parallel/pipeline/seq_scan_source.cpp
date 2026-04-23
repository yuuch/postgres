#include "parallel/pipeline/seq_scan_source.hpp"

namespace pg_volvec {
namespace pipeline {

std::unique_ptr<GlobalSourceState>
SeqScanSource::GetGlobalSourceState(ExecCtx & /*ctx*/)
{
	return std::unique_ptr<GlobalSourceState>(new GlobalSeqScanState(shared_));
}

std::unique_ptr<LocalSourceState>
SeqScanSource::GetLocalSourceState(ExecCtx & /*ctx*/, GlobalSourceState & /*gstate*/)
{
	return std::unique_ptr<LocalSourceState>(new LocalSeqScanState());
}

SourceResultType
SeqScanSource::GetData(ExecCtx & /*ctx*/, PipelineChunk &out, OperatorSourceInput &input)
{
	auto &gstate = static_cast<GlobalSeqScanState &>(input.global_state);
	auto &lstate = static_cast<LocalSeqScanState &>(input.local_state);
	const SeqScanSourceShared &shared = gstate.shared();

	for (;;)
	{
		if (lstate.morsel_active)
		{
			if (scan_->get_next_batch(out))
			{
				int active = out.has_selection ? out.sel.count : out.count;
				if (active <= 0)
					continue;
				return SourceResultType::HAVE_MORE_OUTPUT;
			}
			scan_->clear_source_block_range();
			lstate.morsel_active = false;
		}

		CHECK_FOR_INTERRUPTS();
		uint64 start = pg_atomic_fetch_add_u64(shared.next_block,
		                                       (uint64) shared.morsel_nblocks);
		if (start >= (uint64) shared.total_blocks)
			return SourceResultType::FINISHED;

		uint32 nblocks = (uint32) Min((uint64) shared.morsel_nblocks,
		                              (uint64) shared.total_blocks - start);
		if (!scan_->configure_source_block_range((BlockNumber) start, nblocks))
			elog(ERROR, "pg_volvec SeqScanSource could not configure block range [%lu,+%u)",
			     (unsigned long) start, nblocks);
		lstate.morsel_active = true;
	}
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */
