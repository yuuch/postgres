#pragma once

extern "C" {
#include "postgres.h"
#include "utils/dsa.h"
}

#include "parallel/pipeline/operator.hpp"
#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/sink.hpp"
#include "parallel/pipeline/source.hpp"
#include "parallel/pipeline/tuple_data_collection.hpp"
#include "parallel/pipeline/tuple_data_layout.hpp"

namespace pg_volvec {
namespace pipeline {

struct OpDescriptor;

class OrderGlobalState final : public GlobalSinkState, public GlobalSourceState {
public:
	dsa_area             *dsa = nullptr;
	OpDescriptor         *desc = nullptr;
	dsa_pointer           key_layout_dp = InvalidDsaPointer;
	dsa_pointer           payload_layout_dp = InvalidDsaPointer;
	dsa_pointer           shared_payload_dp = InvalidDsaPointer;
	dsa_pointer           sort_indices_dp = InvalidDsaPointer;
	const TupleDataLayout *key_layout = nullptr;
	const TupleDataLayout *payload_layout = nullptr;
	TupleDataCollection  *payload = nullptr;
	uint32_t              max_rows = 256;
};

class OrderLocalState final : public LocalSinkState, public LocalSourceState {
public:
	uint32_t source_cursor = 0;
};

/*
 * Sort indices array stored at the head of the OrderGlobalState's auxiliary
 * DSA allocation. Built in Finalize, consumed in GetData. Single-segment.
 */
struct OrderSortIndices {
	uint32_t count;
	uint32_t _pad;
	uint32_t indices[];
};

class PhysicalOrder final : public PhysicalOperator {
public:
	/*
	 * Step 6 contract delta: Order now receives serialized key/payload layouts
	 * instead of relying on Q1-specific row PODs. The sink/source bodies remain
	 * placeholder stubs for the later real sort implementation.
	 */
	explicit PhysicalOrder(dsa_pointer key_layout_dp = InvalidDsaPointer,
	                       dsa_pointer payload_layout_dp = InvalidDsaPointer,
	                       dsa_pointer shared_payload_dp = InvalidDsaPointer,
	                       OpDescriptor *desc = nullptr)
		: PhysicalOperator(PhysicalOperatorType::ORDER)
		, key_layout_dp_(key_layout_dp)
		, payload_layout_dp_(payload_layout_dp)
		, shared_payload_dp_(shared_payload_dp)
		, desc_(desc)
	{}

	bool IsSource() const override { return true; }
	bool IsSink() const override { return true; }
	bool IsPipelineBreaker() const override { return true; }
	int  MaxThreads(ExecCtx &ctx) const override { (void) ctx; return 1; }

	std::unique_ptr<GlobalSinkState>   GetGlobalSinkState(ExecCtx &ctx) override;
	std::unique_ptr<LocalSinkState>    GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &gstate) override;
	SinkResultType                     SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input) override;
	SinkCombineResultType              Combine(ExecCtx &ctx, OperatorSinkCombineInput &input) override;
	SinkFinalizeType                   Finalize(ExecCtx &ctx, GlobalSinkState &gstate) override;

	std::unique_ptr<GlobalSourceState> GetGlobalSourceState(ExecCtx &ctx) override;
	std::unique_ptr<LocalSourceState>  GetLocalSourceState(ExecCtx &ctx, GlobalSourceState &gstate) override;
	SourceResultType                   GetData(ExecCtx &ctx, PipelineChunk &out, OperatorSourceInput &input) override;

	dsa_pointer                        key_layout_dp() const { return key_layout_dp_; }
	dsa_pointer                        payload_layout_dp() const { return payload_layout_dp_; }
	dsa_pointer                        shared_payload_dp() const { return shared_payload_dp_; }
	OpDescriptor                      *desc() const { return desc_; }
	const PgVector<OpDescriptor *>    &descs() const { return desc_list_; }
	void                               AttachDescriptor(OpDescriptor *desc) { desc_ = desc; desc_list_.push_back(desc); }  /* see physical_hash_aggregate.hpp Fix A2 */

private:
	dsa_pointer   key_layout_dp_;
	dsa_pointer   payload_layout_dp_;
	dsa_pointer   shared_payload_dp_;
	OpDescriptor *desc_;
	PgVector<OpDescriptor *> desc_list_;
};

}  /* namespace pipeline */
}  /* namespace pg_volvec */
