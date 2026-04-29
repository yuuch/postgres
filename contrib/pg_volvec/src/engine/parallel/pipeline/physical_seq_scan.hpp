#pragma once

extern "C" {
#include "postgres.h"
#include "access/relscan.h"
#include "catalog/pg_type_d.h"
#include "executor/execdesc.h"
#include "storage/block.h"
#include "utils/dsa.h"
#include "utils/rel.h"
}

#include "parallel/pipeline/physical_operator.hpp"
#include "core/data_chunk.hpp"

namespace pg_volvec {

class VecExprProgram;

namespace pipeline {

struct OpDescriptor;
struct SchemaDescriptor;
struct SeqScanSharedPayload;  /* canonical definition in pipeline_descriptor.hpp */

class SeqScanGlobalState final : public GlobalSourceState {
public:
	dsa_area             *dsa = nullptr;
	OpDescriptor         *desc = nullptr;
	SeqScanSharedPayload *shared = nullptr;
	dsa_pointer           shared_payload_dp = InvalidDsaPointer;
	uint32                max_threads = 1;
};

class SeqScanLocalState final : public LocalSourceState {
public:
	Relation        rel = nullptr;
	TableScanDesc   scan_desc = nullptr;
	TupleDesc       scan_tupdesc = nullptr;
	TupleTableSlot *slot = nullptr;
	VecExprProgram *qual_program = nullptr;   /* interpreter-only; built from POD qual desc */
	BlockNumber     current_block = InvalidBlockNumber;
	BlockNumber     end_block = InvalidBlockNumber;
	bool            exhausted = false;
	bool            diag_first_call_logged = false;

	~SeqScanLocalState()
	{
		if (slot != nullptr)
			ExecDropSingleTupleTableSlot(slot);
		if (scan_desc != nullptr)
			table_endscan(scan_desc);
		if (rel != nullptr)
			relation_close(rel, AccessShareLock);
	}
};

class PhysicalSeqScan final : public PhysicalOperator {
public:
	PhysicalSeqScan(Oid relid,
	                dsa_pointer input_schema_dp,
	                dsa_pointer output_schema_dp,
	                dsa_pointer qual_desc_dp,
	                dsa_pointer shared_payload_dp,
	                OpDescriptor *desc = nullptr)
		: PhysicalOperator(PhysicalOperatorType::SEQ_SCAN)
		, relid_(relid)
		, input_schema_dp_(input_schema_dp)
		, output_schema_dp_(output_schema_dp)
		, qual_desc_dp_(qual_desc_dp)
		, shared_payload_dp_(shared_payload_dp)
		, desc_(desc)
	{}

	bool IsSource() const override { return true; }
	bool IsSink() const override { return false; }
	bool IsPipelineBreaker() const override { return false; }

	std::unique_ptr<GlobalSourceState> GetGlobalSourceState(ExecCtx &ctx) override;
	std::unique_ptr<LocalSourceState>  GetLocalSourceState(ExecCtx &ctx, GlobalSourceState &gstate) override;
	SourceResultType                   GetData(ExecCtx &ctx, PipelineChunk &out, OperatorSourceInput &input) override;
	int                                MaxThreads(ExecCtx &ctx) const override;

	Oid            relid() const { return relid_; }
	dsa_pointer    input_schema_dp() const { return input_schema_dp_; }
	dsa_pointer    output_schema_dp() const { return output_schema_dp_; }
	dsa_pointer    qual_desc_dp() const { return qual_desc_dp_; }
	dsa_pointer    shared_payload_dp() const { return shared_payload_dp_; }
	OpDescriptor  *desc() const { return desc_; }
	const PgVector<OpDescriptor *> &descs() const { return desc_list_; }
	void           AttachDescriptor(OpDescriptor *desc) { desc_ = desc; desc_list_.push_back(desc); }  /* see physical_hash_aggregate.hpp Fix A2 */

private:
	Oid          relid_;
	dsa_pointer  input_schema_dp_;
	dsa_pointer  output_schema_dp_;
	dsa_pointer  qual_desc_dp_;
	dsa_pointer  shared_payload_dp_;
	OpDescriptor *desc_;
	PgVector<OpDescriptor *> desc_list_;
};

}  /* namespace pipeline */
}  /* namespace pg_volvec */
