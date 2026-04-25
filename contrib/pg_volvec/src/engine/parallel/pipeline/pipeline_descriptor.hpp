#pragma once

/*
 * pipeline/pipeline_descriptor.hpp
 *
 * Cross-process IR descriptors for a single MetaPipeline bundle, plus the
 * leader-side serialize / worker-side reconstruct entry points.
 *
 * **Status: 3g.2-prep scaffolding.** This header ships the POD layout that
 * §8.5.4.2 of `docs/GLOBAL_LOCAL_STATE_DESIGN.md` (HEAD eb7901b022a) locks
 * down. Function bodies in `pipeline_descriptor.cpp` are intentionally
 * incomplete: `LeaderSerializePipelines` / `WorkerReconstructPipelines`
 * skeletons exist so the symbol resolves, but they error out for any plan
 * shape until 3g.2-final wires Translator -> Serializer -> Worker reconstruct.
 *
 * Anti-pattern compliance (pipeline/AGENTS.md + core/AGENTS.md):
 *   - No palloc'd pointer is ever stored in any *Body field. Only POD
 *     primitives, Oid, and dsa_pointer. Any pointer that crosses the
 *     leader/worker boundary is a DSA offset.
 *   - All `dsa_allocate` happens leader-side under
 *     `MemoryContextSwitchTo(per-query-mcxt)`.
 *
 * Spec: docs/GLOBAL_LOCAL_STATE_DESIGN.md §8.5.4.2 (POD layout, L1079-1162),
 * §8.5.4.3 (leader serialize, L1163-1250), §8.5.4.4 (worker reconstruct,
 * L1251-1299), §8.5.4.5 (lazy GlobalSinkState, L1300-1345),
 * §8.5.4.7 (ExprBytecode constraints, L1346-1357).
 */

extern "C" {
#include "postgres.h"
#include "storage/block.h"
#include "port/atomics.h"
#include "utils/dsa.h"
}

#include <cstdint>
#include <memory>

#include "core/memory.hpp"

namespace pg_volvec {

/*
 * Forward declarations only -- pipeline_descriptor.hpp must not pull in the
 * full operator/pipeline headers. That keeps the descriptor IR a leaf of the
 * include graph and avoids circular dependencies once 3g.2-final wires
 * physical_*.hpp to call StoreSharedPayloadOnDescriptor() on themselves.
 */
namespace pipeline {

class  PhysicalOperator;
struct Pipeline;
struct ExecCtx;
struct MetaPipelineBundle;
struct PipelineSharedControl;        /* defined in dsm_control.hpp */

/* -------------------------------------------------------------------------
 * §8.5.4.2 Operator kind tag (POD).
 * ------------------------------------------------------------------------- */
enum class OpKind : uint8_t {
	SEQ_SCAN       = 0,
	HASH_AGGREGATE = 1,
	ORDER          = 2,
	OUTPUT         = 3,
};

/* -------------------------------------------------------------------------
 * §8.5.4.2 Schema descriptor (POD with FAM).
 *
 * `columns` is a flexible-array trailer; total dsa_allocate size =
 *   offsetof(SchemaDescriptor, columns) + n_columns * sizeof(ColumnSchema).
 * ------------------------------------------------------------------------- */
struct ColumnSchema {
	Oid     type_oid;
	int16_t typlen;
	bool    typbyval;
};

struct SchemaDescriptor {
	uint16_t     n_columns;
	uint16_t     _pad0;
	uint32_t     _pad1;
	ColumnSchema columns[FLEXIBLE_ARRAY_MEMBER];
};

/* -------------------------------------------------------------------------
 * §8.5.4.7 Expression bytecode (POD; integer-index operands; by-value or
 * by-bytes-copied constants; Oid function dispatch via fmgr_info_cxt;
 * host byte order). 3g.2-prep ships the layout only; SerializeExprProgram
 * returns InvalidDsaPointer for nullptr quals and ereport(ERROR) for any
 * non-null qual until 3g.2-final/M-Q1-PERF lowers real expressions.
 * ------------------------------------------------------------------------- */
struct ExprBytecode {
	uint32_t n_insns;
	uint32_t n_consts;
	uint32_t const_pool_bytes;
	uint32_t _pad0;
	/* trailing layout (deferred to 3g.2-final implementation):
	 *   Insn      insns[n_insns];
	 *   ConstSlot consts[n_consts];
	 *   uint8_t   const_pool[const_pool_bytes];
	 */
};

/* -------------------------------------------------------------------------
 * §6.3 + §8.5.4.5 Per-operator shared payload PODs.
 *
 * These structures are dsa_allocated by the leader and addressed via
 * dsa_pointer fields on the corresponding *OpBody. They are NOT owned by
 * any C++ class; the operator's GlobalSinkState/GlobalSourceState C++ object
 * holds a *view* via dsa_get_address() on every method call.
 * ------------------------------------------------------------------------- */
struct SeqScanSharedPayload {
	pg_atomic_uint64 next_block;
	BlockNumber      total_blocks;
	uint32_t         morsel_nblocks;
};

/*
 * AggSharedPayload / SortSharedPayload bodies are deferred to 3g.2-final.
 * 3g.2-prep ships forward declarations so descriptor consumers can hold
 * dsa_pointer fields typed by intent. Sizing/dsa_allocate parameters are
 * known by the leader factory at runtime, not at this header level.
 */
struct AggSharedPayload;
struct SortSharedPayload;

/* -------------------------------------------------------------------------
 * §8.5.4.2 L1133 Aggregate function descriptor (POD per-agg).
 * ------------------------------------------------------------------------- */
struct AggFuncDesc {
	Oid      agg_oid;
	Oid      transtype;
	Oid      finaltype;
	uint16_t input_col_idx;
	uint16_t _pad;
};

/* -------------------------------------------------------------------------
 * §8.5.4.2 L1141 Sort key descriptor (POD per-key).
 * ------------------------------------------------------------------------- */
struct SortKeyDesc {
	Oid      collation_oid;
	uint16_t col_idx;
	bool     asc;
	bool     nulls_first;
	uint32_t _pad;
};

/* -------------------------------------------------------------------------
 * §8.5.4.2 L1090-1162 Per-OpKind body PODs. Each is the payload of the
 * tagged-union slot inside OpDescriptor.body.
 *
 * Sizing rule: every *OpBody is fixed-size (no FAM). Variable-length data
 * (input/output schemas, group key indices, agg/sort descriptors) is held
 * via *separate* dsa_pointer allocations or by inlining in a sibling FAM
 * struct (HashAgg/Order use the latter, see comments at struct site).
 * ------------------------------------------------------------------------- */
struct SeqScanOpBody {
	Oid         relid;
	dsa_pointer input_schema;        /* SchemaDescriptor */
	dsa_pointer output_schema;       /* SchemaDescriptor (may equal input_schema) */
	dsa_pointer qual_bytecode;       /* ExprBytecode, or InvalidDsaPointer */
	dsa_pointer shared_payload;      /* SeqScanSharedPayload */
};

struct HashAggOpBody {
	dsa_pointer input_schema;        /* SchemaDescriptor */
	dsa_pointer output_schema;       /* SchemaDescriptor */
	dsa_pointer group_keys;          /* uint16_t[n_group_keys] */
	dsa_pointer agg_funcs;           /* AggFuncDesc[n_agg_funcs] */
	dsa_pointer shared_payload;      /* AggSharedPayload (lazy: see §8.5.4.5) */
	uint16_t    n_group_keys;
	uint16_t    n_agg_funcs;
	uint32_t    _pad;
};

struct OrderOpBody {
	dsa_pointer input_schema;        /* SchemaDescriptor */
	dsa_pointer sort_keys;           /* SortKeyDesc[n_sort_keys] */
	dsa_pointer shared_payload;      /* SortSharedPayload; may be Invalid (MaxThreads=1) */
	uint16_t    n_sort_keys;
	uint16_t    _pad0;
	uint32_t    _pad1;
};

struct OutputOpBody {
	dsa_pointer input_schema;        /* SchemaDescriptor */
};

/* -------------------------------------------------------------------------
 * §8.5.4.2 L1145-1162 OpDescriptor: tagged union of operator bodies plus
 * fixed-fanout child indexing (OpDescriptor[]-relative inside the parent
 * PipelineDescriptor).
 *
 * `n_children` is the populated count; only `child_indices[0..n_children)`
 * is meaningful. 4-slot inline cap matches the maximum fanout of the four
 * locked operator types (HashAgg/Order are dual Sink+Source, so they
 * appear in two pipelines but each pipeline references the operator via
 * a single child index).
 * ------------------------------------------------------------------------- */
struct OpDescriptor {
	OpKind   kind;
	uint8_t  n_children;
	uint16_t _pad0;
	uint32_t child_indices[4];

	union OpBodyUnion {
		SeqScanOpBody seq_scan;
		HashAggOpBody hash_agg;
		OrderOpBody   order;
		OutputOpBody  output;
	} body;
};

/* -------------------------------------------------------------------------
 * §8.5.4.2 PipelineDescriptor: one entry per pipeline inside a MetaPipeline
 * bundle. `ops` is a dsa_pointer to OpDescriptor[op_count].
 *
 * `global_source_state` / `global_sink_state` start at InvalidDsaPointer at
 * serialize time; they are lazily populated by the leader's first
 * `Sink::GetGlobalSinkState(ExecCtx&)` / `Source::GetGlobalSourceState(...)`
 * call (the one branch where `worker_index == LEADER_WORKER_INDEX`) via
 * StoreSharedPayloadOnDescriptor(). Workers attach via
 * LoadSharedPayloadFromDescriptor().
 *
 * `task_slot_next` is a per-pipeline atomic morsel cursor used by the
 * scheduler to hand out PipelineTask slots round-robin.
 * ------------------------------------------------------------------------- */
struct PipelineDescriptor {
	int32              pipeline_id;
	int32              op_count;
	uint64             dependency_mask;     /* bit i set => depends on pipeline_id i */
	dsa_pointer        ops;                  /* OpDescriptor[op_count] */
	dsa_pointer        global_source_state;  /* lazy; set by leader on first Get */
	dsa_pointer        global_sink_state;    /* lazy; set by leader on first Get */
	pg_atomic_uint32   task_slot_next;
	uint32_t           _pad;
};

/* -------------------------------------------------------------------------
 * §8.5.4.5 Helper API for lazy GlobalSinkState publication.
 *
 * Each PhysicalOperator instance carries a raw `OpDescriptor *desc_` field
 * (added in 3g.2-final when ctors are wired), populated when the worker
 * placement-news the operator in §8.5.4.4 L1264. These two helpers translate
 * between that pointer and the descriptor's `global_sink_state` slot.
 *
 * 3g.2-prep ships declarations only; bodies land in 3g.2-final next to the
 * GetGlobalSinkState/GetGlobalSourceState factories that call them.
 * ------------------------------------------------------------------------- */
void        StoreSharedPayloadOnDescriptor(const PhysicalOperator *op, dsa_pointer dp);
dsa_pointer LoadSharedPayloadFromDescriptor(const PhysicalOperator *op);

/* -------------------------------------------------------------------------
 * §8.5.4.3 Leader entry point.
 *
 * Walks `bundle` (one MetaPipeline tree, post-Translate) and emits a single
 * dsa-rooted PipelineDescriptor[] block. Returns the dsa_pointer that the
 * leader installs into PipelineSharedControl.pipelines_root.
 *
 * 3g.2-prep status: skeleton that errors out for any input. The full
 * implementation lands in 3g.2-final alongside Translator + physical_*
 * ctor wiring.
 * ------------------------------------------------------------------------- */
dsa_pointer LeaderSerializePipelines(MetaPipelineBundle &bundle, dsa_area *dsa);

/* -------------------------------------------------------------------------
 * §8.5.4.4 Worker entry point.
 *
 * Reads `ctl->pipelines_root` from the attached DSA, placement-news every
 * PhysicalOperator into `worker_ctx.mcxt`, builds the C++ Pipeline objects,
 * and appends them to `out` in pipeline_id order.
 *
 * 3g.2-prep status: skeleton that errors out for any input. Worker bootstrap
 * is the second half of 3g.2-final.
 * ------------------------------------------------------------------------- */
void WorkerReconstructPipelines(PipelineSharedControl                 *ctl,
                                ExecCtx                               &worker_ctx,
                                PgVector<std::unique_ptr<Pipeline>>   &out);

}  /* namespace pipeline */
}  /* namespace pg_volvec */
