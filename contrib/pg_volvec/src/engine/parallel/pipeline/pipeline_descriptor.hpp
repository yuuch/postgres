#pragma once

/*
 * pipeline/pipeline_descriptor.hpp
 *
 * Cross-process IR descriptors for a single MetaPipeline bundle, plus the
 * leader-side serialize / worker-side reconstruct entry points.
 *
 * This header ships the POD layout that §8.5.4.2 of
 * `docs/GLOBAL_LOCAL_STATE_DESIGN.md` (HEAD eb7901b022a) locks down, plus the
 * leader-side serialize / worker-side reconstruct entry points used by the
 * MetaPipeline runtime cut-over.
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
#include <memory>  // IWYU pragma: keep

#include "core/memory.hpp"  // IWYU pragma: keep
#include "parallel/pipeline/tuple_data_layout.hpp"

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
	PROJECTION     = 4,
};

/* -------------------------------------------------------------------------
 * §8.5.4.2 Schema descriptor (POD with FAM).
 *
 * `columns` is a flexible-array trailer; total dsa_allocate size =
 *   offsetof(SchemaDescriptor, columns) + n_columns * sizeof(ColumnSchema).
 *
 * 3g.2-final Step 7 contract additions (descriptor-driven SeqScan projection):
 *   - `src_attno` is the 1-based heap attno that PhysicalSeqScan reads via
 *     heap_getattr(tuple, src_attno, tupdesc, &isnull). It is meaningful
 *     ONLY for SeqScan output_schema. For HashAgg / Order / Output schemas
 *     the translator MUST set src_attno = 0 and consumers MUST NOT read it.
 *   - `chunk_slot` is the per-storage-type DataChunk column index in
 *     [0, 16). DataChunk has int32_columns[16], int64_columns[16],
 *     double_columns[16], string_columns[16], nulls[16] (see
 *     core/data_chunk.hpp lines 66-69). Two columns of different storage
 *     kinds may legitimately share the same chunk_slot (one in int32,
 *     another in int64). Translator owns the assignment.
 *   - `decode_kind` selects how SeqScan converts a heap Datum to the
 *     scaled DataChunk slot. CHAR -> int32; NUMERIC(15,2) -> scaled int64
 *     via DirectFunctionCall; DATE -> int32 (DateADT is int32); etc.
 * ------------------------------------------------------------------------- */
enum class ColumnDecodeKind : uint8_t {
	NONE                 = 0,   /* not produced by SeqScan; consumer-defined */
	INT32_CHAR           = 1,   /* DatumGetChar -> int32_columns */
	INT32_DATE           = 2,   /* DatumGetDateADT -> int32_columns */
	INT32_INT4           = 3,   /* DatumGetInt32 -> int32_columns */
	INT64_INT8           = 4,   /* DatumGetInt64 -> int64_columns */
	INT64_NUMERIC_SCALED = 5,   /* numeric * 100 -> int64_columns */
	DOUBLE_FLOAT8        = 6,   /* DatumGetFloat8 -> double_columns */
};

struct ColumnSchema {
	Oid              type_oid;
	int16_t          typlen;
	bool             typbyval;
	uint8_t          chunk_slot;     /* per-storage-type DataChunk slot [0, 16) */
	int16_t          src_attno;      /* 1-based heap attno; 0 if not from SeqScan */
	ColumnDecodeKind decode_kind;    /* how SeqScan converts Datum -> chunk slot */
	uint8_t          _pad0;
};

struct SchemaDescriptor {
	uint16_t     n_columns;
	uint16_t     _pad0;
	uint32_t     _pad1;
	ColumnSchema columns[FLEXIBLE_ARRAY_MEMBER];
};

/* -------------------------------------------------------------------------
 * Generic single-clause qual descriptor for SeqScan.
 *
 * v1 supports a single column-op-const clause with a by-value Datum
 * constant (matches §8.5.4.7 ExprBytecode constraint forbidding by-ref
 * consts). Conjunctive multi-clause quals and by-ref consts require
 * ExprBytecode lowering (post-M-Q1-PERF).
 *
 * `col_attno` is a 1-based heap attno read via heap_getattr against the
 * scan tupdesc; quals run BEFORE projection so this is NOT a chunk_slot.
 *
 * `const_value` holds by-value Datum bytes only. By-ref consts are
 * forbidden in v1.
 * ------------------------------------------------------------------------- */
enum class QualKind : uint8_t {
	NONE         = 0,   /* identically true */
	COL_OP_CONST = 1,
};

enum class QualOp : uint8_t {
	LE = 0,
	LT = 1,
	EQ = 2,
	GE = 3,
	GT = 4,
	NE = 5,
};

struct QualDescriptor {
	QualKind kind;
	QualOp   op;
	uint16_t col_attno;       /* 1-based heap attno (not chunk_slot) */
	uint32_t _pad0;
	Oid      const_typoid;    /* dispatch on this for typed comparison */
	uint64_t const_value;     /* by-value Datum bytes; by-ref forbidden in v1 */
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
 * Projection expression descriptor (POD).
 *
 * PhysicalProjection evaluates a small int64 opcode tape over DataChunk
 * chunk_slot inputs. Constants are pre-scaled integer payloads. The runtime
 * stays interpreter-only for now.
 * ------------------------------------------------------------------------- */
enum class ProjectOp : uint8_t {
	NUMERIC_MUL_VAR_VAR   = 0,
	NUMERIC_MUL_VAR_CONST = 1,
	NUMERIC_SUB_CONST_VAR = 2,
	NUMERIC_ADD_CONST_VAR = 3,
	COPY_VAR              = 4,
};

struct ProjectStep {
	ProjectOp op;
	uint8_t   in_a_chunk_slot;
	uint8_t   in_b_chunk_slot;
	uint8_t   out_chunk_slot;
	int64_t   const_value;
};

struct ProjectExprDesc {
	uint16_t first_step_idx;
	uint16_t n_steps;
	uint8_t  output_chunk_slot;
	int8_t   output_scale;
	uint16_t _pad0;
};

struct ProjectOpBody {
	dsa_pointer input_schema;
	dsa_pointer output_schema;
	dsa_pointer expr_descs;
	dsa_pointer steps;
	uint16_t    n_exprs;
	uint16_t    n_steps_total;
	uint32_t    _pad0;
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
};

/*
 * 3g.2-final step 5 removes the Q1-specific payload aliases entirely. Shared
 * row storage is now described by TupleDataLayout and materialized either as a
 * TupleDataCollection (Order) or an AggregateHashTable over a TDC (HashAgg).
 */

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
	dsa_pointer layout;              /* TupleDataLayout serialized from PG Agg plan */
	dsa_pointer shared_payload;      /* AggregateHashTable (lazy: see §8.5.4.5) */
	uint16_t    n_group_keys;
	uint16_t    n_agg_funcs;
	uint32_t    max_groups;
};

struct OrderOpBody {
	dsa_pointer input_schema;        /* SchemaDescriptor */
	dsa_pointer sort_keys;           /* SortKeyDesc[n_sort_keys] */
	dsa_pointer key_layout;          /* TupleDataLayout for sort-key row storage */
	dsa_pointer payload_layout;      /* TupleDataLayout for full payload row storage */
	dsa_pointer shared_payload;      /* TupleDataCollection; may be Invalid until leader attach */
	dsa_pointer sort_indices;        /* OrderSortIndices; populated in Finalize, consumed in GetData */
	uint16_t    n_sort_keys;
	uint16_t    _pad0;
	uint32_t    max_rows;
};

struct OutputOpBody {
	dsa_pointer input_schema;        /* SchemaDescriptor */
	dsa_pointer layout;              /* TupleDataLayout serialized from input_schema */
	dsa_pointer shared_payload;      /* TupleDataCollection; lazy-init on first GetGlobalSinkState */
	uint32_t    tdc_max_rows;        /* row capacity bound (clamp(plan_rows*1.5, 1024, 1<<20)) */
	uint32_t    _pad0;
};

/* -------------------------------------------------------------------------
 * §8.5.4.2 L1145-1162 OpDescriptor: tagged union of operator bodies plus
 * fixed-fanout child indexing (OpDescriptor[]-relative inside the parent
 * PipelineDescriptor).
 *
 * `n_children` is the populated count; only `child_indices[0..n_children)`
	 * is meaningful. 4-slot inline cap matches the maximum fanout of the five
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
		ProjectOpBody project;
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

/*
 * Step 5 contract delta: HashAggregate / Order descriptor bodies now publish
 * serialized TupleDataLayout PODs so workers reconstruct the exact row codec
 * the leader derived from the real PostgreSQL plan tree.
 */
dsa_pointer     SerializeTupleDataLayout(const TupleDataLayout &layout, dsa_area *dsa);

/* -------------------------------------------------------------------------
 * §8.5.4.3 Leader entry point.
 *
 * Walks `bundle` (one MetaPipeline tree, post-Translate) and emits a single
 * dsa-rooted PipelineDescriptor[] block. Returns the dsa_pointer that the
 * leader installs into PipelineSharedControl.pipelines_root.
 *
 * Serializes one MetaPipeline bundle into a DSA-rooted PipelineDescriptor[]
 * block for publication through PipelineSharedControl.pipelines_root.
 * ------------------------------------------------------------------------- */
dsa_pointer LeaderSerializePipelines(MetaPipelineBundle &bundle, dsa_area *dsa);

/* -------------------------------------------------------------------------
 * §8.5.4.4 Worker entry point.
 *
 * Reads `ctl->pipelines_root` from the attached DSA, placement-news every
 * PhysicalOperator into `worker_ctx.mcxt`, builds the C++ Pipeline objects,
 * and appends them to `out` in pipeline_id order.
 *
 * Reconstructs per-process Pipeline / PhysicalOperator objects from the
 * attached descriptor IR and appends them to `out` in pipeline_id order.
 * ------------------------------------------------------------------------- */
void WorkerReconstructPipelines(PipelineSharedControl                 *ctl,
                                ExecCtx                               &worker_ctx,
                                PgVector<std::unique_ptr<Pipeline>>   &out);

}  /* namespace pipeline */
}  /* namespace pg_volvec */
