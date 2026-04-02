# LLVM JIT Deform to DataChunk in pg_volvec

Status: implemented prototype, verified on 2026-04-02

## 1. Goal

`pg_volvec` wants to avoid the generic PostgreSQL row-to-Datum-to-slot path when scanning analytical tables. The deform JIT generates machine code that reads a `HeapTupleHeader` and writes directly into the typed arrays inside a `DataChunk`.

This removes most of the overhead of:

- `heap_getattr()`
- generic `Datum` unpacking
- repeated branchy type dispatch in the hot scan loop

## 2. Current interface

The current runtime entry is tuple-at-a-time:

```c
typedef void (*JitDeformFunc)(
    HeapTupleHeader tuphdr,
    void **col_data_ptrs,
    uint8_t **col_null_ptrs,
    uint32 row_idx);
```

That means page iteration and visibility handling remain in C++, while the JIT owns the hottest part: tuple decode into column arrays.

## 3. Compilation inputs

The deform JIT is specialized using:

- `TupleDesc`
- `DeformProgram`

The `DeformProgram` matters just as much as the tuple descriptor, because the engine now prunes materialization to the columns the query actually needs. Different target subsets must not accidentally reuse the same compiled function.

## 4. Current implementation

### 4.1 Query-driven column pruning

The scan path no longer deforms a fixed leading prefix of relation attributes. It collects the needed attributes from:

- scan quals
- upper non-scan targetlists

and builds a pruned `DeformProgram`. This was one of the biggest improvements for Q6.

### 4.2 PostgreSQL-like tuple walk

The JIT tuple walk is now much closer to PostgreSQL's own deform logic:

- tuple header and row metadata are available to the generated code
- alignment is handled using byte-alignment semantics
- null bitmap checks and per-attribute availability are part of the JIT path
- varlena handling is explicit instead of relying on unsafe offset guesses

### 4.3 Type coverage that matters today

The currently verified hot path covers the scalar mix used by Q1 and Q6:

- `int32`
- `int64`
- `date32`
- `float8`
- `NUMERIC(15,2)` as scaled `int64`
- string-like grouping keys via `VecStringRef` prefix materialization

### 4.4 Provider auto-load

The deform JIT no longer assumes that `llvmjit` was manually loaded in the backend. It tries to resolve the provider entry points from the process and, if needed, loads the configured provider automatically.

This removed the old requirement to run `LOAD 'llvmjit'` before testing deform JIT.

## 5. Numeric path

The old generic numeric decode path was too expensive for TPC-H. The current deform JIT therefore includes a hot-path specialization for scale-2 numerics:

- values are decoded directly to scaled `int64`
- common TPC-H numeric forms stay out of PostgreSQL's generic numeric conversion path
- cold / unsupported shapes still have a fallback path

This was a key part of making Q6 competitive.

## 6. Interaction with page-wise scan

The scan itself is still driven from C++:

- block iteration
- visibility checks
- page offset traversal

The JIT only handles tuple decode and store. This hybrid split was deliberate:

- page/block logic is complex and easier to keep correct in C++
- tuple decode is the tight, repetitive hotspot that benefits most from JIT

## 7. Current benefits

The current deform JIT is no longer just a proof-of-concept:

- Q1 no-order and Q6 both hit the deform JIT path
- the provider loads automatically
- stringref and numeric cases are supported on the verified workloads
- column pruning prevents needless decode of unrelated columns

Together with the numeric fixed-point path, this removed earlier hotspots such as generic numeric conversion and needless string decode on Q6.

## 8. Current limitations

- the interface is still tuple-at-a-time rather than page-at-a-time
- there is no end-to-end fused scan+expr kernel yet
- some exotic PostgreSQL type / varlena shapes still depend on fallback behavior
- there is no dedicated late-materialization-aware JIT path yet

## 9. Next steps

- add even tighter fixed-layout specializations
- investigate page-level fusion where it helps without destabilizing scan logic
- explore scan+expr fusion for the hottest single-table analytical paths
