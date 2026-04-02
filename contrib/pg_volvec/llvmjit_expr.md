# LLVM JIT Expression Evaluation in pg_volvec

Status: implemented prototype, verified on 2026-04-02

## 1. What problem this solves

The original `VecExprProgram` execution model in `pg_volvec` is a vector interpreter:

- lower PostgreSQL `Expr` trees into linear `VecExprStep` programs
- execute each step in order
- store intermediate results in `registers_i32/i64/f8/nulls`

That model is simple and correct, but it materializes temporary vectors. For an expression like:

```text
(a - b) * c
```

the interpreter effectively does:

```text
tmp[i] = a[i] - b[i]
res[i] = tmp[i] * c[i]
```

The JIT goal is to avoid that intermediate materialization and instead emit a fused row loop closer to:

```text
res[i] = (a[i] - b[i]) * c[i]
```

## 2. Current architecture

The JIT does not replace the step IR. The IR is still useful because:

- it is a compact lowering target from PostgreSQL `Expr`
- it lets the interpreter and JIT share the same semantic layer
- it is already close to an SSA-style dataflow graph

So the current pipeline is:

```text
PostgreSQL Expr
    -> VecExprStep linear IR
    -> either interpreter or LLVM lowering
```

## 3. What is implemented today

### 3.1 JIT compilation is now actually wired in

`CompileExpr()` now calls `try_compile_jit()` after building a valid program. If JIT lowering succeeds, runtime evaluation uses the compiled function. If it fails, the engine falls back to the interpreter.

This means expression JIT is no longer just “code in the tree”; it is part of the real execution path.

### 3.2 Two execution kernels

The current LLVM path emits two loop shapes:

- `dense/no-selection` fast path
- `selected` path for batches with an active selection vector

This avoids forcing the hottest path to pay a per-row `has_sel ? sel[i] : i` penalty.

### 3.3 Fused row evaluation

For supported programs, intermediate values are emitted as LLVM SSA values rather than `tmp[]` arrays. The final result is written once per active row.

That is the key memory-traffic win compared to the interpreter.

### 3.4 Numeric scale semantics

The JIT path now matches the interpreter more closely for fixed-point numerics:

- add/sub compare values at a common scale
- multiply uses widened arithmetic before truncation back to `int64`
- comparisons rescale both inputs to the same comparison scale

This keeps the JIT path aligned with the engine's scaled-integer numeric model.

## 4. Actual function interface

The JIT function still operates over `DataChunk` column arrays rather than over heap tuples directly:

```c
typedef void (*VecExprJitFunc)(
    uint32_t count,
    double **col_f8,
    int64_t **col_i64,
    int32_t **col_i32,
    uint8_t **col_nulls,
    double *res_f8,
    int64_t *res_i64,
    int32_t *res_i32,
    uint8_t *res_nulls,
    uint16_t *sel,
    bool has_sel);
```

This keeps expression JIT focused on columnar arithmetic and predicate evaluation. Tuple decode remains the job of deform / scan.

## 5. What the current JIT is good at

- arithmetic-heavy scalar expressions
- filter predicates used by Q6
- aggregate argument expressions used by Q1 / Q6
- dense row loops where the selection vector is absent

The recent flame graph for Q6 showed `VecExprProgram::evaluate()` dropping to about `1%` of sampled stacks, which is the main signal that the fused JIT path is doing its job.

## 6. What it is not yet doing

### 6.1 No dedicated no-null kernel

Null propagation is still present in the generated loop. There is not yet a dedicated “known non-null” specialization.

### 6.2 No aggregate fusion

The engine still evaluates the expression into a result buffer and then consumes that buffer in aggregation. The next natural step is to fuse:

```text
acc += expr(...)
```

directly into the aggregate update loop.

### 6.3 No explicit SIMD kernel generation

Today the JIT relies on LLVM's normal optimization pipeline. The dense path is much more vectorizer-friendly than before, but the code does not yet emit explicit SIMD kernels or attach custom vectorization metadata.

## 7. Why the current approach is preferable to nested helper calls

The JIT does not try to generate runtime shapes like:

```text
mul(c, sub(a, b))
```

as helper-call trees. Instead it lowers the step IR to straight-line SSA operations in the row loop. That is a better fit for LLVM because:

- it avoids extra call overhead
- it keeps intermediate values in virtual registers
- it is easier for LLVM to optimize and potentially vectorize

## 8. Next steps

- add `dense + no-null` expression kernels
- fuse aggregate argument evaluation with aggregate update
- improve runtime logging so JIT success / fallback is more visible
- broaden supported opcodes without regressing the current Q1 / Q6 path
