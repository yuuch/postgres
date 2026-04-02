# Page-wise Scan in pg_volvec

Status: partially implemented, current production path documented on 2026-04-02

## 1. Why this exists

A row-at-a-time scan path leaves too much performance on the floor for OLAP-style queries. `pg_volvec` therefore pushes scan control closer to the heap page level:

- batch rows into `DataChunk`
- reduce per-tuple executor overhead
- keep the tuple decode loop hot and regular enough for JIT

## 2. What is implemented today

The current scan path is hybrid:

- C++ owns block/page traversal and visibility checks
- deform JIT owns tuple-to-column materialization

This is implemented in `VecSeqScanState`.

Key properties of the current path:

- the scan drives block iteration itself instead of relying entirely on `heap_getnext()`
- the code avoids synchronized-scan start-block surprises that would otherwise skip the relation prefix when using the custom loop
- visible tuples are appended directly into the active `DataChunk`

## 3. Why not JIT the whole page loop yet

There is a tempting end-state where one LLVM function walks the page, checks visibility, decodes the tuple, evaluates the predicate, and updates the aggregate. That is not what the current code does.

The split remains:

- page and buffer control in C++
- tuple decode in LLVM
- expression and aggregate logic in separate vector operators

This keeps correctness risks manageable while still JITing the part that most clearly pays off today.

## 4. What changed recently

The scan path used to be more fragile around start-block handling. The current code explicitly avoids the synchronized-scan wraparound problem, which is important when the executor itself drives block iteration.

This mattered because a custom page loop that starts in the middle of the relation and never wraps would silently skip early blocks.

## 5. Relationship to column pruning

Page-wise scan is much more valuable once combined with query-driven column pruning:

- the scan still sees every visible tuple
- but it only materializes the columns the rest of the plan actually uses

That combination was one of the key reasons Q6 stopped wasting time decoding irrelevant columns.

## 6. Current limitations

- visibility still uses PostgreSQL-side checks, not a JITed visibility kernel
- there is no page-level deform JIT entry point yet
- there is no scan+filter+agg fused kernel yet
- the implementation still targets a narrow single-table analytical shape

## 7. Next useful step

The next worthwhile move is not “JIT every scan detail immediately”. It is to keep the current page-wise control flow and selectively specialize the hottest cases further:

- fixed-layout pages / tuples
- no-null kernels
- fusion with the current expression JIT or aggregate update loop
