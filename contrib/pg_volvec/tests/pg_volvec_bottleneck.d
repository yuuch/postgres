#!/usr/sbin/dtrace -s
/*
 * pg_volvec_bottleneck.d - Profile pg_volvec executor hotspots
 *
 * Captures three orthogonal views of where time goes inside pg_volvec:
 *   1) On-CPU stack profile (every 997 Hz) for the target backend, leaf
 *      frames within pg_volvec.dylib only -> identifies hot leaves.
 *   2) Per-function entry counts on the major Source/Sink/Combine APIs
 *      -> identifies call-frequency outliers (e.g. tiny chunks).
 *   3) Per-function elapsed-time histograms (quantize) on the same APIs
 *      -> identifies tail-latency offenders vs. mean offenders.
 *
 * Usage (run with sudo on macOS; SIP must allow DTrace on Apple Silicon
 * via `csrutil enable --without dtrace`):
 *
 *   sudo dtrace -p <BACKEND_PID> \
 *     -s contrib/pg_volvec/tests/pg_volvec_bottleneck.d \
 *     -o /tmp/pg_volvec_bottleneck.out
 *
 * BACKEND_PID = the leader backend PID (from psql `SELECT pg_backend_pid();`).
 * Stop with Ctrl-C; aggregations print on exit.
 *
 * Why pid$target and not the syscall provider:
 *   pg_volvec spends most of its time inside user-space C++ (DataChunk
 *   loops, TupleData scatter/gather, hash table probe, JIT'd expression
 *   row loops); syscall-level views miss all of it. The pid provider
 *   reaches into pg_volvec.dylib symbols directly.
 */

#pragma D option quiet
#pragma D option bufsize=64m
#pragma D option aggsize=16m
#pragma D option dynvarsize=32m

dtrace:::BEGIN
{
    printf("pg_volvec bottleneck profile — pid=%d\n", $target);
    printf("Press Ctrl-C to stop; results print on exit.\n\n");
    start_ts = timestamp;
}

/*
 * 1) On-CPU stack sample @ 997 Hz, restricted to the target pid.
 *    ustack(8) keeps frames cheap; we filter symbols in post-processing.
 */
profile-997
/pid == $target/
{
    @oncpu_stacks[ustack(8)] = count();
}

/*
 * 2) Per-function entry counts on the major pipeline ops.
 *    Wildcards match the mangled C++ names; one entry probe per
 *    Source.GetData / Sink.SinkChunk / Combine / agg helpers.
 */
pid$target:pg_volvec.dylib::entry
/probefunc != ""/
{
    @entries[probefunc] = count();
}

/*
 * 3) Per-call elapsed-time histograms on the hot APIs.
 *    self->ts is per-thread; quantize() yields a power-of-2 ns histogram.
 */
pid$target:pg_volvec.dylib:*PhysicalSeqScan*GetData*:entry,
pid$target:pg_volvec.dylib:*PhysicalHashAggregate*SinkChunk*:entry,
pid$target:pg_volvec.dylib:*PhysicalHashAggregate*Combine*:entry,
pid$target:pg_volvec.dylib:*PhysicalHashAggregate*GetData*:entry,
pid$target:pg_volvec.dylib:*PhysicalOrder*SinkChunk*:entry,
pid$target:pg_volvec.dylib:*PhysicalOrder*GetData*:entry,
pid$target:pg_volvec.dylib:*OutputSink*SinkChunk*:entry,
pid$target:pg_volvec.dylib:*OutputSink*EmitGlobalTdcToDest*:entry,
pid$target:pg_volvec.dylib:*AggregateHashTableFindOrInsert*:entry,
pid$target:pg_volvec.dylib:*AggregateHashTableCombineRow*:entry,
pid$target:pg_volvec.dylib:*UpdateAggregates*:entry,
pid$target:pg_volvec.dylib:*CombineAggregates*:entry,
pid$target:pg_volvec.dylib:*pg_volvec*Scatter*:entry,
pid$target:pg_volvec.dylib:*pg_volvec*Gather*:entry,
pid$target:pg_volvec.dylib:*DsmTaskQueue*TryPop*:entry
{
    self->ts[probefunc] = timestamp;
}

pid$target:pg_volvec.dylib:*PhysicalSeqScan*GetData*:return,
pid$target:pg_volvec.dylib:*PhysicalHashAggregate*SinkChunk*:return,
pid$target:pg_volvec.dylib:*PhysicalHashAggregate*Combine*:return,
pid$target:pg_volvec.dylib:*PhysicalHashAggregate*GetData*:return,
pid$target:pg_volvec.dylib:*PhysicalOrder*SinkChunk*:return,
pid$target:pg_volvec.dylib:*PhysicalOrder*GetData*:return,
pid$target:pg_volvec.dylib:*OutputSink*SinkChunk*:return,
pid$target:pg_volvec.dylib:*OutputSink*EmitGlobalTdcToDest*:return,
pid$target:pg_volvec.dylib:*AggregateHashTableFindOrInsert*:return,
pid$target:pg_volvec.dylib:*AggregateHashTableCombineRow*:return,
pid$target:pg_volvec.dylib:*UpdateAggregates*:return,
pid$target:pg_volvec.dylib:*CombineAggregates*:return,
pid$target:pg_volvec.dylib:*pg_volvec*Scatter*:return,
pid$target:pg_volvec.dylib:*pg_volvec*Gather*:return,
pid$target:pg_volvec.dylib:*DsmTaskQueue*TryPop*:return
/self->ts[probefunc]/
{
    @latency_ns[probefunc] = quantize(timestamp - self->ts[probefunc]);
    @latency_sum[probefunc] = sum(timestamp - self->ts[probefunc]);
    self->ts[probefunc] = 0;
}

dtrace:::END
{
    printf("\n=== Wall-clock window ===\n");
    printf("elapsed_ns = %d\n\n", timestamp - start_ts);

    printf("=== TOP 20 on-CPU stacks (target pid) ===\n");
    trunc(@oncpu_stacks, 20);
    printa(@oncpu_stacks);

    printf("\n=== Top entry counts (pg_volvec.dylib symbols) ===\n");
    trunc(@entries, 40);
    printa("%-90s %@d\n", @entries);

    printf("\n=== Total ns per hot function (sum) ===\n");
    printa("%-90s %@d ns\n", @latency_sum);

    printf("\n=== Per-call latency histograms (ns) ===\n");
    printa(@latency_ns);
}
