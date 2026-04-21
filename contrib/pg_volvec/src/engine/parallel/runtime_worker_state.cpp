#include "parallel/parallel_runtime_internal.hpp"

namespace pg_volvec {

void
CleanupLocalParallelAggregateProcessState(LocalParallelAggregateProcessState *local)
{
	if (local == nullptr)
		return;
	if (local->root_plan != nullptr)
	{
		delete local->root_plan;
		local->root_plan = nullptr;
	}
	if (local->estate != nullptr)
	{
		if (local->estate->es_snapshot != InvalidSnapshot)
			UnregisterSnapshot(local->estate->es_snapshot);
		FreeExecutorState(local->estate);
		local->estate = nullptr;
	}
	if (local->memory_context != nullptr)
	{
		MemoryContextDelete(local->memory_context);
		local->memory_context = nullptr;
	}
	*local = LocalParallelAggregateProcessState{};
}

void
CleanupLocalParallelAggregateProcessStateOnExit(int code, Datum arg)
{
	LocalParallelAggregateProcessState *local;
#ifdef USE_LLVM
	size_t orphaned_jit_contexts = 0;
#endif

	(void) code;
	local = (LocalParallelAggregateProcessState *) DatumGetPointer(arg);
	if (local == nullptr)
		return;

	/*
	 * Avoid full executor/scan teardown during before_shmem_exit. At that
	 * point PostgreSQL may already be unwinding AIO/read_stream state, and a
	 * full root_plan delete can recurse into heap_endscan()/read_stream
	 * teardown and raise a second FATAL. We only need JIT context accounting
	 * to reach zero before llvm_shutdown().
	 */
	if (local->root_plan != nullptr)
	{
		if (pg_volvec_trace_hooks)
			elog(LOG,
				 "pg_volvec: proc-exit cleanup releasing worker JIT resources pid=%d root_plan=%p",
				 MyProcPid,
				 (void *) local->root_plan);
		local->root_plan->release_jit_resources_for_proc_exit();
		local->root_plan = nullptr;
		local->worker_context.root_plan = nullptr;
		local->worker_context.agg_state = nullptr;
		local->worker_context.hash_join_state = nullptr;
		if (pg_volvec_trace_hooks)
			elog(LOG,
				 "pg_volvec: proc-exit cleanup finished releasing worker JIT resources pid=%d",
				 MyProcPid);
	}
	else if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: proc-exit cleanup saw no root plan pid=%d",
			 MyProcPid);

#ifdef USE_LLVM
	orphaned_jit_contexts =
		pg_volvec_release_all_registered_llvm_jit_contexts_for_proc_exit();
	if (pg_volvec_trace_hooks && orphaned_jit_contexts > 0)
		elog(LOG,
			 "pg_volvec: proc-exit cleanup released %zu orphaned JIT context(s) pid=%d",
			 orphaned_jit_contexts,
			 MyProcPid);
#endif
}

bool
TryInitializeLocalParallelAggregateProcessState(const char *plannedstmt_serialized,
												 const char *query_text,
												 int agg_plan_node_id,
												 int hash_join_plan_node_id,
												 int input_hash_join_plan_node_id,
												 bool require_agg_state,
												 bool need_hash_join_state,
												 const uint8_t *shared_hash_bridge,
												 size_t shared_hash_bridge_size,
												 Oid source_scan_relid,
												 int source_scan_plan_node_id,
												 ParallelTableScanDesc parallel_scan_desc,
												 bool leader,
	LocalParallelAggregateProcessState *local,
												 const char **failure_reason,
												 const uint8_t *serialized_param_exec,
												 size_t serialized_param_exec_size)
{
	MemoryContext worker_context;
	MemoryContext old_context;
	std::unique_ptr<VecPlanState> root_plan;
	Plan *local_init_plan;
	VecAggState *agg_state;
	instr_time init_start;
	instr_time init_end;

	if (failure_reason != nullptr)
		*failure_reason = nullptr;
	if (local != nullptr)
		*local = LocalParallelAggregateProcessState{};
	if (plannedstmt_serialized == nullptr || local == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel worker missing serialized planned statement";
		return false;
	}

	INSTR_TIME_SET_CURRENT(init_start);
	worker_context = AllocSetContextCreate(CurrentMemoryContext,
										   "pg_volvec parallel worker",
										   ALLOCSET_DEFAULT_SIZES);
	old_context = MemoryContextSwitchTo(worker_context);
	local->memory_context = worker_context;
	local->plannedstmt = (PlannedStmt *) stringToNode(plannedstmt_serialized);
	local->query_text = pstrdup(query_text != nullptr ? query_text : "");
	local->estate = CreateExecutorState();
	if (local->plannedstmt->paramExecTypes != NIL)
	{
		int n_param_exec = list_length(local->plannedstmt->paramExecTypes);

		local->estate->es_param_exec_vals =
			(ParamExecData *) palloc0_array(ParamExecData, n_param_exec);
	}
	if (serialized_param_exec != nullptr &&
		serialized_param_exec_size >= sizeof(ParallelParamExecHeader) &&
		local->estate->es_param_exec_vals != nullptr)
	{
		const ParallelParamExecHeader *header =
			(const ParallelParamExecHeader *) serialized_param_exec;
		const ParallelParamExecEntry *entries =
			(const ParallelParamExecEntry *) (serialized_param_exec + sizeof(*header));
		const char *data_base =
			(const char *) (serialized_param_exec + sizeof(*header) +
							(size_t) header->count * sizeof(ParallelParamExecEntry));

		if (header->magic == 0x56565045 &&
			header->count <= (uint32) list_length(local->plannedstmt->paramExecTypes) &&
			header->bytes <= serialized_param_exec_size)
		{
			for (uint32 i = 0; i < header->count; i++)
			{
				const ParallelParamExecEntry *entry = &entries[i];
				ParamExecData *prm = &local->estate->es_param_exec_vals[i];
				char *cursor;
				bool isnull = true;

				if (!entry->valid ||
					entry->offset > header->bytes ||
					entry->size > header->bytes - entry->offset)
					continue;
				cursor = (char *) data_base + entry->offset;
				prm->value = datumRestore(&cursor, &isnull);
				prm->isnull = isnull;
				prm->execPlan = nullptr;
				if (pg_volvec_trace_hooks)
					elog(LOG,
						 "pg_volvec: restored PARAM_EXEC %u type=%u isnull=%s bytes=%u",
						 i,
						 entry->type,
						 prm->isnull ? "true" : "false",
						 entry->size);
			}
		}
	}
	local->estate->es_snapshot = RegisterSnapshot(GetActiveSnapshot());
	local->estate->es_sourceText = local->query_text;
	local->estate->es_plannedstmt = local->plannedstmt;
	ExecInitRangeTable(local->estate,
					   local->plannedstmt->rtable,
					   local->plannedstmt->permInfos,
					   local->plannedstmt->unprunableRelids);

	local->worker_context.memory_context = worker_context;
	local->worker_context.plannedstmt = local->plannedstmt;
	local->worker_context.estate = local->estate;
	local->worker_context.agg_plan_node_id = agg_plan_node_id;
	local->worker_context.hash_join_plan_node_id = hash_join_plan_node_id;
	local->worker_context.input_hash_join_plan_node_id = input_hash_join_plan_node_id;
	local->worker_context.parallel_scan_relid = source_scan_relid;
	local->worker_context.parallel_scan_plan_node_id = source_scan_plan_node_id;
	local->worker_context.parallel_scan_desc = parallel_scan_desc;
	local->worker_context.hash_build_execution =
		need_hash_join_state &&
		(shared_hash_bridge == nullptr ||
		 shared_hash_bridge_size == 0 ||
		 hash_join_plan_node_id != input_hash_join_plan_node_id);
	local->worker_context.leader = leader;
	if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: parallel %s local init binding rel=%u plan_node_id=%d agg_plan_node_id=%d hash_join_plan_node_id=%d input_hash_join_plan_node_id=%d require_agg=%s need_hash=%s",
			 leader ? "leader" : "worker",
			 source_scan_relid,
			 source_scan_plan_node_id,
			 agg_plan_node_id,
			 hash_join_plan_node_id,
			 input_hash_join_plan_node_id,
			 require_agg_state ? "true" : "false",
			 need_hash_join_state ? "true" : "false");

	local_init_plan = local->plannedstmt->planTree;

	root_plan = ExecInitVecPlan(local_init_plan,
								local->estate,
								&local->worker_context);
	if (!root_plan &&
		local_init_plan != local->plannedstmt->planTree)
	{
		if (pg_volvec_trace_hooks)
			elog(LOG,
				 "pg_volvec: parallel %s local init hash-build subtree failed, retrying full root",
				 leader ? "leader" : "worker");
		root_plan = ExecInitVecPlan(local->plannedstmt->planTree,
									local->estate,
									&local->worker_context);
	}
	if (!root_plan)
	{
		MemoryContextSwitchTo(old_context);
		CleanupLocalParallelAggregateProcessState(local);
		if (failure_reason != nullptr)
			*failure_reason = "parallel worker could not initialize VecPlanState";
		return false;
	}

	agg_state = nullptr;
	if (require_agg_state)
	{
		if (agg_plan_node_id >= 0)
			agg_state = root_plan->find_parallel_aggregate_state_by_plan_node_id(agg_plan_node_id);
		else
			agg_state = root_plan->find_parallel_aggregate_state();
		if (agg_state == nullptr || !agg_state->supports_parallel_partial_state())
		{
			MemoryContextSwitchTo(old_context);
			CleanupLocalParallelAggregateProcessState(local);
			if (failure_reason != nullptr)
				*failure_reason = "parallel worker requires partial aggregate support";
			return false;
		}
	}

	local->worker_context.root_plan = root_plan.get();
	local->worker_context.agg_state = agg_state;
	if (need_hash_join_state)
	{
		if (hash_join_plan_node_id >= 0)
			local->worker_context.hash_join_state =
				root_plan->find_parallel_hash_join_state_by_plan_node_id(hash_join_plan_node_id);
		else
			local->worker_context.hash_join_state = root_plan->find_parallel_hash_join_state();
		if (local->worker_context.hash_join_state == nullptr)
		{
			MemoryContextSwitchTo(old_context);
			CleanupLocalParallelAggregateProcessState(local);
			if (failure_reason != nullptr)
				*failure_reason = "parallel worker requires hash join state";
			return false;
		}
	}
	if (shared_hash_bridge != nullptr && shared_hash_bridge_size > 0)
	{
		const char *pack_failure_reason = nullptr;
		VecHashJoinState *input_hash_join_state = nullptr;

		if (TryAttachSharedHashBridgePack(root_plan.get(),
											 shared_hash_bridge,
											 shared_hash_bridge_size,
											 leader,
											 &pack_failure_reason))
			goto shared_hash_bridge_done;
		if (pack_failure_reason != nullptr)
		{
			MemoryContextSwitchTo(old_context);
			CleanupLocalParallelAggregateProcessState(local);
			if (failure_reason != nullptr)
				*failure_reason = pack_failure_reason;
			return false;
		}

		if (input_hash_join_plan_node_id >= 0)
			input_hash_join_state =
				root_plan->find_parallel_hash_join_state_by_plan_node_id(
					input_hash_join_plan_node_id);
		if (input_hash_join_state == nullptr)
			input_hash_join_state = local->worker_context.hash_join_state;
		if (input_hash_join_state == nullptr)
		{
			MemoryContextSwitchTo(old_context);
			CleanupLocalParallelAggregateProcessState(local);
			if (failure_reason != nullptr)
				*failure_reason = "parallel worker requires input hash join state for shared bridge";
			return false;
		}
		input_hash_join_state->attach_shared_finalized_hash_bridge(shared_hash_bridge,
											  shared_hash_bridge_size);
		if (pg_volvec_trace_hooks)
			elog(LOG,
				 "pg_volvec: parallel %s attached shared hash bridge bytes=%zu entries=%zu chunks=%zu input_hash_join_plan_node_id=%d",
				 leader ? "leader" : "worker",
				 shared_hash_bridge_size,
				 input_hash_join_state->parallel_hash_entry_count(),
				 input_hash_join_state->parallel_hash_chunk_count(),
				 input_hash_join_plan_node_id);
	}
shared_hash_bridge_done:
	local->root_plan = root_plan.release();
	INSTR_TIME_SET_CURRENT(init_end);
	INSTR_TIME_SUBTRACT(init_end, init_start);
	local->init_time_us = (uint64) INSTR_TIME_GET_MICROSEC(init_end);
	MemoryContextSwitchTo(old_context);
	return true;
}

bool
ExecuteParallelWorkerSourceLoop(ParallelWorkerExecutionMode mode,
								ParallelWorkerContext &worker_context,
								const char **failure_reason)
{
	if (failure_reason != nullptr)
		*failure_reason = nullptr;

	switch (mode)
	{
		case ParallelWorkerExecutionMode::AggregateProbe:
			if (worker_context.agg_state == nullptr)
			{
				if (failure_reason != nullptr)
					*failure_reason = "parallel source pipeline loop requires aggregate state";
				return false;
			}
			worker_context.agg_state->consume_left_input();
			worker_context.agg_state->finish_sink();
			return true;

		case ParallelWorkerExecutionMode::HashBuild:
			if (worker_context.hash_join_state == nullptr)
			{
				if (failure_reason != nullptr)
					*failure_reason = "parallel source pipeline loop requires hash join state";
				return false;
			}
			worker_context.hash_join_state->consume_build_input_radix();
			return true;

		case ParallelWorkerExecutionMode::QueryScheduler:
			if (failure_reason != nullptr)
				*failure_reason =
					"query scheduler workers dispatch DSM tasks directly";
			return false;
	}
	if (failure_reason != nullptr)
		*failure_reason = "parallel source pipeline loop saw unknown execution mode";
	return false;
}

void
PopulatePartialDiagnostics(const LocalParallelAggregateProcessState &local,
						   ParallelAggPartialState *partial)
{
	VecSeqScanState *scan_state = nullptr;

	if (partial == nullptr)
		return;
	partial->init_time_us = local.init_time_us;
	partial->exec_time_us = local.exec_time_us;
	if (local.worker_context.agg_state != nullptr)
	{
		partial->input_batches = local.worker_context.agg_state->input_batches_consumed();
		partial->input_rows = local.worker_context.agg_state->input_rows_consumed();
	}
	if (local.worker_context.root_plan != nullptr)
		scan_state = local.worker_context.root_plan->find_parallel_source_scan_state();
	if (scan_state != nullptr)
		partial->blocks_opened = scan_state->blocks_opened();
}

void
PopulateHashBuildPartialDiagnostics(const LocalParallelAggregateProcessState &local,
									ParallelHashBuildPartialState *partial)
{
	VecSeqScanState *scan_state = nullptr;

	if (partial == nullptr)
		return;
	partial->init_time_us = local.init_time_us;
	partial->exec_time_us = local.exec_time_us;
	if (local.worker_context.hash_join_state != nullptr)
	{
		partial->input_batches =
			local.worker_context.hash_join_state->build_input_batches_consumed();
		partial->input_rows =
			local.worker_context.hash_join_state->build_input_rows_consumed();
		partial->entry_count =
			local.worker_context.hash_join_state->parallel_hash_entry_count();
		partial->chunk_count =
			local.worker_context.hash_join_state->parallel_hash_chunk_count();
	}
	if (local.worker_context.root_plan != nullptr)
		scan_state = local.worker_context.root_plan->find_parallel_source_scan_state();
	if (scan_state != nullptr)
		partial->blocks_opened = scan_state->blocks_opened();
}

bool
TryInitializeParallelMergeContext(PgVolVecQueryState *query_state,
								  int agg_plan_node_id,
								  int hash_join_plan_node_id,
								  bool need_hash_join_state,
								  ParallelWorkerContext *worker_context,
								  const char **failure_reason)
{
	VecAggState *agg_state = nullptr;

	if (failure_reason != nullptr)
		*failure_reason = nullptr;
	if (worker_context != nullptr)
		*worker_context = ParallelWorkerContext{};

	if (query_state == nullptr ||
		query_state->vec_plan == nullptr ||
		query_state->parallel_plan == nullptr ||
		query_state->parallel_scheduler == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel query state is incomplete";
		return false;
	}

	if (agg_plan_node_id >= 0)
		agg_state = query_state->vec_plan->find_parallel_aggregate_state_by_plan_node_id(agg_plan_node_id);
	else
		agg_state = query_state->vec_plan->find_parallel_aggregate_state();
	if (agg_state == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel leader path requires aggregate state";
		return false;
	}

	if (worker_context != nullptr)
	{
		worker_context->memory_context = query_state->context;
		worker_context->root_plan = query_state->vec_plan;
		worker_context->agg_state = agg_state;
		worker_context->agg_plan_node_id = agg_plan_node_id;
		worker_context->hash_join_plan_node_id = hash_join_plan_node_id;
		worker_context->input_hash_join_plan_node_id = hash_join_plan_node_id;
		if (need_hash_join_state)
		{
			if (hash_join_plan_node_id >= 0)
				worker_context->hash_join_state =
					query_state->vec_plan->find_parallel_hash_join_state_by_plan_node_id(
						hash_join_plan_node_id);
			else
				worker_context->hash_join_state =
					query_state->vec_plan->find_parallel_hash_join_state();
			if (worker_context->hash_join_state == nullptr)
			{
				if (failure_reason != nullptr)
					*failure_reason = "parallel leader path requires hash join state";
				return false;
			}
		}
		worker_context->leader = true;
	}
	return true;
}


} /* namespace pg_volvec */
