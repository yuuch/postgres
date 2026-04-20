bool
TryInitializeLeaderOnlyAggregateWorkerContext(PgVolVecQueryState *query_state,
												   ParallelWorkerContext *worker_context,
												   const ParallelPipelineDesc **source_pipeline_out,
												   const char **failure_reason)
{
	const ParallelPipelineDesc *source_pipeline = nullptr;

	if (source_pipeline_out != nullptr)
		*source_pipeline_out = nullptr;
	if (!TryInitializeParallelMergeContext(query_state, -1, -1, false, worker_context, failure_reason))
		return false;

	for (const auto &pipeline : query_state->parallel_plan->pipelines())
	{
		if (!IsAggregateSourcePipeline(pipeline))
			continue;
		if (source_pipeline != nullptr)
		{
			if (failure_reason != nullptr)
				*failure_reason =
					"parallel leader path currently requires exactly one aggregate source pipeline";
			return false;
		}
		source_pipeline = &pipeline;
	}

	if (source_pipeline == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason =
				"parallel leader path found no aggregate source pipeline";
		return false;
	}
	if (source_pipeline->driver_kind != ParallelPipelineDriverKind::SourceScan)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel leader path requires a source-scan driver";
		return false;
	}
	if (source_pipeline_out != nullptr)
		*source_pipeline_out = source_pipeline;
	return true;
}

static size_t
BuildSerializedParamExec(QueryDesc *queryDesc, uint8_t **buffer_out)
{
	uint32 count;
	size_t entry_bytes;
	size_t data_bytes = 0;
	size_t total_bytes;
	ParallelParamExecHeader *header;
	ParallelParamExecEntry *entries;
	char *cursor;
	ListCell *lc;
	uint32 i = 0;

	if (buffer_out != nullptr)
		*buffer_out = nullptr;
	if (queryDesc == nullptr || queryDesc->plannedstmt == nullptr ||
		queryDesc->estate == nullptr ||
		queryDesc->plannedstmt->paramExecTypes == NIL ||
		queryDesc->estate->es_param_exec_vals == nullptr)
		return 0;
	if (queryDesc->plannedstmt->planTree != nullptr &&
		queryDesc->plannedstmt->planTree->extParam != NULL)
		ExecSetParamPlanMulti(queryDesc->plannedstmt->planTree->extParam,
							  GetPerTupleExprContext(queryDesc->estate));
	count = (uint32) list_length(queryDesc->plannedstmt->paramExecTypes);
	foreach(lc, queryDesc->plannedstmt->paramExecTypes)
	{
		Oid type = lfirst_oid(lc);
		ParamExecData *prm = &queryDesc->estate->es_param_exec_vals[i++];
		int16 typlen = 0;
		bool typbyval = false;

		get_typlenbyval(type, &typlen, &typbyval);
		data_bytes += datumEstimateSpace(prm->value, prm->isnull, typbyval, typlen);
	}
	entry_bytes = (size_t) count * sizeof(ParallelParamExecEntry);
	total_bytes = sizeof(ParallelParamExecHeader) + entry_bytes + data_bytes;
	if (total_bytes == sizeof(ParallelParamExecHeader) + entry_bytes)
		return 0;
	header = (ParallelParamExecHeader *) palloc0(total_bytes);
	header->magic = 0x56565045;
	header->count = count;
	header->bytes = (uint32) total_bytes;
	entries = (ParallelParamExecEntry *) ((uint8_t *) header + sizeof(*header));
	cursor = (char *) header + sizeof(*header) + entry_bytes;
	i = 0;
	foreach(lc, queryDesc->plannedstmt->paramExecTypes)
	{
		Oid type = lfirst_oid(lc);
		ParamExecData *prm = &queryDesc->estate->es_param_exec_vals[i];
		ParallelParamExecEntry *entry = &entries[i];
		int16 typlen = 0;
		bool typbyval = false;
		char *before = cursor;

		get_typlenbyval(type, &typlen, &typbyval);
		entry->type = type;
		entry->typ_len = typlen;
		entry->typ_byval = typbyval;
		entry->valid = prm->execPlan == nullptr;
		entry->isnull = prm->isnull;
		entry->offset = (uint32) (before - ((char *) header + sizeof(*header) + entry_bytes));
		if (entry->valid)
			datumSerialize(prm->value, prm->isnull, typbyval, typlen, &cursor);
		entry->size = (uint32) (cursor - before);
		if (pg_volvec_trace_hooks)
			elog(LOG,
				 "pg_volvec: serialized PARAM_EXEC %u type=%u valid=%s isnull=%s bytes=%u",
				 i,
				 type,
				 entry->valid ? "true" : "false",
				 entry->isnull ? "true" : "false",
				 entry->size);
		i++;
	}
	if (buffer_out != nullptr)
		*buffer_out = (uint8_t *) header;
	return total_bytes;
}

[[maybe_unused]] bool
TryExecuteQuerySchedulerSkeleton(PgVolVecQueryState *query_state,
								 QueryDesc *queryDesc,
								 const char **failure_reason)
{
	ParallelContext *pcxt = nullptr;
	ParallelAggregateSharedControl *control = nullptr;
	ParallelQuerySchedulerShared *scheduler_shared = nullptr;
	ParallelQueryPipelineShared *pipeline_shared = nullptr;
	ParallelQueryTaskShared *task_shared = nullptr;
	ParallelAggPartialState *query_agg_partials = nullptr;
	ParallelHashBuildPartialState *query_hash_partials = nullptr;
	ParallelSortRunState *query_sort_runs = nullptr;
	uint32_t *successor_shared = nullptr;
	VolVecVector<Relation> source_rels{PgMemoryContextAllocator<Relation>(CurrentMemoryContext)};
	VolVecVector<size_t> source_pscan_lens{PgMemoryContextAllocator<size_t>(CurrentMemoryContext)};
	SharedFileSet *partial_fileset = nullptr;
	char *plannedstmt_serialized = nullptr;
	uint8_t *serialized_param_exec = nullptr;
	const char *query_text = "";
	size_t plannedstmt_len;
	size_t query_text_len;
	size_t serialized_param_exec_len = 0;
	size_t pipeline_count;
	size_t source_task_count = 0;
	size_t finalize_task_count = 0;
	size_t successor_count = 0;
	size_t query_agg_partials_len = 0;
	size_t query_hash_partials_len = 0;
	size_t query_sort_runs_len = 0;
	bool entered_parallel = false;
	bool success = false;
	bool merged_aggregate_partials = false;
	bool merged_sort_runs = false;
	int requested_workers;

	if (failure_reason != nullptr)
		*failure_reason = nullptr;
	if (query_state == nullptr ||
		query_state->parallel_plan == nullptr ||
		query_state->parallel_scheduler == nullptr ||
		queryDesc == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "query scheduler skeleton requires initialized query state";
		return false;
	}
	for (const auto &pipeline : query_state->parallel_plan->pipelines())
	{
		if (pipeline.role == ParallelPipelineRole::SortMerge &&
			pipeline.pipeline_id != query_state->parallel_plan->root_pipeline_id())
		{
			if (failure_reason != nullptr)
				*failure_reason =
					"query scheduler only supports root SortMerge-shaped plans safely";
			return false;
		}
		if (pipeline.role == ParallelPipelineRole::HashBuildSource &&
			pipeline.driver_kind == ParallelPipelineDriverKind::BridgeFinalize)
		{
			if (failure_reason != nullptr)
				*failure_reason =
					"query scheduler does not yet support aggregate-bridge hash build source safely";
			return false;
		}
	}

	pipeline_count = query_state->parallel_plan->pipeline_count();
	for (const auto &pipeline : query_state->parallel_plan->pipelines())
	{
		const ParallelPipelineRuntimeState *runtime =
			query_state->parallel_scheduler->get_pipeline_runtime(pipeline.pipeline_id);

		if (runtime == nullptr || !pipeline.source_morsel_driven ||
			runtime->total_blocks == InvalidBlockNumber)
			finalize_task_count += 1;
		else
		{
			source_task_count +=
				(runtime->total_blocks + pg_volvec_parallel_morsel_nblocks - 1) /
				pg_volvec_parallel_morsel_nblocks;
			source_rels.push_back(table_open(pipeline.scan_relid, NoLock));
			source_pscan_lens.push_back(
				table_parallelscan_estimate(source_rels.back(),
										   queryDesc->estate->es_snapshot));
		}
		successor_count += pipeline.successors.size();
	}
	if (source_task_count + finalize_task_count == 0)
	{
		if (failure_reason != nullptr)
			*failure_reason = "query scheduler skeleton found no tasks";
		return false;
	}

	requested_workers = Max(1, pg_volvec_parallel_max_workers);
	query_text = queryDesc->sourceText != nullptr ? queryDesc->sourceText : "";
	plannedstmt_serialized = nodeToString(queryDesc->plannedstmt);
	plannedstmt_len = strlen(plannedstmt_serialized) + 1;
	query_text_len = strlen(query_text) + 1;
	serialized_param_exec_len =
		BuildSerializedParamExec(queryDesc, &serialized_param_exec);

	EnterParallelMode();
	entered_parallel = true;
	pcxt = CreateParallelContext("pg_volvec",
								 "pg_volvec_parallel_worker_main",
								 requested_workers);
	query_agg_partials_len =
		mul_size(sizeof(ParallelAggPartialState),
				 mul_size(pipeline_count, (size_t) pcxt->nworkers));
	query_hash_partials_len =
		mul_size(sizeof(ParallelHashBuildPartialState),
				 mul_size(pipeline_count, (size_t) pcxt->nworkers));
	query_sort_runs_len =
		mul_size(sizeof(ParallelSortRunState),
				 (size_t) (source_task_count + finalize_task_count));
	shm_toc_estimate_chunk(&pcxt->estimator, sizeof(ParallelAggregateSharedControl));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator, plannedstmt_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator, query_text_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	if (serialized_param_exec_len > 0)
	{
		shm_toc_estimate_chunk(&pcxt->estimator, serialized_param_exec_len);
		shm_toc_estimate_keys(&pcxt->estimator, 1);
	}
	shm_toc_estimate_chunk(&pcxt->estimator, sizeof(SharedFileSet));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator, sizeof(ParallelQuerySchedulerShared));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator,
						   sizeof(ParallelQueryPipelineShared) * pipeline_count);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator,
						   sizeof(ParallelQueryTaskShared) *
							   (source_task_count + finalize_task_count));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator,
						   sizeof(uint32_t) * successor_count);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator, query_agg_partials_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator, query_hash_partials_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator, query_sort_runs_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	for (size_t i = 0; i < source_pscan_lens.size(); i++)
	{
		shm_toc_estimate_chunk(&pcxt->estimator, source_pscan_lens[i]);
		shm_toc_estimate_keys(&pcxt->estimator, 1);
	}

	InitializeParallelDSM(pcxt);
	if (pcxt->seg == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "query scheduler skeleton DSM initialization failed";
		goto done;
	}

	control = (ParallelAggregateSharedControl *)
		shm_toc_allocate(pcxt->toc, sizeof(ParallelAggregateSharedControl));
	control->magic = VOLVEC_PARALLEL_MAGIC;
	control->source_pipeline_id = UINT32_MAX;
	control->partial_slot_count = pcxt->nworkers;
	control->wave_pipeline_count = 0;
	control->morsel_nblocks = pg_volvec_parallel_morsel_nblocks;
	control->total_blocks = 0;
	control->source_scan_relid = InvalidOid;
	control->source_scan_plan_node_id = -1;
	control->agg_plan_node_id = -1;
	control->hash_join_plan_node_id = -1;
	control->input_hash_join_plan_node_id = -1;
	control->execution_mode = (uint32) ParallelWorkerExecutionMode::QueryScheduler;
	control->need_hash_join_state = false;
	control->hash_bridge_ready = 0;
	control->reserved[0] = 0;
	control->reserved[1] = 0;
	control->hash_bridge_size = 0;
	pg_atomic_init_u64(&control->next_block, 0);
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_CONTROL, control);

	{
		char *shared_plannedstmt = (char *) shm_toc_allocate(pcxt->toc, plannedstmt_len);
		char *shared_query_text = (char *) shm_toc_allocate(pcxt->toc, query_text_len);

		memcpy(shared_plannedstmt, plannedstmt_serialized, plannedstmt_len);
		memcpy(shared_query_text, query_text, query_text_len);
		shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_PLANNEDSTMT, shared_plannedstmt);
		shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_QUERY_TEXT, shared_query_text);
		if (serialized_param_exec_len > 0)
		{
			uint8_t *shared_param_exec =
				(uint8_t *) shm_toc_allocate(pcxt->toc, serialized_param_exec_len);

			memcpy(shared_param_exec, serialized_param_exec, serialized_param_exec_len);
			shm_toc_insert(pcxt->toc,
						   VOLVEC_PARALLEL_KEY_PARAM_EXEC,
						   shared_param_exec);
		}
	}

	partial_fileset = (SharedFileSet *) shm_toc_allocate(pcxt->toc, sizeof(SharedFileSet));
	SharedFileSetInit(partial_fileset, pcxt->seg);
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_PARTIAL_FILESET, partial_fileset);
	query_agg_partials = (ParallelAggPartialState *)
		shm_toc_allocate(pcxt->toc, query_agg_partials_len);
	memset(query_agg_partials, 0, query_agg_partials_len);
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_QUERY_AGG_PARTIALS,
				   query_agg_partials);
	query_hash_partials = (ParallelHashBuildPartialState *)
		shm_toc_allocate(pcxt->toc, query_hash_partials_len);
	memset(query_hash_partials, 0, query_hash_partials_len);
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_QUERY_HASH_PARTIALS,
				   query_hash_partials);
	query_sort_runs = (ParallelSortRunState *)
		shm_toc_allocate(pcxt->toc, query_sort_runs_len);
	memset(query_sort_runs, 0, query_sort_runs_len);
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_QUERY_SORT_RUNS,
				   query_sort_runs);

	scheduler_shared = (ParallelQuerySchedulerShared *)
		shm_toc_allocate(pcxt->toc, sizeof(ParallelQuerySchedulerShared));
	*scheduler_shared = ParallelQuerySchedulerShared{};
	scheduler_shared->pipeline_count = (uint32) pipeline_count;
	scheduler_shared->task_count = (uint32) (source_task_count + finalize_task_count);
	scheduler_shared->worker_count = (uint32) pcxt->nworkers;
	scheduler_shared->morsel_nblocks = pg_volvec_parallel_morsel_nblocks;
	pg_atomic_init_u32(&scheduler_shared->next_task_scan, 0);
	pg_atomic_init_u32(&scheduler_shared->completed_tasks, 0);
	pg_atomic_init_u32(&scheduler_shared->shutdown, 0);
	pg_atomic_init_u32(&scheduler_shared->error, 0);
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_QUERY_SCHEDULER, scheduler_shared);

	pipeline_shared = (ParallelQueryPipelineShared *)
		shm_toc_allocate(pcxt->toc, sizeof(ParallelQueryPipelineShared) * pipeline_count);
	memset(pipeline_shared, 0, sizeof(ParallelQueryPipelineShared) * pipeline_count);
	successor_shared = (uint32_t *)
		shm_toc_allocate(pcxt->toc, sizeof(uint32_t) * successor_count);
	memset(successor_shared, 0, sizeof(uint32_t) * successor_count);
	{
		uint32 successor_offset = 0;

		for (const auto &pipeline : query_state->parallel_plan->pipelines())
		{
			ParallelQueryPipelineShared &dst = pipeline_shared[pipeline.pipeline_id];

			dst.pipeline_id = pipeline.pipeline_id;
			dst.role = (uint32) pipeline.role;
			dst.driver_kind = (uint32) pipeline.driver_kind;
			dst.task_kind = (uint32) (pipeline.source_morsel_driven ?
				ParallelTaskKind::SourceMorsel :
				ParallelTaskKind::BridgeFinalize);
			dst.input_bridge = (uint32) pipeline.input_bridge;
			dst.output_bridge = (uint32) pipeline.output_bridge;
			dst.total_dependencies = (uint32) pipeline.dependencies.size();
			dst.source_scan_relid = pipeline.scan_relid;
			dst.source_scan_plan_node_id = pipeline.scan_plan_node_id;
			dst.agg_plan_node_id = pipeline.agg_plan_node_id;
			dst.hash_join_plan_node_id = pipeline.hash_join_plan_node_id;
			dst.input_hash_join_plan_node_id = pipeline.input_hash_join_plan_node_id;
			dst.successor_start = successor_offset;
			dst.successor_count = (uint32) pipeline.successors.size();
			for (uint32 successor_id : pipeline.successors)
				successor_shared[successor_offset++] = successor_id;
			pg_atomic_init_u32(&dst.remaining_dependencies,
							   (uint32) pipeline.dependencies.size());
			pg_atomic_init_u32(&dst.completed_tasks, 0);
			pg_atomic_init_u32(&dst.published_workers, 0);
			pg_atomic_init_u32(&dst.completed, 0);
			pg_atomic_init_u32(&dst.hash_bridge_ready, 0);
		}
	}
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_QUERY_PIPELINES, pipeline_shared);
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_QUERY_SUCCESSORS, successor_shared);
	{
		size_t source_idx = 0;

		for (const auto &pipeline : query_state->parallel_plan->pipelines())
		{
			const ParallelPipelineRuntimeState *runtime =
				query_state->parallel_scheduler->get_pipeline_runtime(pipeline.pipeline_id);
			ParallelTableScanDesc qpscan;

			if (runtime == nullptr || !pipeline.source_morsel_driven ||
				runtime->total_blocks == InvalidBlockNumber)
				continue;
			qpscan = (ParallelTableScanDesc) shm_toc_allocate(pcxt->toc,
															 source_pscan_lens[source_idx]);
			table_parallelscan_initialize(source_rels[source_idx],
										  qpscan,
										  queryDesc->estate->es_snapshot);
			shm_toc_insert(pcxt->toc,
						   VOLVEC_PARALLEL_KEY_QUERY_SOURCE_PSCAN_BASE + pipeline.pipeline_id,
						   qpscan);
			source_idx++;
		}
	}

	task_shared = (ParallelQueryTaskShared *)
		shm_toc_allocate(pcxt->toc,
						 sizeof(ParallelQueryTaskShared) *
							 (source_task_count + finalize_task_count));
	memset(task_shared,
		   0,
		   sizeof(ParallelQueryTaskShared) *
			   (source_task_count + finalize_task_count));
	{
		uint32 task_id = 0;

		for (const auto &pipeline : query_state->parallel_plan->pipelines())
		{
			const ParallelPipelineRuntimeState *runtime =
				query_state->parallel_scheduler->get_pipeline_runtime(pipeline.pipeline_id);

			if (runtime == nullptr || !pipeline.source_morsel_driven ||
				runtime->total_blocks == InvalidBlockNumber)
			{
				ParallelQueryTaskShared &task = task_shared[task_id];

				task.task_id = task_id;
				task.pipeline_id = pipeline.pipeline_id;
				task.task_kind = (uint32) ParallelTaskKind::BridgeFinalize;
				task.morsel_start_block = InvalidBlockNumber;
				task.morsel_nblocks = 0;
				pg_atomic_init_u32(&task.state,
								   (uint32) ParallelQueryTaskState::Pending);
				pipeline_shared[pipeline.pipeline_id].total_tasks++;
				task_id++;
				continue;
			}
			for (BlockNumber start = 0;
				 start < runtime->total_blocks;
				 start += pg_volvec_parallel_morsel_nblocks)
			{
				BlockNumber remaining = runtime->total_blocks - start;
				ParallelQueryTaskShared &task = task_shared[task_id];

				task.task_id = task_id;
				task.pipeline_id = pipeline.pipeline_id;
				task.task_kind = (uint32) ParallelTaskKind::SourceMorsel;
				task.morsel_start_block = start;
				task.morsel_nblocks =
					remaining > pg_volvec_parallel_morsel_nblocks ?
					pg_volvec_parallel_morsel_nblocks :
					(uint32) remaining;
				pg_atomic_init_u32(&task.state,
								   (uint32) ParallelQueryTaskState::Pending);
				pipeline_shared[pipeline.pipeline_id].total_tasks++;
				task_id++;
			}
		}
	}
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_QUERY_TASKS, task_shared);

	LaunchParallelWorkers(pcxt);
	WaitForParallelWorkersToFinish(pcxt);
	success = pg_atomic_read_u32(&scheduler_shared->error) == 0;
	if (!success && failure_reason != nullptr)
		*failure_reason = "query scheduler skeleton worker reported an error";
	if (success)
	{
		VecSortState *root_sort_state = dynamic_cast<VecSortState *>(query_state->vec_plan);

		if (root_sort_state != nullptr && query_sort_runs != nullptr)
		{
			DataChunk<DEFAULT_CHUNK_SIZE> chunk;
			bool saw_sort_run = false;

			root_sort_state->reset_external_input();
			for (size_t task_idx = 0;
				 task_idx < source_task_count + finalize_task_count;
				 task_idx++)
			{
				ParallelSortRunState &run = query_sort_runs[task_idx];
				BufFile *file;

				if (run.row_count == 0 || run.file_bytes == 0)
					continue;
				file = BufFileOpenFileSet(&partial_fileset->fs,
										  run.file_name,
										  O_RDONLY,
										  false);
				if (file == nullptr)
				{
					if (failure_reason != nullptr)
						*failure_reason = "query scheduler sort run open failed";
					success = false;
					goto done;
				}
				for (uint32 chunk_idx = 0; chunk_idx < run.chunk_count; chunk_idx++)
				{
					if (!ReadDataChunkRows(file, &chunk))
					{
						BufFileClose(file);
						if (failure_reason != nullptr)
							*failure_reason = "query scheduler sort run read failed";
						success = false;
						goto done;
					}
					root_sort_state->append_external_batch(chunk);
				}
				BufFileClose(file);
				BufFileDeleteFileSet(&partial_fileset->fs,
									 run.file_name,
									 true);
				saw_sort_run = true;
				if (pg_volvec_trace_hooks)
					elog(LOG,
						 "pg_volvec: query scheduler sort run merge task=%u chunks=%u rows=%llu bytes=%llu",
						 run.task_id,
						 run.chunk_count,
						 (unsigned long long) run.row_count,
						 (unsigned long long) run.file_bytes);
			}
			if (saw_sort_run)
			{
				root_sort_state->finish_external_input();
				merged_sort_runs = true;
			}
		}
		for (const auto &pipeline : query_state->parallel_plan->pipelines())
		{
			ParallelWorkerContext merge_context;
			bool merged_pipeline_partials = false;

			if (pipeline.output_bridge != ParallelBridgeKind::Aggregate ||
				pipeline.agg_plan_node_id < 0)
				continue;
			if (!TryInitializeParallelMergeContext(query_state,
												   pipeline.agg_plan_node_id,
												   pipeline.hash_join_plan_node_id,
												   false,
												   &merge_context,
												   failure_reason))
			{
				success = false;
				goto done;
			}
			instr_time pipeline_merge_start;
			uint64 pipeline_merge_rows = 0;
			uint64 pipeline_merge_bytes = 0;
			uint32 pipeline_merge_groups = 0;
			int pipeline_merge_partials = 0;

			INSTR_TIME_SET_CURRENT(pipeline_merge_start);
			for (int worker = 0; worker < pcxt->nworkers_launched; worker++)
			{
				size_t slot_index =
					(size_t) pipeline.pipeline_id * scheduler_shared->worker_count +
					(size_t) worker;
				ParallelAggPartialState &partial = query_agg_partials[slot_index];

				if (partial.naggs == 0)
					continue;
				instr_time partial_merge_start;

				INSTR_TIME_SET_CURRENT(partial_merge_start);
				if (partial.file_backed)
				{
					BufFile *file = BufFileOpenFileSet(&partial_fileset->fs,
													   partial.grouped_file_name,
													   O_RDONLY,
													   false);

					if (file == nullptr ||
						!merge_context.agg_state->merge_parallel_grouped_partial_file(
							file,
							partial))
					{
						if (file != nullptr)
							BufFileClose(file);
						if (failure_reason != nullptr)
							*failure_reason =
								"query scheduler grouped aggregate partial merge failed";
						success = false;
						goto done;
					}
					BufFileClose(file);
					BufFileDeleteFileSet(&partial_fileset->fs,
										 partial.grouped_file_name,
										 true);
				}
				else if (!merge_context.agg_state->merge_parallel_partial_state(partial))
				{
					if (failure_reason != nullptr)
						*failure_reason =
							"query scheduler aggregate partial merge failed";
					success = false;
					goto done;
				}
				pipeline_merge_partials++;
				pipeline_merge_rows += partial.input_rows;
				pipeline_merge_bytes += partial.file_bytes;
				pipeline_merge_groups += partial.group_count;
				if (pg_volvec_trace_hooks)
					elog(LOG,
						 "pg_volvec: query scheduler agg partial merge pipeline=%u worker=%d file=%s groups=%u rows=%llu bytes=%llu merge_us=%llu",
						 pipeline.pipeline_id,
						 worker,
						 partial.file_backed ? "on" : "off",
						 partial.group_count,
						 (unsigned long long) partial.input_rows,
						 (unsigned long long) partial.file_bytes,
						 (unsigned long long) RuntimeElapsedUsSince(partial_merge_start));
				merged_pipeline_partials = true;
				merged_aggregate_partials = true;
			}
			if (merged_pipeline_partials)
			{
				merge_context.agg_state->finish_sink();
				if (pg_volvec_trace_hooks)
					elog(LOG,
						 "pg_volvec: query scheduler agg pipeline merge done pipeline=%u partials=%d groups=%u rows=%llu bytes=%llu total_us=%llu",
						 pipeline.pipeline_id,
						 pipeline_merge_partials,
						 pipeline_merge_groups,
						 (unsigned long long) pipeline_merge_rows,
						 (unsigned long long) pipeline_merge_bytes,
						 (unsigned long long) RuntimeElapsedUsSince(pipeline_merge_start));
			}
		}
		if (!merged_aggregate_partials && !merged_sort_runs)
		{
			if (failure_reason != nullptr)
				*failure_reason =
					"query scheduler completed tasks but produced no aggregate partials or sort runs";
			success = false;
		}
	}

done:
	for (Relation rel : source_rels)
		table_close(rel, NoLock);
	if (partial_fileset != nullptr)
		SharedFileSetDeleteAll(partial_fileset);
	if (pcxt != nullptr)
		DestroyParallelContext(pcxt);
	if (entered_parallel)
		ExitParallelMode();
	if (plannedstmt_serialized != nullptr)
		pfree(plannedstmt_serialized);
	if (serialized_param_exec != nullptr)
		pfree(serialized_param_exec);
	return success;
}

bool
ExecuteParallelTask(const ParallelTaskDesc &task,
					 const ParallelPipelinePlan *parallel_plan,
					 ParallelWorkerContext &worker_context,
					 const char **failure_reason)
{
	const ParallelPipelineDesc *pipeline = nullptr;

	if (failure_reason != nullptr)
		*failure_reason = nullptr;
	if (parallel_plan == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel task execution requires a pipeline plan";
		return false;
	}

	pipeline = parallel_plan->get_pipeline(task.pipeline_id);
	if (pipeline == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel task referenced an unknown pipeline";
		return false;
	}

	switch (task.task_kind)
	{
		case ParallelTaskKind::SourceMorsel:
			if (pipeline->driver_kind != ParallelPipelineDriverKind::SourceScan)
			{
				if (failure_reason != nullptr)
					*failure_reason = "source morsel task does not target a source pipeline";
				return false;
			}
			if (task.morsel_start_block == InvalidBlockNumber || task.morsel_nblocks == 0)
				return true;
			if (pipeline->role == ParallelPipelineRole::HashBuildSource &&
				worker_context.hash_join_state != nullptr)
			{
				if (!worker_context.hash_join_state->configure_build_input_block_range(task.morsel_start_block,
																				 task.morsel_nblocks))
				{
					if (failure_reason != nullptr)
						*failure_reason = "hash build source task could not configure block range";
					return false;
				}
				worker_context.hash_join_state->consume_build_input();
				worker_context.hash_join_state->clear_build_input_block_range();
				return true;
			}
			if (pipeline->output_bridge == ParallelBridgeKind::Aggregate &&
				(pipeline->stage_mask & (uint32_t) ParallelPipelineStage::PartialAgg) != 0 &&
				worker_context.agg_state != nullptr)
			{
				if (!worker_context.agg_state->configure_input_block_range(task.morsel_start_block,
																		  task.morsel_nblocks))
				{
					if (failure_reason != nullptr)
						*failure_reason = "aggregate source task could not configure block range";
					return false;
				}
				worker_context.agg_state->consume_left_input();
				worker_context.agg_state->clear_input_block_range();
				return true;
			}
			if (failure_reason != nullptr)
				*failure_reason = psprintf("source morsel pipeline kernel not implemented (pipeline=%u role=%s driver=%s output_bridge=%u stage_mask=%u)",
										   pipeline->pipeline_id,
										   ParallelPipelineRoleDebugName(pipeline->role),
										   ParallelDriverKindDebugName(pipeline->driver_kind),
										   (unsigned) pipeline->output_bridge,
										   pipeline->stage_mask);
			return false;

		case ParallelTaskKind::BridgeFinalize:
			if (pipeline->role == ParallelPipelineRole::HashBuildFinalize &&
				worker_context.hash_join_state != nullptr)
			{
			worker_context.hash_join_state->publish_hash_bridge();
				worker_context.hash_join_state->finish_parallel_hash_build();
				return true;
			}
			if (pipeline->role == ParallelPipelineRole::AggFinalize &&
				worker_context.agg_state != nullptr)
			{
				worker_context.agg_state->finish_sink();
				return true;
			}
			if (failure_reason != nullptr)
				*failure_reason = psprintf("bridge finalize task kernel not implemented (pipeline=%u role=%s driver=%s input_bridge=%u output_bridge=%u)",
										   pipeline->pipeline_id,
										   ParallelPipelineRoleDebugName(pipeline->role),
										   ParallelDriverKindDebugName(pipeline->driver_kind),
										   (unsigned) pipeline->input_bridge,
										   (unsigned) pipeline->output_bridge);
			return false;
	}

	if (failure_reason != nullptr)
		*failure_reason = "unknown parallel task kind";
	return false;
}

[[maybe_unused]] static bool
TryExecuteParallelHashBuildPhase(PgVolVecQueryState *query_state,
								 QueryDesc *queryDesc,
								 const ParallelPipelineDesc *probe_pipeline,
								 ParallelWorkerContext &merge_context,
								 const char **failure_reason)
{
	const ParallelPipelineRuntimeState *build_runtime = nullptr;
	const ParallelPipelineDesc *build_pipeline = nullptr;
	const char *plannedstmt_serialized = nullptr;
	const char *query_text = nullptr;
	char *plannedstmt_serialized_owned = nullptr;
	Relation source_rel = nullptr;
	size_t source_pscan_len = 0;
	size_t plannedstmt_len = 0;
	size_t query_text_len = 0;
	size_t build_partials_len = 0;
	ParallelContext *pcxt = nullptr;
	ParallelAggregateSharedControl *control = nullptr;
	ParallelTableScanDesc source_pscan = nullptr;
	ParallelHashBuildPartialState *build_partials = nullptr;
	SharedFileSet *partial_fileset = nullptr;
	LocalParallelAggregateProcessState leader_local_state;
	ParallelHashBuildPartialState leader_partial{};
	const char *leader_failure_reason = nullptr;
	bool entered_parallel = false;
	bool leader_local_initialized = false;
	bool success = false;
	int requested_workers;
	size_t total_partial_entries = 0;
	size_t total_partial_chunks = 0;

	if (failure_reason != nullptr)
		*failure_reason = nullptr;
	if (query_state == nullptr ||
		query_state->parallel_plan == nullptr ||
		query_state->parallel_scheduler == nullptr ||
		queryDesc == nullptr ||
		probe_pipeline == nullptr ||
		merge_context.hash_join_state == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build phase has incomplete state";
		return false;
	}
	merge_context.hash_join_state->reset_parallel_hash_build_state();

	build_pipeline = FindLargestHashBuildDependency(query_state->parallel_plan,
													 query_state->parallel_scheduler,
													 probe_pipeline,
													 &build_runtime);
	if (build_pipeline == nullptr || build_runtime == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build phase found no build dependency pipeline";
		return false;
	}
	if (build_pipeline->driver_kind != ParallelPipelineDriverKind::SourceScan ||
		build_pipeline->scan_relid == InvalidOid ||
		build_pipeline->scan_plan_node_id < 0)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build phase requires a source-scan build pipeline";
		return false;
	}
	if (build_runtime->total_blocks == InvalidBlockNumber || build_runtime->total_blocks == 0)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build phase found no eligible build-side blocks";
		return false;
	}
	if (build_pipeline->hash_join_plan_node_id >= 0)
	{
		merge_context.hash_join_state =
			query_state->vec_plan->find_parallel_hash_join_state_by_plan_node_id(
				build_pipeline->hash_join_plan_node_id);
		merge_context.hash_join_plan_node_id = build_pipeline->hash_join_plan_node_id;
		if (merge_context.hash_join_state == nullptr)
		{
			if (failure_reason != nullptr)
				*failure_reason = "parallel hash build phase could not resolve build-side hash join state";
			return false;
		}
	}

	requested_workers = Min(pg_volvec_parallel_max_workers,
							(int) Max(build_runtime->estimated_morsels, 1u));
	if (requested_workers <= 0)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build phase requested zero workers";
		return false;
	}

	query_text = queryDesc->sourceText != nullptr ? queryDesc->sourceText : "";
	plannedstmt_serialized_owned = nodeToString(queryDesc->plannedstmt);
	plannedstmt_serialized = plannedstmt_serialized_owned;
	plannedstmt_len = strlen(plannedstmt_serialized) + 1;
	query_text_len = strlen(query_text) + 1;

	source_rel = table_open(build_pipeline->scan_relid, NoLock);
	source_pscan_len = table_parallelscan_estimate(source_rel, queryDesc->estate->es_snapshot);

	EnterParallelMode();
	entered_parallel = true;
	pcxt = CreateParallelContext("pg_volvec",
								 "pg_volvec_parallel_worker_main",
								 requested_workers);

	shm_toc_estimate_chunk(&pcxt->estimator, sizeof(ParallelAggregateSharedControl));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator, plannedstmt_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator, query_text_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator, source_pscan_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	build_partials_len = mul_size(sizeof(ParallelHashBuildPartialState), pcxt->nworkers);
	shm_toc_estimate_chunk(&pcxt->estimator, build_partials_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator, sizeof(SharedFileSet));
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	InitializeParallelDSM(pcxt);
	if (pcxt->seg == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build DSM initialization failed";
		goto done;
	}

	control = (ParallelAggregateSharedControl *)
		shm_toc_allocate(pcxt->toc, sizeof(ParallelAggregateSharedControl));
	control->magic = VOLVEC_PARALLEL_MAGIC;
	control->source_pipeline_id = build_pipeline->pipeline_id;
	control->partial_slot_count = pcxt->nworkers;
	control->wave_pipeline_count = 0;
	control->morsel_nblocks = pg_volvec_parallel_morsel_nblocks;
	control->total_blocks = build_runtime->total_blocks;
	control->source_scan_relid = build_pipeline->scan_relid;
	control->source_scan_plan_node_id = build_pipeline->scan_plan_node_id;
	control->agg_plan_node_id = -1;
	control->hash_join_plan_node_id = build_pipeline->hash_join_plan_node_id;
	control->input_hash_join_plan_node_id = build_pipeline->input_hash_join_plan_node_id;
	control->execution_mode = (uint32) ParallelWorkerExecutionMode::HashBuild;
	control->need_hash_join_state = true;
	control->hash_bridge_ready = 0;
	control->reserved[0] = 0;
	control->reserved[1] = 0;
	control->hash_bridge_size = 0;
	pg_atomic_init_u64(&control->next_block, 0);
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_CONTROL, control);

	{
		char *shared_plannedstmt = (char *) shm_toc_allocate(pcxt->toc, plannedstmt_len);
		char *shared_query_text = (char *) shm_toc_allocate(pcxt->toc, query_text_len);

		memcpy(shared_plannedstmt, plannedstmt_serialized, plannedstmt_len);
		memcpy(shared_query_text, query_text, query_text_len);
		shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_PLANNEDSTMT, shared_plannedstmt);
		shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_QUERY_TEXT, shared_query_text);
	}

	source_pscan = (ParallelTableScanDesc) shm_toc_allocate(pcxt->toc, source_pscan_len);
	table_parallelscan_initialize(source_rel, source_pscan, queryDesc->estate->es_snapshot);
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_SOURCE_PSCAN, source_pscan);

	build_partials = (ParallelHashBuildPartialState *)
		shm_toc_allocate(pcxt->toc, build_partials_len);
	memset(build_partials, 0, build_partials_len);
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_HASH_BUILD_PARTIALS, build_partials);

	partial_fileset = (SharedFileSet *) shm_toc_allocate(pcxt->toc, sizeof(SharedFileSet));
	SharedFileSetInit(partial_fileset, pcxt->seg);
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_PARTIAL_FILESET, partial_fileset);
	table_close(source_rel, NoLock);
	source_rel = nullptr;

	LaunchParallelWorkers(pcxt);
	if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: launched %d/%d parallel workers for hash build pipeline %u",
			 pcxt->nworkers_launched,
			 pcxt->nworkers,
			 build_pipeline->pipeline_id);
	if (pcxt->nworkers_launched == 0 && !pg_volvec_parallel_leader_participation)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build launched no workers and leader participation is disabled";
		goto done;
	}

	if (pg_volvec_parallel_leader_participation)
	{
		instr_time leader_exec_start;
		instr_time leader_exec_end;
		BufFile *file;
		char file_name[VOLVEC_PARALLEL_MAX_PARTIAL_FILE_NAME];

		if (!TryInitializeLocalParallelAggregateProcessState(plannedstmt_serialized,
															 query_text,
															 -1,
															 build_pipeline->hash_join_plan_node_id,
															 build_pipeline->input_hash_join_plan_node_id,
															 false,
															 true,
															 nullptr,
															 0,
															 build_pipeline->scan_relid,
															 build_pipeline->scan_plan_node_id,
															 source_pscan,
															 true,
															 &leader_local_state,
															 &leader_failure_reason))
		{
			if (failure_reason != nullptr)
				*failure_reason = leader_failure_reason != nullptr ?
					leader_failure_reason :
					"parallel hash build leader local init failed";
			goto done;
		}
		leader_local_initialized = true;
		INSTR_TIME_SET_CURRENT(leader_exec_start);
		if (!ExecuteParallelWorkerSourceLoop(ParallelWorkerExecutionMode::HashBuild,
											 leader_local_state.worker_context,
											 failure_reason))
			goto done;
		INSTR_TIME_SET_CURRENT(leader_exec_end);
		INSTR_TIME_SUBTRACT(leader_exec_end, leader_exec_start);
		leader_local_state.exec_time_us = (uint64) INSTR_TIME_GET_MICROSEC(leader_exec_end);

		FormatParallelPartialFileName(file_name, sizeof(file_name), "build_leader", 0);
		file = BufFileCreateFileSet(&partial_fileset->fs, file_name);
		if (file == nullptr ||
			!leader_local_state.worker_context.hash_join_state->export_parallel_build_partial_file(
				file,
				&leader_partial))
		{
			if (file != nullptr)
				BufFileClose(file);
			if (failure_reason != nullptr)
				*failure_reason = "parallel hash build leader partial export failed";
			goto done;
		}
		strlcpy(leader_partial.file_name, file_name, sizeof(leader_partial.file_name));
		BufFileExportFileSet(file);
		BufFileClose(file);
		PopulateHashBuildPartialDiagnostics(leader_local_state, &leader_partial);
	}

	WaitForParallelWorkersToFinish(pcxt);
	if (leader_local_initialized)
	{
		total_partial_entries += (size_t) leader_partial.entry_count;
		total_partial_chunks += (size_t) leader_partial.chunk_count;
	}
	for (int i = 0; i < pcxt->nworkers_launched; i++)
	{
		total_partial_entries += (size_t) build_partials[i].entry_count;
		total_partial_chunks += (size_t) build_partials[i].chunk_count;
	}
	if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: parallel hash build merge reserve entries=%zu chunks=%zu current_entries=%zu current_chunks=%zu",
			 total_partial_entries,
			 total_partial_chunks,
			 merge_context.hash_join_state->parallel_hash_entry_count(),
			 merge_context.hash_join_state->parallel_hash_chunk_count());
	merge_context.hash_join_state->reserve_parallel_hash_build_capacity(
		total_partial_entries,
		total_partial_chunks);

	if (leader_local_initialized)
	{
		BufFile *file = BufFileOpenFileSet(&partial_fileset->fs,
											 leader_partial.file_name,
											 O_RDONLY,
											 false);

		if (file == nullptr ||
			!merge_context.hash_join_state->merge_parallel_build_partial_file(file, leader_partial))
		{
			if (file != nullptr)
				BufFileClose(file);
			if (failure_reason != nullptr)
				*failure_reason = "parallel hash build leader partial merge failed";
			goto done;
		}
		BufFileClose(file);
		BufFileDeleteFileSet(&partial_fileset->fs, leader_partial.file_name, true);
	}

	for (int i = 0; i < pcxt->nworkers_launched; i++)
	{
		BufFile *file;

		if (pg_volvec_trace_hooks)
			elog(LOG,
				 "pg_volvec: parallel hash build worker[%d] partial init_us=%llu exec_us=%llu blocks=%llu batches=%llu rows=%llu entries=%llu chunks=%llu",
				 i,
				 (unsigned long long) build_partials[i].init_time_us,
				 (unsigned long long) build_partials[i].exec_time_us,
				 (unsigned long long) build_partials[i].blocks_opened,
				 (unsigned long long) build_partials[i].input_batches,
				 (unsigned long long) build_partials[i].input_rows,
				 (unsigned long long) build_partials[i].entry_count,
				 (unsigned long long) build_partials[i].chunk_count);
		file = BufFileOpenFileSet(&partial_fileset->fs,
								 build_partials[i].file_name,
								 O_RDONLY,
								 false);
		if (file == nullptr ||
			!merge_context.hash_join_state->merge_parallel_build_partial_file(file, build_partials[i]))
		{
			if (file != nullptr)
				BufFileClose(file);
			if (failure_reason != nullptr)
				*failure_reason = "parallel hash build partial merge failed";
			goto done;
		}
		BufFileClose(file);
		BufFileDeleteFileSet(&partial_fileset->fs, build_partials[i].file_name, true);
	}

	merge_context.hash_join_state->finish_parallel_hash_build();
	if (merge_context.hash_join_state->estimate_parallel_hash_bridge_size() > MaxAllocSize)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build shared bridge exceeds MaxAllocSize";
		goto done;
	}
	merge_context.hash_join_state->publish_hash_bridge();
	if (merge_context.hash_join_state->shared_hash_bridge_buffer() == nullptr ||
		merge_context.hash_join_state->shared_hash_bridge_size() == 0)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build did not publish a shared hash bridge";
		goto done;
	}
	success = true;

done:
	CleanupLocalParallelAggregateProcessState(&leader_local_state);
	if (source_rel != nullptr)
		table_close(source_rel, NoLock);
	if (partial_fileset != nullptr)
		SharedFileSetDeleteAll(partial_fileset);
	if (pcxt != nullptr)
		DestroyParallelContext(pcxt);
	if (entered_parallel)
		ExitParallelMode();
	if (plannedstmt_serialized_owned != nullptr)
		pfree(plannedstmt_serialized_owned);
	return success;
}

bool
TryExecuteProcessParallelAggregate(PgVolVecQueryState *query_state,
								   QueryDesc *queryDesc,
									 const char **failure_reason)
{
	const ParallelPipelineDesc *source_pipeline = nullptr;
	const ParallelPipelineDesc *execution_source_pipeline = nullptr;
	const ParallelPipelineDesc *ready_aggregate_source_pipeline = nullptr;
	const ParallelPipelineDesc *hash_probe_aggregate_source_pipeline = nullptr;
	ParallelWorkerContext merge_context;
	ParallelContext *pcxt = nullptr;
	ParallelAggregateSharedControl *control = nullptr;
	ParallelAggPartialState *partials = nullptr;
	ParallelAggPartialState leader_partial;
	SharedFileSet *partial_fileset = nullptr;
	ParallelTableScanDesc source_pscan = nullptr;
	Relation source_rel = nullptr;
	char *shared_plannedstmt = nullptr;
	char *shared_query_text = nullptr;
	uint8_t *shared_hash_bridge_dsm = nullptr;
	const ParallelPipelineRuntimeState *source_runtime = nullptr;
	char *plannedstmt_serialized = nullptr;
	const char *query_text = "";
	const char *leader_failure_reason = nullptr;
	const uint8_t *shared_hash_bridge = nullptr;
	uint32 shared_hash_bridge_count = 0;
	Size plannedstmt_len;
	Size query_text_len;
	Size partials_len;
	Size source_pscan_len;
	Size shared_hash_bridge_len = 0;
	int requested_workers;
	int aggregate_source_count = 0;
	int selected_agg_plan_node_id = -1;
	bool need_hash_join_state = false;
	bool file_backed_grouped_partials = false;
	bool entered_parallel = false;
	bool leader_local_initialized = false;
	bool allow_leader_participation = pg_volvec_parallel_leader_participation;
	bool success = false;
	LocalParallelAggregateProcessState leader_local_state;

	memset(&leader_partial, 0, sizeof(leader_partial));

	if (failure_reason != nullptr)
		*failure_reason = nullptr;
	if (query_state == nullptr || queryDesc == nullptr ||
		query_state->parallel_plan == nullptr || query_state->parallel_scheduler == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel aggregate execution requires initialized query state";
		return false;
	}

	const ParallelPipelineDesc *generic_aggregate_source_pipeline = nullptr;

	for (const auto &pipeline : query_state->parallel_plan->pipelines())
	{
		const ParallelPipelineRuntimeState *runtime;

		if (!IsAggregateSourcePipeline(pipeline))
			continue;
		aggregate_source_count++;
		if (pipeline.role == ParallelPipelineRole::GenericSource &&
			generic_aggregate_source_pipeline == nullptr)
			generic_aggregate_source_pipeline = &pipeline;
		if (pipeline.role == ParallelPipelineRole::HashProbeSource &&
			hash_probe_aggregate_source_pipeline == nullptr)
			hash_probe_aggregate_source_pipeline = &pipeline;
		runtime = query_state->parallel_scheduler->get_pipeline_runtime(pipeline.pipeline_id);
		if (runtime == nullptr)
			continue;
		if (runtime->remaining_dependencies == 0 &&
			runtime->ready &&
			pipeline.role == ParallelPipelineRole::GenericSource)
			ready_aggregate_source_pipeline = &pipeline;
		if (execution_source_pipeline == nullptr)
		{
			execution_source_pipeline = &pipeline;
			continue;
		}
		if (merge_context.hash_join_state != nullptr)
		{
			bool prefer_pipeline =
				(execution_source_pipeline->role != ParallelPipelineRole::HashProbeSource &&
				 pipeline.role == ParallelPipelineRole::HashProbeSource);
			if (!prefer_pipeline)
			{
				const ParallelPipelineRuntimeState *current_runtime =
					query_state->parallel_scheduler->get_pipeline_runtime(
						execution_source_pipeline->pipeline_id);

				prefer_pipeline =
					current_runtime != nullptr &&
					runtime->total_blocks != InvalidBlockNumber &&
					current_runtime->total_blocks != InvalidBlockNumber &&
					runtime->total_blocks > current_runtime->total_blocks;
			}
			if (prefer_pipeline)
				execution_source_pipeline = &pipeline;
		}
		else if (source_pipeline != nullptr)
		{
			/* handled below after we've seen the full set of candidates */
		}
		source_pipeline = &pipeline;
	}
	if (ready_aggregate_source_pipeline != nullptr)
		execution_source_pipeline = ready_aggregate_source_pipeline;
	else if (generic_aggregate_source_pipeline != nullptr)
		execution_source_pipeline = generic_aggregate_source_pipeline;
	if (execution_source_pipeline == nullptr)
	{
		execution_source_pipeline = source_pipeline;
	}
	if (execution_source_pipeline != nullptr &&
		execution_source_pipeline->role == ParallelPipelineRole::GenericSource &&
		hash_probe_aggregate_source_pipeline != nullptr &&
		query_state->vec_plan != nullptr)
	{
		VecAggState *generic_agg_state =
			query_state->vec_plan->find_parallel_aggregate_state_by_plan_node_id(
				execution_source_pipeline->agg_plan_node_id);

		/*
		 * File-backed grouped partials are a strong signal that the generic
		 * source path will spill and then force the leader to merge a large
		 * number of partial groups. Q4's DISTINCT-build-side shape is the
		 * canonical example. Prefer the hash-probe candidate instead and let
		 * the existing build-dominance guard decide whether process-parallel
		 * should proceed or fall back.
		 */
		if (generic_agg_state != nullptr &&
			generic_agg_state->uses_file_backed_parallel_partial_state())
		{
			if (pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: process parallel preferring hash probe source pipeline=%u over generic source pipeline=%u because generic grouped partials are file-backed",
					 hash_probe_aggregate_source_pipeline->pipeline_id,
					 execution_source_pipeline->pipeline_id);
			execution_source_pipeline = hash_probe_aggregate_source_pipeline;
		}
	}
	if (execution_source_pipeline == nullptr)
	{
		elog(WARNING, "pg_volvec: TryExecuteProcessParallelAggregate exit: no aggregate source pipeline");
		if (failure_reason != nullptr)
			*failure_reason = "parallel aggregate execution found no aggregate source pipeline";
		return false;
	}
	selected_agg_plan_node_id = execution_source_pipeline->agg_plan_node_id;
	/*
	 * HashProbeSource workers execute the full probe-side subtree through
	 * agg_state->consume_left_input(). They still need an explicit top-level
	 * hash-join handle now that the build side can be pre-built once and handed
	 * to workers through a shared bridge instead of being rebuilt per worker.
	 */
	need_hash_join_state =
		execution_source_pipeline->role == ParallelPipelineRole::HashBuildSource ||
		execution_source_pipeline->role == ParallelPipelineRole::HashProbeSource;
	if (!TryInitializeParallelMergeContext(query_state,
										   selected_agg_plan_node_id,
										   execution_source_pipeline->hash_join_plan_node_id,
										   need_hash_join_state,
										   &merge_context,
										   failure_reason) ||
		merge_context.agg_state == nullptr ||
		!merge_context.agg_state->supports_parallel_partial_state())
	{
		elog(WARNING, "pg_volvec: TryExecuteProcessParallelAggregate exit: merge context init failed (reason: %s)",
			 (failure_reason != nullptr && *failure_reason != nullptr) ? *failure_reason : "agg doesn't support parallel partial");
		if (failure_reason != nullptr && *failure_reason == nullptr)
			*failure_reason = "selected aggregate state does not support parallel partial state";
		return false;
	}
	file_backed_grouped_partials =
		merge_context.agg_state->uses_file_backed_parallel_partial_state();
	if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: process parallel selecting source pipeline=%u role=%s rel=%u plan_node_id=%d agg_plan_node_id=%d output_bridge=%u stage_mask=%u candidates=%d",
			 execution_source_pipeline->pipeline_id,
			 ParallelPipelineRoleDebugName(execution_source_pipeline->role),
			 execution_source_pipeline->scan_relid,
			 execution_source_pipeline->scan_plan_node_id,
			 execution_source_pipeline->agg_plan_node_id,
			 (unsigned) execution_source_pipeline->output_bridge,
			 execution_source_pipeline->stage_mask,
			 aggregate_source_count);

	source_runtime = query_state->parallel_scheduler->get_pipeline_runtime(execution_source_pipeline->pipeline_id);
	if (source_runtime == nullptr || source_runtime->total_blocks == InvalidBlockNumber ||
		source_runtime->total_blocks == 0)
	{
		elog(WARNING, "pg_volvec: TryExecuteProcessParallelAggregate exit: no eligible source blocks");
		if (failure_reason != nullptr)
			*failure_reason = "parallel aggregate execution found no eligible source blocks";
		return false;
	}
	if (execution_source_pipeline->role == ParallelPipelineRole::HashProbeSource)
	{
		BlockNumber build_blocks = 0;
		uint32_t build_pipeline_id = UINT32_MAX;
		double build_rows = -1.0;
		double source_rows = -1.0;
		const ParallelPipelineDesc *build_pipeline = nullptr;
		bool skip_for_build_dominance;

		skip_for_build_dominance =
			ShouldSkipHashProbeParallelForBuildDominatedDependency(query_state->parallel_plan,
																   query_state->parallel_scheduler,
																   execution_source_pipeline,
																   source_runtime,
																   &build_blocks,
																   &build_pipeline_id,
																   &build_rows,
																   &source_rows);
		build_pipeline =
			query_state->parallel_plan != nullptr
				? query_state->parallel_plan->get_pipeline(build_pipeline_id)
				: nullptr;
		source_rows =
			LookupPlannedStmtNodeRows(queryDesc->plannedstmt,
									  execution_source_pipeline->scan_plan_node_id);
		build_rows =
			LookupPlannedStmtNodeRows(queryDesc->plannedstmt,
									  build_pipeline != nullptr ? build_pipeline->scan_plan_node_id : -1);
		if (skip_for_build_dominance &&
			build_rows > 0.0 &&
			source_rows > 0.0 &&
			build_rows <= source_rows * VOLVEC_PARALLEL_HASH_BUILD_SMALL_ROWS_RATIO)
		{
			skip_for_build_dominance = false;
			if (pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: process parallel kept hash probe source pipeline=%u despite build block dominance because build rows=%lld are small relative to probe rows=%lld",
					 execution_source_pipeline->pipeline_id,
					 (long long) llround(build_rows),
					 (long long) llround(source_rows));
		}

		if (skip_for_build_dominance)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: process parallel skipped for hash probe source pipeline=%u blocks=%u build_rows=%lld because build dependency pipeline=%u blocks=%u probe_rows=%lld dominates by ratio>%u",
					 execution_source_pipeline->pipeline_id,
					 (unsigned) source_runtime->total_blocks,
					 (long long) llround(build_rows),
					 build_pipeline_id,
					 (unsigned) build_blocks,
					 (long long) llround(source_rows),
					 VOLVEC_PARALLEL_HASH_BUILD_DOMINANCE_RATIO);
			if (failure_reason != nullptr)
				*failure_reason =
					"parallel aggregate skipped because hash build dependency dominates probe source";
			return false;
		}
	}
	if (execution_source_pipeline->role == ParallelPipelineRole::HashProbeSource &&
		merge_context.hash_join_state != nullptr)
	{
		if (!TryExecuteParallelHashBuildDependencyChain(query_state,
															 queryDesc,
															 execution_source_pipeline,
															 failure_reason))
			return false;
		if (pg_volvec_trace_hooks)
			elog(LOG,
				 "pg_volvec: built parallel hash dependency chain for probe pipeline=%u",
				 execution_source_pipeline->pipeline_id);
		if (!BuildPublishedSharedHashBridgePack(query_state,
														 execution_source_pipeline,
														 (uint8_t **) &shared_hash_bridge,
														 &shared_hash_bridge_len,
														 &shared_hash_bridge_count,
														 failure_reason))
			return false;
		if (shared_hash_bridge == nullptr || shared_hash_bridge_len == 0)
		{
			if (failure_reason != nullptr)
				*failure_reason = "parallel aggregate failed to publish shared hash bridge";
			return false;
		}
		if (pg_volvec_trace_hooks)
			elog(LOG,
				 "pg_volvec: published shared hash bridge for probe pipeline=%u bytes=%zu entries=%zu chunks=%zu",
				 execution_source_pipeline->pipeline_id,
				 (size_t) shared_hash_bridge_len,
				 merge_context.hash_join_state->parallel_hash_entry_count(),
				 merge_context.hash_join_state->parallel_hash_chunk_count());
		if (allow_leader_participation &&
			(shared_hash_bridge_count > 1 ||
			 shared_hash_bridge_len >= VOLVEC_PARALLEL_LARGE_SHARED_HASH_BRIDGE_BYTES))
		{
			allow_leader_participation = false;
			if (pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: disabling leader participation for probe pipeline=%u because leader built %u shared hash bridge(s) totaling %zu bytes",
					 execution_source_pipeline->pipeline_id,
					 shared_hash_bridge_count,
					 (size_t) shared_hash_bridge_len);
		}
	}

	requested_workers = Min(pg_volvec_parallel_max_workers,
							(int) Max(source_runtime->estimated_morsels, 1u));
	if (requested_workers <= 0)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel aggregate execution requested zero workers";
		return false;
	}

	query_text = queryDesc->sourceText != nullptr ? queryDesc->sourceText : "";
	plannedstmt_serialized = nodeToString(queryDesc->plannedstmt);
	plannedstmt_len = strlen(plannedstmt_serialized) + 1;
	query_text_len = strlen(query_text) + 1;
	source_rel = table_open(execution_source_pipeline->scan_relid, NoLock);
	source_pscan_len = table_parallelscan_estimate(source_rel, queryDesc->estate->es_snapshot);

	EnterParallelMode();
	entered_parallel = true;
	pcxt = CreateParallelContext("pg_volvec",
								 "pg_volvec_parallel_worker_main",
								 requested_workers);

	shm_toc_estimate_chunk(&pcxt->estimator, sizeof(ParallelAggregateSharedControl));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator, plannedstmt_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator, query_text_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator, source_pscan_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	partials_len = mul_size(sizeof(ParallelAggPartialState), pcxt->nworkers);
	shm_toc_estimate_chunk(&pcxt->estimator, partials_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator, sizeof(SharedFileSet));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	if (shared_hash_bridge_len > 0)
	{
		shm_toc_estimate_chunk(&pcxt->estimator, shared_hash_bridge_len);
		shm_toc_estimate_keys(&pcxt->estimator, 1);
	}

	InitializeParallelDSM(pcxt);
	if (pcxt->seg == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel aggregate DSM initialization failed";
		goto done;
	}

	control = (ParallelAggregateSharedControl *)
		shm_toc_allocate(pcxt->toc, sizeof(ParallelAggregateSharedControl));
	control->magic = VOLVEC_PARALLEL_MAGIC;
	control->source_pipeline_id = execution_source_pipeline->pipeline_id;
	control->partial_slot_count = pcxt->nworkers;
	control->wave_pipeline_count = 0;
	control->morsel_nblocks = pg_volvec_parallel_morsel_nblocks;
	control->total_blocks = source_runtime->total_blocks;
	control->source_scan_relid = execution_source_pipeline->scan_relid;
	control->source_scan_plan_node_id = execution_source_pipeline->scan_plan_node_id;
	control->agg_plan_node_id = execution_source_pipeline->agg_plan_node_id;
	control->hash_join_plan_node_id = merge_context.hash_join_plan_node_id;
	control->input_hash_join_plan_node_id = merge_context.hash_join_plan_node_id;
	control->execution_mode = (uint32) ParallelWorkerExecutionMode::AggregateProbe;
	control->need_hash_join_state = need_hash_join_state;
	control->hash_bridge_ready = shared_hash_bridge_len > 0 ? 1 : 0;
	control->reserved[0] = 0;
	control->reserved[1] = 0;
	control->hash_bridge_size = shared_hash_bridge_len;
	pg_atomic_init_u64(&control->next_block, 0);
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_CONTROL, control);

	shared_plannedstmt = (char *) shm_toc_allocate(pcxt->toc, plannedstmt_len);
	memcpy(shared_plannedstmt, plannedstmt_serialized, plannedstmt_len);
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_PLANNEDSTMT, shared_plannedstmt);

	shared_query_text = (char *) shm_toc_allocate(pcxt->toc, query_text_len);
	memcpy(shared_query_text, query_text, query_text_len);
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_QUERY_TEXT, shared_query_text);

	source_pscan = (ParallelTableScanDesc) shm_toc_allocate(pcxt->toc, source_pscan_len);
	table_parallelscan_initialize(source_rel, source_pscan, queryDesc->estate->es_snapshot);
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_SOURCE_PSCAN, source_pscan);

	partials = (ParallelAggPartialState *) shm_toc_allocate(pcxt->toc, partials_len);
	memset(partials, 0, partials_len);
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_PARTIALS, partials);
	partial_fileset = (SharedFileSet *) shm_toc_allocate(pcxt->toc, sizeof(SharedFileSet));
	SharedFileSetInit(partial_fileset, pcxt->seg);
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_PARTIAL_FILESET, partial_fileset);
	if (shared_hash_bridge_len > 0)
	{
		shared_hash_bridge_dsm = (uint8_t *) shm_toc_allocate(pcxt->toc, shared_hash_bridge_len);
		memcpy(shared_hash_bridge_dsm, shared_hash_bridge, shared_hash_bridge_len);
		shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_HASH_BRIDGE, shared_hash_bridge_dsm);
	}
	table_close(source_rel, NoLock);
	source_rel = nullptr;

	LaunchParallelWorkers(pcxt);
	if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: launched %d/%d parallel workers for source pipeline %u",
			 pcxt->nworkers_launched,
			 pcxt->nworkers,
			 execution_source_pipeline->pipeline_id);
	if (pcxt->nworkers_launched == 0 && !allow_leader_participation)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel aggregate launched no workers and leader participation is disabled";
		goto done;
	}

	if (allow_leader_participation)
	{
		instr_time leader_exec_start;
		instr_time leader_exec_end;

		if (!TryInitializeLocalParallelAggregateProcessState(plannedstmt_serialized,
																 query_text,
																 execution_source_pipeline->agg_plan_node_id,
																 merge_context.hash_join_plan_node_id,
																 merge_context.hash_join_plan_node_id,
																 true,
																 need_hash_join_state,
																 shared_hash_bridge_dsm,
																 shared_hash_bridge_len,
																 execution_source_pipeline->scan_relid,
																 execution_source_pipeline->scan_plan_node_id,
																 source_pscan,
															 true,
															 &leader_local_state,
															 &leader_failure_reason))
		{
			if (failure_reason != nullptr)
				*failure_reason = leader_failure_reason != nullptr ?
					leader_failure_reason :
					"parallel aggregate leader local init failed";
			goto done;
		}
		leader_local_initialized = true;
		INSTR_TIME_SET_CURRENT(leader_exec_start);
		if (!ExecuteParallelWorkerSourceLoop(ParallelWorkerExecutionMode::AggregateProbe,
											 leader_local_state.worker_context,
											 failure_reason))
			goto done;
		INSTR_TIME_SET_CURRENT(leader_exec_end);
		INSTR_TIME_SUBTRACT(leader_exec_end, leader_exec_start);
		leader_local_state.exec_time_us = (uint64) INSTR_TIME_GET_MICROSEC(leader_exec_end);
		if (file_backed_grouped_partials)
		{
			BufFile *file;
			char file_name[VOLVEC_PARALLEL_MAX_PARTIAL_FILE_NAME];

			FormatParallelPartialFileName(file_name, sizeof(file_name), "leader", 0);
			file = BufFileCreateFileSet(&partial_fileset->fs, file_name);
			if (file == nullptr ||
				!leader_local_state.worker_context.agg_state->export_parallel_grouped_partial_file(
					file,
					&leader_partial))
			{
				if (file != nullptr)
					BufFileClose(file);
				if (failure_reason != nullptr)
					*failure_reason = "parallel aggregate leader grouped partial export failed";
				goto done;
			}
			strlcpy(leader_partial.grouped_file_name,
					file_name,
					sizeof(leader_partial.grouped_file_name));
			BufFileExportFileSet(file);
			BufFileClose(file);
		}
		else if (!leader_local_state.worker_context.agg_state->export_parallel_partial_state(&leader_partial))
		{
			if (failure_reason != nullptr)
				*failure_reason = "parallel aggregate leader partial export failed";
			goto done;
		}
		PopulatePartialDiagnostics(leader_local_state, &leader_partial);
	}

	WaitForParallelWorkersToFinish(pcxt);
	if (leader_local_initialized)
	{
		if (pg_volvec_trace_hooks)
			elog(LOG,
				 "pg_volvec: parallel leader local partial init_us=%llu exec_us=%llu blocks=%llu batches=%llu rows=%llu",
				 (unsigned long long) leader_partial.init_time_us,
				 (unsigned long long) leader_partial.exec_time_us,
				 (unsigned long long) leader_partial.blocks_opened,
				 (unsigned long long) leader_partial.input_batches,
				 (unsigned long long) leader_partial.input_rows);
		if (leader_partial.file_backed)
		{
			BufFile *file = BufFileOpenFileSet(&partial_fileset->fs,
												 leader_partial.grouped_file_name,
												 O_RDONLY,
												 false);

			if (file == nullptr ||
				!merge_context.agg_state->merge_parallel_grouped_partial_file(file, leader_partial))
			{
				if (file != nullptr)
					BufFileClose(file);
				if (failure_reason != nullptr)
					*failure_reason = "parallel aggregate leader grouped partial merge failed";
				goto done;
			}
			BufFileClose(file);
			BufFileDeleteFileSet(&partial_fileset->fs,
								 leader_partial.grouped_file_name,
								 true);
		}
		else if (!merge_context.agg_state->merge_parallel_partial_state(leader_partial))
		{
			if (failure_reason != nullptr)
				*failure_reason = "parallel aggregate leader partial merge failed";
			goto done;
		}
	}
	for (int i = 0; i < pcxt->nworkers_launched; i++)
	{
		if (pg_volvec_trace_hooks)
			elog(LOG,
				 "pg_volvec: parallel worker[%d] partial init_us=%llu exec_us=%llu blocks=%llu batches=%llu rows=%llu",
				 i,
				 (unsigned long long) partials[i].init_time_us,
				 (unsigned long long) partials[i].exec_time_us,
				 (unsigned long long) partials[i].blocks_opened,
				 (unsigned long long) partials[i].input_batches,
				 (unsigned long long) partials[i].input_rows);
		if (partials[i].file_backed)
		{
			BufFile *file = BufFileOpenFileSet(&partial_fileset->fs,
												 partials[i].grouped_file_name,
												 O_RDONLY,
												 false);

			if (file == nullptr ||
				!merge_context.agg_state->merge_parallel_grouped_partial_file(file, partials[i]))
			{
				if (file != nullptr)
					BufFileClose(file);
				if (failure_reason != nullptr)
					*failure_reason = "parallel aggregate grouped partial merge failed";
				goto done;
			}
			BufFileClose(file);
			BufFileDeleteFileSet(&partial_fileset->fs,
								 partials[i].grouped_file_name,
								 true);
		}
		else if (!merge_context.agg_state->merge_parallel_partial_state(partials[i]))
		{
			if (failure_reason != nullptr)
				*failure_reason = "parallel aggregate merge failed";
			goto done;
		}
	}
	/* Load the shared hash bridge for probe-side execution */
	if (merge_context.hash_join_state != nullptr &&
		execution_source_pipeline->role == ParallelPipelineRole::HashProbeSource)
	{
		merge_context.hash_join_state->load_hash_bridge();
	}
	merge_context.agg_state->finish_sink();
	success = true;

done:
	if (!success && failure_reason != nullptr && *failure_reason == nullptr)
		*failure_reason = "unknown process parallel aggregate failure";
	CleanupLocalParallelAggregateProcessState(&leader_local_state);
	if (source_rel != nullptr)
		table_close(source_rel, NoLock);
	if (plannedstmt_serialized != nullptr)
		pfree(plannedstmt_serialized);
	if (partial_fileset != nullptr)
		SharedFileSetDeleteAll(partial_fileset);
	if (pcxt != nullptr)
		DestroyParallelContext(pcxt);
	if (entered_parallel)
		ExitParallelMode();
	return success;
}
