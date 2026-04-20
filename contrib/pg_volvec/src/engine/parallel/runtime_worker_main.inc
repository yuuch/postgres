namespace {

using namespace pg_volvec;

static uint64
ElapsedUsSince(instr_time start)
{
	instr_time end;

	INSTR_TIME_SET_CURRENT(end);
	INSTR_TIME_SUBTRACT(end, start);
	return (uint64) INSTR_TIME_GET_MICROSEC(end);
}

static bool
QuerySchedulerPipelineHasSuccessor(const ParallelQueryPipelineShared *pipeline,
								   const uint32_t *query_successors,
								   uint32 successor_id)
{
	if (pipeline == nullptr || query_successors == nullptr)
		return false;
	for (uint32 i = 0; i < pipeline->successor_count; i++)
	{
		if (query_successors[pipeline->successor_start + i] == successor_id)
			return true;
	}
	return false;
}

static uint32
FindSinglePredecessorPipeline(const ParallelQuerySchedulerShared *query_scheduler,
							  const ParallelQueryPipelineShared *query_pipelines,
							  const uint32_t *query_successors,
							  uint32 pipeline_id,
							  ParallelPipelineRole role)
{
	uint32 found = UINT32_MAX;

	if (query_scheduler == nullptr || query_pipelines == nullptr ||
		query_successors == nullptr)
		return UINT32_MAX;
	for (uint32 pred_id = 0; pred_id < query_scheduler->pipeline_count; pred_id++)
	{
		const ParallelQueryPipelineShared *pred = &query_pipelines[pred_id];

		if ((ParallelPipelineRole) pred->role != role ||
			!QuerySchedulerPipelineHasSuccessor(pred, query_successors, pipeline_id))
			continue;
		if (found != UINT32_MAX)
			return UINT32_MAX;
		found = pred_id;
	}
	return found;
}

static bool
MergeQueryAggregateInputBridge(SharedFileSet *partial_fileset,
							   const ParallelQuerySchedulerShared *query_scheduler,
							   const ParallelQueryPipelineShared *query_pipelines,
							   const uint32_t *query_successors,
							   ParallelAggPartialState *query_agg_partials,
							   uint32 pipeline_id,
							   VecAggState *agg_state,
							   const char **failure_reason)
{
	bool merged_any = false;

	if (failure_reason != nullptr)
		*failure_reason = nullptr;
	if (partial_fileset == nullptr || query_scheduler == nullptr ||
		query_pipelines == nullptr || query_successors == nullptr ||
		query_agg_partials == nullptr || agg_state == nullptr ||
		pipeline_id >= query_scheduler->pipeline_count)
	{
		if (failure_reason != nullptr)
			*failure_reason =
				"query scheduler aggregate bridge merge has incomplete state";
		return false;
	}

	for (uint32 pred_id = 0; pred_id < query_scheduler->pipeline_count; pred_id++)
	{
		const ParallelQueryPipelineShared *pred = &query_pipelines[pred_id];

		if (!QuerySchedulerPipelineHasSuccessor(pred, query_successors, pipeline_id))
			continue;
		for (uint32 worker = 0; worker < query_scheduler->worker_count; worker++)
		{
			size_t slot_index =
				(size_t) pred_id * query_scheduler->worker_count +
				(size_t) worker;
			ParallelAggPartialState *partial = &query_agg_partials[slot_index];

			if (partial->naggs == 0)
				continue;
			if (partial->file_backed)
			{
				BufFile *file = BufFileOpenFileSet(&partial_fileset->fs,
												   partial->grouped_file_name,
												   O_RDONLY,
												   false);

				if (file == nullptr ||
					!agg_state->merge_parallel_grouped_partial_file(file,
																	*partial))
				{
					if (file != nullptr)
						BufFileClose(file);
					if (failure_reason != nullptr)
						*failure_reason =
							"query scheduler aggregate bridge file merge failed";
					return false;
				}
				BufFileClose(file);
			}
			else if (!agg_state->merge_parallel_partial_state(*partial))
			{
				if (failure_reason != nullptr)
					*failure_reason =
						"query scheduler aggregate bridge inline merge failed";
				return false;
			}
			merged_any = true;
			if (pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: query scheduler aggregate bridge merge target=%u pred=%u worker=%u file=%s groups=%u rows=%llu bytes=%llu",
					 pipeline_id,
					 pred_id,
					 worker,
					 partial->file_backed ? "on" : "off",
					 partial->group_count,
					 (unsigned long long) partial->input_rows,
					 (unsigned long long) partial->file_bytes);
		}
	}
	if (!merged_any)
	{
		if (failure_reason != nullptr)
			*failure_reason =
				"query scheduler aggregate bridge found no partials";
		return false;
	}
	agg_state->finish_sink();
	return true;
}

static bool
CompleteQueryPipelineAndReleaseSuccessors(ParallelQuerySchedulerShared *query_scheduler,
										  ParallelQueryPipelineShared *query_pipelines,
										  const uint32_t *query_successors,
										  ParallelQueryPipelineShared *pipeline)
{
	uint32 already_completed = 0;

	if (query_scheduler == nullptr || query_pipelines == nullptr ||
		query_successors == nullptr || pipeline == nullptr)
		return false;
	if (!pg_atomic_compare_exchange_u32(&pipeline->completed,
										&already_completed,
										1))
		return false;
	for (uint32 successor_index = 0;
		 successor_index < pipeline->successor_count;
		 successor_index++)
	{
		uint32 successor_id =
			query_successors[pipeline->successor_start + successor_index];
		ParallelQueryPipelineShared *successor;

		if (successor_id >= query_scheduler->pipeline_count)
			continue;
		successor = &query_pipelines[successor_id];
		(void) pg_atomic_sub_fetch_u32(&successor->remaining_dependencies, 1);
	}
	return true;
}

static bool
ReadQueryBridgePackFile(SharedFileSet *partial_fileset,
						const ParallelQueryPipelineShared *producer,
						VolVecVector<uint8_t> *out,
						const char **failure_reason)
{
	BufFile *file;

	if (failure_reason != nullptr)
		*failure_reason = nullptr;
	if (partial_fileset == nullptr || producer == nullptr || out == nullptr ||
		pg_atomic_read_u32(
			const_cast<pg_atomic_uint32 *>(&producer->hash_bridge_ready)) == 0 ||
		producer->hash_bridge_size == 0 ||
		producer->hash_bridge_size > MaxAllocSize)
	{
		if (failure_reason != nullptr)
			*failure_reason = "query scheduler bridge dependency is not ready";
		return false;
	}
	pg_read_barrier();
	out->clear();
	out->resize((size_t) producer->hash_bridge_size);
	instr_time read_start;

	INSTR_TIME_SET_CURRENT(read_start);
	file = BufFileOpenFileSet(&partial_fileset->fs,
							  producer->hash_bridge_file_name,
							  O_RDONLY,
							  false);
	if (file == nullptr ||
		!BufFileReadAllLocal(file, out->data(), out->size(), false))
	{
		if (file != nullptr)
			BufFileClose(file);
		if (failure_reason != nullptr)
			*failure_reason = "query scheduler could not read hash bridge pack";
		return false;
	}
	BufFileClose(file);
	if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: query scheduler bridge read pipeline=%u bytes=%llu entries=%llu chunks=%llu us=%llu",
			 producer->pipeline_id,
			 (unsigned long long) producer->hash_bridge_size,
			 (unsigned long long) producer->hash_bridge_entries,
			 (unsigned long long) producer->hash_bridge_chunks,
			 (unsigned long long) ElapsedUsSince(read_start));
	return true;
}

static bool
BuildQueryDependencyBridgePack(SharedFileSet *partial_fileset,
							   const ParallelQuerySchedulerShared *query_scheduler,
							   const ParallelQueryPipelineShared *query_pipelines,
							   const uint32_t *query_successors,
							   uint32 pipeline_id,
							   uint8_t **buffer_out,
							   size_t *buffer_size_out,
							   const char **failure_reason)
{
	VolVecVector<VolVecVector<uint8_t>> dependency_packs{
		PgMemoryContextAllocator<VolVecVector<uint8_t>>(CurrentMemoryContext)};
	size_t total_size = sizeof(SerializedSharedHashBridgePackHeader);
	uint32 bridge_count = 0;
	uint8_t *buffer;
	size_t offset;
	instr_time pack_start;

	if (buffer_out != nullptr)
		*buffer_out = nullptr;
	if (buffer_size_out != nullptr)
		*buffer_size_out = 0;
	if (failure_reason != nullptr)
		*failure_reason = nullptr;
	if (partial_fileset == nullptr || query_scheduler == nullptr ||
		query_pipelines == nullptr || query_successors == nullptr ||
		pipeline_id >= query_scheduler->pipeline_count)
		return true;
	INSTR_TIME_SET_CURRENT(pack_start);

	for (uint32 pred_id = 0; pred_id < query_scheduler->pipeline_count; pred_id++)
	{
		const ParallelQueryPipelineShared *pred = &query_pipelines[pred_id];
		VolVecVector<uint8_t> pack{
			PgMemoryContextAllocator<uint8_t>(CurrentMemoryContext)};
		SerializedSharedHashBridgePackHeader header{};
		size_t pack_offset = 0;

		if ((ParallelPipelineRole) pred->role !=
				ParallelPipelineRole::HashBuildFinalize ||
			!QuerySchedulerPipelineHasSuccessor(pred, query_successors, pipeline_id))
			continue;
		if (!ReadQueryBridgePackFile(partial_fileset,
									 pred,
									 &pack,
									 failure_reason))
			return false;
		if (pack.size() < sizeof(header))
		{
			if (failure_reason != nullptr)
				*failure_reason =
					"query scheduler hash bridge pack is truncated";
			return false;
		}
		memcpy(&header, pack.data(), sizeof(header));
		if (header.magic != VOLVEC_SHARED_HASH_BRIDGE_PACK_MAGIC ||
			header.version != VOLVEC_SHARED_HASH_BRIDGE_PACK_VERSION ||
			header.bridge_count == 0)
		{
			if (failure_reason != nullptr)
				*failure_reason =
					"query scheduler hash bridge pack header mismatch";
			return false;
		}
		pack_offset = sizeof(header);
		for (uint32 i = 0; i < header.bridge_count; i++)
		{
			SerializedSharedHashBridgePackEntryHeader entry_header{};

			if (pack_offset > pack.size() ||
				sizeof(entry_header) > pack.size() - pack_offset)
			{
				if (failure_reason != nullptr)
					*failure_reason =
						"query scheduler hash bridge pack entry header is truncated";
				return false;
			}
			memcpy(&entry_header, pack.data() + pack_offset, sizeof(entry_header));
			pack_offset += sizeof(entry_header);
			if (entry_header.bridge_size == 0 ||
				pack_offset > pack.size() ||
				entry_header.bridge_size > pack.size() - pack_offset)
			{
				if (failure_reason != nullptr)
					*failure_reason =
						"query scheduler hash bridge pack entry size is invalid";
				return false;
			}
			pack_offset += entry_header.bridge_size;
		}
		if (pack_offset != pack.size())
		{
			if (failure_reason != nullptr)
				*failure_reason =
					"query scheduler hash bridge pack has trailing bytes";
			return false;
		}
		if (pack.size() - sizeof(header) > MaxAllocSize ||
			total_size > MaxAllocSize ||
			pack.size() - sizeof(header) > MaxAllocSize - total_size)
		{
			if (failure_reason != nullptr)
				*failure_reason =
					"query scheduler dependency bridge pack exceeds MaxAllocSize";
			return false;
		}
		total_size += pack.size() - sizeof(header);
		bridge_count += header.bridge_count;
		dependency_packs.push_back(std::move(pack));
	}

	if (bridge_count == 0)
		return true;
	buffer = (uint8_t *) MemoryContextAlloc(CurrentMemoryContext, total_size);
	SerializedSharedHashBridgePackHeader header{};
	header.bridge_count = bridge_count;
	memcpy(buffer, &header, sizeof(header));
	offset = sizeof(header);
	for (const auto &pack : dependency_packs)
	{
		size_t payload_size = pack.size() - sizeof(header);

		memcpy(buffer + offset, pack.data() + sizeof(header), payload_size);
		offset += payload_size;
	}
	if (offset != total_size)
	{
		if (failure_reason != nullptr)
			*failure_reason =
				"query scheduler dependency bridge pack size mismatch";
		return false;
	}
	if (buffer_out != nullptr)
		*buffer_out = buffer;
	if (buffer_size_out != nullptr)
		*buffer_size_out = total_size;
	if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: query scheduler dependency bridge pack pipeline=%u bridges=%u bytes=%zu us=%llu",
			 pipeline_id,
			 bridge_count,
			 total_size,
			 (unsigned long long) ElapsedUsSince(pack_start));
	return true;
}

static bool
WriteQueryHashBridgePackFile(SharedFileSet *partial_fileset,
							 ParallelQueryPipelineShared *pipeline,
							 VecHashJoinState *hash_join_state,
							 const char **failure_reason)
{
	BufFile *file;
	char file_name[VOLVEC_PARALLEL_MAX_PARTIAL_FILE_NAME];
	SerializedSharedHashBridgePackHeader pack_header{};
	SerializedSharedHashBridgePackEntryHeader entry_header{};
	size_t bridge_size;
	size_t total_size;
	instr_time write_start;

	if (failure_reason != nullptr)
		*failure_reason = nullptr;
	if (partial_fileset == nullptr || pipeline == nullptr ||
		hash_join_state == nullptr ||
		hash_join_state->shared_hash_bridge_buffer() == nullptr ||
		hash_join_state->shared_hash_bridge_size() == 0)
	{
		if (failure_reason != nullptr)
			*failure_reason =
				"query scheduler cannot publish an empty hash bridge";
		return false;
	}
	bridge_size = hash_join_state->shared_hash_bridge_size();
	if (bridge_size > MaxAllocSize ||
		sizeof(pack_header) + sizeof(entry_header) > MaxAllocSize ||
		bridge_size > MaxAllocSize - sizeof(pack_header) - sizeof(entry_header))
	{
		if (failure_reason != nullptr)
			*failure_reason =
				"query scheduler hash bridge pack exceeds MaxAllocSize";
		return false;
	}
	total_size = sizeof(pack_header) + sizeof(entry_header) + bridge_size;
	snprintf(file_name,
			 sizeof(file_name),
			 "pg_volvec_qbridge_%u",
			 pipeline->pipeline_id);
	INSTR_TIME_SET_CURRENT(write_start);
	file = BufFileCreateFileSet(&partial_fileset->fs, file_name);
	if (file == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason =
				"query scheduler could not create hash bridge pack file";
		return false;
	}
	pack_header.bridge_count = 1;
	entry_header.hash_join_plan_node_id = pipeline->hash_join_plan_node_id;
	entry_header.bridge_size = (uint32) bridge_size;
	if (!BufFileWriteAllLocal(file, &pack_header, sizeof(pack_header)) ||
		!BufFileWriteAllLocal(file, &entry_header, sizeof(entry_header)) ||
		!BufFileWriteAllLocal(file,
							  hash_join_state->shared_hash_bridge_buffer(),
							  bridge_size))
	{
		BufFileClose(file);
		if (failure_reason != nullptr)
			*failure_reason =
				"query scheduler could not write hash bridge pack file";
		return false;
	}
	strlcpy(pipeline->hash_bridge_file_name,
			file_name,
			sizeof(pipeline->hash_bridge_file_name));
	pipeline->hash_bridge_size = (uint64) total_size;
	pipeline->hash_bridge_entries =
		(uint64) hash_join_state->parallel_hash_entry_count();
	pipeline->hash_bridge_chunks =
		(uint64) hash_join_state->parallel_hash_chunk_count();
	BufFileExportFileSet(file);
	BufFileClose(file);
	pg_write_barrier();
	pg_atomic_write_u32(&pipeline->hash_bridge_ready, 1);
	if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: query scheduler hash bridge write pipeline=%u hash_node=%d bytes=%zu entries=%zu chunks=%zu us=%llu",
			 pipeline->pipeline_id,
			 pipeline->hash_join_plan_node_id,
			 total_size,
			 hash_join_state->parallel_hash_entry_count(),
			 hash_join_state->parallel_hash_chunk_count(),
			 (unsigned long long) ElapsedUsSince(write_start));
	return true;
}

static bool
FinalizeQueryHashBuildPipeline(SharedFileSet *partial_fileset,
							   const char *plannedstmt_serialized,
							   const char *query_text,
							   ParallelQuerySchedulerShared *query_scheduler,
							   ParallelQueryPipelineShared *query_pipelines,
							   const uint32_t *query_successors,
							   ParallelHashBuildPartialState *query_hash_partials,
							   ParallelQueryPipelineShared *pipeline,
							   const char **failure_reason)
{
	uint32 build_pipeline_id;
	LocalParallelAggregateProcessState finalize_state;
	size_t total_entries = 0;
	size_t total_chunks = 0;
	size_t total_file_bytes = 0;
	uint64 init_us = 0;
	uint64 merge_us = 0;
	uint64 publish_us = 0;
	instr_time total_start;
	instr_time init_start;
	instr_time merge_start;
	instr_time publish_start;

	if (failure_reason != nullptr)
		*failure_reason = nullptr;
	memset(&finalize_state, 0, sizeof(finalize_state));
	INSTR_TIME_SET_CURRENT(total_start);
	if (partial_fileset == nullptr || query_scheduler == nullptr ||
		query_pipelines == nullptr || query_successors == nullptr ||
		query_hash_partials == nullptr || pipeline == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason =
				"query scheduler hash finalize has incomplete state";
		return false;
	}
	pg_read_barrier();
	build_pipeline_id =
		FindSinglePredecessorPipeline(query_scheduler,
									  query_pipelines,
									  query_successors,
									  pipeline->pipeline_id,
									  ParallelPipelineRole::HashBuildSource);
	if (build_pipeline_id == UINT32_MAX)
	{
		if (failure_reason != nullptr)
			*failure_reason =
				"query scheduler hash finalize could not find build source predecessor";
		return false;
	}
	INSTR_TIME_SET_CURRENT(init_start);
	if (!TryInitializeLocalParallelAggregateProcessState(plannedstmt_serialized,
														 query_text,
														 -1,
														 pipeline->hash_join_plan_node_id,
														 pipeline->input_hash_join_plan_node_id,
														 false,
														 true,
														 nullptr,
														 0,
														 InvalidOid,
														 -1,
														 nullptr,
														 false,
														 &finalize_state,
														 failure_reason))
		return false;
	init_us = ElapsedUsSince(init_start);
	for (uint32 worker = 0; worker < query_scheduler->worker_count; worker++)
	{
		size_t slot_index =
			(size_t) build_pipeline_id * query_scheduler->worker_count +
			(size_t) worker;
		ParallelHashBuildPartialState *partial = &query_hash_partials[slot_index];

		total_entries += (size_t) partial->entry_count;
		total_chunks += (size_t) partial->chunk_count;
		total_file_bytes += (size_t) partial->file_bytes;
	}
	INSTR_TIME_SET_CURRENT(merge_start);
	finalize_state.worker_context.hash_join_state->
		reserve_parallel_hash_build_capacity(total_entries, total_chunks);
	for (uint32 worker = 0; worker < query_scheduler->worker_count; worker++)
	{
		size_t slot_index =
			(size_t) build_pipeline_id * query_scheduler->worker_count +
			(size_t) worker;
		ParallelHashBuildPartialState *partial = &query_hash_partials[slot_index];
		BufFile *file;

		if (partial->file_bytes == 0)
			continue;
		file = BufFileOpenFileSet(&partial_fileset->fs,
								  partial->file_name,
								  O_RDONLY,
								  false);
		if (file == nullptr ||
			!finalize_state.worker_context.hash_join_state->
				merge_parallel_build_partial_file(file, *partial))
		{
			if (file != nullptr)
				BufFileClose(file);
			if (failure_reason != nullptr)
				*failure_reason =
					"query scheduler hash finalize partial merge failed";
			CleanupLocalParallelAggregateProcessState(&finalize_state);
			return false;
		}
		BufFileClose(file);
		BufFileDeleteFileSet(&partial_fileset->fs, partial->file_name, true);
	}
	merge_us = ElapsedUsSince(merge_start);
	INSTR_TIME_SET_CURRENT(publish_start);
	finalize_state.worker_context.hash_join_state->finish_parallel_hash_build();
	finalize_state.worker_context.hash_join_state->publish_hash_bridge();
	if (!WriteQueryHashBridgePackFile(partial_fileset,
									  pipeline,
									  finalize_state.worker_context.hash_join_state,
									  failure_reason))
	{
		CleanupLocalParallelAggregateProcessState(&finalize_state);
		return false;
	}
	publish_us = ElapsedUsSince(publish_start);
	if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: query scheduler hash finalize pipeline=%u build_pipeline=%u hash_node=%d partial_bytes=%zu entries=%zu chunks=%zu init_us=%llu merge_us=%llu publish_write_us=%llu total_us=%llu",
			 pipeline->pipeline_id,
			 build_pipeline_id,
			 pipeline->hash_join_plan_node_id,
			 total_file_bytes,
			 total_entries,
			 total_chunks,
			 (unsigned long long) init_us,
			 (unsigned long long) merge_us,
			 (unsigned long long) publish_us,
			 (unsigned long long) ElapsedUsSince(total_start));
	CleanupLocalParallelAggregateProcessState(&finalize_state);
	return true;
}

static bool
PublishCompletedQuerySourcePipeline(SharedFileSet *partial_fileset,
									const ParallelQuerySchedulerShared *query_scheduler,
									ParallelQueryPipelineShared *query_pipelines,
									ParallelAggPartialState *query_agg_partials,
									ParallelHashBuildPartialState *query_hash_partials,
									uint32 pipeline_id,
									int worker_number,
									LocalParallelAggregateProcessState **pipeline_states,
									bool *pipeline_published,
									const char **failure_reason)
{
	ParallelQueryPipelineShared *pipeline;
	LocalParallelAggregateProcessState *pipeline_state;
	size_t slot_index;

	if (failure_reason != nullptr)
		*failure_reason = nullptr;
	if (partial_fileset == nullptr || query_scheduler == nullptr ||
		query_pipelines == nullptr || query_agg_partials == nullptr ||
		query_hash_partials == nullptr || pipeline_states == nullptr ||
		pipeline_published == nullptr ||
		pipeline_id >= query_scheduler->pipeline_count)
		return false;
	if (pipeline_published[pipeline_id])
		return true;
	pipeline = &query_pipelines[pipeline_id];
	if ((ParallelTaskKind) pipeline->task_kind != ParallelTaskKind::SourceMorsel)
		return true;
	if (pipeline->total_tasks == 0 ||
		pg_atomic_read_u32(&pipeline->completed_tasks) < pipeline->total_tasks)
		return true;
	if (worker_number < 0 ||
		(uint32) worker_number >= query_scheduler->worker_count)
	{
		if (failure_reason != nullptr)
			*failure_reason =
				"query scheduler worker number is out of range";
		return false;
	}
	pipeline_state = pipeline_states[pipeline_id];
	slot_index =
		(size_t) pipeline_id * query_scheduler->worker_count +
		(size_t) worker_number;
	if (pipeline_state != nullptr &&
		(ParallelPipelineRole) pipeline->role ==
			ParallelPipelineRole::HashBuildSource &&
		pipeline_state->worker_context.hash_join_state != nullptr)
	{
		ParallelHashBuildPartialState *partial = &query_hash_partials[slot_index];
		BufFile *file;
		char file_name[VOLVEC_PARALLEL_MAX_PARTIAL_FILE_NAME];
		instr_time export_start;

		snprintf(file_name,
				 sizeof(file_name),
				 "pg_volvec_qbuild_%u_%d",
				 pipeline_id,
				 worker_number);
		INSTR_TIME_SET_CURRENT(export_start);
		file = BufFileCreateFileSet(&partial_fileset->fs, file_name);
		if (file == nullptr ||
			!pipeline_state->worker_context.hash_join_state->
				export_parallel_build_partial_file(file, partial))
		{
			if (file != nullptr)
				BufFileClose(file);
			if (failure_reason != nullptr)
				*failure_reason =
					"query scheduler hash build partial export failed";
			return false;
		}
		strlcpy(partial->file_name, file_name, sizeof(partial->file_name));
		BufFileExportFileSet(file);
		BufFileClose(file);
		PopulateHashBuildPartialDiagnostics(*pipeline_state, partial);
		if (pg_volvec_trace_hooks)
			elog(LOG,
				 "pg_volvec: query scheduler hash partial export pipeline=%u worker=%d hash_node=%d bytes=%llu entries=%llu chunks=%llu init_us=%llu exec_us=%llu export_us=%llu",
				 pipeline_id,
				 worker_number,
				 pipeline->hash_join_plan_node_id,
				 (unsigned long long) partial->file_bytes,
				 (unsigned long long) partial->entry_count,
				 (unsigned long long) partial->chunk_count,
				 (unsigned long long) pipeline_state->init_time_us,
				 (unsigned long long) pipeline_state->exec_time_us,
				 (unsigned long long) ElapsedUsSince(export_start));
	}
	if (pipeline_state != nullptr &&
		pipeline_state->worker_context.agg_state != nullptr)
	{
		ParallelAggPartialState *partial = &query_agg_partials[slot_index];
		bool exported_inline;
		bool exported_file = false;
		instr_time export_start;

		/*
		 * Prefer the DSM metadata slot for small grouped aggregates.  TPC-H
		 * Q7/Q12 only publish a handful of groups, so forcing them through a
		 * SharedFileSet costs far more than the payload itself.  Fall back to
		 * files only when the inline representation overflows.
		 */
		INSTR_TIME_SET_CURRENT(export_start);
		exported_inline = pipeline_state->worker_context.agg_state->
			export_parallel_partial_state(partial);
		if (!exported_inline &&
			pipeline_state->worker_context.agg_state->
				uses_file_backed_parallel_partial_state())
		{
			BufFile *file;
			char file_name[VOLVEC_PARALLEL_MAX_PARTIAL_FILE_NAME];

			FormatParallelPartialFileName(file_name,
										 sizeof(file_name),
										 "query_worker",
										 (int) slot_index);
			file = BufFileCreateFileSet(&partial_fileset->fs, file_name);
			if (file == nullptr ||
				!pipeline_state->worker_context.agg_state->
					export_parallel_grouped_partial_file(file, partial))
			{
				if (file != nullptr)
					BufFileClose(file);
				if (failure_reason != nullptr)
					*failure_reason =
						"query scheduler grouped partial export failed";
				return false;
			}
			strlcpy(partial->grouped_file_name,
					file_name,
					sizeof(partial->grouped_file_name));
			BufFileExportFileSet(file);
			BufFileClose(file);
			exported_file = true;
		}
		else if (!exported_inline)
		{
			if (failure_reason != nullptr)
				*failure_reason =
					"query scheduler inline partial export failed";
			return false;
		}
		PopulatePartialDiagnostics(*pipeline_state, partial);
		if (pg_volvec_trace_hooks)
			elog(LOG,
				 "pg_volvec: query scheduler agg partial export pipeline=%u worker=%d file=%s groups=%u bytes=%llu rows=%llu init_us=%llu exec_us=%llu export_us=%llu",
				 pipeline_id,
				 worker_number,
				 exported_file ? "on" : "off",
				 partial->group_count,
				 (unsigned long long) partial->file_bytes,
				 (unsigned long long) partial->input_rows,
				 (unsigned long long) pipeline_state->init_time_us,
				 (unsigned long long) pipeline_state->exec_time_us,
				 (unsigned long long) ElapsedUsSince(export_start));
	}
	if (pipeline_state != nullptr)
	{
		CleanupLocalParallelAggregateProcessState(pipeline_state);
		pipeline_states[pipeline_id] = nullptr;
	}
	pipeline_published[pipeline_id] = true;
	pg_write_barrier();
	(void) pg_atomic_fetch_add_u32(&pipeline->published_workers, 1);
	return true;
}

} /* namespace */

extern "C" PGDLLEXPORT void
pg_volvec_parallel_worker_main(dsm_segment *seg, shm_toc *toc)
{
	using namespace pg_volvec;
	ParallelAggregateSharedControl *control;
	ParallelAggPartialState *partials = nullptr;
	ParallelAggPartialState *query_agg_partials = nullptr;
	ParallelHashBuildPartialState *query_hash_partials = nullptr;
	ParallelSortRunState *query_sort_runs = nullptr;
	ParallelHashBuildPartialState *build_partials = nullptr;
	ParallelHashBuildWavePipelineShared *wave_pipelines = nullptr;
	ParallelQuerySchedulerShared *query_scheduler = nullptr;
	ParallelQueryPipelineShared *query_pipelines = nullptr;
	ParallelQueryTaskShared *query_tasks = nullptr;
	uint32_t *query_successors = nullptr;
	ParallelTableScanDesc source_pscan = nullptr;
	SharedFileSet *partial_fileset;
	const uint8_t *shared_hash_bridge = nullptr;
	const uint8_t *serialized_param_exec = nullptr;
	size_t serialized_param_exec_size = 0;
	const char *plannedstmt_serialized;
	const char *query_text;
	LocalParallelAggregateProcessState *local_state;
	const char *failure_reason = nullptr;
	instr_time exec_start;
	instr_time exec_end;
	ParallelWorkerExecutionMode execution_mode;

	control = (ParallelAggregateSharedControl *) shm_toc_lookup(toc,
																VOLVEC_PARALLEL_KEY_CONTROL,
																false);
	source_pscan = (ParallelTableScanDesc) shm_toc_lookup(toc,
															 VOLVEC_PARALLEL_KEY_SOURCE_PSCAN,
															 true);
	partial_fileset = (SharedFileSet *) shm_toc_lookup(toc,
															VOLVEC_PARALLEL_KEY_PARTIAL_FILESET,
															false);
	plannedstmt_serialized = (const char *) shm_toc_lookup(toc,
														   VOLVEC_PARALLEL_KEY_PLANNEDSTMT,
														   false);
	query_text = (const char *) shm_toc_lookup(toc,
												VOLVEC_PARALLEL_KEY_QUERY_TEXT,
												true);
	serialized_param_exec = (const uint8_t *) shm_toc_lookup(toc,
															 VOLVEC_PARALLEL_KEY_PARAM_EXEC,
															 true);
	if (serialized_param_exec != nullptr)
	{
		const ParallelParamExecHeader *param_header =
			(const ParallelParamExecHeader *) serialized_param_exec;

		if (param_header->magic == 0x56565045)
			serialized_param_exec_size = param_header->bytes;
	}
	if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: worker PARAM_EXEC blob present=%s bytes=%zu",
			 serialized_param_exec != nullptr ? "true" : "false",
			 serialized_param_exec_size);
	if (control != nullptr && control->hash_bridge_ready && control->hash_bridge_size > 0)
		shared_hash_bridge = (const uint8_t *) shm_toc_lookup(toc,
															  VOLVEC_PARALLEL_KEY_HASH_BRIDGE,
															  false);
	execution_mode = control != nullptr ?
		(ParallelWorkerExecutionMode) control->execution_mode :
		ParallelWorkerExecutionMode::AggregateProbe;
	if (execution_mode == ParallelWorkerExecutionMode::AggregateProbe)
	{
		partials = (ParallelAggPartialState *) shm_toc_lookup(toc,
																VOLVEC_PARALLEL_KEY_PARTIALS,
																false);
	}
	else if (execution_mode == ParallelWorkerExecutionMode::HashBuild)
	{
		build_partials = (ParallelHashBuildPartialState *) shm_toc_lookup(
			toc,
			VOLVEC_PARALLEL_KEY_HASH_BUILD_PARTIALS,
			false);
	}
	else if (execution_mode == ParallelWorkerExecutionMode::HashBuildWave)
	{
		build_partials = (ParallelHashBuildPartialState *) shm_toc_lookup(
			toc,
			VOLVEC_PARALLEL_KEY_HASH_BUILD_PARTIALS,
			false);
		wave_pipelines = (ParallelHashBuildWavePipelineShared *) shm_toc_lookup(
			toc,
			VOLVEC_PARALLEL_KEY_HASH_BUILD_WAVE_PIPELINES,
			false);
	}
	else if (execution_mode == ParallelWorkerExecutionMode::QueryScheduler)
	{
		query_scheduler = (ParallelQuerySchedulerShared *) shm_toc_lookup(
			toc,
			VOLVEC_PARALLEL_KEY_QUERY_SCHEDULER,
			false);
		query_pipelines = (ParallelQueryPipelineShared *) shm_toc_lookup(
			toc,
			VOLVEC_PARALLEL_KEY_QUERY_PIPELINES,
			false);
		query_tasks = (ParallelQueryTaskShared *) shm_toc_lookup(
			toc,
			VOLVEC_PARALLEL_KEY_QUERY_TASKS,
			false);
		query_successors = (uint32_t *) shm_toc_lookup(
			toc,
			VOLVEC_PARALLEL_KEY_QUERY_SUCCESSORS,
			false);
		query_agg_partials = (ParallelAggPartialState *) shm_toc_lookup(
			toc,
			VOLVEC_PARALLEL_KEY_QUERY_AGG_PARTIALS,
			false);
		query_hash_partials = (ParallelHashBuildPartialState *) shm_toc_lookup(
			toc,
			VOLVEC_PARALLEL_KEY_QUERY_HASH_PARTIALS,
			false);
		query_sort_runs = (ParallelSortRunState *) shm_toc_lookup(
			toc,
			VOLVEC_PARALLEL_KEY_QUERY_SORT_RUNS,
			false);
	}

	if (control == nullptr || control->magic != VOLVEC_PARALLEL_MAGIC ||
		(execution_mode != ParallelWorkerExecutionMode::HashBuildWave &&
		 execution_mode != ParallelWorkerExecutionMode::QueryScheduler &&
		 source_pscan == nullptr) ||
		partial_fileset == nullptr ||
		plannedstmt_serialized == nullptr ||
		(execution_mode == ParallelWorkerExecutionMode::AggregateProbe && partials == nullptr) ||
		(execution_mode == ParallelWorkerExecutionMode::HashBuild && build_partials == nullptr) ||
		(execution_mode == ParallelWorkerExecutionMode::HashBuildWave &&
		 (build_partials == nullptr || wave_pipelines == nullptr || control->wave_pipeline_count == 0)) ||
		(execution_mode == ParallelWorkerExecutionMode::QueryScheduler &&
		 (query_scheduler == nullptr ||
		  query_scheduler->magic != VOLVEC_QUERY_SCHEDULER_MAGIC ||
		  query_pipelines == nullptr ||
		  query_tasks == nullptr ||
		  query_successors == nullptr ||
		  query_agg_partials == nullptr ||
		  query_hash_partials == nullptr)) ||
		(control->hash_bridge_ready && control->hash_bridge_size > 0 && shared_hash_bridge == nullptr))
		elog(ERROR, "pg_volvec parallel worker missing shared control");
	SharedFileSetAttach(partial_fileset, seg);
	local_state = (LocalParallelAggregateProcessState *)
		MemoryContextAllocZero(TopMemoryContext,
							   sizeof(LocalParallelAggregateProcessState));
	PG_ENSURE_ERROR_CLEANUP(CleanupLocalParallelAggregateProcessStateOnExit,
						   PointerGetDatum(local_state));
	{
		if (execution_mode == ParallelWorkerExecutionMode::HashBuildWave)
		{
			LocalParallelAggregateProcessState **wave_states;

			if (ParallelWorkerNumber < 0 ||
				(uint32) ParallelWorkerNumber >= control->partial_slot_count)
				elog(ERROR, "pg_volvec parallel worker number %d out of range",
					 ParallelWorkerNumber);
			wave_states = (LocalParallelAggregateProcessState **)
				palloc0_array(LocalParallelAggregateProcessState *,
							  control->wave_pipeline_count);
			for (uint32 pipeline_index = 0;
				 pipeline_index < control->wave_pipeline_count;
				 pipeline_index++)
			{
				ParallelHashBuildWavePipelineShared *wave_pipeline =
					&wave_pipelines[pipeline_index];

				wave_states[pipeline_index] =
					(LocalParallelAggregateProcessState *)
					MemoryContextAllocZero(TopMemoryContext,
										   sizeof(LocalParallelAggregateProcessState));
				if (!TryInitializeLocalParallelAggregateProcessState(plannedstmt_serialized,
																	 query_text,
																	 -1,
																	 wave_pipeline->hash_join_plan_node_id,
																	 wave_pipeline->input_hash_join_plan_node_id,
																	 false,
																	 true,
																	 shared_hash_bridge,
																	 (size_t) control->hash_bridge_size,
																	 wave_pipeline->source_scan_relid,
																	 wave_pipeline->source_scan_plan_node_id,
																	 nullptr,
																	 false,
																	 wave_states[pipeline_index],
																	 &failure_reason))
					elog(ERROR, "pg_volvec parallel worker wave init failed: %s",
						 failure_reason != nullptr ? failure_reason : "unknown reason");
			}
			for (;;)
			{
				bool did_work = false;

				for (uint32 pipeline_index = 0;
					 pipeline_index < control->wave_pipeline_count;
					 pipeline_index++)
				{
					ParallelHashBuildWavePipelineShared *wave_pipeline =
						&wave_pipelines[pipeline_index];
					LocalParallelAggregateProcessState *pipeline_state =
						wave_states[pipeline_index];
					uint64 start_block_u64;
					uint32 nblocks;

					CHECK_FOR_INTERRUPTS();
					start_block_u64 =
						pg_atomic_fetch_add_u64(&wave_pipeline->next_block,
												(int64) control->morsel_nblocks);
					if (start_block_u64 >= wave_pipeline->total_blocks)
						continue;
					nblocks = (uint32) Min((uint64) control->morsel_nblocks,
										   (uint64) wave_pipeline->total_blocks - start_block_u64);
					if (!pipeline_state->worker_context.hash_join_state->
							configure_build_input_block_range((BlockNumber) start_block_u64,
															 nblocks))
						elog(ERROR, "pg_volvec parallel worker wave could not configure build block range");
					pipeline_state->worker_context.hash_join_state->consume_build_input();
					pipeline_state->worker_context.hash_join_state->clear_build_input_block_range();
					did_work = true;
				}
				if (!did_work)
					break;
			}
			for (uint32 pipeline_index = 0;
				 pipeline_index < control->wave_pipeline_count;
				 pipeline_index++)
			{
				ParallelHashBuildPartialState *partial =
					&build_partials[pipeline_index * control->partial_slot_count +
									(uint32) ParallelWorkerNumber];
				LocalParallelAggregateProcessState *pipeline_state =
					wave_states[pipeline_index];
				BufFile *file;
				char file_name[VOLVEC_PARALLEL_MAX_PARTIAL_FILE_NAME];

				snprintf(file_name,
						 sizeof(file_name),
						 "pg_volvec_wb_%u_%d",
						 pipeline_index,
						 ParallelWorkerNumber);
				file = BufFileCreateFileSet(&partial_fileset->fs, file_name);
				if (file == nullptr ||
					!pipeline_state->worker_context.hash_join_state->export_parallel_build_partial_file(
						file,
						partial))
				{
					if (file != nullptr)
						BufFileClose(file);
					elog(ERROR, "pg_volvec parallel worker wave partial export failed");
				}
				strlcpy(partial->file_name, file_name, sizeof(partial->file_name));
				BufFileExportFileSet(file);
				BufFileClose(file);
				PopulateHashBuildPartialDiagnostics(*pipeline_state, partial);
				CleanupLocalParallelAggregateProcessState(pipeline_state);
			}
		}
		else if (execution_mode == ParallelWorkerExecutionMode::QueryScheduler)
		{
			LocalParallelAggregateProcessState **pipeline_states;
			bool *pipeline_published;

			pipeline_states = (LocalParallelAggregateProcessState **)
				palloc0_array(LocalParallelAggregateProcessState *,
							  query_scheduler->pipeline_count);
			pipeline_published =
				(bool *) palloc0(sizeof(bool) * query_scheduler->pipeline_count);
			for (;;)
			{
				bool did_work = false;

				CHECK_FOR_INTERRUPTS();
				for (uint32 task_index = 0;
					 task_index < query_scheduler->task_count;
					 task_index++)
				{
					uint32 cursor =
						pg_atomic_fetch_add_u32(&query_scheduler->next_task_scan, 1);
					ParallelQueryTaskShared *task =
						&query_tasks[cursor % query_scheduler->task_count];
					ParallelQueryPipelineShared *pipeline;
					uint32 expected = (uint32) ParallelQueryTaskState::Pending;
					LocalParallelAggregateProcessState *pipeline_state;
					uint32 completed_tasks;
					ParallelTableScanDesc query_pscan;

					if (task->pipeline_id >= query_scheduler->pipeline_count)
						continue;
					pipeline = &query_pipelines[task->pipeline_id];
					if (pg_atomic_read_u32(&pipeline->remaining_dependencies) != 0)
						continue;
					if (!pg_atomic_compare_exchange_u32(&task->state,
														&expected,
														(uint32) ParallelQueryTaskState::Running))
						continue;
					pipeline_state = pipeline_states[task->pipeline_id];
					query_pscan = (ParallelTableScanDesc) shm_toc_lookup(
						toc,
						VOLVEC_PARALLEL_KEY_QUERY_SOURCE_PSCAN_BASE + task->pipeline_id,
						true);
					if (task->task_kind == (uint32) ParallelTaskKind::SourceMorsel)
					{
						if (pipeline_state == nullptr)
						{
							uint8_t *dependency_bridge_pack = nullptr;
							size_t dependency_bridge_pack_size = 0;

							if (!BuildQueryDependencyBridgePack(partial_fileset,
																query_scheduler,
																query_pipelines,
																query_successors,
																task->pipeline_id,
																&dependency_bridge_pack,
																&dependency_bridge_pack_size,
																&failure_reason))
							{
								pg_atomic_write_u32(&query_scheduler->error, 1);
								elog(ERROR,
									 "pg_volvec query scheduler dependency bridge attach failed: %s",
									 failure_reason != nullptr ? failure_reason : "unknown reason");
							}
							pipeline_state =
								(LocalParallelAggregateProcessState *)
								MemoryContextAllocZero(TopMemoryContext,
													   sizeof(LocalParallelAggregateProcessState));
							if (!TryInitializeLocalParallelAggregateProcessState(
									plannedstmt_serialized,
									query_text,
									pipeline->agg_plan_node_id,
									pipeline->hash_join_plan_node_id,
									pipeline->input_hash_join_plan_node_id,
									pipeline->agg_plan_node_id >= 0,
									pipeline->hash_join_plan_node_id >= 0,
									dependency_bridge_pack != nullptr ?
										dependency_bridge_pack :
										shared_hash_bridge,
									dependency_bridge_pack != nullptr ?
										dependency_bridge_pack_size :
										(size_t) control->hash_bridge_size,
									pipeline->source_scan_relid,
									pipeline->source_scan_plan_node_id,
									query_pscan,
									false,
									pipeline_state,
									&failure_reason,
									serialized_param_exec,
									serialized_param_exec_size))
							{
								pg_atomic_write_u32(&query_scheduler->error, 1);
								elog(ERROR, "pg_volvec query scheduler worker init failed: %s",
									 failure_reason != nullptr ? failure_reason : "unknown reason");
							}
							pipeline_states[task->pipeline_id] = pipeline_state;
						}
						if (pipeline->role == (uint32) ParallelPipelineRole::HashBuildSource)
						{
							instr_time task_exec_start;

							INSTR_TIME_SET_CURRENT(task_exec_start);
							if (!pipeline_state->worker_context.hash_join_state->
									configure_build_input_block_range(task->morsel_start_block,
																	 task->morsel_nblocks))
							{
								pg_atomic_write_u32(&query_scheduler->error, 1);
								elog(ERROR, "pg_volvec query scheduler worker could not configure build block range");
							}
							pipeline_state->worker_context.hash_join_state->consume_build_input();
							pipeline_state->worker_context.hash_join_state->clear_build_input_block_range();
							pipeline_state->exec_time_us += ElapsedUsSince(task_exec_start);
						}
						else if ((ParallelBridgeKind) pipeline->output_bridge ==
								 ParallelBridgeKind::SortRuns)
						{
							ParallelSortRunState *run = &query_sort_runs[task->task_id];
							BufFile *file;
							DataChunk<DEFAULT_CHUNK_SIZE> out;
							VecPlanState *row_source = pipeline_state->root_plan;
							VecSortState *root_sort =
								dynamic_cast<VecSortState *>(pipeline_state->root_plan);
							char file_name[VOLVEC_PARALLEL_MAX_PARTIAL_FILE_NAME];
							instr_time task_exec_start;

							snprintf(file_name,
									 sizeof(file_name),
									 "pg_volvec_qsort_%u",
									 task->task_id);
							file = BufFileCreateFileSet(&partial_fileset->fs, file_name);
							if (file == nullptr)
							{
								pg_atomic_write_u32(&query_scheduler->error, 1);
								elog(ERROR, "pg_volvec query scheduler sort run file create failed");
							}
							INSTR_TIME_SET_CURRENT(task_exec_start);
							if (root_sort != nullptr && root_sort->source_plan() != nullptr)
								row_source = root_sort->source_plan();
							if (row_source == nullptr ||
								!row_source->configure_source_block_range(
									task->morsel_start_block,
									task->morsel_nblocks))
							{
								BufFileClose(file);
								pg_atomic_write_u32(&query_scheduler->error, 1);
								elog(ERROR, "pg_volvec query scheduler worker could not configure sort run block range");
							}
							while (row_source->get_next_batch(out))
							{
								int active_count = out.has_selection ? out.sel.count : out.count;

								if (active_count <= 0)
									continue;
								if (!WriteDataChunkRows(file, out))
								{
									BufFileClose(file);
									pg_atomic_write_u32(&query_scheduler->error, 1);
									elog(ERROR, "pg_volvec query scheduler sort run write failed");
								}
								run->chunk_count++;
								run->row_count += (uint64) active_count;
							}
							row_source->clear_source_block_range();
							strlcpy(run->file_name, file_name, sizeof(run->file_name));
							run->task_id = task->task_id;
							run->pipeline_id = task->pipeline_id;
							BufFileExportFileSet(file);
							run->file_bytes = (uint64) BufFileSize(file);
							BufFileClose(file);
							pipeline_state->exec_time_us += ElapsedUsSince(task_exec_start);
							if (pg_volvec_trace_hooks)
								elog(LOG,
									 "pg_volvec: query scheduler sort run export task=%u pipeline=%u chunks=%u rows=%llu bytes=%llu",
									 task->task_id,
									 task->pipeline_id,
									 run->chunk_count,
									 (unsigned long long) run->row_count,
									 (unsigned long long) run->file_bytes);
						}
						else if (pipeline_state->worker_context.agg_state != nullptr)
						{
							instr_time task_exec_start;

							INSTR_TIME_SET_CURRENT(task_exec_start);
							if (!pipeline_state->worker_context.agg_state->
									configure_input_block_range(task->morsel_start_block,
																task->morsel_nblocks))
							{
								pg_atomic_write_u32(&query_scheduler->error, 1);
								elog(ERROR, "pg_volvec query scheduler worker could not configure aggregate input block range");
							}
							pipeline_state->worker_context.agg_state->consume_left_input();
							pipeline_state->worker_context.agg_state->clear_input_block_range();
							pipeline_state->exec_time_us += ElapsedUsSince(task_exec_start);
						}
						}
						else if (task->task_kind == (uint32) ParallelTaskKind::BridgeFinalize &&
								 pipeline->role == (uint32) ParallelPipelineRole::HashBuildSource)
						{
							LocalParallelAggregateProcessState build_state;
							ParallelHashBuildPartialState *partial;
							BufFile *file;
							char file_name[VOLVEC_PARALLEL_MAX_PARTIAL_FILE_NAME];
							uint8_t *dependency_bridge_pack = nullptr;
							size_t dependency_bridge_pack_size = 0;
							size_t slot_index =
								(size_t) task->pipeline_id * query_scheduler->worker_count +
								(size_t) ParallelWorkerNumber;
							instr_time task_exec_start;
							instr_time export_start;

							memset(&build_state, 0, sizeof(build_state));
							if (!BuildQueryDependencyBridgePack(partial_fileset,
																query_scheduler,
																query_pipelines,
																query_successors,
																task->pipeline_id,
																&dependency_bridge_pack,
																&dependency_bridge_pack_size,
																&failure_reason))
							{
								pg_atomic_write_u32(&query_scheduler->error, 1);
								elog(ERROR,
									 "pg_volvec query scheduler hash build source dependency attach failed: %s",
									 failure_reason != nullptr ? failure_reason : "unknown reason");
							}
							if (!TryInitializeLocalParallelAggregateProcessState(
									plannedstmt_serialized,
									query_text,
									pipeline->agg_plan_node_id,
									pipeline->hash_join_plan_node_id,
									pipeline->input_hash_join_plan_node_id,
									pipeline->agg_plan_node_id >= 0,
									true,
									dependency_bridge_pack != nullptr ?
										dependency_bridge_pack :
										shared_hash_bridge,
									dependency_bridge_pack != nullptr ?
										dependency_bridge_pack_size :
										(size_t) control->hash_bridge_size,
									InvalidOid,
									-1,
									nullptr,
									false,
									&build_state,
									&failure_reason,
									serialized_param_exec,
									serialized_param_exec_size))
							{
								pg_atomic_write_u32(&query_scheduler->error, 1);
								elog(ERROR,
									 "pg_volvec query scheduler hash build source init failed: %s",
									 failure_reason != nullptr ? failure_reason : "unknown reason");
							}
							if (build_state.worker_context.agg_state == nullptr ||
								build_state.worker_context.hash_join_state == nullptr)
							{
								CleanupLocalParallelAggregateProcessState(&build_state);
								pg_atomic_write_u32(&query_scheduler->error, 1);
								elog(ERROR,
									 "pg_volvec query scheduler hash build source missing agg/hash state");
							}
							INSTR_TIME_SET_CURRENT(task_exec_start);
							if (!MergeQueryAggregateInputBridge(partial_fileset,
																query_scheduler,
																query_pipelines,
																query_successors,
																query_agg_partials,
																task->pipeline_id,
																build_state.worker_context.agg_state,
																&failure_reason))
							{
								CleanupLocalParallelAggregateProcessState(&build_state);
								pg_atomic_write_u32(&query_scheduler->error, 1);
								elog(ERROR,
									 "pg_volvec query scheduler hash build source aggregate merge failed: %s",
									 failure_reason != nullptr ? failure_reason : "unknown reason");
							}
							build_state.worker_context.hash_join_state->consume_build_input();
							build_state.exec_time_us += ElapsedUsSince(task_exec_start);

							partial = &query_hash_partials[slot_index];
							snprintf(file_name,
									 sizeof(file_name),
									 "pg_volvec_qbuild_%u_%d",
									 task->pipeline_id,
									 ParallelWorkerNumber);
							INSTR_TIME_SET_CURRENT(export_start);
							file = BufFileCreateFileSet(&partial_fileset->fs, file_name);
							if (file == nullptr ||
								!build_state.worker_context.hash_join_state->
									export_parallel_build_partial_file(file, partial))
							{
								if (file != nullptr)
									BufFileClose(file);
								CleanupLocalParallelAggregateProcessState(&build_state);
								pg_atomic_write_u32(&query_scheduler->error, 1);
								elog(ERROR,
									 "pg_volvec query scheduler hash build source partial export failed");
							}
							strlcpy(partial->file_name, file_name, sizeof(partial->file_name));
							BufFileExportFileSet(file);
							BufFileClose(file);
							PopulateHashBuildPartialDiagnostics(build_state, partial);
							if (pg_volvec_trace_hooks)
								elog(LOG,
									 "pg_volvec: query scheduler hash build source pipeline=%u worker=%d bytes=%llu entries=%llu chunks=%llu rows=%llu exec_us=%llu export_us=%llu",
									 task->pipeline_id,
									 ParallelWorkerNumber,
									 (unsigned long long) partial->file_bytes,
									 (unsigned long long) partial->entry_count,
									 (unsigned long long) partial->chunk_count,
									 (unsigned long long) partial->input_rows,
									 (unsigned long long) build_state.exec_time_us,
									 (unsigned long long) ElapsedUsSince(export_start));
							CleanupLocalParallelAggregateProcessState(&build_state);
						}
						else if (task->task_kind == (uint32) ParallelTaskKind::BridgeFinalize &&
								 pipeline->role == (uint32) ParallelPipelineRole::HashBuildFinalize)
						{
							if (!FinalizeQueryHashBuildPipeline(partial_fileset,
														   plannedstmt_serialized,
														   query_text,
														   query_scheduler,
														   query_pipelines,
														   query_successors,
														   query_hash_partials,
														   pipeline,
														   &failure_reason))
						{
							pg_atomic_write_u32(&query_scheduler->error, 1);
							elog(ERROR,
								 "pg_volvec query scheduler hash finalize failed: %s",
								 failure_reason != nullptr ? failure_reason : "unknown reason");
						}
					}
					pg_atomic_write_u32(&task->state,
										 (uint32) ParallelQueryTaskState::Done);
					completed_tasks =
						pg_atomic_fetch_add_u32(&pipeline->completed_tasks, 1) + 1;
					if (completed_tasks == pipeline->total_tasks)
					{
						if ((ParallelTaskKind) pipeline->task_kind !=
							ParallelTaskKind::SourceMorsel)
							(void) CompleteQueryPipelineAndReleaseSuccessors(
								query_scheduler,
								query_pipelines,
								query_successors,
								pipeline);
					}
					pg_atomic_fetch_add_u32(&query_scheduler->completed_tasks, 1);
					did_work = true;
				}
				for (uint32 pipeline_id = 0;
					 pipeline_id < query_scheduler->pipeline_count;
					 pipeline_id++)
				{
					ParallelQueryPipelineShared *pipeline =
						&query_pipelines[pipeline_id];
					uint32 published_workers;

					if ((ParallelTaskKind) pipeline->task_kind !=
						ParallelTaskKind::SourceMorsel ||
						pipeline_published[pipeline_id] ||
						pipeline->total_tasks == 0 ||
						pg_atomic_read_u32(&pipeline->completed_tasks) <
							pipeline->total_tasks)
						continue;
					if (!PublishCompletedQuerySourcePipeline(partial_fileset,
															 query_scheduler,
															 query_pipelines,
															 query_agg_partials,
															 query_hash_partials,
															 pipeline_id,
															 ParallelWorkerNumber,
															 pipeline_states,
															 pipeline_published,
															 &failure_reason))
					{
						pg_atomic_write_u32(&query_scheduler->error, 1);
						elog(ERROR,
							 "pg_volvec query scheduler source publish failed: %s",
							 failure_reason != nullptr ? failure_reason : "unknown reason");
					}
					published_workers =
						pg_atomic_read_u32(&pipeline->published_workers);
					if (published_workers >= query_scheduler->worker_count)
					{
						pg_read_barrier();
						(void) CompleteQueryPipelineAndReleaseSuccessors(
							query_scheduler,
							query_pipelines,
							query_successors,
							pipeline);
					}
					did_work = true;
				}
				if (pg_atomic_read_u32(&query_scheduler->completed_tasks) >=
					query_scheduler->task_count)
					break;
				if (!did_work)
					pg_usleep(1000L);
			}
			for (uint32 pipeline_id = 0;
				 pipeline_id < query_scheduler->pipeline_count;
				 pipeline_id++)
			{
				if (pipeline_states[pipeline_id] != nullptr)
					CleanupLocalParallelAggregateProcessState(pipeline_states[pipeline_id]);
			}
			if (pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: query scheduler worker pid=%d attached pipelines=%u tasks=%u",
					 MyProcPid,
					 query_scheduler->pipeline_count,
					 query_scheduler->task_count);
		}
		else
		{
		if (!TryInitializeLocalParallelAggregateProcessState(plannedstmt_serialized,
															 query_text,
															 control->agg_plan_node_id,
															 control->hash_join_plan_node_id,
															 control->input_hash_join_plan_node_id,
															 execution_mode == ParallelWorkerExecutionMode::AggregateProbe,
															 control->need_hash_join_state,
															 shared_hash_bridge,
															 (size_t) control->hash_bridge_size,
															 control->source_scan_relid,
															 control->source_scan_plan_node_id,
															 source_pscan,
															 false,
															 local_state,
															 &failure_reason))
			elog(ERROR, "pg_volvec parallel worker init failed: %s",
				 failure_reason != nullptr ? failure_reason : "unknown reason");
		if (pg_volvec_trace_hooks)
			elog(LOG,
				 "pg_volvec: parallel worker pid=%d entering execute mode=%s hash_join_plan_node_id=%d input_hash_join_plan_node_id=%d",
				 MyProcPid,
				 ParallelWorkerExecutionModeDebugName(execution_mode),
				 control->hash_join_plan_node_id,
				 control->input_hash_join_plan_node_id);
		INSTR_TIME_SET_CURRENT(exec_start);
		if (!ExecuteParallelWorkerSourceLoop(execution_mode,
											 local_state->worker_context,
											 &failure_reason))
			elog(ERROR, "pg_volvec parallel worker execution failed: %s",
				 failure_reason != nullptr ? failure_reason : "unknown reason");
		INSTR_TIME_SET_CURRENT(exec_end);
		INSTR_TIME_SUBTRACT(exec_end, exec_start);
		local_state->exec_time_us = (uint64) INSTR_TIME_GET_MICROSEC(exec_end);
		if (pg_volvec_trace_hooks)
			elog(LOG,
				 "pg_volvec: parallel worker pid=%d finished execute mode=%s",
				 MyProcPid,
				 ParallelWorkerExecutionModeDebugName(execution_mode));
		if (ParallelWorkerNumber < 0 ||
			(uint32) ParallelWorkerNumber >= control->partial_slot_count)
			elog(ERROR, "pg_volvec parallel worker number %d out of range",
				 ParallelWorkerNumber);
		if (execution_mode == ParallelWorkerExecutionMode::HashBuild)
		{
			BufFile *file;
			char file_name[VOLVEC_PARALLEL_MAX_PARTIAL_FILE_NAME];

			if (pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: parallel worker pid=%d starting hash build partial export",
					 MyProcPid);
			if (pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: parallel worker hash build finished execution rows=%llu batches=%llu entries=%zu chunks=%zu",
					 (unsigned long long)
					 local_state->worker_context.hash_join_state->build_input_rows_consumed(),
					 (unsigned long long)
					 local_state->worker_context.hash_join_state->build_input_batches_consumed(),
					 local_state->worker_context.hash_join_state->parallel_hash_entry_count(),
					 local_state->worker_context.hash_join_state->parallel_hash_chunk_count());

			FormatParallelPartialFileName(file_name,
										 sizeof(file_name),
										 "build_worker",
										 ParallelWorkerNumber);
			file = BufFileCreateFileSet(&partial_fileset->fs, file_name);
			if (pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: parallel worker exporting hash build partial file=%s",
					 file_name);
			if (file == nullptr ||
				!local_state->worker_context.hash_join_state->export_parallel_build_partial_file(
					file,
					&build_partials[ParallelWorkerNumber]))
			{
				if (file != nullptr)
					BufFileClose(file);
				elog(ERROR, "pg_volvec parallel worker hash build partial export failed");
			}
			strlcpy(build_partials[ParallelWorkerNumber].file_name,
					file_name,
					sizeof(build_partials[ParallelWorkerNumber].file_name));
			BufFileExportFileSet(file);
			BufFileClose(file);
			PopulateHashBuildPartialDiagnostics(*local_state,
											   &build_partials[ParallelWorkerNumber]);
		}
		else if (local_state->worker_context.agg_state->uses_file_backed_parallel_partial_state())
		{
			BufFile *file;
			char file_name[VOLVEC_PARALLEL_MAX_PARTIAL_FILE_NAME];

			if (pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: parallel worker pid=%d starting grouped partial export",
					 MyProcPid);
			FormatParallelPartialFileName(file_name,
										 sizeof(file_name),
										 "worker",
										 ParallelWorkerNumber);
			file = BufFileCreateFileSet(&partial_fileset->fs, file_name);
			if (file == nullptr ||
				!local_state->worker_context.agg_state->export_parallel_grouped_partial_file(
					file,
					&partials[ParallelWorkerNumber]))
			{
				if (file != nullptr)
					BufFileClose(file);
				elog(ERROR, "pg_volvec parallel worker grouped partial export failed");
			}
			strlcpy(partials[ParallelWorkerNumber].grouped_file_name,
					file_name,
					sizeof(partials[ParallelWorkerNumber].grouped_file_name));
			BufFileExportFileSet(file);
			BufFileClose(file);
			PopulatePartialDiagnostics(*local_state, &partials[ParallelWorkerNumber]);
		}
		else
		{
			if (pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: parallel worker pid=%d starting inline partial export",
					 MyProcPid);
			if (!local_state->worker_context.agg_state->export_parallel_partial_state(
					 &partials[ParallelWorkerNumber]))
				elog(ERROR, "pg_volvec parallel worker partial export failed");
			PopulatePartialDiagnostics(*local_state, &partials[ParallelWorkerNumber]);
		}
		CleanupLocalParallelAggregateProcessState(local_state);
		}
	}
	PG_END_ENSURE_ERROR_CLEANUP(CleanupLocalParallelAggregateProcessStateOnExit,
								PointerGetDatum(local_state));
}
