#pragma once

#include <cstdint>

extern "C" {
#include "postgres.h"
#include "utils/dsa.h"
#include "utils/memutils.h"
}

namespace pg_volvec {

template <uint16_t Capacity> struct DataChunk;

constexpr uint16_t PIPELINE_DEFAULT_CHUNK_SIZE = 1024;

namespace pipeline {

using PipelineChunk = DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE>;

enum class OperatorResultType : uint8_t {
	NEED_MORE_INPUT,
	HAVE_MORE_OUTPUT,
	FINISHED,
	BLOCKED,
};

enum class SourceResultType : uint8_t {
	HAVE_MORE_OUTPUT,
	FINISHED,
	BLOCKED,
};

enum class SinkResultType : uint8_t {
	NEED_MORE_INPUT,
	FINISHED,
	BLOCKED,
};

enum class SinkCombineResultType : uint8_t {
	FINISHED,
	BLOCKED,
};

enum class SinkFinalizeType : uint8_t {
	READY,
	NO_OUTPUT_POSSIBLE,
	BLOCKED,
};

using PipelineId = uint16_t;
constexpr PipelineId INVALID_PIPELINE_ID = static_cast<PipelineId>(-1);
constexpr int LEADER_WORKER_INDEX = -1;

struct ExecCtx {
	MemoryContext  mcxt;
	dsa_area      *dsa;
	int            worker_index;
};

}
}
