#pragma once

/*
 * pipeline/translator.hpp — recursive PG plan → PhysicalOperator translator.
 *
 * M-FRAME-MIN step 2 stub: Translate() walks a PG plan tree node-by-node and
 * returns nullptr on any unsupported node, propagating nullptr up. Today
 * everything returns nullptr (no shape supported), so the bridge logs WARNING
 * and falls back to standard_ExecutorRun.
 *
 * Real Q1 shape-matching lands in M-FRAME-MIN step 4 inside Translate()'s
 * recursive switch. Q6 is handled later (M-Q6-RESTORE).
 *
 * Spec: PIPELINE_PORT_PLAN.md §15.3.2, §15.4.
 */

#include <memory>

extern "C" {
#include "postgres.h"
#include "nodes/plannodes.h"
}

#include "parallel/pipeline/physical_operator.hpp"

namespace pg_volvec {
namespace pipeline {

class Translator {
public:
	static std::unique_ptr<PhysicalOperator> Translate(PlannedStmt *stmt);

private:
	static std::unique_ptr<PhysicalOperator> TranslatePlan(Plan *plan, PlannedStmt *stmt);
};

}  /* namespace pipeline */
}  /* namespace pg_volvec */
