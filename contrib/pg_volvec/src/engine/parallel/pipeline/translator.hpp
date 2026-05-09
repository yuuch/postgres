#pragma once

/*
 * pipeline/translator.hpp — recursive PG plan → PhysicalOperator translator.
 *
 * Translate() takes a QueryDesc (not just PlannedStmt) so the translator can
 * reach qd->tupDesc and qd->dest when constructing OutputSink, and so the
 * Q1 shape-matcher can stamp NUMERIC scale (typmod) into descriptors for the
 * OutputSink encode path. Returns nullptr on any unsupported plan shape;
 * the bridge then logs WARNING and falls back to standard_ExecutorRun.
 *
 * Current supported trees:
 *   - SeqScan -> Agg with optional top Sort
 *   - one inner HashJoin whose left/right children are scan-backed leaves
 *     (optionally with PostgreSQL's build-side Hash wrapper)
 *
 * The HashJoin slice is intentionally not recursive yet: nested join children
 * and Agg-over-Join shapes still return nullptr so the bridge falls back
 * cleanly instead of pretending broader join support exists.
 *
 * Spec: PIPELINE_PORT_PLAN.md §15.3.2, §15.4; 3g.2-final delta-map §10.
 */

#include <memory>

extern "C" {
#include "postgres.h"
#include "executor/execdesc.h"
#include "nodes/plannodes.h"
}

#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/query_state.hpp"

namespace pg_volvec {
namespace pipeline {

class Translator {
public:
	static std::unique_ptr<PhysicalOperator> Translate(QueryDesc *qd,
													   PgVolVecQueryState *state);

private:
	static std::unique_ptr<PhysicalOperator> TranslatePlan(Plan *plan,
														   QueryDesc *qd,
														   PgVolVecQueryState *state);
};

}  /* namespace pipeline */
}  /* namespace pg_volvec */
