#include "pg_external_catalog.hpp"

namespace duckdbopt {

void PGExternalCatalog::LoadStats(PlannerInfo* root) {
    // TODO: Connect PG pg_statistic to inner stat structs
    // Extract histograms, MCV, NDV, null_count
}

} // namespace duckdbopt
