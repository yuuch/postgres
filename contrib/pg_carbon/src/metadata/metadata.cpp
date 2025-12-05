#include "metadata.h"

extern "C" {
#include "utils/lsyscache.h"
#include "utils/rel.h"
}

namespace pg_carbon {

double MetadataAccessor::GetTableRows(Oid table_oid) {
  // Mock implementation.
  // In reality, we would open the relation and check pg_class or statistics.
  // For now, return a dummy value.
  return 1000.0;
}

} // namespace pg_carbon
