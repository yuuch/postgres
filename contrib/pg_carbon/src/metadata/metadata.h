#ifndef PG_CARBON_METADATA_H
#define PG_CARBON_METADATA_H

#include "../common/memory.h"

namespace pg_carbon {

class MetadataAccessor {
public:
  // Mock method to get table rows. In a real implementation, this would call
  // PG's statistics API.
  static double GetTableRows(Oid table_oid);
};

} // namespace pg_carbon

#endif // PG_CARBON_METADATA_H
