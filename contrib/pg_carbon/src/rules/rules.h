#ifndef PG_CARBON_RULES_H
#define PG_CARBON_RULES_H

#include "../common/memory.h"
#include "../optimizer/memo.h"
#include <vector>

namespace pg_carbon {

class Rule : public PgObject {
public:
  virtual ~Rule() = default;
  virtual bool Matches(GroupExpression *expr) const = 0;
  virtual PgVector<GroupExpression *>
  Transform(GroupExpression *expr) const = 0;
  virtual std::string ToString() const = 0;
};

class RuleGetToScan : public Rule {
public:
  bool Matches(GroupExpression *expr) const override;
  PgVector<GroupExpression *> Transform(GroupExpression *expr) const override;
  std::string ToString() const override { return "RuleGetToScan"; }
};

} // namespace pg_carbon

#endif // PG_CARBON_RULES_H
