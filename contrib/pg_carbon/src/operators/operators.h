#ifndef PG_CARBON_OPERATORS_H
#define PG_CARBON_OPERATORS_H

#include "../common/memory.h"
#include <string>
#include <vector>

extern "C" {
#include "postgres.h"
}

namespace pg_carbon {

class Operator : public PgObject {
public:
  virtual ~Operator() = default;
  virtual bool IsLogical() const = 0;
  virtual bool IsPhysical() const = 0;
  virtual std::string ToString() const = 0;

  void AddInput(Operator *input) { inputs_.push_back(input); }
  const PgVector<Operator *> &GetInputs() const { return inputs_; }

private:
  PgVector<Operator *> inputs_;
};

// --- Logical Operators ---

class LogicalGet : public Operator {
public:
  LogicalGet(Oid table_oid, Index rtindex)
      : table_oid_(table_oid), rtindex_(rtindex) {}
  bool IsLogical() const override { return true; }
  bool IsPhysical() const override { return false; }
  std::string ToString() const override {
    return "LogicalGet(" + std::to_string(table_oid_) + ")";
  }
  Oid GetTableOid() const { return table_oid_; }
  Index GetRtIndex() const { return rtindex_; }

private:
  Oid table_oid_;
  Index rtindex_;
};

class LogicalInnerJoin : public Operator {
public:
  LogicalInnerJoin() = default;
  bool IsLogical() const override { return true; }
  bool IsPhysical() const override { return false; }
  std::string ToString() const override { return "LogicalInnerJoin"; }
};

// --- Physical Operators ---

class PhysicalTableScan : public Operator {
public:
  PhysicalTableScan(Oid table_oid, Index rtindex)
      : table_oid_(table_oid), rtindex_(rtindex) {}
  bool IsLogical() const override { return false; }
  bool IsPhysical() const override { return true; }
  std::string ToString() const override {
    return "PhysicalTableScan(" + std::to_string(table_oid_) + ")";
  }
  Oid GetTableOid() const { return table_oid_; }
  Index GetRtIndex() const { return rtindex_; }

private:
  Oid table_oid_;
  Index rtindex_;
};

class PhysicalNestedLoopJoin : public Operator {
public:
  PhysicalNestedLoopJoin() = default;
  bool IsLogical() const override { return false; }
  bool IsPhysical() const override { return true; }
  std::string ToString() const override { return "PhysicalNestedLoopJoin"; }
};

} // namespace pg_carbon

#endif // PG_CARBON_OPERATORS_H
