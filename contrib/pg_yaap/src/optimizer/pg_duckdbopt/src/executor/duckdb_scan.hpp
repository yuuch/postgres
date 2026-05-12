#pragma once

#include <memory>
#include <set>
#include <string>
#include <vector>

extern "C" {
struct Query;
struct PlannedStmt;
}

namespace duckdbopt {

class Expression;
class LogicalOperator;
class PhysicalOperator;

class DuckDBExecutorScan {
public:
    DuckDBExecutorScan() = default;
    ~DuckDBExecutorScan() = default;

    static void RegisterScanMethods();

    ::PlannedStmt* CreateCustomScanPlan(::Query* pg_query,
                                        LogicalOperator* logical_plan,
                                        PhysicalOperator* physical_plan);
    
private:
    std::string DumpLogicalPlan(LogicalOperator* op, int depth = 0);
    std::string DumpLogicalPlan(LogicalOperator* op, int depth, std::set<const LogicalOperator*>& visited);
    std::string DumpPhysicalPlan(PhysicalOperator* op, int depth = 0);
    std::string DumpPhysicalPlan(PhysicalOperator* op, int depth, std::set<const PhysicalOperator*>& visited);
    std::string DumpExpression(Expression* expr);
    std::string DumpExpression(Expression* expr, std::set<const Expression*>& visited);
    std::string DumpExpressionList(const std::vector<std::unique_ptr<Expression>>& expressions);
    std::string DumpExpressionList(const std::vector<Expression*>& expressions);
};

} // namespace duckdbopt
