#include "duckdb_scan.hpp"
#include "../adapter/duckdb_adapter.hpp"
#include "../physical/physical_plan.hpp"

extern "C" {
#include "postgres.h"
#include "nodes/extensible.h"
#include "nodes/execnodes.h"
#include "commands/explain.h"
#include "commands/explain_format.h"
#include "executor/executor.h"
#include "utils/memutils.h"
}

#include <sstream>
#include <set>

namespace duckdbopt {

namespace {

std::string JoinTypeName(int join_type) {
    switch (join_type) {
        case JOIN_INNER: return "INNER";
        case JOIN_LEFT: return "LEFT";
        case JOIN_FULL: return "FULL";
        case JOIN_RIGHT: return "RIGHT";
        case JOIN_SEMI: return "SEMI";
        case JOIN_ANTI: return "ANTI";
        case JOIN_MARK: return "MARK";
        case JOIN_SINGLE: return "SINGLE";
        default: return std::to_string(join_type);
    }
}

std::string SetOperationName(SetOperationType setop_type) {
    switch (setop_type) {
        case SetOperationType::UNION: return "UNION";
        default: return std::to_string(static_cast<int>(setop_type));
    }
}

} // namespace

// -------------------------------------------------------------
// Custom Scan Method Implementations
// -------------------------------------------------------------

static void ExplainDuckDBScan(CustomScanState *node, List *ancestors, ExplainState *es) {
    CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
    if (cscan->custom_private != nullptr) {
        Node *n = (Node *)linitial((::List*)cscan->custom_private);
        if (IsA(n, String)) {
            char *plan_str = strVal(n);
            ExplainPropertyText("DuckDB Logical Plan", plan_str, es);
        }
    }
}

static TupleTableSlot *ExecDuckDBScan(CustomScanState *node) {
    elog(ERROR, "Execution of DuckDB generated plans is disabled. Use EXPLAIN only.");
    return nullptr;
}

static void BeginDuckDBScan(CustomScanState *node, EState *estate, int eflags) {}
static void EndDuckDBScan(CustomScanState *node) {}
static void ReScanDuckDBScan(CustomScanState *node) {}

static CustomExecMethods duckdb_exec_methods = {
    "DuckDBScan",
    BeginDuckDBScan,
    ExecDuckDBScan,
    EndDuckDBScan,
    ReScanDuckDBScan,
    nullptr, // MarkPos
    nullptr, // RestrPos
    nullptr, // EstimateDSM
    nullptr, // InitializeDSM
    nullptr, // ReInitializeWorker
    nullptr, // InitializeWorker
    nullptr, // Shutdown
    ExplainDuckDBScan
};

static Node *CreateDuckDBScanState(CustomScan *cscan) {
    CustomScanState *cstate = (CustomScanState *)newNode(sizeof(CustomScanState), T_CustomScanState);
    cstate->methods = &duckdb_exec_methods;
    return (Node *)cstate;
}

static CustomScanMethods duckdb_scan_methods = {
    "DuckDBScan",
    CreateDuckDBScanState
};

// -------------------------------------------------------------
// DuckDBExecutorScan Class Methods
// -------------------------------------------------------------

void DuckDBExecutorScan::RegisterScanMethods() {
    RegisterCustomScanMethods(&duckdb_scan_methods);
}

std::string DuckDBExecutorScan::DumpExpression(Expression* expr) {
    std::set<const Expression*> visited;
    return DumpExpression(expr, visited);
}

std::string DuckDBExecutorScan::DumpExpression(Expression* expr, std::set<const Expression*>& visited) {
    if (!expr) {
        return "<null>";
    }

    if (!visited.insert(expr).second) {
        return "<cycle>";
    }

    std::stringstream ss;
    switch (expr->type) {
        case ExpressionType::BOUND_COLUMN_REF: {
            auto col = static_cast<BoundColumnRefExpression*>(expr);
            ss << "BoundColumnRef("
               << col->table_name << "." << col->column_name
               << ", binding=#[" << col->binding.table_index.index << "."
               << col->binding.column_index.index << "]"
               << ")";
            break;
        }
        case ExpressionType::BOUND_CONSTANT: {
            auto constant = static_cast<BoundConstantExpression*>(expr);
            ss << "BoundConstant(" << (constant->is_null ? "NULL" : constant->value) << ")";
            break;
        }
        case ExpressionType::BOUND_FUNCTION: {
            auto function = static_cast<BoundFunctionExpression*>(expr);
            ss << "BoundFunction(" << function->function_name << "#" << function->op_oid;
            if (!function->children.empty()) {
                ss << ", children=[";
                for (size_t i = 0; i < function->children.size(); ++i) {
                    if (i > 0) {
                        ss << ", ";
                    }
                    ss << DumpExpression(function->children[i].get(), visited);
                }
                ss << "]";
            }
            ss << ")";
            break;
        }
        case ExpressionType::BOUND_AGGREGATE: {
            auto aggregate = static_cast<BoundAggregateExpression*>(expr);
            ss << "BoundAggregate(" << aggregate->function_name << "#" << aggregate->agg_oid;
            if (aggregate->is_distinct) {
                ss << ", distinct";
            }
            if (!aggregate->children.empty()) {
                ss << ", children=[";
                for (size_t i = 0; i < aggregate->children.size(); ++i) {
                    if (i > 0) {
                        ss << ", ";
                    }
                    ss << DumpExpression(aggregate->children[i].get(), visited);
                }
                ss << "]";
            }
            ss << ")";
            break;
        }
        case ExpressionType::BOUND_CONJUNCTION: {
            auto conjunction = static_cast<BoundConjunctionExpression*>(expr);
            ss << "BoundConjunction(type=" << conjunction->bool_expr_type << ", children=[";
            for (size_t i = 0; i < conjunction->children.size(); ++i) {
                if (i > 0) {
                    ss << ", ";
                }
                ss << DumpExpression(conjunction->children[i].get(), visited);
            }
            ss << "])";
            break;
        }
        case ExpressionType::BOUND_SUBQUERY: {
            auto subquery = static_cast<BoundSubqueryExpression*>(expr);
            ss << "BoundSubquery(type=" << subquery->sublink_name;
            if (subquery->subquery_plan) {
                ss << ", subquery_plan={"
                   << DumpLogicalPlan(subquery->subquery_plan.get())
                   << "}";
            }
            if (!subquery->children.empty()) {
                ss << ", children=[";
                for (size_t i = 0; i < subquery->children.size(); ++i) {
                    if (i > 0) {
                        ss << ", ";
                    }
                    ss << DumpExpression(subquery->children[i].get(), visited);
                }
                ss << "]";
            }
            ss << ")";
            break;
        }
        default:
            ss << "OpaqueExpression";
            break;
    }

    return ss.str();
}

std::string DuckDBExecutorScan::DumpExpressionList(const std::vector<std::unique_ptr<Expression>>& expressions) {
    std::stringstream ss;
    ss << "[";
    std::set<const Expression*> visited;
    for (size_t i = 0; i < expressions.size(); ++i) {
        if (i > 0) {
            ss << ", ";
        }
        ss << DumpExpression(expressions[i].get(), visited);
    }
    ss << "]";
    return ss.str();
}

std::string DuckDBExecutorScan::DumpExpressionList(const std::vector<Expression*>& expressions) {
    std::stringstream ss;
    ss << "[";
    std::set<const Expression*> visited;
    for (size_t i = 0; i < expressions.size(); ++i) {
        if (i > 0) {
            ss << ", ";
        }
        ss << DumpExpression(expressions[i], visited);
    }
    ss << "]";
    return ss.str();
}

std::string DuckDBExecutorScan::DumpLogicalPlan(LogicalOperator* op, int depth) {
    std::set<const LogicalOperator*> visited;
    return DumpLogicalPlan(op, depth, visited);
}

std::string DuckDBExecutorScan::DumpLogicalPlan(LogicalOperator* op, int depth, std::set<const LogicalOperator*>& visited) {
    if (!op) return "";
    if (!visited.insert(op).second) {
        std::stringstream ss;
        for (int i = 0; i < depth; ++i) ss << "  ";
        ss << "<cycle>\n";
        return ss.str();
    }
    std::stringstream ss;
    for(int i=0; i<depth; ++i) ss << "  "; // indent
    
    switch (op->type) {
        case LogicalOperatorType::LOGICAL_GET: {
            auto get = static_cast<LogicalGet*>(op);
            ss << "LogicalGet(table=" << get->table_name
               << ", table_index=#" << get->table_index.index
               << ", pg_rtindex=" << get->pg_rtindex
               << ", relid=" << get->relid
               << ", filters=" << DumpExpressionList(get->filters)
               << ", estimated_cardinality=" << get->estimated_cardinality << ")\n";
            break;
        }
        case LogicalOperatorType::LOGICAL_FILTER: {
            auto filter = static_cast<LogicalFilter*>(op);
            ss << "LogicalFilter(expressions=" << DumpExpressionList(filter->expressions)
               << ", estimated_cardinality=" << filter->estimated_cardinality << ")\n";
            break;
        }
        case LogicalOperatorType::LOGICAL_PROJECTION: {
            auto projection = static_cast<LogicalProjection*>(op);
            ss << "LogicalProjection(table_index=#" << projection->table_index.index
               << ", output_names=[";
            for (size_t i = 0; i < projection->output_names.size(); ++i) {
                if (i > 0) {
                    ss << ", ";
                }
                ss << projection->output_names[i];
            }
            ss << "]"
               << ", expressions=" << DumpExpressionList(projection->expressions)
               << ", estimated_cardinality=" << projection->estimated_cardinality << ")\n";
            break;
        }
        case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
            auto aggregate = static_cast<LogicalAggregate*>(op);
            ss << "LogicalAggregate(group_index=#" << aggregate->group_index.index
               << ", aggregate_index=#" << aggregate->aggregate_index.index
               << ", group_names=[";
            for (size_t i = 0; i < aggregate->group_names.size(); ++i) {
                if (i > 0) {
                    ss << ", ";
                }
                ss << aggregate->group_names[i];
            }
            ss << "]"
               << ", aggregate_names=[";
            for (size_t i = 0; i < aggregate->aggregate_names.size(); ++i) {
                if (i > 0) {
                    ss << ", ";
                }
                ss << aggregate->aggregate_names[i];
            }
            ss << "]"
               << ", groups=" << DumpExpressionList(aggregate->groups)
               << ", expressions=" << DumpExpressionList(aggregate->expressions)
               << ", estimated_cardinality=" << aggregate->estimated_cardinality << ")\n";
            break;
        }
        case LogicalOperatorType::LOGICAL_DISTINCT: {
            auto distinct = static_cast<LogicalDistinct*>(op);
            ss << "LogicalDistinct(expressions=" << DumpExpressionList(distinct->expressions)
               << ", estimated_cardinality=" << distinct->estimated_cardinality << ")\n";
            break;
        }
        case LogicalOperatorType::LOGICAL_SET_OPERATION: {
            auto setop = static_cast<LogicalSetOperation*>(op);
            ss << "LogicalSetOperation(type=" << SetOperationName(setop->setop_type)
               << ", all=" << (setop->all ? "true" : "false")
               << ", table_index=#" << setop->table_index.index
               << ", output_names=[";
            for (size_t i = 0; i < setop->output_names.size(); ++i) {
                if (i > 0) {
                    ss << ", ";
                }
                ss << setop->output_names[i];
            }
            ss << "]"
               << ", estimated_cardinality=" << setop->estimated_cardinality << ")\n";
            break;
        }
        case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
        case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
        case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
            auto join = static_cast<LogicalComparisonJoin*>(op);
            const char* join_name = op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN
                ? "LogicalDelimJoin"
                : (op->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN ? "LogicalDependentJoin" : "LogicalComparisonJoin");
            ss << join_name << "(type=" << JoinTypeName(join->join_type);
            if (join->children_swapped) {
                ss << ", children_swapped=true";
            }
            if (join->has_mark_index &&
                join->mark_index.index != static_cast<size_t>(-1)) {
                ss << ", mark_index=#" << join->mark_index.index;
            }
            if (op->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN ||
                op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
                auto dependent_join = static_cast<LogicalDependentJoin*>(op);
                if (!dependent_join->correlated_columns.empty()) {
                    ss << ", correlated_columns=[";
                    for (size_t i = 0; i < dependent_join->correlated_columns.size(); ++i) {
                        if (i > 0) {
                            ss << ", ";
                        }
                        auto& col = dependent_join->correlated_columns[i];
                        ss << "#" << col.table_index.index << "." << col.column_index.index;
                    }
                    ss << "]";
                }
                ss << ""
                   << ", perform_delim=" << (dependent_join->perform_delim ? "true" : "false")
                   << ", any_join=" << (dependent_join->any_join ? "true" : "false")
                   << ", invert_result=" << (dependent_join->invert_result ? "true" : "false")
                   << ", propagate_null_values=" << (dependent_join->propagate_null_values ? "true" : "false")
                   << ", is_lateral_join=" << (dependent_join->is_lateral_join ? "true" : "false");
            } else {
                ss << ", perform_delim=false"
                   << ", any_join=false"
                   << ", invert_result=" << ((join->join_type == JOIN_MARK && join->invert_result) ? "true" : "false")
                   << ", propagate_null_values=false"
                   << ", is_lateral_join=false";
            }
            ss << ", conditions=" << DumpExpressionList(join->conditions)
               << ", estimated_cardinality=" << join->estimated_cardinality << ")\n";
            break;
        }
        case LogicalOperatorType::LOGICAL_DELIM_GET: {
            auto delim_get = static_cast<LogicalDelimGet*>(op);
            ss << "LogicalDelimGet(table_index=#" << delim_get->table_index.index
               << ", correlated_columns=[";
            for (size_t i = 0; i < delim_get->correlated_columns.size(); ++i) {
                if (i > 0) {
                    ss << ", ";
                }
                auto& col = delim_get->correlated_columns[i];
                ss << "(" << col.table_index.index << "." << col.column_index.index << ")";
            }
            ss << "]"
               << ", estimated_cardinality=" << delim_get->estimated_cardinality << ")\n";
            break;
        }
        case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
            ss << "LogicalCrossProduct(estimated_cardinality=" << op->estimated_cardinality << ")\n";
            break;
        case LogicalOperatorType::LOGICAL_ORDER: {
            auto order = static_cast<LogicalOrder*>(op);
            ss << "LogicalOrder(orders=[";
            for (size_t i = 0; i < order->orders.size(); ++i) {
                if (i > 0) {
                    ss << ", ";
                }
                ss << DumpExpression(order->orders[i].expression.get());
            }
            ss << "], estimated_cardinality=" << order->estimated_cardinality << ")\n";
            break;
        }
        case LogicalOperatorType::LOGICAL_WINDOW: {
            auto window = static_cast<LogicalWindow*>(op);
            ss << "LogicalWindow(table_index=#" << window->table_index.index
               << ", functions=[";
            for (size_t i = 0; i < window->function_names.size(); ++i) {
                if (i > 0) {
                    ss << ", ";
                }
                ss << window->function_names[i];
            }
            ss << "], output_names=[";
            for (size_t i = 0; i < window->output_names.size(); ++i) {
                if (i > 0) {
                    ss << ", ";
                }
                ss << window->output_names[i];
            }
            ss << "]"
               << ", partitions=" << DumpExpressionList(window->partitions)
               << ", orders=[";
            for (size_t i = 0; i < window->orders.size(); ++i) {
                if (i > 0) {
                    ss << ", ";
                }
                ss << DumpExpression(window->orders[i].expression.get());
            }
            ss << "], estimated_cardinality=" << window->estimated_cardinality << ")\n";
            break;
        }
        case LogicalOperatorType::LOGICAL_LIMIT: {
            auto limit = static_cast<LogicalLimit*>(op);
            ss << "LogicalLimit(limit_count=" << DumpExpression(limit->limit_count.get())
               << ", limit_offset=" << DumpExpression(limit->limit_offset.get())
               << ", estimated_cardinality=" << limit->estimated_cardinality << ")\n";
            break;
        }
        default: ss << "LogicalUnknown\n"; break;
    }
    
    for (auto& child : op->children) {
        ss << DumpLogicalPlan(child.get(), depth + 1, visited);
    }
    return ss.str();
}

std::string DuckDBExecutorScan::DumpPhysicalPlan(PhysicalOperator* op, int depth) {
    std::set<const PhysicalOperator*> visited;
    return DumpPhysicalPlan(op, depth, visited);
}

std::string DuckDBExecutorScan::DumpPhysicalPlan(PhysicalOperator* op, int depth, std::set<const PhysicalOperator*>& visited) {
    if (!op) return "";
    if (!visited.insert(op).second) {
        std::stringstream ss;
        for (int i = 0; i < depth; ++i) ss << "  ";
        ss << "<cycle>\n";
        return ss.str();
    }

    std::stringstream ss;
    for (int i = 0; i < depth; ++i) ss << "  ";

    switch (op->type) {
        case PhysicalOperatorType::TABLE_SCAN: {
            auto scan = static_cast<PhysicalTableScan*>(op);
            ss << "PhysicalTableScan(table=" << scan->table_name
               << ", table_index=#" << scan->table_index.index
               << ", pg_rtindex=" << scan->pg_rtindex
               << ", filters=" << DumpExpressionList(scan->filters)
               << ", estimated_cardinality=" << scan->estimated_cardinality << ")\n";
            break;
        }
        case PhysicalOperatorType::PROJECTION: {
            auto projection = static_cast<PhysicalProjection*>(op);
            ss << "PhysicalProjection(select_list=" << DumpExpressionList(projection->select_list)
               << ", output_names=[";
            for (size_t i = 0; i < projection->output_names.size(); ++i) {
                if (i > 0) {
                    ss << ", ";
                }
                ss << projection->output_names[i];
            }
            ss << "]"
               << ", estimated_cardinality=" << projection->estimated_cardinality << ")\n";
            break;
        }
        case PhysicalOperatorType::FILTER: {
            auto filter = static_cast<PhysicalFilter*>(op);
            ss << "PhysicalFilter(expressions=" << DumpExpressionList(filter->expressions)
               << ", estimated_cardinality=" << filter->estimated_cardinality << ")\n";
            break;
        }
        case PhysicalOperatorType::DISTINCT: {
            auto distinct = static_cast<PhysicalDistinct*>(op);
            ss << "PhysicalDistinct(expressions=" << DumpExpressionList(distinct->expressions)
               << ", estimated_cardinality=" << distinct->estimated_cardinality << ")\n";
            break;
        }
        case PhysicalOperatorType::SET_OPERATION: {
            auto setop = static_cast<PhysicalSetOperation*>(op);
            ss << "PhysicalSetOperation(type=" << SetOperationName(setop->setop_type)
               << ", all=" << (setop->all ? "true" : "false")
               << ", table_index=#" << setop->table_index.index
               << ", output_names=[";
            for (size_t i = 0; i < setop->output_names.size(); ++i) {
                if (i > 0) {
                    ss << ", ";
                }
                ss << setop->output_names[i];
            }
            ss << "]"
               << ", estimated_cardinality=" << setop->estimated_cardinality << ")\n";
            break;
        }
        case PhysicalOperatorType::HASH_JOIN: {
            auto join = static_cast<PhysicalHashJoin*>(op);
            ss << "PhysicalHashJoin(type=" << JoinTypeName(join->join_type)
               << ", dependent=" << (join->dependent ? "true" : "false");
            if (join->children_swapped) {
                ss << ", children_swapped=true";
            }
            if (join->has_mark_index && join->mark_index.index != static_cast<size_t>(-1)) {
                ss << ", mark_index=#" << join->mark_index.index;
            }
            if (join->dependent && !join->correlated_columns.empty()) {
                ss << ", correlated_columns=[";
                for (size_t i = 0; i < join->correlated_columns.size(); ++i) {
                    if (i > 0) {
                        ss << ", ";
                    }
                    auto& col = join->correlated_columns[i];
                    ss << "(" << col.table_index.index << "." << col.column_index.index << ")";
                }
                ss << "]";
            }
            ss << ", invert_result=" << (join->invert_result ? "true" : "false");
            ss << ", conditions=" << DumpExpressionList(join->conditions)
               << ", estimated_cardinality=" << join->estimated_cardinality << ")\n";
            break;
        }
        case PhysicalOperatorType::DELIM_GET: {
            auto delim_get = static_cast<PhysicalDelimGet*>(op);
            ss << "PhysicalDelimGet(table_index=#" << delim_get->table_index.index
               << ", correlated_columns=[";
            for (size_t i = 0; i < delim_get->correlated_columns.size(); ++i) {
                if (i > 0) {
                    ss << ", ";
                }
                auto& col = delim_get->correlated_columns[i];
                ss << "(" << col.table_index.index << "." << col.column_index.index << ")";
            }
            ss << "]"
               << ", estimated_cardinality=" << delim_get->estimated_cardinality << ")\n";
            break;
        }
        case PhysicalOperatorType::CROSS_PRODUCT:
            ss << "PhysicalCrossProduct(estimated_cardinality=" << op->estimated_cardinality << ")\n";
            break;
        case PhysicalOperatorType::HASH_GROUP_BY: {
            auto aggregate = static_cast<PhysicalHashAggregate*>(op);
            ss << "PhysicalHashAggregate(group_index=#" << aggregate->group_index.index
               << ", aggregate_index=#" << aggregate->aggregate_index.index
               << ", group_names=[";
            for (size_t i = 0; i < aggregate->group_names.size(); ++i) {
                if (i > 0) {
                    ss << ", ";
                }
                ss << aggregate->group_names[i];
            }
            ss << "]"
               << ", aggregate_names=[";
            for (size_t i = 0; i < aggregate->aggregate_names.size(); ++i) {
                if (i > 0) {
                    ss << ", ";
                }
                ss << aggregate->aggregate_names[i];
            }
            ss << "]"
               << ", groups=" << DumpExpressionList(aggregate->groups)
               << ", expressions=" << DumpExpressionList(aggregate->expressions)
               << ", estimated_cardinality=" << aggregate->estimated_cardinality << ")\n";
            break;
        }
        case PhysicalOperatorType::ORDER_BY: {
            auto order = static_cast<PhysicalOrderBy*>(op);
            ss << "PhysicalOrder(orders=" << DumpExpressionList(order->orders)
               << ", estimated_cardinality=" << order->estimated_cardinality << ")\n";
            break;
        }
        case PhysicalOperatorType::LIMIT: {
            auto limit = static_cast<PhysicalLimit*>(op);
            ss << "PhysicalLimit(limit_count=" << DumpExpression(limit->limit_count)
               << ", limit_offset=" << DumpExpression(limit->limit_offset)
               << ", estimated_cardinality=" << limit->estimated_cardinality << ")\n";
            break;
        }
        case PhysicalOperatorType::WINDOW: {
            auto window = static_cast<PhysicalWindow*>(op);
            ss << "PhysicalWindow(table_index=#" << window->table_index.index
               << ", functions=[";
            for (size_t i = 0; i < window->function_names.size(); ++i) {
                if (i > 0) {
                    ss << ", ";
                }
                ss << window->function_names[i];
            }
            ss << "], output_names=[";
            for (size_t i = 0; i < window->output_names.size(); ++i) {
                if (i > 0) {
                    ss << ", ";
                }
                ss << window->output_names[i];
            }
            ss << "]"
               << ", partitions=" << DumpExpressionList(window->partitions)
               << ", orders=" << DumpExpressionList(window->orders)
               << ", estimated_cardinality=" << window->estimated_cardinality << ")\n";
            break;
        }
        default:
            ss << "PhysicalUnknown\n";
            break;
    }

    for (auto& child : op->children) {
        ss << DumpPhysicalPlan(child.get(), depth + 1, visited);
    }
    return ss.str();
}

::PlannedStmt* DuckDBExecutorScan::CreateCustomScanPlan(::Query* pg_query,
                                                        LogicalOperator* logical_plan,
                                                        PhysicalOperator* physical_plan) {
    if (!logical_plan || !physical_plan) return nullptr;

    PlannedStmt* pstmt = makeNode(PlannedStmt);
    pstmt->commandType = pg_query->commandType;
    pstmt->queryId = pg_query->queryId;
    pstmt->hasReturning = false; /* Since it's CMD_SELECT, no RETURNING */
    pstmt->hasModifyingCTE = pg_query->hasModifyingCTE;
    pstmt->canSetTag = pg_query->canSetTag;
    pstmt->transientPlan = pg_query->hasSubLinks;
    pstmt->dependsOnRole = false;
    pstmt->parallelModeNeeded = false;
    pstmt->jitFlags = 0;
    pstmt->rtable = pg_query->rtable;
    pstmt->resultRelations = nullptr;
    pstmt->appendRelations = nullptr;

    CustomScan* cscan = makeNode(CustomScan);
    cscan->scan.scanrelid = 0; 
    cscan->scan.plan.targetlist = nullptr;
    cscan->scan.plan.qual = nullptr;
    cscan->scan.plan.lefttree = nullptr;
    cscan->scan.plan.righttree = nullptr;
    cscan->flags = 0;
    cscan->methods = &duckdb_scan_methods;
    
    std::string dump = "Logical Plan:\n";
    dump += DumpLogicalPlan(logical_plan);

    dump += "\nPhysical Plan:\n";
    dump += DumpPhysicalPlan(physical_plan);
    cscan->custom_private = list_make1(makeString(pstrdup(dump.c_str())));

    pstmt->planTree = (Plan*)cscan;

    return pstmt;
}

} // namespace duckdbopt
