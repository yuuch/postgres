#ifndef PG_VEC_IR_H
#define PG_VEC_IR_H

#include "postgres.h"

#include "access/attnum.h"
#include "utils/date.h"

enum
{
	PG_VEC_MAX_INPUTS = 2,
	PG_VEC_MAX_SCAN_COLUMNS = 24,
	PG_VEC_MAX_EXPR_NODES = 64,
	PG_VEC_MAX_FILTER_NODES = 64,
	PG_VEC_MAX_GROUP_KEYS = 8,
	PG_VEC_MAX_AGG_CALLS = 8,
	PG_VEC_MAX_OUTPUT_COLUMNS = PG_VEC_MAX_GROUP_KEYS + PG_VEC_MAX_AGG_CALLS,
	PG_VEC_MAX_OUTPUT_EXPR_NODES = 32,
	PG_VEC_INLINE_STRING_MAX = 128,
	PG_VEC_MAX_JOIN_KEYS = 4
};

typedef enum PgVecPlanKind
{
	PG_VEC_PLAN_UNSUPPORTED = 0,
	PG_VEC_PLAN_SCAN_FILTER_AGG
} PgVecPlanKind;

typedef enum PgVecScalarKind
{
	PG_VEC_SCALAR_INVALID = 0,
	PG_VEC_SCALAR_INT32,
	PG_VEC_SCALAR_DATE32,
	PG_VEC_SCALAR_DECIMAL64_S2,
	PG_VEC_SCALAR_DECIMAL128_S4,
	PG_VEC_SCALAR_DECIMAL128_S6,
	PG_VEC_SCALAR_CHAR1,
	PG_VEC_SCALAR_STRING128
} PgVecScalarKind;

typedef struct PgVecStringConst
{
	uint16		len;
	char		bytes[PG_VEC_INLINE_STRING_MAX];
} PgVecStringConst;

typedef struct PgVecColumnRef
{
	uint8		input_id;
	AttrNumber	attno;
	PgVecScalarKind scalar_kind;
} PgVecColumnRef;

typedef union PgVecConstValue
{
	int32		int32_value;
	DateADT		date32;
	int64		decimal64_s2;
	char		char1;
	PgVecStringConst string128;
} PgVecConstValue;

typedef enum PgVecExprKind
{
	PG_VEC_EXPR_INVALID = 0,
	PG_VEC_EXPR_COLUMN,
	PG_VEC_EXPR_CONST,
	PG_VEC_EXPR_ADD,
	PG_VEC_EXPR_SUB,
	PG_VEC_EXPR_MUL
} PgVecExprKind;

typedef struct PgVecExprNode
{
	PgVecExprKind kind;
	PgVecScalarKind scalar_kind;
	int			left;
	int			right;
	PgVecColumnRef column;
	PgVecConstValue constant;
} PgVecExprNode;

typedef struct PgVecExprProgram
{
	int			root;
	int			nnodes;
	PgVecExprNode nodes[PG_VEC_MAX_EXPR_NODES];
} PgVecExprProgram;

typedef enum PgVecFilterOp
{
	PG_VEC_OP_INVALID = 0,
	PG_VEC_OP_EQ,
	PG_VEC_OP_LT,
	PG_VEC_OP_LE,
	PG_VEC_OP_GT,
	PG_VEC_OP_GE,
	PG_VEC_OP_PREFIX_LIKE
} PgVecFilterOp;

typedef enum PgVecQualKind
{
	PG_VEC_QUAL_INVALID = 0,
	PG_VEC_QUAL_COMPARE,
	PG_VEC_QUAL_AND,
	PG_VEC_QUAL_OR
} PgVecQualKind;

typedef struct PgVecQualNode
{
	PgVecQualKind kind;
	int			left;
	int			right;
	int			lhs_expr;
	int			rhs_expr;
	PgVecFilterOp op;
} PgVecQualNode;

typedef struct PgVecFilterSpec
{
	PgVecExprProgram exprs;
	int			root;
	int			nnodes;
	PgVecQualNode nodes[PG_VEC_MAX_FILTER_NODES];
} PgVecFilterSpec;

typedef enum PgVecAggKind
{
	PG_VEC_AGG_INVALID = 0,
	PG_VEC_AGG_SUM,
	PG_VEC_AGG_AVG,
	PG_VEC_AGG_COUNT,
	PG_VEC_AGG_MIN,
	PG_VEC_AGG_MAX
} PgVecAggKind;

typedef struct PgVecAggCall
{
	PgVecAggKind kind;
	bool		star_arg;
	bool		has_filter;
	bool		zero_if_empty;
	PgVecExprProgram expr;
	PgVecFilterSpec filter;
} PgVecAggCall;

typedef enum PgVecOutputExprKind
{
	PG_VEC_OUTPUT_EXPR_INVALID = 0,
	PG_VEC_OUTPUT_EXPR_GROUP_KEY,
	PG_VEC_OUTPUT_EXPR_AGGREF,
	PG_VEC_OUTPUT_EXPR_CONST,
	PG_VEC_OUTPUT_EXPR_ADD,
	PG_VEC_OUTPUT_EXPR_SUB,
	PG_VEC_OUTPUT_EXPR_MUL,
	PG_VEC_OUTPUT_EXPR_DIV
} PgVecOutputExprKind;

typedef struct PgVecOutputExprNode
{
	PgVecOutputExprKind kind;
	PgVecScalarKind scalar_kind;
	int			left;
	int			right;
	int			index;
	PgVecConstValue constant;
} PgVecOutputExprNode;

typedef struct PgVecOutputExprProgram
{
	int			root;
	int			nnodes;
	PgVecOutputExprNode nodes[PG_VEC_MAX_OUTPUT_EXPR_NODES];
} PgVecOutputExprProgram;

typedef struct PgVecAggSpec
{
	bool		grouped;
	int			ngroup_keys;
	PgVecColumnRef group_keys[PG_VEC_MAX_GROUP_KEYS];
	int			noutputs;
	PgVecOutputExprProgram outputs[PG_VEC_MAX_OUTPUT_COLUMNS];
	int			naggs;
	PgVecAggCall aggs[PG_VEC_MAX_AGG_CALLS];
} PgVecAggSpec;

typedef struct PgVecInputSpec
{
	Oid			relid;
	int			ncolumns;
	PgVecColumnRef columns[PG_VEC_MAX_SCAN_COLUMNS];
	PgVecFilterSpec filter;
} PgVecInputSpec;

typedef enum PgVecJoinKind
{
	PG_VEC_JOIN_INVALID = 0,
	PG_VEC_JOIN_INNER
} PgVecJoinKind;

typedef struct PgVecJoinKey
{
	PgVecColumnRef left;
	PgVecColumnRef right;
} PgVecJoinKey;

typedef struct PgVecJoinSpec
{
	bool		enabled;
	PgVecJoinKind kind;
	int			left_input;
	int			right_input;
	int			nkeys;
	PgVecJoinKey keys[PG_VEC_MAX_JOIN_KEYS];
} PgVecJoinSpec;

typedef struct PgVecPlan
{
	PgVecPlanKind kind;
	int			ninputs;
	PgVecInputSpec inputs[PG_VEC_MAX_INPUTS];
	PgVecJoinSpec join;
	PgVecAggSpec agg;
} PgVecPlan;

#endif
