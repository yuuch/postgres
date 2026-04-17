import os

def split_executor():
    with open('src/engine/executor.cpp', 'r') as f:
        lines = f.readlines()

    header = """#include "volvec_engine.hpp"
#include "hash_table.hpp"
#include "llvmjit_deform_datachunk.h"

#include <algorithm>
#include <cstring>

extern "C" {
#include "utils/lsyscache.h"
#include "access/tableam.h"
#include "access/visibilitymap.h"
#include "access/stratnum.h"
#include "executor/nodeSubplan.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "storage/bufmgr.h"

extern bool pg_volvec_jit_deform;
extern bool pg_volvec_trace_hooks;
}

namespace pg_volvec
{
"""

    footer = "\n} // namespace pg_volvec\n"

    def write_chunk(filename, start_line, end_line):
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w') as out:
            out.write(header)
            out.writelines(lines[start_line-1:end_line])
            if lines[end_line-1].strip() != "} /* namespace pg_volvec */":
                out.write(footer)

    # L24 - L1472 -> executor_common.cpp
    write_chunk('src/engine/exec/executor_common.cpp', 24, 1472)
    # L1473 - L3271 -> agg.cpp
    write_chunk('src/engine/exec/agg.cpp', 1473, 3271)
    # L3272 - L3538 -> seq_scan.cpp
    write_chunk('src/engine/exec/seq_scan.cpp', 3272, 3538)
    # L3539 - L3546 -> filter.cpp
    write_chunk('src/engine/exec/filter.cpp', 3539, 3546)
    # L3547 - L4149 -> hash_join_lookup.cpp
    write_chunk('src/engine/exec/hash_join_lookup.cpp', 3547, 4149)
    # L4150 - L4307 -> project.cpp
    write_chunk('src/engine/exec/project.cpp', 4150, 4307)
    # L4308 - L4346 -> limit.cpp
    write_chunk('src/engine/exec/limit.cpp', 4308, 4346)
    # L4347 - L6952 -> hash_join.cpp
    write_chunk('src/engine/exec/hash_join.cpp', 4347, 6952)
    # L6953 - L10134 -> sort.cpp
    write_chunk('src/engine/exec/sort.cpp', 6953, 10134)
    
    # ExecInitVecPlan ends at the end of the file which is line 10565 roughly
    # We should exclude the closing brace of namespace pg_volvec at the very end
    end_line = len(lines)
    if lines[-1].strip() == "} /* namespace pg_volvec */":
        end_line -= 1
    write_chunk('src/engine/exec/executor_init.cpp', 10135, end_line)

if __name__ == '__main__':
    split_executor()
