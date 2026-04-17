import os
import sys

def split_file():
    with open('src/engine/volvec_engine.hpp', 'r') as f:
        lines = f.readlines()

    def write_chunk(filename, start_line, end_line, extra_includes=None):
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w') as out:
            out.write('#pragma once\n\n')
            if extra_includes:
                for inc in extra_includes:
                    out.write(f'#include "{inc}"\n')
                out.write('\n')
            out.writelines(lines[start_line-1:end_line])

    # We need to keep the very top includes (lines 1-72) for most files
    top_includes = lines[0:72] # Assuming these are the #includes and #defines

    # core/types.hpp (Common types, numeric, enums) L1-575
    write_chunk('src/engine/core/types.hpp', 1, 575)

    # core/memory.hpp L576-656
    write_chunk('src/engine/core/memory.hpp', 576, 656, ['core/types.hpp'])

    # core/hash_table.hpp L657-739
    write_chunk('src/engine/core/hash_table_defs.hpp', 657, 739, ['core/types.hpp', 'core/memory.hpp'])

    # core/data_chunk.hpp L740-865
    write_chunk('src/engine/core/data_chunk.hpp', 740, 865, ['core/types.hpp', 'core/memory.hpp'])

    # core/data_chunk_deform.hpp L866-912
    write_chunk('src/engine/core/data_chunk_deform.hpp', 866, 912, ['core/data_chunk.hpp'])

    # expr/expr.hpp L913-1009
    write_chunk('src/engine/expr/expr.hpp', 913, 1009, ['core/data_chunk.hpp'])

    # exec/plan_state.hpp L1010-1192
    write_chunk('src/engine/exec/plan_state.hpp', 1010, 1192, ['core/data_chunk.hpp', 'expr/expr.hpp'])

    # parallel/parallel_runtime.hpp L1193-1569
    write_chunk('src/engine/parallel/parallel_runtime.hpp', 1193, 1569, ['exec/plan_state.hpp'])

    # exec/seq_scan.hpp L1570-1620
    write_chunk('src/engine/exec/seq_scan.hpp', 1570, 1620, ['exec/plan_state.hpp'])

    # exec/agg.hpp L1621-1852
    write_chunk('src/engine/exec/agg.hpp', 1621, 1852, ['exec/plan_state.hpp', 'core/hash_table_defs.hpp'])

    # exec/filter.hpp L1853-1909
    write_chunk('src/engine/exec/filter.hpp', 1853, 1909, ['exec/plan_state.hpp'])

    # exec/hash_join.hpp L1910-2522
    write_chunk('src/engine/exec/hash_join.hpp', 1910, 2522, ['exec/plan_state.hpp', 'core/hash_table_defs.hpp'])

    # exec/sort.hpp L2523-2637
    write_chunk('src/engine/exec/sort.hpp', 2523, 2637, ['exec/plan_state.hpp'])

    # exec/query_state.hpp L2638-end
    write_chunk('src/engine/exec/query_state.hpp', 2638, len(lines), ['parallel/parallel_runtime.hpp'])

    # Rewrite volvec_engine.hpp to just include everything for backward compatibility
    with open('src/engine/volvec_engine.hpp', 'w') as out:
        out.write('#pragma once\n\n')
        out.write('#include "core/types.hpp"\n')
        out.write('#include "core/memory.hpp"\n')
        out.write('#include "core/hash_table_defs.hpp"\n')
        out.write('#include "core/data_chunk.hpp"\n')
        out.write('#include "core/data_chunk_deform.hpp"\n')
        out.write('#include "expr/expr.hpp"\n')
        out.write('#include "exec/plan_state.hpp"\n')
        out.write('#include "parallel/parallel_runtime.hpp"\n')
        out.write('#include "exec/seq_scan.hpp"\n')
        out.write('#include "exec/agg.hpp"\n')
        out.write('#include "exec/filter.hpp"\n')
        out.write('#include "exec/hash_join.hpp"\n')
        out.write('#include "exec/sort.hpp"\n')
        out.write('#include "exec/query_state.hpp"\n')

if __name__ == '__main__':
    split_file()
