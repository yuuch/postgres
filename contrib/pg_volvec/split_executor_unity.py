import os

def split_executor_unity():
    with open('src/engine/executor.cpp', 'r') as f:
        lines = f.readlines()

    def write_chunk(filename, start_line, end_line):
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w') as out:
            out.write('// Auto-split from executor.cpp\n')
            out.writelines(lines[start_line-1:end_line])

    # Extract the chunks WITHOUT adding headers/footers
    write_chunk('src/engine/exec/executor_common.cpp', 24, 1472)
    write_chunk('src/engine/exec/agg.cpp', 1473, 3271)
    write_chunk('src/engine/exec/seq_scan.cpp', 3272, 3538)
    write_chunk('src/engine/exec/filter.cpp', 3539, 3546)
    write_chunk('src/engine/exec/hash_join_lookup.cpp', 3547, 4149)
    write_chunk('src/engine/exec/project.cpp', 4150, 4307)
    write_chunk('src/engine/exec/limit.cpp', 4308, 4346)
    write_chunk('src/engine/exec/hash_join.cpp', 4347, 6952)
    write_chunk('src/engine/exec/sort.cpp', 6953, 10134)
    
    end_line = len(lines)
    if lines[-1].strip() == "} /* namespace pg_volvec */":
        end_line -= 1
    write_chunk('src/engine/exec/executor_init.cpp', 10135, end_line)

    # Rewrite executor.cpp to just include the chunks
    with open('src/engine/executor.cpp', 'w') as f:
        f.writelines(lines[0:23]) # The top headers and namespace pg_volvec {
        f.write('#include "exec/executor_common.cpp"\n')
        f.write('#include "exec/agg.cpp"\n')
        f.write('#include "exec/seq_scan.cpp"\n')
        f.write('#include "exec/filter.cpp"\n')
        f.write('#include "exec/hash_join_lookup.cpp"\n')
        f.write('#include "exec/project.cpp"\n')
        f.write('#include "exec/limit.cpp"\n')
        f.write('#include "exec/hash_join.cpp"\n')
        f.write('#include "exec/sort.cpp"\n')
        f.write('#include "exec/executor_init.cpp"\n')
        f.write('} /* namespace pg_volvec */\n')

if __name__ == '__main__':
    split_executor_unity()
