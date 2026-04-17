import re

def extract_signatures():
    with open('src/engine/exec/executor_common.cpp', 'r') as f:
        content = f.read()

    # Find static functions. They start with "static " at the beginning of a line,
    # followed by return type, then function name, then arguments, then "{" or " {"
    # Since C++ formatting can vary, we will use a regex.
    # It might be easier to just remove "static " and put them in a header.
    
    lines = content.split('\n')
    out_lines = []
    
    in_decl = False
    current_decl = []
    
    for i, line in enumerate(lines):
        if line.startswith('static '):
            if '=' in line and '(' not in line: # static variable
                pass
            else:
                in_decl = True
                current_decl = [line.replace('static inline ', '').replace('static ', '')]
                if ')' in line and not line.endswith(','):
                    in_decl = False
                    out_lines.append(' '.join(current_decl) + ';')
        elif in_decl:
            current_decl.append(line.strip())
            if ')' in line and not line.endswith(','):
                in_decl = False
                out_lines.append(' '.join(current_decl) + ';')
                
    with open('src/engine/exec/executor_internal.hpp', 'w') as f:
        f.write('#pragma once\n\n')
        f.write('#include "volvec_engine.hpp"\n')
        f.write('namespace pg_volvec {\n\n')
        for decl in out_lines:
            # remove trailing { if any
            decl = decl.split('{')[0].strip()
            if not decl.endswith(';'):
                decl += ';'
            f.write(decl + '\n')
        f.write('\n}\n')

if __name__ == '__main__':
    extract_signatures()
