if not have_cxx
  subdir_done()
endif

pg_volvec_sources = files(
  'src/bridge/pg_volvec.c',
  'src/bridge/state.c',
  'src/bridge/execute.cpp',
  'src/engine/expr.cpp',
  'src/engine/executor.cpp',
)

pg_volvec_optimize_args = [
  '-O3',
  '-march=native',
  '-ftree-vectorize',
  '-funroll-loops',
  '-ffast-math',
]

pg_volvec_mod_args = contrib_mod_args + {
  'cpp_args': contrib_mod_args.get('cpp_args', []) + pg_volvec_optimize_args,
  'c_args': contrib_mod_args.get('c_args', []) + pg_volvec_optimize_args,
}

if llvm.found()
  pg_volvec_sources += files('src/engine/llvmjit_deform_datachunk.cpp', 'src/engine/llvmjit_expr.cpp')
  
  # On macOS, we might need to link against the llvmjit module to find symbols like llvm_create_context
  # because it's not in the main postgres binary.
  # Use an absolute path for reliability in this specific environment.
  llvmjit_lib = cc.find_library('llvmjit', dirs: ['/Users/chenyunwen/proj/postgres/installed/lib'], required: false)
  
  deps = [llvm]
  if llvmjit_lib.found()
    deps += llvmjit_lib
  endif

  pg_volvec_mod_args = pg_volvec_mod_args + {
    'dependencies': contrib_mod_args['dependencies'] + deps,
    'cpp_args': pg_volvec_mod_args['cpp_args'] + llvm.get_variable(configtool: 'cxxflags').split(),
  }
endif

inc = include_directories('src/bridge', 'src/engine')

pg_volvec = shared_module('pg_volvec',
  pg_volvec_sources,
  include_directories: inc,
  c_pch: pch_postgres_h,
  kwargs: pg_volvec_mod_args,
)
contrib_targets += pg_volvec

install_data(
  'pg_volvec.control',
  'pg_volvec--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_volvec',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'smoke',
      'q1',
      'q6',
    ],
  },
}
