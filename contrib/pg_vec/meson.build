if not have_cxx
  subdir_done()
endif

pg_vec_sources = files(
  'src/bridge/pg_vec.c',
  'src/bridge/state.c',
  'src/bridge/execute.c',
  'src/translate/pg_translate.c',
  'src/engine/scan_filter_agg_exec.cpp',
)

pg_vec_mod_args = contrib_mod_args

if llvm.found()
  pg_vec_sources += files('src/engine/llvmjit_deform_datachunk.cpp')
  pg_vec_mod_args = contrib_mod_args + {
    'dependencies': contrib_mod_args['dependencies'] + [llvm],
    'cpp_args': contrib_mod_args['cpp_args'] + llvm.get_variable(configtool: 'cxxflags').split(),
  }
endif

pg_vec = shared_module('pg_vec',
  pg_vec_sources,
  c_pch: pch_postgres_h,
  kwargs: pg_vec_mod_args,
)
contrib_targets += pg_vec

install_data(
  'pg_vec.control',
  'pg_vec--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_vec',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'q1',
      'q3',
      'q4',
      'q5',
      'q7',
      'q8',
      'q9',
      'q10',
      'q11',
      'q12',
      'q14',
      'q19',
      'plain_expr',
      'q6',
    ],
  },
}
