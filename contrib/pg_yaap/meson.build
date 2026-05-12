if not have_cxx
  subdir_done()
endif

pg_yaap_sources = []

pg_yaap_optimize_args = [
  '-O3',
  '-march=native',
  '-ftree-vectorize',
  '-funroll-loops',
  '-ffast-math',
]

pg_yaap_mod_args = contrib_mod_args + {
  'cpp_args': contrib_mod_args.get('cpp_args', []) + ['-std=c++17'] + pg_yaap_optimize_args,
  'c_args': contrib_mod_args.get('c_args', []) + pg_yaap_optimize_args,
}

subdir('src/bridge')
subdir('src/executor')
subdir('src/optimizer')

if llvm.found()
  pg_yaap_sources += files(
    'src/executor/engine/llvmjit_deform_datachunk.cpp',
    'src/executor/engine/llvmjit_expr.cpp',
  )

  deps = [llvm]
  pg_yaap_mod_args = pg_yaap_mod_args + {
    'dependencies': contrib_mod_args['dependencies'] + deps,
    'cpp_args': pg_yaap_mod_args['cpp_args'] + llvm.get_variable(configtool: 'cxxflags').split(),
  }
endif

pg_yaap_inc = include_directories(
  'src/bridge',
  'src/executor/engine',
  'src/optimizer',
)

pg_yaap = shared_module('pg_yaap',
  pg_yaap_sources,
  include_directories: pg_yaap_inc,
  c_pch: pch_postgres_h,
  kwargs: pg_yaap_mod_args,
)
contrib_targets += pg_yaap

install_data(
  'pg_yaap.control',
  'pg_yaap--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_yaap',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'select_star',
    ],
  },
}
