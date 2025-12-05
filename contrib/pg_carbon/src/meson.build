# Sources are relative to this directory
sources = files(
  'bridge/lib.c',
  'optimizer/optimizer.cpp',
  'optimizer/memo.cpp',
  'optimizer/scheduler.cpp',
  'optimizer/translator.cpp',
  'rules/rules.cpp',
)

if meson.project_name() == 'postgresql'
  # --- In-Tree Build ---
  
  add_languages('cpp')

  # Add to contrib targets
  pg_carbon = shared_module('pg_carbon',
    sources,
    kwargs: contrib_mod_args + {
      'dependencies': contrib_mod_args['dependencies'],
      'cpp_args': ['-Wno-unused-parameter'], # Optional: suppress warnings
    },
  )
  contrib_targets += pg_carbon

  # Install control and SQL files (located in parent dir)
  install_data(
    '../pg_carbon.control',
    '../pg_carbon--1.0.sql',
    kwargs: contrib_data_args,
  )

else
  # --- Standalone Build ---

  pg_config = find_program('pg_config', required: true)
  incdir_server = run_command(pg_config, '--includedir-server', check: true).stdout().strip()
  libdir = run_command(pg_config, '--libdir', check: true).stdout().strip()
  pkglibdir = run_command(pg_config, '--pkglibdir', check: true).stdout().strip()
  sharedir = run_command(pg_config, '--sharedir', check: true).stdout().strip()

  inc = include_directories('.', incdir_server)

  shared_module('pg_carbon',
    sources,
    include_directories: inc,
    install: true,
    install_dir: pkglibdir,
    name_prefix: '',
    link_args: ['-Wl,-undefined,dynamic_lookup']
  )

  install_data('../pg_carbon.control',
    install_dir: sharedir + '/extension'
  )

  install_data('../pg_carbon--1.0.sql',
    install_dir: sharedir + '/extension'
  )
endif
