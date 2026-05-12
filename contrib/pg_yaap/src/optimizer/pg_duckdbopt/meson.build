project('pg_duckdbopt', 'cpp', 'c', default_options: ['cpp_std=c++17'])

pg_config = find_program('/Users/chenyunwen/proj/pg_for_opt/pg18installed/bin/pg_config')
psql = find_program('/Users/chenyunwen/proj/pg_for_opt/pg18installed/bin/psql')

pg_includedir = run_command(pg_config, '--includedir-server', check: true).stdout().strip()
pg_libdir = run_command(pg_config, '--libdir', check: true).stdout().strip()
pg_extdir = run_command(pg_config, '--sharedir', check: true).stdout().strip() / 'extension'
pg_pkglibdir = run_command(pg_config, '--pkglibdir', check: true).stdout().strip()

pg_dep = declare_dependency(
  include_directories: include_directories(pg_includedir)
)

pg_duckdbopt_sources = files(
  'src/pg_duckdbopt.cpp',
  'src/adapter/duckdb_adapter.cpp',
  'src/catalog/pg_external_catalog.cpp',
  'src/planner/decorrelate_dependent_join.cpp',
  'src/planner/decorrelate_rewrite_utils.cpp',
  'src/planner/delim_join_cleanup.cpp',
  'src/planner/logical_planner.cpp',
  'src/planner/mark_join_cleanup.cpp',
  'src/planner/planner_normalizer.cpp',
  'src/logical/expression_rewriters.cpp',
  'src/logical/filter_rewrite_utils.cpp',
  'src/logical/logical_utils.cpp',
  'src/logical/mark_join_normalization.cpp',
  'src/optimizer/filter_pushdown.cpp',
  'src/optimizer/join_predicate_extraction.cpp',
  'src/optimizer/optimizer_core.cpp',
  'src/optimizer/join_order_cost_model.cpp',
  'src/optimizer/join_order_filter_info.cpp',
  'src/optimizer/join_order_optimizer.cpp',
  'src/optimizer/join_order_plan_node.cpp',
  'src/optimizer/join_order_plan_enumerator.cpp',
  'src/optimizer/join_order_query_graph.cpp',
  'src/optimizer/join_order_query_graph_edges.cpp',
  'src/optimizer/join_order_query_graph_manager.cpp',
  'src/optimizer/join_order_reconstruct.cpp',
  'src/optimizer/join_order_relation_manager.cpp',
  'src/optimizer/predicate_propagation.cpp',
  'src/optimizer/scan_filter_folding.cpp',
  'src/optimizer/join_order_relation_set.cpp',
  'src/optimizer/optimizer_stats.cpp',
  'src/physical/physical_plan_generator.cpp',
  'src/physical/physical_planner.cpp',
  'src/executor/duckdb_scan.cpp'
)

# On macOS, Postgres extensions require dynamic_lookup for undefined symbols from the backend
shared_module('pg_duckdbopt',
  pg_duckdbopt_sources,
  dependencies: pg_dep,
  name_prefix: '',
  link_args: ['-Wl,-undefined,dynamic_lookup'],
  cpp_args: ['-fno-rtti'],
  install: true,
  install_dir: pg_pkglibdir
)

install_data(
  'pg_duckdbopt.control',
  'pg_duckdbopt--1.0.sql',
  install_dir: pg_extdir
)

test_config = configuration_data()
test_config.set_quoted('PSQL', psql.full_path())
test_config.set_quoted('SOURCE_DIR', meson.current_source_dir())
test_config.set_quoted('BUILD_DIR', meson.current_build_dir())

test_script = configure_file(
  input: 'test/pg_duckdbopt.sh.in',
  output: 'pg_duckdbopt.sh',
  configuration: test_config,
)

test_cases = [
  ['pg_duckdbopt-scan-pushdown', 'sql/scan_pushdown.sql', 'expected/scan_pushdown.out', 'pg_opt_scan'],
  ['pg_duckdbopt-query-wrappers', 'sql/query_wrappers.sql', 'expected/query_wrappers.out', 'pg_opt_wrappers'],
  ['pg_duckdbopt-subquery-decorrelation', 'sql/subquery_decorrelation.sql', 'expected/subquery_decorrelation.out', 'pg_opt_subquery'],
  ['pg_duckdbopt-join-order-graph', 'sql/join_order_graph.sql', 'expected/join_order_graph.out', 'pg_opt_join_graph'],
  ['pg_duckdbopt-join-order-filter-info', 'sql/join_order_filter_info.sql', 'expected/join_order_filter_info.out', 'pg_opt_join_filter'],
  ['pg_duckdbopt-join-order-hypergraph', 'sql/join_order_hypergraph.sql', 'expected/join_order_hypergraph.out', 'pg_opt_join_hyper'],
  ['pg_duckdbopt-join-order-semi-anti', 'sql/join_order_semi_anti.sql', 'expected/join_order_semi_anti.out', 'pg_opt_join_semi_anti'],
  ['pg_duckdbopt-join-order-semi-anti-reorder', 'sql/join_order_semi_anti_reorder.sql', 'expected/join_order_semi_anti_reorder.out', 'pg_opt_join_semi_anti_reorder'],
]

foreach test_case : test_cases
  test_env = environment()
  test_env.set('SQL_FILE', meson.current_source_dir() / test_case[1])
  test_env.set('EXPECTED_FILE', meson.current_source_dir() / test_case[2])
  test_env.set('DB_NAME', test_case[3])
  test(test_case[0], find_program('sh'), args: [test_script], env: test_env)
endforeach
