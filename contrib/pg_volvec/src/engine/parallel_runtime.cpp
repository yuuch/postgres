/*
 * parallel_runtime.cpp
 *
 * This file was formerly the hub that #included the four .inc files.
 * Those have been split into separate translation units:
 *   parallel/runtime_lowering.cpp
 *   parallel/runtime_worker_state.cpp
 *   parallel/runtime_execution.cpp
 *   parallel/runtime_worker_main.cpp
 *
 * This file is now empty and can be removed once the build system
 * no longer references it.
 */
