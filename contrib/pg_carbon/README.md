# pg_carbon: A Next-Generation Optimizer for PostgreSQL

**pg_carbon** is an experimental, extensible query optimizer extension for PostgreSQL, inspired by the state-of-the-art **Columbia** and **Cascades** optimizer frameworks.

It integrates seamlessly with PostgreSQL, replacing or augmenting the default planner to provide a robust platform for advanced query optimization research and development.

## 🚀 Key Features

- **Cascades/Columbia Architecture**: Implements a top-down, rule-based optimization strategy using a sophisticated task scheduling system (`O_Group`, `E_Group`, `O_Expr`, `Apply_Rule`, `O_Inputs`).
- **Memoization**: Efficiently stores and manages groups of equivalent logical and physical expressions (the "Memo") to prune redundant work and ensure optimal sub-problem caching.
- **Rule-Based Transformation**: Separation of logical transformations (exploration) and physical implementations, allowing for easy addition of new optimization rules.
- **PostgreSQL Integration**: Built as a standard PostgreSQL extension. It hooks into the query processing pipeline to intercept and optimize queries while leveraging PostgreSQL's execution engine.

## 🛠️ Build and Installation

pg_carbon comes with a standalone build script using the Meson build system.

### Prerequisites
- A PostgreSQL installation (source or binaries).
- `meson` and `ninja`.
- C++17 compatible compiler.

### Building

Navigate to the extension directory and run the build script:

```bash
cd contrib/pg_carbon
./build.sh
```

This script will:
1. Locate your PostgreSQL installation (using `pg_config`).
2. Compile the extension.
3. Install the extension artifacts (`.so`/`.dylib`, control file, SQL script) into your PostgreSQL directories.

## 📦 Usage

To enable pg_carbon in your database:

1. Connect to your PostgreSQL database:
   ```bash
   psql postgres
   ```

2. Create the extension:
   ```sql
   CREATE EXTENSION pg_carbon;
   ```

3. (Optional) Load it explicitly if not in `shared_preload_libraries`:
   ```sql
   LOAD 'pg_carbon';
   ```

Once loaded, `pg_carbon` works behind the scenes. When a query is submitted, `pg_carbon` intercepts the parse tree, optimizes it using its internal engine, converts the result back into a PostgreSQL `Plan`, and hands it off to the executor.

## 🤝 Relationship with PostgreSQL

pg_carbon is designed to coexist with the standard PostgreSQL planner. It demonstrates how "pluggable optimizers" can be realized in the PostgreSQL ecosystem. While it currently provides a skeleton and core architectural components (Scheduler, Memo, Rules), it aims to eventually support a wide range of SQL features with superior optimization capabilities for complex workload patterns.

## 📄 License

pg_carbon is licensed under the Apache License, Version 2.0. See the [LICENSE](LICENSE) file for more details.
