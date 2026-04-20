# Examples

Runnable code for each option described in the article. All three examples implement the same SCD Type 1 merge workload.

| Folder | Option |
|---|---|
| `1_stored_proc/` | Option 1: Stored Procedures |
| `2_notebook/` | Option 2: Notebooks |
| `3_ml_job/` | Option 3: ML Jobs |

## Prerequisites

### Local tools

1. **Python 3.9+**
2. **[uv](https://docs.astral.sh/uv/getting-started/installation/)** for dependency management
3. **[Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/installation/installation)** (`snow`) for deploying objects and running SQL
4. **[just](https://github.com/casey/just#installation)** as a command runner

### Snowflake connection

Configure a named connection in `~/.snowflake/config.toml`. The examples use the connection name `default` unless overridden:

```bash
SNOW_CONNECTION=my_connection just setup
```

All `just` commands accept this override.

### Snowflake account

The `setup` command creates the following objects:

- Database `PY_LOGS_DEMO` with schema `PUBLIC`
- Internal stage `CODE_STAGE`
- Compute pool `PY_LOGS_DEMO_POOL` (`CPU_X64_XS`, 1-3 nodes)
- Event table `PY_LOGS_DEMO.PUBLIC.EVENTS`

The setup script works as-is when run with the `ACCOUNTADMIN` role. If a different role is used, `ACCOUNTADMIN` should create the compute pool separately and grant usage to that role.

## Setup

```bash
cd examples/

# Install Python dependencies
uv sync

# Create Snowflake objects
just setup
```

## Running the examples

### Option 1: Stored Procedures

```bash
just deploy-proc     # upload handler to stage, create procedure
just run-proc        # call the procedure
```

### Option 2: Notebooks

```bash
just deploy-notebook # upload notebook to stage, create notebook object
just run-notebook    # execute the notebook non-interactively
```

### Option 3: ML Jobs

```bash
just run-job         # submit the job and stream logs until completion
```

No deploy step is needed: the `@remote` decorator packages and submits the function automatically.

### Error scenarios

Each option has commands that trigger a SQL error and a Python error (original state is recovered at the end):

```bash
just run-proc-broken-sql        # column count mismatch
just run-proc-broken-python     # non-existent key column

just run-notebook-broken-sql
just run-notebook-broken-python

just run-job-broken-sql
just run-job-broken-python
```

## Cleanup

To remove all created objects after testing:

```sql
-- Stop the compute pool first (will fail if jobs are still running)
ALTER COMPUTE POOL PY_LOGS_DEMO_POOL STOP ALL;
ALTER COMPUTE POOL PY_LOGS_DEMO_POOL SUSPEND;

DROP DATABASE IF EXISTS PY_LOGS_DEMO;
DROP COMPUTE POOL IF EXISTS PY_LOGS_DEMO_POOL;
```

`DROP DATABASE` cascades and removes the schema, stage, event table, procedure, notebook, and any tables created during the runs.
