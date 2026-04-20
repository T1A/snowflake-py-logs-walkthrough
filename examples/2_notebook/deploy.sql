-- Deploy the SCD1 notebook from stage.
-- The notebook file is uploaded to the stage first (see justfile).
-- Run with: snow sql -f examples/2_notebook/deploy.sql

CREATE OR REPLACE NOTEBOOK PY_LOGS_DEMO.PUBLIC.SCD1_NOTEBOOK
    FROM '@PY_LOGS_DEMO.PUBLIC.CODE_STAGE'
    MAIN_FILE = '2_notebook/scd1_notebook.ipynb'
    QUERY_WAREHOUSE = 'COMPUTE_WH'
    COMPUTE_POOL = 'PY_LOGS_DEMO_POOL'
    RUNTIME_NAME = 'SYSTEM$BASIC_RUNTIME';

-- A live version must be added before the notebook can be executed
ALTER NOTEBOOK PY_LOGS_DEMO.PUBLIC.SCD1_NOTEBOOK ADD LIVE VERSION FROM LAST;

