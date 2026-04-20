-- Deploy the SCD1 stored procedure.
-- The handler file is uploaded to the stage first (see justfile).
-- Run with: snow sql -f examples/1_stored_proc/deploy.sql

CREATE OR REPLACE PROCEDURE PY_LOGS_DEMO.PUBLIC.SCD1_MERGE(
    SRC_TABLE    VARCHAR,
    TGT_TABLE    VARCHAR,
    KEY_COLS     VARCHAR,   -- comma-separated column names, e.g. 'ID'
    TRACKED_COLS VARCHAR    -- comma-separated column names, e.g. 'NAME,STATUS'
)
    RETURNS STRING
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    IMPORTS = ('@PY_LOGS_DEMO.PUBLIC.CODE_STAGE/scd1_proc.py')
    HANDLER = 'scd1_proc.main'
;

ALTER PROCEDURE PY_LOGS_DEMO.PUBLIC.SCD1_MERGE(VARCHAR, VARCHAR, VARCHAR, VARCHAR)
    SET LOG_LEVEL = 'INFO';
