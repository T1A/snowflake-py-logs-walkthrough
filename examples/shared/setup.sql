-- One-time setup for all examples in this repo.
-- Run with: snow sql -f examples/shared/setup.sql

CREATE DATABASE IF NOT EXISTS PY_LOGS_DEMO;
CREATE SCHEMA IF NOT EXISTS PY_LOGS_DEMO.PUBLIC;

-- Internal stage for uploading procedure code
CREATE STAGE IF NOT EXISTS PY_LOGS_DEMO.PUBLIC.CODE_STAGE;

-- Compute pool for running notebooks on container compute
CREATE COMPUTE POOL IF NOT EXISTS PY_LOGS_DEMO_POOL
    MIN_NODES = 1
    MAX_NODES = 3
    INSTANCE_FAMILY = CPU_X64_XS;

-- Event table for capturing Python log messages
CREATE EVENT TABLE IF NOT EXISTS PY_LOGS_DEMO.PUBLIC.EVENTS;

-- Associate event table with the database so all objects inside it log here
ALTER DATABASE PY_LOGS_DEMO SET EVENT_TABLE = PY_LOGS_DEMO.PUBLIC.EVENTS;
