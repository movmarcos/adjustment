-- =============================================================================
-- 01_SETUP_DATABASE.SQL
-- Creates the database, schemas, warehouses, and stages for the Adjustment Engine
-- =============================================================================

-- ─── Database ────────────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS ADJUSTMENT_DB
  DATA_RETENTION_TIME_IN_DAYS = 90
  COMMENT = 'Fact Table Adjustment Engine — stores adjustment metadata, deltas, and materialized adjusted views';

USE DATABASE ADJUSTMENT_DB;

-- ─── Schemas ─────────────────────────────────────────────────────────────────
-- Core schema: adjustment metadata and line items
CREATE SCHEMA IF NOT EXISTS CORE
  WITH MANAGED ACCESS
  COMMENT = 'Core adjustment tables (headers, line items, audit)';

-- Fact schema: original fact data (source of truth, read-only for this app)
CREATE SCHEMA IF NOT EXISTS FACT
  WITH MANAGED ACCESS
  COMMENT = 'Immutable source fact tables';

-- Mart schema: dynamic tables that auto-materialize adjusted views
CREATE SCHEMA IF NOT EXISTS MART
  WITH MANAGED ACCESS
  COMMENT = 'Dynamic tables: auto-materialized adjusted fact views';

-- App schema: Streamlit app, stages, UDFs
CREATE SCHEMA IF NOT EXISTS APP
  WITH MANAGED ACCESS
  COMMENT = 'Streamlit app objects, stages, and helper UDFs';

-- AI schema: Cortex models, search services, semantic models
CREATE SCHEMA IF NOT EXISTS AI
  WITH MANAGED ACCESS
  COMMENT = 'Cortex AI integration objects';

-- ─── Warehouses ──────────────────────────────────────────────────────────────
-- Interactive warehouse for Streamlit queries (auto-suspend aggressive)
CREATE WAREHOUSE IF NOT EXISTS ADJUSTMENT_WH
  WAREHOUSE_SIZE   = 'X-SMALL'
  AUTO_SUSPEND     = 60
  AUTO_RESUME      = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Interactive warehouse for Streamlit and ad-hoc adjustment queries';

-- Background warehouse for tasks, dynamic table refresh
CREATE WAREHOUSE IF NOT EXISTS ADJUSTMENT_TASK_WH
  WAREHOUSE_SIZE   = 'X-SMALL'
  AUTO_SUSPEND     = 60
  AUTO_RESUME      = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Background warehouse for tasks and dynamic table maintenance';

-- ─── Stages ──────────────────────────────────────────────────────────────────
USE SCHEMA APP;

-- Internal stage for Streamlit app files
CREATE STAGE IF NOT EXISTS STREAMLIT_STAGE
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'Streamlit in Snowflake app files';

-- Internal stage for Cortex semantic model YAML
CREATE STAGE IF NOT EXISTS AI_STAGE
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'Cortex AI model definitions and semantic YAML';

-- ─── File Formats ────────────────────────────────────────────────────────────
CREATE FILE FORMAT IF NOT EXISTS CSV_FORMAT
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null', '');

CREATE FILE FORMAT IF NOT EXISTS JSON_FORMAT
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE;

-- ─── Notification Integration (Email/Slack alerts) ──────────────────────────
-- Uncomment and configure for your environment:
--
-- CREATE NOTIFICATION INTEGRATION IF NOT EXISTS ADJ_EMAIL_NOTIFICATION
--   TYPE = EMAIL
--   ENABLED = TRUE
--   ALLOWED_RECIPIENTS = ('team@company.com');
--
-- CREATE NOTIFICATION INTEGRATION IF NOT EXISTS ADJ_SLACK_NOTIFICATION
--   TYPE = QUEUE
--   ENABLED = TRUE
--   DIRECTION = OUTBOUND
--   ... (Slack webhook config);

-- ─── Verify Setup ────────────────────────────────────────────────────────────
SHOW SCHEMAS IN DATABASE ADJUSTMENT_DB;
SHOW WAREHOUSES LIKE 'ADJUSTMENT%';
