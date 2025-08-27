-- Snowflake Setup SQL Commands
-- Execute these commands in Snowflake to prepare the environment

-- 1. Create Warehouse (if not exists)
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    COMMENT = 'Warehouse for SpaceX ETL processing';

-- 2. Create database
CREATE DATABASE IF NOT EXISTS SPACEX_DB
    COMMENT = 'Database for SpaceX launch data';

-- 3. Use database
USE DATABASE SPACEX_DB;

-- 4. Create schema (optional)
CREATE SCHEMA IF NOT EXISTS SPACEX_SCHEMA
    COMMENT = 'Schema for SpaceX data';

-- 5. Use schema
USE SCHEMA SPACEX_SCHEMA;

-- 6. Create table for SpaceX launches
CREATE TABLE IF NOT EXISTS spacex_launches (
    id STRING PRIMARY KEY,
    name STRING NOT NULL,
    date_utc TIMESTAMP_NTZ NOT NULL,
    success BOOLEAN NOT NULL,
    rocket STRING NOT NULL,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 7. Create indexes for optimization (optional)
CREATE INDEX IF NOT EXISTS idx_spacex_launches_date ON spacex_launches(date_utc);
CREATE INDEX IF NOT EXISTS idx_spacex_launches_rocket ON spacex_launches(rocket);
CREATE INDEX IF NOT EXISTS idx_spacex_launches_success ON spacex_launches(success);

-- 8. Create view for successful launches
CREATE OR REPLACE VIEW successful_launches AS
SELECT 
    id,
    name,
    date_utc,
    rocket,
    created_at
FROM spacex_launches
WHERE success = TRUE
ORDER BY date_utc DESC;

-- 9. Create view for rocket statistics
CREATE OR REPLACE VIEW rocket_statistics AS
SELECT 
    rocket,
    COUNT(*) as total_launches,
    SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_launches,
    ROUND(SUM(CASE WHEN success THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate
FROM spacex_launches
GROUP BY rocket
ORDER BY total_launches DESC;

-- 10. Inspect created objects
SHOW TABLES;
SHOW VIEWS;
SHOW INDEXES;

-- 11. Check grants
SHOW GRANTS ON DATABASE SPACEX_DB;
SHOW GRANTS ON SCHEMA SPACEX_SCHEMA;
SHOW GRANTS ON TABLE spacex_launches;

-- 12. Test query
SELECT 
    'Setup completed successfully' as status,
    CURRENT_TIMESTAMP() as timestamp,
    CURRENT_USER() as current_user,
    CURRENT_ACCOUNT() as current_account;
