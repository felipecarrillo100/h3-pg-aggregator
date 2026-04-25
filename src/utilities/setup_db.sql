-- H3 Image Digitalizer Database Setup
-- Run this as postgres user: psql -U postgres -f src/setup_db.sql

-- 1. Create the h3expert user if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = 'h3expert') THEN
        CREATE USER h3expert WITH PASSWORD 'h3password';
    END IF;
END
$$;

-- 2. Create the h3dbtest database owned by h3expert
-- Note: If running this via psql, ensure you are NOT inside a transaction.
SELECT 'CREATE DATABASE h3dbtest OWNER h3expert'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'h3dbtest')\gexec

-- 3. Connect to the new database and enable PostGIS
\c h3dbtest

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- 4. Grant privileges
GRANT ALL PRIVILEGES ON DATABASE h3dbtest TO h3expert;
GRANT ALL ON SCHEMA public TO h3expert;
