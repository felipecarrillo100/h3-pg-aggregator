@echo off
SET PGPASSWORD=postgres
SET PGUSER=postgres
SET PGHOST=localhost
SET PGPORT=5432

echo --- H3 Database Setup Started ---

echo 1. Creating h3expert user...
psql -c "DO $do$ BEGIN IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = 'h3expert') THEN CREATE USER h3expert WITH PASSWORD 'h3password'; END IF; END $do$;"

echo 2. Creating h3dbtest database...
psql -c "SELECT 1 FROM pg_database WHERE datname = 'h3dbtest'" | findstr /C:"1" > nul
if %errorlevel% neq 0 (
    psql -c "CREATE DATABASE h3dbtest OWNER h3expert;"
) else (
    echo Database h3dbtest already exists.
)

echo 3. Enabling PostGIS...
psql -d h3dbtest -c "CREATE EXTENSION IF NOT EXISTS postgis;"
psql -d h3dbtest -c "CREATE EXTENSION IF NOT EXISTS postgis_topology;"

echo 4. Granting Privileges...
psql -d h3dbtest -c "GRANT ALL PRIVILEGES ON DATABASE h3dbtest TO h3expert;"
psql -d h3dbtest -c "GRANT ALL ON SCHEMA public TO h3expert;"

echo --- Setup Complete! ---
echo You can now connect as h3expert:
echo psql -U h3expert -d h3dbtest
pause
