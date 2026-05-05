-- Create the database
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'h3dbtest')
BEGIN
    CREATE DATABASE h3dbtest;
END
GO

USE h3dbtest;
GO

-- Create the login
IF NOT EXISTS (SELECT name FROM sys.server_principals WHERE name = 'h3expert')
BEGIN
    CREATE LOGIN h3expert WITH PASSWORD = 'H3password!', CHECK_POLICY = OFF;
END
GO

-- Create the user
IF NOT EXISTS (SELECT name FROM sys.database_principals WHERE name = 'h3expert')
BEGIN
    CREATE USER h3expert FOR LOGIN h3expert;
    ALTER ROLE db_owner ADD MEMBER h3expert;
END
GO
