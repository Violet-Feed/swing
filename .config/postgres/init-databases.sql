-- Create user for Hive first
CREATE USER hive WITH PASSWORD 'hive123';

-- Create database for JuiceFS metadata
CREATE DATABASE juicefs;

-- Create database for Hive metastore with hive as owner
CREATE DATABASE metastore WITH OWNER hive;

-- Connect to metastore and grant permissions
\c metastore postgres
GRANT ALL ON SCHEMA public TO hive;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO hive;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO hive;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO hive;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO hive;

-- Connect to juicefs and grant permissions
\c juicefs postgres
GRANT ALL ON SCHEMA public TO postgres;
