/*
    =====================================================
    Create Database and Schemas
    =====================================================
    Script Purpose:
        This script is used to create a new database called DataWarehouse
        and it checks if it existed, if it does, it drops it then gets recreated.
        This also creates 3 schemas bronze, silver, gold.

    WARNING:
        This script drops DataWarehouse DB if it exists.
        Proceed with caution.
*/
USE master;
Go
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;

Go
--Create DataWarehouse DB
CREATE DATABASE DataWarehouse;
Go
USE DataWarehouse;
Go
--Creating Schemas
CREATE SCHEMA bronze;
Go
CREATE SCHEMA silver;
Go
CREATE SCHEMA gold;
Go
