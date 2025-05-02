/*

Create Database and Schemas:

This scipt with create Database 'DataWarehouse' after checking if it already exists or not.
If exits it will drop the existing Database and create a new one.
After creating database it will proceed three schemas i.e., bronze, silver and gold.

**Note:

This script will drop the entire 'DataWarehouse' database if it exits.Hence proceed with caution before
executing this script.

*/

USE master;
GO

-- Drop and Recreate database DataWarehouse

IF EXISTS(SELECT 1 FROM sys.databases WHERE name='DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse
END;
GO

--Creating Database DataWarehouse
CREATE DATABASE DataWarehouse;

USE DataWarehouse;

--Creating Schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
