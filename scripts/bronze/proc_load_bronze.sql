/*

=====================================================
Stored Procedure : Load Bronze Layer
=====================================================
Purpose : 
This stored procedure is used to load data into tables in bronze layers.
Before loading data it will truncate the existing data in tables.
Uses 'BULK INSERT' to load the data from CSV files.

Usage Example:
EXEC bronze.load_bronze

*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '===============================================';
		PRINT 'Loading Bronze Layer';
		PRINT '===============================================';
		
		PRINT '-----------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting Data into Table: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'F:\DSML RELATED\SQL Projects\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH
		(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+' seconds';
		PRINT '--------------------';
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting Data into Table: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'F:\DSML RELATED\SQL Projects\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH
		(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+' seconds';
		PRINT '--------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Data into Table: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'F:\DSML RELATED\SQL Projects\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH
		(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+' seconds';
		PRINT '--------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Data into Table: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'F:\DSML RELATED\SQL Projects\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH
		(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+' seconds';
		PRINT '--------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Data into Table: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'F:\DSML RELATED\SQL Projects\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH
		(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+' seconds';
		PRINT '--------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Data into Table: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'F:\DSML RELATED\SQL Projects\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH
		(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR);
		PRINT '--------------------';
		SET @batch_end_time = GETDATE();
		PRINT '***********************************************';
		PRINT 'Load Duration for bronze Layer: '+ CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR)+' seconds';
		PRINT '***********************************************';
	END TRY
	BEGIN CATCH
		PRINT 'Error has occurred while creating bronze layer';
		PRINT 'Error Message' + ERROR_MESSAGE();
	END CATCH
END
