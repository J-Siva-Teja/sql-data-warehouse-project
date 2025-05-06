/*
=====================================================
Stored Procedure : Load Silver Layer
=====================================================
Purpose : 
-- This stored procedure is used to load data into tables in Silver layer
-- Before loading data it will truncate the existing data in tables.
-- Uses the data available in tables created in bronze layer.
-- Before loading data into tables the data is transformed which addresses the issues like
   null values, duplicates, unwanted spaces in texts, inconsistency in data etc.,

Usage Example:
EXEC silver.load_silver

*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	PRINT '==========================================';
	PRINT 'Loading CRM Tables in Silver Schema';
	PRINT '==========================================';
	DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME, @start_time DATETIME, @end_time DATETIME;
	SET @batch_start_time = GETDATE();
	BEGIN TRY
		SET @start_time = GETDATE();
		PRINT '>> Truncating Existing crm_cust_info Table in Silver Schema';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data into Table crm_cust_info in Silver Schema';
		INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)
		SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			 ELSE 'n/a' END AS cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 ELSE 'n/a' END AS cst_gndr,
		cst_create_date
		FROM
		(SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
		) x
		WHERE x.flag_last = 1;
		SET @end_time = GETDATE();
		PRINT 'Loading Time: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+' seconds';
		PRINT '-------------------------------------';

		PRINT '>> Truncating existing crm_prd_info Table in Silver Schema';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data into Table crm_prd_info in Silver Schema';
		SET @start_time = GETDATE();
		INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt)

		SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'Other Sales'
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'T' THEN 'Touring'
			 ELSE 'n/a'
			 END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT 'Loading Time :'+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+' seconds';
		PRINT '-------------------------------------';
		
		PRINT '>> Truncating existing crm_sales_details Table in Silver Schema';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data into Table crm_sales_details in Silver Schema';
		SET @start_time = GETDATE();
		INSERT INTO silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price)

		SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN LEN(sls_order_dt) <8 THEN NULL
			 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			 END AS sls_order_dt,
		CASE WHEN LEN(sls_ship_dt) <8 THEN NULL
			 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			 END AS sls_ship_dt,
		CASE WHEN LEN(sls_due_dt) <8 THEN NULL
			 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			 END AS sls_due_dt,
		CASE WHEN (sls_sales <0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)) THEN  sls_quantity * ABS(sls_price)
			 ELSE sls_sales
			 END AS sls_sales,
		sls_quantity,
		CASE WHEN (sls_price < 0 OR sls_price IS NULL) THEN sls_sales/NULLIF(sls_quantity,0)
			 ELSE sls_price
			 END AS sls_price
		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT 'Loading Time :'+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+' seconds';
		PRINT '-------------------------------------';
		
		PRINT '>> Truncating existing erp_cust_az12 Table in Silver Schema';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data into Table erp_cust_az12 in Silver Schema';
		SET @start_time = GETDATE();
		INSERT INTO silver.erp_cust_az12 (
		cid,
		bdate,
		gen)

		SELECT 
		SUBSTRING(cid, 4, LEN(cid)) AS cid,
		CASE WHEN bdate<'1924-01-01' OR bdate> GETDATE() THEN NULL
			 ELSE bdate
			 END AS dbdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
			 ELSE 'n/a'
			 END AS gen
		FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT 'Loading Time :'+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+' seconds';
		PRINT '-------------------------------------';
		
		PRINT '>> Truncating existing erp_loc_a101 Table in Silver Schema';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data into Table erp_loc_a101 in Silver Schema';
		SET @start_time = GETDATE();
		INSERT INTO silver.erp_loc_a101 (
		cid,
		cntry)

		SELECT 
		REPLACE(cid,'-','') AS cid,
		CASE WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
			 WHEN UPPER(TRIM(cntry))='DE' THEN 'Germany'
			 WHEN cntry IS NULL OR cntry='' THEN 'n/a'
			 ELSE cntry
			 END AS cntry
		FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT 'Loading Time :'+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+' seconds';
		PRINT '-------------------------------------';
		
		PRINT '>> Truncating existing erp_px_cat_g1v2 Table in Silver Schema';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data into Table erp_px_cat_g1v2 in Silver Schema';
		SET @start_time = GETDATE();
		INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintanance)

		SELECT * FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT 'Loading Time :'+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+' seconds';
		PRINT '-------------------------------------';
		SET @batch_end_time = GETDATE();
		PRINT '==========================================';
		PRINT 'Batch Loading Time :'+ CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR)+' seconds';
		PRINT '==========================================';
	END TRY
	BEGIN CATCH
		PRINT 'Error Occurred While loading data into Silver Schema';
		PRINT 'Error Message: '+ ERROR_MESSAGE();
	END CATCH
END
