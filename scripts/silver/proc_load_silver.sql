/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @start_batch_time DATETIME, @end_batch_time DATETIME;
	BEGIN TRY
		SET @start_batch_time = GETDATE();
		PRINT '<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>';
		PRINT 'Executing Silver Layer';
		PRINT '<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>';
		SET @start_time = GETDATE();
		PRINT '==============================================';
		PRINT '>> Truncating table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '------------------------------------------';
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		PRINT '------------------------------------------';
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)

		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE
				WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
				WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
				ELSE 'Unknown'
			END AS cst_marital_status,
			CASE
				WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
				ELSE 'Unknown'
			END AS cst_gender,
			cst_create_date
			FROM (
				SELECT 
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
				FROM bronze.crm_cust_info
				WHERE cst_id IS NOT NULL
		)t WHERE flag_last = 1;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + 'seconds';
		PRINT '==============================================';


		SET @start_time = GETDATE();
		PRINT '==============================================';
		PRINT '>> Truncating table: crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '------------------------------------------';
		PRINT '>> Inserting Data Into: crm_prd_info';
		PRINT '------------------------------------------';
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
			prd_nm,
			ISNULL (prd_cost, 0) AS prd_cost,
			CASE
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'Unknown'
			END AS prd_line,
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(LEAD (prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + 'seconds';
		PRINT '==============================================';


		SET @start_time = GETDATE();
		PRINT '==============================================';
		PRINT '>> Truncating table: crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '------------------------------------------';
		PRINT '>> Inserting Data Into: crm_sales_details';
		PRINT '------------------------------------------';
		INSERT INTO silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)

		SELECT
			sls_ord_num,
			sls_ord_key,
			sls_cust_id,

			CASE
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,

			CASE
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,

			CASE
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,

			CASE
				WHEN sls_sales IS NULL or sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,

			sls_quantity,

			CASE
				WHEN sls_price IS NULL or sls_price <= 0
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price
			END AS sls_price

		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + 'seconds';
		PRINT '==============================================';


		SET @start_time = GETDATE();
		PRINT '==============================================';
		PRINT '>> Truncating table: erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '------------------------------------------';
		PRINT '>> Inserting Data Into: erp_cust_az12';
		PRINT '------------------------------------------';
		INSERT INTO silver.erp_cust_az12 (
		cid, 
		bdate, 
		gen
		)

		SELECT 
		CASE
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
			ELSE cid
		END AS cid,
		CASE
			WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
		END AS bdate,
		CASE
			WHEN gen IS NULL THEN 'n/a'
			WHEN gen = ' ' THEN 'n/a'
			WHEN gen = 'F' THEN 'Female'
			WHEN gen = 'M' THEN 'Male'
			ELSE gen
		END AS gen
		FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + 'seconds';
		PRINT '==============================================';


		SET @start_time = GETDATE();
		PRINT '==============================================';
		PRINT '>> Truncating table: erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '------------------------------------------';
		PRINT '>> Inserting Data Into: erp_loc_a101';
		PRINT '------------------------------------------';
		INSERT INTO silver.erp_loc_a101(
		cid,
		cntry
		)

		SELECT
		REPLACE(cid, '-', '') AS cid,
		CASE
			WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END AS cntry
		FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + 'seconds';
		PRINT '==============================================';


		SET @start_time = GETDATE();
		PRINT '==============================================';
		PRINT '>> Truncating table: erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '------------------------------------------';
		PRINT '>> Inserting Data Into: erp_px_cat_g1v2';
		PRINT '------------------------------------------';
		INSERT INTO silver.erp_px_cat_g1v2 (
		id,
		cat,
		subcat,
		maintenance
		)
		SELECT DISTINCT
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_lpx_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + 'seconds';
		PRINT '==============================================';
		
		SET @end_batch_time = GETDATE();

		PRINT '<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>';
		PRINT '>> Executing Silver Layer is Completed';
		PRINT '>> Total Load Duration: ' + CAST (DATEDIFF(SECOND, @start_batch_time, @end_batch_time) AS NVARCHAR(100)) + 'seconds';
		PRINT '<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>';
	END TRY
	BEGIN CATCH
		PRINT '==============================================';
		PRINT 'Error Occured During Loading Silver Layer';
		PRINT 'Error Message:' + ERROR_MESSAGE();
		PRINT 'Error Message:' + CAST (ERROR_NUMBER() AS NVARCHAR(50));
		PRINT 'Error Message:' + CAST (ERROR_STATE() AS NVARCHAR(50));
		PRINT '==============================================';
	END CATCH
END
