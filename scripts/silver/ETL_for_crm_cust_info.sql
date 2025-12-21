/*
===============================================================================
Sql script: Load Data to crm_cust_info (Bronze -> Silver)
===============================================================================
Script Purpose:
    This sql script performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema table from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver table.
		- Inserts transformed and cleansed data from Bronze into Silver table.
===============================================================================
*/

DECLARE @start_time DATETIME, @end_time DATETIME; 
BEGIN TRY
  
	PRINT '------------------------------------------------';
	PRINT 'Loading crm_cust_info Table';
	PRINT '------------------------------------------------';

	-- Loading silver.crm_cust_info
  SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT '>> Inserting Data Into: silver.crm_cust_info';
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
    
		CASE UPPER(TRIM(cst_marital_status))
			WHEN 'S' THEN 'Single'
			WHEN 'M' THEN 'Married'
			ELSE 'n/a'
		END AS cst_marital_status, -- Normalize marital status values to readable format
    
		CASE UPPER(TRIM(cst_gndr))
			WHEN 'F' THEN 'Female'
			WHEN 'M' THEN 'Male'
			ELSE 'n/a'
		END AS cst_gndr, -- Normalize gender values to readable format
		cst_create_date
    
	FROM (
		SELECT
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
	) t
	WHERE flag_last = 1; -- Select the most recent record per customer

	SET @end_time = GETDATE();

  PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

END TRY
BEGIN CATCH
	PRINT '=========================================='
	PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
	PRINT 'Error Message' + ERROR_MESSAGE();
	PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
	PRINT '=========================================='
END CATCH
