/*
===============================================================================
Sql script: Load Data to erp_px_cat_g1v2 (Bronze -> Silver)
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
		PRINT 'Loading erp_px_cat_g1v2 Table';
		PRINT '------------------------------------------------';

     -- Loading erp_px_cat_g1v2
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

			
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
