/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.
Database: PostgreSQL

Usage Example:
    call bronze.load_bronze(); <- PostgreSQL
===============================================================================
*/

create or replace procedure bronze.load_bronze()
language plpgsql
as $$
declare
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP := clock_timestamp();
    batch_end_time TIMESTAMP;
begin 
	
	RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '================================================';
	
	batch_start_time := clock_timestamp();

    -- CRM TABLES
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------';
	
	-- crm_cust_info
	
 	start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
	truncate table bronze.crm_cust_info;

    RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
	copy bronze.crm_cust_info
	from '/Volumes/Datas/Data Engineering/SQL Data Warehouse by Baraa/datasets/source_crm/cust_info.csv'
	delimiter ','
	csv header;
	
	end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECOND FROM end_time - start_time);
	
	-- crm_prd_info
	start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
	truncate table bronze.crm_prd_info;
	
	RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
	copy bronze.crm_prd_info
	from '/Volumes/Datas/Data Engineering/SQL Data Warehouse by Baraa/datasets/source_crm/prd_info.csv'
	delimiter ','
	csv header;
	
	end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECOND FROM end_time - start_time);
	
	-- crm_sales_details
	
	start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
	truncate table bronze.crm_sales_details;
	
	RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
	copy bronze.crm_sales_details
	from '/Volumes/Datas/Data Engineering/SQL Data Warehouse by Baraa/datasets/source_crm/sales_details.csv'
	delimiter ','
	csv header;
	
	end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECOND FROM end_time - start_time);
	
	-- ERP TABLES
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '------------------------------------------------';

	-- erp_cust_az12
	
	start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
	truncate table bronze.erp_cust_az12;
	
	RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
	copy bronze.erp_cust_az12
	from '/Volumes/Datas/Data Engineering/SQL Data Warehouse by Baraa/datasets/source_erp/CUST_AZ12.csv'
	delimiter ','
	csv header;
	
	end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECOND FROM end_time - start_time);
	
	-- erp_loc_a101
	
	start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
	truncate table bronze.erp_loc_a101;
	
	RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
	copy bronze.erp_loc_a101
	from '/Volumes/Datas/Data Engineering/SQL Data Warehouse by Baraa/datasets/source_erp/LOC_A101.csv'
	delimiter ','
	csv header;
	
	end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECOND FROM end_time - start_time);
	
	-- erp_px_cat_g1v2
	
	start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
	truncate table bronze.erp_px_cat_g1v2;

	RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
	copy bronze.erp_px_cat_g1v2
	from '/Volumes/Datas/Data Engineering/SQL Data Warehouse by Baraa/datasets/source_erp/PX_CAT_G1V2.csv'
	delimiter ','
	csv header;
	
	end_time := clock_timestamp();
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECOND FROM end_time - start_time);

	
	batch_end_time := clock_timestamp();
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Bronze Layer is Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds',
        ROUND(EXTRACT(EPOCH FROM (batch_end_time - batch_start_time)));
    RAISE NOTICE '==========================================';

	EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        RAISE NOTICE 'Error Message: %', SQLERRM;
		RAISE NOTICE 'Error Code: %', SQLSTATE;
        RAISE NOTICE '==========================================';
end;
$$;
