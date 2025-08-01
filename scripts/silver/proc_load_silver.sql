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

Database: PostgreSQL
Usage Example:
    call silver.load_silver();  <- PostgreSQL
===============================================================================
*/

create or replace procedure silver.load_silver()
language plpgsql
as $$
declare
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP := clock_timestamp();
    batch_end_time TIMESTAMP;
begin 
	
	RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '================================================';
	
	batch_start_time := clock_timestamp();

    -- CRM TABLES
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------';

    start_time := clock_timestamp();
	
    -- crm_cust_info
    RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
	truncate table silver.crm_cust_info;

    RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';
	insert into silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)
	select 
		cst_id,
		cst_key,
		trim(cst_firstname) as cst_firstname,	-- unwanted space handling
		trim(cst_lastname) as cst_lastname,		-- unwanted space handling
		case 	when upper(trim(cst_marital_status)) = 'S' then 'Single'  	-- standardization n consistency handling
			when upper(trim(cst_marital_status)) = 'M' then 'Married'	-- upper and trim to make sure there is no unwanted space
			else 'n/a'													-- and not error when found lowercase letter
		end cst_marital_status,
		case 	when upper(trim(cst_gndr)) = 'F' then 'Female'  -- standardization n consistency handling
			when upper(trim(cst_gndr)) = 'M' then 'Male'	-- upper and trim to make sure there is no unwanted space
			else 'n/a'										-- and not error when found lowercase letter
		end cst_gndr,
		cst_create_date
	from(
		select
				*,				
				row_number()over(partition by cst_id order by cst_create_date desc) as flag_last
		from	bronze.crm_cust_info
		where	cst_id is not null
		) as clean
	where flag_last = 1;

	end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECOND FROM end_time - start_time);
	
	-- crm_prd_info
	start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
	truncate table silver.crm_prd_info;
	
	RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';
	insert into silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt)
	select
		prd_id,
		replace(substring(prd_key, 1, 5), '-', '_') as cat_id,		-- relation key with px_cat_g1v2
		substring(prd_key, 7, length(prd_key)) as prd_key,			-- relation key with sales_details
		prd_nm,
		coalesce(prd_cost, 0) as prd_cost,
		case	upper(trim(prd_line)) 	
			when 'M' then 'Mountain'
			when 'R' then 'Road'
			when 'S' then 'Other Sales'
			when 'T' then 'Trail'
			else 'n/a'
		end as prd_line,
		cast(prd_start_dt as date) as prd_start_dt,
		cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt) -1 as date) as prd_end_dt
	from	bronze.crm_prd_info;

	end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECOND FROM end_time - start_time);
	
	-- crm_sales_details
	start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
	truncate table silver.crm_sales_details;
	
	RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';
	
	insert into silver.crm_sales_details(
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

	select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case 	when sls_order_dt = 0 or length(cast(sls_order_dt as varchar)) != 8 then null
			else cast(cast(sls_order_dt as varchar) as date)
		end as 	sls_order_dt,
		case 	when sls_ship_dt = 0 or length(cast(sls_ship_dt as varchar)) != 8 then null
			else cast(cast(sls_ship_dt as varchar) as date)
		end as 	sls_ship_dt,
		case 	when sls_due_dt = 0 or length(cast(sls_due_dt as varchar)) != 8 then null
			else cast(cast(sls_due_dt as varchar) as date)
		end as 	sls_due_dt,
		case 	when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
			then sls_quantity * abs(sls_price)
			else sls_sales
		end as 	sls_sales,
		sls_quantity,
		case 	when sls_price is null or sls_price <= 0 then sls_sales / sls_quantity
			else sls_price
		end as 	sls_price
	from	bronze.crm_sales_details;
	
	end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECOND FROM end_time - start_time);
	
	-- ERP TABLES
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '------------------------------------------------';
	
	-- erp_cust_az12
	start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
	truncate table silver.erp_cust_az12;
	
	RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_az12';
	insert into silver.erp_cust_az12(cid, bdate, gen)
	select 
		case	when cid like 'NAS%' then substring(cid, 4, length(cid))
			else cid
		end as	cid,
		case	when bdate > current_date then null
			else bdate
		end as	bdate,
		case	when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
			when upper(trim(gen)) in ('M', 'MALE') then 'Male'
			else 'n/a'
		end as	gen
	from 	bronze.erp_cust_az12;
	
	end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECOND FROM end_time - start_time);
	
	-- erp_loc_a101
	start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
	truncate table silver.erp_loc_a101;
	
	RAISE NOTICE '>> Inserting Data Into: silver.erp_loc_a101';
	insert into silver.erp_loc_a101(cid, cntry)
	select 
		replace(cid, '-', '') as cid,
		case 	when trim(cntry) = 'DE' THEN 'Germany'
			when trim(cntry) in ('US', 'USA') then 'United States'
			when trim(cntry) = '' or cntry is null then 'n/a'
			else trim(cntry)
		end as 	cntry
	from	bronze.erp_loc_a101;
	
	end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECOND FROM end_time - start_time);
	
	-- erp_px_cat_g1v2
	start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
	truncate table silver.erp_px_cat_g1v2;

	RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_g1v2';
	insert into silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance)
	select 
		id,
		cat,
		subcat,
		maintenance
	from 	bronze.erp_px_cat_g1v2;
	
	end_time := clock_timestamp();
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECOND FROM end_time - start_time);
	
	batch_end_time := clock_timestamp();
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Silver Layer is Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds',
        ROUND(EXTRACT(EPOCH FROM (batch_end_time - batch_start_time)));
    RAISE NOTICE '==========================================';

	EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING LOADING SILVER LAYER';
        RAISE NOTICE 'Error Message: %', SQLERRM;
		RAISE NOTICE 'Error Code: %', SQLSTATE;
        RAISE NOTICE '==========================================';
end;
$$;
