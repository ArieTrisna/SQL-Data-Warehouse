/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
select
          cst_id,
          count(*) 
from      silver.crm_cust_info
group by  cst_id
having    count(*) > 1 or cst_id is null;

-- Check for Unwanted Spaces
-- Expectation: No Results
select 
      cst_key 
from  silver.crm_cust_info
where cst_key != TRIM(cst_key);

-- Data Standardization & Consistency
select distinct
      cst_marital_status 
from  silver.crm_cust_info;

-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
select 
          prd_id,
          count(*) 
from      silver.crm_prd_info
group by  prd_id
having    count(*) > 1 or prd_id is null;

-- Check for Unwanted Spaces
-- Expectation: No Results
select 
      prd_nm 
from  silver.crm_prd_info
where prd_nm != TRIM(prd_nm);

-- Check for NULLs or Negative Values in Cost
-- Expectation: No Results
select 
      prd_cost 
from  silver.crm_prd_info
where prd_cost < 0 or prd_cost is null;

-- Data Standardization & Consistency
select distinct 
      prd_line 
from  silver.crm_prd_info;

-- Check for Invalid Date Orders (Start Date > End Date)
-- Expectation: No Results
select 
      * 
from  silver.crm_prd_info
where prd_end_dt < prd_start_dt;

-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================
-- Check for Invalid Dates
-- Expectation: No Invalid Dates
select 
      NULLIF(sls_due_dt, 0) as sls_due_dt 
from  bronze.crm_sales_details
where sls_due_dt <= 0 
    or LEN(sls_due_dt) != 8 
    or sls_due_dt > 20500101 
    or sls_due_dt < 19000101;

-- Check for Invalid Date Orders (Order Date > Shipping/Due Dates)
-- Expectation: No Results
select 
      * 
from  silver.crm_sales_details
where sls_order_dt > sls_ship_dt 
   or sls_order_dt > sls_due_dt;

-- Check Data Consistency: Sales = Quantity * Price
-- Expectation: No Results
select distinct 
      sls_sales,
      sls_quantity,
      sls_price 
from  silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
   or sls_sales is null
   or sls_quantity is null 
   or sls_price is null
   or sls_sales <= 0 
   or sls_quantity <= 0 
   or sls_price <= 0
order by sls_sales, sls_quantity, sls_price;

-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================
-- Identify Out-of-Range Dates
-- Expectation: Birthdates between 1924-01-01 and Today
select distinct 
      bdate 
from  silver.erp_cust_az12
where bdate < '1924-01-01' 
   or bdate > GETDATE();

-- Data Standardization & Consistency
select distinct 
      gen 
from  silver.erp_cust_az12;

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================
-- Data Standardization & Consistency
select distinct
          cntry 
from      silver.erp_loc_a101
order by  cntry;

-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================
-- Check for Unwanted Spaces
-- Expectation: No Results
select 
      * 
from  silver.erp_px_cat_g1v2
where cat != TRIM(cat) 
   or subcat != TRIM(subcat) 
   or maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
select distinct 
      maintenance 
from  silver.erp_px_cat_g1v2;
