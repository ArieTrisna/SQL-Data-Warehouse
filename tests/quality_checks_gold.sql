/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'gold.dim_customers'
-- ====================================================================
-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results 
select
          customer_key,
          count(*) as duplicate_count
from      gold.dim_customers
group by  customer_key
having    count(*) > 1;

-- ====================================================================
-- Checking 'gold.product_key'
-- ====================================================================
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 
select 
          product_key,
          count(*) as duplicate_count
from      gold.dim_products
group by  product_key
having    count(*) > 1;

-- ====================================================================
-- Checking 'gold.fact_sales'
-- ====================================================================
-- Check the data model connectivity between fact and dimensions
select     * 
from       gold.fact_sales f
left join  gold.dim_customers c
  on       c.customer_key = f.customer_key
left join  gold.dim_products p
  on       p.product_key = f.product_key
where      p.product_key is null or c.customer_key is null 
