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

/*
		Table: crm_cust_info
*/


--Check for nulls or duplicates in primary key
--Expectation : no result

select 
cst_id,
COUNT(*)
from silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null


-- Check for unwanted spaces
-- Expectation : no result

select cst_firstname from silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname)

select cst_lastname from silver.crm_cust_info
where cst_lastname != TRIM(cst_lastname)

select cst_gndr from silver.crm_cust_info
where cst_gndr != TRIM(cst_gndr)

select cst_key from silver.crm_cust_info
where cst_key != TRIM(cst_key)



--Data standardization and consistency

select distinct cst_gndr
from silver.crm_cust_info

select distinct cst_marital_status
from silver.crm_cust_info




--Check quality of silver layer:

--Check for nulls or duplicates in primary key
--Expectation : no result

select 
cst_id,
COUNT(*)
from silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null


-- Check for unwanted spaces
-- Expectation : no result

select cst_firstname from silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname)


select cst_lastname from silver.crm_cust_info
where cst_lastname != TRIM(cst_lastname)

select cst_gndr from silver.crm_cust_info
where cst_gndr != TRIM(cst_gndr)

select cst_key from silver.crm_cust_info
where cst_key != TRIM(cst_key)



--Data standardization and consistency

select distinct cst_gndr
from silver.crm_cust_info

select distinct cst_marital_status
from silver.crm_cust_info






/*
		Table: crm_cust_info
*/


select
prd_id,
count(*)
from bronze.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null


--ckeck for unwanted spaces
--Ecpectation : no result

select prd_nm from bronze.crm_prd_info
where prd_nm != TRIM(prd_nm)



--ckeck for nulls or negative numbers
--Ecpectation : no result

select prd_cost
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null


-- Data Standardization & Consistency
select DISTINCT prd_line
from bronze.crm_prd_info

-- Check for Invalid Date Orders
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt



--Check quality of silver layer:

--Check for nulls or duplicates in primary key
--Expectation : no result

select 
prd_id,
COUNT(*)
from silver.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null


-- Check for unwanted spaces
-- Expectation : no result

select prd_nm from silver.crm_prd_info
where prd_nm != TRIM(prd_nm)

--ckeck for nulls or negative numbers
--Ecpectation : no result

select prd_cost
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null

-- Data Standardization & Consistency
select DISTINCT prd_line
from silver.crm_prd_info

-- Check for Invalid Date Orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt


select *from silver.crm_prd_info






/*
		Table: crm_sales_details
*/


select *from silver.crm_sales_details

--checking for invalid dates
SELECT
NULLIF(sls_ship_dt, 0) sls_ship_dt
FROM bronze. crm_sales_details
WHERE sls_ship_dt < = 0
OR LEN(sls_ship_dt) != 8
OR sls_ship_dt > 20500101
OR sls_ship_dt < 19000101

SELECT
NULLIF(sls_due_dt, 0) sls_due_dt
FROM bronze. crm_sales_details
WHERE sls_due_dt < = 0
OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101


-- check for invalidation date orders

select *from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt


--check data consistency between sales, quantity, and price
-- >> sales = quantity * price
-- >> values must not be null, zero, or negative.
-- >> if sales is negative, zero, or null, derived it using quantity and price
-- >> if price is zero or null, calculate it using sales and quantity
-- >> if price is negative, convert it to positive value

select distinct 
sls_sales as old_sls_sales,
sls_quantity,
sls_price as old_sls_price,

case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
		then sls_quantity * abs(sls_price)
	else sls_sales
end as sls_sales,

case when sls_price is null or sls_price <=0
		then sls_sales / nullif(sls_quantity, 0)
	else sls_price
end as sls_price

from bronze.crm_sales_details
where sls_sales != sls_quantity *sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales<=0 or sls_quantity<=0 or sls_price <= 0
order by sls_sales, sls_quantity, sls_price



--Check quality of silver layer:


-- check for invalidation date orders

select *from silver.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

--check data consistency between sales, quantity, and price
-- >> sales = quantity * price
-- >> values must not be null, zero, or negative.
-- >> if sales is negative, zero, or null, derived it using quantity and price
-- >> if price is zero or null, calculate it using sales and quantity
-- >> if price is negative, convert it to positive value

select distinct 
sls_sales,
sls_quantity,
sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity *sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales<=0 or sls_quantity<=0 or sls_price <= 0
order by sls_sales, sls_quantity, sls_price





/*
	Table : erp_cust_az12
*/


--Identify out of range dates

select distinct bdate
from bronze.erp_cust_az12
where bdate < '1924-01-01' or bdate > GETDATE()

--Data standardization and consistency
select distinct 
gen,
case when UPPER(TRIM(gen)) in ('F', 'FEMALE') then 'Female'
	 when UPPER(TRIM(gen)) in ('M', 'MALE') then 'Male'
	 else 'N/A'
end as gen
from bronze.erp_cust_az12



--Check quality of silver layer:

--Identify out of range dates

select distinct bdate
from silver.erp_cust_az12
where bdate < '1924-01-01' or bdate > GETDATE()

--Data standardization and consistency
select distinct 
gen
from silver.erp_cust_az12

select *from silver.erp_cust_az12




/*
	Table : erp_loc_101
*/

-- data standadization and consistency
select distinct cntry
from bronze.erp_loc_a101
order by cntry



--Check quality of silver layer:

-- data standadization and consistency
select distinct cntry
from silver.erp_loc_a101
order by cntry




/*
	Table : erp_loc_101
*/

-- check for unwanted spaces
select *from bronze.erp_px_cat_g1v2
where cat != TRIM(cat) or subcat != TRIM(subcat) or maintenance != TRIM(maintenance)


-- data standadization and consistency
select distinct
cat
from bronze.erp_px_cat_g1v2

select distinct
subcat
from bronze.erp_px_cat_g1v2

select distinct
maintenance
from bronze.erp_px_cat_g1v2




