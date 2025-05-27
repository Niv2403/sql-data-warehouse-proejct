/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

use master;
--Drop and recreate the data_warehouse database
if exists (select 1 from sys.databases where name = 'data_warehouse')
begin
	alter database data_warehouse set single_user with rollback immediate;
	drop database data_warehouse
end;
go


--create the 'data_warehouse' database
create database data_warehouse;
go

use data_warehouse;
go

--create schemas
Create schema bronze;
go

create schema silver;
go

create schema gold;
go



/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

if object_id('bronze.crm_cust_info','U') is not null
drop table bronze.crm_cust_info;
create table bronze.crm_cust_info(
	cst_id int,
	cst_key nvarchar(50),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_marital_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date date
);


if object_id('bronze.crm_prd_info','U') is not null
drop table bronze.crm_prd_info;
create table bronze.crm_prd_info(
	prd_id int,
	prd_key nvarchar(50),
	prd_nm nvarchar (50) ,
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_dt datetime,
	prd_end_dt datetime
) ;


if object_id('bronze. crm_sales_details','U') is not null
drop table bronze.crm_sales_details;
create table bronze.crm_sales_details( 
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt int,
	sls_ship_dt int,
	sls_due_dt int,
	sls_sales int,
	sls_quantity int,
	sls_price int	
) ;



if object_id('bronze. erp_loc_a101','U') is not null
drop table bronze.erp_loc_a101;
create table bronze.erp_loc_a101(
	cid nvarchar (50),
	cntry nvarchar (50)
);


if object_id('bronze.erp_cust_az12','U') is not null
drop table bronze.erp_cust_az12;
create table bronze.erp_cust_az12( 
	cid nvarchar(50),
	bdate date, 
	gen nvarchar(50)
);


if object_id('bronze. erp_px_cat_g1v2','U') is not null
drop table bronze.erp_px_cat_g1v2;
create table bronze.erp_px_cat_g1v2(
	id nvarchar(50),
	cat nvarchar(50), 
	subcat nvarchar(50),
	maintenance nvarchar(50)
);





/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/


if object_id('silver.crm_cust_info','U') is not null
drop table silver.crm_cust_info;
create table silver.crm_cust_info(
	cst_id int,
	cst_key nvarchar(50),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_marital_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date date,
	dwh_create_date datetime2 default getdate()
);


if object_id('silver.crm_prd_info','U') is not null
drop table silver.crm_prd_info;
create table silver.crm_prd_info(
	prd_id int,
	cat_id nvarchar(50),
	prd_key nvarchar(50),
	prd_nm nvarchar (50) ,
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_dt date,
	prd_end_dt date,
	dwh_create_date datetime2 default getdate()
) ;


if object_id('silver. crm_sales_details','U') is not null
drop table silver.crm_sales_details;
create table silver.crm_sales_details( 
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt date,
	sls_ship_dt date,
	sls_due_dt date,
	sls_sales int,
	sls_quantity int,
	sls_price int	,
	dwh_create_date datetime2 default getdate()
) ;



if object_id('silver. erp_loc_a101','U') is not null
drop table silver.erp_loc_a101;
create table silver.erp_loc_a101(
	cid nvarchar (50),
	cntry nvarchar (50),
	dwh_create_date datetime2 default getdate()
);


if object_id('silver.erp_cust_az12','U') is not null
drop table silver.erp_cust_az12;
create table silver.erp_cust_az12( 
	cid nvarchar(50),
	bdate date, 
	gen nvarchar(50),
	dwh_create_date datetime2 default getdate()
);


if object_id('silver. erp_px_cat_g1v2','U') is not null
drop table silver.erp_px_cat_g1v2;
create table silver.erp_px_cat_g1v2(
	id nvarchar(50),
	cat nvarchar(50), 
	subcat nvarchar(50),
	maintenance nvarchar(50),
	dwh_create_date datetime2 default getdate()
);

go





--Transformation :


/*
		Table: crm_cust_info
*/


--Remove duplicate data
Insert into silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
)
select 
cst_id,
cst_key,
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname,

case when upper(TRIM(cst_marital_status)) = 'S' then 'Single'
	when upper(TRIM(cst_marital_status)) = 'M' then 'Married'
	else 'N/A'
end
cst_marital_status,

case when upper(TRIM(cst_gndr)) = 'F' then 'Female'
	when upper(TRIM(cst_gndr)) = 'M' then 'Male'
	else 'N/A'
end
cst_gndr,
cst_create_date

from(
	select *,
	ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
	from bronze.crm_cust_info
	where cst_id is not null) t
	where flag_last =1




/*
		Table: crm_prd_info
*/


select *from bronze.crm_prd_info

select
prd_id,
count(*)
from bronze.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null



Insert into silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
select
prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
prd_nm,
isnull(prd_cost, 0) as prd_cost,
CASE UPPER(TRIM(prd_line))
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other Sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
END AS prd_line,
CAST (prd_start_dt AS DATE) AS pra_start_dt,
CAST (LEAD (prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
from bronze.crm_prd_info

where REPLACE(SUBSTRING(prd_key,1,5), '-', '_') not in
(select distinct id from bronze.erp_px_cat_g1v2)



select 
prd_id, 
prd_key, 
prd_nm, 
prd_start_dt, 
prd_end_dt,
LEAD (prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509' )




/*
		Table: crm_sales_details
*/


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
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
	ELSE CAST(CAST (sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
	ELSE CAST(CAST (sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,

case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
		then sls_quantity * abs(sls_price)
	else sls_sales
end as sls_sales,

sls_quantity,

case when sls_price is null or sls_price <=0
		then sls_sales / nullif(sls_quantity, 0)
	else sls_price
end as sls_price

from bronze.crm_sales_details


select *from silver.crm_sales_details 





/*
	Table : erp_cust_az12
*/

insert into silver.erp_cust_az12(
	cid,
	bdate,
	gen
)
select 
case when cid like 'NAS%' then SUBSTRING(cid, 4, len(cid))
	else cid
end as cid,
case when bdate > getdate() then null	
	else bdate
end as bdate,
case when UPPER(TRIM(gen)) in ('F', 'FEMALE') then 'Female'
	 when UPPER(TRIM(gen)) in ('M', 'MALE') then 'Male'
	 else 'N/A'
end as gen
from bronze.erp_cust_az12

            --where cid not in (select distinct cst_key from silver.crm_cust_info)




			
/*
	Table : erp_loc_101
*/


insert into silver.erp_loc_a101(
	cid,
	cntry
)
select 
replace(cid,'-', '')cid,

CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	 WHEN TRIM (cntry) IN ('US', 'USA') THEN 'United States'
	 WHEN TRIM (cntry) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM (cntry)
END AS cntry

from bronze.erp_loc_a101

select *from silver.erp_loc_a101;



			
/*
	Table : erp_loc_101
*/

insert into silver.erp_px_cat_g1v2(
	id,
	cat,
	subcat,
	maintenance
)
select 
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2



exec bronze.load_bronze
exec silver.load_silver

