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

Usage Example:
    EXEC bronze.load_bronze;
=====
create or alter procedure bronze.load_bronze
as
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try

		print '===========================================================================================';
		print 'Loading bronze layes...';
		print '===========================================================================================';

		print '-------------------------------------------------------------------------------------------';
		print 'Loading CRM tables.';
		print '-------------------------------------------------------------------------------------------';



		--c1
		set @start_time = GETDATE();
		print '>> Truncating Table: bronze.crm_cust_info';
		
		truncate table bronze.crm_cust_info;

		print '>> Inserting Data Into: bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'D:\DataWarehouse Project Resume\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		--select *from bronze.crm_cust_info;

		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds';
		print '--------------------------------------------------------------';




		--c2
		set @start_time = GETDATE();
		print '>> Truncating Table: bronze.crm_prd_info';
		
		truncate table bronze.crm_prd_info;
		print '>> Inserting Data Into: bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'D:\DataWarehouse Project Resume\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		--select *from bronze.crm_prd_info;

		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds';
		print '--------------------------------------------------------------';



		--c3
		set @start_time = GETDATE();
		print '>> Truncating Table: bronze.crm_sales_details';
		
		truncate table bronze.crm_sales_details;
		print '>> Inserting Data Into: bronze. crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'D:\DataWarehouse Project Resume\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		--select *from bronze.crm_sales_details;

		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds';
		print '--------------------------------------------------------------';



		print '-------------------------------------------------------------------------------------------';
		print 'Loading ERP tables.';
		print '-------------------------------------------------------------------------------------------';



		--e1
		set @start_time = GETDATE();
		print '>> Truncating Table: bronze. erp_loc_a101';
		
		truncate table bronze.erp_loc_a101;
		print '>> Inserting Data Into: bronze. erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'D:\DataWarehouse Project Resume\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		--select *from bronze.erp_loc_a101;

		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds';
		print '--------------------------------------------------------------';




		--e2
		set @start_time = GETDATE();
		print '>> Truncating Table: bronze.erp_cust_az12';
		
		truncate table bronze.erp_cust_az12;
		print '>> Inserting Data Into: bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from 'D:\DataWarehouse Project Resume\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		--select *from bronze.erp_cust_az12;

		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds';
		print '--------------------------------------------------------------';



		--e3
		set @start_time = GETDATE();
		print '>> Truncating Table: bronze.erp_px_cat_g1v2';
		
		truncate table bronze.erp_px_cat_g1v2;
		print '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2
		from 'D:\DataWarehouse Project Resume\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		--select *from bronze. erp_px_cat_g1v2;

		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds';
		print '--------------------------------------------------------------';

		set @batch_end_time = GETDATE();
		print '==================================================='
		print 'Loading bronze layer is completed';
		print '		-Total Load Duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' Seconds';
		
	end try

	begin catch
		print '======================================================================================';
		print 'Error occured during loading bronze layer';
		print 'Error message' + error_message();
		print 'Error message' + cast(error_number() as nvarchar);
		print 'Error message' + cast(error_state() as nvarchar);
		print '======================================================================================';

	end catch

end  
-- end of the procedure for bronze layes

exec bronze.load_bronze


drop procedure bronze.load_bronze








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


create or alter procedure silver.load_silver
as
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try

		print '===========================================================================================';
		print 'Loading silver layes...';
		print '===========================================================================================';

		print '-------------------------------------------------------------------------------------------';
		print 'Loading CRM tables.';
		print '-------------------------------------------------------------------------------------------';



		--c1
		set @start_time = GETDATE();
		print '>> Truncating Table: silver.crm_cust_info';
		
		truncate table silver.crm_cust_info;

		print '>> Inserting Data Into: silver.crm_cust_info';
		bulk insert silver.crm_cust_info
		from 'D:\DataWarehouse Project Resume\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		--select *from silver.crm_cust_info;

		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds';
		print '--------------------------------------------------------------';




		--c2
		set @start_time = GETDATE();
		print '>> Truncating Table: silver.crm_prd_info';
		
		truncate table silver.crm_prd_info;
		print '>> Inserting Data Into: silver.crm_prd_info';
		bulk insert silver.crm_prd_info
		from 'D:\DataWarehouse Project Resume\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		--select *from silver.crm_prd_info;

		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds';
		print '--------------------------------------------------------------';



		--c3
		set @start_time = GETDATE();
		print '>> Truncating Table: silver.crm_sales_details';
		
		truncate table silver.crm_sales_details;
		print '>> Inserting Data Into: silver. crm_sales_details';
		bulk insert silver.crm_sales_details
		from 'D:\DataWarehouse Project Resume\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		--select *from silver.crm_sales_details;

		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds';
		print '--------------------------------------------------------------';



		print '-------------------------------------------------------------------------------------------';
		print 'Loading ERP tables.';
		print '-------------------------------------------------------------------------------------------';



		--e1
		set @start_time = GETDATE();
		print '>> Truncating Table: silver. erp_loc_a101';
		
		truncate table silver.erp_loc_a101;
		print '>> Inserting Data Into: silver. erp_loc_a101';
		bulk insert silver.erp_loc_a101
		from 'D:\DataWarehouse Project Resume\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		--select *from silver.erp_loc_a101;

		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds';
		print '--------------------------------------------------------------';




		--e2
		set @start_time = GETDATE();
		print '>> Truncating Table: silver.erp_cust_az12';
		
		truncate table silver.erp_cust_az12;
		print '>> Inserting Data Into: silver.erp_cust_az12';
		bulk insert silver.erp_cust_az12
		from 'D:\DataWarehouse Project Resume\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		--select *from silver.erp_cust_az12;

		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds';
		print '--------------------------------------------------------------';



		--e3
		set @start_time = GETDATE();
		print '>> Truncating Table: silver.erp_px_cat_g1v2';
		
		truncate table silver.erp_px_cat_g1v2;
		print '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		bulk insert silver.erp_px_cat_g1v2
		from 'D:\DataWarehouse Project Resume\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		--select *from silver. erp_px_cat_g1v2;

		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds';
		print '--------------------------------------------------------------';

		set @batch_end_time = GETDATE();
		print '==================================================='
		print 'Loading silver layer is completed';
		print '		-Total Load Duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' Seconds';
		
	end try

	begin catch
		print '======================================================================================';
		print 'Error occured during loading bronze layer';
		print 'Error message' + error_message();
		print 'Error message' + cast(error_number() as nvarchar);
		print 'Error message' + cast(error_state() as nvarchar);
		print '======================================================================================';

	end catch

end  
-- end of the procedure for silver layes

exec silver.load_silver


drop procedure silver.load_silver
