print'=======================================================';
/*
Bronze Layer - Raw Data Load
- Create tables for CRM, ERP, and Sales datasets
- Load CSV data into Bronze tables using BULK INSERT
- No transformations yet; this is raw data storage
- Includes logging of start/end time for each table load
*/
print'=======================================================';




CREATE TABLE bronze.crm_cust_info(
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname  NVARCHAR(50)
cst_marital_status NVARCHAR(50)
cst_gndr NVARCHAR(50)
cst_create_date DATE
);


CREATE TABLE bronze.crm_prd_info(
prd_id INT,
prd_key NVARCHAR(50),
prd_nm  NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME
);


CREATE TABLE bronze.crm_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);


IF OBJECT ID('bronze.erp_loc_a101','U')is not null
DROP table bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
cid  NVARCHAR(50),
cntry NVARCHAR(50)
);

IF OBJECT ID('bronze.erp_cust_az12','U')is not null
DROP table bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
cid NVARCHAR(50),
bdate DATE,
gen NVARCHAR(50)
);


IF OBJECT ID('bronze.erp_px_cat_giv2','U')is not null
DROP table bronze.erp_px_cat_giv2;
CREATE TABLE bronze.erp_px_cat_giv2(
id  NVARCHAR(50),
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintenance NVARCHAR(50)
);


CREATE OR ALTER PROCEDURE bronze.load_bronze as
BEGIN
  DECLARE @start_time DATETIME, @end_time DATETIME,@Batch_start_time DATETIME,@Batch_end_time DATETIME;
BEGIN TRY
SET @batch_start_time=GETDATE();
PRINT '=========================================';
PRINT 'Loading Broneze Layer';
PRINT '  - Total Load Duration '+CAST (DATEDIFF(SECOND , @batch_start_time,@batch_end_time)as NVARCHAR)+' SECONDS';
PRINT '=========================================';


PRINT '=======================================';
PRINT 'Loading Bronze Layer';
PRINT '=======================================';

PRINT '---------------------------------------';
PRINT 'Loading CRM Tables';
PRINT '---------------------------------------';

SET @start_time=GETDATE();
PRINT '>> Truncating Table:bronze.crm_cust_info';
TRUNCATE TABLE bronze.crm_cust_info;

PRINT'>>Inserting Data Into:bronze.crm_cust_info';
BULK INSERT bronze.crm_cust_info
FROM 'C:\Users\subal\Desktop\DT_W_PROJECT\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH(
FIRSTROW=2,
FIELDTERMINATOR=',',
TABLOCK
);
SET @end_time=GETDATE();
PRINT '>>lOAD dURATION: '+CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR)+' SECONDS';
PRINT '-------------------------------------'


SET @start_time=GETDATE();
PRINT '>> Truncating Table:bronze.crm_prd_info';
TRUNCATE TABLE bronze.crm_prd_info;

PRINT'>>Inserting Data Into:bronze.crm_prd_info';
BULK INSERT bronze.crm_prd_info
FROM 'C:\Users\subal\Desktop\DT_W_PROJECT\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH(
FIRSTROW=2,
FIELDTERMINATOR=',',
TABLOCK
);
SET @end_time=GETDATE();
PRINT '>>lOAD dURATION: '+CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR)+' SECONDS';
PRINT '-------------------------------------'

SET @start_time=GETDATE();
PRINT '>> Truncating Table:bronze.crm_sales_details';
TRUNCATE TABLE bronze.crm_sales_details;

PRINT'>>Inserting Data Into:bronze.crm_sales_details';
BULK INSERT bronze.crm_sales_details
FROM 'C:\Users\subal\Desktop\DT_W_PROJECT\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH(
FIRSTROW=2,
FIELDTERMINATOR=',',
TABLOCK
);
SET @end_time=GETDATE();
PRINT '>>lOAD dURATION: '+CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR)+' SECONDS';
PRINT '-------------------------------------'


PRINT '---------------------------------------';
PRINT 'Loading ERP Tables';
PRINT '---------------------------------------';



SET @start_time=GETDATE();
PRINT '>> Truncating Table:bronze.erp_loc_a101';
TRUNCATE TABLE bronze.erp_loc_a101;

PRINT'>>Inserting Data Into:bronze.erp_loc_a101';
BULK INSERT bronze.erp_loc_a101
FROM 'C:\Users\subal\Desktop\DT_W_PROJECT\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
WITH(
FIRSTROW=2,
FIELDTERMINATOR=',',
TABLOCK
);
SET @end_time=GETDATE();
PRINT '>>lOAD dURATION: '+CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR)+' SECONDS';
PRINT '-------------------------------------'

SET @start_time=GETDATE();
PRINT '>> Truncating Table:bronze.erp_cust_az12';
TRUNCATE TABLE bronze.erp_cust_az12;

PRINT'>>Inserting Data Into:bronze.erp_cust_az12';
BULK INSERT bronze.erp_cust_az12
FROM 'C:\Users\subal\Desktop\DT_W_PROJECT\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
WITH(
FIRSTROW=2,
FIELDTERMINATOR=',',
TABLOCK
);
SET @end_time=GETDATE();
PRINT '>>lOAD dURATION: '+CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR)+' SECONDS';
PRINT '-------------------------------------'


SET @start_time=GETDATE();
PRINT '>> Truncating Table:bronze.erp_px_cat_giv2'
TRUNCATE TABLE bronze.erp_px_cat_giv2;

PRINT'>>Inserting Data Into:bronze.erp_px_cat_giv2';
BULK INSERT bronze.erp_px_cat_giv2
FROM 'C:\Users\subal\Desktop\DT_W_PROJECT\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH(
FIRSTROW=2,
FIELDTERMINATOR=',',
TABLOCK
);
SET @end_time=GETDATE();
PRINT '>>lOAD dURATION: '+CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR)+' SECONDS';
PRINT '-------------------------------------';

SET @batch_end_time=GETDATE();
PRINT '=========================================';
PRINT 'Loading Broneze Layer Is Completed';
PRINT '  - Total Load Duration '+CAST (DATEDIFF(SECOND , @batch_start_time,@batch_end_time)as NVARCHAR)+' SECONDS';
PRINT '=========================================';



END TRY
BEGIN CATCH
   PRINT '=================================================';
   PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
   PRINT 'ERROR MESSAGE'+ERROR_MESSAGE();
   PRINT 'ERROR MESSAGE'+CAST (ERROR_NUMBER()AS NVARCHAR);
   PRINT 'ERROR MESSAGE'+CAST (ERROR_STATE()AS NVARCHAR);
   PRINT '=================================================';
END CATCH

END




