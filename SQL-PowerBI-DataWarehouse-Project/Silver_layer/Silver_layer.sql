print'======================================================';
/*
Silver Layer - Cleaned & Transformed Data
- Normalize customer names, gender, marital status
- Extract product categories and calculate end dates
- Fix invalid sales dates and sales_amount calculations
- Prepare data for dimensional modeling in Gold layer
*/
print'======================================================';

IF OBJECT_ID('silver.crm_cust_info','U')is not null
   DROP TABLE silver.crm_cust_info;

CREATE TABLE silver.crm_cust_info(
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname  NVARCHAR(50),
cst_marital_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);


IF OBJECT_ID('silver.crm_prd_info','U')is not null
   DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info(
prd_id INT,
prd_key NVARCHAR(50),
prd_nm  NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);


IF OBJECT_ID('silver.crm_sales_details','U')is not null
   DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);


IF OBJECT_ID('silver.erp_loc_a101','U')is not null
     DROP table silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101(
cid  NVARCHAR(50),
cntry NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);


IF OBJECT_ID('silver.erp_cust_az12','U')is not null
   DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12(
cid NVARCHAR(50),
bdate DATE,
gen NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);


IF OBJECT_ID('silver.erp_px_cat_giv2','U')is not null
   DROP TABLE silver.erp_px_cat_giv2;
CREATE TABLE silver.erp_px_cat_giv2(
id  NVARCHAR(50),
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintenance NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);



CREATE OR ALTER PROCEDURE silver.load_silver as
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
BEGIN TRY
SET @batch_start_time=GETDATE();
PRINT '========================================';
PRINT 'Loading Silver Layer';
PRINT '  - Total Load Duration '+CAST (DATEDIFF(SECOND , @batch_start_time,@batch_end_time)as NVARCHAR)+' SECONDS';
PRINT '=========================================';


PRINT '=======================================';
PRINT 'Loading Silver Layer';
PRINT '=======================================';

PRINT '---------------------------------------';
PRINT 'Loading CRM Tables';
PRINT '---------------------------------------';

SET @start_time=GETDATE();
print'>>truncate table  silver.crm_cust_info';
truncate table  silver.crm_cust_info;
print '>>inserting data into silver.crm_cust_info';
INSERT INTO silver.crm_cust_info(
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
 TRIM(cst_firstname)as cst_firstname,
 TRIM(cst_lastname)as cst_lastname,
 CASE when UPPER(TRIM(cst_marital_status))='s' then 'Single'
      when UPPER(TRIM(cst_marital_status))='M' then 'Married'
      ELSE 'n/a'
 END cst_marital_status,---Normalize marital status values to readable format
 CASE WHEN UPPER(TRIM(cst_gndr))='F'THEN 'Female'
      WHEN UPPER(TRIM(cst_gndr))='M'THEN 'Male'
      ELSE 'n/a'
END cst_gndr,  ----Normalize gender values to readable format
cst_create_date
from(
select
*,
row_NUMBER()OVER(PARTITION BY cst_id order by cst_create_date DESC)as flag_last
from bronze.crm_cust_info
where cst_id is not null)t
where flag_last=1; ---select the most recent record per customers
SET @end_time=GETDATE();
PRINT '>>lOAD dURATION: '+CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR)+' SECONDS';
PRINT '-------------------------------------';


IF OBJECT_ID('silver.crm_prd_info','U')is not null
DROP TABLE silver.crm_prd_info; 
CREATE TABLE silver.crm_prd_info( 
prd_id INT, 
cat_id NVARCHAR(50),
prd_key NVARCHAR(50), 
prd_nm NVARCHAR(50), 
prd_cost INT, 
prd_line NVARCHAR(50), 
prd_start_dt DATE,
prd_end_dt DATE, 
dwh_create_date DATETIME2 DEFAULT GETDATE() );


SET @start_time=GETDATE();
print'>>truncate table  silver.crm_prd_info';
truncate table  silver.crm_prd_info;
print '>>inserting data into silver.crm_prd_info';
INSERT INTO silver.crm_prd_info(
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
replace(substring(prd_key,1,5),'-','_')as cat_id,---extract category id
substring(prd_key,7,len(prd_key))as prd_key,----extract product id
prd_nm,
isnull(prd_cost,0)as prd_cost,
CASE WHEN UPPER(TRIM(prd_line))='M'THEN 'Mountain'
     WHEN UPPER(TRIM(prd_line))='R'THEN 'Road'
     WHEN UPPER(TRIM(prd_line))='S'THEN 'Other sales'
     WHEN UPPER(TRIM(prd_line))='T'THEN 'Touring'
     ELSE 'n/a'
END prd_line,---map product line codes to descriptive values
CAST(prd_start_dt as DATE) as prd_start_dt,
CAST(lead(prd_start_dt)over(partition by prd_key order by prd_start_dt asc )-1 
         as DATE
         )as prd_end_dt --- calculate end date as one day before the next start date
from bronze.crm_prd_info;
SET @end_time=GETDATE();
PRINT '>>lOAD dURATION: '+CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR)+' SECONDS';
PRINT '-------------------------------------';


IF OBJECT_ID('silver.crm_sales_details','U')is not null
DROP TABLE silver.crm_sales_details; 
CREATE TABLE silver.crm_sales_details( 
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50), 
sls_cust_id INT, 
sls_order_dt DATE,
sls_ship_dt DATE, 
sls_due_dt DATE,
sls_sales INT, 
sls_quantity INT,
sls_price INT, 
dwh_create_date DATETIME2 DEFAULT GETDATE() );

SET @start_time=GETDATE();
print'>>truncate table  silver.crm_sales_details';
truncate table  silver.crm_sales_details;
print '>>inserting data into silver.crm_sales_details';
INSERT INTO silver.crm_sales_details( 
sls_ord_num, 
sls_prd_key, 
sls_cust_id,
sls_order_dt, 
sls_ship_dt, 
sls_due_dt,
sls_sales, 
sls_quantity ,
sls_price  
)
SELECT
sls_ord_num, 
sls_prd_key, 
sls_cust_id,
CASE WHEN sls_order_dt=0 or LEN(sls_order_dt)!=8 THEN NULL 
     ELSE CAST(CAST(sls_order_dt as VARCHAR)AS DATE) 
END AS sls_order_dt,
CASE WHEN sls_ship_dt=0 or LEN(sls_ship_dt)!=8 THEN NULL 
     ELSE CAST(CAST(sls_ship_dt as VARCHAR)AS DATE)
END AS sls_ship_dt, 
CASE WHEN sls_due_dt=0 or LEN(sls_due_dt)!=8 THEN NULL 
     ELSE CAST(CAST(sls_due_dt as VARCHAR)AS DATE)
END AS sls_due_dt, 

CASE WHEN sls_sales is null or sls_sales<=0 or sls_sales!=
                sls_quantity*abs(sls_price) THEN sls_quantity*abs(sls_price)
ELSE sls_sales
end as sls_sales,
sls_quantity ,

CASE WHEN sls_price is null or sls_price<=0
           THEN sls_sales/nullif(sls_quantity,0)
else sls_price
end sls_price
from bronze.crm_sales_details;
SET @end_time=GETDATE();
PRINT '>>lOAD dURATION: '+CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR)+' SECONDS';
PRINT '-------------------------------------';


PRINT '---------------------------------------';
PRINT 'Loading ERP Tables';
PRINT '---------------------------------------';



SET @start_time=GETDATE();
print'>>truncate table  silver.erp_cust_az12';
truncate table  silver.erp_cust_az12;
print '>>inserting data into silver.erp_cust_az12';
INSERT INTO silver.erp_cust_az12(
 cid,
 bdate,
 gen
 )
 select

CASE WHEN cid like 'NAS%'then substring(cid,4,len(cid))
ELSE cid
end cid,
CASE WHEN bdate >getdate() THEN null
ELSE bdate
end bdate,
CASE WHEN UPPER(TRIM(gen)) in ('F','FEMALE')THEN 'Female'
     WHEN UPPER(TRIM(gen)) in ('M','MALE')THEN 'Male'
     ELSE 'n/a'
end as gen
from bronze.erp_cust_az12;
SET @end_time=GETDATE();
PRINT '>>lOAD dURATION: '+CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR)+' SECONDS';
PRINT '-------------------------------------';



SET @start_time=GETDATE();
print'>>truncate table  silver.erp_loc_a101';
truncate table  silver.erp_loc_a101;
print '>>inserting data into silver.erp_loc_a101';
INSERT INTO silver.erp_loc_a101(
cid,
cntry
)
select
replace(cid,'-','')cid,
CASE WHEN TRIM(cntry)='DE'THEN 'Germany'
     WHEN TRIM(cntry) IN ('US','USA')THEN 'United States'
     WHEN TRIM(cntry)='' or cntry is null THEN 'n/a'
     else TRIM(cntry)
end as cntry
from bronze.erp_loc_a101;
SET @end_time=GETDATE();
PRINT '>>lOAD dURATION: '+CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR)+' SECONDS';
PRINT '-------------------------------------';


SET @start_time=GETDATE();
print'>>truncate table  silver.erp_px_cat_giv2';
truncate table  silver.erp_px_cat_giv2;
print '>>inserting data into silver.erp_px_cat_giv2';
insert into silver.erp_px_cat_giv2(
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
from bronze.erp_px_cat_giv2;
SET @end_time=GETDATE();
PRINT '>>lOAD dURATION: '+CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR)+' SECONDS';
PRINT '-------------------------------------';


SET @batch_end_time=GETDATE();
PRINT '=========================================';
PRINT 'Loading Silver Layer Is Completed';
PRINT '  - Total Load Duration '+CAST (DATEDIFF(SECOND , @batch_start_time,@batch_end_time)as NVARCHAR)+' SECONDS';
PRINT '=========================================';

END TRY
BEGIN CATCH
   PRINT '=================================================';
   PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
   PRINT 'ERROR MESSAGE'+ERROR_MESSAGE();
   PRINT 'ERROR MESSAGE'+CAST (ERROR_NUMBER()AS NVARCHAR);
   PRINT 'ERROR MESSAGE'+CAST (ERROR_STATE()AS NVARCHAR);
   PRINT '=================================================';
END CATCH

END



