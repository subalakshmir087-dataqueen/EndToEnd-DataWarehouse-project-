print'=========================================================================';
/*
Gold Layer - Dimensional Models & Fact Table
- dim_customers: Latest customer record with normalized gender/marital status
- dim_products: Latest product record with category/subcategory mapping
- fact_sales: Sales transactions linked to customer & product dimensions
*/
print'=========================================================================';


print'=========================================================================';

print'Loading Gold Layer';

print'=========================================================================';

CREATE OR ALTER VIEW gold.dim_customers as
select 
   Row_number()over(order by cst_id)as customer_key,
    ci.cst_id as customer_id,
    ci.cst_key as customer_number,
    ci.cst_firstname as Firstname,
    ci.cst_lastname as Lastname,
    la.cntry as Country,
    ci.cst_marital_status as Marital_status,
    CASE WHEN ci.cst_gndr!='n/a'THEN ci.cst_gndr
        ELSE COALESCE(ca.gen,'n/a')
   END as gender,
   ca.bdate as Birthday,
   ci.cst_create_date as Create_date
 from silver.crm_cust_info ci
 left join silver.erp_cust_az12 ca
 on          ci.cst_key=ca.cid
 left join silver.erp_loc_a101 la
 on          ci.cst_key=la.cid


 CREATE OR ALTER VIEW gold.dim_products AS
WITH ranked AS (
    SELECT
        ROW_NUMBER() OVER (PARTITION BY pn.prd_key ORDER BY pn.prd_start_dt DESC) AS rn,
        pn.prd_id AS product_id,
        pn.prd_key AS product_number,
        pn.prd_nm AS product_name,
        pn.cat_id AS category_id,
        pc.cat AS category,
        pc.subcat AS subcategory,
        pc.maintenance,
        pn.prd_cost AS cost,
        pn.prd_line AS product_line,
        pn.prd_start_dt AS start_date
    FROM silver.crm_prd_info pn
    LEFT JOIN silver.erp_px_cat_giv2 pc
        ON pn.cat_id = pc.id
)
SELECT
    ROW_NUMBER() OVER (ORDER BY start_date, product_number) AS product_key,
    product_id,
    product_number,
    product_name,
    category_id,
    category,
    subcategory,
    maintenance,
    cost,
    product_line,
    start_date
FROM ranked
WHERE rn = 1;



CREATE OR ALTER VIEW gold.fact_sales as
 select
    sd.sls_ord_num as order_number,
    pr.product_key,
    cu.customer_key,
    sd.sls_order_dt as order_date,
    sd.sls_ship_dt as shipping_date,
    sd.sls_due_dt as due_date,
    sd.sls_sales as sales_amount,
    sd.sls_quantity as quantity,
    sd.sls_price as price
    from silver.crm_sales_details sd
    left join gold.dim_products pr
    on sd.sls_prd_key=pr.product_number
    left join gold.dim_customers cu
    on sd.sls_cust_id=cu.customer_id

   


 


