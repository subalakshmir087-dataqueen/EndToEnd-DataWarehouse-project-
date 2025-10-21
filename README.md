# SQL & Power BI Data Warehouse Project

## Overview
This project demonstrates a **Data Warehouse ETL pipeline** using **SQL Server** and **Power BI**.  
It includes **Bronze, Silver, and Gold layers** with **dimensional modeling** for customers, products, and sales.

---

## Project Layers

### 1. Bronze Layer (Raw Data)
- Raw data loaded from CSV files.
- Tables:
  - **crm_cust_info**: Customer raw data
  - **crm_prd_info**: Product raw data
  - **crm_sales_details**: Sales transactions raw data
  - **erp_cust_az12**: ERP customer info
  - **erp_loc_a101**: ERP location info
  - **erp_px_cat_giv2**: Product category mapping

---

### 2. Silver Layer (Cleansed & Standardized)
- Cleansing & transformations applied.
- Normalized columns (e.g., gender, marital status, product lines, country names).
- Deduplicated records (keep the latest record per primary key).
- Tables:
  - **crm_cust_info**
  - **crm_prd_info**
  - **crm_sales_details**
  - **erp_cust_az12**
  - **erp_loc_a101**
  - **erp_px_cat_giv2**

---

### 3. Gold Layer (Dimensional & Fact Models)
- **Dimensions:**
  - **dim_customers**: Latest customer records with normalized attributes
  - **dim_products**: Latest product records with category/subcategory mapping
- **Fact Table:**
  - **fact_sales**: Sales transactions linked to customer & product dimensions

---

## How to Run the Project

1. **Bronze Layer:** Run `bronze_load.sql` to load raw CSVs into Bronze tables.  
2. **Silver Layer:** Run `silver_load.sql` to clean & transform Bronze tables.  
3. **Gold Layer:** Run `gold_dim_views.sql` to create dimension & fact views.  
4. **Reporting (Optional):** Connect Power BI to Gold layer views for analysis.

---


## Folder Structure
SQL-PowerBI-DataWarehouse-Project/


├── bronze_layer/            # Bronze SQL scripts

├── silver_layer/            # Silver SQL scripts

├── gold_layer/              # Gold SQL scripts & views

├── datasets/                # CSV source data

└── project_documentation/   # README.md 


---

## Notes
- All dates and categorical fields are **normalized**.  
- Data is deduplicated to maintain only **latest valid records**.  
- Power BI can be used to create dashboards from **Gold layer views**.  


