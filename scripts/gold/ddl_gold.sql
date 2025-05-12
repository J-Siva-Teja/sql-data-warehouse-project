/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
  
CREATE VIEW gold.dim_customers AS
SELECT 
  ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key, 
  ci.cst_id AS customer_id, 
  ci.cst_key AS customer_number, 
  CONCAT(ci.cst_firstname, ' ', ci.cst_lastname) AS full_name, 
  ci.cst_marital_status AS marital_status, 
  CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr /* Considering CRM table as Master*/ 
  ELSE COALESCE (eci.gen, 'n/a') END AS gender, 
  cl.cntry AS country, 
  eci.bdate AS birth_date, 
  ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci 
LEFT JOIN silver.erp_cust_az12 eci 
  ON ci.cst_key = eci.cid 
LEFT JOIN silver.erp_loc_a101 cl 
  ON eci.cid = cl.cid;
GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT 
  ROW_NUMBER() OVER (ORDER BY prd.prd_start_dt, prd.prd_id) AS product_key, 
  prd.prd_id AS product_id, 
  prd.prd_key AS product_number, 
  prd.prd_nm AS product_name, 
  prd.cat_id AS category_id, 
  pc.cat AS category, 
  pc.subcat AS sub_category, 
  pc.maintanance AS maintainance, 
  prd.prd_cost AS product_cost, 
  prd.prd_line AS product_line,
  prd.prd_start_dt AS start_date
FROM  silver.crm_prd_info prd 
LEFT JOIN silver.erp_px_cat_g1v2 pc 
  ON pc.id = prd.cat_id
WHERE  prd.prd_end_dt IS NULL; -- This will filter out historical data
GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT 
  cs.sls_ord_num AS order_number, 
  pr.product_key, ci.customer_key, 
  cs.sls_price AS price, 
  cs.sls_quantity AS quantity, 
  cs.sls_sales AS sales, 
  cs.sls_order_dt AS order_date, 
  cs.sls_ship_dt AS ship_date, 
  cs.sls_due_dt AS due_date
FROM silver.crm_sales_details cs 
LEFT JOIN gold.dim_products pr 
  ON cs.sls_prd_key = pr.product_number 
LEFT JOIN gold.dim_customers ci 
  ON cs.sls_cust_id = ci.customer_id
GO
