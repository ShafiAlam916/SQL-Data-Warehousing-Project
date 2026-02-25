-- CRM data is master data here so every table is created using CRM tables in left join

/*
===============================================================================
DDL Script: Create Gold Layer Views
===============================================================================
Script Purpose:
    This script defines the views for the Gold layer of the data warehouse.
    The Gold layer represents the finalized dimensional model (Star Schema),
    including fact and dimension views optimized for analytics.

    Each view applies the necessary transformations and integrates data 
    from the Silver layer to generate refined, enriched, and 
    business-consumable datasets.

Usage:
    - These views are intended to be queried directly for reporting,
      dashboards, and analytical workloads.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

DROP VIEW IF EXISTS gold.dim_customers;

CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
    ci.cst_id            AS customer_id,
    ci.cst_key           AS customer_number,
    ci.cst_firstname     AS first_name,
    ci.cst_lastname      AS last_name,
    lo.cntry             AS country,
    ci.cst_marital_status AS marital_status,
    CASE 
        WHEN ci.cst_gndr != "n/a" THEN ci.cst_gndr
        ELSE COALESCE(cu.gen, "n/a")
    END AS gender,
    cu.bdate             AS birth_date,
    ci.cst_create_date   AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 cu
    ON ci.cst_key = cu.cid
LEFT JOIN silver.erp_loc_a101 lo 
    ON ci.cst_key = lo.cid
;


-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================

DROP VIEW IF EXISTS gold.dim_products;

CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pi.prd_start_date, pi.prd_id) AS product_key,
    pi.prd_id        AS product_id,
    pi.prd_key       AS product_number,
    pi.prd_nm        AS product_name,
    pi.cat_id        AS category_id,
    pc.cat           AS category,
    pc.subcat        AS sub_category,
    pc.maintenance,
    pi.prd_cost      AS product_cost,
    pi.prd_line      AS product_line,    
    pi.prd_start_date AS start_date
FROM silver.crm_prd_info pi
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pi.cat_id = pc.id
WHERE pi.prd_end_date IS NULL
;


-- =============================================================================
-- Create Fact: gold.fact_sales
-- =============================================================================

DROP VIEW IF EXISTS gold.fact_sales;

CREATE VIEW gold.fact_sales AS
SELECT
    s.sls_order_num  AS order_number,
    p.product_key,
    c.customer_key,
    s.sls_order_dt   AS order_date,
    s.sls_ship_dt    AS shipping_date,
    s.sls_due_dt     AS due_date,
    s.sls_sales      AS sales,
    s.sls_quantity   AS quantity,
    s.sls_price      AS price
FROM silver.crm_sales_details s
LEFT JOIN gold.dim_customers c
    ON s.sls_cust_id = c.customer_id
LEFT JOIN gold.dim_products p
    ON s.sls_prd_key = p.product_number
;
