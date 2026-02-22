/*
===============================================================================
Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This script load data into the 'silver' schema from bronze layer tables. 
    It performs the following actions:
    - Truncates the silver tables before loading data.
    - Uses the `INSERT` command to load data from bronze tables to silver tables.
===============================================================================
*/


/*  ==============================================================
                            crm_cust_info 
    ============================================================== */

TRUNCATE silver.crm_cust_info;

INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
WITH cte1 AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date) AS row_num
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
      AND LENGTH(cst_key) = 10
)
SELECT
    CAST(cst_id AS UNSIGNED) AS cst_id,
    cst_key,
    TRIM(cst_firstname) AS first_name,
    TRIM(cst_lastname) AS last_name,
    CASE
        WHEN UPPER(TRIM(cst_marital_status)) = "M" THEN "Married"
        WHEN UPPER(TRIM(cst_marital_status)) = "S" THEN "Single"
        ELSE "n/a"
    END AS cst_marital_status,
    CASE
        WHEN UPPER(TRIM(cst_gndr)) = "M" THEN "Male"
        WHEN UPPER(TRIM(cst_gndr)) = "F" THEN "Female"
        ELSE "n/a"
    END AS cst_gndr,
    STR_TO_DATE(cst_create_date, '%Y-%m-%d') AS cst_create_date
FROM cte1
WHERE row_num = 1;



/*  ==============================================================
                            crm_prd_info 
    ============================================================== */

TRUNCATE silver.crm_prd_info;

INSERT INTO silver.crm_prd_info (
    prd_id,
    prd_key,
    cat_id,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_date,
    prd_end_date
)
SELECT 
    CAST(prd_id AS UNSIGNED) AS prd_id,
    SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
    REPLACE(SUBSTRING(prd_key, 1, 5), "-", "_") AS cat_id,
    TRIM(prd_nm) AS prd_nm,
    CAST(IFNULL(NULLIF(prd_cost, ""), 0) AS UNSIGNED) AS prd_cost,
    CASE UPPER(TRIM(prd_line))
        WHEN "R" THEN "Road"
        WHEN "S" THEN "Sunset"
        WHEN "T" THEN "Touring"
        WHEN "M" THEN "Mountain"
        ELSE "n/a"
    END AS prd_line,
    CAST(prd_start_date AS DATE) AS prd_start_date,
    CAST(
        LEAD(prd_start_date) OVER (PARTITION BY prd_key ORDER BY prd_start_date)
        - INTERVAL 1 DAY AS DATE
    ) AS prd_end_date
FROM bronze.crm_prd_info;



/*  ==============================================================
                            crm_sales_details 
    ============================================================== */

TRUNCATE silver.crm_sales_details;

INSERT INTO silver.crm_sales_details (
    sls_order_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT
    sls_order_num,
    sls_prd_key,
    CAST(sls_cust_id AS UNSIGNED) AS sls_cust_id,

    CASE
        WHEN sls_order_dt <= 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
        ELSE STR_TO_DATE(sls_order_dt, "%Y%m%d")
    END AS sls_order_dt,

    CASE
        WHEN sls_ship_dt <= 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
        ELSE STR_TO_DATE(sls_ship_dt, "%Y%m%d")
    END AS sls_ship_dt,

    CASE
        WHEN sls_due_dt <= 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
        ELSE STR_TO_DATE(sls_due_dt, "%Y%m%d")
    END AS sls_due_dt,

    CASE
        WHEN sls_sales IS NULL
          OR sls_sales <= 0
          OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,

    sls_quantity,

    CASE
        WHEN sls_price IS NULL OR sls_price <= 0
        THEN ROUND(sls_sales / NULLIF(sls_quantity, 0))
        ELSE sls_price
    END AS sls_price

FROM bronze.crm_sales_details;



/*  ==============================================================
                            erp_cust_az12 
    ============================================================== */

TRUNCATE silver.erp_cust_az12;

INSERT INTO silver.erp_cust_az12 (
    cid,
    bdate,
    gen
)
SELECT
    CASE
        WHEN LENGTH(cid) = 13 THEN SUBSTRING(cid, 4, LENGTH(cid))
        ELSE cid
    END AS cid,

    CASE
        WHEN STR_TO_DATE(bdate, "%Y-%m-%d") > NOW() THEN NULL
        ELSE STR_TO_DATE(bdate, "%Y-%m-%d")
    END AS bdate,

    CASE
        WHEN gen IS NULL OR TRIM(gen) = '' THEN "n/a"
        WHEN UPPER(TRIM(gen)) LIKE 'M%' THEN 'Male'
        WHEN UPPER(TRIM(gen)) LIKE 'F%' THEN 'Female'
        ELSE 'n/a'
    END AS gen

FROM bronze.erp_cust_az12;



/*  ==============================================================
                            erp_loc_a101 
    ============================================================== */

TRUNCATE silver.erp_loc_a101;

INSERT INTO silver.erp_loc_a101 (
    cid,
    cntry
)
SELECT
    REPLACE(cid, "-", "") AS cid,
    CASE
        WHEN TRIM(cntry) = "\r"
          OR TRIM(cntry) = " "
          OR cntry IS NULL
        THEN "n/a"
        WHEN TRIM(cntry) LIKE "DE%" THEN "Germany"
        WHEN TRIM(cntry) LIKE "US%" THEN "United States"
        ELSE TRIM(cntry)
    END AS cntry
FROM bronze.erp_loc_a101;



/*  ==============================================================
                            erp_px_cat_g1v2
    ============================================================== */

TRUNCATE silver.erp_px_cat_g1v2;

INSERT INTO silver.erp_px_cat_g1v2 (
    id,
    cat,
    subcat,
    maintenance
)
SELECT
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2;
