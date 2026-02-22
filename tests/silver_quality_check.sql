/*
===============================================================================
Quality Checks – Silver Layer
===============================================================================
Purpose:
    This script performs data quality checks on Silver layer tables.
    It helps identify inconsistencies inherited from the Bronze layer
    and validates that Silver transformations have been correctly applied.

    The checks include:
        - Duplicate detection
        - Leading/trailing space validation
        - Null and blank value detection
        - Data integrity validation
        - Low-cardinality column inspection
        - Date consistency validation
        - Business rule validation (sales calculations)

    These queries are intended for validation and auditing purposes
    before promoting data to the Gold layer.
===============================================================================
*/


/*  ==============================================================
                            crm_cust_info 
    ============================================================== */

-- Check for duplicate customer IDs
SELECT 
    cst_id,
    COUNT(*) AS cnt
FROM silver.crm_cust_info 
GROUP BY cst_id
HAVING cnt > 1;

-- Check for leading or trailing spaces in first name
SELECT 
    cst_firstname 
FROM silver.crm_cust_info 
WHERE cst_firstname != TRIM(cst_firstname);

-- Check data consistency in gender column (low cardinality validation)
SELECT DISTINCT(cst_gndr)
FROM silver.crm_cust_info;

-- Check data consistency in marital status column
SELECT DISTINCT(cst_marital_status)
FROM silver.crm_cust_info;



/*  ==============================================================
                            crm_prd_info 
    ============================================================== */

-- Check for duplicate or NULL product IDs
SELECT 
    prd_id,
    COUNT(*) AS cnt
FROM silver.crm_prd_info 
GROUP BY prd_id
HAVING cnt > 1 OR prd_id IS NULL;

-- Sample validation against bronze sales table
SELECT * 
FROM bronze.crm_sales_details 
WHERE sls_prd_key = "FR-R9%";


-- Check for leading or trailing spaces in product name
SELECT 
    prd_nm
FROM silver.crm_prd_info 
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULL product cost
SELECT 
    prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL;

-- Convert blank strings to NULL for validation
SELECT NULLIF(prd_cost, "") 
FROM silver.crm_prd_info;

-- Identify blank string prices
SELECT * 
FROM silver.crm_prd_info 
WHERE prd_cost = "";

-- Check data consistency in product line column
SELECT DISTINCT(prd_line)
FROM silver.crm_prd_info;

-- Validate product date logic (start date should not be after end date)
SELECT *
FROM silver.crm_prd_info
WHERE prd_start_date < prd_end_date;



/*  ==============================================================
                            crm_sales_details 
    ============================================================== */

-- Check for duplicate order numbers
SELECT 
    sls_order_num,
    COUNT(*)
FROM silver.crm_sales_details
GROUP BY sls_order_num
HAVING COUNT(*) > 1;

-- Check for leading/trailing spaces in order number
SELECT
    sls_order_num
FROM silver.crm_sales_details
WHERE sls_order_num != TRIM(sls_order_num);

-- Validate shipping date format and integrity
SELECT
    sls_ship_dt
FROM silver.crm_sales_details
WHERE sls_ship_dt <= 0 
   OR LENGTH(sls_due_dt) != 8;

-- Check min and max order dates
SELECT 
    MAX(STR_TO_DATE(sls_order_dt, "%Y%m%d")),
    MIN(STR_TO_DATE(sls_order_dt, "%Y%m%d"))
FROM bronze.crm_sales_details;

-- Validate order date against ship and due dates
SELECT *
FROM bronze.crm_sales_details
WHERE CAST(sls_order_dt AS UNSIGNED) > CAST(sls_ship_dt AS UNSIGNED)
   OR CAST(sls_order_dt AS UNSIGNED) > CAST(sls_due_dt AS UNSIGNED);

-- Validate sales calculations and pricing logic
SELECT
    sls_sales,
    sls_quantity,
    sls_price,

    CASE
        WHEN sls_sales IS NULL 
          OR sls_sales <= 0 
          OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,

    CASE
        WHEN sls_price IS NULL 
          OR sls_price <= 0 
        THEN ROUND(sls_sales / NULLIF(sls_quantity, 0))
        ELSE sls_price
    END AS sls_price

FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price 
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0;



/*  ==============================================================
                            erp_cust_az12 
    ============================================================== */

-- Check for NULL, blank, or improperly trimmed customer IDs
SELECT *
FROM silver.erp_cust_az12
WHERE NULLIF(cid, "") IS NULL 
   OR cid != TRIM(cid);

-- Validate birth date is not in the future
SELECT bdate
FROM silver.erp_cust_az12
WHERE STR_TO_DATE(bdate, "%Y-%m-%d") > NOW();

-- Validate birth date length format
SELECT bdate
FROM silver.erp_cust_az12
WHERE LENGTH(bdate) != 10;

-- Validate and standardize gender values
SELECT
    DISTINCT(TRIM(gen)),
    CASE 
        WHEN UPPER(TRIM(gen)) IN ("M", "MALE") THEN "Male"
        WHEN UPPER(TRIM(gen)) IN ("F", "FEMALE") THEN "Female"
        WHEN gen IS NULL OR TRIM(gen) = '' THEN "n/a"
        ELSE "n/a"
    END AS gen,

    CASE 
        WHEN gen IS NULL OR TRIM(gen) = '' THEN "n/a"
        WHEN UPPER(TRIM(gen)) LIKE 'M%' THEN 'Male'
        WHEN UPPER(TRIM(gen)) LIKE 'F%' THEN 'Female'
        ELSE 'n/a'
    END AS gen_new
FROM silver.erp_cust_az12;



/*  ==============================================================
                            erp_loc_a101 
    ============================================================== */

-- Standardize and validate country values
SELECT DISTINCT
    CASE        
        WHEN TRIM(cntry) = "\r" 
          OR TRIM(cntry) = " "
          OR cntry IS NULL 
        THEN "n/a"
        WHEN TRIM(cntry) LIKE "DE%" THEN "Germany"
        WHEN TRIM(cntry) LIKE "US%" THEN "United States"
        ELSE TRIM(cntry)
    END AS cntry
FROM silver.erp_loc_a101;

-- Check for NULL country values
SELECT *
FROM silver.erp_loc_a101
WHERE cntry IS NULL;

-- Inspect hidden characters in country column
SELECT DISTINCT CONCAT('[', cntry, ']')
FROM silver.erp_loc_a101;



/*  ==============================================================
                            erp_px_cat_g1v2
    ============================================================== */

-- Validate trimming issues across all columns
SELECT
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2
WHERE id != TRIM(id) 
   OR cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- Inspect distinct category values
SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2;

-- Inspect distinct subcategory values
SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2;

-- Inspect distinct maintenance values
SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2;
