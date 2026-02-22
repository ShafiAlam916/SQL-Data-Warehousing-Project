/*
===============================================================================
Quality Checks – Gold Layer 
===============================================================================
Script Purpose:
    This script executes validation checks to verify the reliability,
    consistency, and correctness of the Gold layer. The objective is to ensure:

    - Surrogate keys in dimension tables remain unique.
    - Fact tables maintain proper referential integrity with dimensions.
    - Relationships within the star schema are accurate and analytics-ready.

Usage Notes:
    - Review and address any anomalies identified during these validations
      before exposing the data for reporting or analytical consumption.
===============================================================================
*/

/*  ==============================================================
                            dim_customers 
    ============================================================== */
    -- Check for duplicate surrogate keys in dimension
SELECT 
    customer_key,
    COUNT(*) 
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- Check for NULL surrogate keys
SELECT *
FROM gold.dim_customers
WHERE customer_key IS NULL;

-- Validate final gender resolution logic (CRM priority over ERP)
SELECT DISTINCT
    ci.cst_gndr,
    cu.gen,
    CASE 
        WHEN ci.cst_gndr != "n/a" THEN ci.cst_gndr
        ELSE COALESCE(cu.gen, "n/a")
    END AS gender
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 cu
    ON ci.cst_key = cu.cid
LEFT JOIN silver.erp_loc_a101 lo 
    ON ci.cst_key = lo.cid
ORDER BY 1, 2;

/*  ==============================================================
                            dim_products
    ============================================================== */
-- Check for duplicate surrogate keys
SELECT 
    product_key,
    COUNT(*)
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- Check for NULL surrogate keys
SELECT *
FROM gold.dim_products
WHERE product_key IS NULL;

/*  ==============================================================
                            fact_sales
    ============================================================== */
    
-- Full foreign key integrity validation
SELECT *
FROM gold.fact_sales s
JOIN gold.dim_customers c 
    ON c.customer_key = s.customer_key
JOIN gold.dim_products p
    ON p.product_key = s.product_key
WHERE p.product_key IS NULL
   OR c.customer_key IS NULL;
