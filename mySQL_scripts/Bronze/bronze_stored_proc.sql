TRUNCATE bronze.crm_cust_info;
LOAD DATA INFILE 'D:/Courses/Data With Baraa/sql-data-warehouse-project-main/datasets/source_crm/cust_info.csv'
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


TRUNCATE bronze.crm_prd_info;
LOAD DATA INFILE 'D:/Courses/Data With Baraa/sql-data-warehouse-project-main/datasets/source_crm/prd_info.csv'
INTO TABLE bronze.crm_prd_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


TRUNCATE bronze.crm_sales_details;

LOAD DATA INFILE 'D:/Courses/Data With Baraa/sql-data-warehouse-project-main/datasets/source_crm/sales_details.csv'
INTO TABLE bronze.crm_sales_details
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(
    @sls_order_num,
    @sls_prd_key,
    @sls_cust_id,
    @sls_order_dt,
    @sls_ship_dt,
    @sls_due_dt,
    @sls_sales,
    @sls_quantity,
    @sls_price
)
SET
    sls_order_num = NULLIF(@sls_order_num,''),
    sls_prd_key   = NULLIF(@sls_prd_key,''),
    sls_cust_id   = NULLIF(@sls_cust_id,''),
    sls_order_dt  = NULLIF(@sls_order_dt,''),
    sls_ship_dt   = NULLIF(@sls_ship_dt,''),
    sls_due_dt    = NULLIF(@sls_due_dt,''),
    sls_sales     = NULLIF(@sls_sales,''),
    sls_quantity  = NULLIF(@sls_quantity,''),
    sls_price     = NULLIF(@sls_price,'');



TRUNCATE bronze.erp_cust_az12;
LOAD DATA INFILE 'D:/Courses/Data With Baraa/sql-data-warehouse-project-main/datasets/source_erp/cust_az12.csv'
INTO TABLE bronze.erp_cust_az12
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@cid, @bdate, @gen)
SET
    CID = NULLIF(@cid, ''),
    BDATE = NULLIF(@bdate, ''),
    GEN = NULLIF(@gen, '');
-- Using nullif is recommended in bronze layer even though you use varchar for all. this table throws 1261 error which caused by blank strings at last row

TRUNCATE bronze.erp_loc_a101;
LOAD DATA INFILE 'D:/Courses/Data With Baraa/sql-data-warehouse-project-main/datasets/source_erp/loc_a101.csv'
INTO TABLE bronze.erp_loc_a101
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@cid, @cntry)
SET
	CID = NULLIF(trim(@cid), ''),
    CNTRY = NULLIF(trim(@cntry), '');

TRUNCATE bronze.erp_px_cat_g1v2;
LOAD DATA INFILE 'D:/Courses/Data With Baraa/sql-data-warehouse-project-main/datasets/source_erp/px_cat_g1v2.csv'
INTO TABLE bronze.erp_px_cat_g1v2
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
