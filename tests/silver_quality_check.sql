/*  ==============================================================
							crm_cust_info 
    ==============================================================
*/
-- Checking Duplicates
SELECT 
	cst_id,
    count(*) as cnt
FROM silver.crm_cust_info 
group by cst_id
having cnt > 1;

-- Chceking Leading or Trailing spaces
SELECT 
	cst_firstname 
FROM silver.crm_cust_info 
where cst_firstname != trim(cst_firstname)
 ;
 -- Walker, Jenkins, Lauren, Chloe
 
 -- Check fat fingering and data integrity
SELECT 
	DISTINCT(cst_gndr)
FROM silver.crm_cust_info 
 ;
 
SELECT 
	DISTINCT(cst_marital_status)
FROM silver.crm_cust_info 
 ;
 
 
 /*  ==============================================================
							crm_prd_info 
    ==============================================================
*/

 -- Checking Duplicates
SELECT 
	prd_id,
    count(*) as cnt
FROM silver.crm_prd_info 
group by prd_id
having cnt > 1 or prd_id is null;

SELECT * FROM bronze.crm_sales_details where sls_prd_key = "FR-R9%";

-- where replace(substring(prd_key, 1, 5), "-", "_") not in (SELECT id FROM bronze.erp_px_cat_g1v2)
-- where substring(prd_key, 7, length(prd_key)) not in (SELECT sls_prd_key FROM bronze.crm_sales_details)
-- both lines are used to find out if any products from prod table dont present in these tables(erp_px_cat_g1v2, crm_sales_details)  or not and cross check those products availability in those tables


-- Chceking Leading or Trailing spaces
SELECT 
	prd_nm
FROM silver.crm_prd_info 
where prd_nm != trim(prd_nm)
 ;
 
-- Check if any price is less than 0 or null
select 
	prd_cost
from silver.crm_prd_info
where prd_cost is null ; 

-- above will not work as all data is ingested as varchar but beolw will change a blank string into null so then checking if null is available or not will work
-- for similar case in crm_cst_info we have used else in case function so every string value casted as n/a which worked in this scenario
-- length(cst_key) = 10 this line is given for 1st table in cte for the same reason some blank strings were disturbing then
-- but if you want to check if there null or not use next line command to turn "" in null

select nullif(prd_cost, "") from silver.crm_prd_info;
-- or 
select * from silver.crm_prd_info where prd_cost = "";

 -- Check fat fingering and data integrity
SELECT 
	DISTINCT(prd_line)
FROM silver.crm_prd_info
 ;
 
select
	*
from silver.crm_prd_info
where prd_start_date < prd_end_date;



 /*  ==============================================================
							crm_sales_details 
    ==============================================================
*/

select 
	sls_order_num,
    count(*)
from silver.crm_sales_details
group by sls_order_num
having count(*) > 1;

select
	sls_order_num
from silver.crm_sales_details
where sls_order_num != trim(sls_order_num);

-- where sls_prd_key not in (select prd_key from silver.crm_prd_info)
-- where sls_cust_id not in (select cst_id from silver.crm_cust_info)

select
	sls_ship_dt
from silver.crm_sales_details
where sls_ship_dt <= 0 or length(sls_due_dt) != 8;

-- you can check if any date is greater than the data recording date or less than company established date after type  ing

select max(str_to_date(sls_order_dt, "%Y%m%d")), min(str_to_date(sls_order_dt, "%Y%m%d")) from bronze.crm_sales_details;

select
	*
from bronze.crm_sales_details
where cast(sls_order_dt as unsigned) > cast(sls_ship_dt as unsigned) or cast(sls_order_dt as unsigned) > cast(sls_due_dt as unsigned);

select
	 sls_sales,
     sls_quantity,
     sls_price,
    CASE
		WHEN  sls_sales is null OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
    CASE
		WHEN  sls_price is null OR sls_price <= 0 
		THEN  round(sls_sales / nullif(sls_quantity, 0))
		ELSE sls_price
	END AS sls_price

from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price 
or  sls_sales is null or  sls_quantity is null or  sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0;

-- without writing   this many times covert all blank strings as null while loading in bronze

 /*  ==============================================================
							erp_cust_az12 
    ==============================================================
*/

select
	*
from silver.erp_cust_az12
where nullif(cid, "") is null or cid != trim(cid);

select
	bdate
from silver.erp_cust_az12
where str_to_date(bdate, "%Y-%m-%d") > now();

select
	bdate
from silver.erp_cust_az12
where length(bdate) != 10;

select
	distinct(trim(gen)),
    case 
			when upper(trim(gen)) in ("M", "MALE") then "Male"
            when upper(trim(gen)) in ("F", "FEMALE") then "Female"
            when gen is null or trim(gen) = '' then "n/a"
            else "n/a"
	end as gen,
    CASE 
		WHEN gen IS NULL OR TRIM(gen) = '' THEN "n/a"
		WHEN UPPER(TRIM(gen)) LIKE 'M%' THEN 'Male'
		WHEN UPPER(TRIM(gen)) LIKE 'F%' THEN 'Female'
		ELSE 'n/a'
	END as gen_new
from silver.erp_cust_az12;


 /*  ==============================================================
							erp_loc_a101 
    ==============================================================
*/

select
	DISTINCT
    case		
			when trim(cntry) = "\r" or trim(cntry) = " "or cntry is null then "n/a" -- where cntry = "n/a" -returns o rows, actually it doesnt exists
			when trim(cntry) like "DE%" then "Germany"
            when trim(cntry) like "US%" then "United States"
			else trim(cntry)
	end as cntry
from silver.erp_loc_a101;


select
	*
from silver.erp_loc_a101
where cntry is null;

SELECT DISTINCT CONCAT('[', cntry, ']')
FROM silver.erp_loc_a101;



 /*  ==============================================================
							erp_px_cat_g1v2
    ==============================================================
*/


-- where id not in (select cat_id from silver.crm_prd_info)

select
	id,
    cat,
    subcat,
    maintenance
from bronze.erp_px_cat_g1v2
where id != trim(id) or cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance);

select
	distinct cat
from bronze.erp_px_cat_g1v2;

select
	distinct subcat
from bronze.erp_px_cat_g1v2;

select
	distinct maintenance
from bronze.erp_px_cat_g1v2;
-- it showing 2 yes and one no maybe some yes contains character like\r which doesnt go with normal trim nor visible
