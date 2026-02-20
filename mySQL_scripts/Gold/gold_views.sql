
-- CRM data is master data here so every table is created using CRM tables in left join

create view gold.dim_customers as 
	select
		row_number() over(order by cst_id) as customer_key,
		ci.cst_id as customer_id,
		ci.cst_key as customer_number,
		ci.cst_firstname as first_name,
		ci.cst_lastname as last_name,
		lo.cntry as country,
		ci.cst_marital_status as marital_status,
		case 
				when ci.cst_gndr != "n/a" then ci.cst_gndr
				else coalesce(cu.gen, "n/a")
		end as gender,
		cu.bdate as birth_date,
		ci.cst_create_date as create_date
		
	from silver.crm_cust_info ci
	left join silver.erp_cust_az12 cu
	on ci.cst_key = cu.cid
	left join silver.erp_loc_a101 lo 
	on  ci.cst_key = lo.cid;
	;




create view gold.dim_products as
	select
		ROW_NUMBER() over(order by pi.prd_start_date, pi.prd_id) as product_key,
		pi.prd_id as product_id,
		pi.prd_key as product_number,
		pi.prd_nm as product_name,
		pi.cat_id as category_id,
		pc.cat as category,
		pc.subcat as sub_category,
		pc.maintenance,
		pi.prd_cost as product_cost,
		pi.prd_line as product_line,    
		pi.prd_start_date as start_date
	from silver.crm_prd_info pi
	left join silver.erp_px_cat_g1v2 pc
	on pi.cat_id = pc.id
	where pi.prd_end_date is null
	;



create view gold.fact_sales as
select
	s.sls_order_num as order_number,
    p.product_key,
    c.customer_key,
    s.sls_order_dt as order_date,
    s.sls_ship_dt as shipping_date,
    s.sls_due_dt as due_date,
    s.sls_sales as sales,
    s.sls_quantity as quantity,
    s.sls_price as price
from silver.crm_sales_details s
left join gold.dim_customers c
on s.sls_cust_id = c.customer_id
left join  gold.dim_products p
on s.sls_prd_key = p.product_number
;
