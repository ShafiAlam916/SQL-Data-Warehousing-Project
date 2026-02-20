

-- select t.cst_id, count(*) from ( ) t  group by t.cst_id having count(*) > 1;


select distinct
    ci.cst_gndr,
    cu.gen,
    case 
			when ci.cst_gndr != "n/a" then ci.cst_gndr
			else coalesce(cu.gen, "n/a")
	end as gender
from silver.crm_cust_info ci
left join silver.erp_cust_az12 cu
on ci.cst_key = cu.cid
left join silver.erp_loc_a101 lo 
on  ci.cst_key = lo.cid
order by 1, 2
;


-- foreign key integrity

select
	*
from gold.fact_sales s
join gold.dim_customers c 
on c.customer_key = s.customer_key
join gold.dim_products p
on p.product_key = s.product_key
where p.product_key is null   -- or c.customer_key is null
