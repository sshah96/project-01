
  create view "dbt_deployment"."public"."dim_products__dbt_tmp"
    
    
  as (
    select 
    p.product_id,
    p.product_name,
    p.supplier_id,
    s.company_name as supplier_name,
    p.category_id,
    c.category_name,
    p.quantity_per_unit,
    p.unit_price,
    p.units_in_stock,
    p.units_on_order,
    p.reorder_level,
    p.discontinued
from "dbt_deployment"."public"."stg_products" p
left join "dbt_deployment"."public"."suppliers" s on p.supplier_id = s.supplier_id
left join "dbt_deployment"."public"."categories" c on p.category_id = c.category_id
  );