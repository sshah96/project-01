
  
    

  create  table "dbt_deployment"."public"."fact_orders__dbt_tmp"
  
  
    as
  
  (
    

select
    od.order_id,
    od.product_id,
    o.customer_id,
    o.employee_id,
    o.order_date,
    od.unit_price,
    od.quantity,
    od.discount,
    od.unit_price * od.quantity * (1 - od.discount) as total_revenue
from "dbt_deployment"."public"."stg_order_details" od
join "dbt_deployment"."public"."stg_orders" o using (order_id)
  );
  