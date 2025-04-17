

select
  c.customer_id,
  c.company_name as customer_name,
  sum(f.total_revenue) as total_revenue
from "dbt_deployment"."public"."fact_order_details" f
join "dbt_deployment"."public"."dim_customers" c using (customer_id)
group by c.customer_id, c.company_name
order by total_revenue desc