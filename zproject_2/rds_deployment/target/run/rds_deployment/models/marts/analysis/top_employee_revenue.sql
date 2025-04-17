
  
    

  create  table "dbt_deployment"."public"."top_employee_revenue__dbt_tmp"
  
  
    as
  
  (
    

select
  e.employee_key,
  e.first_name || ' ' || e.last_name as employee_name,
  sum(f.total_revenue) as total_revenue
from "dbt_deployment"."public"."fact_order_details" f
join "dbt_deployment"."public"."dim_employees" e on f.employee_id = e.employee_id
group by e.employee_key, employee_name
order by total_revenue desc
  );
  