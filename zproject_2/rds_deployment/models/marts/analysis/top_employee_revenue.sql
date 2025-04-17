{{ config(materialized='table') }}

select
  e.employee_key,
  e.first_name || ' ' || e.last_name as employee_name,
  sum(f.total_revenue) as total_revenue
from {{ ref('fact_order_details') }} f
join {{ ref('dim_employees') }} e on f.employee_id = e.employee_id
group by e.employee_key, employee_name
order by total_revenue desc