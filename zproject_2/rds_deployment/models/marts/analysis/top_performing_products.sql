{{ config(materialized='table') }}

select
  p.product_id,
  p.product_name,
  sum(f.total_revenue) as total_revenue,
  count(distinct f.order_id) as number_of_orders
from {{ ref('fact_order_details') }} f
join {{ ref('dim_products') }} p on f.product_id = p.product_id
group by p.product_id, p.product_name
order by total_revenue desc