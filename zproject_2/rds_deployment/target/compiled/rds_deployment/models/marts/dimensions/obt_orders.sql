select 
    f.order_id,
    f.product_id,
    f.customer_id,
    f.employee_id,
    f.shipper_id,
    f.order_date,
    f.total_revenue,
    f.avg_revenue,
    f.max_revenue,
    f.min_revenue,
    f.line_count,
    f.revenue_rank,

    c.company_name as customer_name,
    p.product_name,
    p.category_name,
    p.supplier_name,
    e.first_name || ' ' || e.last_name as employee_name,
    s.company_name as shipper_name

from "dbt_deployment"."public"."fact_order_details" f
left join "dbt_deployment"."public"."dim_customers" c on f.customer_id = c.customer_id
left join "dbt_deployment"."public"."dim_products" p on f.product_id = p.product_id
left join "dbt_deployment"."public"."dim_employees" e on f.employee_id = e.employee_id
left join "dbt_deployment"."public"."shippers" s on f.shipper_id = s.shipper_id