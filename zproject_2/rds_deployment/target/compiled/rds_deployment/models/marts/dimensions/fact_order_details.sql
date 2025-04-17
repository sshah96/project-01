

with base as (
    select 
        od.order_id,
        od.product_id,
        o.customer_id,
        o.employee_id,
        o.ship_via as shipper_id,
        o.order_date,
        od.unit_price,
        od.quantity,
        od.discount,
        od.unit_price * od.quantity * (1 - od.discount) as revenue
    from "dbt_deployment"."public"."stg_order_details" od
    join "dbt_deployment"."public"."stg_orders" o on od.order_id = o.order_id
    
        and o.order_date >= (select max(order_date) from "dbt_deployment"."public"."fact_order_details")
    
),
aggregated as (
    select 
        order_id,
        product_id,
        customer_id,
        employee_id,
        shipper_id,
        cast(order_date as date) as order_date,
        sum(revenue) as total_revenue,
        avg(revenue) as avg_revenue,
        max(revenue) as max_revenue,
        min(revenue) as min_revenue,
        count(*) as line_count,
        rank() over (partition by customer_id order by sum(revenue) desc) as revenue_rank
    from base
    group by order_id, product_id, customer_id, employee_id, shipper_id, order_date
)
select * from aggregated