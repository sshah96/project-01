

with source as (
    select * from "dbt_deployment"."public"."stg_orders"
),

final as (
    select
        order_id as order_key,
        customer_id,
        employee_id,
        order_date,
        required_date,
        shipped_date,
        ship_via,
        freight,
        ship_name,
        ship_address,
        ship_city,
        ship_region,
        ship_postal_code,
        ship_country
    from source
)

select * from final