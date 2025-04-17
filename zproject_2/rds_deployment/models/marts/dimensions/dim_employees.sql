with source_data as (
    select * from {{ ref('stg_employees') }}
),
scd2 as (
    select 
        {{ dbt_utils.generate_surrogate_key(['employee_id']) }} as employee_key,
        *,
        current_timestamp as dbt_updated_at
    from source_data
)
select * from scd2