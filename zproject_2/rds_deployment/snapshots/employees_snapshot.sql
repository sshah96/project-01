{% snapshot employees_snapshot %}
{{
    config(
      target_schema='public',
      unique_key='employee_id',
      strategy='check',
      check_cols=['first_name', 'last_name', 'title', 'region', 'address']
    )
}}
select * from {{ ref('stg_employees') }}
{% endsnapshot %}