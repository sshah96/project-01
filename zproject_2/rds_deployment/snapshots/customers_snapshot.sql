{% snapshot customers_snapshot %}
{{
    config(
      target_schema='public',
      unique_key='customer_id',
      strategy='check',
      check_cols=['company_name', 'contact_name', 'contact_title', 'address', 'city', 'region', 'postal_code', 'country', 'phone']
    )
}}
select * from {{ ref('stg_customers') }}
{% endsnapshot %}