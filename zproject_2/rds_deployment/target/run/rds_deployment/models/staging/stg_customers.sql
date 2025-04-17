
  create view "dbt_deployment"."public"."stg_customers__dbt_tmp"
    
    
  as (
    select * from "dbt_deployment"."public"."customers"
  );