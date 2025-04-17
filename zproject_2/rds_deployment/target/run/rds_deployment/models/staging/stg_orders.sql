
  create view "dbt_deployment"."public"."stg_orders__dbt_tmp"
    
    
  as (
    select * from "dbt_deployment"."public"."orders"
  );