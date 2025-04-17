
  create view "dbt_deployment"."public"."stg_order_details__dbt_tmp"
    
    
  as (
    select * from "dbt_deployment"."public"."order_details"
  );