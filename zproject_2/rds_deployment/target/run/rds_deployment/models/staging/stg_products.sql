
  create view "dbt_deployment"."public"."stg_products__dbt_tmp"
    
    
  as (
    select * from "dbt_deployment"."public"."products"
  );