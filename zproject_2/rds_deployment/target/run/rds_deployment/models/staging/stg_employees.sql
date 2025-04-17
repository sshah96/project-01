
  create view "dbt_deployment"."public"."stg_employees__dbt_tmp"
    
    
  as (
    select * from "dbt_deployment"."public"."employees"
  );