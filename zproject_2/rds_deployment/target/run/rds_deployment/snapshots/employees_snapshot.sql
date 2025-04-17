
      
  
    

  create  table "dbt_deployment"."public"."employees_snapshot"
  
  
    as
  
  (
    
    

    select *,
        md5(coalesce(cast(employee_id as varchar ), '')
         || '|' || coalesce(cast(now()::timestamp without time zone as varchar ), '')
        ) as dbt_scd_id,
        now()::timestamp without time zone as dbt_updated_at,
        now()::timestamp without time zone as dbt_valid_from,
        
  
  coalesce(nullif(now()::timestamp without time zone, now()::timestamp without time zone), null)
  as dbt_valid_to
from (
        

select * from "dbt_deployment"."public"."stg_employees"
    ) sbq



  );
  
  