
    
    

select
    employee_key as unique_field,
    count(*) as n_records

from "dbt_deployment"."public"."dim_employees"
where employee_key is not null
group by employee_key
having count(*) > 1


