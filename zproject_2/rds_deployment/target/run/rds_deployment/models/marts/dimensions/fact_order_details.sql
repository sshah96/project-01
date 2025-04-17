
      -- back compat for old kwarg name
  
  
        
            
                
                
            
                
                
            
        
    

    

    merge into "dbt_deployment"."public"."fact_order_details" as DBT_INTERNAL_DEST
        using "fact_order_details__dbt_tmp214524825405" as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.order_id = DBT_INTERNAL_DEST.order_id
                ) and (
                    DBT_INTERNAL_SOURCE.product_id = DBT_INTERNAL_DEST.product_id
                )

    
    when matched then update set
        "order_id" = DBT_INTERNAL_SOURCE."order_id","product_id" = DBT_INTERNAL_SOURCE."product_id","customer_id" = DBT_INTERNAL_SOURCE."customer_id","employee_id" = DBT_INTERNAL_SOURCE."employee_id","shipper_id" = DBT_INTERNAL_SOURCE."shipper_id","order_date" = DBT_INTERNAL_SOURCE."order_date","total_revenue" = DBT_INTERNAL_SOURCE."total_revenue","avg_revenue" = DBT_INTERNAL_SOURCE."avg_revenue","max_revenue" = DBT_INTERNAL_SOURCE."max_revenue","min_revenue" = DBT_INTERNAL_SOURCE."min_revenue","line_count" = DBT_INTERNAL_SOURCE."line_count","revenue_rank" = DBT_INTERNAL_SOURCE."revenue_rank"
    

    when not matched then insert
        ("order_id", "product_id", "customer_id", "employee_id", "shipper_id", "order_date", "total_revenue", "avg_revenue", "max_revenue", "min_revenue", "line_count", "revenue_rank")
    values
        ("order_id", "product_id", "customer_id", "employee_id", "shipper_id", "order_date", "total_revenue", "avg_revenue", "max_revenue", "min_revenue", "line_count", "revenue_rank")


  