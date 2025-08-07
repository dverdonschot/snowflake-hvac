
  create or replace   view HVAC_ANALYTICS.dbt_dev.stg_hvac__service_calls
  
  
  
  
  as (
    /*
    Staging model for service calls data
    
    This model cleans and standardizes service call data by:
    - Adding calculated fields (cost per hour, priority levels)
    - Filtering out $0 warranty calls for performance analysis
    - Standardizing service types for consistent reporting
*/



SELECT 
    service_call_id,
    customer_id,
    technician_id,
    equipment_id,
    service_date,
    service_type,
    duration_hours,
    labor_cost,
    parts_cost,
    total_cost,
    
    -- Calculated fields
    CASE 
        WHEN duration_hours > 0 THEN ROUND(total_cost / duration_hours, 2)
        ELSE NULL
    END AS cost_per_hour,
    
    CASE 
        WHEN service_type = 'Emergency' THEN 'High Priority'
        WHEN service_type = 'Repair' THEN 'Medium Priority'
        WHEN service_type = 'Installation' THEN 'Medium Priority'
        ELSE 'Standard Priority'
    END AS priority_level,
    
    -- Service efficiency indicators
    CASE 
        WHEN duration_hours <= 2 THEN 'Fast'
        WHEN duration_hours <= 4 THEN 'Normal'
        ELSE 'Slow'
    END AS service_speed_category,
    
    -- Cost efficiency indicators
    CASE 
        WHEN duration_hours > 0 AND (total_cost / duration_hours) <= 100 THEN 'Efficient'
        WHEN duration_hours > 0 AND (total_cost / duration_hours) <= 150 THEN 'Standard'
        ELSE 'Expensive'
    END AS cost_efficiency_category

FROM HVAC_ANALYTICS.dbt_dev.service_calls
WHERE 
    total_cost > 0  -- Remove $0 warranty calls for performance analysis
    AND duration_hours > 0  -- Remove invalid duration records
  );

