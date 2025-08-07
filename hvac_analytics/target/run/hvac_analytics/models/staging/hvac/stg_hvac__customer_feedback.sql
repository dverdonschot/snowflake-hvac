
  create or replace   view HVAC_ANALYTICS.dbt_dev.stg_hvac__customer_feedback
  
  
  
  
  as (
    /*
    Staging model for customer feedback data
    
    This model standardizes customer feedback by:
    - Creating satisfaction categories from numeric scores
    - Calculating composite satisfaction scores
    - Standardizing feedback types for analysis
*/



SELECT 
    feedback_id,
    service_call_id,
    customer_id,
    feedback_date,
    feedback_type,
    overall_satisfaction,
    technician_rating,
    timeliness_rating,
    quality_rating,
    value_rating,
    comments,
    would_recommend,
    follow_up_required,
    source,
    
    -- Satisfaction categories (industry standard)
    CASE 
        WHEN overall_satisfaction >= 9 THEN 'Excellent'
        WHEN overall_satisfaction >= 7 THEN 'Good'
        WHEN overall_satisfaction >= 5 THEN 'Fair'
        WHEN overall_satisfaction >= 3 THEN 'Poor'
        ELSE 'Very Poor'
    END AS satisfaction_category,
    
    -- Net Promoter Score categories
    CASE 
        WHEN overall_satisfaction >= 9 THEN 'Promoter'
        WHEN overall_satisfaction >= 7 THEN 'Passive'
        ELSE 'Detractor'
    END AS nps_category,
    
    -- Calculate composite service score (average of all ratings)
    ROUND(
        (overall_satisfaction + technician_rating + timeliness_rating + 
         quality_rating + value_rating) / 5.0,
        1
    ) AS composite_service_score,
    
    -- Flag exceptional experiences (very high or very low)
    CASE 
        WHEN overall_satisfaction >= 9 THEN 'Exceptional Positive'
        WHEN overall_satisfaction <= 3 THEN 'Critical Issue'
        ELSE 'Standard'
    END AS experience_flag,
    
    -- Identify feedback requiring attention
    CASE 
        WHEN overall_satisfaction <= 5 OR follow_up_required = TRUE THEN TRUE
        ELSE FALSE
    END AS requires_attention

FROM HVAC_ANALYTICS.dbt_dev.customer_feedback
WHERE 
    overall_satisfaction IS NOT NULL  -- Ensure we have valid ratings
  );

