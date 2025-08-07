/*
    Intermediate model combining service calls with customer feedback
    
    This model creates a comprehensive view of service performance by:
    - Joining service calls with customer feedback
    - Creating composite performance indicators
    - Preparing data for final analytics mart
*/

{{ config(materialized='view') }}

WITH service_with_feedback AS (
    SELECT 
        sc.service_call_id,
        sc.technician_id,
        sc.customer_id,
        sc.equipment_id,
        sc.service_date,
        sc.service_type,
        sc.priority_level,
        sc.duration_hours,
        sc.total_cost,
        sc.cost_per_hour,
        sc.service_speed_category,
        sc.cost_efficiency_category,
        
        -- Customer feedback metrics
        cf.overall_satisfaction,
        cf.technician_rating,
        cf.timeliness_rating,
        cf.quality_rating,
        cf.value_rating,
        cf.satisfaction_category,
        cf.nps_category,
        cf.composite_service_score,
        cf.would_recommend,
        cf.requires_attention,
        
        -- Create overall service performance score (1-10 scale)
        CASE 
            WHEN cf.overall_satisfaction IS NOT NULL THEN
                ROUND(
                    -- Weight satisfaction scores (40%)
                    (cf.composite_service_score * 0.4) +
                    -- Weight efficiency (30%)
                    (CASE 
                        WHEN sc.service_speed_category = 'Fast' THEN 8
                        WHEN sc.service_speed_category = 'Normal' THEN 6
                        ELSE 4
                    END * 0.3) +
                    -- Weight cost efficiency (30%)
                    (CASE 
                        WHEN sc.cost_efficiency_category = 'Efficient' THEN 8
                        WHEN sc.cost_efficiency_category = 'Standard' THEN 6
                        ELSE 4
                    END * 0.3),
                    1
                )
            ELSE NULL
        END AS overall_performance_score
        
    FROM {{ ref('stg_hvac__service_calls') }} sc
    LEFT JOIN {{ ref('stg_hvac__customer_feedback') }} cf 
        ON sc.service_call_id = cf.service_call_id
)

SELECT 
    *,
    
    -- Performance tier based on overall score
    CASE 
        WHEN overall_performance_score >= 8 THEN 'Excellent'
        WHEN overall_performance_score >= 6 THEN 'Good'
        WHEN overall_performance_score >= 4 THEN 'Fair'
        WHEN overall_performance_score IS NOT NULL THEN 'Poor'
        ELSE 'No Feedback'
    END AS performance_tier,
    
    -- Flag for high-value services (cost > $500)
    CASE 
        WHEN total_cost >= 500 THEN TRUE
        ELSE FALSE
    END AS high_value_service,
    
    -- Service month for time-based analysis
    DATE_TRUNC('month', service_date) AS service_month

FROM service_with_feedback