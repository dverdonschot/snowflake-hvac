/*
    Service Performance Dashboard Mart
    
    This model provides executive-level insights into technician performance by:
    - Aggregating service metrics by technician
    - Creating KPIs for dashboard consumption
    - Ranking technicians by performance
    - Providing actionable insights for management
*/

{{ config(
    materialized='table',
    description='Executive dashboard showing technician service performance metrics'
) }}

WITH technician_performance AS (
    SELECT 
        t.technician_id,
        t.technician_name,
        t.technician_level,
        t.experience_category,
        t.hourly_rate,
        t.rate_category,
        t.avg_skill_level,
        t.technical_skill_avg,
        t.customer_service_skill,
        t.years_experience,
        t.tenure_months,
        
        -- Service volume metrics
        COUNT(spm.service_call_id) AS total_service_calls,
        COUNT(CASE WHEN spm.service_type = 'Emergency' THEN 1 END) AS emergency_calls,
        COUNT(CASE WHEN spm.high_value_service = TRUE THEN 1 END) AS high_value_services,
        
        -- Customer satisfaction metrics  
        COUNT(CASE WHEN spm.overall_satisfaction IS NOT NULL THEN 1 END) AS feedback_received,
        ROUND(AVG(spm.overall_satisfaction), 1) AS avg_satisfaction_score,
        ROUND(AVG(spm.composite_service_score), 1) AS avg_composite_score,
        
        -- Performance distribution
        COUNT(CASE WHEN spm.satisfaction_category = 'Excellent' THEN 1 END) AS excellent_reviews,
        COUNT(CASE WHEN spm.satisfaction_category = 'Good' THEN 1 END) AS good_reviews,
        COUNT(CASE WHEN spm.satisfaction_category IN ('Fair', 'Poor', 'Very Poor') THEN 1 END) AS poor_reviews,
        
        -- Efficiency metrics
        ROUND(AVG(spm.duration_hours), 1) AS avg_service_duration,
        ROUND(AVG(spm.cost_per_hour), 2) AS avg_cost_per_hour,
        ROUND(AVG(spm.total_cost), 2) AS avg_service_value,
        
        -- Quality indicators
        COUNT(CASE WHEN spm.would_recommend = TRUE THEN 1 END) AS recommendations,
        COUNT(CASE WHEN spm.requires_attention = TRUE THEN 1 END) AS issues_requiring_attention,
        ROUND(AVG(spm.overall_performance_score), 1) AS avg_performance_score
        
    FROM {{ ref('stg_hvac__technicians') }} t
    LEFT JOIN {{ ref('int_service_performance_metrics') }} spm 
        ON t.technician_id = spm.technician_id
    GROUP BY 
        t.technician_id, t.technician_name, t.technician_level, 
        t.experience_category, t.hourly_rate, t.rate_category,
        t.avg_skill_level, t.technical_skill_avg, t.customer_service_skill,
        t.years_experience, t.tenure_months
)

SELECT 
    *,
    
    -- Calculate percentage metrics
    CASE 
        WHEN feedback_received > 0 THEN 
            ROUND(excellent_reviews * 100.0 / feedback_received, 1)
        ELSE NULL
    END AS excellent_review_rate,
    
    CASE 
        WHEN feedback_received > 0 THEN 
            ROUND(recommendations * 100.0 / feedback_received, 1)
        ELSE NULL
    END AS recommendation_rate,
    
    CASE 
        WHEN total_service_calls > 0 THEN 
            ROUND(feedback_received * 100.0 / total_service_calls, 1)
        ELSE NULL
    END AS feedback_response_rate,
    
    -- Performance rankings (1 = best)
    ROW_NUMBER() OVER (ORDER BY avg_performance_score DESC) AS performance_rank,
    ROW_NUMBER() OVER (ORDER BY avg_satisfaction_score DESC) AS satisfaction_rank,
    ROW_NUMBER() OVER (ORDER BY avg_cost_per_hour ASC) AS efficiency_rank,
    
    -- Overall grade calculation
    CASE 
        WHEN avg_performance_score >= 8 AND excellent_review_rate >= 60 THEN 'A+'
        WHEN avg_performance_score >= 7 AND excellent_review_rate >= 50 THEN 'A'
        WHEN avg_performance_score >= 6 AND excellent_review_rate >= 40 THEN 'B+'
        WHEN avg_performance_score >= 5 AND excellent_review_rate >= 30 THEN 'B'
        WHEN avg_performance_score >= 4 THEN 'C'
        WHEN avg_performance_score IS NOT NULL THEN 'D'
        ELSE 'No Data'
    END AS performance_grade,
    
    -- Key insights for management
    CASE 
        WHEN total_service_calls = 0 THEN 'No service activity'
        WHEN feedback_response_rate < 30 THEN 'Low customer feedback rate'
        WHEN avg_performance_score >= 8 THEN 'Top performer - consider for mentoring'
        WHEN avg_performance_score <= 4 THEN 'Requires performance improvement'
        WHEN issues_requiring_attention > total_service_calls * 0.2 THEN 'High issue rate - needs attention'
        ELSE 'Steady performer'
    END AS management_insight

FROM technician_performance
ORDER BY avg_performance_score DESC, total_service_calls DESC