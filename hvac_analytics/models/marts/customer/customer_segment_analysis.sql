/*
    Customer Segment Analysis
    
    This model provides comprehensive analysis of customer segments including:
    - Segment performance metrics and KPIs
    - Comparative analysis across segments
    - Actionable insights for marketing and operations
    - Segment health indicators
*/

{{ config(
    materialized='table',
    docs={'node_color': 'orange'}
) }}

WITH segment_metrics AS (
    SELECT 
        segment_name,
        marketing_persona,
        risk_category,
        value_tier,
        
        -- Segment size and composition
        COUNT(*) as customer_count,
        COUNT(*) / SUM(COUNT(*)) OVER()::FLOAT * 100 as segment_percentage,
        
        -- Financial metrics
        SUM(lifetime_value) as total_segment_value,
        AVG(lifetime_value) as avg_lifetime_value,
        MEDIAN(lifetime_value) as median_lifetime_value,
        STDDEV(lifetime_value) as ltv_std_deviation,
        
        -- Service frequency metrics
        AVG(total_service_calls) as avg_service_calls,
        MEDIAN(total_service_calls) as median_service_calls,
        SUM(total_service_calls) as total_segment_service_calls,
        
        -- Recency metrics
        AVG(days_since_last_service) as avg_days_since_service,
        MEDIAN(days_since_last_service) as median_days_since_service,
        COUNT(CASE WHEN days_since_last_service <= 90 THEN 1 END) as active_last_90_days,
        COUNT(CASE WHEN days_since_last_service > 365 THEN 1 END) as inactive_over_year,
        
        -- Satisfaction metrics
        AVG(avg_satisfaction) as segment_satisfaction,
        STDDEV(avg_satisfaction) as satisfaction_consistency,
        AVG(net_promoter_score) as segment_nps,
        
        -- Service behavior patterns
        AVG(emergency_calls) as avg_emergency_calls,
        AVG(maintenance_ratio) as avg_maintenance_ratio,
        COUNT(CASE WHEN emergency_calls > 2 THEN 1 END) as high_emergency_customers,
        COUNT(CASE WHEN maintenance_ratio > 0.5 THEN 1 END) as proactive_customers,
        
        -- Payment behavior
        AVG(avg_payment_delay_days) as avg_payment_delay,
        AVG(late_payment_ratio) as avg_late_payment_ratio,
        COUNT(CASE WHEN late_payment_ratio > 0.2 THEN 1 END) as payment_risk_customers,
        
        -- Customer type distribution
        COUNT(CASE WHEN customer_type = 'residential' THEN 1 END) as residential_customers,
        COUNT(CASE WHEN customer_type = 'commercial' THEN 1 END) as commercial_customers,
        
        -- Anomaly detection
        COUNT(CASE WHEN is_anomaly = TRUE THEN 1 END) as anomaly_customers
        
    FROM {{ ref('customer_segments') }}
    GROUP BY segment_name, marketing_persona, risk_category, value_tier
),

segment_rankings AS (
    SELECT 
        *,
        -- Rank segments by various metrics
        ROW_NUMBER() OVER (ORDER BY avg_lifetime_value DESC) as value_rank,
        ROW_NUMBER() OVER (ORDER BY segment_satisfaction DESC) as satisfaction_rank,
        ROW_NUMBER() OVER (ORDER BY avg_service_calls DESC) as frequency_rank,
        ROW_NUMBER() OVER (ORDER BY avg_days_since_service ASC) as recency_rank,
        
        -- Calculate segment health score (0-100)
        (
            CASE 
                WHEN avg_days_since_service <= 90 THEN 25
                WHEN avg_days_since_service <= 180 THEN 20
                WHEN avg_days_since_service <= 365 THEN 15
                ELSE 5
            END +
            CASE 
                WHEN segment_satisfaction >= 8 THEN 25
                WHEN segment_satisfaction >= 7 THEN 20
                WHEN segment_satisfaction >= 6 THEN 15
                ELSE 5
            END +
            CASE 
                WHEN avg_late_payment_ratio <= 0.1 THEN 25
                WHEN avg_late_payment_ratio <= 0.2 THEN 20
                WHEN avg_late_payment_ratio <= 0.3 THEN 15
                ELSE 5
            END +
            CASE 
                WHEN avg_maintenance_ratio >= 0.4 THEN 25
                WHEN avg_maintenance_ratio >= 0.2 THEN 20
                WHEN avg_maintenance_ratio >= 0.1 THEN 15
                ELSE 5
            END
        ) as segment_health_score
        
    FROM segment_metrics
),

actionable_insights AS (
    SELECT 
        *,
        -- Generate actionable insights
        CASE 
            WHEN segment_name = 'Champions' AND segment_satisfaction < 8 THEN 'Focus on service quality to maintain champion status'
            WHEN segment_name = 'At Risk' THEN 'Implement retention campaign with personalized offers'
            WHEN segment_name = 'New Customers' AND avg_service_calls < 2 THEN 'Develop onboarding program to increase engagement'
            WHEN segment_name = 'Lost Customers' THEN 'Create win-back campaign with special pricing'
            WHEN avg_late_payment_ratio > 0.3 THEN 'Implement payment reminder system and flexible payment plans'
            WHEN avg_emergency_calls > 2 THEN 'Promote preventive maintenance programs'
            ELSE 'Continue current engagement strategy'
        END as primary_recommendation,
        
        -- Marketing channel recommendations
        CASE 
            WHEN value_tier IN ('Platinum', 'Gold') THEN 'Personal account manager, premium communication channels'
            WHEN segment_name = 'New Customers' THEN 'Educational content, welcome series, referral programs'
            WHEN risk_category = 'High Churn Risk' THEN 'Retention emails, loyalty rewards, satisfaction surveys'
            WHEN avg_maintenance_ratio > 0.5 THEN 'Maintenance plan upsells, seasonal reminders'
            ELSE 'Standard marketing automation, newsletter'
        END as marketing_strategy,
        
        -- Operational recommendations
        CASE 
            WHEN high_emergency_customers / customer_count::FLOAT > 0.3 THEN 'Prioritize preventive maintenance education'
            WHEN avg_payment_delay > 30 THEN 'Implement automated payment reminders'
            WHEN segment_satisfaction < 6 THEN 'Quality improvement initiative required'
            ELSE 'Maintain current service standards'
        END as operational_focus
        
    FROM segment_rankings
)

SELECT 
    segment_name,
    marketing_persona,
    risk_category,
    value_tier,
    
    -- Size and composition
    customer_count,
    ROUND(segment_percentage, 1) as segment_percentage,
    residential_customers,
    commercial_customers,
    
    -- Financial performance
    total_segment_value,
    ROUND(avg_lifetime_value, 2) as avg_lifetime_value,
    ROUND(median_lifetime_value, 2) as median_lifetime_value,
    
    -- Service metrics
    ROUND(avg_service_calls, 1) as avg_service_calls,
    total_segment_service_calls,
    ROUND(avg_days_since_service, 0) as avg_days_since_service,
    active_last_90_days,
    inactive_over_year,
    
    -- Quality metrics  
    ROUND(segment_satisfaction, 2) as avg_satisfaction,
    ROUND(segment_nps, 1) as segment_nps,
    ROUND(satisfaction_consistency, 2) as satisfaction_variability,
    
    -- Behavioral insights
    ROUND(avg_emergency_calls, 1) as avg_emergency_calls,
    ROUND(avg_maintenance_ratio * 100, 1) as maintenance_percentage,
    proactive_customers,
    high_emergency_customers,
    
    -- Payment behavior
    ROUND(avg_payment_delay, 1) as avg_payment_delay_days,
    ROUND(avg_late_payment_ratio * 100, 1) as late_payment_percentage,
    payment_risk_customers,
    
    -- Performance rankings
    value_rank,
    satisfaction_rank,
    frequency_rank,
    recency_rank,
    segment_health_score,
    
    -- Insights and recommendations
    primary_recommendation,
    marketing_strategy,
    operational_focus,
    
    -- Special flags
    anomaly_customers,
    CASE 
        WHEN segment_health_score >= 80 THEN 'Excellent'
        WHEN segment_health_score >= 60 THEN 'Good'
        WHEN segment_health_score >= 40 THEN 'Needs Attention'
        ELSE 'Critical'
    END as segment_health_status,
    
    -- Metadata
    CURRENT_TIMESTAMP() as analysis_date
    
FROM actionable_insights
ORDER BY avg_lifetime_value DESC, customer_count DESC