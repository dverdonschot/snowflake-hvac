/*
    Customer Features for ML Segmentation
    
    This model creates comprehensive customer features for machine learning segmentation:
    - RFM Analysis (Recency, Frequency, Monetary)
    - Service patterns and preferences
    - Seasonal behavior analysis
    - Payment behavior metrics
    - Customer satisfaction trends
    - Equipment diversity metrics
*/

{{ config(materialized='view') }}

WITH service_metrics AS (
    SELECT 
        sc.customer_id,
        -- Recency: days since last service
        DATEDIFF('day', MAX(sc.service_date), CURRENT_DATE()) as days_since_last_service,
        
        -- Frequency: total service interactions
        COUNT(DISTINCT sc.service_call_id) as total_service_calls,
        COUNT(DISTINCT sc.service_call_id) / NULLIF(DATEDIFF('month', MIN(sc.service_date), CURRENT_DATE()), 0) as avg_monthly_service_calls,
        
        -- Monetary: customer lifetime value
        SUM(sc.total_cost) as lifetime_value,
        AVG(sc.total_cost) as avg_service_cost,
        MEDIAN(sc.total_cost) as median_service_cost,
        
        -- Service type patterns
        COUNT(CASE WHEN sc.service_type = 'Emergency' THEN 1 END) as emergency_calls,
        COUNT(CASE WHEN sc.service_type = 'Maintenance' THEN 1 END) as maintenance_calls,
        COUNT(CASE WHEN sc.service_type = 'Repair' THEN 1 END) as repair_calls,
        COUNT(CASE WHEN sc.service_type = 'Installation' THEN 1 END) as installation_calls,
        
        -- Service patterns ratios
        COUNT(CASE WHEN sc.service_type = 'Emergency' THEN 1 END) / NULLIF(COUNT(DISTINCT sc.service_call_id), 0)::FLOAT as emergency_ratio,
        COUNT(CASE WHEN sc.service_type = 'Maintenance' THEN 1 END) / NULLIF(COUNT(DISTINCT sc.service_call_id), 0)::FLOAT as maintenance_ratio,
        
        -- Seasonal patterns
        COUNT(CASE WHEN MONTH(sc.service_date) IN (6,7,8) THEN 1 END) as summer_calls,
        COUNT(CASE WHEN MONTH(sc.service_date) IN (12,1,2) THEN 1 END) as winter_calls,
        COUNT(CASE WHEN MONTH(sc.service_date) IN (3,4,5) THEN 1 END) as spring_calls,
        COUNT(CASE WHEN MONTH(sc.service_date) IN (9,10,11) THEN 1 END) as fall_calls,
        
        -- Time-based patterns
        AVG(sc.duration_hours) as avg_service_duration,
        COUNT(CASE WHEN DAYOFWEEK(sc.service_date) IN (1,7) THEN 1 END) as weekend_calls,
        
        -- Cost efficiency metrics
        AVG(sc.total_cost / NULLIF(sc.duration_hours, 0)) as avg_cost_per_hour,
        
        -- Equipment diversity
        COUNT(DISTINCT sc.equipment_id) as unique_equipment_count,
        
        -- Service consistency
        MIN(sc.service_date) as first_service_date,
        MAX(sc.service_date) as last_service_date,
        DATEDIFF('day', MIN(sc.service_date), MAX(sc.service_date)) as customer_lifespan_days
        
    FROM {{ ref('stg_hvac__service_calls') }} sc
    GROUP BY sc.customer_id
),

payment_metrics AS (
    SELECT 
        sc.customer_id,
        COUNT(DISTINCT p.payment_id) as total_payments,
        AVG(DATEDIFF('day', i.issue_date, p.payment_date)) as avg_payment_delay_days,
        COUNT(CASE WHEN DATEDIFF('day', i.issue_date, p.payment_date) > 30 THEN 1 END) as late_payments,
        COUNT(CASE WHEN DATEDIFF('day', i.issue_date, p.payment_date) <= 7 THEN 1 END) as fast_payments,
        SUM(p.amount) as total_payments_amount
    FROM {{ ref('stg_hvac__service_calls') }} sc
    JOIN {{ ref('invoices') }} i ON sc.service_call_id = i.service_call_id
    JOIN {{ ref('payments') }} p ON i.invoice_id = p.invoice_id
    GROUP BY sc.customer_id
),

satisfaction_metrics AS (
    SELECT 
        sc.customer_id,
        COUNT(DISTINCT cf.feedback_id) as feedback_responses,
        AVG(cf.overall_satisfaction) as avg_satisfaction,
        AVG(cf.technician_rating) as avg_technician_rating,
        AVG(cf.timeliness_rating) as avg_timeliness_rating,
        AVG(cf.quality_rating) as avg_quality_rating,
        AVG(cf.value_rating) as avg_value_rating,
        COUNT(CASE WHEN cf.overall_satisfaction >= 9 THEN 1 END) as promoter_responses,
        COUNT(CASE WHEN cf.overall_satisfaction <= 6 THEN 1 END) as detractor_responses,
        COUNT(CASE WHEN cf.would_recommend = TRUE THEN 1 END) as would_recommend_count
    FROM {{ ref('stg_hvac__service_calls') }} sc
    JOIN {{ ref('customer_feedback') }} cf ON sc.service_call_id = cf.service_call_id
    GROUP BY sc.customer_id
)

SELECT 
    c.customer_id,
    c.customer_name,
    c.customer_type,
    c.created_at,
    DATEDIFF('day', c.created_at, CURRENT_DATE()) as customer_age_days,
    
    -- Service metrics
    COALESCE(sm.days_since_last_service, 9999) as days_since_last_service,
    COALESCE(sm.total_service_calls, 0) as total_service_calls,
    COALESCE(sm.avg_monthly_service_calls, 0) as avg_monthly_service_calls,
    COALESCE(sm.lifetime_value, 0) as lifetime_value,
    COALESCE(sm.avg_service_cost, 0) as avg_service_cost,
    COALESCE(sm.median_service_cost, 0) as median_service_cost,
    
    -- Service type patterns
    COALESCE(sm.emergency_calls, 0) as emergency_calls,
    COALESCE(sm.maintenance_calls, 0) as maintenance_calls,
    COALESCE(sm.repair_calls, 0) as repair_calls,
    COALESCE(sm.installation_calls, 0) as installation_calls,
    COALESCE(sm.emergency_ratio, 0) as emergency_ratio,
    COALESCE(sm.maintenance_ratio, 0) as maintenance_ratio,
    
    -- Seasonal patterns
    COALESCE(sm.summer_calls, 0) as summer_calls,
    COALESCE(sm.winter_calls, 0) as winter_calls,
    COALESCE(sm.spring_calls, 0) as spring_calls,
    COALESCE(sm.fall_calls, 0) as fall_calls,
    
    -- Time patterns
    COALESCE(sm.avg_service_duration, 0) as avg_service_duration,
    COALESCE(sm.weekend_calls, 0) as weekend_calls,
    COALESCE(sm.avg_cost_per_hour, 0) as avg_cost_per_hour,
    
    -- Equipment diversity
    COALESCE(sm.unique_equipment_count, 0) as unique_equipment_count,
    COALESCE(sm.customer_lifespan_days, 0) as customer_lifespan_days,
    
    -- Payment behavior
    COALESCE(pm.avg_payment_delay_days, 0) as avg_payment_delay_days,
    COALESCE(pm.late_payments, 0) as late_payments,
    COALESCE(pm.fast_payments, 0) as fast_payments,
    CASE 
        WHEN pm.total_payments > 0 THEN LEAST(1.0, pm.late_payments / pm.total_payments::FLOAT)
        ELSE 0 
    END as late_payment_ratio,
    
    -- Satisfaction metrics
    COALESCE(sat.avg_satisfaction, 5.0) as avg_satisfaction,
    COALESCE(sat.avg_technician_rating, 5.0) as avg_technician_rating,
    COALESCE(sat.avg_timeliness_rating, 5.0) as avg_timeliness_rating,
    COALESCE(sat.avg_quality_rating, 5.0) as avg_quality_rating,
    COALESCE(sat.avg_value_rating, 5.0) as avg_value_rating,
    COALESCE(sat.feedback_responses, 0) as feedback_responses,
    CASE 
        WHEN sat.feedback_responses > 0 
        THEN (sat.promoter_responses - sat.detractor_responses) / sat.feedback_responses::FLOAT * 100
        ELSE 0 
    END as net_promoter_score,
    
    -- Derived business metrics
    CASE 
        WHEN sm.total_service_calls IS NULL OR sm.total_service_calls = 0 THEN 'New Customer'
        WHEN sm.days_since_last_service > 365 THEN 'Inactive'
        WHEN sm.lifetime_value > 5000 AND sat.avg_satisfaction > 8 THEN 'VIP'
        WHEN sm.emergency_calls > 2 AND sat.avg_satisfaction < 6 THEN 'High Maintenance'
        WHEN sm.maintenance_ratio > 0.5 THEN 'Proactive'
        ELSE 'Standard'
    END as preliminary_segment

FROM {{ ref('customers') }} c
LEFT JOIN service_metrics sm ON c.customer_id = sm.customer_id
LEFT JOIN payment_metrics pm ON c.customer_id = pm.customer_id  
LEFT JOIN satisfaction_metrics sat ON c.customer_id = sat.customer_id