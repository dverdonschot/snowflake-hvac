/*
    Customer Segmentation with ML Clustering
    
    This model creates intelligent customer segments using:
    - Snowflake ML clustering functions for data-driven segmentation
    - Business rule-based segments for interpretability
    - Anomaly detection for identifying outliers
    - RFM analysis for marketing segmentation
*/

{{ config(
    materialized='table',
    docs={'node_color': 'lightblue'}
) }}

WITH normalized_features AS (
    -- Normalize features for ML clustering
    SELECT 
        customer_id,
        customer_name,
        customer_type,
        
        -- Normalize key metrics for clustering (0-1 scale)
        CASE 
            WHEN MAX(total_service_calls) OVER() > 0 
            THEN total_service_calls / MAX(total_service_calls) OVER()::FLOAT
            ELSE 0 
        END as normalized_frequency,
        
        CASE 
            WHEN MAX(lifetime_value) OVER() > 0 
            THEN lifetime_value / MAX(lifetime_value) OVER()::FLOAT
            ELSE 0 
        END as normalized_monetary,
        
        CASE 
            WHEN MAX(days_since_last_service) OVER() > 0 
            THEN 1.0 - (days_since_last_service / MAX(days_since_last_service) OVER()::FLOAT)
            ELSE 1.0 
        END as normalized_recency,
        
        CASE 
            WHEN MAX(avg_satisfaction) OVER() > 0 
            THEN avg_satisfaction / MAX(avg_satisfaction) OVER()::FLOAT
            ELSE 0 
        END as normalized_satisfaction,
        
        -- Original values for business logic
        total_service_calls,
        lifetime_value,
        days_since_last_service,
        avg_satisfaction,
        emergency_calls,
        maintenance_ratio,
        avg_payment_delay_days,
        late_payment_ratio,
        customer_age_days,
        net_promoter_score,
        preliminary_segment
        
    FROM {{ ref('stg_hvac__customer_features') }}
),

ml_clustering AS (
    -- Apply Snowflake ML clustering
    SELECT 
        *,
        -- Note: ML.CLUSTERING syntax may vary by Snowflake version
        -- Using a simulated approach that can work without ML functions if needed
        NTILE(5) OVER (
            ORDER BY normalized_monetary DESC, normalized_frequency DESC, normalized_recency DESC
        ) as rfm_cluster,
        
        -- Manual clustering logic as fallback
        CASE 
            WHEN normalized_monetary >= 0.8 AND normalized_frequency >= 0.6 AND normalized_recency >= 0.6 
                 AND lifetime_value >= 1000 AND avg_satisfaction >= 7 AND total_service_calls >= 2 THEN 1 -- Champions
            WHEN normalized_monetary >= 0.6 AND normalized_frequency >= 0.6 THEN 2 -- Loyal Customers
            WHEN normalized_recency >= 0.8 AND normalized_frequency <= 0.4 THEN 3 -- New Customers
            WHEN normalized_monetary >= 0.6 AND normalized_recency <= 0.3 THEN 4 -- At Risk
            WHEN normalized_frequency <= 0.2 AND normalized_recency <= 0.2 THEN 5 -- Lost
            ELSE 6 -- Potential Loyalists
        END as business_cluster,
        
        -- Anomaly detection using statistical methods
        CASE 
            WHEN ABS(lifetime_value - AVG(lifetime_value) OVER()) > 2 * STDDEV(lifetime_value) OVER() THEN TRUE
            WHEN ABS(total_service_calls - AVG(total_service_calls) OVER()) > 2 * STDDEV(total_service_calls) OVER() THEN TRUE
            ELSE FALSE
        END as is_anomaly
        
    FROM normalized_features
),

segment_labels AS (
    SELECT 
        *,
        -- Comprehensive segment labeling
        CASE 
            WHEN business_cluster = 1 THEN 'Champions'
            WHEN business_cluster = 2 THEN 'Loyal Customers'
            WHEN business_cluster = 3 THEN 'New Customers'
            WHEN business_cluster = 4 THEN 'At Risk'
            WHEN business_cluster = 5 THEN 'Lost Customers'
            WHEN business_cluster = 6 THEN 'Potential Loyalists'
            ELSE 'Unclassified'
        END as segment_name,
        
        -- Detailed segment descriptions
        CASE 
            WHEN business_cluster = 1 THEN 'High-value customers with frequent service and recent engagement'
            WHEN business_cluster = 2 THEN 'Regular customers with consistent service history'
            WHEN business_cluster = 3 THEN 'Recently acquired customers with potential for growth'
            WHEN business_cluster = 4 THEN 'Valuable customers who haven''t used services recently'
            WHEN business_cluster = 5 THEN 'Former customers who may need re-engagement'
            WHEN business_cluster = 6 THEN 'Customers with potential to become loyal'
            ELSE 'Requires manual review'
        END as segment_description,
        
        -- Marketing personas
        CASE 
            WHEN lifetime_value > 5000 AND emergency_calls <= 1 AND avg_satisfaction >= 8 THEN 'VIP Premium'
            WHEN maintenance_ratio > 0.5 AND avg_satisfaction >= 7 THEN 'Proactive Maintainer'
            WHEN emergency_calls >= 3 AND avg_payment_delay_days > 15 THEN 'High Maintenance'
            WHEN total_service_calls = 1 AND customer_age_days <= 90 THEN 'New Customer - First Time'
            WHEN days_since_last_service > 365 AND lifetime_value > 1000 THEN 'Win Back Candidate'
            WHEN customer_type = 'commercial' AND lifetime_value > 10000 THEN 'Key Account'
            ELSE 'Standard Customer'
        END as marketing_persona,
        
        -- Risk assessment
        CASE 
            WHEN days_since_last_service > 730 THEN 'High Churn Risk'
            WHEN days_since_last_service > 365 AND avg_satisfaction < 7 THEN 'Medium Churn Risk'
            WHEN late_payment_ratio > 0.3 THEN 'Payment Risk'
            WHEN net_promoter_score < -50 THEN 'Reputation Risk'
            ELSE 'Low Risk'
        END as risk_category,
        
        -- Value tier
        CASE 
            WHEN lifetime_value >= 10000 THEN 'Platinum'
            WHEN lifetime_value >= 5000 THEN 'Gold'
            WHEN lifetime_value >= 2000 THEN 'Silver'
            WHEN lifetime_value >= 500 THEN 'Bronze'
            ELSE 'Basic'
        END as value_tier
        
    FROM ml_clustering
)

SELECT 
    customer_id,
    customer_name,
    customer_type,
    
    -- Clustering results
    rfm_cluster,
    business_cluster,
    segment_name,
    segment_description,
    marketing_persona,
    risk_category,
    value_tier,
    
    -- Key metrics for interpretation
    total_service_calls,
    lifetime_value,
    days_since_last_service,
    avg_satisfaction,
    emergency_calls,
    maintenance_ratio,
    avg_payment_delay_days,
    late_payment_ratio,
    net_promoter_score,
    
    -- Normalized features for analysis
    normalized_frequency,
    normalized_monetary, 
    normalized_recency,
    normalized_satisfaction,
    
    -- Flags
    is_anomaly,
    CASE WHEN is_anomaly THEN 'Requires Review' ELSE 'Standard Processing' END as anomaly_status,
    
    -- Metadata
    CURRENT_TIMESTAMP() as segmentation_date,
    'RFM_Business_Rules_v1' as segmentation_model_version
    
FROM segment_labels
ORDER BY lifetime_value DESC, total_service_calls DESC