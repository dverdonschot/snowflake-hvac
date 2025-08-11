/*
    Test that customer features are complete and reasonable
*/

-- Test: No customers should have impossible feature combinations
SELECT 
    customer_id,
    total_service_calls,
    lifetime_value,
    days_since_last_service
FROM {{ ref('stg_hvac__customer_features') }}
WHERE 
    -- Customers with service calls should have positive lifetime value
    (total_service_calls > 0 AND lifetime_value <= 0) OR
    -- Customers with no service calls shouldn't have recent service dates
    (total_service_calls = 0 AND days_since_last_service < 9999) OR
    -- Negative values that shouldn't exist
    total_service_calls < 0 OR
    lifetime_value < 0