/*
    Test logical consistency of segment assignments
    Champions should have high lifetime value and good satisfaction
*/

-- Test: Champions should meet minimum criteria
SELECT 
    customer_id,
    segment_name,
    lifetime_value,
    avg_satisfaction,
    total_service_calls
FROM {{ ref('customer_segments') }}
WHERE segment_name = 'Champions'
  AND (
    lifetime_value < 1000 OR 
    avg_satisfaction < 7 OR
    total_service_calls < 2
  )