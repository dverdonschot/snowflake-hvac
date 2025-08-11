/*
    Test that all customers are properly segmented and no customer appears in multiple segments
*/

-- Test: Every customer should have exactly one segment
SELECT 
    customer_id,
    COUNT(*) as segment_count
FROM {{ ref('customer_segments') }}
GROUP BY customer_id
HAVING COUNT(*) != 1