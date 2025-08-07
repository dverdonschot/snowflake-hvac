/*
    Staging model for technicians data
    
    This model cleans and enriches technician data by:
    - Calculating overall skill averages from individual skill scores
    - Creating experience categories for analysis
    - Standardizing technician levels
*/



SELECT 
    technician_id,
    technician_name,
    phone,
    technician_level,
    hourly_rate,
    hire_date,
    years_experience,
    certification_level,
    
    -- Individual skill scores (1-10 scale)
    hvac_installation_skill,
    electrical_skill,
    refrigeration_skill,
    ductwork_skill,
    diagnostics_skill,
    customer_service_skill,
    safety_protocols_skill,
    
    -- Calculated overall skill average
    ROUND(
        (hvac_installation_skill + electrical_skill + refrigeration_skill + 
         ductwork_skill + diagnostics_skill + customer_service_skill + 
         safety_protocols_skill) / 7.0, 
        1
    ) AS avg_skill_level,
    
    -- Experience categories for analysis
    CASE 
        WHEN years_experience <= 2 THEN 'Junior (0-2 years)'
        WHEN years_experience <= 5 THEN 'Mid-level (3-5 years)'
        WHEN years_experience <= 10 THEN 'Senior (6-10 years)'
        ELSE 'Expert (10+ years)'
    END AS experience_category,
    
    -- Rate categories
    CASE 
        WHEN hourly_rate <= 35 THEN 'Entry Level'
        WHEN hourly_rate <= 50 THEN 'Standard'
        WHEN hourly_rate <= 65 THEN 'Premium'
        ELSE 'Expert Level'
    END AS rate_category,
    
    -- Calculate tenure in months
    DATEDIFF('month', hire_date, CURRENT_DATE()) AS tenure_months,
    
    -- Technical skill average (excluding customer service)
    ROUND(
        (hvac_installation_skill + electrical_skill + refrigeration_skill + 
         ductwork_skill + diagnostics_skill + safety_protocols_skill) / 6.0,
        1
    ) AS technical_skill_avg

FROM HVAC_ANALYTICS.dbt_dev.technicians