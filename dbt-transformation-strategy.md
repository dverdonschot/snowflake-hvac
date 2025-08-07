# DBT Transformation Strategy for HVAC Analytics

## Overview

Now that we have 19 tables with 14,964 records of rich HVAC business data, let's build a layered dbt transformation architecture that follows best practices and delivers business value.

## DBT Architecture Layers

### 1. **Staging Layer** (`staging/`)
- **Purpose**: Clean, standardize, and document raw data
- **Materialization**: Views (no cost, fresh data)
- **Naming**: `stg_<source>__<table_name>`

### 2. **Intermediate Layer** (`intermediate/`)
- **Purpose**: Business logic, joins, aggregations
- **Materialization**: Views (complex logic, reusable)
- **Naming**: `int_<business_concept>`

### 3. **Marts Layer** (`marts/`)
- **Purpose**: Final analytics-ready tables
- **Materialization**: Tables (fast queries, dashboards)
- **Naming**: `<business_area>_<entity>`

## Small Example: Customer Service Performance Analysis

Let's start with a **Customer Service Performance Dashboard** - a common analytics need that touches multiple tables and demonstrates key dbt concepts.

### Business Question
*"How effective are our technicians at resolving customer issues, and what drives customer satisfaction?"*

### Data Flow Example

```
Raw Seeds (19 tables)
         ↓
Staging Layer (3 models)
  ├── stg_hvac__service_calls
  ├── stg_hvac__technicians  
  └── stg_hvac__customer_feedback
         ↓
Intermediate Layer (2 models)
  ├── int_service_performance_metrics
  └── int_technician_skill_analysis
         ↓
Marts Layer (1 model)
  └── service_performance_dashboard
```

## Implementation Plan

### Phase 1: Basic Staging Models (Start Here)

#### **stg_hvac__service_calls**
Clean and standardize service call data:
```sql
-- Clean service calls with calculated fields
SELECT 
    service_call_id,
    customer_id,
    technician_id,
    service_date,
    service_type,
    duration_hours,
    labor_cost,
    parts_cost,
    total_cost,
    -- Calculated fields
    ROUND(total_cost / duration_hours, 2) AS cost_per_hour,
    CASE 
        WHEN service_type = 'Emergency' THEN 'High Priority'
        WHEN service_type = 'Repair' THEN 'Medium Priority'
        ELSE 'Standard'
    END AS priority_level
FROM {{ ref('service_calls') }}
WHERE total_cost > 0  -- Remove $0 warranty calls
```

#### **stg_hvac__technicians**
Clean technician data with skill averages:
```sql
-- Technicians with skill analysis
SELECT 
    technician_id,
    technician_name,
    technician_level,
    years_experience,
    hourly_rate,
    hire_date,
    -- Calculate average skill level
    ROUND((hvac_installation_skill + electrical_skill + refrigeration_skill + 
           ductwork_skill + diagnostics_skill + customer_service_skill + 
           safety_protocols_skill) / 7.0, 1) AS avg_skill_level
FROM {{ ref('technicians') }}
```

#### **stg_hvac__customer_feedback**
Standardize feedback scores:
```sql
-- Customer feedback with standardized scoring
SELECT 
    feedback_id,
    service_call_id,
    customer_id,
    feedback_date,
    overall_satisfaction,
    technician_rating,
    -- Create satisfaction categories
    CASE 
        WHEN overall_satisfaction >= 8 THEN 'Excellent'
        WHEN overall_satisfaction >= 6 THEN 'Good'
        WHEN overall_satisfaction >= 4 THEN 'Fair'
        ELSE 'Poor'
    END AS satisfaction_category,
    would_recommend
FROM {{ ref('customer_feedback') }}
```

### Phase 2: Intermediate Business Logic

#### **int_service_performance_metrics**
Combine service calls with feedback:
```sql
-- Service performance with customer satisfaction
SELECT 
    sc.service_call_id,
    sc.technician_id,
    sc.customer_id,
    sc.service_date,
    sc.service_type,
    sc.duration_hours,
    sc.total_cost,
    sc.cost_per_hour,
    cf.overall_satisfaction,
    cf.satisfaction_category,
    cf.would_recommend,
    -- Performance indicators
    CASE WHEN sc.duration_hours <= 2 THEN 'Fast' ELSE 'Slow' END AS service_speed,
    CASE WHEN sc.cost_per_hour <= 100 THEN 'Efficient' ELSE 'Expensive' END AS cost_efficiency
FROM {{ ref('stg_hvac__service_calls') }} sc
LEFT JOIN {{ ref('stg_hvac__customer_feedback') }} cf 
    ON sc.service_call_id = cf.service_call_id
```

### Phase 3: Final Analytics Mart

#### **service_performance_dashboard**
Executive dashboard table:
```sql
-- Technician performance summary for dashboards
SELECT 
    t.technician_id,
    t.technician_name,
    t.technician_level,
    t.avg_skill_level,
    COUNT(spm.service_call_id) AS total_service_calls,
    ROUND(AVG(spm.overall_satisfaction), 1) AS avg_satisfaction,
    ROUND(AVG(spm.duration_hours), 1) AS avg_service_time,
    ROUND(AVG(spm.cost_per_hour), 2) AS avg_cost_per_hour,
    COUNT(CASE WHEN spm.satisfaction_category = 'Excellent' THEN 1 END) AS excellent_reviews,
    ROUND(COUNT(CASE WHEN spm.would_recommend THEN 1 END) * 100.0 / COUNT(*), 1) AS recommendation_rate,
    -- Performance score (1-10)
    ROUND(
        (AVG(spm.overall_satisfaction) + 
         (10 - AVG(spm.duration_hours)) + 
         (200 - AVG(spm.cost_per_hour)) / 20) / 3, 1
    ) AS performance_score
FROM {{ ref('stg_hvac__technicians') }} t
LEFT JOIN {{ ref('int_service_performance_metrics') }} spm 
    ON t.technician_id = spm.technician_id
GROUP BY t.technician_id, t.technician_name, t.technician_level, t.avg_skill_level
ORDER BY performance_score DESC
```

## Key Learning Concepts in This Example

### 1. **dbt Jinja & Macros**
- `{{ ref('table_name') }}` - References other dbt models
- Automatic dependency management

### 2. **Data Quality**
- Filter out $0 warranty calls in staging
- Handle NULLs with LEFT JOINs

### 3. **Business Logic**
- Calculate KPIs (cost per hour, satisfaction rates)
- Create categories from continuous values
- Performance scoring algorithms

### 4. **Layered Architecture**
- Raw data → Clean data → Business logic → Analytics
- Each layer builds on the previous
- Reusable intermediate models

## Next Phase Ideas (After Tutorial)

### Customer Analytics
- `customer_lifetime_value`
- `customer_churn_analysis` 
- `geographic_service_analysis`

### Financial Analytics
- `revenue_analysis_monthly`
- `parts_profitability`
- `technician_utilization`

### Operational Analytics
- `equipment_failure_patterns`
- `inventory_optimization`
- `emergency_response_times`

## Getting Started Command

```bash
# Create the directory structure
mkdir -p models/staging/hvac
mkdir -p models/intermediate
mkdir -p models/marts/service

# Start with the first staging model
# We'll create stg_hvac__service_calls.sql first
```

## Success Metrics

After implementing this example, you'll have:
- ✅ Clean, documented staging models
- ✅ Reusable business logic in intermediate layer  
- ✅ Analytics-ready mart for dashboards
- ✅ Understanding of dbt best practices
- ✅ Foundation for expanding to more complex models

Ready to implement the first staging model?