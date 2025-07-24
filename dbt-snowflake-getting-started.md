# DBT + Snowflake Getting Started Guide
## HVAC Service Company Data Model

### Overview
This guide will help you set up DBT with Snowflake to model an HVAC service company with limited costs (targeting <$7 budget over 21 days).

### HVAC Company Data Model Design

#### Core Business Entities
- **Customers**: Residential/commercial clients with HVAC systems
- **Service Technicians**: Field staff performing installations/repairs
- **Equipment**: HVAC units, parts inventory
- **Service Orders**: Work requests and scheduling
- **Parts Orders**: Inventory procurement
- **Invoicing**: Billing and payments

#### Dimensional Model Structure
```
Fact Tables:
- fact_service_calls (grain: one row per service visit)
- fact_parts_usage (grain: one row per part used)
- fact_sales (grain: one row per equipment sale)

Dimension Tables:
- dim_customers
- dim_technicians  
- dim_equipment_types
- dim_parts
- dim_dates
- dim_service_types
```

### Cost-Efficient Fake Data Generation

#### Data Volume Strategy (Budget: <$7)
- **Customers**: 500 records
- **Service Calls**: 2,000 records (past 12 months)
- **Parts**: 200 different parts
- **Technicians**: 15 staff members
- **Equipment**: 50 different models

#### Fake Data Tools
1. **Python with Faker library**:
```python
pip install faker
# Generate CSV files locally, then upload to Snowflake
```

2. **Snowflake SQL Generator Functions**:
```sql
-- Use RANDOM(), UNIFORM(), DATEADD() for synthetic data
SELECT 
  UNIFORM(1, 500, RANDOM()) as customer_id,
  DATEADD(day, -UNIFORM(1, 365, RANDOM()), CURRENT_DATE) as service_date
```

3. **Sample Data Generation Script**:
```python
from faker import Faker
import csv
import random
from datetime import datetime, timedelta

fake = Faker()

# Generate customers (500 records)
customers = []
for i in range(500):
    customers.append({
        'customer_id': i+1,
        'name': fake.company(),
        'address': fake.address(),
        'phone': fake.phone_number(),
        'customer_type': random.choice(['residential', 'commercial'])
    })
```

### Snowflake + DBT Connection Setup

#### 1. Snowflake Setup
```sql
-- Create database and warehouse (run in Snowflake console)
CREATE DATABASE hvac_analytics;
CREATE WAREHOUSE dbt_wh WITH 
  WAREHOUSE_SIZE = 'X-SMALL'  -- Cheapest option
  AUTO_SUSPEND = 60           -- Suspend after 1 minute
  AUTO_RESUME = TRUE;

-- Create user for DBT
CREATE USER dbt_user 
  PASSWORD = 'your_secure_password'
  DEFAULT_WAREHOUSE = dbt_wh;

GRANT ROLE SYSADMIN TO USER dbt_user;
```

#### 2. DBT Installation & Setup
```bash
# Install DBT with Snowflake adapter
pip install dbt-snowflake

# Initialize DBT project
dbt init hvac_analytics
cd hvac_analytics
```

#### 3. DBT Profile Configuration
Create `~/.dbt/profiles.yml`:
```yaml
hvac_analytics:
  outputs:
    dev:
      type: snowflake
      account: your_account_identifier  # Found in Snowflake URL
      user: dbt_user
      password: your_secure_password
      role: SYSADMIN
      database: hvac_analytics
      warehouse: dbt_wh
      schema: dbt_dev
      threads: 2
      keepalives_idle: 240
  target: dev
```

#### 4. Test Connection
```bash
dbt debug
```

### Project Structure
```
hvac_analytics/
├── dbt_project.yml
├── models/
│   ├── staging/
│   │   ├── stg_customers.sql
│   │   ├── stg_service_calls.sql
│   │   └── stg_parts.sql
│   ├── intermediate/
│   │   └── int_customer_metrics.sql
│   └── marts/
│       ├── dim_customers.sql
│       ├── dim_technicians.sql
│       └── fact_service_calls.sql
├── seeds/
│   ├── customers.csv
│   ├── technicians.csv
│   └── parts.csv
└── tests/
```

### CI/CD with GitHub Actions

#### 1. Repository Setup
```bash
git init
git add .
git commit -m "Initial DBT project"
git remote add origin your_repo_url
git push -u origin main
```

#### 2. GitHub Actions Workflow
Create `.github/workflows/dbt.yml`:
```yaml
name: DBT Pipeline

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
          
      - name: Install dependencies
        run: |
          pip install dbt-snowflake
          
      - name: DBT Test
        env:
          DBT_PROFILES_DIR: .
        run: |
          echo "${{ secrets.DBT_PROFILES_YML }}" > profiles.yml
          dbt deps
          dbt test
          
  deploy:
    needs: test
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
          
      - name: Install dependencies
        run: pip install dbt-snowflake
        
      - name: DBT Run
        env:
          DBT_PROFILES_DIR: .
        run: |
          echo "${{ secrets.DBT_PROFILES_YML }}" > profiles.yml
          dbt deps
          dbt run
```

#### 3. GitHub Secrets Setup
Add these secrets to your GitHub repository:
- `DBT_PROFILES_YML`: Your profiles.yml content

### Cost Management Tips

1. **Use X-SMALL warehouse** (cheapest compute)
2. **Auto-suspend after 60 seconds**
3. **Limit data volume** during development
4. **Use SAMPLE() function** for testing:
   ```sql
   SELECT * FROM large_table SAMPLE (10 ROWS)
   ```
5. **Monitor usage** in Snowflake console

### Sample DBT Models

#### staging/stg_customers.sql
```sql
SELECT 
    customer_id,
    customer_name,
    address,
    phone,
    customer_type,
    created_at
FROM {{ ref('customers') }}
```

#### marts/fact_service_calls.sql
```sql
SELECT 
    sc.service_call_id,
    sc.customer_id,
    sc.technician_id,
    sc.service_date,
    sc.service_type,
    sc.duration_hours,
    sc.total_cost,
    c.customer_type,
    t.technician_level
FROM {{ ref('stg_service_calls') }} sc
LEFT JOIN {{ ref('dim_customers') }} c ON sc.customer_id = c.customer_id  
LEFT JOIN {{ ref('dim_technicians') }} t ON sc.technician_id = t.technician_id
```

### Next Steps

1. Generate and upload seed data to Snowflake
2. Create basic staging models
3. Build dimensional models
4. Add data tests
5. Set up CI/CD pipeline
6. Create dashboards in your BI tool of choice

### Budget Monitoring
- Check Snowflake usage daily in Account → Billing
- Target: <$0.33/day to stay under $7 total
- Suspend warehouse when not in use