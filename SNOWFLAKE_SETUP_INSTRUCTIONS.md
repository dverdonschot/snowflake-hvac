# Snowflake Setup Instructions for HVAC Analytics

## Prerequisites
- Snowflake account (trial or paid)
- dbt-snowflake installed: `pip install dbt-snowflake`

## Step 1: Snowflake Database Setup

Log into your Snowflake console and run these SQL commands:

```sql
-- Create database and warehouse
CREATE DATABASE hvac_analytics;
CREATE WAREHOUSE dbt_wh WITH 
  WAREHOUSE_SIZE = 'X-SMALL'  -- Cheapest option
  AUTO_SUSPEND = 60           -- Suspend after 1 minute
  AUTO_RESUME = TRUE;

-- Create user for dbt (replace with secure password)
CREATE USER dbt_user 
  PASSWORD = 'YourSecurePassword123!'
  DEFAULT_WAREHOUSE = dbt_wh;

-- Grant permissions
GRANT ROLE SYSADMIN TO USER dbt_user;
GRANT USAGE ON WAREHOUSE dbt_wh TO USER dbt_user;
GRANT ALL ON DATABASE hvac_analytics TO USER dbt_user;
```

## Step 2: dbt Profile Configuration

Create or update `~/.dbt/profiles.yml`:

```yaml
hvac_analytics:
  outputs:
    dev:
      type: snowflake
      account: YOUR_ACCOUNT_IDENTIFIER  # Found in Snowflake URL (e.g., abc12345.us-east-1)
      user: dbt_user
      password: YourSecurePassword123!
      role: SYSADMIN
      database: hvac_analytics
      warehouse: dbt_wh
      schema: dbt_dev
      threads: 2
      keepalives_idle: 240
  target: dev
```

**To find your account identifier:**
- Look at your Snowflake URL: `https://YOUR_ACCOUNT_IDENTIFIER.snowflakecomputing.com`
- Use everything before `.snowflakecomputing.com`

## Step 3: Test Connection

```bash
cd hvac_analytics
dbt debug
```

You should see: ✅ Connection test: [OK connection ok]

## Step 4: Load Data to Snowflake

```bash
# Load all CSV files as tables
dbt seed

# Verify data loaded
dbt run
```

## Step 5: Verify Data in Snowflake

Run in Snowflake console:

```sql
USE DATABASE hvac_analytics;
USE SCHEMA dbt_dev;

-- Check row counts
SELECT 'customers' as table_name, COUNT(*) as rows FROM customers
UNION ALL
SELECT 'technicians', COUNT(*) FROM technicians
UNION ALL
SELECT 'equipment_types', COUNT(*) FROM equipment_types
UNION ALL
SELECT 'parts', COUNT(*) FROM parts
UNION ALL
SELECT 'service_calls', COUNT(*) FROM service_calls
UNION ALL
SELECT 'parts_usage', COUNT(*) FROM parts_usage;
```

Expected row counts:
- customers: ~500
- technicians: ~16
- equipment_types: ~51
- parts: ~201
- service_calls: ~2,001
- parts_usage: ~4,099

## Cost Management

- **Estimated cost**: $0.05-0.15 per dbt seed run
- **Budget tracking**: Check Account → Billing in Snowflake console
- **Auto-suspend**: Warehouse suspends after 60 seconds of inactivity
- **Target**: Stay under $7 total over 21 days

## Troubleshooting

### Connection Issues
- Verify account identifier format
- Check username/password
- Ensure user has SYSADMIN role

### Seed Issues
- Check CSV file formats in `seeds/` directory
- Verify column types in `dbt_project.yml`
- Run `dbt seed --full-refresh` to reload data

### Next Steps
Once data is loaded, you can:
1. Create staging models in `models/staging/`
2. Build dimensional models in `models/marts/`
3. Add data tests
4. Set up dashboards