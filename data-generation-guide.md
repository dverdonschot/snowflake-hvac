# HVAC Analytics Data Generation Guide

## Overview

This guide provides comprehensive documentation for the HVAC Service Company data generation system, which creates realistic synthetic training data for dbt + Snowflake analytics. The system generates **20+ interconnected datasets** representing a complete HVAC business operation while maintaining strict cost controls (<$7 budget over 21 days on Snowflake).

## Project Structure

```
snowflake-hvac/
├── data_generation/           # Primary data generation folder
│   ├── main.py               # Main generation script
│   ├── *.csv                 # Generated CSV files (20+ tables)
├── hvac_analytics/           # dbt project folder
│   ├── dbt_project.yml       # dbt configuration
│   ├── models/               # SQL transformation models
│   ├── seeds/                # CSV seed files for dbt
│   └── logs/                 # dbt execution logs
├── CLAUDE.md                 # Project instructions
├── devenv.nix               # Nix development environment
└── pyproject.toml           # Python dependencies
```

## Development Environment Setup

### Prerequisites

The project uses **devenv** (Nix-based) for reproducible development environments with:
- Python 3.12
- uv package manager
- Node.js 22
- Claude Code CLI (auto-installed)

### Activation Commands

```bash
# Activate development environment (required first step)
devenv shell

# Install dependencies (if needed)
uv sync
```

## Data Generation Process

### Primary Command

```bash
# Navigate to data generation folder
cd data_generation

# Generate all training data (main command)
python main.py
```

### Generation Script Details (`main.py`)

The data generation script (`/home/ewt/snowflake-hvac/data_generation/main.py`) creates **24,479 total records** across 19 interconnected tables:

#### Core Business Entities (Lines 8-176)

1. **Customers** (500 records) - `generate_customers()`
   - Residential and commercial clients
   - Realistic names, addresses, phone numbers using Faker library
   - Customer type classification and creation timestamps

2. **Technicians** (15 records) - `generate_technicians()`
   - Service staff with detailed skill matrices
   - Skill levels: junior, senior, lead, specialist
   - 7 skill categories: HVAC Installation, Electrical, Refrigeration, etc.
   - Hourly rates, experience years, certification levels

3. **Equipment Types** (50 records) - `generate_equipment_types()`
   - HVAC units from major brands (Carrier, Trane, Lennox, etc.)
   - Equipment types: Central AC, Heat Pump, Furnace, etc.
   - BTU ratings and energy efficiency ratings

4. **Parts** (200 records) - `generate_parts()`
   - Inventory components with categories
   - Part types: Filter, Compressor, Coil, Fan Motor, etc.
   - Unit costs and supplier information

#### Extended Business Operations (Lines 103-663)

5. **Installed Devices** (800 records) - `generate_installed_devices()`
   - Customer equipment tracking with warranties
   - Installation dates, warranty periods, serial numbers
   - Status tracking and maintenance history

6. **Service Calls** (2000 records) - `generate_service_calls()`
   - Primary work orders with service types
   - Duration tracking, cost calculations (labor + parts)
   - Service types: Maintenance, Repair, Installation, Emergency, Inspection

7. **Parts Usage** (3990 records) - `generate_parts_usage()`
   - Junction table linking service calls to parts consumed
   - Quantity tracking and usage dates

8. **Incident Response** (150 records) - `generate_incident_response()`
   - Emergency call management system
   - Response times, severity levels, resolution tracking
   - Incident types: No Heat/AC, Gas Leak, System Failure, etc.

#### Business Support Systems (Lines 215-663)

9. **Vehicle Fleet** (20 records) - `generate_vehicle_fleet()`
   - Company vehicle tracking
   - Vehicle assignments, maintenance schedules, GPS tracking

10. **Mailing List** (800 records) - `generate_mailing_list()`
    - Marketing contacts including existing customers + prospects
    - Email subscriptions, contact preferences, interests

11. **Subscriptions** (250 records) - `generate_subscriptions()`
    - Service contracts and maintenance plans
    - Plan types: Basic, Premium, Comprehensive, Commercial
    - Auto-renewal tracking and service schedules

12. **Invoices** (1700 records) - `generate_invoices()`
    - Billing records with tax calculations
    - Payment terms, status tracking, markup calculations

13. **Payments** (655 records) - `generate_payments()`
    - Payment processing with multiple methods
    - Transaction IDs, processing fees, partial payments

14. **Inventory** (200 records) - `generate_inventory()`
    - Stock level management with ABC classification
    - Reorder points, warehouse locations, usage tracking

15. **Appointments** (2000 records) - `generate_appointments()`
    - Scheduling system with future bookings
    - Historical appointments linked to service calls
    - Future appointments for pipeline tracking

16. **Quotes** (680 records) - `generate_quotes()`
    - Sales estimates and proposals
    - Labor calculations, equipment costs, total amounts
    - Quote tracking and follow-up management

17. **Work Orders** (2000 records) - `generate_work_orders()`
    - Detailed work instructions for service calls
    - Safety requirements, completion notes, priority levels

18. **Customer Feedback** (800 records) - `generate_customer_feedback()`
    - Satisfaction surveys and reviews
    - Rating systems (1-10 scale) for multiple categories
    - Comment collection and follow-up tracking

19. **Leads** (600 records) - `generate_leads()`
    - Sales pipeline management
    - Lead sources, conversion tracking, probability scoring

## Data Architecture Patterns

### Referential Integrity
- All foreign keys properly maintained across tables
- Customer IDs link across customers → service_calls → invoices → payments
- Equipment IDs connect equipment_types → installed_devices → service_calls
- Technician assignments maintained across multiple tables

### Temporal Consistency
- Realistic date relationships (appointments precede service calls)
- Service dates occur after customer creation dates
- Warranty periods calculated from installation dates
- Payment dates fall within reasonable ranges after invoice dates

### Business Logic Implementation
- Cost calculations include labor rates, markup, taxes
- Skill-based technician assignment probabilities
- Emergency calls have faster response times
- Inventory reorder points and ABC classification

### Data Quality Standards
- Industry-appropriate values (HVAC brands, part types, service classifications)
- Realistic geographic and demographic data via Faker library
- Proper data type handling for dates, decimals, booleans

## Generated Output

### File Structure
Running `python main.py` creates **19 CSV files** in the `data_generation/` directory:

**Core Files:**
- `customers.csv` (500 records)
- `technicians.csv` (15 records) 
- `equipment_types.csv` (50 records)
- `parts.csv` (200 records)
- `service_calls.csv` (2000 records)
- `parts_usage.csv` (3990 records)

**Operations Files:**
- `invoices.csv` (1700 records)
- `payments.csv` (655 records)
- `appointments.csv` (2000 records)
- `work_orders.csv` (2000 records)
- `inventory.csv` (200 records)

**Analytics Files:**
- `customer_feedback.csv` (800 records)
- `incident_response.csv` (150 records)
- `quotes.csv` (680 records)
- `leads.csv` (600 records)

**Support Files:**
- `vehicle_fleet.csv` (20 records)
- `mailing_list.csv` (800 records)
- `subscriptions.csv` (250 records)
- `installed_devices.csv` (800 records)

### File Processing
Files are automatically copied to `hvac_analytics/seeds/` for dbt processing. The save function is implemented in `main.py:666-671`.

## Integration with dbt + Snowflake

### DBT Project Structure
```bash
cd hvac_analytics

# Test Snowflake connection
dbt debug

# Install dependencies  
dbt deps

# Load CSV data to Snowflake
dbt seed

# Execute transformations
dbt run

# Run data quality tests
dbt test
```

### Cost Management Strategy
- Data volumes calibrated for <$7 Snowflake budget over 21 days
- Auto-suspend warehouse configuration (60 seconds)
- X-SMALL warehouse size for development
- Efficient data types defined in `dbt_project.yml`
- Staging tables use views, marts use tables for performance

### Snowflake Configuration
- Warehouse: `COMPUTE_WH` (X-SMALL, auto-suspend 60s)
- Database: `HVAC_ANALYTICS`
- Schema: `HVAC_RAW` for seeds, `HVAC_ANALYTICS` for marts
- Column type specifications prevent Snowflake inference overhead

## Customization Options

### Scaling Data Volume
Modify the generation parameters in `main.py:677-698`:

```python
# Current defaults
customers = generate_customers(500)
technicians = generate_technicians(15) 
service_calls = generate_service_calls(customers, technicians, equipment, 2000)
# etc.

# For larger datasets, increase parameters:
customers = generate_customers(1000)
service_calls = generate_service_calls(customers, technicians, equipment, 5000)
```

### Adding New Data Types
1. Create new generation function following existing patterns
2. Add to `datasets` dictionary in `main()` function
3. Include in `save_to_csv()` call
4. Update dbt seed configuration accordingly

### Modifying Business Rules
- Adjust skill level distributions in `generate_technicians()`
- Modify service type probabilities in `generate_service_calls()`
- Change geographic distribution via Faker localization
- Update equipment brands, part types, or service categories

## Troubleshooting

### Common Issues

1. **Permission Errors**: Ensure `devenv shell` is active
2. **Missing Dependencies**: Run `uv sync` to install packages
3. **File Path Issues**: Run generation from `data_generation/` directory
4. **Memory Issues**: Reduce dataset sizes for limited memory systems

### Validation Commands

```bash
# Check total record count
wc -l /home/ewt/snowflake-hvac/data_generation/*.csv

# Verify file creation
ls -la /home/ewt/snowflake-hvac/data_generation/*.csv

# Check for data consistency
head -5 /home/ewt/snowflake-hvac/data_generation/customers.csv
```

### Expected Output Summary
```
Generated comprehensive HVAC training data:
- customers: 500 records
- technicians: 15 records  
- equipment_types: 50 records
- parts: 200 records
- installed_devices: 800 records
- service_calls: 2000 records
- parts_usage: 3990 records
- incident_response: 150 records
- vehicle_fleet: 20 records
- mailing_list: 800 records
- subscriptions: 250 records
- invoices: 1700 records
- payments: 655 records
- inventory: 200 records
- appointments: 2000 records
- quotes: 680 records
- work_orders: 2000 records
- customer_feedback: 800 records
- leads: 600 records

Total: 24,479 records across 19 tables
```

## Next Steps

After successful data generation:

1. **Load to Snowflake**: Use `dbt seed` to upload CSV files
2. **Run Transformations**: Execute `dbt run` for analytics models
3. **Test Data Quality**: Run `dbt test` for validation
4. **Monitor Costs**: Check Snowflake usage to stay within budget
5. **Explore Analytics**: Use generated marts for business intelligence

This synthetic dataset provides a comprehensive foundation for HVAC industry analytics, supporting everything from operational dashboards to predictive maintenance models while maintaining realistic business relationships and data quality standards.