# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an HVAC Service Company data generation project that creates comprehensive synthetic training data for dbt + Snowflake analytics. The project generates realistic business data for an HVAC company across 20+ interconnected tables while maintaining strict cost controls (<$7 budget over 21 days on Snowflake).

## Development Environment

### Primary Commands
```bash
# Activate development environment (required first)
devenv shell

# Generate all training data (main command)
cd data_generation && python main.py

# Install dependencies (if needed)
uv sync
```

The project uses devenv (Nix-based) for reproducible development environments with Python 3.12, uv package manager, Node.js 22, and Claude Code CLI auto-installed.

### DBT Integration
```bash
# Navigate to dbt project
cd hvac_analytics

# DBT commands (after Snowflake setup)
dbt debug          # Test connection
dbt deps           # Install dependencies  
dbt seed           # Load CSV data
dbt run            # Execute transformations
dbt test           # Run data tests
```

## Architecture

### Comprehensive Data Model
The application generates 20+ interconnected datasets representing a complete HVAC business:

**Core Business Entities:**
- `customers` (500) - Residential/commercial clients
- `technicians` (15) - Service staff with detailed skill matrices
- `equipment_types` (50) - HVAC units from major brands (Carrier, Trane, etc.)
- `parts` (200) - Inventory components
- `service_calls` (2000) - Primary work orders
- `parts_usage` - Junction table for parts consumption

**Extended Business Operations:**
- `installed_devices` (800) - Customer equipment tracking with warranties
- `incident_response` (150) - Emergency call management
- `vehicle_fleet` (20) - Company vehicle tracking
- `mailing_list` (800) - Marketing contacts
- `subscriptions` (250) - Service contracts
- `invoices` - Billing records with tax calculations
- `payments` - Payment processing with multiple methods
- `inventory` - Stock level management with ABC classification
- `appointments` (500) - Scheduling with future bookings
- `quotes` (400) - Sales estimates
- `work_orders` - Detailed work instructions
- `customer_feedback` (800) - Satisfaction surveys
- `leads` (600) - Sales pipeline

### Data Architecture Patterns
- **Referential Integrity**: All foreign keys properly maintained across tables
- **Temporal Consistency**: Realistic date relationships (appointments precede service calls)
- **Business Logic**: Cost calculations include labor rates, markup, taxes
- **Data Quality**: Industry-appropriate values (HVAC brands, part types, service classifications)

## Key Technical Details

### Data Generation Logic (data_generation/main.py)
- Parameterized functions for easy scaling
- Faker library provides realistic names, addresses, phone numbers
- Business rules embedded (emergency calls have faster response times)
- Skill-based technician assignment probabilities
- Geographic and temporal data consistency

### DBT Project Structure (hvac_analytics/)
- Configured for Snowflake with cost optimization (X-SMALL warehouse)
- Seed data type definitions for all 20+ tables
- Materialization strategy: staging (views), marts (tables)
- Column type specifications to prevent Snowflake inference overhead

### Cost Management Strategy
- Data volumes calibrated for <$7 Snowflake budget over 21 days
- Auto-suspend warehouse configuration (60 seconds)
- Efficient data types in dbt_project.yml
- Detailed cost monitoring guidelines in dbt-snowflake-getting-started.md

## Generated Output Files

Running `python main.py` in data_generation/ creates 20 CSV files including:
- Core: customers, technicians, equipment_types, parts, service_calls, parts_usage
- Operations: invoices, payments, appointments, work_orders, inventory
- Analytics: customer_feedback, incident_response, quotes, leads
- Support: vehicle_fleet, mailing_list, subscriptions, installed_devices