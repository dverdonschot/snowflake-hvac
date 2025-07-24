# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an HVAC Service Company data generation project that creates synthetic training data for dbt + Snowflake analytics. The project generates realistic business data for an HVAC company while maintaining strict cost controls (<$7 budget over 21 days on Snowflake).

## Development Environment

The project uses devenv (Nix-based) for reproducible development environments:

```bash
# Activate development environment
devenv shell

# Generate training data
python main.py
```

The devenv configuration provides Python 3.12, uv package manager, Node.js 22, and Claude Code CLI.

## Architecture

### Data Model
The application generates 6 interconnected datasets representing a complete HVAC business:

- **customers** (500 records) - Residential/commercial clients
- **technicians** (15 records) - Service staff with skill levels  
- **equipment_types** (50 records) - HVAC units from major brands
- **parts** (200 records) - Inventory for repairs
- **service_calls** (2000 records) - Work orders linking customers, technicians, equipment
- **parts_usage** - Junction table tracking parts consumed per service call

### Key Relationships
- Service calls reference customers, technicians, and equipment via foreign keys
- Parts usage links service calls to specific parts with quantities
- Cost calculations include labor (duration × technician rate) + parts costs
- Date ranges are realistic (service calls within last year, customers within 2 years)

## Dependencies

Core packages (pyproject.toml):
- `faker>=19.0.0` - Realistic synthetic data generation
- `pandas>=2.0.0` - Data manipulation and CSV export  
- `numpy>=1.24.0` - Numerical operations

## Generated Output

Running `python main.py` creates these CSV files:
- `customers.csv`
- `technicians.csv`
- `equipment_types.csv` 
- `parts.csv`
- `service_calls.csv`
- `parts_usage.csv`

## Integration Context

This data is designed for dbt transformation and Snowflake warehousing following the specifications in `dbt-snowflake-getting-started.md`. The data volumes and structure support:

- Dimensional modeling (fact tables: service_calls, parts_usage; dimensions: customers, technicians, etc.)
- Cost-efficient Snowflake usage with X-SMALL warehouse
- Realistic HVAC business analytics (technician performance, customer analysis, parts inventory)

## Data Quality Features

- Referential integrity maintained across all datasets
- Industry-appropriate data (real HVAC brands: Carrier, Trane, Lennox, etc.)
- Realistic business logic for costs, durations, and service types
- Parameterized generation functions for easy scaling