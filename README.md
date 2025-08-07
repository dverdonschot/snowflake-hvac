# HVAC Analytics Platform Documentation

## =Ë Overview
This project demonstrates a comprehensive HVAC service company analytics platform built with **Snowflake**, **dbt**, and advanced data science techniques. It includes synthetic data generation, business intelligence models, and cutting-edge analytics use cases.

## <¯ Project Goals
- Create realistic HVAC business training data (20+ interconnected tables)
- Build scalable analytics infrastructure with dbt + Snowflake
- Demonstrate advanced Snowflake features (ML, geospatial, real-time processing)
- Maintain strict cost controls (<$7 budget over 21 days)

---

## =Ú Documentation Structure

Follow these guides in order for the best learning experience:

### Phase 1: Foundation Setup
- **[01-snowflake-setup-instructions.md](01-snowflake-setup-instructions.md)** - Initial Snowflake account and warehouse configuration
- **[02-data-generation-guide.md](02-data-generation-guide.md)** - Generate comprehensive HVAC business data
- **[03-dbt-snowflake-getting-started.md](03-dbt-snowflake-getting-started.md)** - dbt setup and basic transformations

### Phase 2: Analytics Development
- **[04-dbt-transformation-strategy.md](04-dbt-transformation-strategy.md)** - Advanced dbt modeling patterns
- **[05-hvac-data-enhancement-plan.md](05-hvac-data-enhancement-plan.md)** - Data quality and enhancement strategies

### Phase 3: Advanced Analytics
- **[06-advanced-analytics-roadmap.md](06-advanced-analytics-roadmap.md)** - Strategic business intelligence roadmap
- **[07-snowflake-advanced-tasks.md](07-snowflake-advanced-tasks.md)** - 5 hands-on advanced Snowflake implementations

### Reference Files
- **[CLAUDE.md](CLAUDE.md)** - Project instructions and development environment setup

---

## =€ Quick Start

### Prerequisites
- Snowflake account (trial or paid)
- Python 3.12+ with uv package manager
- Node.js 22+ (for devenv setup)

### Setup Commands
```bash
# 1. Setup development environment
devenv shell

# 2. Generate training data
cd data_generation && python main.py

# 3. Initialize dbt project
cd hvac_analytics && dbt debug

# 4. Load data and run transformations
dbt seed && dbt run && dbt test
```

---

## =Ê Generated Data Overview

The project creates **20+ interconnected datasets** representing a complete HVAC business:

### Core Business Entities
- **Customers** (500) - Residential/commercial clients
- **Technicians** (15) - Service staff with skill matrices
- **Equipment Types** (50) - HVAC units from major brands
- **Service Calls** (2000) - Primary work orders
- **Parts** (200) - Inventory components

### Extended Operations
- **Installed Devices** (800) - Customer equipment tracking
- **Invoices & Payments** - Complete billing cycle
- **Appointments** (500) - Scheduling system
- **Customer Feedback** (800) - Satisfaction surveys
- **Leads** (600) - Sales pipeline

### Advanced Analytics Ready
- **Vehicle Fleet** (20) - Company vehicle tracking
- **Incident Response** (150) - Emergency call management
- **Inventory Management** - Stock levels and ABC classification
- **Work Orders** - Detailed service instructions
- **Subscriptions** (250) - Service contracts

---

## =' Architecture Components

### Data Layer
- **Snowflake Data Warehouse** - X-SMALL warehouse for cost optimization
- **dbt Transformations** - Staging ’ Intermediate ’ Marts pattern
- **CSV Seeds** - Training data loaded via dbt seed

### Analytics Layer
- **Business Intelligence Models** - Service performance, customer analysis
- **Machine Learning** - Snowflake ML functions for clustering and forecasting
- **Real-time Processing** - Streams and Tasks for operational dashboards
- **Geospatial Analytics** - Route optimization and territory management

### Advanced Features
- **Cortex AI** - Text analysis of service notes
- **Data Marketplace** - External weather and economic data
- **Vector Search** - Similarity analysis for service issues
- **Time Series Forecasting** - Demand prediction models

---

## =° Cost Management

### Budget Optimization
- **Target**: <$7 over 21 days
- **Warehouse**: X-SMALL with 60-second auto-suspend
- **Data Volume**: Calibrated for minimal storage costs
- **Query Patterns**: Efficient transformations and materialized views

### Monitoring
- Resource monitors configured for credit usage alerts
- Daily cost tracking recommendations
- Query optimization guidelines

---

## <¯ Learning Outcomes

By completing this project, you'll gain hands-on experience with:

### Snowflake Platform
- Data warehousing fundamentals
- Advanced SQL and window functions
- Machine learning with Snowflake ML
- Real-time data processing (Streams/Tasks)
- Geospatial analytics
- Cortex AI for text analysis
- Data Marketplace integration

### Analytics Engineering
- dbt modeling patterns (staging, intermediate, marts)
- Data quality testing and validation
- Documentation and lineage
- Incremental models and snapshots

### Business Intelligence
- Customer segmentation and RFM analysis
- Operational dashboards and KPIs
- Predictive maintenance models
- Route optimization algorithms
- Financial performance analysis

### Data Science Applications
- Time series forecasting
- Text analytics and NLP
- Vector similarity search
- Anomaly detection
- Recommendation systems

---

## =È Business Value Demonstrations

### Operational Efficiency
- 25% reduction in travel costs through route optimization
- 15% improvement in technician utilization
- 50% faster issue resolution with real-time dashboards

### Customer Experience
- Proactive maintenance scheduling
- Personalized service recommendations
- Faster response times for emergency calls

### Financial Performance
- Dynamic pricing optimization
- Accurate demand forecasting
- Improved cash flow through payment analysis

### Strategic Insights
- Customer lifetime value modeling
- Market expansion opportunities
- Service quality optimization

---

## = Incremental Development

### Initial Data Generation
```bash
cd data_generation && python main.py
```

### Incremental Updates
```bash
cd data_generation && python main.py --incremental
```

This adds realistic amounts of new data:
- 5 new customers
- 2 new technicians
- 200 new service calls
- Related invoices, feedback, etc.

---

## > Contributing

This project serves as both a learning resource and a template for HVAC analytics implementations. Key areas for extension:

### Data Enhancements
- Additional equipment manufacturers
- More detailed service taxonomies
- Seasonal demand patterns
- Customer demographic data

### Analytics Extensions
- Predictive maintenance algorithms
- Customer churn prevention
- Dynamic pricing models
- IoT sensor integration

### Platform Integrations
- CRM system connections
- Accounting software APIs
- Fleet management systems
- Weather service integration

---

## =Þ Support & Resources

### Documentation Issues
- Check troubleshooting sections in each guide
- Review Snowflake documentation for platform features
- Verify cost monitoring is properly configured

### Learning Resources
- [Snowflake Documentation](https://docs.snowflake.com/)
- [dbt Documentation](https://docs.getdbt.com/)
- [HVAC Industry Analytics Best Practices](06-advanced-analytics-roadmap.md)

### Cost Management
- Monitor credit usage daily
- Use Resource Monitors for automated alerts
- Review query performance for optimization opportunities

---

*Start with [01-snowflake-setup-instructions.md](01-snowflake-setup-instructions.md) to begin your HVAC analytics journey!*