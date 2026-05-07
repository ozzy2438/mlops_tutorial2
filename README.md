# mlops_tutorial2
# MLOps Retail Data Platform

This project is a small team-based MLOps and analytics engineering practice project.

The goal is to build a modern retail data platform using:

- GitHub for collaboration and pull requests
- Azure Blob Storage for large raw CSV files
- Snowflake for cloud data warehousing
- dbt-style modelling concepts for RAW, STAGING, and MARTS layers
- Jira for sprint planning and task tracking

## Dataset

The project uses six retail CSV files:

- raw_customers.csv
- raw_orders.csv
- raw_order_items.csv
- raw_products.csv
- raw_stores.csv
- raw_supplies.csv

Together, these files represent a food retail ordering system:

Customer → Order → Order Items → Product → Supplies  
Store → Order

## Sprint 1 — Data Platform Setup

Current sprint goals:

- Create GitHub repository
- Invite collaborator
- Upload raw data files to Azure Blob Storage
- Define project structure
- Prepare initial README documentation

## Team Roles

- Data / ML Engineer: responsible for data ingestion, DVC/Azure integration, and model workflow
- Analytics Engineer: responsible for data modelling, Snowflake tables, business metrics, and documentation
