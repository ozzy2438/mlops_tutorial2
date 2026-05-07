# Data Inventory

## Azure Blob Storage

- **Storage account:** `ozzymldata2410`
- **Container:** `mlops-data`

## Raw Data Files

The following raw CSV files are stored in Azure Blob Storage:

- `raw_customers.csv`
- `raw_orders.csv`
- `raw_order_items.csv`
- `raw_products.csv`
- `raw_stores.csv`
- `raw_supplies.csv`

## High-Level Data Relationship

The raw retail data follows these relationships:

```text
Customer -> Order -> Order Items -> Product -> Supplies
Store -> Order
```

Customers place orders. Orders contain order items. Order items reference products, and products are supported by supplies. Stores are also associated with orders.

## GitHub Storage Policy

Raw CSV files are intentionally kept outside GitHub because they are large and should be managed in Azure Blob Storage as data assets.

GitHub should only store code, documentation, metadata, configuration, and pipeline files needed to build, test, and operate the retail data platform.
