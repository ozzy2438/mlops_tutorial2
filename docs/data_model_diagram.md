# Retail Data Model Diagram

This diagram shows all six raw CSV files used by the project.

```text
raw_customers.csv
      |
      v
raw_orders.csv <------ raw_stores.csv
      |
      v
raw_order_items.csv
      |
      v
raw_products.csv
      |
      v
raw_supplies.csv
```

File count check:

1. `raw_customers.csv`
2. `raw_orders.csv`
3. `raw_order_items.csv`
4. `raw_products.csv`
5. `raw_stores.csv`
6. `raw_supplies.csv`

So the correct schema has six raw files, not four.
