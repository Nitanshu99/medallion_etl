# Silver ETL Layer

This module transforms raw "Bronze" data into a "Silver" layer of clean, conformed, and query-ready tables.

## Overview

* **Source:** `data/bronze/parquet/`
* **Target:** `data/silver/`
* **Purpose:** To apply standardization, data-type correction, renaming, and light transformations (like flattening) to the raw data. This layer ensures all data is consistent and linkable before any business-level aggregation (which happens in the "Gold" layer).

---

## Core Components

* **`transform/`**: Houses all transformation logic. Each table has its own module (e.g., `customers.py`) containing a single, dedicated transformation function.
* **`load/`**: Contains a reusable data-saving module (`saver.py`) responsible for writing the transformed DataFrames to the `data/silver/` directory in Parquet format.
* **`root/medallion_dagster/silver.py`**: (Dagster IO) Defines the Dagster assets for the Silver layer. Each asset corresponds to a transformed table and uses the functions from the `transform/` modules to define its computation. This file manages dependencies (e.g., this Silver asset depends on that Bronze asset) and handles the I/O operations within the Dagster framework.
* **`run_silver.py`**: The main *manual* orchestrator (for non-Dagster execution) that:
    1.  Loads all Bronze tables.
    2.  Executes the transformation functions in the correct order.
    3.  Uses the `saver` to load the resulting DataFrames into the Silver layer.

---

## Transformation Logic by Table

### 1. General Transformations

* **Monetary Columns:** All columns representing money (e.g., `price`, `subtotal`, `tax_paid`, `order_total`) are assumed to be in **cents**. They are renamed with a `_cents` suffix (e.g., `price_cents`) to make this unit explicit. This logic is centralized in `transform/common.py`.

### 2. Table-Specific Rules

#### `raw_customers` -> `customers`
* **`id`** is renamed to **`customer_id`** (standardized primary key).

#### `raw_orders` -> `orders`
* **`id`** is renamed to **`order_id`** (standardized primary key).
* **`customer`** is renamed to **`customer_id`** (conformed foreign key).
* Monetary columns (`subtotal`, `tax_paid`, `order_total`) are renamed (see General Transformations).

#### `raw_stores` -> `stores`
* **`id`** is renamed to **`store_id`** (standardized primary key).
* **`opened_at`** (timestamp) is cast to a **`date`** type, as the time component is not relevant.

#### `raw_products` -> `products`
* **`price`** is renamed to **`price_cents`** (see General Transformations).

#### `raw_items` -> `order_items`
* **Table Rename:** The table is renamed to `order_items` for clarity.
* **`id`** is renamed to **`order_item_id`** (standardized primary key).

#### `raw_supplies` -> `supplies`
* **`id`** is renamed to **`supply_id`** (standardized primary key).

#### `support_tickets` -> `support_tickets`
This table requires the most significant transformation.
* **Key Conformance:**
    * The `customer_external_id` (e.g., `139`) does not match the UUIDs in the `customers` table.
    * A join is performed with the `raw_orders` table (in memory) using `order_id` to look up the correct UUID `customer_id`.
    * A new **`customer_id`** column is created.
    * The original `customer_external_id` column is dropped.
* **Struct Flattening:**
    * The `sentiment` column (a struct like `{'model': 'demo', 'score': 0.21}`) is flattened into two new columns:
        * **`sentiment_score`** (e.g., `0.21`)
        * **`sentiment_model`** (e.g., `demo`)
    * The original `sentiment` column is dropped.
* **Data Types:**
    * The `tags` column is preserved as a list/array.
    * `null` values (e.g., in `order_id` or `resolved_at`) are preserved as `null`.