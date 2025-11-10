# Gold Layer ETL

This module transforms the standardized data from the Silver layer into aggregated, analytics-ready "gold" tables. These tables are designed for direct use in business intelligence (BI) tools, dashboards, and final analysis.

This layer's outputs are stored in `data/gold/`.

---

## Gold Table Outputs

This pipeline generates the following parquet files in the `data/gold/` directory:

### 1. `aov_by_store_month.parquet`

* **Description:** Provides the Average Order Value (AOV) calculated per store, per month. This table is ideal for tracking store performance and seasonal trends.
* **Key Columns:**
    * `store_id`
    * `store_name`
    * `year`
    * `month`
    * `average_order_value_cents`

### 2. `orders_ticket_summary.parquet`

* **Description:** An enriched orders table that includes a count of support tickets for *every* order. This table is crucial for calculating the "ticket-per-order" rate. Orders with no tickets have a `ticket_count` of 0.
* **Key Columns:**
    * `order_id`
    * `ordered_at`
    * `store_id`
    * `store_name`
    * `customer_id`
    * `customer_name`
    * `ticket_count`

---

## Transformation Logic & Business Rationale

The design of these gold tables is based on the following analytical requirements:

### For `aov_by_store_month.parquet`

1.  **Granularity (Store & Month):** A single, global AOV (e.g., "$15.25") is not very actionable. By aggregating by `store` and `month`, we create a rich dataset that allows analysts to answer key business questions like:
    * "How is the Philadelphia store's AOV trending over time?"
    * "Is there seasonality in our sales?"
    * "How do different locations compare to each other?"

2.  **Metric (`order_total_cents`):** We chose `order_total_cents` as the basis for AOV. This represents the total revenue collected from the customer for the transaction, making it the most accurate measure for this metric.

3.  **Scope (No Exclusions):** This table represents a "Gross AOV" and includes all orders from the `orders` table. We do not join with `support_tickets` to exclude "refunded" orders. This keeps the metric simple, clearly defined, and fast to calculate. A separate, more complex "Net AOV" metric could be built later if required.

### For `orders_ticket_summary.parquet`

1.  **Objective (Ticket-per-Order Rate):** The primary goal is to enable the calculation of the "ticket-per-order" rate. This is a critical operational metric for understanding customer friction and support costs.

2.  **Completeness (Left Join):** To calculate an accurate rate, we **must** include *all* orders. We use the `orders` table as the base and perform a `LEFT JOIN` against the ticket counts. This ensures that orders with no support tickets are included with a `ticket_count` of 0, which is essential for the denominator of the metric.

3.  **Enrichment (Ready for Analysis):** A simple `[order_id, ticket_count]` table is not a "gold" table. By pre-joining `customer` and `store` information, we make the table immediately ready for analysis. A BI tool can instantly use this file to create visualizations for questions like:
    * "Which *stores* generate the most support tickets?"
    * "Do *specific customers* submit a high number of tickets?"

4.  **Scope (Ignore `None` `order_id`):** Support tickets in the silver data that had a `None` `order_id` were excluded from this calculation, as they cannot be attributed to a specific order and are out of scope for this particular analysis.