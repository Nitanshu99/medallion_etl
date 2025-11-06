# Medallion Architecture Directory Structure

```
medallion_etl/
│
├── data/
│   ├── bronze/
│   │   ├── sales_raw/
│   │   └── customers_raw/
│   ├── silver/
│   │   └── sales_customers/
│   └── gold/
│       └── metrics/
│
├── bronze/
│   ├── __init__.py
│   ├── extract/
│   │   ├── __init__.py
│   │   ├── csv_extractor.py
│   │   └── jsonl_extractor.py
│   └── load/
│       ├── __init__.py
│       ├── csv_loader.py
│       └── jsonl_loader.py
│
├── silver/
│   ├── __init__.py
│   ├── transform/
│   │   ├── __init__.py
│   │   ├── sales_cleaner.py
│   │   ├── customer_cleaner.py
│   │   └── data_joiner.py
│   └── load/
│       ├── __init__.py
│       └── silver_loader.py
│
├── gold/
│   ├── __init__.py
│   ├── average_order_value.py
│   ├── order_tickets_count.py
│   └── load/
│       ├── __init__.py
│       └── metrics_loader.py
│
├── pipeline.py
├── requirements.txt
├── .env
└── README.md
```

## File Descriptions

### Bronze Layer (`bronze/`)

**Extract (`bronze/extract/`)**
- **csv_extractor.py**: Contains `extract_local_csv()` operation
- **jsonl_extractor.py**: Contains `extract_azure_jsonl()` operation

**Load (`bronze/load/`)**
- **csv_loader.py**: Contains `load_bronze_csv()` operation
- **jsonl_loader.py**: Contains `load_bronze_jsonl()` operation

### Silver Layer (`silver/`)

**Transform (`silver/transform/`)**
- **sales_cleaner.py**: Contains `clean_and_standardize_sales()` operation
- **customer_cleaner.py**: Contains `clean_and_standardize_customers()` operation
- **data_joiner.py**: Contains `join_and_enrich()` operation

**Load (`silver/load/`)**
- **silver_loader.py**: Contains `load_silver()` operation

### Gold Layer (`gold/`)
- **average_order_value.py**: Contains `create_average_order_value()` operation
- **order_tickets_count.py**: Contains `create_order_tickets_count()` operation

**Load (`gold/load/`)**
- **metrics_loader.py**: Contains `load_gold_metrics()` operation

### Root Level
- **pipeline.py**: Main orchestration file that imports from all layers and defines the DAG
- **requirements.txt**: Python dependencies
- **.env**: Environment variables (Azure connection strings, paths, etc.)
- **README.md**: Documentation