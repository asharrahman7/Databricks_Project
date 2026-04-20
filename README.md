# 🏗️ Databricks Data Engineering Project — Medallion Architecture

An end-to-end data engineering pipeline built on **Databricks**, integrating data from two source systems (**CRM** and **ERP**) through a **Bronze → Silver → Gold** medallion architecture, delivering a production-ready star schema for analytics and BI consumption.

---

## 📌 Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Source Data](#source-data)
- [Project Structure](#project-structure)
- [Layer Breakdown](#layer-breakdown)
  - [Bronze Layer](#-bronze-layer---raw-ingestion)
  - [Silver Layer](#-silver-layer---cleansing--transformation)
  - [Gold Layer](#-gold-layer---star-schema--analytics)
- [Key Technical Highlights](#key-technical-highlights)
- [Tech Stack](#tech-stack)
- [How to Run](#how-to-run)

---

## Project Overview

This project demonstrates a full data engineering workflow on the Databricks Lakehouse platform. Raw CSV files from two source systems are ingested, cleaned, standardised, and modelled into a dimensional star schema using **Delta Lake** and **Unity Catalog**.

The pipeline is structured around the **Medallion Architecture** — a layered design pattern that progressively improves data quality and structure as data moves from raw ingestion through to business-ready analytical tables.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        SOURCE SYSTEMS                           │
│           CRM (3 files)              ERP (3 files)              │
└───────────────────┬──────────────────────────┬──────────────────┘
                    │                          │
                    ▼                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                      🥉 BRONZE LAYER                            │
│         Raw Delta tables — no transformation applied            │
│  workspace.bronze.crm_*       workspace.bronze.erp_*            │
└─────────────────────────────────┬───────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                      🥈 SILVER LAYER                            │
│   Cleaned, standardised, type-cast, and renamed tables          │
│   workspace.silver.crm_*       workspace.silver.erp_*           │
└─────────────────────────────────┬───────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                      🥇 GOLD LAYER                              │
│         Star schema — Dimensions + Fact table                   │
│   dim_customers  │  dim_products  │  fact_sales                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Source Data

| Source | File | Description |
|--------|------|-------------|
| CRM | `cust_info.csv` | Customer demographics and identifiers |
| CRM | `prd_info.csv` | Product catalogue with pricing and line info |
| CRM | `sales_details.csv` | Transactional sales data |
| ERP | `CUST_AZ12.csv` | Extended customer info (birthdate, gender) |
| ERP | `LOC_A101.csv` | Customer location data (country) |
| ERP | `PX_CAT_G1V2.csv` | Product category and subcategory mapping |

---

## Project Structure

```
Databricks_Project/
│
├── Dataset/                        # Source CSV files
│   ├── source_crm/
│   └── source_erp/
│
└── Code/
    ├── Bronze/
    │   └── Bronze_Layer.ipynb      # Config-driven ingestion for all 6 sources
    │
    ├── Silver/
    │   ├── Silver_crm_cust_info.ipynb
    │   ├── Silver_crm_prd_info.ipynb
    │   ├── Silver_crm_sales_details.ipynb
    │   ├── Silver_erp_cust_az12.ipynb
    │   ├── Silver_erp_loc_a101.ipynb
    │   ├── Silver_erp_px_cat_g1v2.ipynb
    │   └── Silver_orchestration.ipynb  # ← Single entry point for Silver layer
    │
    └── Gold/
        ├── Gold_dim_customers.ipynb
        ├── Gold_dim_products.ipynb
        ├── Gold_fact_sales.ipynb
        └── Gold_orchestration.ipynb    # ← Single entry point for Gold layer
```

---

## Layer Breakdown

### 🥉 Bronze Layer — Raw Ingestion

**Notebook:** `Bronze_Layer.ipynb`

The Bronze layer ingests all raw CSV files from Databricks Volumes and writes them directly to Delta tables with no transformations applied — preserving the source data exactly as received.

**Key design decisions:**
- Uses a **config-driven ingestion loop** (`INGESTION_CONFIG`) — a list of dictionaries containing source path and target table name for each file. Adding a new data source requires no new code, only a new config entry.
- All tables are written using `.format("delta").saveAsTable(...)` with **Unity Catalog 3-part naming** (`workspace.bronze.<table>`).
- `inferSchema=True` is used at ingestion — schema enforcement happens in Silver.

```python
INGESTION_CONFIG = [
    {"source": "crm", "path": "/Volumes/.../cust_info.csv",     "table": "crm_cust_info"},
    {"source": "crm", "path": "/Volumes/.../prd_info.csv",      "table": "crm_prd_info"},
    {"source": "crm", "path": "/Volumes/.../sales_details.csv", "table": "crm_sales_details"},
    {"source": "erp", "path": "/Volumes/.../CUST_AZ12.csv",     "table": "erp_cust_az12"},
    {"source": "erp", "path": "/Volumes/.../LOC_A101.csv",      "table": "erp_loc_a101"},
    {"source": "erp", "path": "/Volumes/.../PX_CAT_G1V2.csv",   "table": "erp_px_cat_g1v2"},
]
```

---

### 🥈 Silver Layer — Cleansing & Transformation

**Orchestration:** `Silver_orchestration.ipynb` → runs all 6 Silver notebooks in sequence via `dbutils.notebook.run()`

Each Silver notebook applies targeted transformations appropriate to its source table. Common patterns applied across all notebooks:

**1. Schema-aware string trimming**
Dynamically detects all `StringType` columns at runtime — no hardcoded column names:
```python
for field in df.schema.fields:
    if isinstance(field.dataType, StringType):
        df = df.withColumn(field.name, trim(col(field.name)))
```

**2. Value normalisation**
Inconsistent representations are standardised using `F.when()` with `F.upper()` for case-insensitive matching:
- Marital status: `S` → `Single`, `M` → `Married`
- Gender: `M` → `Male`, `F` → `Female`
- Product line: `M` → `Mountain`, `R` → `Road`, `S` → `Other Sales`, `T` → `Touring`
- Country: `DE` → `Germany`, `US`/`USA`/`Usa` → `United States`, null/empty → `N/A`

**3. Date parsing with validation**
Dates stored as integers (e.g. `20130101`) are validated before conversion — checking for sentinel zero values and incorrect length before applying `to_date()`:
```python
F.when(
    (col("sls_order_dt") == 0) | (length(col("sls_order_dt")) != 8), None
).otherwise(F.to_date(col("sls_order_dt").cast("string"), "yyyyMMdd"))
```

**4. Smart price imputation**
When `sls_price` is null or invalid, it is derived from business logic rather than filled with zero:
```python
F.when(
    col("sls_price").isNull() | (col("sls_price") <= 0),
    col("sls_sales") / col("sls_quantity")
).otherwise(col("sls_price"))
```

**5. Additional transformations by table**

| Notebook | Key Transformation |
|----------|--------------------|
| `Silver_crm_cust_info` | Null customer ID filter, gender & marital status normalisation |
| `Silver_crm_prd_info` | Product key parsing (extracts `cat_id` via regex), null-safe cost with `coalesce`, date casting |
| `Silver_crm_sales_details` | Date validation & parsing, smart price imputation |
| `Silver_erp_cust_az12` | Customer number standardisation, birth date casting |
| `Silver_erp_loc_a101` | Dash removal from customer IDs (`regexp_replace`), multi-value country normalisation |
| `Silver_erp_px_cat_g1v2` | `YES`/`NO` string → native `Boolean` type for maintenance flag |

**6. Consistent column renaming**
All tables use a `RENAME_MAP` dictionary to rename cryptic source column names to business-friendly equivalents (e.g. `cst_gndr` → `gender`, `prd_nm` → `product_name`).

---

### 🥇 Gold Layer — Star Schema & Analytics

**Orchestration:** `Gold_orchestration.ipynb` → runs all 3 Gold notebooks in dependency order

The Gold layer produces a **star schema** by joining cleaned Silver tables and applying final business logic. All tables are written as Delta tables to `workspace.gold.*`.

#### `dim_customers`
Merges three Silver tables — CRM customers, ERP customers, and ERP location — into a single analytical dimension:

```sql
SELECT
    ROW_NUMBER() OVER (ORDER BY ci.customer_id) AS customer_key,  -- Surrogate key
    ci.customer_id,
    ci.customer_number,
    ci.first_name,
    ci.last_name,
    la.country,
    ci.marital_status,
    CASE
        WHEN ci.gender <> 'n/a' THEN ci.gender
        ELSE COALESCE(ca.gender, 'n/a')        -- CRM takes priority; ERP is fallback
    END AS gender,
    ca.birth_date AS birthdate,
    ci.created_date AS create_date
FROM workspace.silver.crm_customers ci
LEFT JOIN workspace.silver.erp_customer ca ON ci.customer_number = ca.customer_number
LEFT JOIN workspace.silver.erp_cutomer_location la ON ci.customer_number = la.customer_number
```

#### `dim_products`
Joins CRM product data with ERP category data. Surrogate key ordered by `start_date` and `product_number` for deterministic ordering:

```sql
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.start_date, pn.product_number) AS product_key,
    pn.product_id, pn.product_number, pn.product_name,
    pn.category_id, pc.category, pc.subcategory, pc.maintenance_flag,
    pn.product_line, pn.start_date
FROM workspace.silver.crm_products pn
LEFT JOIN workspace.silver.erp_product_category pc ON pn.category_id = pc.category_id
```

#### `fact_sales`
Resolves all foreign keys to surrogate keys from the dimension tables, producing an analytics-ready fact table:

```sql
SELECT
    sd.order_number,
    pr.product_key,
    cu.customer_key,
    sd.order_date, sd.ship_date, sd.due_date,
    sd.sales_amount, sd.quantity, sd.price
FROM silver.crm_sales sd
LEFT JOIN gold.dim_products pr ON sd.product_number = pr.product_number
LEFT JOIN gold.dim_customers cu ON sd.customer_id = cu.customer_id
```

**Final star schema:**

```
          dim_customers          dim_products
         (customer_key)         (product_key)
               │                      │
               └──────────────────────┘
                          │
                     fact_sales
              (customer_key, product_key,
               order_date, sales_amount,
               quantity, price)
```

---

## Key Technical Highlights

| Pattern | Where Used | Why It Matters |
|---------|-----------|----------------|
| Config-driven ingestion loop | Bronze | Scalable — new sources need no new code |
| Schema-aware trimming | All Silver notebooks | Type-safe, works regardless of schema changes |
| Smart price imputation | `Silver_crm_sales_details` | Business-logic recovery vs. naive zero-fill |
| Date validation before parsing | `Silver_crm_sales_details` | Handles sentinel values and malformed data |
| Boolean type casting | `Silver_erp_px_cat_g1v2` | Correct type semantics for downstream queries |
| Cross-system ID normalisation | `Silver_erp_loc_a101` | Enables clean joins across CRM and ERP |
| Gender resolution policy | `Gold_dim_customers` | Conflict resolution with explicit precedence rule |
| Surrogate key generation | Gold dimension tables | Standard data warehouse pattern for BI tools |
| Orchestration notebooks | Silver + Gold layers | Single entry point per layer — production-ready |
| Sanity checks throughout | All notebooks | Data quality validation at every stage |

---

## Tech Stack

| Technology | Purpose |
|-----------|---------|
| **Databricks** | Unified data engineering and analytics platform |
| **Apache Spark / PySpark** | Distributed data processing |
| **Delta Lake** | ACID-compliant storage format for all tables |
| **Unity Catalog** | Governed, 3-level namespace (`catalog.schema.table`) |
| **Databricks Volumes** | Source file storage |
| **dbutils.notebook.run()** | Notebook orchestration |
| **Spark SQL** | Gold layer transformations and joins |
| **Python** | Primary development language |

---

## How to Run

### Prerequisites
- A Databricks workspace with Unity Catalog enabled
- A catalog named `workspace` with three schemas: `bronze`, `silver`, `gold`
- Source CSV files uploaded to `/Volumes/workspace/bronze/source_systems/`

### Execution Order

```
1. Bronze_Layer.ipynb               ← Ingest all 6 source files

2. Silver_orchestration.ipynb       ← Runs all 6 Silver notebooks in sequence

3. Gold_orchestration.ipynb         ← Runs dim_customers → dim_products → fact_sales
```

> Each orchestration notebook uses `dbutils.notebook.run()` to chain notebooks automatically. You only need to trigger the orchestration notebook for each layer.

### Manual execution (per notebook)

If you prefer to run individual notebooks, follow this order:

```
Bronze_Layer
    ↓
Silver_crm_cust_info → Silver_crm_prd_info → Silver_crm_sales_details
Silver_erp_cust_az12 → Silver_erp_loc_a101 → Silver_erp_px_cat_g1v2
    ↓
Gold_dim_customers → Gold_dim_products → Gold_fact_sales
```

---

## Author

**Ashar Abdur Rahman**  
Data Engineering | PySpark | Databricks | Delta Lake

[![GitHub](https://img.shields.io/badge/GitHub-asharrahman7-181717?style=flat&logo=github)](https://github.com/asharrahman7)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0A66C2?style=flat&logo=linkedin)](https://www.linkedin.com/in/asharrahman7)

---

*Built with Databricks · Delta Lake · Unity Catalog · PySpark*
