# Databricks notebook source
# DBTITLE 1,Day 1 — Title & Agenda
# MAGIC %md
# MAGIC # 🏭 Databricks Data Modelling for CPG
# MAGIC ### Day 1 — Lakehouse Foundations · Unity Catalog · SQL for Modellers · CPG Domain
# MAGIC #### Dr. Yogesh Ghorpade | DATA • AI • CLOUD | 4-Day Intensive Training | Databricks on AWS
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC > **Day 1 of 4** &nbsp;•&nbsp; **7.25 Contact Hours** &nbsp;•&nbsp; **Serverless Compute (Auto-selected)** &nbsp;•&nbsp; **Schema: `lakemeter_catalog.labuser3`**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📅 Day 1 — Full Agenda
# MAGIC
# MAGIC | Time | # | Module | Core Topics | Lab Exercise |
# MAGIC |------|---|--------|-------------|--------------|
# MAGIC | 9:00–11:00 AM | **1.1** | Databricks Lakehouse Foundations | Delta Lake · ACID · Bronze-Silver-Gold · Time Travel · MERGE | Create Delta tables, Time Travel, MERGE operations |
# MAGIC | 11:00 AM–1:00 PM | **1.2** | Unity Catalog & Data Discovery | Catalog-Schema-Table · Metadata · Tagging · Lineage · Access Controls | Discover CPG datasets, document metadata |
# MAGIC | 1:00–1:45 PM | — | 🍽️ *Lunch Break* | — | — |
# MAGIC | 1:45–3:15 PM | **1.3** | Databricks SQL for Modellers | DDL · CTAS · Views · CTEs · Joins · Window Functions · Constraints | Dimension/fact structures, CPG sales profiling |
# MAGIC | 3:15–5:00 PM | **1.4** | CPG Domain & Modelling Requirements | Product · Customer · Retailer · Sales · Promotions · Grain · KPIs | Map CPG business processes to model requirements |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Day 1 Learning Objectives
# MAGIC
# MAGIC By the end of Day 1 you will confidently:
# MAGIC - ✅ Explain the **Databricks Lakehouse architecture** and Delta Lake's ACID, Time Travel, and MERGE capabilities
# MAGIC - ✅ Navigate **Unity Catalog** and apply metadata, tagging, and access-control best practices
# MAGIC - ✅ Write **production-grade Databricks SQL** — DDL, CTAS, views, CTEs, window functions
# MAGIC - ✅ Identify CPG **business domains**, define grain, and map processes to dimensional model requirements
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🗂️ 4-Day Programme Navigation
# MAGIC
# MAGIC | Day | Notebook | Focus Area |
# MAGIC |-----|----------|-----------|
# MAGIC | **📍 Day 1 ← You Are Here** | Day 1 — Lakehouse Foundations & CPG Domain | Foundations |
# MAGIC | Day 2 | Day 2 — Dimensional Modelling & Fact Tables | Star Schema Design |
# MAGIC | Day 3 | Day 3 — DevOps, CI/CD, Optimization & Advanced Modelling | Engineering Excellence |
# MAGIC | Day 4 | Day 4 — Genie, Governance & Capstone | AI + Governance + Capstone |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🛠️ Lab Environment Quick Reference
# MAGIC
# MAGIC ```
# MAGIC Catalog  →  lakemeter_catalog
# MAGIC Schema   →  labuser3
# MAGIC Prefix   →  d1_     (all Day 1 tables)
# MAGIC Compute  →  Serverless SQL (no cluster setup needed)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Module 1.1 — Databricks Lakehouse Foundations
# MAGIC %md
# MAGIC ---
# MAGIC ## 📦 Module 1.1 — Databricks Lakehouse Foundations
# MAGIC ### 9:00 – 11:00 AM · 2 Hours
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What is the Databricks Lakehouse?
# MAGIC
# MAGIC The **Databricks Lakehouse** combines the best of two worlds on a single platform:
# MAGIC
# MAGIC ```
# MAGIC   Data Lake         Data Warehouse         Databricks Lakehouse
# MAGIC   ┌────────┐          ┌────────┐          ┌──────────────┐
# MAGIC   │ Open     │    +     │ ACID     │    =     │ Open Storage    │
# MAGIC   │ Scalable │          │ Perf.   │          │ + ACID + Gov.   │
# MAGIC   │ Cheap    │          │ Govern. │          │ + AI/ML + BI    │
# MAGIC   └────────┘          └────────┘          └──────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### 🏗️ Medallion Architecture — Bronze → Silver → Gold
# MAGIC
# MAGIC | Layer | Purpose | Characteristics | Today's Lab Tables |
# MAGIC |-------|---------|-----------------|--------------------|
# MAGIC | **🥉 Bronze** | Raw as-is ingestion | Unprocessed, append-only, source fidelity | `d1_raw_products`, `d1_raw_retailers`, `d1_raw_sales` |
# MAGIC | **🥈 Silver** | Cleansed & conformed | Validated, deduped, standardised types | Built in Day 2 |
# MAGIC | **🥇 Gold** | Business-ready aggregates | KPI-ready, BI-optimised, governed | Built in Day 2 |
# MAGIC
# MAGIC ### 🔑 Delta Lake Core Capabilities
# MAGIC
# MAGIC | Feature | What It Does | SQL Syntax |
# MAGIC |---------|-------------|------------|
# MAGIC | **ACID Transactions** | Concurrent reads/writes with full consistency | Automatic on all Delta tables |
# MAGIC | **Time Travel** | Query any historical version of a table | `SELECT * FROM t VERSION AS OF 1` |
# MAGIC | **Schema Enforcement** | Rejects data violating the table schema | Automatic |
# MAGIC | **Schema Evolution** | Safely add / rename columns | `ALTER TABLE t ADD COLUMNS (col TYPE)` |
# MAGIC | **MERGE (Upsert)** | INSERT + UPDATE + DELETE atomically | `MERGE INTO target USING source ON key...` |
# MAGIC | **OPTIMIZE** | Compact small files, improve query speed | `OPTIMIZE t ZORDER BY (col)` |
# MAGIC
# MAGIC > 🏋️‍♂️ **Hands-On Lab 1.1:** Run the cells below to set up your environment, create CPG Bronze tables, explore Time Travel, and practice MERGE operations.

# COMMAND ----------

# DBTITLE 1,Lab 1.1 — Setup: Catalog, Schema & Clean-Slate Reset
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- LAB 1.1 SETUP — Set Catalog/Schema and Reset Day 1 Tables
# MAGIC -- Run this cell FIRST before any other Day 1 cell
# MAGIC -- ============================================================
# MAGIC
# MAGIC USE CATALOG lakemeter_catalog;
# MAGIC USE SCHEMA labuser3;
# MAGIC
# MAGIC -- Safe reset: drop Day 1 tables if they exist (idempotent)
# MAGIC DROP TABLE IF EXISTS d1_raw_products;
# MAGIC DROP TABLE IF EXISTS d1_raw_retailers;
# MAGIC DROP TABLE IF EXISTS d1_raw_sales;
# MAGIC DROP TABLE IF EXISTS d1_raw_promotions;
# MAGIC DROP TABLE IF EXISTS d1_staging_products;
# MAGIC DROP TABLE IF EXISTS d1_profile_products;
# MAGIC DROP VIEW  IF EXISTS d1_v_active_products;
# MAGIC
# MAGIC SELECT
# MAGIC   'Day 1 Lab Environment Ready ✅' AS status,
# MAGIC   current_catalog()               AS active_catalog,
# MAGIC   current_schema()                AS active_schema,
# MAGIC   current_timestamp()             AS reset_at;

# COMMAND ----------

# DBTITLE 1,Lab 1.1a — Create Bronze Delta Tables (Products, Retailers, Sales)
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- LAB 1.1a — CREATE BRONZE DELTA TABLES
# MAGIC -- Bronze = Raw source data ingested as-is, no transformation
# MAGIC -- Each table carries rich COMMENT and TBLPROPERTIES metadata
# MAGIC -- ============================================================
# MAGIC
# MAGIC USE CATALOG lakemeter_catalog;
# MAGIC USE SCHEMA labuser3;
# MAGIC
# MAGIC -- ────────────────────────────────────────────────────────────
# MAGIC -- TABLE 1: PRODUCTS  ← Source: SAP ERP / Product Master
# MAGIC -- ────────────────────────────────────────────────────────────
# MAGIC CREATE OR REPLACE TABLE d1_raw_products (
# MAGIC   product_id    STRING        COMMENT 'Natural product code from SAP ERP',
# MAGIC   product_name  STRING        NOT NULL COMMENT 'Full product description',
# MAGIC   brand         STRING        COMMENT 'Brand (Maggi, KitKat, Nescafe, Milo, Boost)',
# MAGIC   category      STRING        COMMENT 'Primary category: Noodles | Chocolates | Beverages | Sauces',
# MAGIC   sub_category  STRING        COMMENT 'Secondary classification within category',
# MAGIC   pack_size     STRING        COMMENT 'Pack weight or volume (e.g. 70g, 200ml)',
# MAGIC   mrp           DECIMAL(10,2) COMMENT 'Maximum Retail Price in INR',
# MAGIC   cost_price    DECIMAL(10,2) COMMENT 'Manufacturer cost in INR',
# MAGIC   launch_date   DATE          COMMENT 'Market introduction date',
# MAGIC   status        STRING        COMMENT 'Active | Discontinued',
# MAGIC   ingested_ts   TIMESTAMP     COMMENT 'Batch ingestion timestamp'
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Bronze: Raw CPG product master from SAP ERP (Nestle India). Updated daily.'
# MAGIC TBLPROPERTIES (
# MAGIC   'layer'          = 'bronze',
# MAGIC   'source_system'  = 'SAP_ERP',
# MAGIC   'domain'         = 'product',
# MAGIC   'owning_team'    = 'data_engineering',
# MAGIC   'refresh'        = 'daily'
# MAGIC );
# MAGIC
# MAGIC INSERT INTO d1_raw_products VALUES
# MAGIC ('P001','Maggi 2-Minute Noodles 70g', 'Maggi',  'Noodles',    'Instant Noodles',  '70g',   12.00,  7.50,'2018-01-15','Active',       current_timestamp()),
# MAGIC ('P002','Maggi Atta Noodles 80g',     'Maggi',  'Noodles',    'Wheat Noodles',    '80g',   14.00,  8.50,'2019-03-10','Active',       current_timestamp()),
# MAGIC ('P003','KitKat 4-Finger 45g',        'KitKat', 'Chocolates', 'Wafer Bars',       '45g',   40.00, 25.00,'2015-06-20','Active',       current_timestamp()),
# MAGIC ('P004','Munch Bar 35g',              'Munch',  'Chocolates', 'Chocolate Bars',   '35g',   15.00,  9.00,'2016-04-05','Active',       current_timestamp()),
# MAGIC ('P005','Nescafe Classic 200g',       'Nescafe','Beverages',  'Instant Coffee',   '200g', 350.00,210.00,'2017-09-12','Active',       current_timestamp()),
# MAGIC ('P006','Nescafe Classic 50g',        'Nescafe','Beverages',  'Instant Coffee',   '50g',   95.00, 58.00,'2017-09-12','Active',       current_timestamp()),
# MAGIC ('P007','Milo 400g Tin',              'Milo',   'Beverages',  'Malted Drinks',    '400g', 280.00,170.00,'2018-07-18','Active',       current_timestamp()),
# MAGIC ('P008','Boost Chocolate 200ml',      'Boost',  'Beverages',  'RTD Drinks',       '200ml', 25.00, 15.00,'2020-02-14','Active',       current_timestamp()),
# MAGIC ('P009','Maggi Ketchup 1kg',          'Maggi',  'Sauces',     'Ketchup',          '1kg',   85.00, 52.00,'2019-11-30','Discontinued', current_timestamp()),
# MAGIC ('P010','KitKat Dark 45g',            'KitKat', 'Chocolates', 'Wafer Bars',       '45g',   45.00, 28.00,'2021-05-10','Active',       current_timestamp()),
# MAGIC ('P011','Milkybar 12g',               'Milkybar','Chocolates','White Chocolate',  '12g',   10.00,  6.00,'2017-03-22','Active',       current_timestamp()),
# MAGIC ('P012','Nescafe Sunrise 200g',       'Nescafe','Beverages',  'Coffee-Chicory',   '200g', 195.00,118.00,'2018-05-15','Active',       current_timestamp());
# MAGIC
# MAGIC -- ────────────────────────────────────────────────────────────
# MAGIC -- TABLE 2: RETAILERS  ← Source: CRM / Trade Master
# MAGIC -- ────────────────────────────────────────────────────────────
# MAGIC CREATE OR REPLACE TABLE d1_raw_retailers (
# MAGIC   retailer_id   STRING   COMMENT 'Retailer code from CRM',
# MAGIC   retailer_name STRING   NOT NULL,
# MAGIC   channel       STRING   COMMENT 'MT=Modern Trade | GT=General Trade | EC=Ecommerce',
# MAGIC   banner        STRING   COMMENT 'Retail chain or banner name',
# MAGIC   city          STRING,
# MAGIC   state         STRING,
# MAGIC   region        STRING   COMMENT 'North | South | East | West',
# MAGIC   is_active     BOOLEAN,
# MAGIC   ingested_ts   TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Bronze: Retailer and trade customer master from CRM'
# MAGIC TBLPROPERTIES ('layer'='bronze','source_system'='CRM','domain'='retailer');
# MAGIC
# MAGIC INSERT INTO d1_raw_retailers VALUES
# MAGIC ('R001','Big Basket Mumbai',          'EC','BigBasket','Mumbai',    'Maharashtra','West', true, current_timestamp()),
# MAGIC ('R002','DMart Powai',                'MT','DMart',    'Mumbai',    'Maharashtra','West', true, current_timestamp()),
# MAGIC ('R003','Reliance Fresh Andheri',     'MT','Reliance', 'Mumbai',    'Maharashtra','West', true, current_timestamp()),
# MAGIC ('R004','Spencer New Delhi',          'MT','Spencer',  'New Delhi', 'Delhi',      'North',true, current_timestamp()),
# MAGIC ('R005','More Supermarket Bangalore', 'MT','More',     'Bangalore', 'Karnataka',  'South',true, current_timestamp()),
# MAGIC ('R006','Ramesh General Store Pune',  'GT','Kirana',   'Pune',      'Maharashtra','West', true, current_timestamp()),
# MAGIC ('R007','Flipkart Grocery',           'EC','Flipkart', 'Bangalore', 'Karnataka',  'South',true, current_timestamp()),
# MAGIC ('R008','Star Bazaar Chennai',        'MT','Star',     'Chennai',   'Tamil Nadu', 'South',true, current_timestamp()),
# MAGIC ('R009','Amazon Fresh Hyderabad',     'EC','Amazon',   'Hyderabad', 'Telangana',  'South',true, current_timestamp()),
# MAGIC ('R010','Corner Store Kolkata',       'GT','Kirana',   'Kolkata',   'West Bengal','East', false,current_timestamp());
# MAGIC
# MAGIC -- ────────────────────────────────────────────────────────────
# MAGIC -- TABLE 3: SALES TRANSACTIONS  ← Source: Order Management System
# MAGIC -- ────────────────────────────────────────────────────────────
# MAGIC CREATE OR REPLACE TABLE d1_raw_sales (
# MAGIC   order_id      STRING,
# MAGIC   order_date    DATE,
# MAGIC   product_id    STRING,
# MAGIC   retailer_id   STRING,
# MAGIC   quantity      INT            COMMENT 'Units sold (cases)',
# MAGIC   unit_price    DECIMAL(10,2)  COMMENT 'Invoice price per unit',
# MAGIC   discount_pct  DECIMAL(5,2)   COMMENT 'Trade discount as % of invoice price',
# MAGIC   net_revenue   DECIMAL(12,2)  COMMENT 'Revenue after discount: qty * price * (1 - disc/100)',
# MAGIC   promo_code    STRING         COMMENT 'Promotion code if order is promo-driven, else NULL',
# MAGIC   ingested_ts   TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Bronze: Daily sales order lines from Order Management System. Grain: 1 row = 1 product sold to 1 retailer on 1 date.'
# MAGIC TBLPROPERTIES ('layer'='bronze','source_system'='OMS','domain'='sales');
# MAGIC
# MAGIC INSERT INTO d1_raw_sales VALUES
# MAGIC ('SO001','2024-01-05','P001','R001', 500, 12.00, 5.0,  5700.00,'PROMO_JAN',current_timestamp()),
# MAGIC ('SO002','2024-01-08','P003','R002', 200, 40.00, 0.0,  8000.00, NULL,       current_timestamp()),
# MAGIC ('SO003','2024-01-12','P005','R003', 100,350.00,10.0, 31500.00,'PROMO_JAN',current_timestamp()),
# MAGIC ('SO004','2024-01-15','P002','R004', 300, 14.00, 0.0,  4200.00, NULL,       current_timestamp()),
# MAGIC ('SO005','2024-01-20','P007','R005',  80,280.00, 5.0, 21280.00,'PROMO_JAN',current_timestamp()),
# MAGIC ('SO006','2024-02-03','P001','R001', 600, 12.00, 5.0,  6840.00, NULL,       current_timestamp()),
# MAGIC ('SO007','2024-02-07','P004','R006', 150, 15.00, 0.0,  2250.00, NULL,       current_timestamp()),
# MAGIC ('SO008','2024-02-14','P003','R007', 250, 40.00, 8.0,  9200.00,'VDAY',     current_timestamp()),
# MAGIC ('SO009','2024-02-18','P008','R008', 400, 25.00, 0.0, 10000.00, NULL,       current_timestamp()),
# MAGIC ('SO010','2024-02-25','P006','R009', 120, 95.00, 5.0, 10830.00, NULL,       current_timestamp()),
# MAGIC ('SO011','2024-03-01','P010','R002', 180, 45.00, 0.0,  8100.00, NULL,       current_timestamp()),
# MAGIC ('SO012','2024-03-05','P001','R003', 800, 12.00,10.0,  8640.00,'PROMO_Q1', current_timestamp()),
# MAGIC ('SO013','2024-03-10','P007','R001',  60,280.00, 0.0, 16800.00, NULL,       current_timestamp()),
# MAGIC ('SO014','2024-03-15','P005','R004',  90,350.00,15.0, 26775.00,'PROMO_Q1', current_timestamp()),
# MAGIC ('SO015','2024-03-20','P002','R005', 350, 14.00, 0.0,  4900.00, NULL,       current_timestamp());
# MAGIC
# MAGIC -- Quick validation
# MAGIC SELECT 'Products'  AS table_name, COUNT(*) AS rows FROM d1_raw_products  UNION ALL
# MAGIC SELECT 'Retailers' AS table_name, COUNT(*) AS rows FROM d1_raw_retailers UNION ALL
# MAGIC SELECT 'Sales'     AS table_name, COUNT(*) AS rows FROM d1_raw_sales;

# COMMAND ----------

# DBTITLE 1,Lab 1.1b — Delta Lake Time Travel
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- LAB 1.1b — DELTA LAKE TIME TRAVEL
# MAGIC -- Time Travel lets you query ANY historical version of your table.
# MAGIC -- Every write (INSERT/UPDATE/DELETE/MERGE) creates a new version.
# MAGIC -- ============================================================
# MAGIC
# MAGIC USE CATALOG lakemeter_catalog;
# MAGIC USE SCHEMA labuser3;
# MAGIC
# MAGIC -- STEP 1: Check what the table looks like RIGHT NOW
# MAGIC SELECT 'CURRENT (before update)' AS snapshot, product_id, product_name, mrp, status
# MAGIC FROM d1_raw_products
# MAGIC ORDER BY product_id;

# COMMAND ----------

# DBTITLE 1,Lab 1.1b — Time Travel: Make Changes & View History
# MAGIC %sql
# MAGIC -- STEP 2: Make changes to create new versions in the Delta log
# MAGIC
# MAGIC -- UPDATE: Price adjustment for Maggi (inflation pass-through)
# MAGIC UPDATE d1_raw_products SET mrp = 15.00 WHERE product_id = 'P001';
# MAGIC
# MAGIC -- UPDATE: Discontinue a product
# MAGIC UPDATE d1_raw_products SET status = 'Discontinued' WHERE product_id = 'P002';
# MAGIC
# MAGIC -- INSERT: Add a brand-new product
# MAGIC INSERT INTO d1_raw_products VALUES
# MAGIC ('P013','Maggi Oats Noodles 75g','Maggi','Noodles','Healthy Noodles','75g',16.00,10.00,'2024-01-01','Active',current_timestamp());
# MAGIC
# MAGIC -- STEP 3: View the FULL DELTA HISTORY — every operation is recorded!
# MAGIC DESCRIBE HISTORY d1_raw_products;

# COMMAND ----------

# DBTITLE 1,Lab 1.1b — Time Travel: Query Previous Versions
# MAGIC %sql
# MAGIC -- STEP 4: Time Travel — Query the ORIGINAL data (Version 1)
# MAGIC -- Note: Version 0 = CREATE TABLE (empty), Version 1 = initial INSERT
# MAGIC -- Use Case: Audit what data looked like before a pipeline error
# MAGIC
# MAGIC SELECT 'Version 1 — Original Load' AS time_travel_snapshot,
# MAGIC        product_id, product_name, mrp, status
# MAGIC FROM d1_raw_products VERSION AS OF 1
# MAGIC WHERE product_id IN ('P001','P002','P013')
# MAGIC
# MAGIC UNION ALL
# MAGIC -- ── Compare with CURRENT ──────────────────────────────────────
# MAGIC SELECT 'CURRENT — After Updates' AS time_travel_snapshot,
# MAGIC        product_id, product_name, mrp, status
# MAGIC FROM d1_raw_products
# MAGIC WHERE product_id IN ('P001','P002','P013')
# MAGIC ORDER BY product_id, time_travel_snapshot DESC;
# MAGIC
# MAGIC -- BONUS: You can also restore a table to a previous version:
# MAGIC -- RESTORE TABLE d1_raw_products TO VERSION AS OF 1;

# COMMAND ----------

# DBTITLE 1,Lab 1.1c — MERGE Operations (Incremental Upsert)
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- LAB 1.1c — DELTA LAKE MERGE (UPSERT)
# MAGIC -- MERGE is the cornerstone of incremental loading in the Lakehouse.
# MAGIC -- One atomic statement handles: UPDATE existing, INSERT new, DELETE removed.
# MAGIC -- ============================================================
# MAGIC
# MAGIC USE CATALOG lakemeter_catalog;
# MAGIC USE SCHEMA labuser3;
# MAGIC
# MAGIC -- STEP 1: Create a STAGING table simulating the incoming ERP batch
# MAGIC CREATE OR REPLACE TABLE d1_staging_products AS
# MAGIC SELECT * FROM d1_raw_products WHERE 1 = 0; -- Empty clone of structure
# MAGIC
# MAGIC INSERT INTO d1_staging_products VALUES
# MAGIC -- Record 1: EXISTING product, PRICE CHANGED → triggers UPDATE
# MAGIC ('P001','Maggi 2-Minute Noodles 70g','Maggi','Noodles','Instant Noodles','70g',14.00,7.50,'2018-01-15','Active',current_timestamp()),
# MAGIC -- Record 2: EXISTING discontinued, RE-ACTIVATED → triggers UPDATE
# MAGIC ('P009','Maggi Ketchup 1kg','Maggi','Sauces','Ketchup','1kg',85.00,52.00,'2019-11-30','Active',current_timestamp()),
# MAGIC -- Record 3: BRAND NEW product not in target → triggers INSERT
# MAGIC ('P014','KitKat Minis 250g','KitKat','Chocolates','Wafer Bars','250g',120.00,72.00,'2024-03-01','Active',current_timestamp());
# MAGIC
# MAGIC SELECT 'Staging batch ready — 3 records to merge' AS note;
# MAGIC SELECT * FROM d1_staging_products;

# COMMAND ----------

# DBTITLE 1,Lab 1.1c — Execute MERGE & Verify Results
# MAGIC %sql
# MAGIC -- STEP 2: Execute the MERGE — Upsert staging into the target table
# MAGIC MERGE INTO d1_raw_products AS target
# MAGIC USING d1_staging_products     AS source
# MAGIC ON target.product_id = source.product_id
# MAGIC
# MAGIC -- Condition: record exists AND a tracked field has changed
# MAGIC WHEN MATCHED AND (
# MAGIC   target.mrp    <> source.mrp   OR
# MAGIC   target.status <> source.status
# MAGIC ) THEN UPDATE SET
# MAGIC   target.mrp         = source.mrp,
# MAGIC   target.status      = source.status,
# MAGIC   target.ingested_ts = current_timestamp()
# MAGIC
# MAGIC -- New record from source not present in target
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC   product_id, product_name, brand, category, sub_category,
# MAGIC   pack_size, mrp, cost_price, launch_date, status, ingested_ts
# MAGIC ) VALUES (
# MAGIC   source.product_id, source.product_name, source.brand, source.category, source.sub_category,
# MAGIC   source.pack_size, source.mrp, source.cost_price, source.launch_date, source.status, current_timestamp()
# MAGIC );
# MAGIC
# MAGIC -- STEP 3: Verify MERGE outcomes
# MAGIC SELECT
# MAGIC   product_id,
# MAGIC   product_name,
# MAGIC   mrp,
# MAGIC   status,
# MAGIC   ingested_ts,
# MAGIC   CASE
# MAGIC     WHEN product_id = 'P001' THEN '→ UPDATED: price raised to 14'
# MAGIC     WHEN product_id = 'P009' THEN '→ UPDATED: status re-activated'
# MAGIC     WHEN product_id = 'P014' THEN '→ INSERTED: brand new SKU'
# MAGIC     ELSE 'unchanged'
# MAGIC   END AS merge_outcome
# MAGIC FROM d1_raw_products
# MAGIC WHERE product_id IN ('P001','P009','P014','P002','P003')
# MAGIC ORDER BY product_id;

# COMMAND ----------

# DBTITLE 1,Module 1.2 — Unity Catalog & Data Discovery
# MAGIC %md
# MAGIC ---
# MAGIC ## 🗂️ Module 1.2 — Unity Catalog & Data Discovery
# MAGIC ### 11:00 AM – 1:00 PM · 2 Hours
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What is Unity Catalog?
# MAGIC
# MAGIC **Unity Catalog (UC)** is Databricks' unified governance layer for all data and AI assets — tables, files, models, and functions — across all Databricks workspaces in an account.
# MAGIC
# MAGIC ### 🏗️ Unity Catalog Hierarchy
# MAGIC
# MAGIC ```
# MAGIC   Account
# MAGIC   └── Metastore  (one per region)
# MAGIC       └── Catalog   (e.g., lakemeter_catalog)
# MAGIC           └── Schema  (e.g., labuser3)
# MAGIC               └── Tables, Views, Functions, Volumes
# MAGIC ```
# MAGIC
# MAGIC ### 📌 Key Unity Catalog Concepts
# MAGIC
# MAGIC | Concept | Description | SQL Command |
# MAGIC |---------|-------------|-------------|
# MAGIC | **Catalog** | Top-level namespace, maps to a data domain | `SHOW CATALOGS` |
# MAGIC | **Schema** | Logical grouping of tables within a catalog | `SHOW SCHEMAS IN catalog` |
# MAGIC | **Table** | Managed or External Delta/Parquet/CSV table | `SHOW TABLES IN schema` |
# MAGIC | **Comment** | Human-readable description on any object | `COMMENT ON TABLE t IS '...'` |
# MAGIC | **TBLPROPERTIES** | Key-value operational metadata | `ALTER TABLE t SET TBLPROPERTIES (k=v)` |
# MAGIC | **Lineage** | Auto-tracked read/write relationships | Unity Catalog UI → Lineage tab |
# MAGIC | **Access Controls** | Fine-grained GRANT / REVOKE on any object | `GRANT SELECT ON TABLE t TO user` |
# MAGIC
# MAGIC > 🏋️ **Hands-On Lab 1.2:** Explore the catalog hierarchy, describe tables, and apply metadata to your CPG bronze tables.

# COMMAND ----------

# DBTITLE 1,Lab 1.2a — Unity Catalog Discovery
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- LAB 1.2a — UNITY CATALOG NAVIGATION & DISCOVERY
# MAGIC -- Learn to navigate the 3-level namespace and explore objects
# MAGIC -- ============================================================
# MAGIC
# MAGIC -- 1. Explore available catalogs
# MAGIC SHOW CATALOGS;
# MAGIC
# MAGIC -- 2. List all schemas in your catalog (run separately or view below)
# MAGIC -- SHOW SCHEMAS IN lakemeter_catalog;
# MAGIC
# MAGIC -- 3. List Day 1 tables
# MAGIC SHOW TABLES IN lakemeter_catalog.labuser3 LIKE 'd1_*';
# MAGIC
# MAGIC -- 4. Full extended description: columns, types, partitions, properties, location
# MAGIC DESCRIBE TABLE EXTENDED lakemeter_catalog.labuser3.d1_raw_products;

# COMMAND ----------

# DBTITLE 1,Lab 1.2b — Metadata Management: Comments & TBLPROPERTIES
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- LAB 1.2b — METADATA: COMMENTS, TBLPROPERTIES & DATA PROFILE
# MAGIC -- Metadata makes data discoverable, trustworthy, and governed
# MAGIC -- ============================================================
# MAGIC
# MAGIC USE CATALOG lakemeter_catalog;
# MAGIC USE SCHEMA labuser3;
# MAGIC
# MAGIC -- 1. Table-level comments — visible in Unity Catalog UI search
# MAGIC COMMENT ON TABLE d1_raw_products IS
# MAGIC   'Bronze: CPG product master from SAP ERP. Covers Nestle India brands (Maggi, KitKat, Nescafe, Milo). Daily batch ingestion. Source: SAP ERP module MM.';
# MAGIC
# MAGIC COMMENT ON TABLE d1_raw_sales IS
# MAGIC   'Bronze: Sales order lines from OMS. Grain = 1 row per product x retailer x order date. Covers Jan-Mar 2024 Q1 transaction data.';
# MAGIC
# MAGIC -- 2. Column-level enrichment
# MAGIC ALTER TABLE d1_raw_sales
# MAGIC   ALTER COLUMN discount_pct COMMENT 'Trade discount applied as % of invoice price. Driven by negotiated terms or active promotions.';
# MAGIC
# MAGIC ALTER TABLE d1_raw_sales
# MAGIC   ALTER COLUMN net_revenue   COMMENT 'Net revenue after discount: quantity x unit_price x (1 - discount_pct/100).';
# MAGIC
# MAGIC -- 3. Operational metadata via TBLPROPERTIES
# MAGIC ALTER TABLE d1_raw_products
# MAGIC SET TBLPROPERTIES (
# MAGIC   'refresh_frequency'   = 'daily_06:00_IST',
# MAGIC   'sla_hours'           = '4',
# MAGIC   'data_classification' = 'internal',
# MAGIC   'business_owner'      = 'category_management_team',
# MAGIC   'pii_fields'          = 'none'
# MAGIC );
# MAGIC
# MAGIC -- 4. Quick data profile — understand coverage and completeness
# MAGIC SELECT
# MAGIC   COUNT(*)                                       AS total_products,
# MAGIC   COUNT(DISTINCT brand)                          AS distinct_brands,
# MAGIC   COUNT(DISTINCT category)                       AS distinct_categories,
# MAGIC   SUM(CASE WHEN status='Active' THEN 1 ELSE 0 END) AS active_skus,
# MAGIC   ROUND(AVG((mrp - cost_price)/mrp*100),1)       AS avg_gross_margin_pct,
# MAGIC   MIN(launch_date)                               AS earliest_launch,
# MAGIC   MAX(launch_date)                               AS latest_launch
# MAGIC FROM d1_raw_products;
# MAGIC
# MAGIC -- Verify properties applied
# MAGIC SHOW TBLPROPERTIES d1_raw_products;

# COMMAND ----------

# DBTITLE 1,Module 1.3 — Databricks SQL for Modellers
# MAGIC %md
# MAGIC ---
# MAGIC ## 📝 Module 1.3 — Databricks SQL for Modellers
# MAGIC ### 1:45 – 3:15 PM · 1.5 Hours
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Databricks SQL is a fully ANSI-compatible SQL engine with powerful extensions. As a data modeller you'll use these constructs daily:
# MAGIC
# MAGIC | SQL Feature | When to Use It | Pattern |
# MAGIC |-------------|---------------|----------|
# MAGIC | **DDL (CREATE TABLE / ALTER)** | Define table structures with types, constraints, comments | `CREATE OR REPLACE TABLE t (col TYPE COMMENT '...')` |
# MAGIC | **CTAS (CREATE TABLE AS SELECT)** | Build Silver/Gold tables from upstream queries | `CREATE OR REPLACE TABLE gold AS SELECT ...` |
# MAGIC | **Views** | Reusable named queries — no data stored | `CREATE OR REPLACE VIEW v AS SELECT ...` |
# MAGIC | **CTEs** | Break complex queries into readable named steps | `WITH cte AS (SELECT ...) SELECT * FROM cte` |
# MAGIC | **JOINs** | Combine fact + dimension tables | `INNER/LEFT/CROSS JOIN` |
# MAGIC | **Window Functions** | Rank, running totals, period-over-period | `RANK() OVER (PARTITION BY ... ORDER BY ...)` |
# MAGIC | **CONSTRAINTS** | Enforce data integrity rules | `NOT NULL`, `CHECK`, `PRIMARY KEY` (informational) |
# MAGIC | **SQL Warehouses** | Serverless compute for BI queries | Auto-selected in this lab |
# MAGIC
# MAGIC > 🏋️ **Hands-On Lab 1.3:** Build CTAS tables, create views, write CTEs, and apply window functions on your CPG data.

# COMMAND ----------

# DBTITLE 1,Lab 1.3a — DDL, CTAS & Product Profiling
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- LAB 1.3a — DDL & CTAS: Build a Derived Profiling Table
# MAGIC -- CTAS (Create Table As Select) is the primary pattern
# MAGIC -- for building Silver and Gold layer tables in the Lakehouse
# MAGIC -- ============================================================
# MAGIC
# MAGIC USE CATALOG lakemeter_catalog;
# MAGIC USE SCHEMA labuser3;
# MAGIC
# MAGIC -- CTAS — Enrich the raw product table with calculated attributes
# MAGIC CREATE OR REPLACE TABLE d1_profile_products AS
# MAGIC SELECT
# MAGIC   product_id,
# MAGIC   product_name,
# MAGIC   brand,
# MAGIC   category,
# MAGIC   sub_category,
# MAGIC   pack_size,
# MAGIC   mrp,
# MAGIC   cost_price,
# MAGIC   -- Derived: Gross margin
# MAGIC   ROUND((mrp - cost_price) / mrp * 100, 2)           AS gross_margin_pct,
# MAGIC   launch_date,
# MAGIC   -- Derived: Days since launch (age of the SKU)
# MAGIC   DATEDIFF(current_date(), launch_date)               AS days_since_launch,
# MAGIC   -- Derived: Product maturity classification
# MAGIC   CASE
# MAGIC     WHEN DATEDIFF(current_date(), launch_date) <= 365  THEN 'New Launch'
# MAGIC     WHEN DATEDIFF(current_date(), launch_date) <= 1095 THEN 'Growing'
# MAGIC     ELSE 'Established'
# MAGIC   END                                                  AS product_maturity,
# MAGIC   -- Derived: Price tier
# MAGIC   CASE
# MAGIC     WHEN mrp < 20   THEN 'Economy (<₹20)'
# MAGIC     WHEN mrp < 100  THEN 'Mid-Range (₹20-100)'
# MAGIC     ELSE                 'Premium (>₹100)'
# MAGIC   END                                                  AS price_tier,
# MAGIC   status
# MAGIC FROM d1_raw_products;
# MAGIC
# MAGIC -- View enriched output
# MAGIC SELECT brand, category, product_maturity, price_tier,
# MAGIC        COUNT(*) AS skus, ROUND(AVG(gross_margin_pct),1) AS avg_margin
# MAGIC FROM d1_profile_products
# MAGIC GROUP BY brand, category, product_maturity, price_tier
# MAGIC ORDER BY brand, avg_margin DESC;

# COMMAND ----------

# DBTITLE 1,Lab 1.3b — Views & CTEs
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- LAB 1.3b — VIEWS & CTEs
# MAGIC -- Views = reusable named queries (no data stored)
# MAGIC -- CTEs = readable, composable SQL building blocks
# MAGIC -- ============================================================
# MAGIC
# MAGIC USE CATALOG lakemeter_catalog;
# MAGIC USE SCHEMA labuser3;
# MAGIC
# MAGIC -- 1. CREATE A VIEW — Business-friendly layer over raw data
# MAGIC CREATE OR REPLACE VIEW d1_v_active_products AS
# MAGIC SELECT
# MAGIC   product_id,
# MAGIC   product_name,
# MAGIC   brand,
# MAGIC   category,
# MAGIC   mrp,
# MAGIC   ROUND((mrp - cost_price) / mrp * 100, 2) AS margin_pct
# MAGIC FROM d1_raw_products
# MAGIC WHERE status = 'Active';
# MAGIC
# MAGIC -- Test: brand performance from the view
# MAGIC SELECT brand,
# MAGIC        COUNT(*)                   AS active_skus,
# MAGIC        ROUND(MIN(mrp),0)          AS min_mrp,
# MAGIC        ROUND(MAX(mrp),0)          AS max_mrp,
# MAGIC        ROUND(AVG(margin_pct),1)   AS avg_margin_pct
# MAGIC FROM d1_v_active_products
# MAGIC GROUP BY brand
# MAGIC ORDER BY avg_margin_pct DESC;

# COMMAND ----------

# DBTITLE 1,Lab 1.3b — CTE: Multi-Step Sales Analysis
# MAGIC %sql
# MAGIC -- 2. CTEs — Break complex logic into readable, named steps
# MAGIC -- Business Question: Rank retailers by revenue and identify their top-selling brand
# MAGIC
# MAGIC USE CATALOG lakemeter_catalog;
# MAGIC USE SCHEMA labuser3;
# MAGIC
# MAGIC WITH
# MAGIC -- CTE 1: Enrich sales with product and retailer details
# MAGIC enriched AS (
# MAGIC   SELECT
# MAGIC     s.order_id,
# MAGIC     s.order_date,
# MAGIC     r.retailer_name,
# MAGIC     r.channel,
# MAGIC     r.region,
# MAGIC     p.brand,
# MAGIC     p.category,
# MAGIC     s.quantity,
# MAGIC     s.net_revenue
# MAGIC   FROM d1_raw_sales     s
# MAGIC   JOIN d1_raw_retailers r ON s.retailer_id = r.retailer_id
# MAGIC   JOIN d1_raw_products  p ON s.product_id  = p.product_id
# MAGIC ),
# MAGIC
# MAGIC -- CTE 2: Summarise total revenue and units per retailer
# MAGIC retailer_summary AS (
# MAGIC   SELECT
# MAGIC     retailer_name, channel, region,
# MAGIC     COUNT(DISTINCT order_id)          AS orders,
# MAGIC     SUM(quantity)                      AS total_units,
# MAGIC     ROUND(SUM(net_revenue), 0)         AS total_revenue
# MAGIC   FROM enriched
# MAGIC   GROUP BY retailer_name, channel, region
# MAGIC ),
# MAGIC
# MAGIC -- CTE 3: Find top brand per retailer
# MAGIC top_brand AS (
# MAGIC   SELECT
# MAGIC     retailer_name,
# MAGIC     brand,
# MAGIC     SUM(net_revenue)                   AS brand_revenue,
# MAGIC     RANK() OVER (
# MAGIC       PARTITION BY retailer_name
# MAGIC       ORDER BY SUM(net_revenue) DESC
# MAGIC     )                                   AS brand_rank
# MAGIC   FROM enriched
# MAGIC   GROUP BY retailer_name, brand
# MAGIC )
# MAGIC
# MAGIC -- Final: Join summary with top brand
# MAGIC SELECT
# MAGIC   rs.retailer_name,
# MAGIC   rs.channel,
# MAGIC   rs.region,
# MAGIC   rs.orders,
# MAGIC   rs.total_units,
# MAGIC   rs.total_revenue,
# MAGIC   tb.brand                               AS top_brand,
# MAGIC   RANK() OVER (ORDER BY rs.total_revenue DESC) AS retailer_rank
# MAGIC FROM retailer_summary rs
# MAGIC JOIN top_brand        tb ON rs.retailer_name = tb.retailer_name AND tb.brand_rank = 1
# MAGIC ORDER BY rs.total_revenue DESC;

# COMMAND ----------

# DBTITLE 1,Lab 1.3c — Window Functions: Ranking, Running Totals & MoM
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- LAB 1.3c — WINDOW FUNCTIONS
# MAGIC -- Compute analytics OVER a window of rows WITHOUT collapsing them.
# MAGIC -- Essential for: ranking, running totals, moving averages, MoM.
# MAGIC -- ============================================================
# MAGIC
# MAGIC USE CATALOG lakemeter_catalog;
# MAGIC USE SCHEMA labuser3;
# MAGIC
# MAGIC WITH monthly_brand AS (
# MAGIC   SELECT
# MAGIC     DATE_FORMAT(s.order_date,'yyyy-MM') AS yr_month,
# MAGIC     p.brand,
# MAGIC     SUM(s.net_revenue)                   AS revenue,
# MAGIC     SUM(s.quantity)                       AS units
# MAGIC   FROM d1_raw_sales    s
# MAGIC   JOIN d1_raw_products p ON s.product_id = p.product_id
# MAGIC   GROUP BY DATE_FORMAT(s.order_date,'yyyy-MM'), p.brand
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   yr_month,
# MAGIC   brand,
# MAGIC   revenue,
# MAGIC   units,
# MAGIC
# MAGIC   -- 1. RANK: brand revenue rank within each month
# MAGIC   RANK() OVER (
# MAGIC     PARTITION BY yr_month
# MAGIC     ORDER BY revenue DESC
# MAGIC   )                                     AS rank_in_month,
# MAGIC
# MAGIC   -- 2. RUNNING TOTAL: cumulative revenue per brand over time
# MAGIC   SUM(revenue) OVER (
# MAGIC     PARTITION BY brand
# MAGIC     ORDER BY yr_month
# MAGIC     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC   )                                     AS cumulative_revenue,
# MAGIC
# MAGIC   -- 3. LAG: previous month's revenue (for MoM comparison)
# MAGIC   LAG(revenue,1) OVER (
# MAGIC     PARTITION BY brand ORDER BY yr_month
# MAGIC   )                                     AS prev_month_revenue,
# MAGIC
# MAGIC   -- 4. MoM GROWTH %
# MAGIC   ROUND(
# MAGIC     (revenue - LAG(revenue,1) OVER (PARTITION BY brand ORDER BY yr_month))
# MAGIC     / NULLIF(LAG(revenue,1) OVER (PARTITION BY brand ORDER BY yr_month),0) * 100
# MAGIC   , 1)                                  AS mom_growth_pct
# MAGIC
# MAGIC FROM monthly_brand
# MAGIC ORDER BY yr_month, rank_in_month;

# COMMAND ----------

# DBTITLE 1,Module 1.4 — CPG Domain & Modelling Requirements
# MAGIC %md
# MAGIC ---
# MAGIC ## 🏪 Module 1.4 — CPG Domain & Modelling Requirements
# MAGIC ### 3:15 – 5:00 PM · 1.75 Hours
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What is CPG (Consumer Packaged Goods)?
# MAGIC
# MAGIC **CPG** = fast-moving packaged consumer products: food, beverages, personal care, household items.
# MAGIC Major CPG companies: Nestle, Unilever, P&G, HUL, ITC. This lab uses **Nestle India** as the reference company.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏢 CPG Business Domains
# MAGIC
# MAGIC | Domain | Description | Key Entities | Business Questions |
# MAGIC |--------|-------------|-------------|--------------------|
# MAGIC | **Product** | Brand → Category → Sub-Cat → SKU hierarchy | Products, Brands, Categories, Pack Sizes | Which brands have >30% margin? |
# MAGIC | **Retailer/Customer** | Who buys from us | Modern Trade, General Trade, Ecommerce | What is DMart's Q1 offtake? |
# MAGIC | **Sales** | Revenue, volume, price realisation | Orders, Invoices, Discounts, Net Revenue | What is Q1 net revenue by brand+channel? |
# MAGIC | **Distribution** | Product availability coverage | Numeric Dist %, Weighted Dist % | What % of stores stock Maggi Noodles? |
# MAGIC | **Trade Promotions** | Discounts, schemes, in-store activations | Promos, Mechanics, Promo Spend, ROI | What % of sales is promo-driven? |
# MAGIC | **Supply Chain** | Inventory, logistics, demand planning | Stock, Days Cover, Fill Rate | What is warehouse inventory days cover? |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📏 Defining the Grain
# MAGIC
# MAGIC > **"The grain is the single most important design decision in dimensional modelling."** — Ralph Kimball
# MAGIC
# MAGIC The **grain** defines what ONE row in a fact table represents:
# MAGIC
# MAGIC | Grain Definition | Fact Table Type | Example Table |
# MAGIC |-----------------|-----------------|---------------|
# MAGIC | 1 sales order line: product × retailer × date | Transactional | `fact_sales_transaction` |
# MAGIC | 1 inventory snapshot: product × warehouse × week-end | Periodic Snapshot | `fact_inventory_snapshot` |
# MAGIC | 1 event: product promoted at a retailer in a week | Factless Fact | `fact_promotion_coverage` |
# MAGIC | 1 order lifecycle: from placement to delivery | Accumulating Snapshot | `fact_order_lifecycle` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🗺️ CPG Process → Dimensional Model Mapping
# MAGIC
# MAGIC | Business Process | Grain | Dimensions | Measures / Facts |
# MAGIC |-----------------|-------|-----------|------------------|
# MAGIC | Sales Ordering | Order line | Product, Retailer, Date, Promotion | Units, Gross Revenue, Net Revenue, Discount ₹ |
# MAGIC | Inventory Mgmt | Product × Warehouse × Week | Product, Warehouse, Date | Opening Stock, Closing Stock, Days Cover |
# MAGIC | Trade Promotion | Promo × Retailer × Week | Product, Retailer, Promotion, Date | Promo Spend, Incremental Units, ROI % |
# MAGIC | Distribution | Product × Route × Day | Product, Route, Retailer, Date | Numeric Dist %, Weighted Dist % |
# MAGIC
# MAGIC > 🏋️ **Hands-On Lab 1.4:** Use the SQL below to profile your CPG data, define grains, and document model requirements.

# COMMAND ----------

# DBTITLE 1,Lab 1.4 — CPG Business Requirements Profiling
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- LAB 1.4 — CPG BUSINESS REQUIREMENTS PROFILING
# MAGIC -- Understand the data landscape before designing the model.
# MAGIC -- Profile: volumes, cardinalities, coverage, quality gaps.
# MAGIC -- ============================================================
# MAGIC
# MAGIC USE CATALOG lakemeter_catalog;
# MAGIC USE SCHEMA labuser3;
# MAGIC
# MAGIC -- ── 1. PRODUCT DOMAIN ───────────────────────────────────────
# MAGIC SELECT '1. Products' AS domain,
# MAGIC   COUNT(*)                                           AS total_skus,
# MAGIC   COUNT(DISTINCT brand)                              AS brands,
# MAGIC   COUNT(DISTINCT category)                           AS categories,
# MAGIC   SUM(CASE WHEN status='Active' THEN 1 ELSE 0 END)   AS active_skus,
# MAGIC   ROUND(AVG((mrp-cost_price)/mrp*100),1)             AS avg_margin_pct,
# MAGIC   MIN(mrp) AS min_price, MAX(mrp) AS max_price
# MAGIC FROM d1_raw_products
# MAGIC
# MAGIC UNION ALL
# MAGIC -- ── 2. RETAILER DOMAIN ──────────────────────────────────────
# MAGIC SELECT '2. Retailers',
# MAGIC   COUNT(*), COUNT(DISTINCT channel), COUNT(DISTINCT region),
# MAGIC   SUM(CASE WHEN is_active THEN 1 ELSE 0 END),
# MAGIC   NULL, NULL, NULL
# MAGIC FROM d1_raw_retailers
# MAGIC
# MAGIC UNION ALL
# MAGIC -- ── 3. SALES DOMAIN ─────────────────────────────────────────
# MAGIC SELECT '3. Sales Orders',
# MAGIC   COUNT(*), COUNT(DISTINCT product_id), COUNT(DISTINCT retailer_id),
# MAGIC   SUM(CASE WHEN promo_code IS NOT NULL THEN 1 ELSE 0 END),
# MAGIC   ROUND(SUM(net_revenue),0), MIN(net_revenue), MAX(net_revenue)
# MAGIC FROM d1_raw_sales;
# MAGIC
# MAGIC -- ── 4. CHANNEL & REGION REVENUE BREAKDOWN ───────────────────
# MAGIC SELECT
# MAGIC   r.channel,
# MAGIC   r.region,
# MAGIC   COUNT(DISTINCT s.order_id)      AS orders,
# MAGIC   SUM(s.quantity)                  AS total_units,
# MAGIC   ROUND(SUM(s.net_revenue),0)      AS total_revenue,
# MAGIC   ROUND(AVG(s.discount_pct),1)     AS avg_discount_pct,
# MAGIC   COUNT(CASE WHEN s.promo_code IS NOT NULL THEN 1 END) AS promo_orders
# MAGIC FROM d1_raw_sales     s
# MAGIC JOIN d1_raw_retailers r ON s.retailer_id = r.retailer_id
# MAGIC GROUP BY r.channel, r.region
# MAGIC ORDER BY total_revenue DESC;
# MAGIC
# MAGIC -- ── 5. TOP BRANDS BY REVENUE ────────────────────────────────
# MAGIC SELECT
# MAGIC   p.brand,
# MAGIC   p.category,
# MAGIC   COUNT(DISTINCT s.order_id) AS orders,
# MAGIC   SUM(s.quantity)             AS units,
# MAGIC   ROUND(SUM(s.net_revenue),0) AS net_revenue,
# MAGIC   ROUND(AVG(s.discount_pct),1) AS avg_disc_pct
# MAGIC FROM d1_raw_sales    s
# MAGIC JOIN d1_raw_products p ON s.product_id = p.product_id
# MAGIC GROUP BY p.brand, p.category
# MAGIC ORDER BY net_revenue DESC;

# COMMAND ----------

# DBTITLE 1,Day 1 — Summary & Day 2 Preview
# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ Day 1 Summary — Key Takeaways
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏆 What You Accomplished Today
# MAGIC
# MAGIC | Module | Topic | Key Output |
# MAGIC |--------|-------|------------|
# MAGIC | **1.1** | Lakehouse Foundations | Bronze Delta tables · Time Travel demo · MERGE upsert |
# MAGIC | **1.2** | Unity Catalog | Catalogued tables · Comments · TBLPROPERTIES · Data profile |
# MAGIC | **1.3** | SQL for Modellers | CTAS · Views · CTEs · Window functions (RANK, LAG, SUM OVER) |
# MAGIC | **1.4** | CPG Domain | Profiled CPG data · Defined grains · Process-to-model mapping |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Core Concepts to Remember
# MAGIC
# MAGIC | Concept | One-Liner |
# MAGIC |---------|----------|
# MAGIC | **Lakehouse** | Data Lake openness + Data Warehouse governance — on one platform |
# MAGIC | **Delta Lake** | Open format: ACID, Time Travel, MERGE, Schema Evolution over Parquet |
# MAGIC | **Bronze Layer** | Raw data "as-is" — land first, validate later |
# MAGIC | **Time Travel** | `VERSION AS OF n` — query or restore any historical state |
# MAGIC | **MERGE** | One atomic statement: INSERT new + UPDATE changed + DELETE removed |
# MAGIC | **Unity Catalog** | 3-level namespace: Catalog → Schema → Table. Governance built in. |
# MAGIC | **Grain** | What ONE row means — the most important modelling decision |
# MAGIC | **CTE** | `WITH name AS (...)` — readable, composable SQL building blocks |
# MAGIC | **Window Functions** | Compute analytics OVER rows without collapsing: RANK, LAG, SUM OVER |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 Coming Up — Day 2 Preview
# MAGIC
# MAGIC Tomorrow you will design and build a **complete CPG Star Schema**:
# MAGIC
# MAGIC 1. **Module 2.1:** Star vs Snowflake schemas · Surrogate keys · Conformed dimensions
# MAGIC 2. **Module 2.2:** Build all 5 Dimension tables — Product, Retailer, Geography, Date (Fiscal Calendar), Promotion
# MAGIC 3. **Module 2.3:** Build all 4 Fact tables — Sales Transaction, Inventory Snapshot, Factless Promotion Coverage, Accumulating Order Lifecycle
# MAGIC 4. **Module 2.4:** Implement the Medallion Architecture end-to-end — Bronze → Silver → Gold with lineage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📚 Homework / Self-Study
# MAGIC
# MAGIC - Experiment with `RESTORE TABLE d1_raw_products TO VERSION AS OF 0` and observe the effect
# MAGIC - Add a `d1_raw_promotions` table with 5 promotion records and merge it with `d1_raw_sales`
# MAGIC - Read: [Databricks Delta Lake Documentation](https://docs.databricks.com/delta/index.html)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC > *Day 1 complete! Excellent work. See you tomorrow for the heart of dimensional modelling.* 👏