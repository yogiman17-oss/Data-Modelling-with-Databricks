# Databricks notebook source
# DBTITLE 1,Day 1 Lab Overview
# MAGIC %md
# MAGIC # Day 1 Lab Exercises: Lakehouse Foundations, Unity Catalog & SQL for Modellers
# MAGIC
# MAGIC Hands-on practice for Day 1 of the **Databricks Data Modelling for CPG** programme. You work as a data modeller at **SnackCo**, a CPG manufacturer selling noodles, snacks, beverages, dairy and home-care products through Indian retail chains.
# MAGIC
# MAGIC | # | Exercise | Day 1 module | Time |
# MAGIC |---|---|---|---|
# MAGIC | 1.1 | Delta Lake & Time Travel: recover from a bad DELETE | 1.1 Lakehouse Foundations | 20 min |
# MAGIC | 1.2 | Unity Catalog metadata: comments and tags | 1.2 Unity Catalog & Data Discovery | 20 min |
# MAGIC | 1.3 | Upsert the product master with MERGE | 1.1 / 1.4 Lakehouse & CPG Domain | 25 min |
# MAGIC | 1.4 | CTEs and window functions: top SKU and MoM growth | 1.3 SQL for Modellers | 25 min |
# MAGIC | SC | Self-Check: automated validation of all 4 exercises | All | 5 min |
# MAGIC
# MAGIC **Total: about 95 minutes.**
# MAGIC
# MAGIC ### How to work through this lab
# MAGIC 1. Set your catalog and schema in **Lab Configuration** and run it.
# MAGIC 2. Run **Lab Setup** once. It creates the source tables (`lx1_raw_*`) and resets your exercise objects, so you can always start again.
# MAGIC 3. For each exercise, read the documentation cell, then write your SQL in the **Your Work** cell below it.
# MAGIC 4. Stuck? Open the **Reference solution** at the bottom of each documentation cell, but try on your own first.
# MAGIC 5. Run **Self-Check** at the end. Every check shows PASS or FAIL with a hint.
# MAGIC
# MAGIC ### Source data created by the setup
# MAGIC | Table | Grain | Rows | Key columns |
# MAGIC |---|---|---|---|
# MAGIC | `lx1_raw_products` | One row per SKU | 20 | sku, product_name, brand, category, pack_size, list_price |
# MAGIC | `lx1_raw_sales` | One row per POS transaction (Jan to Jun 2026) | 2,400 | txn_id, txn_date, sku, region, retailer, channel, qty, unit_price, discount_amt |
# MAGIC | `lx1_product_updates` | Nightly product feed (3 changed + 2 new SKUs) | 5 | same columns as lx1_raw_products |
# MAGIC
# MAGIC **Net sales** in this lab = `qty * unit_price - discount_amt`.

# COMMAND ----------

# DBTITLE 1,Lab Configuration
# Set your own catalog and schema here, then run this cell.
CATALOG = "lakemeter_catalog"
SCHEMA = "labuser3"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"Working in {CATALOG}.{SCHEMA}. All lab objects use the prefix lx1_")

# COMMAND ----------

# DBTITLE 1,Lab Setup - Create Source Data
# MAGIC %sql
# MAGIC -- Creates the Day 1 source tables and resets your exercise objects. Safe to re-run at any time.
# MAGIC
# MAGIC CREATE OR REPLACE TABLE lx1_raw_products AS SELECT * FROM VALUES
# MAGIC   ('SKU001','Maggi Masala 70g','Maggi','Noodles','70g',14.0),        ('SKU002','Maggi Masala 280g','Maggi','Noodles','280g',52.0),
# MAGIC   ('SKU003','Maggi Atta 75g','Maggi','Noodles','75g',25.0),          ('SKU004','Lays Classic 52g','Lays','Snacks','52g',20.0),
# MAGIC   ('SKU005','Lays Magic Masala 90g','Lays','Snacks','90g',30.0),     ('SKU006','Kurkure Masala 90g','Kurkure','Snacks','90g',20.0),
# MAGIC   ('SKU007','Kurkure Chilli 45g','Kurkure','Snacks','45g',10.0),     ('SKU008','Tropicana Orange 1L','Tropicana','Beverages','1L',110.0),
# MAGIC   ('SKU009','Tropicana Mixed 1L','Tropicana','Beverages','1L',115.0),('SKU010','Tropicana Apple 200ml','Tropicana','Beverages','200ml',25.0),
# MAGIC   ('SKU011','Dove Soap 100g','Dove','Personal Care','100g',60.0),    ('SKU012','Dove Shampoo 340ml','Dove','Personal Care','340ml',325.0),
# MAGIC   ('SKU013','Surf Excel 1kg','Surf Excel','Home Care','1kg',140.0),  ('SKU014','Surf Excel 500g','Surf Excel','Home Care','500g',75.0),
# MAGIC   ('SKU015','Parle-G 250g','Parle-G','Biscuits','250g',25.0),        ('SKU016','Parle-G 800g','Parle-G','Biscuits','800g',80.0),
# MAGIC   ('SKU017','Amul Butter 100g','Amul','Dairy','100g',58.0),          ('SKU018','Amul Butter 500g','Amul','Dairy','500g',275.0),
# MAGIC   ('SKU019','Amul Cheese 200g','Amul','Dairy','200g',135.0),         ('SKU020','Amul Ghee 1L','Amul','Dairy','1L',650.0)
# MAGIC AS t(sku, product_name, brand, category, pack_size, list_price);
# MAGIC
# MAGIC CREATE OR REPLACE TABLE lx1_raw_sales AS
# MAGIC WITH g AS (
# MAGIC   SELECT id,
# MAGIC     concat('SKU', lpad(CAST(pmod(hash(id,'s'),20)+1 AS STRING),3,'0'))                                     AS sku,
# MAGIC     element_at(array('North','South','East','West','Central'), pmod(hash(id,'r'),5)+1)                       AS region,
# MAGIC     element_at(array('D-Mart','Reliance Smart','Big Bazaar','More','Spencers','Star Bazaar'), pmod(hash(id,'t'),6)+1) AS retailer,
# MAGIC     element_at(array('Modern Trade','General Trade','E-Commerce'), pmod(hash(id,'c'),3)+1)                   AS channel,
# MAGIC     pmod(hash(id,'q'),12)+1 AS qty,
# MAGIC     pmod(hash(id,'d'),15)   AS disc_pct
# MAGIC   FROM range(2400))
# MAGIC SELECT CAST(g.id + 100000 AS BIGINT) AS txn_id,
# MAGIC        date_add(DATE'2026-01-01', CAST(pmod(hash(g.id,'dt'),181) AS INT)) AS txn_date,
# MAGIC        g.sku, g.region, g.retailer, g.channel,
# MAGIC        CAST(g.qty AS INT) AS qty, p.list_price AS unit_price,
# MAGIC        ROUND(g.qty * p.list_price * g.disc_pct / 100, 2) AS discount_amt
# MAGIC FROM g JOIN lx1_raw_products p ON g.sku = p.sku;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE lx1_product_updates AS SELECT * FROM VALUES
# MAGIC   ('SKU001','Maggi Masala 70g','Maggi','Noodles','70g',15.0),
# MAGIC   ('SKU008','Tropicana Orange 1L','Tropicana','Beverages','1L',120.0),
# MAGIC   ('SKU013','Surf Excel 1kg','Surf Excel','Home Care','1kg',150.0),
# MAGIC   ('SKU021','Maggi Hot Heads 71g','Maggi','Noodles','71g',35.0),
# MAGIC   ('SKU022','Lays Cream Onion 52g','Lays','Snacks','52g',20.0)
# MAGIC AS t(sku, product_name, brand, category, pack_size, list_price);
# MAGIC
# MAGIC -- Reset exercise objects
# MAGIC DROP TABLE IF EXISTS lx1_bronze_sales;
# MAGIC DROP TABLE IF EXISTS lx1_silver_products;
# MAGIC DROP VIEW  IF EXISTS lx1_v_top_sku_by_region;
# MAGIC DROP VIEW  IF EXISTS lx1_v_monthly_mom;
# MAGIC
# MAGIC SELECT 'lx1_raw_products' AS table_name, COUNT(*) AS row_count FROM lx1_raw_products
# MAGIC UNION ALL SELECT 'lx1_raw_sales', COUNT(*) FROM lx1_raw_sales
# MAGIC UNION ALL SELECT 'lx1_product_updates', COUNT(*) FROM lx1_product_updates;

# COMMAND ----------

# DBTITLE 1,Exercise 1.1 - Delta Lake & Time Travel
# MAGIC %md
# MAGIC ## Exercise 1.1: Delta Lake & Time Travel (20 minutes)
# MAGIC
# MAGIC **Objective:** Use the Delta transaction log to inspect table history, read an older version, and roll back a bad change.
# MAGIC
# MAGIC **Business scenario:** Last night a developer ran a DELETE on the Bronze POS table with the wrong filter and removed every **North** region transaction. The North sales head needs the figures for a 10 AM review. Re-ingesting would take hours, so you will recover the data with Time Travel.
# MAGIC
# MAGIC **Concepts recap**
# MAGIC * Every write to a Delta table creates a new **version** in the transaction log.
# MAGIC * `DESCRIBE HISTORY` lists versions, operations, users and metrics.
# MAGIC * `VERSION AS OF n` / `TIMESTAMP AS OF ts` reads an older snapshot without changing the table.
# MAGIC * `RESTORE TABLE ... TO VERSION AS OF n` rolls the table back and is itself recorded as a new version.
# MAGIC
# MAGIC **Steps**
# MAGIC 1. Drop `lx1_bronze_sales` if it exists, then create it with `CREATE TABLE ... AS SELECT * FROM lx1_raw_sales`. (Dropping first makes the new table start at version 0.)
# MAGIC 2. Record the row count, then simulate the incident: `DELETE` all rows where `region = 'North'`.
# MAGIC 3. Run `DESCRIBE HISTORY lx1_bronze_sales` and find the version before the DELETE.
# MAGIC 4. Compare row counts of the current table and the earlier version using `VERSION AS OF`.
# MAGIC 5. Roll back with `RESTORE TABLE`.
# MAGIC
# MAGIC **Expected result:** `lx1_bronze_sales` has all 2,400 rows again, and its history shows CREATE, DELETE and RESTORE operations.
# MAGIC
# MAGIC **Tip:** If RESTORE fails with `ConcurrentWriteException`, the platform ran an automatic compaction at the same moment. Simply re-run the RESTORE.
# MAGIC
# MAGIC <details><summary><b>Reference solution (open only if stuck)</b></summary>
# MAGIC
# MAGIC ```sql
# MAGIC DROP TABLE IF EXISTS lx1_bronze_sales;
# MAGIC CREATE TABLE lx1_bronze_sales AS SELECT * FROM lx1_raw_sales;
# MAGIC
# MAGIC DELETE FROM lx1_bronze_sales WHERE region = 'North';
# MAGIC
# MAGIC DESCRIBE HISTORY lx1_bronze_sales;
# MAGIC
# MAGIC SELECT 'current' AS snapshot, COUNT(*) AS row_count FROM lx1_bronze_sales
# MAGIC UNION ALL
# MAGIC SELECT 'version 0', COUNT(*) FROM lx1_bronze_sales VERSION AS OF 0;
# MAGIC
# MAGIC RESTORE TABLE lx1_bronze_sales TO VERSION AS OF 0;
# MAGIC SELECT COUNT(*) AS rows_after_restore FROM lx1_bronze_sales;
# MAGIC ```
# MAGIC </details>

# COMMAND ----------

# DBTITLE 1,Exercise 1.1 - Your Work
# MAGIC %sql
# MAGIC -- Exercise 1.1: write your SQL below, then run this cell.
# MAGIC -- Step 1: DROP TABLE IF EXISTS lx1_bronze_sales; CREATE TABLE lx1_bronze_sales AS SELECT ...
# MAGIC -- Step 2: DELETE the North region rows
# MAGIC -- Step 3: DESCRIBE HISTORY ...
# MAGIC -- Step 4: compare COUNT(*) now vs VERSION AS OF 0
# MAGIC -- Step 5: RESTORE TABLE ... TO VERSION AS OF ...
# MAGIC
# MAGIC SELECT 'Exercise 1.1: add your SQL above this line' AS status;

# COMMAND ----------

# DBTITLE 1,Exercise 1.2 - Unity Catalog Metadata
# MAGIC %md
# MAGIC ## Exercise 1.2: Unity Catalog Metadata - Comments and Tags (20 minutes)
# MAGIC
# MAGIC **Objective:** Make a table discoverable and self-documenting using Unity Catalog comments and tags.
# MAGIC
# MAGIC **Business scenario:** New analysts and the Genie Space keep misreading the Bronze sales columns; one team treated `discount_amt` as a percentage. The data steward asks you to document the table so its meaning is visible in Catalog Explorer, in `DESCRIBE`, and to AI tools, and to tag it with its quality tier.
# MAGIC
# MAGIC **Concepts recap**
# MAGIC * `COMMENT ON TABLE` and `ALTER TABLE ... ALTER COLUMN ... COMMENT` store descriptions in Unity Catalog.
# MAGIC * `ALTER TABLE ... SET TAGS ('key' = 'value')` adds searchable key-value tags.
# MAGIC * This workspace has **governed tags** with restricted values (e.g. `domain`, `sensitivity`). Use the free-form key `quality_tier` for this exercise.
# MAGIC * `information_schema.tables`, `.columns` and `.table_tags` expose this metadata to SQL.
# MAGIC
# MAGIC **Steps**
# MAGIC 1. Add a meaningful table comment to `lx1_bronze_sales`.
# MAGIC 2. Add column comments to at least **3** columns: `qty`, `unit_price`, `discount_amt` (include units such as INR).
# MAGIC 3. Tag the table with `quality_tier = 'bronze'`.
# MAGIC 4. Verify with `DESCRIBE TABLE EXTENDED` and a query on `information_schema.table_tags`.
# MAGIC
# MAGIC **Expected result:** The table comment, 3 or more column comments and the `quality_tier` tag are all visible.
# MAGIC
# MAGIC <details><summary><b>Reference solution (open only if stuck)</b></summary>
# MAGIC
# MAGIC ```sql
# MAGIC COMMENT ON TABLE lx1_bronze_sales IS 'Bronze POS sales from retail outlets, loaded as received (Jan-Jun 2026)';
# MAGIC
# MAGIC ALTER TABLE lx1_bronze_sales ALTER COLUMN qty          COMMENT 'Units sold in the transaction';
# MAGIC ALTER TABLE lx1_bronze_sales ALTER COLUMN unit_price   COMMENT 'List price per unit in INR';
# MAGIC ALTER TABLE lx1_bronze_sales ALTER COLUMN discount_amt COMMENT 'Trade discount amount in INR (not a percentage)';
# MAGIC
# MAGIC ALTER TABLE lx1_bronze_sales SET TAGS ('quality_tier' = 'bronze');
# MAGIC
# MAGIC DESCRIBE TABLE EXTENDED lx1_bronze_sales;
# MAGIC SELECT tag_name, tag_value FROM information_schema.table_tags WHERE table_name = 'lx1_bronze_sales';
# MAGIC ```
# MAGIC </details>

# COMMAND ----------

# DBTITLE 1,Exercise 1.2 - Your Work
# MAGIC %sql
# MAGIC -- Exercise 1.2: write your SQL below, then run this cell.
# MAGIC -- Step 1: COMMENT ON TABLE lx1_bronze_sales IS '...'
# MAGIC -- Step 2: ALTER TABLE lx1_bronze_sales ALTER COLUMN <col> COMMENT '...'   (qty, unit_price, discount_amt)
# MAGIC -- Step 3: ALTER TABLE lx1_bronze_sales SET TAGS ('quality_tier' = 'bronze')
# MAGIC -- Step 4: DESCRIBE TABLE EXTENDED lx1_bronze_sales
# MAGIC
# MAGIC SELECT 'Exercise 1.2: add your SQL above this line' AS status;

# COMMAND ----------

# DBTITLE 1,Exercise 1.3 - MERGE Upsert
# MAGIC %md
# MAGIC ## Exercise 1.3: Upsert the Product Master with MERGE (25 minutes)
# MAGIC
# MAGIC **Objective:** Apply inserts and updates from a change feed to a Silver table in one atomic `MERGE` statement.
# MAGIC
# MAGIC **Business scenario:** Every night a modern-trade retailer sends SnackCo a product feed, `lx1_product_updates`. Tonight it contains **3 price changes** (SKU001, SKU008, SKU013) and **2 new SKUs** (SKU021, SKU022). Reports must never see a half-applied feed, so the load must be a single transaction.
# MAGIC
# MAGIC **Concepts recap**
# MAGIC * `MERGE INTO target USING source ON <key>` compares rows by business key.
# MAGIC * `WHEN MATCHED THEN UPDATE` changes existing rows; `WHEN NOT MATCHED THEN INSERT` adds new ones.
# MAGIC * The whole MERGE is one ACID transaction and one version in the table history.
# MAGIC * Always match on the **business key** (`sku`), never on load timestamps.
# MAGIC
# MAGIC **Steps**
# MAGIC 1. Create `lx1_silver_products` from `lx1_raw_products`, adding an `updated_at` timestamp column.
# MAGIC 2. Write a `MERGE` from `lx1_product_updates` on `sku` that updates `product_name`, `list_price` and `updated_at`, and inserts new SKUs.
# MAGIC 3. Check the result: 22 products, SKU001 now costs 15.0, and `DESCRIBE HISTORY` shows a MERGE with its metrics (`numTargetRowsUpdated`, `numTargetRowsInserted`).
# MAGIC 4. Bonus: run the MERGE again. What happens to the row count, and why?
# MAGIC
# MAGIC **Expected result:** 22 rows, 3 updated, 2 inserted. Re-running changes no row counts (the MERGE is idempotent).
# MAGIC
# MAGIC <details><summary><b>Reference solution (open only if stuck)</b></summary>
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REPLACE TABLE lx1_silver_products AS
# MAGIC SELECT *, current_timestamp() AS updated_at FROM lx1_raw_products;
# MAGIC
# MAGIC MERGE INTO lx1_silver_products t
# MAGIC USING lx1_product_updates s
# MAGIC ON t.sku = s.sku
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   t.product_name = s.product_name,
# MAGIC   t.list_price   = s.list_price,
# MAGIC   t.updated_at   = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN INSERT (sku, product_name, brand, category, pack_size, list_price, updated_at)
# MAGIC VALUES (s.sku, s.product_name, s.brand, s.category, s.pack_size, s.list_price, current_timestamp());
# MAGIC
# MAGIC SELECT COUNT(*) AS products FROM lx1_silver_products;
# MAGIC SELECT operation, operationMetrics FROM (DESCRIBE HISTORY lx1_silver_products) WHERE operation = 'MERGE';
# MAGIC ```
# MAGIC </details>

# COMMAND ----------

# DBTITLE 1,Exercise 1.3 - Your Work
# MAGIC %sql
# MAGIC -- Exercise 1.3: write your SQL below, then run this cell.
# MAGIC -- Step 1: CREATE OR REPLACE TABLE lx1_silver_products AS SELECT *, current_timestamp() AS updated_at FROM lx1_raw_products
# MAGIC -- Step 2: MERGE INTO lx1_silver_products t USING lx1_product_updates s ON t.sku = s.sku
# MAGIC --         WHEN MATCHED THEN UPDATE SET ...  WHEN NOT MATCHED THEN INSERT ...
# MAGIC -- Step 3: verify counts and DESCRIBE HISTORY
# MAGIC
# MAGIC SELECT 'Exercise 1.3: add your SQL above this line' AS status;

# COMMAND ----------

# DBTITLE 1,Exercise 1.4 - CTEs and Window Functions
# MAGIC %md
# MAGIC ## Exercise 1.4: CTEs and Window Functions - Top SKU and MoM Growth (25 minutes)
# MAGIC
# MAGIC **Objective:** Write readable analytical SQL with CTEs, `ROW_NUMBER()` and `LAG()`, and publish it as reusable views.
# MAGIC
# MAGIC **Business scenario:** The national sales director wants two views for the Monday review:
# MAGIC * the **top-selling SKU by units** in each region, and
# MAGIC * **month-over-month (MoM) net-sales growth** for each region.
# MAGIC
# MAGIC The analyst's current query uses three nested subqueries and nobody can maintain it.
# MAGIC
# MAGIC **Concepts recap**
# MAGIC * A **CTE** (`WITH name AS (...)`) breaks a query into named, readable steps.
# MAGIC * `ROW_NUMBER() OVER (PARTITION BY region ORDER BY units_sold DESC, net_sales DESC)` ranks rows within each region; keep `rn = 1`.
# MAGIC * `LAG(net_sales) OVER (PARTITION BY region ORDER BY month)` returns the previous month's value (NULL for the first month).
# MAGIC * MoM growth % = `(net_sales - prev_month_net_sales) / prev_month_net_sales * 100`.
# MAGIC
# MAGIC **Steps**
# MAGIC 1. Create view `lx1_v_top_sku_by_region` with columns `region, sku, units_sold, net_sales`, one row per region. Rank by `units_sold` descending, then `net_sales` descending as the tie-breaker. Source: `lx1_bronze_sales`.
# MAGIC 2. Create view `lx1_v_monthly_mom` with columns `region, month, net_sales, prev_month_net_sales, mom_growth_pct`. Use `date_trunc('MONTH', txn_date)` for the month.
# MAGIC 3. Query both views and sanity-check the results.
# MAGIC
# MAGIC **Expected result:** The top-SKU view has 5 rows (one per region). The MoM view has 30 rows (5 regions x 6 months), and `prev_month_net_sales` is NULL only for January.
# MAGIC
# MAGIC <details><summary><b>Reference solution (open only if stuck)</b></summary>
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REPLACE VIEW lx1_v_top_sku_by_region AS
# MAGIC WITH sales AS (
# MAGIC   SELECT region, sku, SUM(qty) AS units_sold, SUM(qty * unit_price - discount_amt) AS net_sales
# MAGIC   FROM lx1_bronze_sales GROUP BY region, sku),
# MAGIC ranked AS (
# MAGIC   SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY units_sold DESC, net_sales DESC) AS rn
# MAGIC   FROM sales)
# MAGIC SELECT region, sku, units_sold, net_sales FROM ranked WHERE rn = 1;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW lx1_v_monthly_mom AS
# MAGIC WITH m AS (
# MAGIC   SELECT region, date_trunc('MONTH', txn_date) AS month, SUM(qty * unit_price - discount_amt) AS net_sales
# MAGIC   FROM lx1_bronze_sales GROUP BY region, date_trunc('MONTH', txn_date)),
# MAGIC l AS (
# MAGIC   SELECT *, LAG(net_sales) OVER (PARTITION BY region ORDER BY month) AS prev_month_net_sales FROM m)
# MAGIC SELECT region, month, net_sales, prev_month_net_sales,
# MAGIC        ROUND((net_sales - prev_month_net_sales) / prev_month_net_sales * 100, 2) AS mom_growth_pct
# MAGIC FROM l;
# MAGIC
# MAGIC SELECT * FROM lx1_v_top_sku_by_region ORDER BY region;
# MAGIC SELECT * FROM lx1_v_monthly_mom ORDER BY region, month;
# MAGIC ```
# MAGIC </details>

# COMMAND ----------

# DBTITLE 1,Exercise 1.4 - Your Work
# MAGIC %sql
# MAGIC -- Exercise 1.4: write your SQL below, then run this cell.
# MAGIC -- Step 1: CREATE OR REPLACE VIEW lx1_v_top_sku_by_region AS WITH sales AS (...), ranked AS (... ROW_NUMBER() OVER (...)) SELECT ...
# MAGIC -- Step 2: CREATE OR REPLACE VIEW lx1_v_monthly_mom AS WITH m AS (...), l AS (... LAG(net_sales) OVER (...)) SELECT ...
# MAGIC -- Step 3: SELECT * FROM each view
# MAGIC
# MAGIC SELECT 'Exercise 1.4: add your SQL above this line' AS status;

# COMMAND ----------

# DBTITLE 1,Self-Check Instructions
# MAGIC %md
# MAGIC ## Self-Check: Validate Your Day 1 Work (5 minutes)
# MAGIC
# MAGIC Run the next cell. It inspects the objects you built, using Delta history, `information_schema` and recomputed answers, and reports each check as **PASS** or **FAIL**.
# MAGIC
# MAGIC | Exercise | What is checked |
# MAGIC |---|---|
# MAGIC | 1.1 | Table exists, history contains DELETE and RESTORE, all 2,400 rows are back |
# MAGIC | 1.2 | Table comment, 3 or more column comments, `quality_tier` tag |
# MAGIC | 1.3 | 22 products, SKU001 price updated to 15.0, new SKU021 inserted, a MERGE in history |
# MAGIC | 1.4 | Top-SKU view correct for every region, MoM view has 30 rows with NULL previous month only in January |
# MAGIC
# MAGIC Fix any FAIL using its hint, re-run that exercise, then run the Self-Check again. A score of **100%** means Day 1 is complete.

# COMMAND ----------

# DBTITLE 1,Self-Check - Run Validation
import html

CHECKS = []
def check(exercise, name, hint):
    def register(fn):
        CHECKS.append((exercise, name, hint, fn))
        return fn
    return register

def scalar(sql):
    return spark.sql(sql).collect()[0][0]

def exists(obj):
    return spark.catalog.tableExists(f"{CATALOG}.{SCHEMA}.{obj}")

def history_ops(table):
    return [r.operation for r in spark.sql(f"DESCRIBE HISTORY {table}").collect()]

INFO = f"{CATALOG}.information_schema"

# ---------------- Exercise 1.1 ----------------
@check("1.1", "lx1_bronze_sales exists", "Create it with CREATE TABLE lx1_bronze_sales AS SELECT * FROM lx1_raw_sales")
def _():
    return exists("lx1_bronze_sales"), "Table found" if exists("lx1_bronze_sales") else "Table not found"

@check("1.1", "History contains a DELETE", "Run DELETE FROM lx1_bronze_sales WHERE region = 'North'")
def _():
    ops = history_ops("lx1_bronze_sales")
    return "DELETE" in ops, f"Operations: {', '.join(reversed(ops))}"

@check("1.1", "History contains a RESTORE", "Run RESTORE TABLE lx1_bronze_sales TO VERSION AS OF 0")
def _():
    ops = history_ops("lx1_bronze_sales")
    return "RESTORE" in ops, f"Operations: {', '.join(reversed(ops))}"

@check("1.1", "All rows recovered", "Restore to the version before the DELETE (find it in DESCRIBE HISTORY)")
def _():
    cur, src = scalar("SELECT COUNT(*) FROM lx1_bronze_sales"), scalar("SELECT COUNT(*) FROM lx1_raw_sales")
    return cur == src, f"{cur:,} rows now vs {src:,} expected"

# ---------------- Exercise 1.2 ----------------
@check("1.2", "Table comment set", "COMMENT ON TABLE lx1_bronze_sales IS '...'")
def _():
    c = scalar(f"SELECT comment FROM {INFO}.tables WHERE table_schema = '{SCHEMA}' AND table_name = 'lx1_bronze_sales'")
    return bool(c), f"Comment: {c}" if c else "No table comment"

@check("1.2", "At least 3 column comments", "ALTER TABLE lx1_bronze_sales ALTER COLUMN qty COMMENT '...' (repeat for unit_price, discount_amt)")
def _():
    n = scalar(f"SELECT COUNT(*) FROM {INFO}.columns WHERE table_schema = '{SCHEMA}' AND table_name = 'lx1_bronze_sales' AND comment IS NOT NULL AND comment <> ''")
    return n >= 3, f"{n} commented column(s)"

@check("1.2", "quality_tier tag present", "ALTER TABLE lx1_bronze_sales SET TAGS ('quality_tier' = 'bronze')")
def _():
    v = spark.sql(f"SELECT tag_value FROM {INFO}.table_tags WHERE schema_name = '{SCHEMA}' AND table_name = 'lx1_bronze_sales' AND tag_name = 'quality_tier'").collect()
    return len(v) > 0, f"quality_tier = {v[0][0]}" if v else "Tag not found"

# ---------------- Exercise 1.3 ----------------
@check("1.3", "lx1_silver_products has 22 products", "Create from lx1_raw_products (20) and MERGE the feed (+2 new)")
def _():
    n = scalar("SELECT COUNT(*) FROM lx1_silver_products")
    return n == 22, f"{n} rows (expected 22)"

@check("1.3", "SKU001 price updated to 15.0", "WHEN MATCHED THEN UPDATE SET t.list_price = s.list_price")
def _():
    p = scalar("SELECT MAX(list_price) FROM lx1_silver_products WHERE sku = 'SKU001'")
    return p is not None and float(p) == 15.0, f"SKU001 list_price = {p}"

@check("1.3", "New SKU021 inserted", "WHEN NOT MATCHED THEN INSERT (...) VALUES (...)")
def _():
    n = scalar("SELECT COUNT(*) FROM lx1_silver_products WHERE sku = 'SKU021'")
    return n == 1, f"{n} row(s) for SKU021"

@check("1.3", "MERGE recorded in history", "Use a single MERGE INTO statement (not separate UPDATE/INSERT)")
def _():
    ops = history_ops("lx1_silver_products")
    return "MERGE" in ops, f"Operations: {', '.join(reversed(ops))}"

# ---------------- Exercise 1.4 ----------------
@check("1.4", "Top-SKU view: one row per region", "PARTITION BY region and keep only rn = 1")
def _():
    n, d = spark.sql("SELECT COUNT(*), COUNT(DISTINCT region) FROM lx1_v_top_sku_by_region").collect()[0]
    return n == 5 and d == 5, f"{n} rows, {d} distinct regions (expected 5 and 5)"

@check("1.4", "Top-SKU view: correct SKU per region", "ORDER BY units_sold DESC, net_sales DESC inside ROW_NUMBER()")
def _():
    expected = {(r.region, r.sku) for r in spark.sql("""
        WITH s AS (SELECT region, sku, SUM(qty) u, SUM(qty * unit_price - discount_amt) n FROM lx1_raw_sales GROUP BY region, sku),
        r AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY u DESC, n DESC) rn FROM s)
        SELECT region, sku FROM r WHERE rn = 1""").collect()}
    actual = {(r.region, r.sku) for r in spark.sql("SELECT region, sku FROM lx1_v_top_sku_by_region").collect()}
    wrong = sorted(expected - actual)
    return not wrong, "All regions correct" if not wrong else f"Expected but missing: {wrong}"

@check("1.4", "MoM view: 30 rows with mom_growth_pct", "Group by region and date_trunc('MONTH', txn_date); include a mom_growth_pct column")
def _():
    cols = [c.lower() for c in spark.table("lx1_v_monthly_mom").columns]
    n = scalar("SELECT COUNT(*) FROM lx1_v_monthly_mom")
    return n == 30 and "mom_growth_pct" in cols, f"{n} rows; columns: {', '.join(cols)}"

@check("1.4", "MoM view: LAG is NULL only for the first month", "LAG(net_sales) OVER (PARTITION BY region ORDER BY month)")
def _():
    n = scalar("SELECT COUNT(*) FROM lx1_v_monthly_mom WHERE prev_month_net_sales IS NULL")
    return n == 5, f"{n} NULL previous-month values (expected 5, one per region)"

# ---------------- Run and report ----------------
results, by_ex = [], {}
for ex, name, hint, fn in CHECKS:
    try:
        ok, detail = fn()
    except Exception as e:
        ok, detail = False, f"{type(e).__name__}: {str(e).strip().splitlines()[0][:180]}"
    ok = bool(ok)
    results.append((ex, name, ok, detail, hint))
    by_ex.setdefault(ex, [0, 0]); by_ex[ex][0] += ok; by_ex[ex][1] += 1

passed, total = sum(r[2] for r in results), len(results)
pct = passed / total * 100
color = "#2E7D32" if pct == 100 else "#EF6C00" if pct >= 60 else "#C62828"
verdict = "Day 1 lab complete. Well done!" if pct == 100 else "Some checks failed. Use the hints below, fix, and re-run."
summary = "".join(
    f"<td style='padding:6px 14px;text-align:center;border:1px solid #CFD8DC'><b>Exercise {ex}</b><br>{p}/{t} "
    f"<span style='color:{'#2E7D32' if p == t else '#C62828'}'>{'COMPLETE' if p == t else 'INCOMPLETE'}</span></td>"
    for ex, (p, t) in by_ex.items())
rows = "".join(
    f"<tr style='background:{'#E8F5E9' if ok else '#FFEBEE'}'><td style='padding:5px 8px'>{ex}</td><td style='padding:5px 8px'>{html.escape(name)}</td>"
    f"<td style='padding:5px 8px;font-weight:bold;color:{'#2E7D32' if ok else '#C62828'}'>{'PASS' if ok else 'FAIL'}</td>"
    f"<td style='padding:5px 8px'>{html.escape(str(detail))}</td><td style='padding:5px 8px;color:#555'>{'' if ok else html.escape(hint)}</td></tr>"
    for ex, name, ok, detail, hint in results)
displayHTML(f"""<div style='font-family:Inter,Arial,sans-serif'>
<div style='background:{color};color:white;padding:14px 18px;border-radius:6px'>
<div style='font-size:14px'>DAY 1 LAB SELF-CHECK | {CATALOG}.{SCHEMA}</div>
<div style='font-size:28px;font-weight:bold'>{passed} / {total} checks passed ({pct:.0f}%)</div><div>{verdict}</div></div>
<table style='border-collapse:collapse;margin:12px 0'><tr>{summary}</tr></table>
<table style='border-collapse:collapse;width:100%;font-size:13px'>
<tr style='background:#0D47A1;color:white'><th style='padding:6px'>Ex</th><th style='padding:6px'>Check</th><th style='padding:6px'>Status</th><th style='padding:6px'>Detail</th><th style='padding:6px'>Hint (if failed)</th></tr>
{rows}</table></div>""")

# COMMAND ----------

# DBTITLE 1,Day 1 Key Takeaways
# MAGIC %md
# MAGIC ## Day 1 Key Takeaways
# MAGIC
# MAGIC * **Delta Lake** records every change as a version; Time Travel and RESTORE turn mistakes into a few minutes of recovery instead of hours of re-ingestion.
# MAGIC * **Unity Catalog** comments and tags are part of the model. They make tables discoverable for people and for AI tools such as Genie.
# MAGIC * **MERGE** is the standard pattern for idempotent upserts: match on the business key, update the matches, insert the rest.
# MAGIC * **CTEs and window functions** (`ROW_NUMBER`, `LAG`) replace nested subqueries with readable, maintainable analytics SQL.
# MAGIC
# MAGIC **Next:** Day 2 builds a full CPG star schema (dimensions, facts and Gold aggregates) on top of these foundations.