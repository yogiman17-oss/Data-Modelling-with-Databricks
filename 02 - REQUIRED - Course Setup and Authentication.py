# Databricks notebook source
# MAGIC %md
# MAGIC ![DB Academy](https://files.training.databricks.com/binder/prod_main/automated-deployment-with-declarative-automation-bundles-en_us-2.4.0/images/20261014T140722Z/Automated Deployment with Declarative Automation Bundles/Includes/images/common/db-academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # 02 - REQUIRED - Course Setup and Authentication
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook prepares your lab environment for the **Automated Deployment with Declarative Automation Bundles (DABs)** course. You'll create the dev, stage, and prod catalogs and data the rest of the course depends on, then install and authenticate the **Databricks CLI** so you can work with DABs from inside a notebook.
# MAGIC
# MAGIC The CLI authentication pattern shown here is for **training convenience only**. It uses the short-lived API token that Databricks issues to the running notebook session. In a real environment, follow your organization's security policies. 
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this notebook, you will be able to:
# MAGIC
# MAGIC 1. **Configure your lab environment** by creating the dev, stage, and prod catalogs, schemas, and volumes used throughout the course.
# MAGIC 2. **Reference your unique catalogs** using the `catalog_dev`, `catalog_stage`, and `catalog_prod` Python variables.
# MAGIC 3. **Authenticate the Databricks CLI** by exporting the notebook's short-lived API token as `DATABRICKS_TOKEN` and the workspace URL as `DATABRICKS_HOST` (training-only pattern).
# MAGIC 4. **Install the Databricks CLI** inside a Databricks notebook.
# MAGIC 5. **Run basic CLI commands** (`-v`, `--help`, `catalogs list`) to verify the installation and explore the CLI.

# COMMAND ----------

# MAGIC %md
# MAGIC **Note**: If you see the warning `Warn: Failed to load git info from /api/2.0/workspace/get-status` while **validating**, **deploying**, or **running** a Declarative Automation Bundle (DAB) during this course, you can safely ignore it and proceed.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## REQUIRED - SELECT A COMPUTE ENVIRONMENT
# MAGIC <div style="
# MAGIC   border-left: 4px solid #f44336;
# MAGIC   background: #ffebee;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">Select All-Purpose Compute</strong>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC This notebook requires **all-purpose compute** (Dedicated). Serverless is not supported for this notebook.
# MAGIC
# MAGIC Follow these steps to attach an all-purpose compute cluster:
# MAGIC
# MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your `labuser_USERNAME` cluster.
# MAGIC     - By default, the notebook might use **Serverless**.
# MAGIC
# MAGIC 2. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
# MAGIC
# MAGIC     - In the drop-down, select **More**.
# MAGIC
# MAGIC     - In the **Attach to an existing compute resource** pop-up, select the first drop-down. You will see a unique cluster name in that drop-down. Please select that cluster.
# MAGIC
# MAGIC ⚠️ **NOTE:** If the cluster shows a **terminated** state (red dot in the cluster picker), it needs to be started before you can attach. Click the cluster, then **Start**, and wait a few minutes until you see a green dot.
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the following cell to configure your working environment for this course. 
# MAGIC
# MAGIC It will create the following catalogs, volumes and files:
# MAGIC - Catalog: **labuser_UNIQUE_ID_1_dev**
# MAGIC   - Schema: **default**
# MAGIC     - Volume: **health**
# MAGIC       - *dev_health.csv* : Small subset of prod data, anonymized *PII*, 7,500 rows
# MAGIC
# MAGIC - Catalog: **labuser_UNIQUE_ID_2_stage**
# MAGIC   - Schema: **default**
# MAGIC     - Volume: **health**
# MAGIC       - *stage_health.csv* : Subset of prod data, 35,000 rows
# MAGIC
# MAGIC - Catalog: **labuser_UNIQUE_ID_3_prod**
# MAGIC   - Schema: **default**
# MAGIC     - Volume: **health**
# MAGIC       - *2025-01-01_health.csv*
# MAGIC       - *2025-01-02_health.csv*
# MAGIC       - *2025-01-03_health.csv*
# MAGIC       - Simulates production CSV files landing to this cloud storage location daily.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-02-catalog-setup-REQUIRED

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Explore Your Environment
# MAGIC
# MAGIC 1. Let's quickly explore the course folder structure and files.
# MAGIC
# MAGIC     a. In the left navigation bar, select the folder icon.
# MAGIC
# MAGIC     b. Ensure you are in the main course folder named **Automated Deployment with Declarative Automation Bundles**.
# MAGIC     
# MAGIC     c. In the main course folder, you will see various folders, each folder contains specific files for each demonstration or lab.

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Manually view your catalogs and data for this course.
# MAGIC
# MAGIC     a. In the workspace sidebar, select the catalog icon.
# MAGIC
# MAGIC     b. Confirm the classroom setup script has created three new catalogs for you:
# MAGIC       - **labuser_UNIQUE_ID_1_dev**
# MAGIC       - **labuser_UNIQUE_ID_2_stage**
# MAGIC       - **labuser_UNIQUE_ID_3_prod**
# MAGIC
# MAGIC     c. Expand each **labuser_UNIQUE_ID** catalog and **default** from above and notice the following:
# MAGIC       - A volume named **health** was created in each catalog
# MAGIC       - It contains **one or more CSV files** for the specific environment:
# MAGIC         - development (1 csv file, small sample of production data, no PII)
# MAGIC         - stage (1 csv file, larger sample of production data, contains PII)
# MAGIC         - production (3 csv files)

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Throughout the course, the following Python variables will be used to dynamically reference your unique course catalogs:
# MAGIC     - `catalog_dev`
# MAGIC     - `catalog_stage`
# MAGIC     - `catalog_prod`
# MAGIC
# MAGIC     Run the code below and confirm the variables refer to your catalog names.
# MAGIC

# COMMAND ----------

print(f'catalog_dev: {catalog_dev}')
print(f'catalog_stage: {catalog_stage}')
print(f'catalog_prod: {catalog_prod}')

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Configure Databricks CLI Authentication
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #f44336;
# MAGIC   background: #ffebee;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">Training Environment Setup</strong>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC In this section, you will **install and configure the Databricks CLI** for this course by reading the notebook's short-lived API token and exporting it as `DATABRICKS_TOKEN`, along with the workspace URL as `DATABRICKS_HOST` using the provided classroom setup script.
# MAGIC
# MAGIC ⚠️ **Important**
# MAGIC - This pattern is for **training convenience only**, not a production pattern.
# MAGIC - The token lives in this notebook's environment variables. Anyone who can run cells in this notebook can read those values.
# MAGIC - The token is **short-lived**. If the notebook detaches or you start a new session, re-run this cell to refresh the credentials.
# MAGIC
# MAGIC **In a production environment, follow your organization's security policies. Common patterns:**
# MAGIC   - **Databricks Secret Management** for storing personal access tokens.
# MAGIC   - **OAuth machine-to-machine** with a service principal for automation and CI/CD.
# MAGIC   - **Workload identity federation** for long-running jobs.
# MAGIC
# MAGIC **Generate a production-grade token:**
# MAGIC   - **Personal access tokens (PATs)**, for individual user automation:
# MAGIC     [AWS](https://docs.databricks.com/aws/en/dev-tools/auth/pat) |
# MAGIC     [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/pat) |
# MAGIC     [GCP](https://docs.databricks.com/gcp/en/dev-tools/auth/pat)
# MAGIC   - **OAuth machine-to-machine (service principal)**, for jobs and CI/CD pipelines:
# MAGIC     [AWS](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m) |
# MAGIC     [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/oauth-m2m) |
# MAGIC     [GCP](https://docs.databricks.com/gcp/en/dev-tools/auth/oauth-m2m)
# MAGIC   - **Databricks authentication overview**, full list of supported auth methods:
# MAGIC     [AWS](https://docs.databricks.com/aws/en/dev-tools/auth/) |
# MAGIC     [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/) |
# MAGIC     [GCP](https://docs.databricks.com/gcp/en/dev-tools/auth/)
# MAGIC   - **Secret management**, for storing tokens securely once issued:
# MAGIC     [AWS](https://docs.databricks.com/aws/en/security/secrets/) |
# MAGIC     [Azure](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/) |
# MAGIC     [GCP](https://docs.databricks.com/gcp/en/security/secrets/)
# MAGIC   </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1. Install the Databricks CLI
# MAGIC
# MAGIC A token is just like a username and password. Treat it as sensitive.
# MAGIC
# MAGIC - If a token is exposed, **delete it immediately**
# MAGIC - Never share tokens with others
# MAGIC
# MAGIC For this training, we will temporarily store credentials in environment variables so we can use the CLI in a notebook for training.
# MAGIC
# MAGIC
# MAGIC 1. Run the cell below to authenticate and install the CLI for use with a Databricks Notebook. 
# MAGIC
# MAGIC     For this training, we will install it in our workspace using a script we set up for you (it uses the notebook token and environment variables).
# MAGIC
# MAGIC     **NOTE:** For more information on installing the Databricks CLI, view the **Install or update the Databricks CLI** documentation:
# MAGIC     [AWS](https://docs.databricks.com/aws/en/dev-tools/cli/install) |
# MAGIC     [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/install) |
# MAGIC     [GCP](https://docs.databricks.com/gcp/en/dev-tools/cli/install)

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-Common-Install-CLI

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2. Simple Databricks CLI Commands
# MAGIC The following cells run basic CLI commands in a notebook using `%sh`.
# MAGIC   - `%sh`: Allows you to run shell code in your notebook.
# MAGIC
# MAGIC This will get you familiar with the CLI in a notebook and confirm it was installed correctly.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Run the following cell to view the version of the CLI. You should see that you are using **Databricks CLI v0.298.0**.
# MAGIC
# MAGIC
# MAGIC     **NOTE:** View the documentation on running shell commands in Databricks notebooks - **Code languages in notebooks**:
# MAGIC     [AWS](https://docs.databricks.com/aws/en/notebooks/notebooks-code#code-languages-in-notebooks) |
# MAGIC     [Azure](https://learn.microsoft.com/en-us/azure/databricks/notebooks/notebooks-code#code-languages-in-notebooks) |
# MAGIC     [GCP](https://docs.databricks.com/gcp/en/notebooks/notebooks-code#code-languages-in-notebooks)

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks -v

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Run the `--help` command to view documentation for the Databricks CLI.
# MAGIC
# MAGIC     **Databricks CLI commands** documentation:
# MAGIC     [AWS](https://docs.databricks.com/aws/en/dev-tools/cli/commands) |
# MAGIC     [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/commands) |
# MAGIC     [GCP](https://docs.databricks.com/gcp/en/dev-tools/cli/commands)

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks --help

# COMMAND ----------

# MAGIC %md
# MAGIC 3. View help for commands to manage catalogs.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks catalogs --help

# COMMAND ----------

# MAGIC %md
# MAGIC 4. View available catalogs using the Databricks CLI.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks catalogs list

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="
# MAGIC   border-left: 4px solid #1976d2;
# MAGIC   background: #e3f2fd;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC   color: #333;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Production Note</strong>
# MAGIC
# MAGIC This course uses **dedicated classic compute** for installing and running the Databricks CLI inside a notebook. We chose classic for a few practical reasons:
# MAGIC
# MAGIC - **`%sh` is fully supported and predictable.** The CLI install pattern (writing the binary to `~/bin/databricks` or `/usr/local/bin/databricks`) works the same way on every classic cluster.
# MAGIC
# MAGIC - **The lab environment is on classic, Dedicated Access mode, Unrestricted policy**, so the install script in this course matches the cluster you're learning on in your Vocareum provided Databricks Academy lab.
# MAGIC
# MAGIC - Running in a notebook enables a streamlined and efficient training experience to learn how to use DABs.
# MAGIC
# MAGIC ⚠️ **In production, you typically do not run `databricks bundle deploy` from a notebook.**
# MAGIC
# MAGIC The standard production pattern is to deploy bundles from:
# MAGIC
# MAGIC - A **developer's local machine** with the Databricks CLI configured against a workspace, or
# MAGIC - A **CI/CD runner** (GitHub Actions, Azure DevOps, GitLab CI, Jenkins) authenticated as a **service principal** via OAuth machine-to-machine.
# MAGIC
# MAGIC Running DAB commands from a notebook enables you to focus on bundle structure and behavior without installing the CLI locally. Once you understand bundles, switch to deploying from your actual dev machine or your team's CI pipeline.
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC In this notebook, you set up everything required for the rest of the course:
# MAGIC
# MAGIC 1. Created the **dev**, **stage**, and **prod** catalogs and the **health** volumes that hold the course data.
# MAGIC 2. Verified that the `catalog_dev`, `catalog_stage`, and `catalog_prod` Python variables reference your unique catalogs.
# MAGIC 3. Configured **Databricks CLI authentication** using the notebook's short-lived API token (training-only pattern).
# MAGIC 4. Installed the **Databricks CLI** in your notebook environment.
# MAGIC 5. Ran foundational CLI commands (`-v`, `--help`, `catalogs list`) to confirm the installation.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>