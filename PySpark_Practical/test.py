# Df bannao do column ka customer _first name and customer _last name
# Create third column by concatenate and Kumar in between
# New column with lit constant value
# Create another column from the lit column by type casting
# Same thing with kolas


from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, lit
from pyspark.sql.types import IntegerType

# Initialize Spark session
spark = SparkSession.builder.appName("Concat Example").getOrCreate()

# Sample data
data = [("Sumit", "Verma"), ("Amit", "Sharma"), ("Ravi", "Singh")]

# Create DataFrame
df = spark.createDataFrame(data, ["customer_first_name", "customer_last_name"])

# Add a new column with 'Kumar' in between
df = df.withColumn("full_name", concat_ws(" ", df.customer_first_name, lit("Kumar"), df.customer_last_name))

# Add a new column with a constant value
df = df.withColumn("constant_value", lit("100"))

# Create another column by typecasting the above column to Integer
df = df.withColumn("constant_value_int", df["constant_value"].cast(IntegerType()))

# Show DataFrame
df.show()


--------------------------------------------


import pyspark.pandas as ps
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Initialize Spark session
spark = SparkSession.builder.appName("Koalas Example").getOrCreate()

# Sample data
data = [("Sumit", "Verma"), ("Amit", "Sharma"), ("Ravi", "Singh")]

# Create a Koalas DataFrame
df = ps.DataFrame(data, columns=["customer_first_name", "customer_last_name"])

# Add a new column with 'Kumar' in between
df["full_name"] = df["customer_first_name"] + " Kumar " + df["customer_last_name"]

# Add a new column with a constant value
df["constant_value"] = "100"

# Create another column by typecasting the above column to Integer
df["constant_value_int"] = df["constant_value"].astype(int)

# Show DataFrame
print(df)



# Koalas (pyspark.pandas) uses df["column_name"] instead of withColumn().
# Concatenation is simpler using + instead of concat_ws().
# Typecasting is done via .astype() instead of .cast().


------------------------------------




--------------------------------------

Azure Databricks Architecture

1️⃣ Control Plane (Managed by Azure)
Manages notebooks, jobs, metadata, and cluster configurations.
Handles authentication, access control, and workspace management.
Orchestrates auto-scaling, cluster provisioning, and job scheduling.
Stores notebook & job metadata in Azure-managed storage.

2️⃣ Data Plane (Runs in User’s Subscription)
Contains compute clusters (VMs) that run Apache Spark workloads.
Reads/writes data from Azure Data Lake, Blob Storage, SQL DBs, etc..
Runs ETL, Machine Learning, and Data Analytics workloads.
Uses Databricks Runtime (optimized Spark engine) for performance.

3️⃣ Storage Layer
Supports multiple Azure storage options:
Azure Data Lake Storage (ADLS)
Azure Blob Storage
Delta Lake (for ACID-compliant tables)
Azure SQL Database, Synapse Analytics

4️⃣ Integration & Security
Integrates with Azure services:
✅ Azure Data Factory (ADF) for orchestration
✅ Azure Synapse Analytics for data warehousing
✅ Power BI for visualization
✅ Azure Machine Learning (AML) for AI/ML

Enterprise-grade security:
✅ Azure AD authentication
✅ Role-based access control (RBAC)
✅ Virtual Networks (VNet) for secure access

🔥 Summary
✅ Control Plane → Manages clusters & jobs
✅ Data Plane → Runs Spark computations
✅ Storage Layer → Stores data in ADLS, Blob, or Delta Lake
✅ Integration & Security → Connects with Azure services & ensures security

---------------------------------------------------
Why Unity Catalog is Better? 🚀
✅ Centralized Governance – Manages permissions across all workspaces & clouds in one place.
✅ Fine-Grained Access Control – Supports row/column-level security & data masking.
✅ Cross-Workspace Data Sharing – Enables secure data sharing between multiple Databricks workspaces.
✅ Audit & Lineage Tracking – Provides end-to-end data lineage & audit logs for compliance.

💡 Unity Catalog simplifies governance, security, and access control for all your data in Databricks! 

----------------------------------------------------

Managed Table vs. External Table in Azure Databricks

Feature	Managed Table 🏆	External Table 🌍
Storage Location	Stored in Databricks-managed storage	Stored in external storage (ADLS, Blob, etc.)
Data Ownership	Databricks controls & manages the data	User controls & manages the data
Data Deletion	Dropped table = Data deleted	Dropped table ≠ Data deleted (only metadata is removed)
Use Case	Best for ETL, temp storage, & full governance	Best for existing external data sources
    
💡 Use Managed Tables for full control & governance, and External Tables for 
flexible storage with external control! 🚀

------------------------------------------------------------

Why Delta Table is Better? 🚀
✅ ACID Transactions – Ensures data integrity & consistency (unlike Parquet, CSV).
✅ Schema Enforcement & Evolution – Prevents corrupt data and supports schema changes.
✅ Time Travel & Versioning – Enables rollback & historical data queries.
✅ Performance Optimization – Uses auto-compaction, indexing, & data skipping for faster queries.

💡 Delta Tables combine the reliability of databases with the scalability of data lakes! 

------------------------------------------------------------

LAG() - sales_data


SELECT 
    sales_id,
    customer_name,
    sales_date,
    sales_amount,
    LAG(sales_amount, 1, 0) OVER (ORDER BY sales_date) AS prev_sales
FROM sales_data;

------------------------------------------------------------

Spark Optimization Techniques 🚀
✅ 1. Use DataFrame API Instead of RDDs – DataFrames are optimized with Catalyst Optimizer & Tungsten Execution Engine.
✅ 2. Enable Data Serialization – Use Kryo Serialization for faster object serialization.
✅ 3. Optimize Shuffles & Joins – Use Broadcast Joins for small tables & Repartition() to reduce shuffle overhead.
✅ 4. Cache & Persist Data – Use .cache() or .persist(StorageLevel.MEMORY_AND_DISK) to reuse intermediate results.
✅ 5. Optimize File Formats – Use Parquet/Delta instead of CSV for efficient storage & fast reads.

💡 Efficient Spark tuning improves performance, reduces costs, and accelerates big data processing! 


----------------------------------------------------------

Why Chaining is Better Than Creating New Variables in Spark?
✅ 1. Avoids Unnecessary Intermediate DataFrames – Reduces memory usage & overhead.
✅ 2. Improves Performance – Spark optimizes chained transformations using Catalyst Optimizer.
✅ 3. Reduces Shuffle & Recomputations – Minimizes shuffle operations & avoids recomputation of intermediate results.
✅ 4. Cleaner & More Readable Code – Chaining keeps transformations compact & eliminates clutter.

💡 Use method chaining (df.withColumn().filter().select()) for better performance & cleaner Spark code! 

---------------------------------------------------------------

Deploying a Dev Project from Azure Repository to Azure Databricks Using CI/CD & Databricks Asset Bundles (DAB) 🚀


🔹 Steps for CI/CD Deployment (Multi-Repo)
✅ 1. Store Code in Azure Repos

Keep separate repositories for different projects/environments.

Use branching strategy (main, dev, feature branches).

✅ 2. Set Up Databricks Asset Bundles (DAB)

Databricks Asset Bundles (DAB) simplify CI/CD deployments using configuration files (databricks.yml).

Organize code structure:

├── src/
├── notebooks/
├── jobs/
├── databricks.yml  # Defines deployment settings


✅ 3. Define databricks.yml for Deployment

Create databricks.yml in the repo to configure deployment:

bundle:
  name: my_project_dev
targets:
  dev:
    workspace:
      host: https://adb-<workspace-id>.<region>.azuredatabricks.net
    jobs:
      - name: my_etl_job
        job_clusters:
          - new_cluster:
              spark_version: 11.3.x-scala2.12
              node_type_id: Standard_DS3_v2
              num_workers: 2

✅ 4. Deploy to Databricks Workspace using CLI

Install Databricks CLI and authenticate:
databricks auth login --host https://adb-<workspace-id>.<region>.azuredatabricks.net

Deploy using DAB commands:
databricks bundle deploy --target dev


Run jobs after deployment:
databricks bundle run --target dev


✅ 5. Automate with Azure DevOps Pipelines

Use YAML-based Azure DevOps pipeline to automate deployment:

trigger:
  branches:
    include:
      - dev
jobs:
  - job: DeployToDatabricks
    pool:
      vmImage: 'ubuntu-latest'
    steps:
      - script: |
          pip install databricks-cli
          databricks auth login --token $(DATABRICKS_TOKEN)
          databricks bundle deploy --target dev
        env:
          DATABRICKS_TOKEN: $(DATABRICKS_TOKEN)


✅ Databricks Asset Bundles (DAB) streamline deployments to workspaces.
✅ Use databricks.yml to define job configurations & clusters.
✅ Automate deployments using Azure DevOps pipelines.
✅ Deploy via CLI or pipeline with databricks bundle deploy.

💡 CI/CD with Databricks Asset Bundles ensures reliable, repeatable, and automated deployments!


-------------------------------------------------------

Create a databricks.yml in Your Repo (Defines Deployment Settings)


bundle:
  name: my_project_dev
targets:
  dev:
    workspace:
      host: https://adb-<workspace-id>.<region>.azuredatabricks.net
    jobs:
      - name: my_etl_job
        job_clusters:
          - new_cluster:
              spark_version: 11.3.x-scala2.12
              node_type_id: Standard_DS3_v2
              num_workers: 2


Azure DevOps CI/CD YAML (azure-pipelines.yml)


trigger:
  branches:
    include:
      - dev  # Triggers pipeline on 'dev' branch commits

pool:
  vmImage: 'ubuntu-latest'  # Uses a Linux VM for execution

variables:
  DATABRICKS_HOST: "https://adb-<workspace-id>.<region>.azuredatabricks.net"
  DATABRICKS_TOKEN: $(DATABRICKS_TOKEN)  # Store this securely in Azure DevOps secrets

steps:
  - task: UsePythonVersion@0
    inputs:
      versionSpec: '3.x'

  - script: |
      python -m pip install --upgrade pip
      pip install databricks-cli
      databricks auth login --token $(DATABRICKS_TOKEN)
    displayName: "Install Databricks CLI & Authenticate"

  - script: |
      databricks bundle deploy --target dev
    displayName: "Deploy to Databricks Workspace"

  - script: |
      databricks bundle run --target dev
    displayName: "Run Databricks Job"



Explanation of Key Sections
✅ Triggers on dev Branch – Runs when changes are pushed to dev.
✅ Uses Databricks CLI – Installs CLI & authenticates using a secure token.
✅ Deploys to Databricks – Uses databricks bundle deploy.
✅ Runs the Job – Executes the Databricks job using databricks bundle run.

💡 This CI/CD pipeline ensures automated & secure deployment of your Databricks workloads!


--------------------------------------------------------

When to Use pyspark.udf, pandas_udf, and pyspark.pandas.apply?

1️⃣ pyspark.udf (User Defined Function)
Use when applying Python functions row-wise on PySpark DataFrame.
Slower than pandas_udf as it does not leverage vectorization.
Best for complex logic that isn't supported natively by PySpark.'


from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

def square(x):
    return x * x

square_udf = udf(square, IntegerType())
df = df.withColumn("squared_value", square_udf(df["value"]))


2️⃣ pandas_udf (Vectorized Pandas UDF)
Use when applying vectorized functions to PySpark DataFrames.
Faster than pyspark.udf as it uses Apache Arrow for efficient execution.
Best for operations on large datasets that involve Pandas-like computations.
  
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import IntegerType
import pandas as pd

@pandas_udf(IntegerType())
def square_pandas_udf(x: pd.Series) -> pd.Series:
    return x * x

df = df.withColumn("squared_value", square_pandas_udf(df["value"]))


3️⃣ pyspark.pandas.apply (Koalas)
Use when working with pyspark.pandas (Koalas DataFrame) to apply functions.
Best for row-wise and element-wise transformations on Pandas-on-Spark DataFrames.
More readable & Pythonic than UDFs.

import pyspark.pandas as ps
df = ps.DataFrame({'value': [1, 2, 3, 4]})
def square(x):
    return x * x

df['squared_value'] = df['value'].apply(square)
print(df)


When to Use What?
Method	When to Use?	Performance
pyspark.udf	When applying Python functions to PySpark DataFrame (row-wise).	❌ Slow (Python Execution)
pandas_udf	When applying vectorized Pandas functions in PySpark.	✅ Fast (Uses Apache Arrow)
pyspark.pandas.apply	When working with Pandas-on-Spark (pyspark.pandas).	✅ Efficient & Pythonic

Use pandas_udf for best performance, pyspark.udf for flexibility, and pyspark.pandas.apply for Pandas-on-Spark transformations! 

--------------------------------------
##################################################
##################################################
##################################################


API endpoints on flask, data architecture creation data warehouser, bring model to producton
, azure databricks ke andar  workflows and job and pipeline banate hai

how to create job and pipeline  inside  azure databricks workflows



stg file write to take approveal from manager
azure repo code in azure versionong dev, stg, uat, prod

ci/cd 

curently working in data enginnering part More 

how to create api in flask and deploy and create api endpoint , test inside postman, model call in 
deployu thoruh api


python script - create python project and depoly 
src 
notebook
config 


azure devops -> azure repo  (for cicd - git gitlab)


i write code in pyspark when  we need to write comlex logic then koalas because of distributed-


highly optimize , reduce cost, 


because of the role i am not 

i onpen to work in any profile 
i can work 24*7
my passion is to work


i am not looking for money as of now looking for work as of now i am egar to work i need an opporutintiy 
where i can contribute in the team



✅ 4. Approval Flow for STG to UAT to PROD

- stage: DeployToStaging
  jobs:
    - job: ApproveStage
      pool:
        vmImage: 'ubuntu-latest'
      steps:
        - script: echo "Waiting for manager approval"
