# Databricks notebook source
# MAGIC %md
# MAGIC ## Configure databricks to download secrets from Azure KeyVault
# MAGIC 
# MAGIC 1. Navigate to #secrets/createScope
# MAGIC 2. Get DNS value from KeyVault Properties
# MAGIC 3. Get Resource ID from KeyVault Properties
# MAGIC 4. Create Secret Scope

# COMMAND ----------

# Get required secrets from azure keyvault
blob_storage_url = dbutils.secrets.get(scope = "databricks_scope", key = "blobstorageurl")
blob_storage_key = dbutils.secrets.get(scope = "databricks_scope", key = "blobstoragekey")
adw_connection = dbutils.secrets.get(scope = "databricks_scope", key = "dwconnection")
blob_storage_temp_dir = dbutils.secrets.get(scope = "databricks_scope", key = "blobstoragetempdir")

# COMMAND ----------

# Set configuration
spark.conf.set(blob_storage_url, blob_storage_key)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Spark driver to SQL DW
# MAGIC 
# MAGIC The Spark driver connects to SQL DW via JDBC using a username and password. We recommended that you use the connection string provided by Azure portal, which enables Secure Sockets Layer (SSL) encryption for all data sent between the Spark driver and the SQL DW instance through the JDBC connection. To verify that the SSL encryption is enabled, you can search for encrypt=true in the connection string. To allow the Spark driver to reach SQL DW, we recommend that you set Allow access to Azure services to ON on the firewall pane of the SQL DW server through Azure portal. This setting allows communications from all Azure IP addresses and all Azure subnets, which allows Spark drivers to reach the SQL DW instance.
# MAGIC 
# MAGIC For more info see [here](https://docs.databricks.com/spark/latest/data-sources/azure/sql-data-warehouse.html)

# COMMAND ----------

 # Load data from a SQL DW query.
df = spark.read.format("com.databricks.spark.sqldw").option("url", adw_connection).option("tempDir", blob_storage_temp_dir).option("forwardSparkAzureStorageCredentials", "true").option("query", "select count(*) as counter from table_name").load()

df.show()

# COMMAND ----------

