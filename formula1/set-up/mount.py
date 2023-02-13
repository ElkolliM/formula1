# Databricks notebook source
storage_account_name = 'formula1md'
storage_container_name_raw ='raw'
storage_container_name_processed = 'processed'
storage_container_name_presentation = 'presentation'
storage_container_name_demo = 'demo'

# COMMAND ----------

dbutils.fs.mount(
    source=f'wasbs://{storage_container_name_demo}@{storage_account_name}.blob.core.windows.net',
    mount_point=f'/mnt/{storage_account_name}/demo',
    extra_configs={f'fs.azure.account.key.{storage_account_name}.blob.core.windows.net':dbutils.secrets.get('formula1-scope','key-dhiamontadatalake')}
)

# COMMAND ----------

def mount_adls(container_name)
dbutils.fs.mount(
    source=f'wasbs://{storage_container_name_processed}@{storage_account_name}.blob.core.windows.net',
    mount_point=f'/mnt/{storage_account_name}/{container_name}',
    extra_configs={f'fs.azure.account.key.{storage_account_name}.blob.core.windows.net':dbutils.secrets.get('formula1-scope','key-dhiamontadatalake')}
)

# COMMAND ----------

mount_adls("processed")

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

mount_

# COMMAND ----------



# COMMAND ----------

dbutils.fs.ls("/mnt/formula1md/presentation")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1md/processed")