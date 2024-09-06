# Databricks notebook source
# MAGIC %md
# MAGIC # Carregamento e tratamento dos dados de CNAE da _bronze_ pra _silver_
# MAGIC
# MAGIC Os dados de CNAE podem sempre ser sobrescritos pois não é necessário guardar histórico.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros:
# MAGIC 1. Reference: referencia da pasta com a data de disponibilização pela Receita Federal
# MAGIC

# COMMAND ----------

dbutils.widgets.text("reference", "")
reference = dbutils.widgets.get("reference")

# COMMAND ----------

cnaes_path = f"abfss://bronze@dlsbasecnpjreceita.dfs.core.windows.net/{reference}/Cnaes.parquet"

# COMMAND ----------

cnaes_df = (spark.read.parquet(cnaes_path)
            .withColumnsRenamed({"Prop_0": "codigo_cnae", 
                                 "Prop_1": "descricao_cnae"}))

# COMMAND ----------

cnaes_df.write.mode("overwrite").saveAsTable("silver.cnaes")

# COMMAND ----------

dbutils.notebook.exit("Success")
