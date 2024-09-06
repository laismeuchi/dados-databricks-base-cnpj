# Databricks notebook source
# MAGIC %md
# MAGIC # Carregamento e tratamento dos dados de Municicipios da _bronze_ pra _silver_
# MAGIC
# MAGIC Os dados de Municipio podem sempre ser sobrescritos pois não é necessário guardar histórico.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros:
# MAGIC 1. Reference: referencia da pasta com a data de disponibilização pela Receita Federal
# MAGIC

# COMMAND ----------

dbutils.widgets.text("reference", "")
reference = dbutils.widgets.get("reference")

# COMMAND ----------

municipios_path = f"abfss://bronze@dlsbasecnpjreceita.dfs.core.windows.net/{reference}/Municipios.parquet"

# COMMAND ----------

municipios_path_df = (spark.read.parquet(municipios_path)
                      .withColumnsRenamed({"Prop_0": "codigo_municipio", 
                                           "Prop_1": "descricao_municipio"}))

# COMMAND ----------

municipios_path_df.write.mode("overwrite").saveAsTable("silver.municipios")

# COMMAND ----------

dbutils.notebook.exit("Success")
