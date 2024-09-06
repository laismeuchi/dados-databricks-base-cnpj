# Databricks notebook source
# MAGIC %md
# MAGIC # Carregamento e tratamento dos dados de Natureza Juridicaa da _bronze_ pra _silver_
# MAGIC
# MAGIC Os dados de Natureza podem sempre ser sobrescritos pois não é necessário guardar histórico.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros:
# MAGIC 1. Reference: referencia da pasta com a data de disponibilização pela Receita Federal
# MAGIC

# COMMAND ----------

dbutils.widgets.text("reference", "")
reference = dbutils.widgets.get("reference")

# COMMAND ----------

natureza_juridica_path = f"abfss://bronze@dlsbasecnpjreceita.dfs.core.windows.net/{reference}/Naturezas.parquet"

# COMMAND ----------

natureza_juridica = (spark.read.parquet(natureza_juridica_path)
            .withColumnsRenamed({"Prop_0": "codigo_natureza_juridica", 
                                 "Prop_1": "descricao_natureza_juridica"}))

# COMMAND ----------

natureza_juridica.write.mode("overwrite").saveAsTable("silver.natureza_juridica")

# COMMAND ----------

dbutils.notebook.exit("Success")
