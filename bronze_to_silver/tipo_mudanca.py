# Databricks notebook source
# MAGIC %md
# MAGIC # Carregamento e tratamento dos dados de Tipo de Mudança
# MAGIC
# MAGIC Os dados de Tipo de Mudança são fixos e são definidos aqui nesse escopo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros:
# MAGIC
# MAGIC Não é necessário parâmetros pois são fixos.

# COMMAND ----------

tipo_mudanca = spark.createDataFrame([
    ("1", "Estado"),
    ("2", "Cidade")
], ["codigo_tipo_mudanca", "descricao_tipo_mudanca"])

# COMMAND ----------

tipo_mudanca.write.mode("overwrite").saveAsTable("silver.tipo_mudanca")

# COMMAND ----------

dbutils.notebook.exit("Success")
