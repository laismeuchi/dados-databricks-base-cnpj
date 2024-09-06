# Databricks notebook source
# MAGIC %md
# MAGIC # Carregamento e tratamento dos dados de Situação Cadastral
# MAGIC
# MAGIC Os dados de Situação Cadastral são fixos e os valores são definidos no dicionário de dados da Receita Federal.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros:
# MAGIC
# MAGIC Não é necessário parâmetros pois são fixos.
# MAGIC

# COMMAND ----------

situacao_cadastral = spark.createDataFrame([
    ("01", "Nula"),
    ("2", "Ativa"),
    ("3", "Suspensa"),
    ("4", "Inapta"),
    ("08", "Baixada")
], ["codigo_situacao_cadastral", "descricao_situacao_cadastral"])

# COMMAND ----------

situacao_cadastral.write.mode("overwrite").saveAsTable("silver.situacao_cadastral")

# COMMAND ----------

dbutils.notebook.exit("Success")
