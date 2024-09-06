# Databricks notebook source
# MAGIC %md
# MAGIC # Carregamento e tratamento dos dados de Porte da Empresa
# MAGIC
# MAGIC Os dados de Porte da Empresa são fixos e os valores são definidos no dicionário de dados da Receita Federal.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros:
# MAGIC
# MAGIC Não é necessário parâmetros pois são fixos.
# MAGIC

# COMMAND ----------

porte_empresa = spark.createDataFrame([
    ("00", "Não Informado"),
    ("01", "Micro Empresa"),
    ("03", "Empresa de Pequeno Porte"),
    ("05", "Demais")
], ["codigo_porte_empresa", "descricao_porte_empresa"])

# COMMAND ----------

porte_empresa.write.mode("overwrite").saveAsTable("silver.porte_empresa")

# COMMAND ----------

dbutils.notebook.exit("Success")
