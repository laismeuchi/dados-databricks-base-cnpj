# Databricks notebook source
# MAGIC %md
# MAGIC # Executa todos os notebooks de carregamento e tratamento dos dados da _bronze_ pra _silver_
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros:
# MAGIC 1. reference: referencia da pasta com a data de disponibilização pela Receita Federal

# COMMAND ----------

dbutils.widgets.text("reference", "")
reference = dbutils.widgets.get("reference")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executa os notebooks que são fixos

# COMMAND ----------

dbutils.notebook.run("porte_empresa",0,{})

# COMMAND ----------

dbutils.notebook.run("situacao_cadastral",0,{})

# COMMAND ----------

dbutils.notebook.run("tipo_mudanca",0,{})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executa os notebooks dos que serão sempre sobresctitos com a ultima dispnibilização da Receita Federal
# MAGIC

# COMMAND ----------

dbutils.notebook.run("cnaes",0,{"reference":reference})

# COMMAND ----------

dbutils.notebook.run("municipios",0,{"reference":reference})

# COMMAND ----------

dbutils.notebook.run("natureza_juridica",0,{"reference":reference})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executa os notebooks dos que serão sempre adicionados com a ultima dispnibilização da Receita Federal
# MAGIC

# COMMAND ----------

for i in range(0,10):
    dbutils.notebook.run("estabelecimentos",0,{"reference":reference,
                                               "file_num": i})

# COMMAND ----------

for i in range(0,10):
    dbutils.notebook.run("empresas",0,{"reference":reference,
                                               "file_num": i})
