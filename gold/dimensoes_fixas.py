# Databricks notebook source
# MAGIC %md
# MAGIC # Carregamento das dimensões fixas da _silver_ pra _gold_
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table gold.dim_municipios(
# MAGIC   codigo_municipio STRING,
# MAGIC   descricao_municipio STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into gold.dim_municipios
# MAGIC select * from silver.municipios

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table gold.dim_cnaes(
# MAGIC   codigo_cnae STRING,
# MAGIC   descricao_cnae STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into gold.dim_cnaes
# MAGIC select * from silver.cnaes

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table gold.dim_natureza_juridica(
# MAGIC   codigo_natureza_juridica STRING,
# MAGIC   descricao_natureza_juridica STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into gold.dim_natureza_juridica
# MAGIC select * from silver.natureza_juridica

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table gold.dim_porte_empresa(
# MAGIC   codigo_porte_empresa STRING,
# MAGIC   descricao_porte_empresa STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into gold.dim_porte_empresa
# MAGIC select * from silver.porte_empresa

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table gold.dim_situacao_cadastral(
# MAGIC   codigo_situacao_cadastral STRING,
# MAGIC   descricao_situacao_cadastral STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into gold.dim_situacao_cadastral
# MAGIC select * from silver.situacao_cadastral

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table gold.dim_tipo_mudanca(
# MAGIC   codigo_tipo_mudanca STRING,
# MAGIC   descricao_tipo_mudanca STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into gold.dim_tipo_mudanca
# MAGIC select * from silver.tipo_mudanca
