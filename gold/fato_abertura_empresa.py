# Databricks notebook source
# MAGIC %md
# MAGIC # Cria a fato_abertura_empresa na camada _gold_ 
# MAGIC
# MAGIC Cria a fato fazendo o agrupamento dos dados de acordo com as regras e granularidades estabelecidas

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold.fato_abertura_empresa (
# MAGIC   referencia STRING,
# MAGIC   codigo_cnae_primario STRING,
# MAGIC   codigo_natureza_juridica STRING,
# MAGIC   codigo_porte_empresa STRING,
# MAGIC   uf STRING,
# MAGIC   codigo_municipio STRING,
# MAGIC   data_inicio_atividade DATE,
# MAGIC   quantidade_empresas_abertas BIGINT)
# MAGIC USING delta
# MAGIC PARTITIONED BY (referencia)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into gold.fato_abertura_empresa
# MAGIC select 
# MAGIC est.reference,
# MAGIC est.codigo_cnae_primario,
# MAGIC emp.codigo_natureza_juridica,
# MAGIC emp.codigo_porte_empresa,
# MAGIC est.uf,
# MAGIC est.codigo_municipio,
# MAGIC est.data_inicio_atividade,
# MAGIC count(1) as quantidade_empresas_abertas
# MAGIC from silver.estabelecimentos est
# MAGIC inner join silver.empresas emp on est.cnpj_basico = emp.cnpj_basico and est.reference = emp.reference
# MAGIC group by all
