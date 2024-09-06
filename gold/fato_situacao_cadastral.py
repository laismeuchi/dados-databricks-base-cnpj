# Databricks notebook source
# MAGIC %md
# MAGIC # Cria a fato_situacao_cadastral na camada _gold_ 
# MAGIC
# MAGIC Cria a fato fazendo o agrupamento dos dados de acordo com as regras e granularidades estabelecidas

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold.fato_situcao_cadastral (
# MAGIC   referencia STRING,
# MAGIC   codigo_cnae_primario STRING,
# MAGIC   -- codigo_situacao_cadastral STRING,
# MAGIC   codigo_natureza_juridica STRING,
# MAGIC   codigo_porte_empresa STRING,
# MAGIC   uf STRING,
# MAGIC   codigo_municipio STRING,
# MAGIC   quantidade_empresas_ativas BIGINT,
# MAGIC   quantidade_empresas_suspensas BIGINT,
# MAGIC   quantidade_empresas_inaptas BIGINT,
# MAGIC   quantidade_empresas_baixadas BIGINT)
# MAGIC USING delta
# MAGIC PARTITIONED BY (referencia)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into gold.fato_situcao_cadastral
# MAGIC select 
# MAGIC est.reference,
# MAGIC est.codigo_cnae_primario,
# MAGIC emp.codigo_natureza_juridica,
# MAGIC emp.codigo_porte_empresa,
# MAGIC est.uf,
# MAGIC est.codigo_municipio,
# MAGIC sum(case when est.codigo_situacao_cadastral = '02' then 1 else 0 end) as quantidade_empresas_ativas,
# MAGIC sum(case when est.codigo_situacao_cadastral = '03' then 1 else 0 end) as quantidade_empresas_suspensas,
# MAGIC sum(case when est.codigo_situacao_cadastral = '04' then 1 else 0 end) as quantidade_empresas_inaptas,
# MAGIC sum(case when est.codigo_situacao_cadastral = '08' then 1 else 0 end) as quantidade_empresas_baixadas
# MAGIC from silver.estabelecimentos est
# MAGIC inner join silver.empresas emp on est.cnpj_basico = emp.cnpj_basico and est.reference = emp.reference
# MAGIC group by all
