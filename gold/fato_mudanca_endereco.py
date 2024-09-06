# Databricks notebook source
# MAGIC %md
# MAGIC # Cria a fato_mudanca_endereco na camada _gold_ 
# MAGIC
# MAGIC Cria a fato fazendo o agrupamento dos dados de acordo com as regras e granularidades estabelecidas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros:
# MAGIC 1. reference: referencia da pasta com a data de disponibilização pela Receita Federal
# MAGIC 2. previous_reference: referencia anterior para fazer a comparação. Por default já calcula a anterior

# COMMAND ----------

# MAGIC %run ../utils/functions

# COMMAND ----------

dbutils.widgets.text("reference", "")
reference = dbutils.widgets.get("reference")

previous_reference = get_previous_reference(reference)
dbutils.widgets.text("previous_reference", previous_reference)


# COMMAND ----------

print(dbutils.widgets.get("reference"))
print(previous_reference)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold.fato_mudanca_endereco (
# MAGIC   referencia_origem STRING,
# MAGIC   referencia_destino STRING,
# MAGIC   codigo_cnae_primario STRING,
# MAGIC   codigo_situacao_cadastral STRING,
# MAGIC   codigo_natureza_juridica STRING,
# MAGIC   codigo_porte_empresa STRING,
# MAGIC   uf_origem STRING,
# MAGIC   codigo_municipio_origem STRING,
# MAGIC   uf_destino STRING,
# MAGIC   codigo_municipio_destino STRING,
# MAGIC   codigo_tipo_mudanca INT,
# MAGIC   quantidade_mudancas BIGINT)
# MAGIC USING delta
# MAGIC PARTITIONED BY (referencia_destino)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into gold.fato_mudanca_endereco
# MAGIC select 
# MAGIC es_origem.reference as referencia_origem,
# MAGIC es_destino.reference as referencia_destino,
# MAGIC es_origem.codigo_cnae_primario,
# MAGIC es_origem.codigo_situacao_cadastral,
# MAGIC emp.codigo_natureza_juridica,
# MAGIC emp.codigo_porte_empresa,
# MAGIC es_origem.uf as uf_origem, 
# MAGIC es_origem.codigo_municipio as codigo_municipio_origem,
# MAGIC es_destino.uf as uf_destino, 
# MAGIC es_destino.codigo_municipio as codigo_municipio_destino,
# MAGIC case when es_origem.uf <> es_destino.uf then 1
# MAGIC else 2 end as codigo_tipo_mudanca,
# MAGIC count(1) as quantidade_mudancas
# MAGIC from silver.estabelecimentos es_origem
# MAGIC inner join silver.estabelecimentos es_destino on es_origem.cnpj_basico = es_destino.cnpj_basico 
# MAGIC   and es_origem.cnpj_ordem = es_destino.cnpj_ordem 
# MAGIC   and es_origem.cnpj_dv = es_destino.cnpj_dv
# MAGIC inner join silver.empresas emp on es_origem.cnpj_basico = emp.cnpj_basico and emp.reference = es_origem.reference
# MAGIC where es_origem.reference = getArgument("previous_reference")
# MAGIC and es_destino.reference = getArgument("reference")
# MAGIC and (es_origem.uf <> es_destino.uf or es_origem.codigo_municipio <> es_destino.codigo_municipio)
# MAGIC group by all

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select 
# MAGIC -- referencia_origem, codigo_tipo_mudanca,
# MAGIC -- count(1)
# MAGIC -- from gold.fato_mudanca_endereco
# MAGIC -- where referencia_origem = getArgument("previous_reference")
# MAGIC -- group by all
