# Databricks notebook source
# MAGIC %md
# MAGIC # Carregamento e tratamento dos dados de Estabelecimentos da _bronze_ pra _silver_
# MAGIC
# MAGIC Os dados de Estabelecimentos são inseridos mantendo o histórico baseado na pasta de referencia conforme disponibilizado pela Receita Federal.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros:
# MAGIC 1. file_num: número do arquivo que deverá ser lido na execução
# MAGIC 2. reference: referencia da pasta com a data de disponibilização pela Receita Federal

# COMMAND ----------

dbutils.widgets.text("reference", "")
dbutils.widgets.text("file_num", "")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import lit, col, to_date

# COMMAND ----------

file_num = dbutils.widgets.get("file_num")
reference = dbutils.widgets.get("reference")

# COMMAND ----------

estabelecimentos_path = f"abfss://bronze@dlsbasecnpjreceita.dfs.core.windows.net/{reference}/Estabelecimentos{file_num}.parquet"
print(estabelecimentos_path)

# COMMAND ----------

columns = {"Prop_0": "cnpj_basico", 
           "Prop_1": "cnpj_ordem",
           "Prop_2": "cnpj_dv",
           "Prop_3": "identificador_matriz_filial",
           "Prop_4": "nome_fantasia",
           "Prop_5": "codigo_situacao_cadastral",
           "Prop_6": "data_situacao_cadastral_string",
           "Prop_7": "motivo_situacao_cadastral",
           "Prop_8": "nome_cidade_exterior",
           "Prop_9": "codigo_pais",
           "Prop_10": "data_inicio_atividade_string",
           "Prop_11": "codigo_cnae_primario",
           "Prop_12": "codigo_cnae_secundario",
           "Prop_13": "tipo_logradouro",
           "Prop_14": "logradouro",
           "Prop_15": "numero",
           "Prop_16": "complemento",
           "Prop_17": "bairro",
           "Prop_18": "cep",
           "Prop_19": "uf",
           "Prop_20": "codigo_municipio",
           "Prop_21": "ddd_telefone1",
           "Prop_22": "telefone1",
           "Prop_23": "ddd_telefone2",
           "Prop_24": "telefone2",
           "Prop_25": "ddd_fax",
           "Prop_26": "fax",
           "Prop_27": "email",
           "Prop_28": "situacao_especial",
           "Prop_29": "data_situacao_especial_string"
           	}

# COMMAND ----------

# get only text values, not numeric
regex_pattern_string = '^[A-Za-z]+$'

# COMMAND ----------

# DBTITLE 1,limpa os registros para não pegar estados que não sejam texto
estabelecimentos_df = (spark.read.parquet(estabelecimentos_path)
               .withColumnsRenamed(columns)
               .withColumn("reference", lit(reference))
               .withColumn("file_num", lit(file_num))
               .withColumn("data_situacao_cadastral", to_date(col("data_situacao_cadastral_string"), "yyyyMMdd"))
               .withColumn("data_inicio_atividade", to_date(col("data_inicio_atividade_string"), "yyyyMMdd"))
               .withColumn("data_situacao_especial", to_date(col("data_situacao_especial_string"), "yyyyMMdd"))
               .filter(col("uf").rlike(regex_pattern_string))
            ).drop("data_situacao_cadastral_string","data_inicio_atividade_string")

# COMMAND ----------

estabelecimentos_df.write.mode("append").partitionBy("reference", "file_num").saveAsTable("silver.estabelecimentos")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select count(1), uf, reference
# MAGIC -- from silver.estabelecimentos
# MAGIC -- group by all

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select reference, file_num, count(1)
# MAGIC -- from silver.estabelecimentos
# MAGIC -- group by all

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select *
# MAGIC -- from silver.estabelecimentos
# MAGIC -- where year(data_inicio_atividade) = 1109
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select year(data_inicio_atividade), count(1) 
# MAGIC -- from silver.estabelecimentos
# MAGIC -- group by all
