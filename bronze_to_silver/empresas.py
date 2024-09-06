# Databricks notebook source
# MAGIC %md
# MAGIC # Carregamento e tratamento dos dados de Empresas da _bronze_ pra _silver_
# MAGIC
# MAGIC Os dados de Empresas são inseridos mantendo o histórico baseado na pasta de referencia conforme disponibilizado pela Receita Federal.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## Parâmetros:
# MAGIC 1. file_num: número do arquivo que deverá ser lido na execução
# MAGIC 2. reference: referencia da pasta com a data de disponibilização pela Receita Federal

# COMMAND ----------

dbutils.widgets.text("reference", "")
dbutils.widgets.text("file_num", "")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import lit

# COMMAND ----------

file_num = dbutils.widgets.get("file_num")
reference = dbutils.widgets.get("reference")

# COMMAND ----------

empresas_path = f"abfss://bronze@dlsbasecnpjreceita.dfs.core.windows.net/{reference}/Empresas{file_num}.parquet"
print(empresas_path)

# COMMAND ----------

columns = {"Prop_0": "cnpj_basico", 
           "Prop_1": "razao_social",
           "Prop_2": "codigo_natureza_juridica",
           "Prop_3": "qualificacao_responsavel",
           "Prop_4": "capital_social",
           "Prop_5": "codigo_porte_empresa",
           "Prop_6": "ente_federativo"	}

# COMMAND ----------

empresas_df = (spark.read.parquet(empresas_path)
               .withColumnsRenamed(columns)
               .withColumn("reference", lit(reference))
               .withColumn("file_num", lit(file_num))
)

# COMMAND ----------

schema = StructType([
    StructField("cnpj_basico", StringType(), True),
    StructField("razao_social", StringType(), True),
    StructField("codigo_natureza_juridica", StringType(), True),
    StructField("qualificacao_responsavel", StringType(), True),
    StructField("capital_social", DoubleType(), True),
    StructField("codigo_porte_empresa", StringType(), True),
    StructField("ente_federativo", StringType(), True)
])

# COMMAND ----------

empresas_df.write.mode("append").partitionBy("reference", "file_num").saveAsTable("silver.empresas")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select reference, file_num, count(1)
# MAGIC from silver.empresas
# MAGIC group by all

# COMMAND ----------

dbutils.notebook.exit("Success")
