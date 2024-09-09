# Databricks notebook source
# MAGIC %md
# MAGIC ## Cria a dimensão de calendário
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime, timedelta

# COMMAND ----------

startdate = datetime.strptime('1900-01-01','%Y-%m-%d')
enddate   = (datetime.now() + timedelta(days=365 * 3)).replace(month=12, day=31)


# COMMAND ----------

print(startdate)
print(enddate)

# COMMAND ----------

column_rule_df = spark.createDataFrame(
    [
        ("Data_Inteiro", "cast(date_format(date, 'yyyyMMdd') as int)"),  # 20230101
        ("Ano", "year(date)"),  # 2023
        ("Trimestre", "quarter(date)"),  # 1
        ("Mes", "month(date)"),  # 1
        ("Dia", "day(date)"),  # 1
        ("Semana", "weekofyear(date)"),  # 1
        ("Trimestre_Nome", "concat(quarter(date),'º trimestre')") ,  # 1º trimestre
        ("Trimestre_Nome_Abreviado", "concat('T',quarter(date))") ,  # T1
        ("Trimestre_Numero_2d", "date_format(date, 'QQ')"),  # 01
        ("Numero_Mes_2d", "date_format(date, 'MM')"),  # 01
        ("Dia_Numero_2d", "date_format(date, 'dd')"),  # 01
        ("Dia_Semana_Numero", "dayofweek(date)"),  # 1
        ("Ano_Mes", "date_format(date, 'yyyy/MM')"),  # 2023/01
        (
            "Nome_Mes_Completo",
            "CASE WHEN month(date) = 1 THEN 'Janeiro' WHEN month(date) = 2 THEN 'Fevereiro' WHEN month(date) = 3 THEN 'Março' WHEN month(date) = 4 THEN 'ABril' WHEN month(date) = 5 THEN 'Maio' WHEN month(date) = 6 THEN 'Junho' WHEN month(date) = 7 THEN 'Julho' WHEN month(date) = 8 THEN 'Agosto' WHEN month(date) = 9 THEN 'Setembro' WHEN month(date) = 10 THEN 'Outubro' WHEN month(date) = 11 THEN 'Novembro' WHEN month(date) = 12 THEN 'Dezembro' ELSE '' END",
        ),  # Janeiro
        (
            "Nome_Mes_Abreviado",
            "CASE WHEN month(date) = 1 THEN 'Jan' WHEN month(date) = 2 THEN 'Fev' WHEN month(date) = 3 THEN 'Mar' WHEN month(date) = 4 THEN 'Apr' WHEN month(date) = 5 THEN 'Mai' WHEN month(date) = 6 THEN 'Jun' WHEN month(date) = 7 THEN 'Jul' WHEN month(date) = 8 THEN 'Ago' WHEN month(date) = 9 THEN 'Set' WHEN month(date) = 10 THEN 'Out' WHEN month(date) = 11 THEN 'Nov' WHEN month(date) = 12 THEN 'Dez' ELSE '' END",
        ),  # Jan
        (
            "Dia_Semana",
            "CASE WHEN dayofweek(date) = 1 THEN 'Domingo' WHEN dayofweek(date) = 2 THEN 'Segunda-feira' WHEN dayofweek(date) = 3 THEN 'Terça-feira' WHEN dayofweek(date) = 4 THEN 'Quarta-feira' WHEN dayofweek(date) = 5 THEN 'Quinta-feira' WHEN dayofweek(date) = 6 THEN 'Sexta-Feira' WHEN dayofweek(date) = 7 THEN 'Sábado' ELSE '' END",
        ),  # Segunda-feira
        (
            "Dia_Semana_Abreviado",
            "CASE WHEN dayofweek(date) = 1 THEN 'Dom' WHEN dayofweek(date) = 2 THEN 'Seg' WHEN dayofweek(date) = 3 THEN 'Ter' WHEN dayofweek(date) = 4 THEN 'Qua' WHEN dayofweek(date) = 5 THEN 'Qui' WHEN dayofweek(date) = 6 THEN 'Sex' WHEN dayofweek(date) = 7 THEN 'Sáb' ELSE '' END",
        )  # Seg
    ],
    ["new_column_name", "expression"],
)

# COMMAND ----------

start = int(startdate.timestamp())
stop  = int(enddate.timestamp())
df = spark.range(start, stop, 60*60*24).select(col("id").cast("timestamp").cast("date").alias("Date"))

# COMMAND ----------

for row in column_rule_df.collect():
    new_column_name = row["new_column_name"]
    expression = expr(row["expression"])
    df = df.withColumn(new_column_name, expression)

# COMMAND ----------

# display(df)

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("gold.dim_calendario")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select * from gold.dim_calendario
