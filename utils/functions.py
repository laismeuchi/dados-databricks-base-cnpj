# Databricks notebook source
from pyspark.sql.functions import right, left

# COMMAND ----------

def get_previous_reference(reference):
    month = int(reference[-2:])
    year = int(reference[:4])

    if month > 1 and month < 12:
        previous_month = month - 1
        previous_year = year
    elif month == 1:
        previous_month = 12
        previous_year = year - 1
    else:
        previous_month = 11
        previous_year = year

    return f"{previous_year:04d}-{previous_month:02d}"
