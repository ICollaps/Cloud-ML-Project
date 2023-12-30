# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC # Chargement des données

# COMMAND ----------

df = spark.table("train_casted")
df_cleaned = spark.table("cleaned_train_casted")

# COMMAND ----------

# MAGIC %md
# MAGIC # Distribution des etiquettes DPE avant et après nettoyage 

# COMMAND ----------

dpe_grouped = df.groupBy("Etiquette_DPE").count().withColumnRenamed("count", "count_df")
dpe_cleaned_grouped = df_cleaned.groupBy("Etiquette_DPE").count().withColumnRenamed("count", "count_df_cleaned")

count_dpe = dpe_grouped.join(dpe_cleaned_grouped, on="Etiquette_DPE", how="full_outer")

display(count_dpe)
