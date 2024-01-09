# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC # Chargement des données

# COMMAND ----------

# df = spark.table("train_casted")
df = spark.table("raw_data")
# df_cleaned = spark.table("cleaned_train_casted")
df_cleaned = spark.table("cleaned_data")

# COMMAND ----------

display(df_cleaned)

# COMMAND ----------

# MAGIC %md
# MAGIC # Distribution des etiquettes DPE avant et après nettoyage 

# COMMAND ----------

dpe_grouped = df.groupBy("Etiquette_DPE").count().withColumnRenamed("count", "count_df")
dpe_cleaned_grouped = df_cleaned.groupBy("Etiquette_DPE").count().withColumnRenamed("count", "count_df_cleaned")

count_dpe = dpe_grouped.join(dpe_cleaned_grouped, on="Etiquette_DPE", how="full_outer")

display(count_dpe)

# COMMAND ----------

# MAGIC %md
# MAGIC # Distribution des années de construction

# COMMAND ----------

display(df_cleaned)

# COMMAND ----------

# MAGIC %md
# MAGIC # Word Cloud qualité d'isolation

# COMMAND ----------

display(df_cleaned)

# COMMAND ----------

# MAGIC %md
# MAGIC # Analyse par DPE

# COMMAND ----------

display(df_cleaned)
