# Databricks notebook source
# MAGIC %md
# MAGIC # Point de montage

# COMMAND ----------

dbutils.fs.mount(source="wasbs://groupe10@groupe10.blob.core.windows.net",mount_point="/mnt/groupe10mount",extra_configs={"fs.azure.account.key.groupe10.blob.core.windows.net":dbutils.secrets.get(scope="groupe10scope",key="secretgroupe10")})

# COMMAND ----------

# MAGIC %md
# MAGIC # Typage des données

# COMMAND ----------

from pyspark.sql.types import StringType, StructField, StructType, IntegerType, FloatType
schema = StructType([
    StructField("c0", IntegerType(), True),
    StructField("NDPE", StringType(), True),
    StructField("Configuration_installation_chauffage_n2", StringType(), True),
    StructField("Facteur_couverture_solaire_saisi", FloatType(), True),
    StructField("Surface_habitable_desservie_par_installation_ECS", FloatType(), True),
    StructField("EmissionGES_eclairage", FloatType(), True),
    StructField("Cage_escalier", StringType(), True),
    StructField("Conso_5usages_finale_energie_n2", FloatType(), True),
    StructField("Type_generateur_froid", StringType(), True),
    StructField("Type_emetteur_installation_chauffage_n2", StringType(), True),
    StructField("Surface_totale_capteurs_photovoltaique", FloatType(), True),
    StructField("Nom_commune_Brut", StringType(), True),
    StructField("Conso_chauffage_depensier_installation_chauffage_n1", FloatType(), True),
    StructField("Cout_chauffage_energie_n2", FloatType(), True),
    StructField("Emission_GES_chauffage_energie_n2", FloatType(), True),
    StructField("Code_INSEE_BAN", StringType(), True),
    StructField("Type_energie_n3", StringType(), True),
    StructField("Etiquette_GES", StringType(), True),
    StructField("Type_generateur_n1_installation_n2", StringType(), True),
    StructField("Codepostal_brut", IntegerType(), True),
    StructField("Description_generateur_chauffage_n2_installation_n2", StringType(), True),
    StructField("Facteur_couverture_solaire", FloatType(), True),
    StructField("Annee_construction", FloatType(), True),
    StructField("Classe_altitude", StringType(), True),
    StructField("Codepostal_BAN", FloatType(), True),
    StructField("Conso_5usages_m2e_finale", FloatType(), True),
    StructField("Conso_5usagese_finale", FloatType(), True),
    StructField("Etiquette_DPE", StringType(), True),
    StructField("Hauteur_sous_plafond", FloatType(), True),
    StructField("N_departement_BAN", StringType(), True),
    StructField("Qualite_isolation_enveloppe", StringType(), True),
    StructField("Qualite_isolation_menuiseries", StringType(), True),
    StructField("Qualite_isolation_murs", StringType(), True),
    StructField("Qualite_isolation_plancher_bas", StringType(), True),
    StructField("Qualite_isolation_plancher_haut_comble_amenage", StringType(), True),
    StructField("Qualite_isolation_plancher_haut_comble_perdu", StringType(), True),
    StructField("Qualite_isolation_plancher_haut_toit_terrase", StringType(), True),
    StructField("Surface_habitable_immeuble", FloatType(), True),
    StructField("Surface_habitable_logement", FloatType(), True),
    StructField("Type_batiment", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC # Importation des données

# COMMAND ----------

df = spark.read.csv('/mnt/groupe10mount/train.csv', header=True, schema=schema)
df_val = spark.read.csv('/mnt/groupe10mount/val.csv', header=True, schema=schema)

# COMMAND ----------

# MAGIC %md
# MAGIC # Renommage des noms de colonnes

# COMMAND ----------

import re

def clean_column_name(column_name):
    # Supprime les accents et les caractères spéciaux
    column_name = re.sub(r'[^\x00-\x7F]+','_', column_name)
    # Remplace les caractères non autorisés par des underscores
    return re.sub(r'[ ,;{}()\n\t=]', '_', column_name)

df_cleaned = df.toDF(*(clean_column_name(c) for c in df.columns))
df_val_cleaned = df_val.toDF(*(clean_column_name(c) for c in df_val.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC # Ecriture de la table de données bruts

# COMMAND ----------

df_cleaned.write.mode("overwrite").saveAsTable("raw_data")
df_val_cleaned.write.mode("overwrite").saveAsTable("raw_val_data")
