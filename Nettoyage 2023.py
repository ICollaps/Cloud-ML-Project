# Databricks notebook source
df = spark.table("train")
raw_len = df.count()
print(raw_len)

display(df)

# COMMAND ----------

cleaned_df = df.drop(
    "_c0",
    "N_DPE",
    "Facteur_couverture_solaire",
    "Surface_habitable_desservie_par_installation_ECS",
    "Emission_GES__clairage",
    "Conso_5_usages___finale__nergie_n_2",
    "Conso_chauffage_d_pensier_installation_chauffage_n_1",
    "Emission_GES_chauffage__nergie_n_2",
    "Code_INSEE__BAN_",
    "Description_g_n_rateur_chauffage_n_2_installation_n_2",
    "Conso_5_usages/m____finale",
    "Conso_5_usages___finale",
    "Configuration_installation_chauffage_n_2",
    "Facteur_couverture_solaire_saisi",
    "Cage_d'escalier",
    "Type_g_n_rateur_froid",
    "Type__metteur_installation_chauffage_n_2",
    "Surface_totale_capteurs_photovolta_que",
    "Type__nergie_n_3", # la rajouter si besoin
    "Etiquette_GES",
    "Type_g_n_rateur_n_1_installation_n_2",
    "Qualit__isolation_plancher_haut_comble_am_nag_",
    "Qualit__isolation_plancher_haut_toit_terrase",
    "Surface_habitable_immeuble"
)
print(cleaned_df.columns)
display(cleaned_df)

# COMMAND ----------

# Nombre de lignes à échantillonner pour chaque classe
num_rows_to_sample = 12470

# DataFrame pour le dataset rééquilibré
balanced_df = cleaned_df.filter(df["Etiquette_DPE"] == 'A')

# Ajouter des échantillons de chaque classe au DataFrame rééquilibré
classes = ['F', 'E', 'B', 'D', 'C', 'G']  # Liste de toutes les classes sauf 'A'

for label in classes:
    # Échantillonnage sans remplacement pour chaque classe
    sampled_df = cleaned_df.filter(cleaned_df["Etiquette_DPE"] == label).sample(False, num_rows_to_sample / cleaned_df.filter(cleaned_df["Etiquette_DPE"] == label).count())
    balanced_df = balanced_df.unionAll(sampled_df)

# Afficher le nombre de lignes par classe pour vérifier
print(balanced_df.columns)
balanced_df.groupBy("Etiquette_DPE").count().show()
display(balanced_df)

# COMMAND ----------

# test
# test2
balanced_df.write.mode("overwrite").saveAsTable("cleaned_train")
