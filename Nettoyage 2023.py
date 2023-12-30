# Databricks notebook source
# MAGIC %md
# MAGIC # Importation des données bruts

# COMMAND ----------

# df = spark.table("train_casted")
df = spark.table("raw_data")
raw_len = df.count()
print(raw_len)

display(df)

# COMMAND ----------

# no_nulls_columns = ["EmissionGES_eclairage" , "Conso_5usages_finale_energie_n2" , "Conso_chauffage_depensier_installation_chauffage_n1" , "Cout_chauffage_energie_n2" , "Emission_GES_chauffage_energie_n2" , "Codepostal_brut" , "Annee_construction" , "Codepostal_BAN" , "Conso_5usages_m2e_finale" , "Conso_5usagese_finale" , "Hauteur_sous_plafond" , "Surface_habitable_logement" , "Nom_commune_Brut" , "Code_INSEE_BAN" , "Etiquette_GES" , "Classe_altitude" , "Etiquette_DPE" , "N_departement_BAN" , "Qualite_isolation_enveloppe" , "Qualite_isolation_menuiseries" , "Qualite_isolation_murs" , "Qualite_isolation_plancher_bas" , "Type_batiment"]

# COMMAND ----------

display(df)

# COMMAND ----------

keeped_columns = ["Conso_5usages_finale_energie_n2" , "Conso_chauffage_depensier_installation_chauffage_n1" , "Codepostal_brut" , "Annee_construction" , "Codepostal_BAN" , "Conso_5usages_m2e_finale" , "Conso_5usagese_finale" , "Hauteur_sous_plafond" , "Surface_habitable_logement" , "Nom_commune_Brut" , "Classe_altitude" , "Etiquette_DPE" , "N_departement_BAN" , "Qualite_isolation_enveloppe" , "Qualite_isolation_menuiseries" , "Qualite_isolation_murs" , "Qualite_isolation_plancher_bas" , "Type_batiment"]

df_selected = df.select(*keeped_columns)

# COMMAND ----------

print(df_selected.columns)
display(df_selected)

# COMMAND ----------

# MAGIC %md
# MAGIC # Suppression des variables non utilisées

# COMMAND ----------

# cleaned_df = df.drop(
#     # "_c0",
#     "c0",
#     "NDPE",
#     "N_DPE",
#     "Facteur_couverture_solaire",
#     "Surface_habitable_desservie_par_installation_ECS",
#     "Emission_GES__clairage",
#     "Conso_5_usages___finale__nergie_n_2",
#     "Conso_chauffage_d_pensier_installation_chauffage_n_1",
#     "Emission_GES_chauffage__nergie_n_2",
#     "Code_INSEE__BAN_",
#     "Description_g_n_rateur_chauffage_n_2_installation_n_2",
#     "Conso_5_usages/m____finale",
#     "Conso_5_usages___finale",
#     "Configuration_installation_chauffage_n_2",
#     "Facteur_couverture_solaire_saisi",
#     "Cage_d'escalier",
#     "Type_g_n_rateur_froid",
#     "Type__metteur_installation_chauffage_n_2",
#     "Surface_totale_capteurs_photovolta_que",
#     "Type__nergie_n_3", # la rajouter si besoin
#     "Etiquette_GES",
#     "Type_g_n_rateur_n_1_installation_n_2",
#     "Qualit__isolation_plancher_haut_comble_am_nag_",
#     "Qualit__isolation_plancher_haut_toit_terrase",
#     "Surface_habitable_immeuble"
# )
# print(cleaned_df.columns)
# display(cleaned_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Rééquilibration du dataset

# COMMAND ----------

# Affichage du déséquilibre du dataset
df_selected.groupBy("Etiquette_DPE").count().show()

# COMMAND ----------

# Nombre de lignes à échantillonner pour chaque classe
num_rows_to_sample = 12470

# DataFrame pour le dataset rééquilibré
balanced_df = df_selected.filter(df["Etiquette_DPE"] == 'A')

# Ajouter des échantillons de chaque classe au DataFrame rééquilibré
classes = ['F', 'E', 'B', 'D', 'C', 'G']  # Liste de toutes les classes sauf 'A'

for label in classes:
    # Échantillonnage sans remplacement pour chaque classe
    sampled_df = df_selected.filter(df_selected["Etiquette_DPE"] == label).sample(False, num_rows_to_sample / df_selected.filter(df_selected["Etiquette_DPE"] == label).count())
    balanced_df = balanced_df.unionAll(sampled_df)

# Afficher le nombre de lignes par classe pour vérifier
print(balanced_df.columns)
balanced_df.groupBy("Etiquette_DPE").count().show()
display(balanced_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Ecriture de la table de données nettoyées

# COMMAND ----------

# test
# balanced_df.write.mode("overwrite").saveAsTable("cleaned_train_casted")
balanced_df.write.mode("overwrite").saveAsTable("cleaned_data")
