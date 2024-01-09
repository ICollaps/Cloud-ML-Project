# Databricks notebook source
# MAGIC %md
# MAGIC # API DOCUMENTATION 
# MAGIC
# MAGIC #### Prédiction du DPE
# MAGIC
# MAGIC <details>
# MAGIC  <summary><code>POST</code> <code><b>/model/model_metier_api/1/invocations</b></code> <code>(envoie les données pour prédiction)</code></summary>
# MAGIC
# MAGIC ##### Paramètres
# MAGIC
# MAGIC > | nom                                         | type      | data type |
# MAGIC > |---------------------------------------------|-----------|-----------|
# MAGIC > | Conso_5usages_finale_energie_n2             | obligatoire | float     |
# MAGIC > | Conso_chauffage_depensier_installation_chauffage_n1 | obligatoire | float     |
# MAGIC > | Codepostal_brut                             | obligatoire | int       |
# MAGIC > | Annee_construction                          | obligatoire | float     |
# MAGIC > | Codepostal_BAN                              | obligatoire | int       |
# MAGIC > | Conso_5usages_m2e_finale                    | obligatoire | int       |
# MAGIC > | Conso_5usagese_finale                       | obligatoire | float     |
# MAGIC > | Hauteur_sous_plafond                        | obligatoire | float     |
# MAGIC > | Surface_habitable_logement                  | obligatoire | float     |
# MAGIC > | Nom_commune_Brut                            | obligatoire | int       |
# MAGIC > | Classe_altitude                             | obligatoire | int       |
# MAGIC > | N_departement_BAN                           | obligatoire | int       |
# MAGIC > | Qualite_isolation_enveloppe                 | obligatoire | int       |
# MAGIC > | Qualite_isolation_menuiseries               | obligatoire | int       |
# MAGIC > | Qualite_isolation_murs                      | obligatoire | int       |
# MAGIC > | Qualite_isolation_plancher_bas              | obligatoire | int       |
# MAGIC > | Type_batiment                               | obligatoire | int       |
# MAGIC
# MAGIC
# MAGIC ##### Réponses
# MAGIC
# MAGIC > | http code | content-type         | réponse                                                      |
# MAGIC > |-----------|----------------------|--------------------------------------------------------------|
# MAGIC > | `200`     | `application/json`   | Prédiction sous forme de JSON  ex : ['D', 'F', 'F']                              |
# MAGIC > | `400`     | `application/json`   | `{"code":"400","message":"Requête invalide"}`                |
# MAGIC > | `500`     | `application/json`   | `{"code":"500","message":"Erreur interne du serveur"}`       |
# MAGIC
# MAGIC ##### Exemple cURL
# MAGIC
# MAGIC > ```shell
# MAGIC > curl \
# MAGIC   -u token:$DATABRICKS_TOKEN \
# MAGIC   -X POST \
# MAGIC   -H "Content-Type: application/json; format=pandas-split" \
# MAGIC   -d@data.json \
# MAGIC   https://adb-5030543344023554.14.azuredatabricks.net/model/model_metier_api/1/invocations
# MAGIC > ```
# MAGIC
# MAGIC ##### Exemple data
# MAGIC > ```json
# MAGIC > {
# MAGIC   "columns": [
# MAGIC     "Conso_5usages_finale_energie_n2",
# MAGIC     "Conso_chauffage_depensier_installation_chauffage_n1",
# MAGIC     "Codepostal_brut",
# MAGIC     "Annee_construction",
# MAGIC     "Codepostal_BAN",
# MAGIC     "Conso_5usages_m2e_finale",
# MAGIC     "Conso_5usagese_finale",
# MAGIC     "Hauteur_sous_plafond",
# MAGIC     "Surface_habitable_logement",
# MAGIC     "Nom_commune_Brut",
# MAGIC     "Classe_altitude",
# MAGIC     "N_departement_BAN",
# MAGIC     "Qualite_isolation_enveloppe",
# MAGIC     "Qualite_isolation_menuiseries",
# MAGIC     "Qualite_isolation_murs",
# MAGIC     "Qualite_isolation_plancher_bas",
# MAGIC     "Type_batiment"
# MAGIC   ],
# MAGIC   "data": [
# MAGIC     [
# MAGIC       28554.400390625,
# MAGIC       126435.1015625,
# MAGIC       73130,
# MAGIC       1973.0843698975805,
# MAGIC       73130,
# MAGIC       154,
# MAGIC       131111.5,
# MAGIC       2.5,
# MAGIC       84.10402855938703,
# MAGIC       20134,
# MAGIC       0,
# MAGIC       73,
# MAGIC       1,
# MAGIC       0,
# MAGIC       1,
# MAGIC       4,
# MAGIC       1
# MAGIC     ]
# MAGIC   ]
# MAGIC }
# MAGIC > ```
# MAGIC
# MAGIC </details>
# MAGIC

# COMMAND ----------

import os
import requests
import numpy as np
import pandas as pd
import json

def create_tf_serving_json(data):
  return {'inputs': {name: data[name].tolist() for name in data.keys()} if isinstance(data, dict) else data.tolist()}

def score_model(dataset):
  url = 'https://adb-5030543344023554.14.azuredatabricks.net/model/model_metier_api/1/invocations'
  headers = {'Authorization': f'Bearer {os.environ.get("DATABRICKS_TOKEN")}', 'Content-Type': 'application/json'}
  ds_dict = dataset.to_dict(orient='split') if isinstance(dataset, pd.DataFrame) else create_tf_serving_json(dataset)
  data_json = json.dumps(ds_dict, allow_nan=True)
  response = requests.request(method='POST', headers=headers, url=url, data=data_json)
  if response.status_code != 200:
    raise Exception(f'Request failed with status {response.status_code}, {response.text}')
  return response.json()

# COMMAND ----------

data = {
  "columns": [
    "Conso_5usages_finale_energie_n2",
    "Conso_chauffage_depensier_installation_chauffage_n1",
    "Codepostal_brut",
    "Annee_construction",
    "Codepostal_BAN",
    "Conso_5usages_m2e_finale",
    "Conso_5usagese_finale",
    "Hauteur_sous_plafond",
    "Surface_habitable_logement",
    "Nom_commune_Brut",
    "Classe_altitude",
    "N_departement_BAN",
    "Qualite_isolation_enveloppe",
    "Qualite_isolation_menuiseries",
    "Qualite_isolation_murs",
    "Qualite_isolation_plancher_bas",
    "Type_batiment"
  ],
  "data": [
    [
      28554.400390625,
      126435.1015625,
      73130,
      1973.0843698975805,
      73130,
      154,
      131111.5,
      2.5,
      84.10402855938703,
      20134,
      0,
      73,
      1,
      0,
      1,
      4,
      1
    ],
    [
      4106.358729515268,
      3914.199951171875,
      75004,
      1973.0843698975805,
      75004,
      181,
      4715.2001953125,
      2.5,
      26,
      15199,
      2,
      75,
      0,
      1,
      2,
      4,
      0
    ],
    [
      5051.2001953125,
      17246.900390625,
      89320,
      1947,
      89320,
      182,
      16214.7001953125,
      2.799999952316284,
      88.9000015258789,
      4970,
      2,
      89,
      1,
      1,
      2,
      2,
      2
    ]
  ]
}

# COMMAND ----------

df = pd.DataFrame(data=data['data'], columns=data['columns'])

os.environ["DATABRICKS_TOKEN"] = "dapi21c891542cd98f709cb69080c7644fa5"

try:
    predictions = score_model(df)
    print(predictions)
except Exception as e:
    print("Erreur", e)
