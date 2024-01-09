# Databricks notebook source
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
