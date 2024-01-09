# Databricks notebook source
df = spark.table("cleaned_data")
display(df)

# COMMAND ----------

data = df.toPandas()

# COMMAND ----------

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.preprocessing import LabelEncoder
from sklearn.impute import SimpleImputer

# COMMAND ----------

# Sélection des colonnes pertinentes et nettoyage des données
features = data.select_dtypes(include=[int, float, 'category', object]).drop('Etiquette_DPE', axis=1)
target = data['Etiquette_DPE']

# COMMAND ----------

# Encodage des variables catégorielles
label_encoders = {}
for column in features.select_dtypes(include=['category', object]).columns:
    label_encoders[column] = LabelEncoder()
    features[column] = label_encoders[column].fit_transform(features[column].astype(str))


# COMMAND ----------

# Gestion des valeurs manquantes
imputer = SimpleImputer(strategy='mean')
features = pd.DataFrame(imputer.fit_transform(features), columns=features.columns)

# COMMAND ----------

# Séparation en ensembles d'entraînement et de test
X_train, X_test, y_train, y_test = train_test_split(features, target, test_size=0.2, random_state=42)

# COMMAND ----------

# Création et entraînement du modèle
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# COMMAND ----------

# Prédiction et évaluation
y_pred = model.predict(X_test)
accuracy = accuracy_score(y_test, y_pred)
print(f'Accuracy: {accuracy}')

# COMMAND ----------

# import mlflow
# from mlflow.models.signature import infer_signature

# train = X_train
# predictions = model.predict(X_test)
# signature = infer_signature(train, predictions)
# input_example = X_train.head(3)
# conda_env = {
#     'channels': ['conda-forge'],
#     'dependencies': [
#         'python=3.8.10',
#         {
#     'pip': [
#         'mlflow==1.30.0',
#         'cloudpickle==1.6.0',
#         'databricks-automl-runtime==0.2.6.4',
#         'holidays==0.12',
#         'koalas==1.8.2',
#         'psutil==5.8.0',
#         'scikit-learn==0.24.1',
#         'typing-extensions==3.7.4.3'
#     ]
#         }],
#     'name': 'mlflow-env'
# }
# mlflow.sklearn.log_model(model, "to_api",conda_env=conda_env,signature=signature, input_example=input_example)

