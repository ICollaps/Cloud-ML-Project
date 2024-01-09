# Databricks notebook source
# MAGIC %pip install flaml

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import numpy as np
import pandas as pd
from flaml import AutoML
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score

# COMMAND ----------

df = spark.table("raw_data")
display(df)


# COMMAND ----------

data = df.toPandas()
df_cleaned = data.drop(columns=["c0", "NDPE"])

# COMMAND ----------

y = df_cleaned["Etiquette_DPE"]
X = df_cleaned.drop(columns=["Etiquette_DPE"])

# COMMAND ----------

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# COMMAND ----------

automl = AutoML()

automl_settings = {
    "time_budget": 480,
    "metric": 'accuracy',
    "task": 'classification',
    "log_file_name": "flaml.log",
}

# COMMAND ----------

automl.fit(X_train=X_train, y_train=y_train, **automl_settings)

# COMMAND ----------

y_pred = automl.predict(X_test)


# COMMAND ----------

accuracy = accuracy_score(y_test, y_pred)
classification_rep = classification_report(y_test, y_pred)

print("Accuracy:", accuracy)
print("Classification Report:\n", classification_rep)
