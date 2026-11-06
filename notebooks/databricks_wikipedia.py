# Databricks notebook source
# MAGIC %md
# MAGIC # Analisi e classificazione degli articoli di Wikipedia
# MAGIC
# MAGIC Notebook per Databricks che esegue l'intero progetto con il pacchetto `src/wikipedia` del repository:
# MAGIC
# MAGIC 1. download del dataset in un Volume di Unity Catalog e lettura con Spark;
# MAGIC 2. controllo di qualità e pulizia (duplicati, testi vuoti, testi con più categorie);
# MAGIC 3. analisi esplorativa per categoria e nuvole di parole;
# MAGIC 4. confronto dei classificatori, valutazione sul test set e salvataggio del modello;
# MAGIC 5. classificazione di nuovi articoli.
# MAGIC
# MAGIC **Come usarlo**: clonare il repository in Databricks (*Workspace → Create → Git folder*) e aprire questo
# MAGIC notebook dalla cartella `notebooks/`. Il percorso del Volume si imposta con il widget in alto.

# COMMAND ----------

# MAGIC %pip install "wordcloud>=1.9"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import sys

# Il pacchetto si trova in ../src rispetto a questo notebook
sys.path.append(os.path.abspath("../src"))

import matplotlib.pyplot as plt
import pandas as pd

from wikipedia import analysis, classifier, export, wordclouds
from wikipedia.data import clean_dataset, download_dataset, duplication_by_category, quality_report, read_dataset, stratified_split
from wikipedia.spark import cache
from wikipedia.text import tokenize

dbutils.widgets.text("volume", "/Volumes/workspace/default/wikipedia", "Volume per dati e modello")
VOLUME = dbutils.widgets.get("volume").rstrip("/")
DATA_PATH = f"{VOLUME}/wikipedia.csv"
MODEL_PATH = f"{VOLUME}/models/wikipedia_classifier"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Caricamento
# MAGIC Il Volume va creato una sola volta, per esempio con
# MAGIC `CREATE VOLUME IF NOT EXISTS workspace.default.wikipedia` (catalogo e schema dipendono dall'area di lavoro).
# MAGIC Il CSV (circa 1 GB) viene scaricato nel Volume solo se non è già presente, e letto direttamente da Spark.

# COMMAND ----------

download_dataset(DATA_PATH)
raw = read_dataset(spark, DATA_PATH)
display(raw.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Qualità dei dati e pulizia
# MAGIC Metà delle righe sono duplicati esatti, distribuiti in modo molto diverso tra le categorie:
# MAGIC il bilanciamento apparente (circa 10.000 righe per categoria) è un effetto dei duplicati.

# COMMAND ----------

print(quality_report(raw))
display(duplication_by_category(raw))

# COMMAND ----------

articles = cache(clean_dataset(raw))
print(f"Articoli dopo la pulizia: {articles.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Analisi esplorativa e insights

# COMMAND ----------

display(analysis.category_stats(articles).reset_index())

# COMMAND ----------

display(analysis.extreme_articles(articles).reset_index())

# COMMAND ----------

tokens = cache(tokenize(articles, "documents").select("categoria", "tokens"))
counts = cache(analysis.word_counts(tokens))
distinctive = analysis.distinctive_words(counts, n=100)
display(distinctive[distinctive["rank"] <= 10])

# COMMAND ----------

fig = wordclouds.wordcloud_grid(distinctive)
plt.show()

# COMMAND ----------

display(analysis.lexical_stats(tokens).reset_index())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Classificatore
# MAGIC I modelli vengono confrontati su un validation set ricavato dal training set; il migliore per F1 macro
# MAGIC tra quelli con pesi per classe viene riaddestrato su tutto il training set e valutato una sola volta sul test set.

# COMMAND ----------

train, test = stratified_split(articles, 0.2)
train_sub, validation = stratified_split(train, 0.2, seed=43)
comparison = classifier.compare_models(train_sub, validation)
display(comparison.reset_index())

# COMMAND ----------

best = classifier.select_model(comparison)
model, labels = classifier.train_final_model(train, classifier.candidate_models()[best])
rows, metrics = classifier.evaluate(model, test, labels)
print(best, metrics)
display(classifier.per_class_report(rows).reset_index())

# COMMAND ----------

classifier.save_model(model, MODEL_PATH)
# Versione leggera per l'API REST (src/wikipedia/api.py), che non richiede Spark
export.export_model(model, f"{VOLUME}/models/esportato", model_name=best, test_metrics=metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Classificazione di nuovi articoli

# COMMAND ----------

loaded = classifier.load_model(MODEL_PATH)
new_articles = spark.createDataFrame([
    ("Esempio 1", "The striker scored twice in the second half as the club won the championship final."),
    ("Esempio 2", "The hospital opened a new oncology ward to treat patients with rare forms of cancer."),
    ("Esempio 3", "The company reported quarterly revenue of 3 billion dollars after acquiring its rival."),
], ["title", "documents"])
display(classifier.predict(loaded, new_articles))
