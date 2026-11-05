"""Classificatore automatico della categoria degli articoli con Spark ML.

Il testo usato è `documents`, che contiene già il `summary` come primo paragrafo: unirli
duplicherebbe il sommario. La pipeline completa è:

    RegexTokenizer -> StopWordsRemover -> CountVectorizer -> IDF -> classificatore -> IndexToString

e riceve direttamente il testo grezzo, quindi il modello salvato si può usare su nuovi articoli.
"""

import time

import pandas as pd
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression, NaiveBayes, RandomForestClassifier
from pyspark.ml.feature import IDF, CountVectorizer, IndexToString, StringIndexer, UnivariateFeatureSelector
from pyspark.sql import functions as F
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix, f1_score, recall_score

from .data import LABEL, SEED, TEXT
from .spark import cache
from .text import tokenizer_stages

VOCAB_SIZE = 50_000
MIN_DF = 5
PREDICTION = "categoria_prevista"


def featurizer(input_col=TEXT, vocab_size=VOCAB_SIZE, min_df=MIN_DF):
    """Pipeline che trasforma il testo in vettori TF-IDF ("features") e la categoria in indice ("label").

    CountVectorizer costruisce un vocabolario delle vocab_size parole più frequenti (presenti in almeno
    min_df articoli): a differenza di HashingTF non ci sono collisioni tra parole diverse e i
    termini restano interpretabili.
    """
    return Pipeline(stages=[
        *tokenizer_stages(input_col, "tokens"),
        CountVectorizer(inputCol="tokens", outputCol="tf", vocabSize=vocab_size, minDF=min_df),
        IDF(inputCol="tf", outputCol="features"),
        StringIndexer(inputCol=LABEL, outputCol="label"),
    ])


def logistic_regression(reg_param, standardization=False, class_weights=True):
    """Regressione logistica multinomiale.

    Spark per default standardizza le feature (standardization=True): con il TF-IDF questo
    porta le parole rare sulla stessa scala di quelle frequenti e ne gonfia l'influenza,
    peggiorando le previsioni. Qui la standardizzazione è disattivata per default.
    Con class_weights=True (default) ogni articolo pesa in modo inversamente proporzionale
    alla dimensione della sua categoria (colonna "weight", vedi with_class_weights).
    """
    model = LogisticRegression(maxIter=100, regParam=reg_param, standardization=standardization)
    return model.setWeightCol("weight") if class_weights else model


# Modelli senza pesi per classe o con la standardizzazione di Spark: vengono confrontati
# per misurare l'effetto delle due scelte, ma non possono essere scelti come modello finale
REFERENCE_MODELS = {
    "Regressione logistica senza pesi per classe (regParam=0.01)",
    "Regressione logistica con standardizzazione (regParam=0.01)",
}


def candidate_models(seed=SEED):
    """Classificatori da confrontare. Tutti, tranne quelli di riferimento, usano i pesi per classe."""
    return {
        "Regressione logistica (regParam=0.001)": logistic_regression(0.001),
        "Regressione logistica (regParam=0.01)": logistic_regression(0.01),
        "Regressione logistica (regParam=0.1)": logistic_regression(0.1),
        "Regressione logistica senza pesi per classe (regParam=0.01)": logistic_regression(0.01, class_weights=False),
        # Impostazione predefinita di Spark
        "Regressione logistica con standardizzazione (regParam=0.01)": logistic_regression(0.01, standardization=True),
        # Modello classico per il testo: veloce e adatto a feature sparse non negative
        "Naive Bayes": NaiveBayes(modelType="multinomial", smoothing=1.0, weightCol="weight"),
        # Le foreste gestiscono male decine di migliaia di feature sparse (con 50.000 parole
        # l'addestramento esaurisce la memoria): si usano le 2.000 parole più informative
        "Random forest (2.000 parole)": random_forest(seed=seed),
    }


def select_model(comparison):
    """Il modello con l'F1 macro più alto, esclusi quelli di riferimento."""
    return comparison.drop(index=REFERENCE_MODELS, errors="ignore")["f1_macro"].idxmax()


def random_forest(n_features=2000, seed=SEED):
    """Random forest preceduta dalla selezione delle n_features parole più legate alla categoria (test F)."""
    return Pipeline(stages=[
        UnivariateFeatureSelector(featuresCol="features", outputCol="selected", labelCol="label",
                                  selectionMode="numTopFeatures")
        .setFeatureType("continuous").setLabelType("categorical").setSelectionThreshold(n_features),
        RandomForestClassifier(featuresCol="selected", numTrees=100, maxDepth=10, seed=seed, weightCol="weight"),
    ])


def with_class_weights(df, label_col="label"):
    """Aggiunge la colonna "weight": n_articoli / (n_categorie x articoli della categoria).

    Con categorie molto sbilanciate (politics ha 30 volte meno articoli di medicine) i pesi
    evitano che il modello trascuri le categorie piccole. I modelli che non usano weightCol
    ignorano la colonna.
    """
    counts = {row[label_col]: row["count"] for row in df.groupBy(label_col).count().collect()}
    total, classes = sum(counts.values()), len(counts)
    mapping = F.create_map(*[F.lit(x) for label, n in counts.items() for x in (label, total / (classes * n))])
    return df.withColumn("weight", mapping[F.col(label_col)])


def predictions_to_pandas(predictions, labels):
    """Categoria reale e prevista, come stringhe, in un DataFrame pandas."""
    rows = predictions.select(LABEL, "prediction").toPandas()
    rows["prevista"] = [labels[int(i)] for i in rows["prediction"]]
    return rows[[LABEL, "prevista"]]


def classification_metrics(rows):
    """Accuratezza, F1 macro (media non pesata tra le classi, sensibile alle classi piccole),
    F1 pesato e recall minimo, cioè il recall della categoria riconosciuta peggio."""
    classes = sorted(rows[LABEL].unique())
    return {
        "accuratezza": accuracy_score(rows[LABEL], rows["prevista"]),
        "f1_macro": f1_score(rows[LABEL], rows["prevista"], average="macro"),
        "f1_pesato": f1_score(rows[LABEL], rows["prevista"], average="weighted"),
        "recall_minimo": recall_score(rows[LABEL], rows["prevista"], labels=classes, average=None).min(),
    }


def per_class_report(rows):
    report = classification_report(rows[LABEL], rows["prevista"], output_dict=True, zero_division=0)
    classes = sorted(rows[LABEL].unique())
    return pd.DataFrame({c: report[c] for c in classes}).T.rename(columns={"support": "articoli"})


def confusion(rows):
    classes = sorted(rows[LABEL].unique())
    matrix = confusion_matrix(rows[LABEL], rows["prevista"], labels=classes)
    return pd.DataFrame(matrix, index=classes, columns=classes)


def compare_models(train, validation, models=None, featurizer_pipeline=None, verbose=False):
    """Addestra i modelli su train e li valuta su validation.

    Le feature vengono calcolate una sola volta (fit su train) e tenute in cache:
    ogni modello parte dagli stessi vettori, quindi il confronto è alla pari.
    """
    models = candidate_models() if models is None else models
    feats = (featurizer_pipeline or featurizer()).fit(train)
    labels = feats.stages[-1].labels
    train_f = cache(with_class_weights(feats.transform(train).select("features", "label")))
    val_f = cache(feats.transform(validation).select(LABEL, "features", "label"))
    results = []
    for name, model in models.items():
        start = time.time()
        fitted = model.fit(train_f)
        rows = predictions_to_pandas(fitted.transform(val_f), labels)
        results.append({"modello": name, **classification_metrics(rows), "secondi": time.time() - start})
        if verbose:
            r = results[-1]
            print(f"  {name}: accuratezza {r['accuratezza']:.3f}, F1 macro {r['f1_macro']:.3f}, "
                  f"recall minimo {r['recall_minimo']:.3f} ({r['secondi']:.0f} s)", flush=True)
    return pd.DataFrame(results).set_index("modello").sort_values("f1_macro", ascending=False)


def train_final_model(train, classifier, featurizer_pipeline=None):
    """Addestra featurizer e classificatore e li unisce in un'unica PipelineModel.

    L'ultimo stage converte l'indice previsto nel nome della categoria (colonna categoria_prevista).
    """
    feats = (featurizer_pipeline or featurizer()).fit(train)
    train_f = cache(with_class_weights(feats.transform(train).select("features", "label")))
    fitted = classifier.fit(train_f)
    labels = feats.stages[-1].labels
    to_name = IndexToString(inputCol="prediction", outputCol=PREDICTION, labels=labels)
    # Lo StringIndexer serve solo in addestramento: il modello finale non richiede la categoria
    text_stages = feats.stages[:-1]
    return PipelineModel(stages=[*text_stages, fitted, to_name]), labels


def evaluate(model, test, labels):
    """Previsioni sul test set e metriche."""
    predictions = model.transform(test)
    rows = predictions.select(LABEL, PREDICTION).toPandas().rename(columns={PREDICTION: "prevista"})
    # Categorie mai viste in addestramento non possono essere previste: vengono escluse dal calcolo
    rows = rows[rows[LABEL].isin(set(labels))]
    return rows, classification_metrics(rows)


def predict(model, articles, top_k=3):
    """Categoria prevista per nuovi articoli, con le top_k categorie più probabili.

    articles è un DataFrame Spark con almeno la colonna documents (e opzionalmente title).
    """
    labels = model.stages[-1].getLabels()
    predictions = model.transform(articles)
    columns = [c for c in ["title"] if c in articles.columns]
    rows = predictions.select(*columns, PREDICTION, "probability").toPandas()
    probabilities = rows.pop("probability").apply(lambda v: v.toArray())
    rows["probabilita"] = probabilities.apply(lambda p: float(p.max()))
    rows["alternative"] = probabilities.apply(
        lambda p: ", ".join(f"{labels[i]} ({p[i]:.0%})" for i in p.argsort()[::-1][:top_k])
    )
    return rows


def save_model(model, path):
    model.write().overwrite().save(str(path))


def load_model(path):
    return PipelineModel.load(str(path))


def top_terms(model, n=10):
    """Per la regressione logistica: le parole con coefficiente più alto per ogni categoria."""
    stages = model.stages
    vocabulary = next(s for s in stages if hasattr(s, "vocabulary")).vocabulary
    classifier = next((s for s in stages if hasattr(s, "coefficientMatrix")), None)
    if classifier is None:
        raise ValueError("top_terms richiede un modello di regressione logistica")
    labels = stages[-1].getLabels()
    matrix = classifier.coefficientMatrix.toArray()
    return pd.DataFrame(
        {labels[i]: [vocabulary[j] for j in matrix[i].argsort()[::-1][:n]] for i in range(len(labels))}
    ).T
