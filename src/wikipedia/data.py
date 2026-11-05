"""Caricamento, controllo di qualità e pulizia del dataset di articoli di Wikipedia."""

import shutil
import urllib.request
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

DATASET_URL = "https://proai-datasets.s3.eu-west-3.amazonaws.com/wikipedia.csv"
COLUMNS = ["title", "summary", "documents", "categoria"]
LABEL = "categoria"
TEXT = "documents"
SEED = 42


def download_dataset(destination, url=DATASET_URL):
    """Scarica il CSV (circa 1 GB) se non è già presente e restituisce il percorso."""
    destination = Path(destination)
    if not destination.exists():
        destination.parent.mkdir(parents=True, exist_ok=True)
        partial = destination.with_suffix(".part")
        with urllib.request.urlopen(url) as response, open(partial, "wb") as out:
            shutil.copyfileobj(response, out, length=16 * 1024 * 1024)
        partial.rename(destination)
    return destination


def read_dataset(spark, path):
    """Legge il CSV direttamente con Spark, senza passare da pandas.

    I testi contengono a capo e virgolette: servono multiLine ed escape delle virgolette doppie.
    La prima colonna del file è un indice senza nome e viene scartata.
    """
    df = spark.read.csv(str(path), header=True, multiLine=True, quote='"', escape='"')
    return df.select(*COLUMNS)


def word_count(column):
    """Numero di parole di una colonna di testo: 0 per testi nulli o vuoti."""
    trimmed = F.trim(F.coalesce(F.col(column), F.lit("")))
    return F.when(trimmed == "", 0).otherwise(F.size(F.split(trimmed, r"\s+")))


def _is_empty(column):
    return F.trim(F.coalesce(F.col(column), F.lit(""))) == ""


def _ambiguous_texts(df):
    """Testi presenti con più di una categoria."""
    return (
        df.groupBy(TEXT).agg(F.countDistinct(LABEL).alias("n_categorie"))
        .filter(F.col("n_categorie") > 1)
        .select(TEXT)
    )


def quality_report(df):
    """Conteggi che descrivono la qualità del dataset grezzo."""
    unique = df.dropDuplicates()
    ambiguous = _ambiguous_texts(unique.filter(~_is_empty(TEXT)))
    return {
        "righe": df.count(),
        "righe_uniche": unique.count(),
        "testi_vuoti": unique.filter(_is_empty(TEXT)).count(),
        "testi_con_piu_categorie": ambiguous.count(),
        "righe_con_testi_ambigui": unique.join(ambiguous, TEXT).count(),
    }


def duplication_by_category(df):
    """Righe totali e righe uniche per categoria: mostra quanto ogni categoria è gonfiata dai duplicati."""
    total = df.groupBy(LABEL).agg(F.count("*").alias("righe"))
    unique = df.dropDuplicates().groupBy(LABEL).agg(F.count("*").alias("righe_uniche"))
    return (
        total.join(unique, LABEL)
        .withColumn("quota_duplicati", 1 - F.col("righe_uniche") / F.col("righe"))
        .orderBy(F.desc("quota_duplicati"))
    )


def clean_dataset(df):
    """Dataset pulito: un articolo per testo, con una sola categoria.

    1. rimuove le righe duplicate;
    2. rimuove gli articoli con testo vuoto;
    3. rimuove i testi associati a più categorie, che non hanno un'etichetta univoca;
    4. tiene una sola riga per testo (stesso contenuto con titoli diversi).
    """
    articles = df.dropDuplicates().filter(~_is_empty(TEXT))
    articles = articles.join(_ambiguous_texts(articles), TEXT, "left_anti")
    return articles.dropDuplicates([TEXT]).select(*COLUMNS)


def stratified_split(df, test_fraction=0.2, seed=SEED):
    """Split train/test che mantiene in entrambi la proporzione di ogni categoria.

    randomSplit di Spark non è stratificato: con categorie molto piccole (es. politics)
    il test set potrebbe contenerne pochissimi esempi. Qui ogni categoria viene divisa
    separatamente secondo un ordine casuale riproducibile.
    """
    ordered = df.withColumn("_rand", F.rand(seed))
    window = Window.partitionBy(LABEL).orderBy("_rand")
    ranked = ordered.withColumn("_rank", F.percent_rank().over(window))
    test = ranked.filter(F.col("_rank") < test_fraction).drop("_rand", "_rank")
    train = ranked.filter(F.col("_rank") >= test_fraction).drop("_rand", "_rank")
    return train, test
