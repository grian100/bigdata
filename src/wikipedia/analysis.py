"""Analisi esplorativa e insights sui contenuti, calcolati in Spark.

Le funzioni restituiscono DataFrame pandas: i risultati sono aggregati e piccoli,
mentre i calcoli sui testi restano distribuiti in Spark.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from .data import LABEL, TEXT, word_count


def category_stats(df):
    """Per ogni categoria: numero di articoli, quota sul totale e lunghezza dei testi in parole."""
    total = df.count()
    words = df.withColumn("parole", word_count(TEXT)).withColumn("parole_summary", word_count("summary"))
    stats = (
        words.groupBy(LABEL)
        .agg(
            F.count("*").alias("articoli"),
            F.avg("parole").alias("parole_media"),
            F.percentile_approx("parole", 0.5).alias("parole_mediana"),
            F.min("parole").alias("parole_min"),
            F.max("parole").alias("parole_max"),
            F.avg("parole_summary").alias("parole_summary_media"),
        )
        .withColumn("quota", F.col("articoli") / F.lit(total))
        .orderBy(F.desc("articoli"))
        .toPandas()
        .set_index(LABEL)
    )
    return stats[["articoli", "quota", "parole_media", "parole_mediana", "parole_min", "parole_max",
                  "parole_summary_media"]]


def overall_stats(df):
    """Statistiche sulla lunghezza dei testi per l'intero dataset."""
    words = df.withColumn("parole", word_count(TEXT))
    row = words.agg(
        F.count("*").alias("articoli"),
        F.avg("parole").alias("parole_media"),
        F.percentile_approx("parole", 0.5).alias("parole_mediana"),
        F.min("parole").alias("parole_min"),
        F.max("parole").alias("parole_max"),
    ).first()
    return row.asDict()


def extreme_articles(df):
    """Titolo e lunghezza dell'articolo più lungo e di quello più corto di ogni categoria."""
    words = df.withColumn("parole", word_count(TEXT))
    longest = Window.partitionBy(LABEL).orderBy(F.desc("parole"), "title")
    shortest = Window.partitionBy(LABEL).orderBy("parole", "title")

    def pick(window):
        return (
            words.withColumn("_rank", F.row_number().over(window))
            .filter(F.col("_rank") == 1)
            .select(LABEL, "title", "parole")
            .toPandas()
            .set_index(LABEL)
        )

    # Calcolati separatamente: lo stesso articolo può essere sia il più lungo sia il più corto
    longest_df, shortest_df = pick(longest), pick(shortest)
    return longest_df.join(shortest_df, lsuffix="_piu_lungo", rsuffix="_piu_corto").sort_index()


def word_counts(tokens_df, tokens_col="tokens"):
    """Conteggio delle parole per categoria (DataFrame Spark con categoria, word, count)."""
    return (
        tokens_df.select(LABEL, F.explode(tokens_col).alias("word"))
        .groupBy(LABEL, "word")
        .count()
    )


def _top_per_category(df, score_col, n):
    window = Window.partitionBy(LABEL).orderBy(F.desc(score_col), "word")
    return (
        df.withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") <= n)
        .orderBy(LABEL, "rank")
    )


def top_words(counts, n=20):
    """Le n parole più frequenti di ogni categoria."""
    return _top_per_category(counts, "count", n).toPandas()


def distinctive_words(counts, n=20, min_count=20):
    """Le n parole più caratteristiche di ogni categoria.

    Le parole più frequenti sono spesso simili in tutte le categorie. Qui il punteggio è
        conteggio nella categoria x log(frequenza relativa nella categoria / frequenza relativa complessiva)
    ed è alto per le parole frequenti nella categoria e relativamente rare nelle altre;
    vale 0 per una parola distribuita in modo uniforme. Le parole con meno di min_count
    occorrenze nella categoria vengono ignorate.
    """
    category_totals = counts.groupBy(LABEL).agg(F.sum("count").alias("totale_categoria"))
    word_totals = counts.groupBy("word").agg(F.sum("count").alias("totale_parola"))
    grand_total = counts.agg(F.sum("count")).first()[0]
    scored = (
        counts.filter(F.col("count") >= min_count)
        .join(category_totals, LABEL)
        .join(word_totals, "word")
        .withColumn(
            "rapporto",
            (F.col("count") / F.col("totale_categoria")) / (F.col("totale_parola") / F.lit(grand_total)),
        )
        .withColumn("score", F.col("count") * F.log(F.col("rapporto")))
        .filter(F.col("score") > 0)
        .select(LABEL, "word", "count", "rapporto", "score")
    )
    return _top_per_category(scored, "score", n).toPandas()


def lexical_stats(tokens_df, tokens_col="tokens"):
    """Indicatori linguistici per categoria, calcolati articolo per articolo e poi mediati.

    - ricchezza lessicale: parole distinte / parole totali dell'articolo (dopo le stopword);
      viene calcolata sui primi 300 termini di ogni articolo, perché il rapporto diminuisce
      con la lunghezza del testo e altrimenti confronterebbe soprattutto le lunghezze;
    - lunghezza media delle parole, in lettere.
    """
    head = F.slice(F.col(tokens_col), 1, 300)
    per_article = tokens_df.select(
        LABEL,
        F.size(head).alias("n"),
        F.size(F.array_distinct(head)).alias("distinte"),
        F.aggregate(F.transform(F.col(tokens_col), F.length), F.lit(0), lambda acc, x: acc + x).alias("lettere"),
        F.size(tokens_col).alias("n_tot"),
    ).filter(F.col("n") >= 50)
    return (
        per_article.groupBy(LABEL)
        .agg(
            F.avg(F.col("distinte") / F.col("n")).alias("ricchezza_lessicale"),
            (F.sum("lettere") / F.sum("n_tot")).alias("lunghezza_media_parole"),
            F.count("*").alias("articoli_considerati"),
        )
        .orderBy(F.desc("ricchezza_lessicale"))
        .toPandas()
        .set_index(LABEL)
    )
