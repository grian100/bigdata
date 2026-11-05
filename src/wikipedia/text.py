"""Tokenizzazione comune a analisi esplorativa, nuvole di parole e classificatore."""

from pyspark.ml.feature import RegexTokenizer, StopWordsRemover

# Parole molto comuni negli articoli enciclopedici, escluse dalle analisi di frequenza
# in aggiunta alla lista standard di Spark (non sono usate dal classificatore)
EXTRA_STOPWORDS = [
    "also", "one", "two", "three", "may", "many", "first", "new", "used", "use", "using",
    "known", "including", "however", "well", "since", "became", "later", "often", "made",
    "several", "part", "time", "years", "year", "would", "could", "like", "include",
    "includes", "called", "number", "among", "within", "us", "can", "based",
]


def tokenizer_stages(input_col, output_col="tokens", extra_stopwords=()):
    """Stage di Spark ML che trasformano un testo in una lista di parole significative.

    - tiene solo le sequenze di lettere (niente numeri e punteggiatura) in minuscolo;
    - scarta le parole di una sola lettera;
    - rimuove le stopword inglesi di Spark, che includono i frammenti delle contrazioni
      (don't -> "don", "t"), più quelle indicate in extra_stopwords.
    """
    raw = f"{output_col}_raw"
    tokenizer = RegexTokenizer(
        inputCol=input_col, outputCol=raw, pattern="[a-z]+", gaps=False, toLowercase=True, minTokenLength=2
    )
    stopwords = StopWordsRemover.loadDefaultStopWords("english") + list(extra_stopwords)
    remover = StopWordsRemover(inputCol=raw, outputCol=output_col, stopWords=stopwords)
    return [tokenizer, remover]


def tokenize(df, input_col, output_col="tokens", extra_stopwords=EXTRA_STOPWORDS):
    """Aggiunge a df la colonna con le parole significative del testo."""
    for stage in tokenizer_stages(input_col, output_col, extra_stopwords):
        df = stage.transform(df)
    return df.drop(f"{output_col}_raw")
