#per visualizzare la nuvola di parole utilizziamo la libreria WorldCloud - se non installata è da importare nel cluster tramite il package installer di python pip
#!pip install wordcloud


#per la creazione della nuvola di parole agiamo sul campo documents tonenizzando il testo in singole parole, rimuovendo la punteggiatura e filtrando una serie di parole che non sono interessanti nella creazione della nuvola (es: to, as, this, ecc..)  
from wordcloud import WordCloud

# instanziamo spark
spark = SparkSession.builder.appName("WordCloudPerCategory").getOrCreate()
spark_df_new_clean = spark_df_new.withColumn("documents_clean", regexp_replace(lower(col("documents")), r"[^\w\s]", ""))  # rimuovi punteggiatura
spark_df_new_words = spark_df_new_clean.withColumn("word", explode(split(col("documents_clean"), r"\s+"))).withColumn("word", trim(col("word"))) # Tokenizza il testo in parole

# rimuovi parole
stopwords = set([
    "the", "and", "is", "in", "of", "a", "to", "as", "for", "with", "on", "by","he","she","who","also","had", 
    "an", "be", "that", "are", "was", "this", "it", "from", "at", "or", "which", "his", "her", "where","were"
])
df_filtered = spark_df_new_words.filter(~col("word").isin(stopwords)).filter(col("word") != "")

# conta le parole per categoria
word_counts = df_filtered.groupBy("categoria", "word").count()

# colleziona i risultati in pandas per WordCloud
pandas_counts = word_counts.toPandas()

# genera e visualizza tramite WordCloud una nuvola di parole per ciascuna categoria
categories = pandas_counts['categoria'].unique()

for category in categories:
    sub_df = pandas_counts[pandas_counts['categoria'] == category]
    word_freq = dict(zip(sub_df['word'], sub_df['count']))

    wordcloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(word_freq)

    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    plt.title(f"Per la categoria: {category}", fontsize=16)
    plt.show()
