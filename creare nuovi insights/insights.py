# la verifica di nuovi insigths risulta utile nello sviluppo dello strumento orientando le scelte di ricerca tramite la valutazione di quanti articoli 
# sono scritti per ciascuna categoria e quali sono le parole chiave che vengono utilizzate più di altre
 
#calcolo densità articoli per categoria
spark_df_new.groupBy("categoria").agg(count("*").alias("article_count")).orderBy("article_count", ascending=False).show(truncate=False)


#tendenze linguistiche 
# pulizia e tokenizzazione degli articoli
df_clean = spark_df_new.withColumn("clean_doc", lower(regexp_replace(col("documents"), r"[^\w\s]", "")))
df_words = df_clean.withColumn("word", explode(split(col("clean_doc"), r"\s+"))).withColumn("word", trim(col("word")))

# rimuovere alcune parole non significative alla ricerca
stopwords = StopWordsRemover().getStopWords()
df_filtered = df_words.filter(~col("word").isin(stopwords)).filter(col("word") != "")

# contare le parole per categoria più utilizzate
word_counts = df_filtered.groupBy("categoria", "word").count()
word_counts.orderBy("categoria", "count", ascending=False).show(100, truncate=False)



#per esportare il dato del conteggio delle parole 
word_counts.write.csv("out_word_count.csv", header=True)
print(word_counts)