# esaminiamo la struttura del dataset e lo schema
spark_df.printSchema()
spark_df.columns


# verifica della presenza di possibili duplicati nel dataset
spark_df_duplicates= spark_df.groupBy(*spark_df.columns).agg(count('*').alias('count')).filter(col('count')>1)
spark_df_duplicates.show()

# confermate le duplicazioni creo la nuova tabella senza la loro presenza
spark_df_new=spark_df_duplicates.drop('count')
spark_df_new.show()


# distribuzione delle categorie e numero di articoli associati
spark_df_new.groupBy("categoria").agg(count("*").alias("article_count")) \
  .orderBy("article_count", ascending=False) \
  .show(15, truncate=False)
  

# verifica della lunghezza dei testi in 'summary' e 'documents'
spark_dfl = spark_df_new.withColumn("summary_word_count", size(split("summary", r"\s+"))) \
       .withColumn("document_word_count", size(split("documents", r"\s+")))

spark_dfl.select("title", "summary_word_count", "document_word_count").show(15)


# si crea una statistica descrittiva della numerosità degli elementi del dataset
spark_dfl.select("summary_word_count", "document_word_count").describe().show()


# parole che con più frequenza sono presenti nei testi
spark_df_words = spark_df_new.withColumn("clean_doc", regexp_replace(lower(col("documents")), r"[^\w\s]", "")) \
             .withColumn("word", explode(split(col("clean_doc"), r"\s+"))) \
             .withColumn("word", trim(col("word"))) \
             .filter(col("word") != "")

top_words = spark_df_words.groupBy("word").count().orderBy("count", ascending=False)
top_words.show(20)


# verifichiamo i valori nulli e la qualità dei dati
spark_df_new.select([spark_sum(col(c).isNull().cast("int")).alias(c + "_nulls") for c in spark_df_new.columns]).show()


# pur essendo il dataset composto da valori categorici agiamo sullla numerosità degli articoli per creeare una visualizzazione
pdf = spark_df_new.select("categoria").groupBy("categoria").count().toPandas()
pdf.sort_values("count", ascending=False).head(10).plot(kind='bar', x='categoria', y='count')
plt.title("Distribuzione categorie per numero di articoli")
plt.ylabel("Numero di articoli")
plt.show() 

# misuriamo quindi la media di parole per articolo senza tenere conto della categoria
avg_word_count = spark_dfl.select(avg("document_word_count").alias("average_word_count"))
avg_word_count.show()

#dal campo document_word_count estraiamo la lunghezza dell'articolo più lungo
max_article_length=spark_dfl.orderBy(spark_dfl.document_word_count.desc())
max_article_length.show(1)

#dal campo document_word_count estraiamo la lunghezza dell'articolo più corto
min_article_length=spark_dfl.orderBy(spark_dfl.document_word_count.asc()).where(spark_dfl.document_word_count>-1)
min_article_length.show(1)