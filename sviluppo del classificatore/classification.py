# il classificatore si propone di analizzare le parole presenti nel campo summary e in quello documents per ciascun articolo è di associarli alla categoria correttaemnte, tramite la Logistic Regression si valuta l'accuratezza del modello proposto

# instanziamo una sessione di spark
spark = SparkSession.builder.appName("WikipediaCategoryClassifier").getOrCreate()

# si procede a unire e pulire 'summary' e 'documents' ottenendo un nuovo campo da cui si rimuove la punteggiatura
spark_df_new_sm = spark_df_new.withColumn("text", concat_ws(" ", col("summary"), col("documents")))
spark_df_new_tx = spark_df_new_sm.withColumn("text", lower(regexp_replace(col("text"), r"[^\w\s]", "")))

# filtro i record senza testo o categoria dal nuovo campo
df_filter = spark_df_new_tx.filter((col("text").isNotNull()) & (col("categoria").isNotNull()))

# definisco la pipeline per il ML con tutti i processi dalla tokenizzazione, rimozione di parole, mappatura dei termini rimasti, convertire le colonne di tipo stringa categoriale in indici numerici e classificazione
tokenizer = Tokenizer(inputCol="text", outputCol="tokens")
stopwords_remover = StopWordsRemover(inputCol="tokens", outputCol="filtered")
hashing_tf = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=10000)
idf = IDF(inputCol="rawFeatures", outputCol="features")
label_indexer = StringIndexer(inputCol="categoria", outputCol="label")
classifier = LogisticRegression(maxIter=20, regParam=0.1)

pipeline = Pipeline(stages=[
    tokenizer,
    stopwords_remover,
    hashing_tf,
    idf,
    label_indexer,
    classifier
])

# split del dataset e addestramento
train_data, test_data = spark_df_new_tx.randomSplit([0.8, 0.2], seed=0)
model = pipeline.fit(train_data)

# valutazione delle prestazioni con calcolo dell'accuratezza
predictions = model.transform(test_data)
evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
accuracy = evaluator.evaluate(predictions)

print(f"Accuratezza del modello: {accuracy:.2%}")


# valuta la classificazione con la Logistic Regression proviamo a confrontare utilizzando un altro modello di classificazione cioè il Random Forest Classifier

# operiamo ancora con unire e pulire 'summary' e 'documents' ottenendo un nuovo campo da cui si rimuove la punteggiatura
df = spark_df_new.fillna({"summary": "", "documents": ""})
df = df.withColumn("text", concat_ws(" ", col("summary"), col("documents")))
df = df.filter(col("categoria").isNotNull())

# generiamo un preprocessing tramite una pipeline
tokenizer = Tokenizer(inputCol="text", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=10000)
idf = IDF(inputCol="rawFeatures", outputCol="features")
label_indexer = StringIndexer(inputCol="categoria", outputCol="label")

# istanziamo il modello Random Forest
rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=100)

# costruzione della pipeline
pipeline = Pipeline(stages=[
    tokenizer, 
    remover, 
    hashingTF, 
    idf, 
    label_indexer, 
    rf
])

# split dei dati in training e test
train, test = df.randomSplit([0.8, 0.2], seed=0)

# procediamo con l'addestramento 
model = pipeline.fit(train)

# valutazione delle prestazioni con calcolo dell'accuratezza
predictions = model.transform(test)
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print(f"Accuracy del modello Random Forest: {accuracy:.2%}")