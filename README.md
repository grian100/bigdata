# Analisi di Wikipedia
Questo progetto si prefigge di ottimizzare l'analisi e la categorizzazione dei contenuti di Wikipedia, attraverso la data analysis e il machine learning. L'obiettivo principale è comprendere meglio il vasto patrimonio di contenuti informativi offerti da Wikipedia e sviluppare un sistema di classificazione automatica che consenta di categorizzare efficacemente i nuovi articoli futuri.

# Servizi e librerie utilizzati
- <strong>AWS S3</strong> (Servizio cloud di archiviazione object oriented)
- <strong>Databricks</strong> piattaforma di Data Intelligence basata su un data lakehouse ovvero un'architettura dati moderna che unifica le capacità di un data lake e di un data warehouse in un'unica piattaforma, offrendo flessibilità e scalabilità per dati strutturati e non, combinata con le funzionalità di gestione [https://www.databricks.com/]
- <strong>Pandas DataFrame</strong> libreria in grado di rappresentare e lavorare con dati tabulari
- <strong>Apache Spark</strong> il quale è dotato di librerie di livello superiore, tra cui il supporto per query SQL, streaming di dati, apprendimento automatico ed elaborazione di grafici
- <strong>WorldCloud</strong> libreria per la creazione della nuvola di parole

# Obiettivi del progetto
1. Analisi Descrittiva dei Contenuti
    - Il primo obiettivo del progetto è condurre un'<strong>analisi esplorativa</strong> dei dati (EDA) per capire le caratteristiche dei contenuti di Wikipedia suddivisi in diverse categorie tematiche, come ad esempio:
      - Cultura, Economia, Medicina, Tecnologia, Politica, Scienza, e altre.
    - L'analisi esplorativa prevede:
        - Il conteggio degli articoli presenti per ogni categoria
        - Il numero medio di parole per articolo
        - La lunghezza dell'articolo più lungo e di quello più corto per ciascuna categoria
        - La creazione di nuvole di parole rappresentative per ogni categoria, per identificare i termini più frequenti e rilevanti
      
2. Sviluppo di un Classificatore Automatico
   - Il secondo obiettivo è creare un modello di machine learning capace di <strong>classificare automaticamente</strong> gli articoli in base alla loro categoria. Il sistema di classificazione verrà addestrato utilizzando dati di testo presenti nelle seguenti colonne del dataset:
       - Sommario (summary): Introduzione breve dell'articolo
       - Testo Completo (documents): Contenuto completo dell'articolo
     
3. Identificazione di Nuovi Insights
      - L'analisi consentirà anche di ottenere preziosi insights sui contenuti di Wikipedia, come la <strong>densità di articoli</strong> per categoria o le <strong>tendenze linguistiche</strong> associate a determinati argomenti. Queste informazioni possono aiutare Wikimedia a migliorare l'organizzazione delle pagine e a ottimizzare i propri sforzi editoriali

# Repository
BIGDATA_PROJECT/
- README.md
- import e dataframe
  - intro.py
- analisi esplorativa dati
  - eda.py
- creazione della nuvola di parole
  - jpeg/
    - *.png # output parole più richieste per categoria     
  - world_cloud.py
- sviluppo del classificatore
  - classification.py
- creare nuovi insights
  - insights.py

# Workflow
Le librerie principali sono <strong>pandas, sklearn, pyspark</strong>, per il caricamento e l'analisi esplorativa, con la misura della media del numero di parole e il conteggio dell'articolo più corto e più lungo.

Per la successiva creaione della nuvola di parole usiamo la libreria <strong>WordCloud</strong> raggruppando le parole per categoria, fitrando in un set gli articoli o preposizioni da non tenere conto.

Per lo sviluppo del classificatore si realizza una pipeline che gestisce la tokenizzazione (separa le singole parole), rimozione di parole, mappatura dei termini rimasti, convertire le colonne di tipo stringa categoriale in indici numerici e classificazione:
 - <strong><mark>pipeline = Pipeline(stages=[tokenizer, stopwords_remover, hashing_tf, idf, label_indexer, classifier ])</mark></strong>
utilizza nella classificazione la <strong>Regressione Logistica</strong> in modo da ottenere l'accuratezza sul set di training che corrisponde all'80% del dataset e confrontarlo con il restante 20% del set di test.

Si procede quindi successivamente a confrontare l'accuratezza ottenuta con questo modello con uno nuovo modello ottenuto impiegando queta volta il <strong>Random Forest Classifier</strong> addestrando ancora il set di training e quello di test.

Per la misura dei nuovi insights si utilizzano delle <strong>regex espression</strong> per la pulizia e tokenizzazione degli articoli, si filtrano le parole non significative con il <strong>filter</strong>, fino al conteggio delle singole parole per categoria.
