# Analisi di Wikipedia
Questo progetto si prefigge di ottimizzare l'analisi e la categorizzazione dei contenuti di Wikipedia, attraverso la data analysis e il machine learning. L'obiettivo principale è comprendere meglio il vasto patrimonio di contenuti informativi offerti da Wikipedia e sviluppare un sistema di classificazione automatica che consenta di categorizzare efficacemente i nuovi articoli futuri.

# Servizi e librerie utilizzati
- <strong>AWS S3</strong> (Servizio cloud di archiviazione object oriented)
- <strong>Databricks</strong> piattaforma di Data Intelligence basata su una lakehouse che fornisce una base aperta e unificata per tutti i dati
- <strong>Pandas DataFrame</strong> libreria in grado di rappresentare e lavorare con dati tabulari
- <strong>Apache Spark</strong> il quale è dotato di librerie di livello superiore, tra cui il supporto per query SQL, streaming di dati, apprendimento automatico ed elaborazione di grafici
- <strong>WorldCloud</strong> libreria per la creazione della nuvola di parole

# Obiettivi del progetto
- Analisi Descrittiva dei Contenuti: Il primo obiettivo del progetto è condurre un'<strong>analisi esplorativa</strong> dei dati (EDA) per capire le caratteristiche dei contenuti di Wikipedia suddivisi in diverse categorie tematiche, come ad esempio: - Cultura, Economia, Medicina, Tecnologia, Politica, Scienza, e altre. L'analisi esplorativa prevede: - Il conteggio degli articoli presenti per ogni categoria. - Il numero medio di parole per articolo. - La lunghezza dell'articolo più lungo e di quello più corto per ciascuna categoria. - La creazione di nuvole di parole rappresentative per ogni categoria, per identificare i termini più frequenti e rilevanti.
- Sviluppo di un Classificatore Automatico: Il secondo obiettivo è creare un modello di machine learning capace di <strong>classificare automaticamente</strong> gli articoli in base alla loro categoria. Il sistema di classificazione verrà addestrato utilizzando dati di testo presenti nelle seguenti colonne del dataset: - Sommario (summary): Introduzione breve dell'articolo. - Testo Completo (documents): Contenuto completo dell'articolo.
- Identificazione di Nuovi Insights: L'analisi consentirà anche di ottenere preziosi insights sui contenuti di Wikipedia, come la <strong>densità di articoli</strong> per categoria o le <strong>tendenze linguistiche</strong> associate a determinati argomenti. Queste informazioni possono aiutare Wikimedia a migliorare l'organizzazione delle pagine e a ottimizzare i propri sforzi editoriali.

# Repository
BIGDATA_PROJECT/
- README.md
- src/
    - import e dataframe
      - intro.py
    - analisi esplorativa dati
      - eda.py
    - creazione della nuvola di parole
      - jpeg
      - world_cloud.py
    - sviluppo del classificatore
      - classification.py
    - creare nuovi insights
      - insights.py
