# Analisi di Wikipedia
Questo progetto si prefigge di ottimizzare l'analisi e la categorizzazione dei contenuti di Wikipedia, attraverso la data analysis e il machine learning. L'obiettivo principale è comprendere meglio il vasto patrimonio di contenuti informativi offerti da Wikipedia e sviluppare un sistema di classificazione automatica che consenta di categorizzare efficacemente i nuovi articoli futuri.

# Servizi e librerie utilizzati
- <strong>AWS S3</strong> (servizio cloud di archiviazione a oggetti), da cui viene scaricato il dataset
- <strong>Databricks</strong>, piattaforma di Data Intelligence basata su un data lakehouse, ovvero un'architettura dati moderna che unifica le capacità di un data lake e di un data warehouse in un'unica piattaforma, offrendo flessibilità e scalabilità per dati strutturati e non [https://www.databricks.com/]
- <strong>Apache Spark 3.5</strong> (PySpark), con le librerie per query SQL ed elaborazione distribuita e <strong>Spark ML</strong> per il machine learning
- <strong>Pandas</strong> per i risultati aggregati e <strong>Matplotlib</strong> per i grafici
- <strong>WordCloud</strong>, libreria per la creazione delle nuvole di parole
- <strong>Scikit-learn</strong> per il calcolo delle metriche di classificazione, <strong>pytest</strong> per i test
- <strong>FastAPI</strong> e <strong>Uvicorn</strong> per l'API REST, che usa il modello esportato con <strong>NumPy</strong> e <strong>SciPy</strong>

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
   - Il secondo obiettivo è creare un modello di machine learning capace di <strong>classificare automaticamente</strong> gli articoli in base alla loro categoria, utilizzando il testo degli articoli:
       - Sommario (summary): introduzione breve dell'articolo
       - Testo Completo (documents): contenuto completo dell'articolo, che contiene già il sommario come primo paragrafo

3. Identificazione di Nuovi Insights
      - L'analisi consente anche di ottenere insights sui contenuti di Wikipedia, come la <strong>densità di articoli</strong> per categoria o le <strong>tendenze linguistiche</strong> associate a determinati argomenti. Queste informazioni possono aiutare Wikimedia a migliorare l'organizzazione delle pagine e a ottimizzare i propri sforzi editoriali.

# Repository
```
bigdata/
├── src/wikipedia/
│   ├── spark.py        # sessione Spark (locale o Databricks)
│   ├── data.py         # download, lettura con Spark, controllo di qualità, pulizia, split stratificato
│   ├── text.py         # tokenizzazione e stopword comuni a tutte le analisi
│   ├── analysis.py     # statistiche per categoria, parole frequenti e caratteristiche, indicatori linguistici
│   ├── wordclouds.py   # nuvole di parole
│   ├── classifier.py   # pipeline Spark ML, confronto dei modelli, valutazione, previsione
│   ├── export.py       # esportazione del modello e previsioni con NumPy, senza Spark
│   └── api.py          # API REST (FastAPI) per le previsioni
├── notebooks/
│   ├── databricks_wikipedia.py   # notebook Databricks con l'intero progetto
│   └── risultati.ipynb           # notebook con tutti i risultati, già eseguito
├── images/                       # grafici e nuvole di parole (images/wordclouds/*.png)
├── models/esportato/             # modello finale esportato, usato dall'API e da predict.py
├── tests/
│   ├── conftest.py               # dataset sintetico e sessione Spark per i test
│   ├── test_wikipedia.py
│   └── test_api.py
├── eda.py              # analisi esplorativa, insights e nuvole di parole
├── train.py            # addestramento e valutazione del classificatore
├── predict.py          # classificazione di nuovi articoli
├── requirements.txt
└── pytest.ini
```

# Utilizzo

### Su Databricks
1. Clonare il repository in Databricks: *Workspace → Create → Git folder*, con l'URL del repository.
2. Creare un Volume di Unity Catalog per dati e modello, per esempio con `CREATE VOLUME IF NOT EXISTS workspace.default.wikipedia`.
3. Aprire `notebooks/databricks_wikipedia.py`, impostare il percorso del Volume nel widget in alto ed eseguire tutte le celle.

Il notebook scarica il dataset nel Volume, esegue analisi e addestramento e salva il modello nel Volume.

### In locale
Serve Java 17 (richiesto da Spark) e Python 3.10 o successivo. Dalla cartella del progetto:
```bash
python -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt    # su Linux/macOS: .venv/bin/python
.venv\Scripts\python.exe eda.py                                 # circa 3 minuti
.venv\Scripts\python.exe train.py --confronto-duplicati         # addestramento e valutazione
.venv\Scripts\python.exe predict.py --testo "The striker scored twice in the cup final."
.venv\Scripts\python.exe -m uvicorn wikipedia.api:app --app-dir src   # API REST su http://localhost:8000
.venv\Scripts\python.exe -m pytest                              # test
```
Al primo avvio il dataset (circa 1 GB) viene scaricato in `data/`. Tabelle e risultati vengono salvati in `reports/`, grafici e nuvole di parole in `images/`, il modello in `models/`.

Il modello finale esportato è già incluso nel repository (`models/esportato`, circa 6 MB): `predict.py` e l'API REST funzionano subito dopo il clone, senza scaricare il dataset né addestrare il modello, e non richiedono Java.

<strong>Nota per Windows</strong>: Spark scrive file su disco solo se è configurato `HADOOP_HOME` con `winutils.exe`. Senza, analisi e addestramento funzionano (i risultati vengono salvati con pandas) e il modello esportato per l'API viene salvato comunque, ma la pipeline Spark completa (`models/wikipedia_classifier`, usata da `predict.py --spark`) no. Per salvarla conviene usare Databricks, Linux, macOS o WSL.

# API REST
Avvio, dalla cartella del progetto e con le dipendenze installate:
```bash
python -m uvicorn wikipedia.api:app --app-dir src --host 0.0.0.0 --port 8000
```
Si usa `python -m uvicorn` invece di `uvicorn` perché funziona anche quando la cartella degli eseguibili di Python non è nel PATH. La documentazione interattiva, da cui si possono provare le richieste, è su http://localhost:8000/docs.

L'API non avvia Spark: carica all'avvio il modello esportato da `train.py` (vocabolario, pesi IDF e coefficienti della regressione logistica) e calcola le previsioni con NumPy. Le previsioni sono identiche a quelle della pipeline Spark: i test lo verificano per regressione logistica e Naive Bayes, e in una verifica sul test set completo (14.440 articoli) le categorie previste coincidevano tutte e le probabilità differivano al massimo di 4·10⁻¹⁶. Classificare l'intero test set richiede circa 10 secondi, meno di un millisecondo per articolo.

| Metodo | Percorso | Descrizione |
|---|---|---|
| GET | `/health` | verifica che il servizio sia attivo |
| GET | `/model` | categorie, dimensione del vocabolario, metriche sul test set, data di addestramento e versioni |
| POST | `/predict` | categoria di un articolo |
| POST | `/predict/batch` | categoria di più articoli (al massimo 100 per richiesta) |

I due endpoint di previsione accettano il parametro `top_k` (default 3), il numero di categorie alternative da restituire. Esempio con PowerShell:
```powershell
$body = @{ titolo = "Esempio"; testo = "The new clinic treats patients with chronic heart disease. Doctors combine medication, diet and exercise programs, and nurses follow each patient after discharge from the hospital. A clinical trial is testing whether a new drug reduces the risk of a second heart attack, and early results suggest fewer complications than the standard therapy." } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri http://localhost:8000/predict -ContentType "application/json; charset=utf-8" -Body $body
```
o con curl:
```bash
curl -X POST http://localhost:8000/predict -H "Content-Type: application/json" -d '{"titolo": "Esempio", "testo": "The new clinic treats patients with chronic heart disease. Doctors combine medication, diet and exercise programs, and nurses follow each patient after discharge from the hospital. A clinical trial is testing whether a new drug reduces the risk of a second heart attack, and early results suggest fewer complications than the standard therapy."}'
```
Risposta:
```json
{
  "titolo": "Esempio",
  "categoria": "research",
  "probabilita": 0.319,
  "alternative": [
    {"categoria": "research", "probabilita": 0.319},
    {"categoria": "medicine", "probabilita": 0.225},
    {"categoria": "engineering", "probabilita": 0.082}
  ],
  "parole_riconosciute": 36,
  "avviso": "Previsione poco affidabile: probabilità bassa (32%)."
}
```
- `parole_riconosciute` è il numero di parole del testo presenti nel vocabolario del modello. Se nessuna parola è nota (per esempio un testo non in inglese) `categoria` e `probabilita` sono `null`.
- La risposta contiene un `avviso` quando le parole riconosciute sono meno di 20 o la probabilità è sotto 0,4. Le soglie vengono dal test set:

  | Testo usato | Accuratezza | Probabilità media |
  |---|---|---|
  | prime 20 parole dell'articolo | 0.668 | 0.24 |
  | prime 50 parole | 0.816 | 0.39 |
  | prime 100 parole | 0.864 | 0.57 |
  | prime 200 parole | 0.884 | 0.73 |
  | articolo completo | 0.902 | 0.87 |

  Il modello è addestrato su articoli completi (mediana di 219 parole riconosciute), quindi su testi brevi le probabilità sono basse e gli errori più frequenti. Nell'esempio il testo parla di medicina, ma il modello esita tra `research` e `medicine`, la confusione più frequente anche sul test set, e l'avviso lo segnala. La probabilità è un buon indicatore di affidabilità: gli articoli del test set con probabilità sopra 0,8 sono classificati correttamente nel 95% dei casi, quelli sotto 0,4 al massimo nel 65%.
- Le categorie riflettono i sotto-argomenti del dataset (vedi [Insights](#insights)): per esempio `sports` è quasi solo tennis, e un testo sul calcio viene classificato come `engineering`, tra le cui parole più importanti c'è *footballer*.
- Il testo deve contenere da 1 a 500.000 caratteri; richieste non valide ricevono errore 422 con il dettaglio del problema.

Configurazione tramite variabili d'ambiente:
- `WIKIPEDIA_MODEL_DIR`: cartella del modello esportato (default `models/esportato`).
- `WIKIPEDIA_API_KEY`: se impostata, tutte le richieste tranne `/health` devono contenere l'header `X-API-Key` con questo valore, altrimenti ricevono errore 401.

In produzione il servizio va esposto tramite HTTPS, per esempio dietro un reverse proxy, e con `WIKIPEDIA_API_KEY` impostata. L'API non salva i testi ricevuti.

# Workflow
1. <strong>Caricamento</strong>: il CSV viene letto direttamente da Spark con `spark.read.csv` (`multiLine` ed `escape` delle virgolette, perché i testi contengono a capo e virgolette), senza passare da pandas: con 1 GB di dati caricare tutto in memoria sul driver vanificherebbe l'uso di Spark.

2. <strong>Qualità dei dati e pulizia</strong>: si rimuovono i duplicati esatti, i testi vuoti e i testi presenti con più categorie, e si tiene un solo articolo per testo.

3. <strong>Analisi esplorativa</strong>: per ogni categoria, numero e quota di articoli, parole per articolo (media, mediana, minimo, massimo), articolo più lungo e più corto.

4. <strong>Parole e nuvole di parole</strong>: il testo viene tokenizzato con `RegexTokenizer` (solo lettere, in minuscolo) e filtrato con `StopWordsRemover`, con la stessa procedura in tutte le analisi. Per ogni categoria si calcolano le parole più frequenti e le <strong>parole caratteristiche</strong>, frequenti nella categoria e relativamente rare nelle altre, che alimentano le nuvole di parole.

5. <strong>Classificatore</strong>: pipeline Spark ML
   - <strong><mark>RegexTokenizer → StopWordsRemover → CountVectorizer → IDF → classificatore → IndexToString</mark></strong>

   Lo split train/test (80/20) è stratificato per categoria. Vengono confrontati su un validation set ricavato dal training set:
   - regressione logistica con diversi valori di regolarizzazione e con <strong>pesi per classe</strong>: ogni articolo pesa in modo inversamente proporzionale alla dimensione della sua categoria, perché le categorie sono molto sbilanciate (`politics` ha 30 volte meno articoli di `medicine`);
   - per confronto, la stessa regressione logistica senza pesi per classe;
   - regressione logistica con la standardizzazione delle feature, impostazione predefinita di Spark: con il TF-IDF amplifica le parole rare e peggiora i risultati;
   - Naive Bayes, il modello classico per la classificazione di testi, e random forest, preceduta dalla selezione delle 2.000 parole più informative, perché con 50.000 feature sparse esaurisce la memoria.

   Tutti i modelli tranne quello di riferimento senza pesi usano i pesi per classe. Il modello finale è il migliore per F1 macro tra quelli con i pesi; viene valutato una sola volta sul test set e salvato come pipeline completa, che riceve direttamente il testo grezzo. Viene anche esportato in un formato leggero, usato dall'API REST senza Spark.

6. <strong>Insights</strong>: duplicazione e densità degli articoli per categoria, sotto-argomenti dominanti, indicatori linguistici (ricchezza lessicale e lunghezza delle parole).

# Risultati
Tutti i risultati sono calcolati con Spark 3.5 in locale sul dataset completo e sono riportati, con i grafici, nel notebook [`notebooks/risultati.ipynb`](notebooks/risultati.ipynb).

### Qualità dei dati
| | Righe |
|---|---|
| Dataset grezzo | 153.232 |
| Righe distinte | 75.986 |
| Testi vuoti | 463 |
| Righe con testi presenti in più categorie | 2.435 (1.192 testi) |
| <strong>Articoli dopo la pulizia</strong> | <strong>72.178</strong> |

<strong>Metà del dataset è composta da duplicati esatti.</strong> Il dataset grezzo sembra bilanciato, con circa 10.000 righe per categoria, ma è un effetto dei duplicati, distribuiti in modo molto diverso tra le categorie: `politics` ha 11.358 righe ma solo <strong>243 articoli distinti</strong> (98% di duplicati), `medicine` 8.351 articoli distinti su 10.076 righe.

![Duplicati per categoria](images/duplicati_per_categoria.png)

### Analisi esplorativa
- Un articolo ha in media circa 780 parole, ma la distribuzione è molto asimmetrica: la mediana è 395 parole e pochi articoli molto lunghi (fino a 33.479 parole) alzano la media.
- `politics` ha gli articoli più lunghi (mediana di 1.130 parole), `pets` e `transport` i più brevi (mediana di circa 150–160 parole).
- Gli articoli più corti hanno poche parole e spesso non sono veri articoli: pagine di servizio come `wikipedia:wikiproject council/proposals/android` o elenchi come `companies listed on the new york stock exchange`.

Le tabelle complete (articoli e parole per categoria, articolo più lungo e più corto di ogni categoria) sono nel notebook e vengono salvate da `eda.py` in `reports/`.

### Nuvole di parole
Le nuvole mostrano le <strong>parole caratteristiche</strong> di ogni categoria, con un punteggio pari al conteggio nella categoria moltiplicato per il logaritmo del rapporto tra frequenza nella categoria e frequenza complessiva. Le parole semplicemente frequenti sono simili tra le categorie e non le distinguono: per esempio *references* (dal titolo della sezione delle fonti) è tra le 20 più frequenti in 10 categorie su 15, *world* in 7, *american* in 6.

![Nuvole di parole](images/wordclouds.png)

### Insights
- <strong>Le categorie riflettono il modo in cui è stato costruito il dataset più che il significato del loro nome.</strong> `science` è dominata da articoli di aviazione (*aircraft*, *engine*, *flight*), `trade` dalla storia del commercio di pellicce in Nord America (*fort*, *fur*, *hudson*, *bay*), `engineering` da articoli su Dresda e la Sassonia, `sports` quasi solo dal tennis. Le categorie non sono quindi un campione rappresentativo dei rispettivi temi, e un classificatore addestrato su questi dati potrebbe sbagliare su articoli dello stesso tema ma di altri sotto-argomenti (per esempio il calcio per `sports`).
- <strong>Densità degli articoli</strong>: `transport`, `medicine` e `research` sono le categorie più coperte (9–11% degli articoli ciascuna); `culture` (3%) e soprattutto `politics` (0,3%) le meno coperte. Sono le categorie su cui concentrare la raccolta di nuovi articoli.
- <strong>Tendenze linguistiche</strong>: `humanities`, `pets` e `culture` hanno il lessico più vario, `energy` e `transport` il più ripetitivo, con molti termini tecnici ricorrenti. Le parole sono più lunghe in `politics`, `research` ed `economics` (linguaggio più astratto) e più corte in `sports`.

### Classificatore
Confronto sul validation set (11.550 articoli). Il *recall minimo* è il recall della categoria riconosciuta peggio:

| Modello | Accuratezza | F1 macro | Recall minimo |
|---|---|---|---|
| <strong>Regressione logistica (regParam=0.01)</strong> | <strong>0.908</strong> | <strong>0.892</strong> | <strong>0.703</strong> |
| Regressione logistica (regParam=0.001) | 0.902 | 0.889 | 0.676 |
| Regressione logistica (regParam=0.1) | 0.901 | 0.882 | 0.757 |
| Regressione logistica senza pesi per classe (riferimento) | 0.907 | 0.883 | 0.405 |
| Regressione logistica con standardizzazione (impostazione predefinita di Spark) | 0.868 | 0.848 | 0.541 |
| Naive Bayes | 0.847 | 0.832 | 0.622 |
| Random forest (2.000 parole) | 0.731 | 0.701 | 0.425 |

Tutti i modelli tranne quello senza pesi usano i pesi per classe.

Valutazione finale del modello scelto, regressione logistica con pesi per classe, sul test set (14.440 articoli):

| | Accuratezza | F1 macro | Recall di `politics` |
|---|---|---|---|
| <strong>Dati puliti</strong> | <strong>0.902</strong> | <strong>0.882</strong> | <strong>0.574</strong> |
| Stesso modello valutato come nel progetto originale (dati con duplicati, split casuale) | 0.945 | 0.944 | – |

- Con i duplicati il <strong>65% degli articoli del test set è già presente nel training set</strong>: l'accuratezza del 94,5% misura in buona parte la memoria del modello, non la capacità di classificare articoli nuovi. Il valore realistico è il 90,2%.
- I <strong>pesi per classe</strong> lasciano praticamente invariata l'accuratezza e migliorano l'F1 macro (da 0,883 a 0,892 sul validation set), perché aumentano il recall della categoria peggiore da 0,41 a 0,70. `politics`, la categoria più piccola (233 articoli), resta la più difficile: sul test set ha F1 0,59 (precisione 0,60, recall 0,57). Con soli 47 articoli nel test set, però, queste metriche sono molto variabili: un articolo in più o in meno riconosciuto sposta il recall di 2 punti.
- La standardizzazione delle feature, attiva per default in Spark, fa perdere 4 punti: con il TF-IDF porta le parole rare sulla stessa scala di quelle frequenti e ne gonfia l'influenza.
- La random forest è nettamente peggiore: i modelli ad alberi non sono adatti a decine di migliaia di feature testuali sparse.
- Gli errori più frequenti sono tra `medicine` e `research` (187 articoli di `medicine` classificati come `research` e 127 nel senso opposto), perché molti articoli parlano di ricerca medica. Gli articoli di `politics` non riconosciuti (20 su 47) sono sparsi tra molte categorie, al massimo 6 nella stessa (`culture`).

![Matrice di confusione](images/matrice_confusione.png)
