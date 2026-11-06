"""Esportazione del classificatore in un formato leggero, usabile senza Spark.

La PipelineModel di Spark richiede una JVM per fare previsioni, e su Windows non si può
salvare senza winutils. Il modello finale però è semplice: tokenizzazione con
un'espressione regolare, conteggio delle parole del vocabolario, pesi IDF e un modello
lineare (regressione logistica o Naive Bayes multinomiale). Questo modulo ne estrae i
parametri in un file NumPy e ricalcola le stesse previsioni con NumPy e SciPy.

File esportati nella cartella indicata:
- modello.npz: vocabolario, stopword, pesi IDF, coefficienti, intercette e categorie;
- metadata.json: tipo di modello, tokenizzazione, metriche sul test set, data e versioni.

Questo modulo non importa pyspark: l'API REST lo usa per caricare il modello.
"""

import json
import re
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

import numpy as np
from scipy import sparse

PARAMS_FILE = "modello.npz"
METADATA_FILE = "metadata.json"
SUPPORTED = {"LogisticRegressionModel": "regressione_logistica", "NaiveBayesModel": "naive_bayes"}


def _versions():
    result = {}
    for package in ("pyspark", "numpy", "scipy"):
        try:
            result[package] = version(package)
        except PackageNotFoundError:
            pass
    return result


def export_model(model, path, model_name="", test_metrics=None):
    """Salva i parametri della PipelineModel prodotta da classifier.train_final_model.

    Sono supportati la regressione logistica e il Naive Bayes multinomiale.
    """
    stages = {type(s).__name__: s for s in model.stages}
    estimator = next((s for s in model.stages if type(s).__name__ in SUPPORTED), None)
    if estimator is None:
        names = ", ".join(type(s).__name__ for s in model.stages)
        raise ValueError(f"Esportazione supportata solo per regressione logistica e Naive Bayes (stage: {names})")
    tokenizer, remover = stages["RegexTokenizer"], stages["StopWordsRemover"]
    if tokenizer.getGaps() or not tokenizer.getToLowercase() or remover.getCaseSensitive():
        raise ValueError("Esportazione supportata solo per la tokenizzazione di wikipedia.text")
    if stages["CountVectorizerModel"].getBinary() or stages["CountVectorizerModel"].getMinTF() != 1:
        raise ValueError("Esportazione supportata solo per CountVectorizer con binary=False e minTF=1")

    kind = SUPPORTED[type(estimator).__name__]
    if kind == "regressione_logistica":
        coefficients = estimator.coefficientMatrix.toArray()
        intercepts = estimator.interceptVector.toArray()
    else:
        if estimator.getModelType() != "multinomial":
            raise ValueError("Esportazione supportata solo per il Naive Bayes multinomiale")
        coefficients, intercepts = estimator.theta.toArray(), estimator.pi.toArray()

    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(
        path / PARAMS_FILE,
        vocabolario=np.array(stages["CountVectorizerModel"].vocabulary),
        stopword=np.array(remover.getStopWords()),
        idf=stages["IDFModel"].idf.toArray(),
        coefficienti=coefficients,
        intercette=intercepts,
        categorie=np.array(model.stages[-1].getLabels()),
    )
    metadata = {
        "modello": model_name or type(estimator).__name__,
        "tipo": kind,
        "tokenizzazione": {"pattern": tokenizer.getPattern(), "lunghezza_minima": tokenizer.getMinTokenLength()},
        "categorie": list(model.stages[-1].getLabels()),
        "parole_nel_vocabolario": len(stages["CountVectorizerModel"].vocabulary),
        "metriche_test": test_metrics or {},
        "data_addestramento": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "versioni": _versions(),
    }
    (path / METADATA_FILE).write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")
    return metadata


class ExportedModel:
    """Classificatore caricato da export_model: stesse previsioni della pipeline Spark, senza JVM."""

    def __init__(self, params, metadata):
        self.metadata = metadata
        self.kind = metadata["tipo"]
        self.labels = [str(c) for c in params["categorie"]]
        self.index = {word: i for i, word in enumerate(params["vocabolario"].tolist())}
        self.stopwords = set(params["stopword"].tolist())
        self.idf = params["idf"]
        self.coefficients = params["coefficienti"]
        self.intercepts = params["intercette"]
        self.pattern = re.compile(metadata["tokenizzazione"]["pattern"])
        self.min_length = metadata["tokenizzazione"]["lunghezza_minima"]

    @classmethod
    def load(cls, path):
        path = Path(path)
        metadata = json.loads((path / METADATA_FILE).read_text(encoding="utf-8"))
        with np.load(path / PARAMS_FILE, allow_pickle=False) as params:
            return cls({key: params[key] for key in params.files}, metadata)

    def tokenize(self, text):
        """Come RegexTokenizer + StopWordsRemover: parole in minuscolo, senza stopword."""
        tokens = self.pattern.findall(text.lower())
        return [t for t in tokens if len(t) >= self.min_length and t not in self.stopwords]

    def features(self, texts):
        """Matrice TF-IDF (articoli x vocabolario) e numero di parole riconosciute per articolo."""
        rows, cols, known = [], [], []
        for i, text in enumerate(texts):
            ids = [self.index[t] for t in self.tokenize(text) if t in self.index]
            rows += [i] * len(ids)
            cols += ids
            known.append(len(ids))
        counts = sparse.csr_matrix(
            (np.ones(len(cols)), (rows, cols)), shape=(len(texts), len(self.index))
        )  # i duplicati (i, j) vengono sommati: conteggio delle parole
        return counts.multiply(self.idf).tocsr(), np.array(known)

    def predict_proba(self, texts):
        """Probabilità di ogni categoria (colonne nell'ordine di self.labels)."""
        x, known = self.features(texts)
        margins = np.asarray(x @ self.coefficients.T) + self.intercepts
        if self.kind == "regressione_logistica" and self.coefficients.shape[0] == 1:
            p = 1 / (1 + np.exp(-margins[:, 0]))  # caso binario di Spark: un solo vettore di coefficienti
            return np.column_stack([1 - p, p]), known
        margins -= margins.max(axis=1, keepdims=True)
        probabilities = np.exp(margins)
        return probabilities / probabilities.sum(axis=1, keepdims=True), known

    def predict(self, texts, top_k=3):
        """Per ogni testo: categoria prevista, probabilità, top_k alternative e parole riconosciute."""
        probabilities, known = self.predict_proba(texts)
        results = []
        for p, n in zip(probabilities, known):
            order = np.argsort(-p, kind="stable")[:top_k]  # a parità vince la prima, come in Spark
            results.append({
                "categoria": self.labels[order[0]],
                "probabilita": float(p[order[0]]),
                "alternative": [{"categoria": self.labels[i], "probabilita": float(p[i])} for i in order],
                "parole_riconosciute": int(n),
            })
        return results
