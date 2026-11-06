import numpy as np
import pytest
from fastapi.testclient import TestClient
from pyspark.ml.classification import LogisticRegression, NaiveBayes

from wikipedia import classifier, export
from wikipedia.api import MAX_BATCH_SIZE, create_app
from wikipedia.data import stratified_split

NEW_ARTICLES = [
    "The patient started a new therapy for the disease.",
    "The football team won the league match in the new stadium!",
    "Solar and wind power feed the electricity grid; the turbine is renewable.",
    "Totally unrelated words: xylophone zebra quixotic.",  # nessuna parola nel vocabolario
    "",
]


def _train(clean, estimator):
    train, test = stratified_split(clean, test_fraction=0.25)
    model, _ = classifier.train_final_model(train, estimator, classifier.featurizer(vocab_size=1000, min_df=2))
    return model, test


@pytest.fixture(scope="module")
def exported(clean, tmp_path_factory):
    model, _ = _train(clean, classifier.logistic_regression(0.01))
    path = tmp_path_factory.mktemp("esportato")
    export.export_model(model, path, model_name="test", test_metrics={"accuratezza": 1.0})
    return path


ESTIMATORS = {
    "logistica": lambda: classifier.logistic_regression(0.01),
    "logistica_standardizzata": lambda: LogisticRegression(maxIter=50, regParam=0.01, standardization=True),
    "naive_bayes": lambda: NaiveBayes(),
}


@pytest.mark.parametrize("name", ESTIMATORS)
def test_export_matches_spark(spark, clean, tmp_path, name):
    model, test = _train(clean, ESTIMATORS[name]())
    export.export_model(model, tmp_path)
    loaded = export.ExportedModel.load(tmp_path)

    articles = test.select("documents").toPandas()["documents"].tolist() + NEW_ARTICLES
    df = spark.createDataFrame([(t,) for t in articles], ["documents"])
    rows = model.transform(df).select(classifier.PREDICTION, "probability").toPandas()
    expected = np.vstack([v.toArray() for v in rows["probability"]])

    probabilities, _ = loaded.predict_proba(articles)
    np.testing.assert_allclose(probabilities, expected, atol=1e-9)
    predicted = [p["categoria"] for p in loaded.predict(articles)]
    assert predicted == rows[classifier.PREDICTION].tolist()


def test_export_rejects_random_forest(clean, tmp_path):
    model, _ = _train(clean, classifier.random_forest(n_features=20))
    with pytest.raises(ValueError, match="regressione logistica"):
        export.export_model(model, tmp_path)


def test_exported_tokenizer_matches_spark_rules(exported):
    model = export.ExportedModel.load(exported)
    # minuscole, solo lettere, almeno 2 caratteri, senza stopword
    assert model.tokenize("The Patient's 3 new X-ray tests, a dog!") == ["patient", "new", "ray", "tests", "dog"]


@pytest.fixture(scope="module")
def client(exported):
    with TestClient(create_app(model_dir=exported, api_key="")) as test_client:
        yield test_client


def test_health_and_model_info(client):
    assert client.get("/health").json() == {"stato": "ok"}
    info = client.get("/model").json()
    assert info["modello"] == "test"
    assert sorted(info["categorie"]) == ["energy", "medicine", "sports"]
    assert info["metriche_test"] == {"accuratezza": 1.0}


def test_predict(client):
    response = client.post("/predict", json={"titolo": "Nuovo", "testo": NEW_ARTICLES[0]})
    assert response.status_code == 200
    body = response.json()
    assert body["titolo"] == "Nuovo"
    assert body["categoria"] == "medicine"
    assert 0 < body["probabilita"] <= 1
    assert [a["categoria"] for a in body["alternative"]][0] == "medicine"
    assert len(body["alternative"]) == 3
    assert body["parole_riconosciute"] == 3  # patient, therapy, disease
    assert "solo 3 parole riconosciute" in body["avviso"]


def test_predict_top_k(client):
    body = client.post("/predict?top_k=1", json={"testo": NEW_ARTICLES[1]}).json()
    assert body["categoria"] == "sports"
    assert len(body["alternative"]) == 1
    # top_k maggiore del numero di categorie: vengono restituite tutte
    assert len(client.post("/predict?top_k=20", json={"testo": NEW_ARTICLES[1]}).json()["alternative"]) == 3
    assert client.post("/predict?top_k=0", json={"testo": NEW_ARTICLES[1]}).status_code == 422


def test_predict_unknown_words(client):
    body = client.post("/predict", json={"testo": NEW_ARTICLES[3]}).json()
    assert body["categoria"] is None and body["probabilita"] is None
    assert body["parole_riconosciute"] == 0
    assert "vocabolario" in body["avviso"]


def test_predict_batch(client):
    articles = [{"titolo": f"a{i}", "testo": t} for i, t in enumerate(NEW_ARTICLES[:3])]
    body = client.post("/predict/batch", json={"articoli": articles}).json()
    assert [p["categoria"] for p in body["previsioni"]] == ["medicine", "sports", "energy"]
    assert [p["titolo"] for p in body["previsioni"]] == ["a0", "a1", "a2"]


@pytest.mark.parametrize("payload, path", [
    ({"testo": ""}, "/predict"),
    ({"titolo": "senza testo"}, "/predict"),
    ({"articoli": []}, "/predict/batch"),
    ({"articoli": [{"testo": "x"}] * (MAX_BATCH_SIZE + 1)}, "/predict/batch"),
])
def test_validation_errors(client, payload, path):
    assert client.post(path, json=payload).status_code == 422


def test_api_key(exported):
    with TestClient(create_app(model_dir=exported, api_key="segreta")) as secured:
        assert secured.get("/health").status_code == 200
        assert secured.get("/model").status_code == 401
        assert secured.post("/predict", json={"testo": "x"}, headers={"X-API-Key": "errata"}).status_code == 401
        assert secured.post("/predict", json={"testo": "x"}, headers={"X-API-Key": "segreta"}).status_code == 200
