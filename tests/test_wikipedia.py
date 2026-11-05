import os
import random
import sys

import pytest
from pyspark.ml.classification import LogisticRegression, NaiveBayes

from wikipedia import analysis, classifier
from wikipedia.data import (
    clean_dataset,
    duplication_by_category,
    quality_report,
    read_dataset,
    stratified_split,
    word_count,
)
from wikipedia.spark import get_spark
from wikipedia.text import tokenize

VOCABULARY = {
    "sports": ["football", "match", "league", "goal", "team", "player", "season", "stadium"],
    "medicine": ["patient", "disease", "treatment", "clinical", "symptoms", "therapy", "cancer", "drug"],
    "energy": ["solar", "wind", "power", "electricity", "grid", "turbine", "renewable", "oil"],
}
COMMON = ["history", "world", "people", "city", "group", "system", "large", "public"]


def _article(category, rng, i):
    words = rng.choices(VOCABULARY[category], k=30) + rng.choices(COMMON, k=30)
    rng.shuffle(words)
    text = f"{category.title()} article {i}. " + " ".join(words) + "."
    return (f"{category} {i}", text.split(".")[0], text, category)


@pytest.fixture(scope="session")
def spark():
    session = get_spark("test", driver_memory="2g")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def raw(spark):
    rng = random.Random(0)
    rows = [_article(c, rng, i) for c in VOCABULARY for i in range(60)]
    rows += rows[:20]                                           # 20 duplicati esatti
    rows.append(("Vuoto", "", "   ", "sports"))                 # testo vuoto
    rows.append(("Ambiguo A", "x", "same text in two categories", "sports"))
    rows.append(("Ambiguo B", "x", "same text in two categories", "energy"))
    rows.append(("Altro titolo", rows[0][1], rows[0][2], rows[0][3]))  # stesso testo, altro titolo
    return spark.createDataFrame(rows, ["title", "summary", "documents", "categoria"])


@pytest.fixture(scope="session")
def clean(raw):
    return clean_dataset(raw).cache()


def test_read_dataset_handles_multiline_and_quotes(spark, tmp_path):
    path = tmp_path / "sample.csv"
    path.write_text(
        ',title,summary,documents,categoria\n'
        '0,A,"Riga 1","Testo su\npiù righe con ""virgolette"", e virgole",sports\n'
        '1,B,"Riga 2","Secondo testo",energy\n',
        encoding="utf-8",
    )
    df = read_dataset(spark, path)
    rows = df.orderBy("title").collect()
    assert df.columns == ["title", "summary", "documents", "categoria"]
    assert len(rows) == 2
    assert rows[0].documents == 'Testo su\npiù righe con "virgolette", e virgole'


def test_word_count_handles_null_and_empty(spark):
    df = spark.createDataFrame([("uno due  tre",), ("",), ("   ",), (None,)], "t string")
    assert [r.n for r in df.select(word_count("t").alias("n")).collect()] == [3, 0, 0, 0]


def test_quality_report(raw):
    report = quality_report(raw)
    assert report["righe"] == 180 + 20 + 4
    assert report["righe_uniche"] == 180 + 4
    assert report["testi_vuoti"] == 1
    assert report["testi_con_piu_categorie"] == 1
    assert report["righe_con_testi_ambigui"] == 2


def test_clean_dataset(clean):
    assert clean.count() == 180
    titles = {r.title for r in clean.select("title").collect()}
    assert not titles & {"Vuoto", "Ambiguo A", "Ambiguo B"}
    assert clean.select("documents").distinct().count() == 180


def test_duplication_by_category(raw):
    table = {r.categoria: r for r in duplication_by_category(raw).collect()}
    assert table["sports"].righe == 60 + 20 + 3
    assert table["sports"].righe_uniche == 60 + 3
    assert table["medicine"].quota_duplicati == 0


def test_stratified_split(clean):
    train, test = stratified_split(clean, test_fraction=0.25)
    counts = {r.categoria: r["count"] for r in test.groupBy("categoria").count().collect()}
    assert counts == {"sports": 15, "medicine": 15, "energy": 15}
    assert train.count() + test.count() == clean.count()
    assert train.join(test, "documents").count() == 0
    again, _ = stratified_split(clean, test_fraction=0.25)
    assert sorted(r.title for r in train.collect()) == sorted(r.title for r in again.collect())


def test_tokenize(spark):
    df = spark.createDataFrame([("The team's 2023 season: don't stop! It is a GOAL.",)], "documents string")
    tokens = tokenize(df, "documents").first().tokens
    assert tokens == ["team", "season", "stop", "goal"]


def test_category_stats_and_extremes(clean):
    stats = analysis.category_stats(clean)
    assert set(stats.index) == set(VOCABULARY)
    assert stats["articoli"].sum() == 180
    assert stats["quota"].sum() == pytest.approx(1)
    assert (stats["parole_min"] <= stats["parole_media"]).all()
    extremes = analysis.extreme_articles(clean)
    assert (extremes["parole_piu_lungo"] >= extremes["parole_piu_corto"]).all()


def test_distinctive_words(clean):
    counts = analysis.word_counts(tokenize(clean, "documents"))
    distinctive = analysis.distinctive_words(counts, n=5, min_count=5)
    for category, group in distinctive.groupby("categoria"):
        # Le parole caratteristiche sono quelle della categoria, non quelle comuni a tutte
        assert set(group["word"]) <= set(VOCABULARY[category]) | {category}
        assert group["score"].is_monotonic_decreasing
    top = analysis.top_words(counts, n=3)
    assert len(top) == 9


def test_lexical_stats(clean):
    stats = analysis.lexical_stats(tokenize(clean, "documents"))
    assert stats["ricchezza_lessicale"].between(0, 1).all()
    assert (stats["lunghezza_media_parole"] > 3).all()


def test_classifier_end_to_end(spark, clean):
    train, test = stratified_split(clean, test_fraction=0.25)
    feats = classifier.featurizer(vocab_size=1000, min_df=2)
    comparison = classifier.compare_models(
        train, test, {"logistica": LogisticRegression(maxIter=50, regParam=0.01), "nb": NaiveBayes()},
        featurizer_pipeline=feats,
    )
    assert set(comparison.index) == {"logistica", "nb"}
    assert (comparison["accuratezza"] > 0.9).all()

    model, labels = classifier.train_final_model(
        train, LogisticRegression(maxIter=50, regParam=0.01), classifier.featurizer(vocab_size=1000, min_df=2)
    )
    rows, metrics = classifier.evaluate(model, test, labels)
    assert metrics["f1_macro"] > 0.9
    assert classifier.per_class_report(rows).shape[0] == 3
    assert classifier.confusion(rows).to_numpy().sum() == test.count()

    new = spark.createDataFrame([("Nuovo", "The patient started a new therapy for the disease.")],
                                ["title", "documents"])
    predicted = classifier.predict(model, new)
    assert predicted.loc[0, classifier.PREDICTION] == "medicine"
    assert 0 < predicted.loc[0, "probabilita"] <= 1
    terms = classifier.top_terms(model, n=3)
    for category, words in VOCABULARY.items():
        assert terms.loc[category].isin(words + [category]).any()


@pytest.mark.skipif(sys.platform == "win32" and not os.environ.get("HADOOP_HOME"),
                    reason="su Windows Spark scrive su disco solo con HADOOP_HOME e winutils")
def test_save_and_load(spark, clean, tmp_path):
    train, _ = stratified_split(clean)
    model, _ = classifier.train_final_model(train, NaiveBayes(), classifier.featurizer(vocab_size=500, min_df=1))
    classifier.save_model(model, tmp_path / "model")
    loaded = classifier.load_model(tmp_path / "model")
    assert loaded.transform(clean).select(classifier.PREDICTION).count() == clean.count()


def test_random_forest_with_feature_selection(clean):
    train, test = stratified_split(clean, test_fraction=0.25)
    comparison = classifier.compare_models(
        train, test, {"rf": classifier.random_forest(n_features=20)},
        featurizer_pipeline=classifier.featurizer(vocab_size=1000, min_df=2),
    )
    assert comparison.loc["rf", "accuratezza"] > 0.8


def test_class_weights(spark):
    df = spark.createDataFrame([(0.0,)] * 6 + [(1.0,)] * 2, "label double")
    weights = {r.label: r.weight for r in classifier.with_class_weights(df).distinct().collect()}
    assert weights == {0.0: pytest.approx(8 / (2 * 6)), 1.0: pytest.approx(8 / (2 * 2))}


def test_logistic_regression_defaults(spark):
    model = classifier.logistic_regression(0.01)
    assert model.getStandardization() is False
    assert model.getWeightCol() == "weight"
    assert not classifier.logistic_regression(0.01, class_weights=False).isSet("weightCol")


def test_candidate_models_use_class_weights(spark):
    for name, model in classifier.candidate_models().items():
        estimator = model.getStages()[-1] if hasattr(model, "getStages") else model
        # Solo il modello di riferimento "senza pesi" li esclude
        assert estimator.isSet("weightCol") != ("senza pesi" in name), name


def test_select_model_skips_reference_models():
    import pandas as pd
    reference = next(iter(classifier.REFERENCE_MODELS))
    comparison = pd.DataFrame({"f1_macro": [0.95, 0.90, 0.80]}, index=[reference, "con pesi A", "con pesi B"])
    assert classifier.select_model(comparison) == "con pesi A"


def test_metrics_include_worst_class_recall():
    import pandas as pd
    rows = pd.DataFrame({"categoria": ["a", "a", "b", "b"], "prevista": ["a", "a", "b", "a"]})
    assert classifier.classification_metrics(rows)["recall_minimo"] == 0.5
