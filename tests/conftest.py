"""Dataset sintetico e sessione Spark condivisi dai test."""

import random

import pytest

from wikipedia.data import clean_dataset
from wikipedia.spark import get_spark

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
