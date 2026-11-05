"""Analisi esplorativa, nuvole di parole e insights sugli articoli di Wikipedia.

Esegue in Spark:
1. controllo di qualità del dataset grezzo (duplicati, testi vuoti, testi con più categorie);
2. pulizia del dataset;
3. statistiche per categoria: numero di articoli, parole per articolo, articolo più lungo e più corto;
4. parole più frequenti e parole caratteristiche di ogni categoria, indicatori linguistici;
5. nuvole di parole per categoria.

Le tabelle vengono salvate in reports/, i grafici in images/.
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from wikipedia import analysis, wordclouds
from wikipedia.data import LABEL, clean_dataset, download_dataset, duplication_by_category, quality_report, read_dataset
from wikipedia.spark import cache, get_spark
from wikipedia.text import tokenize

DATA_PATH = ROOT / "data" / "wikipedia.csv"
REPORTS = ROOT / "reports"
IMAGES = ROOT / "images"


def plot_duplication(duplication, path):
    table = duplication.set_index(LABEL).sort_values("righe_uniche")
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.barh(table.index, table["righe"], color="#b0b0b0", label="righe nel dataset grezzo")
    ax.barh(table.index, table["righe_uniche"], color="#1f4e79", label="articoli distinti")
    for y, (rows, unique) in enumerate(zip(table["righe"], table["righe_uniche"])):
        ax.text(rows + 100, y, f"{unique:,} / {rows:,}".replace(",", "."), va="center", fontsize=8)
    ax.set_xlabel("righe")
    ax.set_title("Righe per categoria: dataset grezzo e articoli distinti")
    ax.set_xlim(0, table["righe"].max() * 1.5)
    ax.legend(loc="upper right")
    fig.tight_layout()
    fig.savefig(path, dpi=110)
    plt.close(fig)


def main():
    pd.set_option("display.width", 160)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.float_format", "{:.2f}".format)
    REPORTS.mkdir(exist_ok=True)
    IMAGES.mkdir(exist_ok=True)

    spark = get_spark("wikipedia-eda")
    raw = read_dataset(spark, download_dataset(DATA_PATH))

    print("=== Qualità del dataset grezzo ===")
    report = quality_report(raw)
    for key, value in report.items():
        print(f"  {key}: {value:,}".replace(",", "."))
    (REPORTS / "qualita_dati.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    duplication = duplication_by_category(raw).toPandas()
    print(f"\nDuplicati per categoria:\n{duplication.to_string(index=False)}\n")
    duplication.to_csv(REPORTS / "duplicati_per_categoria.csv", index=False)
    plot_duplication(duplication, IMAGES / "duplicati_per_categoria.png")

    articles = cache(clean_dataset(raw))
    print("=== Statistiche sul dataset pulito ===")
    print(f"Articoli: {articles.count():,}".replace(",", "."))
    print(f"Lunghezza dei testi (parole): {analysis.overall_stats(articles)}\n")
    stats = analysis.category_stats(articles)
    print(f"Per categoria:\n{stats}\n")
    stats.to_csv(REPORTS / "statistiche_categorie.csv")
    extremes = analysis.extreme_articles(articles)
    print(f"Articolo più lungo e più corto per categoria:\n{extremes}\n")
    extremes.to_csv(REPORTS / "articoli_estremi.csv")

    print("=== Parole e indicatori linguistici ===")
    tokens = cache(tokenize(articles, "documents").select(LABEL, "tokens"))
    counts = cache(analysis.word_counts(tokens))
    top = analysis.top_words(counts, n=20)
    top.to_csv(REPORTS / "parole_frequenti.csv", index=False)
    distinctive = analysis.distinctive_words(counts, n=100)
    distinctive.to_csv(REPORTS / "parole_caratteristiche.csv", index=False)
    summary = distinctive[distinctive["rank"] <= 8].groupby(LABEL)["word"].apply(", ".join)
    print(f"Parole caratteristiche:\n{summary.to_string()}\n")
    lexical = analysis.lexical_stats(tokens)
    print(f"Indicatori linguistici:\n{lexical}\n")
    lexical.to_csv(REPORTS / "indicatori_linguistici.csv")

    paths = wordclouds.save_wordclouds(distinctive, IMAGES / "wordclouds")
    fig = wordclouds.wordcloud_grid(distinctive)
    fig.savefig(IMAGES / "wordclouds.png", dpi=80)
    plt.close(fig)
    print(f"Nuvole di parole salvate: {len(paths)} immagini in images/wordclouds/")
    spark.stop()


if __name__ == "__main__":
    main()
