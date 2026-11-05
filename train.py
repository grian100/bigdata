"""Addestramento e valutazione del classificatore della categoria degli articoli.

Passaggi:
1. pulizia del dataset (duplicati, testi vuoti, testi con più categorie);
2. split stratificato train/test (80/20); dal training set si ricava un validation set (20%);
3. confronto dei modelli sul validation set, con le stesse feature TF-IDF;
4. il modello migliore (F1 macro) viene riaddestrato su tutto il training set e valutato sul test set;
5. salvataggio della pipeline completa in models/.

Opzioni:
    --confronto-duplicati   ripete la valutazione come nel progetto originale (dati con duplicati,
                            split casuale) per misurare quanto i duplicati gonfiano l'accuratezza
"""

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pyspark.sql import functions as F

from wikipedia import classifier
from wikipedia.data import LABEL, SEED, TEXT, clean_dataset, download_dataset, read_dataset, stratified_split
from wikipedia.spark import cache, get_spark

DATA_PATH = ROOT / "data" / "wikipedia.csv"
MODEL_PATH = ROOT / "models" / "wikipedia_classifier"
REPORTS = ROOT / "reports"
IMAGES = ROOT / "images"


def plot_confusion(matrix, path):
    normalized = matrix.div(matrix.sum(axis=1), axis=0)
    fig, ax = plt.subplots(figsize=(10, 8.5))
    image = ax.imshow(normalized, cmap="Blues", vmin=0, vmax=1)
    for (i, j), value in np.ndenumerate(normalized.to_numpy()):
        if value >= 0.01:
            ax.text(j, i, f"{value:.0%}", ha="center", va="center", fontsize=7,
                    color="white" if value > 0.5 else "black")
    ax.set_xticks(range(len(matrix.columns)), matrix.columns, rotation=60, ha="right")
    ax.set_yticks(range(len(matrix.index)), matrix.index)
    ax.set_xlabel("categoria prevista")
    ax.set_ylabel("categoria reale")
    ax.set_title("Matrice di confusione sul test set (quota per categoria reale)")
    fig.colorbar(image, ax=ax, fraction=0.04)
    fig.tight_layout()
    fig.savefig(path, dpi=110)
    plt.close(fig)


def duplicates_comparison(raw, model):
    """Valutazione come nel progetto originale: dati con duplicati e randomSplit."""
    data = raw.filter(F.trim(F.coalesce(F.col(TEXT), F.lit(""))) != "")
    train, test = data.randomSplit([0.8, 0.2], seed=SEED)
    seen = train.select(TEXT).distinct()
    leaked = test.join(seen, TEXT, "left_semi").count() / test.count()
    fitted, labels = classifier.train_final_model(train, model)
    _, metrics = classifier.evaluate(fitted, test, labels)
    return {**metrics, "quota_test_presente_nel_training": leaked}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--confronto-duplicati", action="store_true")
    args = parser.parse_args()

    pd.set_option("display.width", 160)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.float_format", "{:.3f}".format)
    REPORTS.mkdir(exist_ok=True)
    IMAGES.mkdir(exist_ok=True)

    spark = get_spark("wikipedia-train")
    raw = read_dataset(spark, download_dataset(DATA_PATH))
    articles = cache(clean_dataset(raw))
    train, test = stratified_split(articles, 0.2)
    train_sub, validation = stratified_split(train, 0.2, seed=SEED + 1)
    train, test, train_sub, validation = (cache(d) for d in (train, test, train_sub, validation))
    print(f"Articoli: {articles.count():,} - training {train.count():,} "
          f"(di cui validation {validation.count():,}) - test {test.count():,}\n".replace(",", "."))

    print("Confronto dei modelli sul validation set:")
    start = time.time()
    comparison = classifier.compare_models(train_sub, validation, verbose=True)
    print(comparison, f"\n({time.time() - start:.0f} s)\n")
    comparison.to_csv(REPORTS / "confronto_modelli.csv")

    best = comparison["f1_macro"].idxmax()
    print(f"Modello scelto: {best}", flush=True)
    model, labels = classifier.train_final_model(train, classifier.candidate_models()[best])
    rows, metrics = classifier.evaluate(model, test, labels)
    print(f"Test set: {', '.join(f'{k} {v:.3f}' for k, v in metrics.items())}\n")
    report = classifier.per_class_report(rows)
    print(f"Metriche per categoria:\n{report}\n")
    report.to_csv(REPORTS / "metriche_per_categoria.csv")
    matrix = classifier.confusion(rows)
    matrix.to_csv(REPORTS / "matrice_confusione.csv")
    plot_confusion(matrix, IMAGES / "matrice_confusione.png")
    if hasattr(model.stages[-2], "coefficientMatrix"):
        terms = classifier.top_terms(model, n=10)
        print(f"Parole con il peso più alto per categoria:\n{terms.apply(', '.join, axis=1).to_string()}\n")
        terms.to_csv(REPORTS / "parole_classificatore.csv")

    summary = {"modello": best, "test": metrics, "confronto": comparison.to_dict(orient="index")}
    if args.confronto_duplicati:
        print("Valutazione come nel progetto originale (dati con duplicati, split casuale):")
        summary["con_duplicati"] = duplicates_comparison(raw, classifier.candidate_models()[best])
        print(f"{summary['con_duplicati']}\n")
    (REPORTS / "risultati_classificatore.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    try:
        classifier.save_model(model, MODEL_PATH)
        print(f"Modello salvato in {MODEL_PATH.relative_to(ROOT).as_posix()}")
    except Exception as error:  # su Windows Spark scrive su disco solo con winutils
        print(f"Impossibile salvare il modello ({type(error).__name__}). Su Windows Spark richiede "
              "HADOOP_HOME con winutils.exe: eseguire l'addestramento su Databricks, Linux, macOS o WSL.")
    spark.stop()


if __name__ == "__main__":
    main()
