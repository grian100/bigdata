"""Classificazione di nuovi articoli con il modello prodotto da train.py.

Esempi:
    python predict.py --testo "The team won the league after a penalty shootout in the final match."
    python predict.py --csv nuovi_articoli.csv     # CSV con la colonna documents (e opzionalmente title)

Per default usa il modello esportato in models/esportato, che non richiede Spark (lo stesso
dell'API REST). Con --spark usa la PipelineModel salvata in models/wikipedia_classifier.
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

import pandas as pd

SPARK_MODEL_PATH = ROOT / "models" / "wikipedia_classifier"
EXPORT_PATH = ROOT / "models" / "esportato"


def with_spark(args):
    from wikipedia import classifier
    from wikipedia.spark import get_spark

    path = args.modello or SPARK_MODEL_PATH
    if not path.exists():
        sys.exit(f"Modello Spark non trovato in {path}: eseguire prima 'python train.py'.")
    spark = get_spark("wikipedia-predict", driver_memory="2g")
    model = classifier.load_model(path)
    if args.testo:
        articles = spark.createDataFrame([("testo", args.testo)], ["title", "documents"])
    else:
        articles = spark.read.csv(str(args.csv), header=True, multiLine=True, quote='"', escape='"')
        if "documents" not in articles.columns:
            sys.exit("Il CSV deve contenere la colonna 'documents'.")
    result = classifier.predict(model, articles)
    spark.stop()
    return result


def with_export(args):
    from wikipedia.export import ExportedModel

    path = args.modello or EXPORT_PATH
    if not (path / "modello.npz").exists():
        sys.exit(f"Modello esportato non trovato in {path}: eseguire prima 'python train.py'.")
    model = ExportedModel.load(path)
    if args.testo:
        articles = pd.DataFrame({"title": ["testo"], "documents": [args.testo]})
    else:
        articles = pd.read_csv(args.csv)
        if "documents" not in articles.columns:
            sys.exit("Il CSV deve contenere la colonna 'documents'.")
    predictions = model.predict(articles["documents"].fillna("").astype(str).tolist())
    result = pd.DataFrame({
        "categoria_prevista": [p["categoria"] for p in predictions],
        "probabilita": [p["probabilita"] for p in predictions],
        "alternative": [", ".join(f"{a['categoria']} ({a['probabilita']:.0%})" for a in p["alternative"])
                        for p in predictions],
    })
    if "title" in articles.columns:
        result.insert(0, "title", articles["title"].to_numpy())
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--testo", help="testo dell'articolo da classificare")
    source.add_argument("--csv", type=Path, help="file CSV con una colonna documents")
    parser.add_argument("--spark", action="store_true", help="usa la PipelineModel di Spark")
    parser.add_argument("--modello", type=Path, help="cartella del modello (default in base a --spark)")
    args = parser.parse_args()

    result = with_spark(args) if args.spark else with_export(args)
    pd.set_option("display.width", 160)
    pd.set_option("display.max_colwidth", 80)
    print(result.to_string(index=False))


if __name__ == "__main__":
    main()
