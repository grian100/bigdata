"""Classificazione di nuovi articoli con il modello salvato da train.py.

Esempi:
    python predict.py --testo "The team won the league after a penalty shootout in the final match."
    python predict.py --csv nuovi_articoli.csv     # CSV con la colonna documents (e opzionalmente title)
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

import pandas as pd

from wikipedia import classifier
from wikipedia.spark import get_spark

MODEL_PATH = ROOT / "models" / "wikipedia_classifier"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--testo", help="testo dell'articolo da classificare")
    source.add_argument("--csv", type=Path, help="file CSV con una colonna documents")
    parser.add_argument("--modello", type=Path, default=MODEL_PATH, help="cartella del modello salvato")
    args = parser.parse_args()

    if not args.modello.exists():
        sys.exit(f"Modello non trovato in {args.modello}: eseguire prima 'python train.py'.")

    spark = get_spark("wikipedia-predict", driver_memory="2g")
    model = classifier.load_model(args.modello)
    if args.testo:
        articles = spark.createDataFrame([("testo", args.testo)], ["title", "documents"])
    else:
        articles = spark.read.csv(str(args.csv), header=True, multiLine=True, quote='"', escape='"')
        if "documents" not in articles.columns:
            sys.exit("Il CSV deve contenere la colonna 'documents'.")

    result = classifier.predict(model, articles)
    pd.set_option("display.width", 160)
    pd.set_option("display.max_colwidth", 80)
    print(result.to_string(index=False))
    spark.stop()


if __name__ == "__main__":
    main()
