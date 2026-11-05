"""Analisi e classificazione automatica degli articoli di Wikipedia con Apache Spark."""

from .data import (
    DATASET_URL,
    LABEL,
    SEED,
    TEXT,
    clean_dataset,
    download_dataset,
    duplication_by_category,
    quality_report,
    read_dataset,
    stratified_split,
)
from .spark import get_spark
from .text import tokenize
