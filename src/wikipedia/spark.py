"""Creazione della sessione Spark, in locale o su Databricks."""

import os
import sys

from pyspark.sql import SparkSession


def get_spark(app_name="wikipedia", driver_memory="6g"):
    """Restituisce la sessione Spark attiva o ne crea una locale.

    Su Databricks esiste già una sessione (la variabile `spark`) e viene riutilizzata così com'è;
    in locale viene creata una sessione che usa tutti i core disponibili.
    """
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    # I processi worker devono usare lo stesso interprete Python del driver
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def cache(df):
    """Tiene in memoria il DataFrame quando possibile.

    Sul compute serverless di Databricks le API di cache non sono supportate: in quel caso
    il DataFrame viene restituito così com'è e ricalcolato quando serve.
    """
    try:
        return df.cache()
    except Exception:
        return df
