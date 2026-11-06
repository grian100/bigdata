"""API REST per classificare articoli nella categoria di Wikipedia.

Avvio, dalla cartella del progetto:
    python -m uvicorn wikipedia.api:app --app-dir src

Documentazione interattiva su http://localhost:8000/docs.

L'API usa il modello esportato da train.py in models/esportato (vedi wikipedia.export):
le previsioni sono calcolate con NumPy, senza avviare Spark, quindi rispondono in pochi
millisecondi e il servizio non richiede Java.

Configurazione tramite variabili d'ambiente:
- WIKIPEDIA_MODEL_DIR: cartella con modello.npz e metadata.json (default: models/esportato)
- WIKIPEDIA_API_KEY: se impostata, ogni richiesta (tranne /health) deve avere l'header
  X-API-Key con questo valore
"""

import os
import secrets
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Security
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, ConfigDict, Field

from .export import ExportedModel

DEFAULT_MODEL_DIR = Path(__file__).resolve().parents[2] / "models" / "esportato"
MAX_BATCH_SIZE = 100
MAX_TEXT_LENGTH = 500_000  # l'articolo più lungo del dataset ha circa 200.000 caratteri

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


class Article(BaseModel):
    model_config = ConfigDict(json_schema_extra={"example": {
        "titolo": "Esempio",
        "testo": "The hospital opened a new oncology ward to treat patients with rare forms of cancer.",
    }})

    titolo: str | None = Field(default=None, max_length=1000, description="titolo dell'articolo (facoltativo)")
    testo: str = Field(min_length=1, max_length=MAX_TEXT_LENGTH, description="testo dell'articolo, in inglese")


class ArticleBatch(BaseModel):
    articoli: list[Article] = Field(min_length=1, max_length=MAX_BATCH_SIZE)


class Alternative(BaseModel):
    categoria: str
    probabilita: float


class Prediction(BaseModel):
    titolo: str | None = None
    categoria: str | None = Field(description="categoria prevista; null se il testo non contiene parole note al modello")
    probabilita: float | None = Field(description="probabilità stimata della categoria prevista")
    alternative: list[Alternative] = Field(description="le categorie più probabili, in ordine decrescente")
    parole_riconosciute: int = Field(description="parole del testo presenti nel vocabolario del modello")
    avviso: str | None = Field(default=None, description="presente quando la previsione è poco affidabile")


class BatchPrediction(BaseModel):
    previsioni: list[Prediction]


class ModelInfo(BaseModel):
    modello: str
    tipo: str
    categorie: list[str]
    parole_nel_vocabolario: int
    metriche_test: dict[str, float]
    data_addestramento: str
    versioni: dict[str, str]


# Soglie dell'avviso, ricavate dal test set. Il modello è addestrato su articoli completi
# (mediana di 219 parole riconosciute): troncando gli articoli alle prime 20 parole
# (circa 11 riconosciute) l'accuratezza scende al 67%, con 50 parole all'82%.
# Gli articoli con probabilità sotto 0,4 sono classificati correttamente al massimo nel 65% dei casi,
# quelli sopra 0,8 nel 95%.
MIN_KNOWN_WORDS = 20
MIN_PROBABILITY = 0.4


def _model_dir():
    return Path(os.environ.get("WIKIPEDIA_MODEL_DIR", DEFAULT_MODEL_DIR))


def _to_prediction(article, result):
    if result["parole_riconosciute"] == 0:
        return Prediction(
            titolo=article.titolo, categoria=None, probabilita=None, alternative=[], parole_riconosciute=0,
            avviso="Il testo non contiene parole del vocabolario del modello (il modello conosce solo testi in inglese).",
        )
    warnings = []
    if result["parole_riconosciute"] < MIN_KNOWN_WORDS:
        warnings.append(f"solo {result['parole_riconosciute']} parole riconosciute (il modello è addestrato su "
                        "articoli completi)")
    if result["probabilita"] < MIN_PROBABILITY:
        warnings.append(f"probabilità bassa ({result['probabilita']:.0%})")
    warning = f"Previsione poco affidabile: {'; '.join(warnings)}." if warnings else None
    return Prediction(titolo=article.titolo, avviso=warning, **result)


def create_app(model_dir=None, api_key=None):
    """Crea l'applicazione. Senza argomenti legge la configurazione dalle variabili d'ambiente."""

    @asynccontextmanager
    async def lifespan(app):
        # Il modello viene caricato una sola volta, all'avvio
        app.state.model = ExportedModel.load(model_dir or _model_dir())
        yield

    app = FastAPI(
        title="Classificazione degli articoli di Wikipedia",
        description=(
            "Prevede la categoria di un articolo in inglese tra quelle del dataset di Wikipedia "
            "(regressione logistica su TF-IDF, addestrata con Spark ML). Ogni previsione riporta le "
            "categorie più probabili e quante parole del testo il modello conosce."
        ),
        version="1.0.0",
        lifespan=lifespan,
    )
    expected_key = api_key if api_key is not None else os.environ.get("WIKIPEDIA_API_KEY")

    def check_api_key(key: str | None = Security(api_key_header)):
        if expected_key and not (key and secrets.compare_digest(key, expected_key)):
            raise HTTPException(status_code=401, detail="API key mancante o non valida")

    def predict(request: Request, articles: list[Article], top_k: int) -> list[Prediction]:
        model = request.app.state.model
        results = model.predict([a.testo for a in articles], top_k=min(top_k, len(model.labels)))
        return [_to_prediction(a, r) for a, r in zip(articles, results)]

    TopK = Query(3, ge=1, le=20, description="numero di categorie alternative da restituire")

    @app.get("/health", tags=["servizio"])
    def health():
        """Verifica che il servizio sia attivo."""
        return {"stato": "ok"}

    @app.get("/model", response_model=ModelInfo, tags=["modello"], dependencies=[Depends(check_api_key)])
    def model_info(request: Request):
        """Informazioni sul modello: categorie, dimensione del vocabolario, metriche sul test set e versioni."""
        return request.app.state.model.metadata

    @app.post("/predict", response_model=Prediction, tags=["previsioni"], dependencies=[Depends(check_api_key)])
    def predict_one(article: Article, request: Request, top_k: int = TopK):
        """Categoria di un singolo articolo."""
        return predict(request, [article], top_k)[0]

    @app.post("/predict/batch", response_model=BatchPrediction, tags=["previsioni"],
              dependencies=[Depends(check_api_key)])
    def predict_batch(batch: ArticleBatch, request: Request, top_k: int = TopK):
        """Categoria di più articoli in una sola richiesta (al massimo 100)."""
        return {"previsioni": predict(request, batch.articoli, top_k)}

    return app


app = create_app()
