"""
FastAPI Backend Server for Fake News Classification Pipeline.
Exposes REST endpoints for article classification, Kafka ingestion,
HDFS storage inspection, metrics, and live SSE streaming to the dashboard.
"""

import asyncio
import json
import logging
import os
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

from src.config import (
    API_HOST,
    API_PORT,
    HDFS_BASE_DIR,
    HDFS_URL,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_INPUT_TOPIC,
    METRICS_FILE,
)
from src.ml.predictor import NewsPredictor
from src.ml.train import train_and_evaluate
from src.storage.hdfs_client import get_hdfs_client
from src.streaming.kafka_hdfs_consumer import (
    BUFFER_LOCK,
    LIVE_EVENT_BUFFER,
    NewsHDFSConsumer,
)
from src.streaming.kafka_producer import NewsKafkaProducer

logger = logging.getLogger("FakeNewsAPI")
logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="Fake News Classification Pipeline API",
    description="Kafka ingestion, HDFS storage, and TF-IDF NLP classification for real-time news credibility assessment.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global services
predictor = NewsPredictor()
hdfs_client = get_hdfs_client()
producer = NewsKafkaProducer()
consumer_service: Optional[NewsHDFSConsumer] = None

# Simulator control
simulator_thread: Optional[threading.Thread] = None
simulator_running = False

# Background consumer thread inside API container for standalone ease
def run_internal_consumer():
    global consumer_service
    try:
        consumer_service = NewsHDFSConsumer()
        consumer_service.start_consuming()
    except Exception as e:
        logger.warning(f"Internal consumer stopped: {e}")

# Pydantic Schemas
class ArticlePredictRequest(BaseModel):
    title: str = Field(..., example="Senate passes bipartisan infrastructure funding bill")
    text: str = Field(..., example="WASHINGTON (Reuters) - The U.S. Senate on Friday passed a $1 trillion bipartisan infrastructure bill.")
    subject: Optional[str] = Field("politics", example="politics")

class ArticleIngestRequest(BaseModel):
    title: str = Field(..., example="Viral Report Claims Moon is Made of Cheese")
    text: str = Field(..., example="Shocking revelations from anonymous sources reveal lunar secrets.")
    subject: Optional[str] = "general"
    source: Optional[str] = "api_client"

class RetrainRequest(BaseModel):
    limit_per_class: Optional[int] = Field(None, example=3000)

@app.on_event("startup")
def startup_event():
    # Start consumer in background thread
    t = threading.Thread(target=run_internal_consumer, daemon=True)
    t.start()
    logger.info("Started background Kafka consumer thread.")

@app.get("/api/health")
def get_health():
    """Checks the health of the API, ML model, Kafka, and HDFS."""
    hdfs_ok = hdfs_client.check_connection()
    kafka_ok = producer.is_connected()
    model_ok = predictor.is_ready()

    return {
        "status": "healthy" if model_ok else "degraded",
        "timestamp": time.time(),
        "services": {
            "model_ready": model_ok,
            "hdfs_connected": hdfs_ok,
            "hdfs_url": HDFS_URL,
            "kafka_connected": kafka_ok,
            "kafka_brokers": KAFKA_BOOTSTRAP_SERVERS,
            "simulator_running": simulator_running,
        },
    }

@app.post("/api/predict")
def predict_article(req: ArticlePredictRequest):
    """
    Labels an article as 'reliable' or 'unreliable' with confidence score and signal tokens.
    """
    result = predictor.predict(title=req.title, text=req.text)
    return result

@app.post("/api/ingest")
def ingest_article(req: ArticleIngestRequest):
    """
    Publishes an article into Kafka topic 'news-articles' and routes to HDFS + classification.
    """
    event = producer.publish_article(
        title=req.title,
        text=req.text,
        subject=req.subject or "general",
        source=req.source or "api_ingest",
    )

    # In case consumer is running co-located, immediately process as well to ensure instant feedback
    if consumer_service:
        classified = consumer_service.process_article(event)
        return {
            "status": "ingested_and_classified",
            "kafka_event": event,
            "classification": classified["classification"],
            "article_id": classified["article_id"],
        }

    # Fallback direct processing
    pred = predictor.predict(req.title, req.text)
    rec = {
        "article_id": event.get("article_id", f"art_{int(time.time()*1000)}"),
        "title": req.title,
        "text_snippet": (req.text[:250] + "...") if len(req.text) > 250 else req.text,
        "subject": req.subject,
        "source": req.source,
        "published_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "processed_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "classification": pred,
    }
    hdfs_client.write_record(rec)
    with BUFFER_LOCK:
        LIVE_EVENT_BUFFER.append(rec)

    return {
        "status": "ingested",
        "kafka_event": event,
        "classification": pred,
        "article_id": rec["article_id"],
    }

@app.get("/api/stream")
async def event_stream():
    """
    Server-Sent Events (SSE) endpoint providing real-time feed of classified articles.
    """
    async def event_generator():
        last_seen = 0
        while True:
            with BUFFER_LOCK:
                current_events = list(LIVE_EVENT_BUFFER)

            if len(current_events) > last_seen:
                for event in current_events[last_seen:]:
                    yield {
                        "event": "classified_article",
                        "data": json.dumps(event),
                    }
                last_seen = len(current_events)

            # Send a heartbeat every 3 seconds to keep connection alive
            yield {
                "event": "heartbeat",
                "data": json.dumps({"time": time.time(), "total_events": len(current_events)}),
            }
            await asyncio.sleep(1.5)

    return EventSourceResponse(event_generator())

@app.get("/api/hdfs/files")
def get_hdfs_files():
    """Lists files and partitions stored on Hadoop HDFS."""
    files = hdfs_client.list_files(HDFS_BASE_DIR)
    return {
        "base_dir": HDFS_BASE_DIR,
        "hdfs_connected": hdfs_client.is_connected,
        "total_files": len(files),
        "files": files,
    }

@app.get("/api/hdfs/content")
def get_hdfs_content(path: str = Query(..., description="File path on HDFS")):
    """Reads head/snippet of a file stored in HDFS."""
    snippet = hdfs_client.read_sample_content(path, max_bytes=8192)
    lines = [json.loads(line) for line in snippet.splitlines() if line.strip()]
    return {
        "path": path,
        "records_sample": lines[:20],
        "raw_snippet": snippet[:1000],
    }

@app.get("/api/metrics")
def get_metrics():
    """Fetches ML evaluation metrics, pipeline throughput, and classification counts."""
    metrics_data = {}
    if METRICS_FILE.exists():
        try:
            with open(METRICS_FILE, "r", encoding="utf-8") as f:
                metrics_data = json.load(f)
        except Exception:
            pass

    with BUFFER_LOCK:
        recent_events = list(LIVE_EVENT_BUFFER)

    reliable_count = sum(1 for e in recent_events if e.get("classification", {}).get("is_reliable", False))
    unreliable_count = len(recent_events) - reliable_count

    return {
        "model_metrics": metrics_data,
        "pipeline_stats": {
            "recent_stream_count": len(recent_events),
            "reliable_count": reliable_count,
            "unreliable_count": unreliable_count,
            "average_confidence": round(
                sum(e.get("classification", {}).get("confidence", 0.5) for e in recent_events)
                / max(len(recent_events), 1),
                4,
            ),
        },
    }

@app.post("/api/simulator/start")
def start_simulator(background_tasks: BackgroundTasks, interval: float = Query(2.0, ge=0.5, le=10.0)):
    """Starts simulated news stream into Kafka in background."""
    global simulator_running, simulator_thread
    if simulator_running:
        return {"status": "already_running"}

    simulator_running = True

    def run_sim():
        global simulator_running
        logger.info(f"Simulator thread started with interval {interval}s")
        producer.run_simulation(interval_seconds=interval)

    simulator_thread = threading.Thread(target=run_sim, daemon=True)
    simulator_thread.start()
    return {"status": "started", "interval_seconds": interval}

@app.post("/api/simulator/stop")
def stop_simulator():
    """Stops the simulated news stream."""
    global simulator_running
    simulator_running = False
    return {"status": "stopped"}

@app.post("/api/retrain")
def retrain_model(req: RetrainRequest, background_tasks: BackgroundTasks):
    """Triggers ML model retraining on full or sampled dataset."""
    def do_train():
        train_and_evaluate(limit_per_class=req.limit_per_class)
        predictor.load_model()

    background_tasks.add_task(do_train)
    return {"status": "retraining_started", "limit_per_class": req.limit_per_class}

# Mount static frontend directory
STATIC_DIR = Path(__file__).resolve().parent / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

@app.get("/", response_class=HTMLResponse)
def serve_index():
    index_file = STATIC_DIR / "index.html"
    if index_file.exists():
        with open(index_file, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse("<h1>Fake News Classification Pipeline API is Running!</h1><p>Visit <a href='/docs'>/docs</a> for Swagger UI.</p>")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.api.main:app", host=API_HOST, port=API_PORT, reload=False)
