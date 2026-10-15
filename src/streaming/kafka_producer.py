"""
Kafka News Producer.
Ingests news articles into Kafka topic 'news-articles'.
Supports both ad-hoc article publishing and continuous stream simulation from dataset.
"""

import json
import logging
import os
import random
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_INPUT_TOPIC,
    RAW_DATA_DIR,
    SAMPLE_DATA_DIR,
)

logger = logging.getLogger("NewsKafkaProducer")
logging.basicConfig(level=logging.INFO)

class NewsKafkaProducer:
    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self._init_producer()

    def _init_producer(self, retries: int = 2, delay: int = 1):
        """Initializes KafkaProducer with connection retry logic."""
        for i in range(retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers.split(","),
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    retries=1,
                    max_block_ms=2000,
                    request_timeout_ms=3000,
                )
                logger.info(f"Connected to Kafka broker at {self.bootstrap_servers}")
                return
            except (KafkaError, Exception) as e:
                logger.warning(
                    f"Kafka broker not available at {self.bootstrap_servers} ({e}). Retry {i+1}/{retries} in {delay}s..."
                )
                time.sleep(delay)
        logger.warning("Operating without active Kafka connection (will buffer locally or retry on demand).")

    def is_connected(self) -> bool:
        return self.producer is not None

    def publish_article(
        self,
        title: str,
        text: str,
        subject: str = "general",
        source: str = "manual_ingest",
        topic: str = KAFKA_INPUT_TOPIC,
    ) -> Dict[str, Any]:
        """
        Publishes a single article event into Kafka topic.
        """
        article_id = str(uuid.uuid4())[:8]
        event = {
            "article_id": article_id,
            "title": title,
            "text": text,
            "subject": subject,
            "source": source,
            "published_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
            "timestamp": time.time(),
        }

        if not self.is_connected():
            self._init_producer(retries=1)

        if self.is_connected():
            try:
                future = self.producer.send(topic, key=article_id, value=event)
                self.producer.flush(timeout=5)
                record_metadata = future.get(timeout=5)
                event["kafka_partition"] = record_metadata.partition
                event["kafka_offset"] = record_metadata.offset
                event["status"] = "published"
                logger.info(f"Published article {article_id} to {topic} [part={record_metadata.partition}, offset={record_metadata.offset}]")
                return event
            except Exception as e:
                logger.error(f"Kafka send error: {e}")
                event["status"] = "kafka_error"
                event["error"] = str(e)
                return event
        else:
            event["status"] = "broker_offline"
            return event

    def run_simulation(
        self,
        interval_seconds: float = 2.0,
        max_articles: Optional[int] = None,
        topic: str = KAFKA_INPUT_TOPIC,
    ):
        """
        Continuously reads articles from dataset and publishes them to Kafka.
        """
        logger.info("Starting Streaming News Simulator...")
        # Find data
        candidates = [
            (RAW_DATA_DIR / "True.csv", "Reuters / Ground Truth"),
            (RAW_DATA_DIR / "Fake.csv", "Disinformation Wire"),
            (SAMPLE_DATA_DIR / "True_sample.csv", "AP / Reuters"),
            (SAMPLE_DATA_DIR / "Fake_sample.csv", "Viral Conspiracy Feed"),
        ]

        articles_pool = []
        for path, src in candidates:
            if path.exists():
                try:
                    df = pd.read_csv(path, nrows=500)
                    for _, row in df.iterrows():
                        articles_pool.append({
                            "title": str(row.get("title", "")),
                            "text": str(row.get("text", "")),
                            "subject": str(row.get("subject", "general")),
                            "source": src,
                        })
                except Exception as e:
                    logger.warning(f"Could not load {path}: {e}")

        if not articles_pool:
            logger.error("No articles available for streaming simulation.")
            return

        random.shuffle(articles_pool)
        logger.info(f"Loaded {len(articles_pool)} articles for streaming.")

        count = 0
        idx = 0
        while True:
            if max_articles and count >= max_articles:
                break

            art = articles_pool[idx % len(articles_pool)]
            idx += 1
            count += 1

            self.publish_article(
                title=art["title"],
                text=art["text"],
                subject=art["subject"],
                source=art["source"],
                topic=topic,
            )
            time.sleep(interval_seconds)

if __name__ == "__main__":
    producer = NewsKafkaProducer()
    producer.run_simulation(interval_seconds=2.5)
