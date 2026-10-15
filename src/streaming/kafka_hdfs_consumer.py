"""
Kafka-to-HDFS Stream Consumer & Classifier.
Consumes news articles from Kafka topic 'news-articles',
classifies them with TF-IDF ML model (reliable vs unreliable + confidence score),
persists enriched records to Hadoop HDFS, and forwards to 'classified-articles' topic.
"""

import collections
import json
import logging
import os
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from src.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CONSUMER_GROUP,
    KAFKA_INPUT_TOPIC,
    KAFKA_OUTPUT_TOPIC,
)
from src.ml.predictor import NewsPredictor
from src.storage.hdfs_client import get_hdfs_client

logger = logging.getLogger("NewsHDFSConsumer")
logging.basicConfig(level=logging.INFO)

# Global in-memory ring buffer for real-time frontend streaming
LIVE_EVENT_BUFFER = collections.deque(maxlen=100)
BUFFER_LOCK = threading.Lock()

class NewsHDFSConsumer:
    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        input_topic: str = KAFKA_INPUT_TOPIC,
        output_topic: str = KAFKA_OUTPUT_TOPIC,
        group_id: str = KAFKA_CONSUMER_GROUP,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.group_id = group_id

        self.predictor = NewsPredictor()
        self.hdfs_client = get_hdfs_client()
        self.consumer = None
        self.producer = None
        self.is_running = False

        self.total_processed = 0
        self.total_reliable = 0
        self.total_unreliable = 0

    def _init_connections(self, retries: int = 10, delay: int = 3):
        """Initializes Kafka consumer and producer with retries."""
        logger.info(f"Connecting to Kafka at {self.bootstrap_servers}...")
        for i in range(retries):
            try:
                self.consumer = KafkaConsumer(
                    self.input_topic,
                    bootstrap_servers=self.bootstrap_servers.split(","),
                    group_id=self.group_id,
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    consumer_timeout_ms=1000,
                )
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers.split(","),
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                )
                logger.info("Kafka consumer and downstream producer successfully initialized!")
                return True
            except KafkaError as e:
                logger.warning(f"Kafka brokers not available yet ({e}). Retrying {i+1}/{retries} in {delay}s...")
                time.sleep(delay)
            except Exception as e:
                logger.error(f"Consumer init error: {e}")
                time.sleep(delay)
        logger.warning("Could not establish Kafka consumer connection immediately.")
        return False

    def process_article(self, article_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Classifies the article, attaches confidence score, writes to HDFS,
        and returns enriched payload.
        """
        title = article_data.get("title", "")
        text = article_data.get("text", "")

        # Run NLP classification
        prediction = self.predictor.predict(title, text)

        enriched_record = {
            "article_id": article_data.get("article_id", f"art_{int(time.time()*1000)}"),
            "title": title,
            "text_snippet": (text[:250] + "...") if len(text) > 250 else text,
            "subject": article_data.get("subject", "general"),
            "source": article_data.get("source", "stream"),
            "published_at": article_data.get("published_at", datetime.utcnow().isoformat()),
            "processed_at": datetime.utcnow().isoformat(),
            "classification": {
                "label": prediction.get("label", "unreliable"),
                "is_reliable": prediction.get("is_reliable", False),
                "confidence": prediction.get("confidence", 0.50),
                "probabilities": prediction.get("probabilities", {}),
                "top_signals": prediction.get("top_signals", []),
            },
        }

        # Update metrics
        self.total_processed += 1
        if enriched_record["classification"]["is_reliable"]:
            self.total_reliable += 1
        else:
            self.total_unreliable += 1

        # Write to HDFS
        today = datetime.utcnow().strftime("%Y-%m-%d")
        self.hdfs_client.write_record(enriched_record, partition_date=today)

        # Store in circular in-memory buffer for instant live UI display
        with BUFFER_LOCK:
            LIVE_EVENT_BUFFER.append(enriched_record)

        # Forward to classified Kafka topic if connected
        if self.producer:
            try:
                self.producer.send(self.output_topic, value=enriched_record)
            except Exception as e:
                logger.debug(f"Could not forward to output topic: {e}")

        logger.info(
            f"Processed article: '{title[:40]}...' -> [{enriched_record['classification']['label'].upper()} - {enriched_record['classification']['confidence']*100:.1f}%]"
        )
        return enriched_record

    def start_consuming(self):
        """Starts main consumer loop."""
        self.is_running = True
        self._init_connections()

        logger.info(f"Starting consumer loop on topic '{self.input_topic}'...")
        while self.is_running:
            if not self.consumer:
                if not self._init_connections(retries=1):
                    time.sleep(3)
                    continue

            try:
                message_batch = self.consumer.poll(timeout_ms=1000)
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        try:
                            article_payload = message.value
                            self.process_article(article_payload)
                        except Exception as e:
                            logger.error(f"Error processing message: {e}")
            except Exception as e:
                logger.warning(f"Consumer loop notice: {e}")
                time.sleep(2)

    def stop(self):
        self.is_running = False
        if self.consumer:
            self.consumer.close()

if __name__ == "__main__":
    consumer = NewsHDFSConsumer()
    consumer.start_consuming()
