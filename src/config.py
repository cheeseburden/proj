"""
Configuration parameters for Fake News Pipeline.
Reads from environment variables with safe development defaults.
"""

import os
from pathlib import Path

# Base Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
SAMPLE_DATA_DIR = DATA_DIR / "sample"
MODELS_DIR = DATA_DIR / "models"
MODELS_DIR.mkdir(parents=True, exist_ok=True)

MODEL_FILE = MODELS_DIR / "fake_news_model.pkl"
VECTORIZER_FILE = MODELS_DIR / "tfidf_vectorizer.pkl"
METRICS_FILE = MODELS_DIR / "model_metrics.json"

# Kafka Settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "news-articles")
KAFKA_OUTPUT_TOPIC = os.getenv("KAFKA_OUTPUT_TOPIC", "classified-articles")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "fake-news-hdfs-consumer")

# HDFS Settings
# WebHDFS default port on Hadoop 3 is 9870
HDFS_URL = os.getenv("HDFS_URL", "http://hdfs-namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")
HDFS_BASE_DIR = os.getenv("HDFS_BASE_DIR", "/data/news")
# Local fallback storage path in case HDFS is undergoing safe mode initialization
LOCAL_BUFFER_DIR = DATA_DIR / "hdfs_buffer"
LOCAL_BUFFER_DIR.mkdir(parents=True, exist_ok=True)

# API Server Settings
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))

# Training & Classification Settings
MAX_FEATURES = int(os.getenv("MAX_FEATURES", "10000"))
NGRAM_RANGE = (1, 2)
MODEL_TYPE = os.getenv("MODEL_TYPE", "logistic_regression") # or 'naive_bayes'
CONFIDENCE_THRESHOLD = 0.50
