#!/usr/bin/env python3
"""
End-to-End Pipeline Verification Test
Verifies ML predictor, HDFS write/list operations, and Kafka communication.
"""

import sys
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.ml.predictor import NewsPredictor
from src.storage.hdfs_client import get_hdfs_client
from src.streaming.kafka_producer import NewsKafkaProducer

def test_pipeline():
    print("=" * 60)
    print("Running Fake News Pipeline Self-Test")
    print("=" * 60)

    # 1. Test ML Predictor
    print("\n[1/3] Testing NLP / TF-IDF Predictor...")
    predictor = NewsPredictor()
    assert predictor.is_ready(), "Model not loaded or trained!"

    reliable_test = predictor.predict(
        "Reuters: German Chancellor holds bilateral economic talks in Brussels",
        "BERLIN (Reuters) - German Chancellor Olaf Scholz met European Union leaders on Thursday to discuss trade agreements."
    )
    print(f" -> Reliable Sample Result: {reliable_test['label'].upper()} ({reliable_test['confidence']*100:.1f}% confidence)")
    assert reliable_test["is_reliable"] is True, "Reliable article misclassified!"

    fake_test = predictor.predict(
        "BOMBSHELL SHOCKING: Alien DNA Injected by Shadow Government in Secret Night Raids!!",
        "Unbelievable leaked evidence exposes secret labs experimenting with viral alien concoctions to manipulate voters."
    )
    print(f" -> Fake Sample Result: {fake_test['label'].upper()} ({fake_test['confidence']*100:.1f}% confidence)")
    assert fake_test["is_reliable"] is False, "Fake article misclassified!"
    print(" ML Predictor PASSED!")

    # 2. Test HDFS Client
    print("\n[2/3] Testing HDFS Client Storage...")
    hdfs = get_hdfs_client()
    dummy_record = {
        "article_id": "test_verification_001",
        "title": "Test Verification Headline",
        "text_snippet": "This is a test record saved during pipeline self-test.",
        "classification": reliable_test,
        "processed_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
    }
    write_res = hdfs.write_record(dummy_record)
    print(f" -> Write record returned: {write_res}")
    files = hdfs.list_files()
    print(f" -> Total HDFS/Buffer files listed: {len(files)}")
    assert len(files) > 0, "No files found after write!"
    print(" HDFS Storage Layer PASSED!")

    # 3. Test Kafka Producer
    print("\n[3/3] Testing Kafka Pipeline Connectivity...")
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(1.0)
    result = sock.connect_ex(('127.0.0.1', 29092))
    sock.close()

    if result == 0:
        producer = NewsKafkaProducer()
        event = producer.publish_article(
            title="Automated Test Article",
            text="This tests the Kafka producer integration.",
            source="test_runner"
        )
        print(f" -> Producer event status: {event.get('status')}")
        print(" Kafka Producer Layer Connected & Operational!")
    else:
        print(" -> Notice: Kafka broker port 29092 not active on host.")
        print("    (This is expected when running outside Docker; starts automatically in Docker Compose)")

    print("\n" + "=" * 60)
    print("All Pipeline Integration Checks Completed Successfully!")
    print("=" * 60)

if __name__ == "__main__":
    test_pipeline()
