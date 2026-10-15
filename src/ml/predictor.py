"""
Inference predictor for news article credibility.
Provides prediction label, confidence score, probability distribution,
and top contributing words.
"""

import json
import sys
from pathlib import Path
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from typing import Any, Dict, List, Optional

import joblib
import numpy as np

from src.config import METRICS_FILE, MODEL_FILE, VECTORIZER_FILE
from src.ml.preprocess import clean_text, prepare_article_text

class NewsPredictor:
    def __init__(self):
        self.model = None
        self.vectorizer = None
        self.metrics = None
        self.load_model()

    def load_model(self) -> bool:
        """Loads serialized model, vectorizer, and evaluation metrics."""
        try:
            if MODEL_FILE.exists() and VECTORIZER_FILE.exists():
                self.model = joblib.load(MODEL_FILE)
                self.vectorizer = joblib.load(VECTORIZER_FILE)
                if METRICS_FILE.exists():
                    with open(METRICS_FILE, "r", encoding="utf-8") as f:
                        self.metrics = json.load(f)
                return True
        except Exception as e:
            print(f"Warning: Failed to load trained model artifacts: {e}")
        return False

    def is_ready(self) -> bool:
        return self.model is not None and self.vectorizer is not None

    def predict(self, title: str = "", text: str = "") -> Dict[str, Any]:
        """
        Classifies an article into 'reliable' or 'unreliable'
        with confidence score and explanatory signal tokens.
        """
        if not self.is_ready():
            # Try reloading in case training just finished
            if not self.load_model():
                return {
                    "error": "Model not loaded or trained yet. Trigger training first.",
                    "label": "unknown",
                    "confidence": 0.0,
                }

        combined_text = prepare_article_text(title, text)
        if not combined_text.strip():
            return {
                "label": "unreliable",
                "is_reliable": False,
                "confidence": 0.50,
                "probabilities": {"reliable": 0.50, "unreliable": 0.50},
                "status": "empty_input",
                "explanation": [],
            }

        tfidf_vec = self.vectorizer.transform([combined_text])

        # Probabilities: class 0 = unreliable (Fake), class 1 = reliable (True)
        if hasattr(self.model, "predict_proba"):
            probs = self.model.predict_proba(tfidf_vec)[0]
            prob_unreliable = float(probs[0])
            prob_reliable = float(probs[1])
        else:
            decision = self.model.decision_function(tfidf_vec)[0]
            prob_reliable = 1.0 / (1.0 + np.exp(-decision))
            prob_unreliable = 1.0 - prob_reliable

        is_reliable = bool(prob_reliable >= 0.50)
        label = "reliable" if is_reliable else "unreliable"
        confidence = prob_reliable if is_reliable else prob_unreliable

        # Extract salient tokens for explainability
        feature_names = self.vectorizer.get_feature_names_out()
        nonzero_indices = tfidf_vec.nonzero()[1]
        token_signals = []

        if hasattr(self.model, "coef_"):
            coefs = self.model.coef_[0]
            for idx in nonzero_indices:
                word = feature_names[idx]
                tfidf_val = tfidf_vec[0, idx]
                impact = coefs[idx] * tfidf_val
                token_signals.append({
                    "word": str(word),
                    "impact": round(float(impact), 4),
                    "indication": "reliable" if impact > 0 else "unreliable",
                })
            # Sort by absolute impact
            token_signals = sorted(token_signals, key=lambda x: abs(x["impact"]), reverse=True)[:10]

        return {
            "label": label,
            "is_reliable": is_reliable,
            "confidence": round(confidence, 4),
            "probabilities": {
                "reliable": round(prob_reliable, 4),
                "unreliable": round(prob_unreliable, 4),
            },
            "top_signals": token_signals,
            "word_count": len(combined_text.split()),
        }
