"""
Model training pipeline for Fake News Classification.
Uses TF-IDF + Logistic Regression and Naive Bayes to classify articles as reliable vs unreliable.
"""

import json
import os
import sys
import time
from pathlib import Path
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import joblib
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import MultinomialNB

from src.config import (
    MAX_FEATURES,
    METRICS_FILE,
    MODEL_FILE,
    NGRAM_RANGE,
    RAW_DATA_DIR,
    SAMPLE_DATA_DIR,
    VECTORIZER_FILE,
)
from src.ml.preprocess import prepare_article_text

def load_data(limit_per_class: int = None):
    """
    Loads True.csv and Fake.csv from raw directory or fallback sample directory.
    True = 1 (reliable), Fake = 0 (unreliable).
    """
    raw_true = RAW_DATA_DIR / "True.csv"
    raw_fake = RAW_DATA_DIR / "Fake.csv"

    sample_true = SAMPLE_DATA_DIR / "True_sample.csv"
    sample_fake = SAMPLE_DATA_DIR / "Fake_sample.csv"

    if raw_true.exists() and raw_fake.exists():
        print(f"Loading full dataset from {RAW_DATA_DIR}...")
        df_true = pd.read_csv(raw_true, nrows=limit_per_class)
        df_fake = pd.read_csv(raw_fake, nrows=limit_per_class)
    elif sample_true.exists() and sample_fake.exists():
        print(f"Loading sample dataset from {SAMPLE_DATA_DIR}...")
        df_true = pd.read_csv(sample_true, nrows=limit_per_class)
        df_fake = pd.read_csv(sample_fake, nrows=limit_per_class)
    else:
        raise FileNotFoundError(
            "Neither raw dataset nor sample dataset found. Run scripts/download_dataset.py first."
        )

    # Assign labels: 1 = reliable, 0 = unreliable
    df_true["label"] = 1
    df_fake["label"] = 0

    combined = pd.concat([df_true, df_fake], ignore_index=True)
    combined = combined.sample(frac=1.0, random_state=42).reset_index(drop=True)

    # Fill NaN values
    combined["title"] = combined["title"].fillna("")
    combined["text"] = combined["text"].fillna("")

    print(f"Loaded {len(combined)} total articles ({len(df_true)} reliable, {len(df_fake)} unreliable).")
    return combined

def train_and_evaluate(limit_per_class: int = None):
    start_time = time.time()
    print("=" * 60)
    print("Training Fake News Classification Pipeline (TF-IDF + ML)")
    print("=" * 60)

    df = load_data(limit_per_class=limit_per_class)

    print("Preprocessing text data (cleaning, normalising)...")
    texts = [
        prepare_article_text(t, b)
        for t, b in zip(df["title"], df["text"])
    ]
    labels = df["label"].values

    print(f"Splitting data into 80% train, 20% test...")
    X_train_raw, X_test_raw, y_train, y_test = train_test_split(
        texts, labels, test_size=0.2, random_state=42, stratify=labels
    )

    print(f"Extracting TF-IDF features (max_features={MAX_FEATURES}, ngram_range={NGRAM_RANGE})...")
    vectorizer = TfidfVectorizer(
        max_features=MAX_FEATURES,
        ngram_range=NGRAM_RANGE,
        stop_words="english",
        sublinear_tf=True,
    )
    X_train = vectorizer.fit_transform(X_train_raw)
    X_test = vectorizer.transform(X_test_raw)

    print("Training Logistic Regression Model...")
    lr_model = LogisticRegression(C=1.0, max_iter=1000, random_state=42, class_weight="balanced")
    lr_model.fit(X_train, y_train)

    lr_preds = lr_model.predict(X_test)
    lr_probs = lr_model.predict_proba(X_test)[:, 1]

    lr_acc = accuracy_score(y_test, lr_preds)
    lr_prec = precision_score(y_test, lr_preds)
    lr_rec = recall_score(y_test, lr_preds)
    lr_f1 = f1_score(y_test, lr_preds)
    lr_auc = roc_auc_score(y_test, lr_probs)
    lr_cm = confusion_matrix(y_test, lr_preds).tolist()

    print(f"Logistic Regression Results:")
    print(f"  Accuracy:  {lr_acc * 100:.2f}%")
    print(f"  Precision: {lr_prec * 100:.2f}%")
    print(f"  Recall:    {lr_rec * 100:.2f}%")
    print(f"  F1 Score:  {lr_f1 * 100:.2f}%")
    print(f"  ROC-AUC:   {lr_auc * 100:.2f}%")

    print("\nTraining Multinomial Naive Bayes Model...")
    nb_model = MultinomialNB(alpha=0.1)
    nb_model.fit(X_train, y_train)

    nb_preds = nb_model.predict(X_test)
    nb_probs = nb_model.predict_proba(X_test)[:, 1]

    nb_acc = accuracy_score(y_test, nb_preds)
    nb_prec = precision_score(y_test, nb_preds)
    nb_rec = recall_score(y_test, nb_preds)
    nb_f1 = f1_score(y_test, nb_preds)
    nb_auc = roc_auc_score(y_test, nb_probs)
    nb_cm = confusion_matrix(y_test, nb_preds).tolist()

    print(f"Naive Bayes Results:")
    print(f"  Accuracy:  {nb_acc * 100:.2f}%")
    print(f"  Precision: {nb_prec * 100:.2f}%")
    print(f"  Recall:    {nb_rec * 100:.2f}%")
    print(f"  F1 Score:  {nb_f1 * 100:.2f}%")
    print(f"  ROC-AUC:   {nb_auc * 100:.2f}%")

    # Select best model based on F1 score
    if lr_f1 >= nb_f1:
        best_model_name = "Logistic Regression"
        best_model = lr_model
        primary_metrics = {
            "accuracy": round(lr_acc, 4),
            "precision": round(lr_prec, 4),
            "recall": round(lr_rec, 4),
            "f1": round(lr_f1, 4),
            "roc_auc": round(lr_auc, 4),
            "confusion_matrix": lr_cm,
        }
    else:
        best_model_name = "Multinomial Naive Bayes"
        best_model = nb_model
        primary_metrics = {
            "accuracy": round(nb_acc, 4),
            "precision": round(nb_prec, 4),
            "recall": round(nb_rec, 4),
            "f1": round(nb_f1, 4),
            "roc_auc": round(nb_auc, 4),
            "confusion_matrix": nb_cm,
        }

    # Extract top influential vocabulary words
    feature_names = vectorizer.get_feature_names_out()
    lr_coefs = lr_model.coef_[0]
    top_reliable_idx = np.argsort(lr_coefs)[-15:][::-1]
    top_unreliable_idx = np.argsort(lr_coefs)[:15]

    top_reliable_features = [
        {"word": str(feature_names[i]), "weight": round(float(lr_coefs[i]), 3)}
        for i in top_reliable_idx
    ]
    top_unreliable_features = [
        {"word": str(feature_names[i]), "weight": round(float(abs(lr_coefs[i])), 3)}
        for i in top_unreliable_idx
    ]

    metrics_data = {
        "selected_model": best_model_name,
        "trained_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "dataset_size": len(df),
        "train_samples": len(y_train),
        "test_samples": len(y_test),
        "vocab_size": len(feature_names),
        "primary_metrics": primary_metrics,
        "models_comparison": {
            "logistic_regression": {
                "accuracy": round(lr_acc, 4),
                "precision": round(lr_prec, 4),
                "recall": round(lr_rec, 4),
                "f1": round(lr_f1, 4),
                "roc_auc": round(lr_auc, 4),
                "confusion_matrix": lr_cm,
            },
            "naive_bayes": {
                "accuracy": round(nb_acc, 4),
                "precision": round(nb_prec, 4),
                "recall": round(nb_rec, 4),
                "f1": round(nb_f1, 4),
                "roc_auc": round(nb_auc, 4),
                "confusion_matrix": nb_cm,
            },
        },
        "top_features": {
            "reliable_indicators": top_reliable_features,
            "unreliable_indicators": top_unreliable_features,
        },
        "duration_seconds": round(time.time() - start_time, 2),
    }

    # Save artifacts
    print(f"\nSaving model to {MODEL_FILE}...")
    joblib.dump(best_model, MODEL_FILE)

    print(f"Saving vectorizer to {VECTORIZER_FILE}...")
    joblib.dump(vectorizer, VECTORIZER_FILE)

    print(f"Saving metrics report to {METRICS_FILE}...")
    with open(METRICS_FILE, "w", encoding="utf-8") as f:
        json.dump(metrics_data, f, indent=2)

    print(f"\nTraining completed successfully in {metrics_data['duration_seconds']}s!")
    print(f"Best Model: {best_model_name} with Accuracy={primary_metrics['accuracy']*100:.2f}%")
    return metrics_data

if __name__ == "__main__":
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else None
    train_and_evaluate(limit_per_class=limit)
