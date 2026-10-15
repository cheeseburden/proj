"""
Machine Learning & NLP package for Fake News Classification.
"""

from .preprocess import clean_text, prepare_article_text
from .predictor import NewsPredictor

__all__ = ["clean_text", "prepare_article_text", "NewsPredictor"]
