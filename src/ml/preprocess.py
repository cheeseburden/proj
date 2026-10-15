"""
Text preprocessing and feature preparation for NLP fake news classification.
"""

import re
import string

def clean_text(text: str) -> str:
    """
    Cleans raw text by stripping HTML, URLs, non-alphanumeric noise,
    and normalizing whitespace.
    """
    if not isinstance(text, str):
        return ""

    # Convert to lowercase
    text = text.lower()

    # Remove URL links
    text = re.sub(r'https?://\S+|www\.\S+', ' ', text)

    # Remove HTML tags
    text = re.sub(r'<.*?>+', ' ', text)

    # Remove brackets, quotes, punctuation
    text = re.sub(r'\[.*?\]', ' ', text)
    text = re.sub(f'[{re.escape(string.punctuation)}]', ' ', text)

    # Remove newlines and extra spaces
    text = re.sub(r'\n+', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()

    return text

def prepare_article_text(title: str = "", text: str = "") -> str:
    """
    Combines headline title and body text into a unified representation.
    The title carries strong thematic weight in fake news detection.
    """
    clean_t = clean_text(title or "")
    clean_b = clean_text(text or "")

    # Give title double weight or concatenate directly
    if clean_t and clean_b:
        return f"{clean_t} {clean_t} {clean_b}"
    elif clean_t:
        return clean_t
    return clean_b
