"""
Sentiment Utilities for AI Banking Intelligence Platform
-------------------------------------------------------

Handles:
1. Final sentiment score calculation
2. Rating normalization
3. Conflict correction (rating vs text)
4. Sentiment label classification
"""


def normalize_rating(rating):
    """
    Convert rating (1–5) to sentiment scale (-1 to +1)
    """
    if rating is None:
        return 0.0
    try:
        rating = float(rating)
    except (TypeError, ValueError):
        return 0.0
    if rating != rating:  # NaN
        return 0.0
    return (rating - 3) / 2


def calculate_final_sentiment(text_sentiment, rating):
    """
    Combine text sentiment with rating sentiment
    """

    if rating is None:
        rating_value = 3.0
    else:
        try:
            rating_value = float(rating)
        except (TypeError, ValueError):
            rating_value = 3.0
        if rating_value != rating_value:  # NaN
            rating_value = 3.0

    normalized_rating = normalize_rating(rating_value)

    # Fusion formula
    final_score = (
        0.7 * text_sentiment +
        0.3 * normalized_rating
    )

    # Correction rule
    # If rating is very positive but sentiment is negative
    if rating_value >= 4 and final_score < 0:
        final_score = abs(final_score) * 0.5

    # If rating is very negative but sentiment positive
    if rating_value <= 2 and final_score > 0:
        final_score = -abs(final_score) * 0.5

    # Clamp score range
    final_score = max(-1, min(1, final_score))

    return final_score


def sentiment_label(score):
    """
    Convert sentiment score to label
    """

    if score >= 0.15:
        return "Positive"

    elif score <= -0.15:
        return "Negative"

    else:
        return "Neutral"


def analyze_sentiment(text_sentiment, rating):
    """
    Full sentiment analysis pipeline

    Returns:
    final_score
    sentiment_label
    """

    final_score = calculate_final_sentiment(text_sentiment, rating)

    label = sentiment_label(final_score)

    return final_score, label