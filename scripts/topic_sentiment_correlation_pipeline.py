from scripts.topic_sentiment_correlation import TopicSentimentCorrelation


def main():

    engine = TopicSentimentCorrelation()

    results = engine.compute()

    print("\nTopic Sentiment Correlation")

    for bank, corr in results.items():
        print(f"{bank}: {corr}")