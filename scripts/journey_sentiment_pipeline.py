from scripts.journey_sentiment import JourneySentiment


def main():

    engine = JourneySentiment()

    results = engine.compute()

    print("\nCustomer Journey Sentiment")

    for stage, score in results.items():
        print(f"{stage}: {score}")