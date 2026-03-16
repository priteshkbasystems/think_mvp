from scripts.source_concordance import SourceConcordance


def main():

    engine = SourceConcordance()

    results = engine.compute()

    print("\nSource Sentiment Concordance:\n")

    for bank, data in results.items():

        print(f"\n{bank}")

        for source, score in data["sources"].items():

            print(f"{source}: {round(score,3)}")

        print("Concordance:", data["concordance_score"])