from scripts.transformation_lag_analysis import TransformationLagAnalysis


def main():

    engine = TransformationLagAnalysis()

    results = engine.compute()

    print("\nTransformation Lag Analysis\n")

    for bank, data in results.items():

        print(bank, "Lag:", data["lag_years"], "years",
              "Correlation:", data["correlation"])