from scripts.success_factor_detection import SuccessFactorDetection


def main():

    engine = SuccessFactorDetection()

    factors = engine.compute()

    print("\nTop Transformation Success Factors\n")

    for _, row in factors.iterrows():

        print(row["topic_id"], round(row["sentiment"],3))