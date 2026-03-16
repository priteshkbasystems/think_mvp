from scripts.narrative_evolution_analysis import NarrativeEvolutionAnalysis


def main():

    engine = NarrativeEvolutionAnalysis()

    table, trends = engine.compute()

    print("\nTransformation Narrative Evolution\n")

    print(table)

    print("\nNarrative Trend Scores\n")

    for bank, trend in trends.items():

        print(bank, trend)