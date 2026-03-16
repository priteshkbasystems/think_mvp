import numpy as np
from sklearn.linear_model import LinearRegression


class ScenarioSimulator:

    def __init__(self):
        print("Loading Scenario Simulator...")

    def simulate(self, narrative_scores, sentiment_scores):

        years = sorted(narrative_scores.keys())

        X = []
        y = []

        for year in years:

            if year in sentiment_scores:
                X.append([narrative_scores[year]])
                y.append(sentiment_scores[year])

        if len(X) < 2:
            return None

        model = LinearRegression()
        model.fit(X, y)

        def predict(new_narrative_score):
            return float(model.predict([[new_narrative_score]])[0])

        return predict