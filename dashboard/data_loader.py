import json
import pandas as pd

TREND_JSON = "../04_Analysis_Output/bank_trend_data.json"

def load_sentiment_data():

    with open(TREND_JSON) as f:
        data = json.load(f)

    rows = []

    for bank, info in data.items():

        for year, score in info["yearly_sentiment"].items():

            rows.append({
                "company": bank,
                "year": int(year),
                "customer_sentiment": score
            })

    return pd.DataFrame(rows)