import pandas as pd
from sklearn.linear_model import LinearRegression

def predict_sentiment(df):

    future = []

    for bank in df["company"].unique():

        bank_df = df[df.company==bank]

        X = bank_df["year"].values.reshape(-1,1)
        y = bank_df["customer_sentiment"].values

        model = LinearRegression()
        model.fit(X,y)

        pred_2026 = model.predict([[2026]])[0]

        future.append({
            "bank": bank,
            "prediction": pred_2026
        })

    return pd.DataFrame(future)