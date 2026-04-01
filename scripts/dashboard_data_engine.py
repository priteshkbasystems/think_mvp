import sqlite3
import os
import numpy as np
from collections import defaultdict
from scipy.stats import pearsonr
from sklearn.linear_model import LinearRegression

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"
FALLBACK_DB_PATHS = [
    "/content/think_mvp/transformation_cache.db",
    "transformation_cache.db",
]


def get_db_connection():
    
    conn = sqlite3.connect(DB_PATH)
    return conn


# ==========================================
# TOPIC SENTIMENT
# ==========================================

TOPIC_KEYWORDS = {
    "Mobile App": ["app","mobile","ui","interface"],
    "Login": ["login","otp","authentication"],
    "Payments": ["payment","transfer","transaction"],
    "Performance": ["slow","lag","crash"],
    "Fees": ["fee","charge","cost"]
}


def generate_topic_sentiment(cursor):

    cursor.execute("""
    SELECT bank_id, bank_name, review_text, sentiment_score
    FROM review_sentiments
    """)

    rows = cursor.fetchall()

    topic_data = defaultdict(lambda: {"count":0,"sentiment":0})

    for bank_id, bank, text, score in rows:

        text = text.lower()

        for topic,keywords in TOPIC_KEYWORDS.items():

            if any(k in text for k in keywords):

                topic_data[(bank_id, bank, topic)]["count"] += 1
                topic_data[(bank_id, bank, topic)]["sentiment"] += score


    for (bank_id, bank, topic), data in topic_data.items():

        cursor.execute("""
        INSERT INTO complaint_topics
        (bank_id, bank_name, topic_id, keywords, review_count)
        VALUES (?,?,?,?,?)
        """,(bank_id, bank, topic, topic, data["count"]))


# ==========================================
# CORRELATION
# ==========================================

def compute_correlation(cursor):

    cursor.execute("SELECT DISTINCT bank_id, bank_name FROM narrative_scores")
    banks=[(r[0], r[1]) for r in cursor.fetchall()]

    for bank_id, bank in banks:

        cursor.execute("""
        SELECT year, score FROM narrative_scores
        WHERE bank_id=?
        """,(bank_id,))
        narrative=dict(cursor.fetchall())

        cursor.execute("""
        SELECT year, sentiment FROM sentiment_scores
        WHERE bank_id=?
        """,(bank_id,))
        sentiment=dict(cursor.fetchall())

        years=set(narrative.keys()) & set(sentiment.keys())

        if len(years)<2:
            continue

        x=[narrative[y] for y in years]
        y=[sentiment[y] for y in years]

        r,_=pearsonr(x,y)

        cursor.execute("""
        INSERT OR REPLACE INTO narrative_sentiment_correlation
        (bank_id, bank_name, correlation)
        VALUES (?,?,?)
        """,(bank_id, bank, float(r)))


# ==========================================
# LAG DETECTION
# ==========================================

def compute_lag(cursor):

    cursor.execute("SELECT DISTINCT bank_id, bank_name FROM narrative_scores")
    banks=[(r[0], r[1]) for r in cursor.fetchall()]

    for bank_id, bank in banks:

        cursor.execute("""
        SELECT year, score FROM narrative_scores
        WHERE bank_id=?
        """,(bank_id,))
        narrative=dict(cursor.fetchall())

        cursor.execute("""
        SELECT year, sentiment FROM sentiment_scores
        WHERE bank_id=?
        """,(bank_id,))
        sentiment=dict(cursor.fetchall())

        lag_scores={}

        for lag in range(1,3):

            x=[]
            y=[]

            for year in narrative:

                if year+lag in sentiment:
                    x.append(narrative[year])
                    y.append(sentiment[year+lag])

            if len(x)>2:
                r,_=pearsonr(x,y)
                lag_scores[lag]=r

        if lag_scores:

            best_lag=max(lag_scores,key=lag_scores.get)

            cursor.execute("""
            INSERT OR REPLACE INTO narrative_lag
            (bank_id, bank_name, lag_months)
            VALUES (?,?,?)
            """,(bank_id, bank, best_lag*12))


# ==========================================
# SENTIMENT PREDICTION
# ==========================================

def generate_prediction(cursor):

    cursor.execute("SELECT DISTINCT bank_id, bank_name FROM sentiment_scores")
    banks=[(r[0], r[1]) for r in cursor.fetchall()]

    for bank_id, bank in banks:

        cursor.execute("""
        SELECT year,sentiment FROM sentiment_scores
        WHERE bank_id=?
        """,(bank_id,))

        rows=cursor.fetchall()

        if len(rows)<3:
            continue

        years=np.array([r[0] for r in rows]).reshape(-1,1)
        sentiments=np.array([r[1] for r in rows])

        model=LinearRegression()
        model.fit(years,sentiments)

        pred=model.predict([[2026]])[0]

        cursor.execute("""
        INSERT OR REPLACE INTO sentiment_predictions
        (bank_id, bank_name, year, predicted_sentiment)
        VALUES (?,?,?,?)
        """,(bank_id, bank, 2026, float(pred)))


# ==========================================
# NARRATIVE HIGHLIGHTS
# ==========================================

def generate_highlights(cursor):

    cursor.execute("""
    SELECT DISTINCT bank_id, bank_name, year, file_path
    FROM corporate_document_sentiment_rollup
    """)

    rows=cursor.fetchall()

    for bank_id, bank, year, _path in rows:
        if bank_id is None:
            continue

        highlight=f"Major digital initiative mentioned in {year} report"

        cursor.execute("""
        INSERT INTO narrative_highlights
        (bank_id, bank_name, year, highlight)
        VALUES (?,?,?,?)
        """,(bank_id, bank, year, highlight))


# ==========================================
# MAIN
# ==========================================

def main():

    conn=get_db_connection()
    cursor=conn.cursor()

    print("Generating Topic Sentiment")
    generate_topic_sentiment(cursor)

    print("Computing Narrative ↔ Sentiment Correlation")
    compute_correlation(cursor)

    print("Computing Narrative Lag")
    compute_lag(cursor)

    print("Generating AI Sentiment Predictions")
    generate_prediction(cursor)

    print("Generating Narrative Highlights")
    generate_highlights(cursor)

    conn.commit()
    # conn.close()

    print("Dashboard data generation completed")


if __name__=="__main__":
    main()