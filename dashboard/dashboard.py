# dashboard.py

import streamlit as st
from data_loader import load_sentiment_data
from charts import sentiment_timeline
from charts import correlation_heatmap
from charts import peer_scorecard

st.set_page_config(
    page_title="Bank Transformation Intelligence",
    layout="wide"
)

st.title("AI Banking Intelligence Platform")

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Overview",
    "Corporate Narrative",
    "Customer Sentiment",
    "Correlation",
    "Peer Comparison",
    "Predictions"
])

df = load_sentiment_data()

with tab1:
    st.header("Overview Scorecard")
    peer_scorecard(df)

with tab2:
    st.header("Corporate Narrative Analysis")

with tab3:
    st.header("Customer Sentiment Timeline")
    sentiment_timeline(df)

with tab4:
    st.header("Transformation Impact")
    correlation_heatmap()

with tab5:
    st.header("Peer Comparison")

with tab6:
    st.header("Future Sentiment Prediction")