# charts.py

import plotly.express as px
import streamlit as st
import plotly.graph_objects as go

# ==========================================
# SENTIMENT TIMELINE
# ==========================================

def sentiment_timeline(df):

    fig = px.line(
        df,
        x="year",
        y="customer_sentiment",
        color="company",
        markers=True,
        title="Customer Sentiment Trend (2020–2025)"
    )

    st.plotly_chart(fig, use_container_width=True)

# ==========================================
# CORRELATION HEATMAP
# ==========================================

def correlation_heatmap():

    import pandas as pd
    import plotly.express as px

    data = {
        "company": ["SCBX","KBank","KTB","BBL"],
        "app": [0.65,0.72,0.51,0.43],
        "payments": [0.52,0.68,0.47,0.40],
        "service": [0.31,0.55,0.44,0.36]
    }

    df = pd.DataFrame(data)

    fig = px.imshow(
        df.set_index("company"),
        color_continuous_scale="RdYlGn",
        title="Transformation → Customer Impact Correlation"
    )

    st.plotly_chart(fig, use_container_width=True)

# ==========================================
# PEER SCORECARD
# ==========================================

def peer_scorecard(df):

    summary = df.groupby("company")["customer_sentiment"].mean()

    for bank, score in summary.items():

        if score > 0.2:
            color = "green"
        elif score < -0.2:
            color = "red"
        else:
            color = "orange"

        st.metric(
            label=bank,
            value=round(score,3),
            delta="Trend Score"
        )

# ==========================================
# LAG ANALYSIS
# ==========================================

def lag_analysis():

    lag_data = {
        "bank": ["SCBX","KBank","KTB","BBL"],
        "lag_months": [6,4,8,10]
    }

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=lag_data["bank"],
        y=lag_data["lag_months"]
    ))

    fig.update_layout(
        title="Announcement → Customer Impact Lag"
    )

    st.plotly_chart(fig)