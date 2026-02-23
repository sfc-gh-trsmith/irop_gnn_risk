import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Model Diagnostics", page_icon="📊", layout="wide")

@st.cache_resource
def get_session():
    return get_active_session()

session = get_session()

st.title("Model Diagnostics")
st.markdown("ML model performance metrics and explainability for technical validation")

st.info("This page is designed for Data Scientists and ML Engineers to validate model behavior and inspect feature attributions.")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Feature Importance (SHAP Attribution)")
    
    shap_data = session.sql("""
        SELECT 
            f.key as feature,
            AVG(ABS(f.value::FLOAT)) as avg_importance
        FROM IROP_GNN_RISK.IROP_MART.FLIGHT_RISK,
             LATERAL FLATTEN(input => SHAP_ATTRIBUTION) f
        WHERE FLIGHT_DATE = CURRENT_DATE AND SHAP_ATTRIBUTION IS NOT NULL
        GROUP BY f.key
        ORDER BY avg_importance DESC
        LIMIT 15
    """).to_pandas()
    
    if not shap_data.empty:
        fig = px.bar(
            shap_data, 
            x='AVG_IMPORTANCE', 
            y='FEATURE', 
            orientation='h',
            color='AVG_IMPORTANCE',
            color_continuous_scale='RdYlGn_r'
        )
        fig.update_layout(
            height=400,
            margin=dict(l=0, r=0, t=10, b=0),
            yaxis={'categoryorder': 'total ascending'},
            showlegend=False,
            coloraxis_showscale=False
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No SHAP attribution data available. Ensure SHAP_ATTRIBUTION column is populated.")

with col2:
    st.subheader("GNN Embedding Space")
    
    embedding_data = session.sql("""
        SELECT 
            FLIGHT_KEY,
            RISK_BAND,
            GNN_EMBEDDING[0]::FLOAT as dim1,
            GNN_EMBEDDING[1]::FLOAT as dim2,
            ROUND(FLIGHT_RISK_SCORE_0_100, 1) as risk_score,
            MISCONNECT_PAX_AT_RISK as pax_at_risk
        FROM IROP_GNN_RISK.IROP_MART.FLIGHT_RISK
        WHERE FLIGHT_DATE = CURRENT_DATE AND GNN_EMBEDDING IS NOT NULL
        LIMIT 100
    """).to_pandas()
    
    if not embedding_data.empty:
        fig = px.scatter(
            embedding_data, 
            x='DIM1', 
            y='DIM2', 
            color='RISK_BAND',
            hover_data=['FLIGHT_KEY', 'RISK_SCORE', 'PAX_AT_RISK'],
            color_discrete_map={'High': '#FF4136', 'Medium': '#FF851B', 'Low': '#2ECC40'}
        )
        fig.update_layout(
            height=400,
            margin=dict(l=0, r=0, t=10, b=0),
            xaxis_title='Embedding Dimension 1',
            yaxis_title='Embedding Dimension 2'
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No GNN embedding data available. Ensure GNN_EMBEDDING column is populated.")

st.markdown("---")

st.subheader("Risk Score Distribution")

dist_data = session.sql("""
    SELECT 
        ROUND(FLIGHT_RISK_SCORE_0_100, 0) as risk_score,
        COUNT(*) as count
    FROM IROP_GNN_RISK.IROP_MART.FLIGHT_RISK
    WHERE FLIGHT_DATE = CURRENT_DATE
    GROUP BY ROUND(FLIGHT_RISK_SCORE_0_100, 0)
    ORDER BY risk_score
""").to_pandas()

if not dist_data.empty:
    fig = px.histogram(
        dist_data,
        x='RISK_SCORE',
        y='COUNT',
        nbins=20,
        color_discrete_sequence=['#4ECDC4']
    )
    fig.update_layout(
        height=250,
        margin=dict(l=0, r=0, t=10, b=0),
        xaxis_title='Risk Score',
        yaxis_title='Flight Count',
        bargap=0.1
    )
    fig.add_vline(x=40, line_dash="dash", line_color="orange", annotation_text="Medium threshold")
    fig.add_vline(x=70, line_dash="dash", line_color="red", annotation_text="High threshold")
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

st.subheader("Sample Predictions with Risk Drivers")

sample_predictions = session.sql("""
    SELECT 
        FLIGHT_KEY,
        FLIGHT_NUMBER,
        DEPARTURE_STATION || '-' || ARRIVAL_STATION as ROUTE,
        ROUND(FLIGHT_RISK_SCORE_0_100, 1) as RISK_SCORE,
        RISK_BAND,
        ROUND(CREW_LEGALITY_COMPONENT, 2) as CREW,
        ROUND(AIRPORT_ENV_COMPONENT, 2) as AIRPORT,
        ROUND(PAX_COMPONENT, 2) as PAX,
        ROUND(MAINTENANCE_COMPONENT, 2) as MAINT,
        ROUND(GNN_NETWORK_CRITICALITY, 1) as GNN_SCORE
    FROM IROP_GNN_RISK.IROP_MART.FLIGHT_RISK
    WHERE FLIGHT_DATE = CURRENT_DATE
    ORDER BY FLIGHT_RISK_SCORE_0_100 DESC
    LIMIT 20
""").to_pandas()

if not sample_predictions.empty:
    def highlight_risk(row):
        if row['RISK_BAND'] == 'High':
            return ['background-color: #ffcccc'] * len(row)
        elif row['RISK_BAND'] == 'Medium':
            return ['background-color: #fff3cd'] * len(row)
        return [''] * len(row)
    
    styled_df = sample_predictions.style.apply(highlight_risk, axis=1)
    st.dataframe(styled_df, use_container_width=True, hide_index=True)

st.markdown("---")

model_col1, model_col2, model_col3 = st.columns(3)

with model_col1:
    st.metric("Total Flights Scored", f"{len(sample_predictions) if not sample_predictions.empty else 0}")

with model_col2:
    if not embedding_data.empty:
        st.metric("Embeddings Available", f"{len(embedding_data)}")
    else:
        st.metric("Embeddings Available", "0")

with model_col3:
    if not shap_data.empty:
        st.metric("SHAP Features", f"{len(shap_data)}")
    else:
        st.metric("SHAP Features", "0")

st.caption("Model outputs refreshed in real-time from IROP_MART.FLIGHT_RISK")
