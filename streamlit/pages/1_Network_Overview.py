import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import altair as alt
import pydeck as pdk
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Network Overview", page_icon="🌐", layout="wide")

@st.cache_resource
def get_session():
    return get_active_session()

session = get_session()

st.title("Network Overview")
st.markdown("Real-time network risk visualization and KPI monitoring")

st.subheader("Network KPIs by Hub")

hub_metrics = session.sql("""
    SELECT 
        DEPARTURE_STATION as hub,
        COUNT(*) as total_flights,
        COUNT_IF(FLIGHT_RISK_SCORE_0_100 >= 70) as high_risk,
        COUNT_IF(FLIGHT_RISK_SCORE_0_100 >= 40 AND FLIGHT_RISK_SCORE_0_100 < 70) as medium_risk,
        COUNT_IF(FLIGHT_RISK_SCORE_0_100 < 40) as low_risk,
        ROUND(AVG(FLIGHT_RISK_SCORE_0_100), 1) as avg_risk,
        SUM(MISCONNECT_PAX_AT_RISK) as total_pax_at_risk,
        ROUND(SUM(REVENUE_AT_RISK_USD), 0) as total_revenue_at_risk
    FROM IROP_GNN_RISK.IROP_MART.FLIGHT_RISK
    WHERE FLIGHT_DATE = CURRENT_DATE AND HUB_FLAG = TRUE
    GROUP BY DEPARTURE_STATION
    ORDER BY high_risk DESC
""").to_pandas()

if not hub_metrics.empty:
    cols = st.columns(len(hub_metrics))
    for idx, (_, row) in enumerate(hub_metrics.iterrows()):
        with cols[idx]:
            st.markdown(f"### {row['HUB']}")
            st.metric("Total Flights", row['TOTAL_FLIGHTS'])
            st.metric("High Risk", row['HIGH_RISK'], delta=f"Avg: {row['AVG_RISK']}")
            st.metric("Pax at Risk", f"{row['TOTAL_PAX_AT_RISK']:,}")
            st.metric("Revenue Risk", f"${row['TOTAL_REVENUE_AT_RISK']:,.0f}")

st.markdown("---")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Network Map")
    
    airport_coords = {
        'ATL': [33.6407, -84.4277], 'JFK': [40.6413, -73.7781],
        'DTW': [42.2124, -83.3534], 'LAX': [33.9416, -118.4085],
        'MCO': [28.4312, -81.3081], 'DFW': [32.8998, -97.0403],
        'SEA': [47.4502, -122.3088], 'SLC': [40.7899, -111.9791],
        'MSP': [44.8848, -93.2223], 'BOS': [42.3656, -71.0096],
        'MIA': [25.7959, -80.2870], 'SFO': [37.6213, -122.3790],
        'ORD': [41.9742, -87.9073], 'DEN': [39.8561, -104.6737],
        'LHR': [51.4700, -0.4543], 'CDG': [49.0097, 2.5479],
        'FRA': [50.0379, 8.5622], 'AMS': [52.3105, 4.7683],
        'NRT': [35.7720, 140.3929], 'ICN': [37.4602, 126.4407]
    }
    
    route_data = session.sql("""
        SELECT 
            DEPARTURE_STATION,
            ARRIVAL_STATION,
            COUNT(*) as flight_count,
            AVG(FLIGHT_RISK_SCORE_0_100) as avg_risk
        FROM IROP_GNN_RISK.IROP_MART.FLIGHT_RISK
        WHERE FLIGHT_DATE = CURRENT_DATE
        GROUP BY DEPARTURE_STATION, ARRIVAL_STATION
    """).to_pandas()
    
    if not route_data.empty:
        arc_data = []
        for _, row in route_data.iterrows():
            dep = row['DEPARTURE_STATION']
            arr = row['ARRIVAL_STATION']
            if dep in airport_coords and arr in airport_coords:
                risk = row['AVG_RISK']
                color = [255, 0, 0, 200] if risk >= 70 else [255, 165, 0, 200] if risk >= 40 else [0, 255, 0, 200]
                arc_data.append({
                    'source': airport_coords[dep],
                    'target': airport_coords[arr],
                    'count': row['FLIGHT_COUNT'],
                    'risk': risk,
                    'color': color
                })
        
        arc_layer = pdk.Layer(
            'ArcLayer',
            data=arc_data,
            get_source_position='source',
            get_target_position='target',
            get_source_color='color',
            get_target_color='color',
            get_width=2,
            pickable=True
        )
        
        airport_data = [{'name': k, 'coords': v} for k, v in airport_coords.items() if k in hub_metrics['HUB'].values]
        scatter_layer = pdk.Layer(
            'ScatterplotLayer',
            data=airport_data,
            get_position='coords',
            get_fill_color=[0, 100, 200, 200],
            get_radius=50000,
            pickable=True
        )
        
        view_state = pdk.ViewState(latitude=39.8, longitude=-98.5, zoom=3, pitch=30)
        
        st.pydeck_chart(pdk.Deck(
            layers=[arc_layer, scatter_layer],
            initial_view_state=view_state,
            map_style=None
        ))

with col2:
    st.subheader("Risk Distribution")
    
    risk_dist = session.sql("""
        SELECT 
            CASE 
                WHEN FLIGHT_RISK_SCORE_0_100 >= 70 THEN 'High (70-100)'
                WHEN FLIGHT_RISK_SCORE_0_100 >= 40 THEN 'Medium (40-69)'
                ELSE 'Low (0-39)'
            END as risk_category,
            COUNT(*) as flight_count
        FROM IROP_GNN_RISK.IROP_MART.FLIGHT_RISK
        WHERE FLIGHT_DATE = CURRENT_DATE
        GROUP BY risk_category
        ORDER BY risk_category
    """).to_pandas()
    
    if not risk_dist.empty:
        color_map = {'High (70-100)': '#FF4136', 'Medium (40-69)': '#FF851B', 'Low (0-39)': '#2ECC40'}
        fig = px.pie(
            risk_dist,
            values='FLIGHT_COUNT',
            names='RISK_CATEGORY',
            color='RISK_CATEGORY',
            color_discrete_map=color_map,
            hole=0.4
        )
        fig.update_layout(margin=dict(t=0, b=0, l=0, r=0))
        st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

st.subheader("Top 10 Network-Critical Flights")

top_flights = session.sql("""
    SELECT 
        FLIGHT_NUMBER,
        DEPARTURE_STATION,
        ARRIVAL_STATION,
        ROUND(FLIGHT_RISK_SCORE_0_100, 1) as RISK_SCORE,
        RISK_BAND,
        MISCONNECT_PAX_AT_RISK as PAX_AT_RISK,
        ROUND(REVENUE_AT_RISK_USD, 0) as REVENUE_AT_RISK,
        ROUND(GNN_NETWORK_CRITICALITY, 1) as NETWORK_CRITICALITY,
        DOWNLINE_LEGS_AFFECTED_COUNT as DOWNLINE_AFFECTED
    FROM IROP_GNN_RISK.IROP_MART.FLIGHT_RISK
    WHERE FLIGHT_DATE = CURRENT_DATE
    ORDER BY GNN_NETWORK_CRITICALITY DESC NULLS LAST
    LIMIT 10
""").to_pandas()

if not top_flights.empty:
    def highlight_risk(row):
        if row['RISK_BAND'] == 'High':
            return ['background-color: #ffcccc'] * len(row)
        elif row['RISK_BAND'] == 'Medium':
            return ['background-color: #fff3cd'] * len(row)
        return [''] * len(row)
    
    styled_df = top_flights.style.apply(highlight_risk, axis=1)
    st.dataframe(styled_df, use_container_width=True, hide_index=True)

st.markdown("---")

st.subheader("Hub-to-Hub Risk Heatmap")

hub_matrix = session.sql("""
    SELECT 
        DEPARTURE_STATION as origin,
        ARRIVAL_STATION as destination,
        ROUND(AVG(FLIGHT_RISK_SCORE_0_100), 1) as avg_risk
    FROM IROP_GNN_RISK.IROP_MART.FLIGHT_RISK
    WHERE FLIGHT_DATE = CURRENT_DATE
        AND HUB_FLAG = TRUE
    GROUP BY DEPARTURE_STATION, ARRIVAL_STATION
""").to_pandas()

if not hub_matrix.empty:
    chart = alt.Chart(hub_matrix).mark_rect().encode(
        x=alt.X('DESTINATION:N', title='Destination'),
        y=alt.Y('ORIGIN:N', title='Origin'),
        color=alt.Color('AVG_RISK:Q', scale=alt.Scale(scheme='redyellowgreen', reverse=True), title='Avg Risk'),
        tooltip=['ORIGIN', 'DESTINATION', 'AVG_RISK']
    ).properties(height=300)
    
    st.altair_chart(chart, use_container_width=True)
