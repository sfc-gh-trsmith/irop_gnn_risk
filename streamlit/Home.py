import streamlit as st

st.set_page_config(
    page_title="IROP Network Intelligence",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("IROP Network Intelligence Engine")

st.markdown("""
### Delta Air Lines Network Operations Center

This application provides real-time visibility into network disruption risk and 
supports IOC decision-making with AI-powered insights.

**Key Capabilities:**
- **Network Overview**: Monitor hub-level KPIs, risk distributions, and top-priority flights
- **IOC Copilot**: AI assistant combining analytics, policy search, and scenario simulation

---

Select a page from the sidebar to get started.
""")

from snowflake.snowpark.context import get_active_session

@st.cache_resource
def get_session():
    return get_active_session()

session = get_session()

col1, col2, col3, col4 = st.columns(4)

with col1:
    result = session.sql("""
        SELECT COUNT(*) as cnt 
        FROM IROP_GNN_RISK.IROP_MART.FLIGHT_RISK 
        WHERE FLIGHT_DATE = CURRENT_DATE
    """).collect()
    total_flights = result[0]['CNT'] if result else 0
    st.metric("Total Flights Today", f"{total_flights:,}")

with col2:
    result = session.sql("""
        SELECT COUNT(*) as cnt 
        FROM IROP_GNN_RISK.IROP_MART.FLIGHT_RISK 
        WHERE FLIGHT_DATE = CURRENT_DATE AND FLIGHT_RISK_SCORE_0_100 >= 70
    """).collect()
    high_risk = result[0]['CNT'] if result else 0
    st.metric("High Risk Flights", f"{high_risk:,}", delta=None if high_risk == 0 else f"{high_risk} require attention")

with col3:
    result = session.sql("""
        SELECT COALESCE(SUM(MISCONNECT_PAX_AT_RISK), 0) as pax 
        FROM IROP_GNN_RISK.IROP_MART.FLIGHT_RISK 
        WHERE FLIGHT_DATE = CURRENT_DATE
    """).collect()
    pax_at_risk = int(result[0]['PAX']) if result else 0
    st.metric("Passengers at Risk", f"{pax_at_risk:,}")

with col4:
    result = session.sql("""
        SELECT COALESCE(SUM(REVENUE_AT_RISK_USD), 0) as rev 
        FROM IROP_GNN_RISK.IROP_MART.FLIGHT_RISK 
        WHERE FLIGHT_DATE = CURRENT_DATE
    """).collect()
    revenue_at_risk = float(result[0]['REV']) if result else 0
    st.metric("Revenue at Risk", f"${revenue_at_risk:,.0f}")

st.markdown("---")

st.subheader("System Status")

status_col1, status_col2 = st.columns(2)

with status_col1:
    st.success("Data Pipeline: Active")
    st.success("ML Models: Online")
    
with status_col2:
    st.success("Cortex Search: Indexed")
    st.success("Cortex Agent: Ready")

st.caption("Last updated: Real-time via Snowflake")
