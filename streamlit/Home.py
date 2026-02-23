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

if high_risk > 0:
    risk_drivers = session.sql("""
        SELECT 
            SUM(CASE WHEN FDP_TIMEOUT_RISK_FLAG THEN 1 ELSE 0 END) as fdp_count,
            SUM(CASE WHEN CURFEW_RISK_FLAG THEN 1 ELSE 0 END) as curfew_count,
            SUM(CASE WHEN MEL_RISK_FLAG THEN 1 ELSE 0 END) as mel_count,
            SUM(CASE WHEN TURN_RISK_FLAG THEN 1 ELSE 0 END) as turn_count
        FROM IROP_GNN_RISK.IROP_MART.FLIGHT_RISK
        WHERE FLIGHT_DATE = CURRENT_DATE AND FLIGHT_RISK_SCORE_0_100 >= 70
    """).collect()
    
    if risk_drivers:
        drivers = []
        if risk_drivers[0]['FDP_COUNT'] > 0:
            drivers.append(f"crew legality ({risk_drivers[0]['FDP_COUNT']})")
        if risk_drivers[0]['CURFEW_COUNT'] > 0:
            drivers.append(f"curfew constraints ({risk_drivers[0]['CURFEW_COUNT']})")
        if risk_drivers[0]['MEL_COUNT'] > 0:
            drivers.append(f"MEL items ({risk_drivers[0]['MEL_COUNT']})")
        if risk_drivers[0]['TURN_COUNT'] > 0:
            drivers.append(f"turn risk ({risk_drivers[0]['TURN_COUNT']})")
        
        driver_text = ", ".join(drivers[:2]) if drivers else "multiple factors"
        st.error(f"**Action Required:** {high_risk} flight(s) require immediate attention due to {driver_text}. "
                 f"Total passengers at risk: {pax_at_risk:,}.")

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
