import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from snowflake.snowpark.context import get_active_session
from snowflake.cortex import Complete
import json

st.set_page_config(page_title="IOC Copilot", page_icon="🤖", layout="wide")

@st.cache_resource
def get_session():
    return get_active_session()

session = get_session()

st.title("IOC Copilot")
st.markdown("AI-powered assistant for flight operations decision support")

if 'messages' not in st.session_state:
    st.session_state.messages = []
if 'selected_flight' not in st.session_state:
    st.session_state.selected_flight = None

col_left, col_right = st.columns([1, 1])

with col_left:
    st.subheader("Flight Selection")
    
    filter_col1, filter_col2, filter_col3 = st.columns(3)
    
    with filter_col1:
        stations = session.sql("""
            SELECT DISTINCT DEPARTURE_STATION 
            FROM IROP_GNN_RISK.IROP_MART.FLIGHT_RISK 
            WHERE FLIGHT_DATE = CURRENT_DATE
            ORDER BY 1
        """).to_pandas()
        selected_station = st.selectbox("Departure Station", ['All'] + stations['DEPARTURE_STATION'].tolist())
    
    with filter_col2:
        risk_filter = st.selectbox("Risk Band", ['All', 'High', 'Medium', 'Low'])
    
    with filter_col3:
        flag_filter = st.multiselect("Risk Flags", ['FDP Timeout', 'Curfew', 'MEL', 'Turn'])
    
    where_clauses = ["FLIGHT_DATE = CURRENT_DATE"]
    if selected_station != 'All':
        where_clauses.append(f"DEPARTURE_STATION = '{selected_station}'")
    if risk_filter != 'All':
        where_clauses.append(f"RISK_BAND = '{risk_filter}'")
    if 'FDP Timeout' in flag_filter:
        where_clauses.append("FDP_TIMEOUT_RISK_FLAG = TRUE")
    if 'Curfew' in flag_filter:
        where_clauses.append("CURFEW_RISK_FLAG = TRUE")
    if 'MEL' in flag_filter:
        where_clauses.append("MEL_RISK_FLAG = TRUE")
    if 'Turn' in flag_filter:
        where_clauses.append("TURN_RISK_FLAG = TRUE")
    
    where_sql = " AND ".join(where_clauses)
    
    flights_df = session.sql(f"""
        SELECT 
            FLIGHT_KEY,
            FLIGHT_NUMBER,
            DEPARTURE_STATION || '-' || ARRIVAL_STATION as ROUTE,
            ROUND(FLIGHT_RISK_SCORE_0_100, 0) as RISK,
            RISK_BAND,
            MISCONNECT_PAX_AT_RISK as PAX
        FROM IROP_GNN_RISK.IROP_MART.FLIGHT_RISK
        WHERE {where_sql}
        ORDER BY FLIGHT_RISK_SCORE_0_100 DESC
        LIMIT 50
    """).to_pandas()
    
    if not flights_df.empty:
        st.dataframe(
            flights_df,
            use_container_width=True,
            hide_index=True,
            selection_mode="single-row",
            on_select="rerun",
            key="flight_table"
        )
        
        if "flight_table" in st.session_state and st.session_state.flight_table.selection.rows:
            selected_idx = st.session_state.flight_table.selection.rows[0]
            st.session_state.selected_flight = flights_df.iloc[selected_idx]['FLIGHT_KEY']
    
    st.markdown("---")
    
    if st.session_state.selected_flight:
        st.subheader(f"Flight Detail: {st.session_state.selected_flight}")
        
        detail = session.sql(f"""
            SELECT *
            FROM IROP_GNN_RISK.IROP_MART.FLIGHT_RISK
            WHERE FLIGHT_KEY = '{st.session_state.selected_flight}'
        """).to_pandas()
        
        if not detail.empty:
            row = detail.iloc[0]
            
            metric_col1, metric_col2 = st.columns(2)
            with metric_col1:
                st.metric("Risk Score", f"{row['FLIGHT_RISK_SCORE_0_100']:.0f}/100")
                st.metric("Network Criticality", f"{row['GNN_NETWORK_CRITICALITY']:.0f}" if pd.notna(row['GNN_NETWORK_CRITICALITY']) else "N/A")
            with metric_col2:
                st.metric("Pax at Risk", f"{row['MISCONNECT_PAX_AT_RISK']:,}")
                st.metric("Revenue at Risk", f"${row['REVENUE_AT_RISK_USD']:,.0f}")
            
            st.markdown("**Risk Components:**")
            components = {
                'Crew Legality': row['CREW_LEGALITY_COMPONENT'],
                'Airport/Environment': row['AIRPORT_ENV_COMPONENT'],
                'Passenger Impact': row['PAX_COMPONENT'],
                'Maintenance': row['MAINTENANCE_COMPONENT']
            }
            
            fig = go.Figure(go.Bar(
                x=list(components.values()),
                y=list(components.keys()),
                orientation='h',
                marker_color=['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4']
            ))
            fig.update_layout(
                height=200,
                margin=dict(l=0, r=0, t=0, b=0),
                xaxis_title="Component Score"
            )
            st.plotly_chart(fig, use_container_width=True)
            
            st.markdown("**Active Risk Flags:**")
            flags = []
            if row['FDP_TIMEOUT_RISK_FLAG']:
                flags.append("🕐 Crew FDP Timeout")
            if row['CURFEW_RISK_FLAG']:
                flags.append("🌙 Curfew Risk")
            if row['MEL_RISK_FLAG']:
                flags.append("🔧 MEL Active")
            if row['TURN_RISK_FLAG']:
                flags.append("⏱️ Turn Risk")
            
            if flags:
                for flag in flags:
                    st.warning(flag)
            else:
                st.success("No critical flags active")

with col_right:
    st.subheader("Copilot Chat")
    
    tab1, tab2, tab3 = st.tabs(["IOC Analyst", "Ops & Policy", "What-If Simulator"])
    
    with tab1:
        st.markdown("Ask questions about flight data, risk metrics, and network status.")
        
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])
        
        if prompt := st.chat_input("Ask about flight risk or network status...", key="analyst_input"):
            st.session_state.messages.append({"role": "user", "content": prompt})
            with st.chat_message("user"):
                st.markdown(prompt)
            
            context = ""
            if st.session_state.selected_flight:
                context = f"Context: User has selected flight {st.session_state.selected_flight}. "
            
            full_prompt = f"""You are an IOC Flight Manager assistant. {context}
            
User question: {prompt}

Provide a helpful, data-driven response. If you need to query data, describe what you would query.
Focus on actionable insights for operations."""
            
            with st.chat_message("assistant"):
                with st.spinner("Thinking..."):
                    try:
                        response = Complete(
                            model='claude-3-5-sonnet',
                            prompt=full_prompt,
                            session=session
                        )
                        st.markdown(response)
                        st.session_state.messages.append({"role": "assistant", "content": response})
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
    
    with tab2:
        st.markdown("Search FAR 117, MEL procedures, curfew rules, and IROP playbooks.")
        
        policy_query = st.text_input("Search policies and regulations...", key="policy_search")
        
        if policy_query:
            with st.spinner("Searching..."):
                try:
                    results = session.sql(f"""
                        SELECT 
                            DOC_TYPE,
                            TITLE,
                            CONTENT,
                            STATION_CODE
                        FROM IROP_GNN_RISK.IROP_MART.POLICY_DOCUMENTS
                        WHERE CONTAINS(LOWER(CONTENT), LOWER('{policy_query}'))
                           OR CONTAINS(LOWER(TITLE), LOWER('{policy_query}'))
                        LIMIT 5
                    """).to_pandas()
                    
                    if not results.empty:
                        for _, doc in results.iterrows():
                            with st.expander(f"📄 {doc['TITLE']} ({doc['DOC_TYPE']})"):
                                st.markdown(doc['CONTENT'][:2000] + "..." if len(doc['CONTENT']) > 2000 else doc['CONTENT'])
                    else:
                        st.info("No matching documents found. Try different search terms.")
                except Exception as e:
                    st.error(f"Search error: {str(e)}")
    
    with tab3:
        st.markdown("Simulate recovery actions and see projected impact.")
        
        if st.session_state.selected_flight:
            st.info(f"Simulating for: **{st.session_state.selected_flight}**")
            
            sim_col1, sim_col2 = st.columns(2)
            
            with sim_col1:
                st.markdown("**Add Delay**")
                delay_minutes = st.slider("Delay (minutes)", 0, 120, 30, key="delay_slider")
                if st.button("Simulate Delay", key="sim_delay"):
                    with st.spinner("Running simulation..."):
                        try:
                            results = session.sql(f"""
                                SELECT * FROM TABLE(
                                    IROP_GNN_RISK.IROP_MART.SIMULATE_DELAY(
                                        '{st.session_state.selected_flight}',
                                        {delay_minutes}
                                    )
                                )
                            """).to_pandas()
                            
                            if not results.empty:
                                st.dataframe(results, use_container_width=True, hide_index=True)
                                total_delta_pax = results['DELTA_MISCONNECT_PAX'].sum()
                                total_delta_rev = results['DELTA_REVENUE_USD'].sum()
                                st.metric("Total Added Misconnect Pax", f"{total_delta_pax:+,.0f}")
                                st.metric("Total Added Revenue Risk", f"${total_delta_rev:+,.0f}")
                        except Exception as e:
                            st.error(f"Simulation error: {str(e)}")
            
            with sim_col2:
                st.markdown("**Assign Reserve Crew**")
                duty_id = st.text_input("Duty ID (for crew simulation)", key="duty_input")
                if st.button("Simulate Reserve Crew", key="sim_crew"):
                    if duty_id:
                        with st.spinner("Running simulation..."):
                            try:
                                results = session.sql(f"""
                                    SELECT * FROM TABLE(
                                        IROP_GNN_RISK.IROP_MART.SIMULATE_RESERVE_CREW(
                                            '{duty_id}'
                                        )
                                    )
                                """).to_pandas()
                                
                                if not results.empty:
                                    st.dataframe(results, use_container_width=True, hide_index=True)
                                else:
                                    st.info("No results returned")
                            except Exception as e:
                                st.error(f"Simulation error: {str(e)}")
                    else:
                        st.warning("Enter a Duty ID to simulate")
        else:
            st.warning("Select a flight from the table to run simulations.")

st.sidebar.markdown("---")
if st.sidebar.button("Clear Chat History"):
    st.session_state.messages = []
    st.rerun()
