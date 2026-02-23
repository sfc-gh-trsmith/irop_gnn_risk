import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from snowflake.snowpark.context import get_active_session
import json

st.set_page_config(page_title="IOC Copilot", page_icon="🤖", layout="wide")

@st.cache_resource
def get_session():
    return get_active_session()

session = get_session()

def cortex_complete(model: str, prompt: str) -> str:
    escaped_prompt = prompt.replace("'", "''")
    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', '{escaped_prompt}') as RESPONSE
    """).to_pandas()
    return result['RESPONSE'].iloc[0] if not result.empty else ""

def get_downstream_flights(flight_key: str, max_depth: int = 3) -> pd.DataFrame:
    return session.sql(f"""
        WITH RECURSIVE downstream AS (
            SELECT 
                ar.FLIGHT_KEY,
                ar.NEXT_FLIGHT_KEY,
                ar.TAIL_NUMBER,
                1 as depth
            FROM IROP_GNN_RISK.ATOMIC.AIRCRAFT_ROTATION ar
            WHERE ar.FLIGHT_KEY = '{flight_key}'
            
            UNION ALL
            
            SELECT 
                ar.FLIGHT_KEY,
                ar.NEXT_FLIGHT_KEY,
                ar.TAIL_NUMBER,
                d.depth + 1
            FROM IROP_GNN_RISK.ATOMIC.AIRCRAFT_ROTATION ar
            JOIN downstream d ON ar.FLIGHT_KEY = d.NEXT_FLIGHT_KEY
            WHERE d.depth < {max_depth} AND d.NEXT_FLIGHT_KEY IS NOT NULL
        )
        SELECT 
            d.FLIGHT_KEY,
            d.depth,
            fr.FLIGHT_NUMBER,
            fr.DEPARTURE_STATION,
            fr.ARRIVAL_STATION,
            ROUND(fr.FLIGHT_RISK_SCORE_0_100, 0) as RISK_SCORE,
            fr.MISCONNECT_PAX_AT_RISK as PAX_AT_RISK
        FROM downstream d
        JOIN IROP_GNN_RISK.IROP_MART.FLIGHT_RISK fr ON d.FLIGHT_KEY = fr.FLIGHT_KEY
        ORDER BY d.depth
    """).to_pandas()

st.title("IOC Copilot")
st.markdown("AI-powered assistant for flight operations decision support")

if 'messages' not in st.session_state:
    st.session_state.messages = []
if 'selected_flight' not in st.session_state:
    st.session_state.selected_flight = None

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
    high_risk_count = flights_df[flights_df['RISK_BAND'] == 'High'].shape[0]
    if high_risk_count > 0:
        st.error(f"**Alert:** {high_risk_count} high-risk flight(s) in current filter require immediate attention!")
    
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
        
        metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
        with metric_col1:
            st.metric("Risk Score", f"{row['FLIGHT_RISK_SCORE_0_100']:.0f}/100")
        with metric_col2:
            st.metric("Network Criticality", f"{row['GNN_NETWORK_CRITICALITY']:.0f}" if pd.notna(row['GNN_NETWORK_CRITICALITY']) else "N/A")
        with metric_col3:
            st.metric("Pax at Risk", f"{row['MISCONNECT_PAX_AT_RISK']:,}")
        with metric_col4:
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
        
        st.markdown("**Downstream Impact Chain:**")
        try:
            downstream = get_downstream_flights(st.session_state.selected_flight)
            if not downstream.empty and len(downstream) > 1:
                labels = [f"{r['DEPARTURE_STATION']}-{r['ARRIVAL_STATION']} ({r['FLIGHT_NUMBER']})" 
                          for _, r in downstream.iterrows()]
                sources = list(range(len(labels) - 1))
                targets = list(range(1, len(labels)))
                values = [max(1, int(r['PAX_AT_RISK'])) for _, r in downstream.iterrows()][:-1]
                colors = ['#FF4136' if r['RISK_SCORE'] >= 70 else '#FF851B' if r['RISK_SCORE'] >= 40 else '#2ECC40' 
                          for _, r in downstream.iterrows()]
                
                fig = go.Figure(go.Sankey(
                    node=dict(
                        label=labels,
                        pad=15,
                        thickness=20,
                        color=colors
                    ),
                    link=dict(
                        source=sources,
                        target=targets,
                        value=values,
                        color='rgba(150,150,150,0.4)'
                    )
                ))
                fig.update_layout(height=200, margin=dict(l=0, r=0, t=10, b=10))
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No downstream flights found in rotation.")
        except Exception as e:
            st.info(f"Downstream chain unavailable: {str(e)}")

st.markdown("---")

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
                    response = cortex_complete('claude-3-5-sonnet', full_prompt)
                    st.markdown(response)
                    st.session_state.messages.append({"role": "assistant", "content": response})
                except Exception as e:
                    st.error(f"Error: {str(e)}")

with tab2:
    st.markdown("Search FAR 117, MEL procedures, curfew rules, and IROP playbooks using semantic search.")
    
    policy_query = st.text_input("Search policies and regulations...", key="policy_search")
    
    if policy_query:
        with st.spinner("Searching with Cortex Search..."):
            try:
                escaped_query = policy_query.replace("'", "''")
                results = session.sql(f"""
                    SELECT 
                        DOC_TYPE,
                        TITLE,
                        CONTENT,
                        STATION_CODE
                    FROM TABLE(
                        SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                            '{escaped_query}',
                            'IROP_GNN_RISK.IROP_MART.IROP_GNN_RISK_SEARCH_SVC',
                            OBJECT_CONSTRUCT('columns', ARRAY_CONSTRUCT('DOC_TYPE', 'TITLE', 'CONTENT', 'STATION_CODE'), 'limit', 5)
                        )
                    )
                """).to_pandas()
                
                if not results.empty:
                    st.success(f"Found {len(results)} relevant document(s)")
                    for _, doc in results.iterrows():
                        with st.expander(f"📄 {doc['TITLE']} ({doc['DOC_TYPE']})"):
                            content = str(doc['CONTENT']) if doc['CONTENT'] else ""
                            st.markdown(content[:2000] + "..." if len(content) > 2000 else content)
                else:
                    st.info("No matching documents found. Try different search terms.")
            except Exception as e:
                st.error(f"Search error: {str(e)}")

with tab3:
    st.markdown("Simulate recovery actions and see projected impact.")
    
    if st.session_state.selected_flight:
        st.info(f"Simulating for: **{st.session_state.selected_flight}**")
        
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
        
        st.markdown("---")
        
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
        
        st.markdown("---")
        
        st.markdown("**Swap Tail with Another Flight**")
        try:
            available_flights = session.sql(f"""
                SELECT DISTINCT FLIGHT_KEY, FLIGHT_NUMBER, TAIL_NUMBER
                FROM IROP_GNN_RISK.IROP_MART.FLIGHT_RISK
                WHERE FLIGHT_DATE = CURRENT_DATE
                  AND FLIGHT_KEY != '{st.session_state.selected_flight}'
                  AND TAIL_NUMBER IS NOT NULL
                ORDER BY FLIGHT_NUMBER
                LIMIT 20
            """).to_pandas()
            
            if not available_flights.empty:
                swap_options = [f"{r['FLIGHT_NUMBER']} ({r['TAIL_NUMBER']})" 
                                for _, r in available_flights.iterrows()]
                selected_swap = st.selectbox("Swap tail with:", swap_options, key="swap_select")
                
                if st.button("Simulate Tail Swap", key="sim_tail"):
                    swap_flight_num = selected_swap.split(' ')[0]
                    swap_flight_key = available_flights[
                        available_flights['FLIGHT_NUMBER'] == swap_flight_num
                    ].iloc[0]['FLIGHT_KEY']
                    
                    with st.spinner("Running tail swap simulation..."):
                        try:
                            results = session.sql(f"""
                                SELECT * FROM TABLE(
                                    IROP_GNN_RISK.IROP_MART.SIMULATE_TAIL_SWAP(
                                        '{st.session_state.selected_flight}',
                                        '{swap_flight_key}'
                                    )
                                )
                            """).to_pandas()
                            
                            if not results.empty:
                                st.dataframe(results, use_container_width=True, hide_index=True)
                                total_delta_pax = results['DELTA_MISCONNECT_PAX'].sum()
                                total_delta_rev = results['DELTA_REVENUE_USD'].sum()
                                col1, col2 = st.columns(2)
                                with col1:
                                    st.metric("Delta Misconnect Pax", f"{total_delta_pax:+,.0f}")
                                with col2:
                                    st.metric("Delta Revenue Risk", f"${total_delta_rev:+,.0f}")
                                if 'SWAP_BENEFIT' in results.columns:
                                    st.info(f"Swap Assessment: {results.iloc[0]['SWAP_BENEFIT']}")
                        except Exception as e:
                            st.error(f"Simulation error: {str(e)}")
            else:
                st.info("No other flights available for tail swap.")
        except Exception as e:
            st.warning(f"Could not load swap options: {str(e)}")
    else:
        st.warning("Select a flight from the table to run simulations.")

st.sidebar.markdown("---")
if st.sidebar.button("Clear Chat History"):
    st.session_state.messages = []
    st.rerun()
