USE ROLE IDENTIFIER($IROP_GNN_RISK_ROLE);
USE DATABASE IDENTIFIER($IROP_GNN_RISK_DB);
USE SCHEMA IROP_MART;
USE WAREHOUSE IDENTIFIER($IROP_GNN_RISK_WH);

CREATE OR REPLACE TABLE POLICY_DOCUMENTS (
    doc_id VARCHAR(50) NOT NULL,
    doc_type VARCHAR(50) NOT NULL,
    station_code VARCHAR(10),
    fleet_type VARCHAR(20),
    title VARCHAR(200) NOT NULL,
    content VARCHAR(16000000) NOT NULL,
    effective_date DATE,
    last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (doc_id)
);

CREATE OR REPLACE CORTEX SEARCH SERVICE IROP_GNN_RISK_SEARCH_SVC
ON content
ATTRIBUTES doc_type, station_code, fleet_type, title
WAREHOUSE = IDENTIFIER($IROP_GNN_RISK_WH)
TARGET_LAG = '1 hour'
AS (
    SELECT 
        doc_id,
        doc_type,
        station_code,
        fleet_type,
        title,
        content
    FROM POLICY_DOCUMENTS
);

CREATE OR REPLACE CORTEX AGENT IROP_GNN_RISK_AGENT
PROMPT = $$ 
You are an IOC (Integrated Operations Center) copilot assistant for Delta Air Lines network operations.
Your role is to help IOC Flight Managers and Duty Managers make data-driven decisions about 
irregular operations (IROP) recovery.

You have access to the following capabilities:
1. STRUCTURED DATA QUERIES: Use the analyst tool to query flight risk scores, passenger impact,
   revenue at risk, and network criticality metrics.
2. POLICY LOOKUP: Use the search tool to find relevant regulations, SOPs, and playbooks including
   FAR Part 117 crew legality rules, MEL procedures, airport curfews, and IROP recovery guidelines.
3. SCENARIO SIMULATION: Use simulation functions to model the impact of interventions like
   adding delays, swapping tails, or assigning reserve crew.

When responding:
- Provide specific, actionable insights
- Cite relevant regulations or policies when applicable
- Quantify impact in terms of passengers affected and revenue at risk
- Suggest intervention options with trade-offs

For flight-related questions, always include:
- Current risk score and risk band
- Key risk drivers (crew, maintenance, weather, passenger)
- Downstream impact (affected connections, revenue exposure)
$$
TOOLS = (
    IROP_GNN_RISK_ANALYTICS USING (
        semantic_model_file => '@IROP_GNN_RISK.RAW.IROP_GNN_RISK_STAGE/semantic_model/irop_gnn_risk_semantic.yaml'
    ),
    IROP_GNN_RISK_POLICY_SEARCH USING (
        cortex_search_service => 'IROP_GNN_RISK.IROP_MART.IROP_GNN_RISK_SEARCH_SVC'
    )
)
WAREHOUSE = IDENTIFIER($IROP_GNN_RISK_WH);
