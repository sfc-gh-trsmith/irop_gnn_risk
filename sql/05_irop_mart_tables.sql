USE ROLE IDENTIFIER($IROP_GNN_RISK_ROLE);
USE DATABASE IDENTIFIER($IROP_GNN_RISK_DB);
USE SCHEMA IROP_MART;
USE WAREHOUSE IDENTIFIER($IROP_GNN_RISK_WH);

CREATE OR REPLACE TABLE FLIGHT_RISK (
    risk_id VARCHAR(50) NOT NULL,
    flight_key VARCHAR(50) NOT NULL,
    flight_number VARCHAR(10) NOT NULL,
    departure_station VARCHAR(5) NOT NULL,
    arrival_station VARCHAR(5) NOT NULL,
    flight_date DATE NOT NULL,
    sched_dep_utc TIMESTAMP_NTZ NOT NULL,
    sched_arr_utc TIMESTAMP_NTZ NOT NULL,
    snapshot_ts TIMESTAMP_NTZ NOT NULL,
    tail_number VARCHAR(10),
    fleet_type VARCHAR(20),
    hub_flag BOOLEAN DEFAULT FALSE,
    route_type VARCHAR(20),
    flight_risk_score_0_100 FLOAT NOT NULL,
    network_impact_score_0_100 FLOAT DEFAULT 0,
    crew_legality_component FLOAT DEFAULT 0,
    airport_env_component FLOAT DEFAULT 0,
    pax_component FLOAT DEFAULT 0,
    maintenance_component FLOAT DEFAULT 0,
    gnn_network_criticality FLOAT DEFAULT 0,
    gnn_embedding ARRAY,
    downline_legs_affected_count INT DEFAULT 0,
    misconnect_pax_at_risk INT DEFAULT 0,
    revenue_at_risk_usd FLOAT DEFAULT 0,
    risk_band VARCHAR(10),
    network_impact_band VARCHAR(10),
    shap_attribution VARIANT,
    risk_drivers ARRAY,
    fdp_timeout_risk_flag BOOLEAN DEFAULT FALSE,
    curfew_risk_flag BOOLEAN DEFAULT FALSE,
    mel_risk_flag BOOLEAN DEFAULT FALSE,
    turn_risk_flag BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (risk_id)
);

CREATE OR REPLACE TABLE FLIGHT_RISK_HISTORY (
    history_id VARCHAR(50) NOT NULL,
    risk_id VARCHAR(50) NOT NULL,
    flight_key VARCHAR(50) NOT NULL,
    snapshot_ts TIMESTAMP_NTZ NOT NULL,
    flight_risk_score_0_100 FLOAT,
    network_impact_score_0_100 FLOAT,
    change_reason VARCHAR(200),
    PRIMARY KEY (history_id)
);

CREATE OR REPLACE TABLE SIMULATION_RESULTS (
    simulation_id VARCHAR(50) NOT NULL,
    simulation_type VARCHAR(30) NOT NULL,
    source_flight_key VARCHAR(50),
    target_flight_key VARCHAR(50),
    simulation_params VARIANT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    affected_flights ARRAY,
    total_delta_misconnect_pax INT,
    total_delta_revenue_usd FLOAT,
    recommendation_text VARCHAR(1000),
    PRIMARY KEY (simulation_id)
);
