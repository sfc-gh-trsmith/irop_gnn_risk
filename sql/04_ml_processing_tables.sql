USE ROLE IDENTIFIER($IROP_GNN_RISK_ROLE);
USE DATABASE IDENTIFIER($IROP_GNN_RISK_DB);
USE SCHEMA ML_PROCESSING;
USE WAREHOUSE IDENTIFIER($IROP_GNN_RISK_WH);

CREATE OR REPLACE TABLE DELAY_PREDICTIONS (
    prediction_id VARCHAR(50) NOT NULL,
    flight_key VARCHAR(50) NOT NULL,
    snapshot_ts TIMESTAMP_NTZ NOT NULL,
    predicted_delay_minutes FLOAT,
    delay_risk_score FLOAT,
    model_version VARCHAR(20),
    feature_importance VARIANT,
    PRIMARY KEY (prediction_id)
);

CREATE OR REPLACE TABLE TURN_PREDICTIONS (
    prediction_id VARCHAR(50) NOT NULL,
    flight_key VARCHAR(50) NOT NULL,
    snapshot_ts TIMESTAMP_NTZ NOT NULL,
    turn_success_prob FLOAT,
    turn_risk_flags ARRAY,
    model_version VARCHAR(20),
    feature_importance VARIANT,
    PRIMARY KEY (prediction_id)
);

CREATE OR REPLACE TABLE CREW_TIMEOUT_PREDICTIONS (
    prediction_id VARCHAR(50) NOT NULL,
    duty_id VARCHAR(50) NOT NULL,
    snapshot_ts TIMESTAMP_NTZ NOT NULL,
    timeout_prob FLOAT,
    time_to_timeout_minutes INT,
    model_version VARCHAR(20),
    feature_importance VARIANT,
    PRIMARY KEY (prediction_id)
);

CREATE OR REPLACE TABLE PNR_MISCONNECT_PREDICTIONS (
    prediction_id VARCHAR(50) NOT NULL,
    pnr_id VARCHAR(20) NOT NULL,
    trip_id VARCHAR(50) NOT NULL,
    snapshot_ts TIMESTAMP_NTZ NOT NULL,
    pnr_misconnect_prob FLOAT,
    connection_leg_at_risk VARCHAR(50),
    model_version VARCHAR(20),
    feature_importance VARIANT,
    PRIMARY KEY (prediction_id)
);

CREATE OR REPLACE TABLE AOG_RISK_PREDICTIONS (
    prediction_id VARCHAR(50) NOT NULL,
    tail_number VARCHAR(10) NOT NULL,
    snapshot_ts TIMESTAMP_NTZ NOT NULL,
    aog_risk_score FLOAT,
    critical_mel_flag BOOLEAN DEFAULT FALSE,
    mel_narrative_summary VARCHAR(500),
    model_version VARCHAR(20),
    feature_importance VARIANT,
    PRIMARY KEY (prediction_id)
);

CREATE OR REPLACE TABLE GNN_FLIGHT_EMBEDDINGS (
    embedding_id VARCHAR(50) NOT NULL,
    flight_key VARCHAR(50) NOT NULL,
    tail_number VARCHAR(10),
    snapshot_ts TIMESTAMP_NTZ NOT NULL,
    gnn_embedding ARRAY,
    gnn_network_criticality FLOAT,
    attention_weights VARIANT,
    downline_legs_affected_count INT DEFAULT 0,
    model_version VARCHAR(20),
    PRIMARY KEY (embedding_id)
);
