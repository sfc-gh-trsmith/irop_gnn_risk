USE ROLE IDENTIFIER($IROP_GNN_RISK_ROLE);
USE DATABASE IDENTIFIER($IROP_GNN_RISK_DB);
USE SCHEMA RAW;
USE WAREHOUSE IDENTIFIER($IROP_GNN_RISK_WH);

CREATE OR REPLACE TABLE WEATHER_ATC (
    record_id VARCHAR(50) NOT NULL,
    sector_id VARCHAR(20),
    station_code VARCHAR(10),
    valid_time_utc TIMESTAMP_NTZ NOT NULL,
    convective_index FLOAT,
    visibility_category VARCHAR(20),
    crosswind_knots FLOAT,
    icing_risk_index FLOAT,
    edct_delay_mean INT,
    holding_probability FLOAT,
    flow_program_flag BOOLEAN DEFAULT FALSE,
    airspace_capacity_index FLOAT,
    raw_payload VARIANT,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
