USE ROLE IDENTIFIER($IROP_GNN_RISK_ROLE);
USE DATABASE IDENTIFIER($IROP_GNN_RISK_DB);
USE SCHEMA ATOMIC;
USE WAREHOUSE IDENTIFIER($IROP_GNN_RISK_WH);

CREATE OR REPLACE TABLE FLIGHT_INSTANCE (
    flight_key VARCHAR(50) NOT NULL,
    flight_number VARCHAR(10) NOT NULL,
    departure_station VARCHAR(5) NOT NULL,
    arrival_station VARCHAR(5) NOT NULL,
    flight_date DATE NOT NULL,
    leg_id INT DEFAULT 1,
    sched_dep_utc TIMESTAMP_NTZ NOT NULL,
    sched_arr_utc TIMESTAMP_NTZ NOT NULL,
    act_dep_utc TIMESTAMP_NTZ,
    act_arr_utc TIMESTAMP_NTZ,
    turn_buffer_minutes INT,
    current_delay_departure INT DEFAULT 0,
    current_delay_arrival INT DEFAULT 0,
    gate_id VARCHAR(10),
    status VARCHAR(20) DEFAULT 'SCHEDULED',
    delay_codes ARRAY,
    block_time_minutes INT,
    tail_number VARCHAR(10),
    aircraft_fleet_type VARCHAR(20),
    pax_count INT DEFAULT 0,
    connecting_pax_pct FLOAT DEFAULT 0,
    elite_pax_count INT DEFAULT 0,
    intl_connector_flag BOOLEAN DEFAULT FALSE,
    revenue_at_risk_usd FLOAT DEFAULT 0,
    delay_risk_score FLOAT,
    turn_success_prob FLOAT,
    misconnect_prob FLOAT,
    network_criticality_score FLOAT,
    PRIMARY KEY (flight_key)
);

CREATE OR REPLACE TABLE AIRCRAFT_ROTATION (
    rotation_id VARCHAR(50) NOT NULL,
    tail_number VARCHAR(10) NOT NULL,
    flight_key VARCHAR(50) NOT NULL,
    flight_date DATE NOT NULL,
    sequence_position INT NOT NULL,
    prev_flight_key VARCHAR(50),
    next_flight_key VARCHAR(50),
    fleet_type VARCHAR(20) NOT NULL,
    aircraft_age_years FLOAT,
    owner_flag BOOLEAN DEFAULT TRUE,
    etops_capable_flag BOOLEAN DEFAULT FALSE,
    utilization_hours_24h FLOAT DEFAULT 0,
    overnight_location VARCHAR(5),
    next_maintenance_due_ts TIMESTAMP_NTZ,
    maintenance_station_flag BOOLEAN DEFAULT FALSE,
    mel_apu_flag BOOLEAN DEFAULT FALSE,
    mel_item_code VARCHAR(20),
    mel_severity VARCHAR(10),
    mel_expiry_ts TIMESTAMP_NTZ,
    aog_risk_score FLOAT DEFAULT 0,
    PRIMARY KEY (rotation_id),
    FOREIGN KEY (flight_key) REFERENCES FLIGHT_INSTANCE(flight_key)
);

CREATE OR REPLACE TABLE CREW_DUTY_PERIOD (
    duty_id VARCHAR(50) NOT NULL,
    pairing_id VARCHAR(30) NOT NULL,
    duty_date DATE NOT NULL,
    crew_base VARCHAR(5) NOT NULL,
    captain_id VARCHAR(20),
    fo_id VARCHAR(20),
    fa_count INT DEFAULT 0,
    report_time_utc TIMESTAMP_NTZ NOT NULL,
    scheduled_release_time_utc TIMESTAMP_NTZ NOT NULL,
    num_segments INT DEFAULT 1,
    augmented_crew_flag BOOLEAN DEFAULT FALSE,
    fdp_limit_minutes INT NOT NULL,
    fdp_time_used_minutes INT DEFAULT 0,
    fdp_remaining_minutes INT,
    rest_in_last_168_hours_minutes INT,
    time_zone_span_hours FLOAT DEFAULT 0,
    crew_timeout_risk_score FLOAT DEFAULT 0,
    reserve_crew_available_flag BOOLEAN DEFAULT FALSE,
    reserve_crew_eta_minutes INT,
    PRIMARY KEY (duty_id)
);

CREATE OR REPLACE TABLE CREW_ASSIGNMENT (
    assignment_id VARCHAR(50) NOT NULL,
    flight_key VARCHAR(50) NOT NULL,
    duty_id VARCHAR(50) NOT NULL,
    role VARCHAR(20) NOT NULL,
    leg_sequence_in_duty INT DEFAULT 1,
    PRIMARY KEY (assignment_id),
    FOREIGN KEY (flight_key) REFERENCES FLIGHT_INSTANCE(flight_key),
    FOREIGN KEY (duty_id) REFERENCES CREW_DUTY_PERIOD(duty_id)
);

CREATE OR REPLACE TABLE PNR_TRIP (
    pnr_id VARCHAR(20) NOT NULL,
    trip_id VARCHAR(50) NOT NULL,
    primary_customer_id VARCHAR(30),
    origin VARCHAR(5) NOT NULL,
    destination VARCHAR(5) NOT NULL,
    itinerary_flight_keys ARRAY NOT NULL,
    intl_flag BOOLEAN DEFAULT FALSE,
    group_size INT DEFAULT 1,
    elite_status_level VARCHAR(20),
    fare_class_bucket VARCHAR(5),
    rebook_flexibility_index FLOAT DEFAULT 0.5,
    loyalty_value_index FLOAT DEFAULT 0.5,
    estimated_voucher_cost_usd FLOAT DEFAULT 0,
    pnr_misconnect_prob FLOAT DEFAULT 0,
    pnr_reaccom_complexity_score FLOAT DEFAULT 0,
    PRIMARY KEY (trip_id)
);

CREATE OR REPLACE TABLE AIRPORT_CAPABILITY (
    station_code VARCHAR(5) NOT NULL,
    hub_flag BOOLEAN DEFAULT FALSE,
    country VARCHAR(50),
    region VARCHAR(50),
    runway_config VARCHAR(50),
    gate_count INT,
    widebody_gate_count INT,
    ground_start_cart_count INT,
    customs_capacity_per_hour INT,
    curfew_start_local TIME,
    curfew_end_local TIME,
    slot_controlled_flag BOOLEAN DEFAULT FALSE,
    mct_dom_dom_minutes INT DEFAULT 45,
    mct_dom_intl_minutes INT DEFAULT 90,
    mct_intl_dom_minutes INT DEFAULT 90,
    gdp_active_flag BOOLEAN DEFAULT FALSE,
    gdp_avg_delay_minutes INT DEFAULT 0,
    atc_congestion_index FLOAT DEFAULT 0,
    airport_disruption_index FLOAT DEFAULT 0,
    timezone_offset_utc INT DEFAULT 0,
    PRIMARY KEY (station_code)
);

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
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (record_id)
);
