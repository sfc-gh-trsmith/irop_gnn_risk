USE ROLE IDENTIFIER($IROP_GNN_RISK_ROLE);
USE DATABASE IDENTIFIER($IROP_GNN_RISK_DB);
USE SCHEMA IROP_MART;
USE WAREHOUSE IDENTIFIER($IROP_GNN_RISK_WH);

CREATE OR REPLACE FUNCTION SIMULATE_DELAY(flight_key STRING, delay_minutes INT)
RETURNS TABLE (
    affected_flight_key VARCHAR,
    original_risk_score FLOAT,
    new_risk_score FLOAT,
    delta_misconnect_pax INT,
    delta_revenue_usd FLOAT,
    impact_reason VARCHAR
)
LANGUAGE SQL
AS
$$
    WITH source_flight AS (
        SELECT 
            f.flight_key,
            f.tail_number,
            f.arrival_station,
            f.sched_arr_utc,
            DATEADD('minute', delay_minutes, f.sched_arr_utc) AS new_arr_utc
        FROM IROP_GNN_RISK.ATOMIC.FLIGHT_INSTANCE f
        WHERE f.flight_key = flight_key
    ),
    affected_downstream AS (
        SELECT 
            ar.next_flight_key AS affected_flight_key,
            fr.flight_risk_score_0_100 AS original_risk_score,
            LEAST(100, fr.flight_risk_score_0_100 + (delay_minutes * 2.5)) AS new_risk_score,
            FLOOR(fr.misconnect_pax_at_risk * (delay_minutes / 30.0)) AS delta_misconnect_pax,
            fr.revenue_at_risk_usd * (delay_minutes / 60.0) AS delta_revenue_usd,
            'Downstream turn impact from delayed inbound' AS impact_reason
        FROM source_flight sf
        JOIN IROP_GNN_RISK.ATOMIC.AIRCRAFT_ROTATION ar 
            ON ar.tail_number = sf.tail_number 
            AND ar.flight_key = sf.flight_key
        JOIN IROP_GNN_RISK.IROP_MART.FLIGHT_RISK fr 
            ON fr.flight_key = ar.next_flight_key
        WHERE ar.next_flight_key IS NOT NULL
        
        UNION ALL
        
        SELECT
            flight_key AS affected_flight_key,
            fr.flight_risk_score_0_100 AS original_risk_score,
            LEAST(100, fr.flight_risk_score_0_100 + (delay_minutes * 1.5)) AS new_risk_score,
            FLOOR(fr.misconnect_pax_at_risk * 0.3) AS delta_misconnect_pax,
            fr.revenue_at_risk_usd * 0.2 AS delta_revenue_usd,
            'Direct delay impact on source flight' AS impact_reason
        FROM IROP_GNN_RISK.IROP_MART.FLIGHT_RISK fr
        WHERE fr.flight_key = flight_key
    )
    SELECT * FROM affected_downstream
$$;

CREATE OR REPLACE FUNCTION SIMULATE_TAIL_SWAP(flight_key_a STRING, flight_key_b STRING)
RETURNS TABLE (
    affected_flight_key VARCHAR,
    original_risk_score FLOAT,
    new_risk_score FLOAT,
    delta_misconnect_pax INT,
    delta_revenue_usd FLOAT,
    swap_benefit VARCHAR
)
LANGUAGE SQL
AS
$$
    WITH swap_analysis AS (
        SELECT 
            fra.flight_key AS affected_flight_key,
            fra.flight_risk_score_0_100 AS original_risk_score,
            CASE 
                WHEN fra.mel_risk_flag = TRUE THEN fra.flight_risk_score_0_100 * 0.6
                ELSE fra.flight_risk_score_0_100
            END AS new_risk_score,
            CASE 
                WHEN fra.mel_risk_flag = TRUE THEN -FLOOR(fra.misconnect_pax_at_risk * 0.4)
                ELSE 0
            END AS delta_misconnect_pax,
            CASE 
                WHEN fra.mel_risk_flag = TRUE THEN -fra.revenue_at_risk_usd * 0.3
                ELSE 0
            END AS delta_revenue_usd,
            CASE 
                WHEN fra.mel_risk_flag = TRUE THEN 'MEL risk eliminated via tail swap'
                ELSE 'No significant benefit from swap'
            END AS swap_benefit
        FROM IROP_GNN_RISK.IROP_MART.FLIGHT_RISK fra
        WHERE fra.flight_key = flight_key_a
        
        UNION ALL
        
        SELECT 
            frb.flight_key AS affected_flight_key,
            frb.flight_risk_score_0_100 AS original_risk_score,
            frb.flight_risk_score_0_100 * 1.1 AS new_risk_score,
            FLOOR(frb.misconnect_pax_at_risk * 0.1) AS delta_misconnect_pax,
            frb.revenue_at_risk_usd * 0.1 AS delta_revenue_usd,
            'Potential increased risk from receiving swapped tail' AS swap_benefit
        FROM IROP_GNN_RISK.IROP_MART.FLIGHT_RISK frb
        WHERE frb.flight_key = flight_key_b
    )
    SELECT * FROM swap_analysis
$$;

CREATE OR REPLACE FUNCTION SIMULATE_RESERVE_CREW(duty_id STRING)
RETURNS TABLE (
    affected_flight_key VARCHAR,
    original_risk_score FLOAT,
    new_risk_score FLOAT,
    delta_misconnect_pax INT,
    delta_revenue_usd FLOAT,
    crew_action_result VARCHAR
)
LANGUAGE SQL
AS
$$
    WITH crew_duty AS (
        SELECT 
            cd.duty_id,
            cd.crew_timeout_risk_score,
            cd.fdp_remaining_minutes,
            cd.reserve_crew_eta_minutes
        FROM IROP_GNN_RISK.ATOMIC.CREW_DUTY_PERIOD cd
        WHERE cd.duty_id = duty_id
    ),
    affected_flights AS (
        SELECT 
            ca.flight_key,
            fr.flight_risk_score_0_100 AS original_risk_score,
            CASE 
                WHEN cd.crew_timeout_risk_score > 0.5 
                THEN GREATEST(10, fr.flight_risk_score_0_100 - 40)
                ELSE fr.flight_risk_score_0_100 - 10
            END AS new_risk_score,
            CASE 
                WHEN cd.crew_timeout_risk_score > 0.5 
                THEN -FLOOR(fr.misconnect_pax_at_risk * 0.6)
                ELSE -FLOOR(fr.misconnect_pax_at_risk * 0.1)
            END AS delta_misconnect_pax,
            CASE 
                WHEN cd.crew_timeout_risk_score > 0.5 
                THEN -fr.revenue_at_risk_usd * 0.5
                ELSE -fr.revenue_at_risk_usd * 0.1
            END AS delta_revenue_usd,
            CASE 
                WHEN cd.crew_timeout_risk_score > 0.5 
                THEN 'FDP timeout prevented by reserve crew assignment'
                ELSE 'Minor improvement from reserve crew buffer'
            END AS crew_action_result
        FROM crew_duty cd
        JOIN IROP_GNN_RISK.ATOMIC.CREW_ASSIGNMENT ca ON ca.duty_id = cd.duty_id
        JOIN IROP_GNN_RISK.IROP_MART.FLIGHT_RISK fr ON fr.flight_key = ca.flight_key
    )
    SELECT * FROM affected_flights
$$;
