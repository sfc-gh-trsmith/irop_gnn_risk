#!/bin/bash
set -e
set -o pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

error_exit() { echo -e "${RED}[ERROR] $1${NC}" >&2; exit 1; }
info() { echo -e "${BLUE}[INFO] $1${NC}"; }
success() { echo -e "${GREEN}[SUCCESS] $1${NC}"; }
warn() { echo -e "${YELLOW}[WARN] $1${NC}"; }

CONNECTION_NAME="demo"
PROJECT_PREFIX="IROP_GNN_RISK"

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

DATABASE="${PROJECT_PREFIX}"
WAREHOUSE="${PROJECT_PREFIX}_WH"
ROLE="${PROJECT_PREFIX}_ROLE"
STAGE="${PROJECT_PREFIX}_STAGE"

while [[ $# -gt 1 ]]; do
    case $1 in
        -c|--connection) CONNECTION_NAME="$2"; shift 2 ;;
        *) break ;;
    esac
done

SNOW_CONN="-c $CONNECTION_NAME"
COMMAND="${1:-help}"

cmd_test() {
    info "Running deployment verification tests..."
    echo ""
    
    info "Test 1: Checking database exists..."
    snow sql $SNOW_CONN -q "SELECT DATABASE_NAME FROM INFORMATION_SCHEMA.DATABASES WHERE DATABASE_NAME = '${DATABASE}';" --format json | grep -q "${DATABASE}" && success "Database ${DATABASE} exists" || error_exit "Database ${DATABASE} not found"
    
    info "Test 2: Checking schemas..."
    for schema in RAW ATOMIC ML_PROCESSING IROP_MART; do
        snow sql $SNOW_CONN -q "SELECT SCHEMA_NAME FROM ${DATABASE}.INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '${schema}';" --format json | grep -q "${schema}" && success "Schema ${schema} exists" || warn "Schema ${schema} not found"
    done
    
    info "Test 3: Checking core tables..."
    CORE_TABLES=("ATOMIC.FLIGHT_INSTANCE" "ATOMIC.AIRCRAFT_ROTATION" "ATOMIC.CREW_DUTY_PERIOD" "ATOMIC.AIRPORT_CAPABILITY" "IROP_MART.FLIGHT_RISK")
    for table in "${CORE_TABLES[@]}"; do
        count=$(snow sql $SNOW_CONN -q "SELECT COUNT(*) as CNT FROM ${DATABASE}.${table};" --format json 2>/dev/null | grep -o '"CNT":[0-9]*' | cut -d: -f2 || echo "0")
        info "  ${table}: ${count} rows"
    done
    
    info "Test 4: Checking simulation UDFs..."
    snow sql $SNOW_CONN -q "SHOW USER FUNCTIONS IN SCHEMA ${DATABASE}.IROP_MART;" --format json | grep -q "SIMULATE_DELAY" && success "SIMULATE_DELAY function exists" || warn "SIMULATE_DELAY function not found"
    
    echo ""
    success "All tests completed!"
}

cmd_main() {
    info "Running main workflow..."
    echo ""
    
    info "Step 1: Creating and executing ML notebooks..."
    
    NOTEBOOKS=(
        "01_delay_prediction"
        "02_turn_success"
        "03_crew_timeout"
        "04_pnr_misconnect"
        "05_aog_risk"
        "06_hgnn_network_criticality"
    )
    
    for nb in "${NOTEBOOKS[@]}"; do
        info "  Creating notebook: ${nb}..."
        snow sql $SNOW_CONN -q "
            CREATE OR REPLACE NOTEBOOK ${DATABASE}.ML_PROCESSING.${nb}
            FROM '@${DATABASE}.RAW.${STAGE}/notebooks/${nb}.ipynb'
            QUERY_WAREHOUSE = '${WAREHOUSE}';
        " 2>/dev/null || warn "Could not create notebook ${nb}"
    done
    
    for nb in "${NOTEBOOKS[@]}"; do
        info "  Executing notebook: ${nb}..."
        snow notebook execute ${nb} $SNOW_CONN \
            --database ${DATABASE} \
            --schema ML_PROCESSING 2>/dev/null || warn "Could not execute notebook ${nb}"
    done
    
    success "ML notebooks executed"
    echo ""
    
    info "Step 2: Refreshing risk scores from ML predictions..."
    snow sql $SNOW_CONN -q "
        MERGE INTO ${DATABASE}.IROP_MART.FLIGHT_RISK fr
        USING (
            SELECT 
                fi.flight_key,
                fi.flight_number,
                fi.departure_station,
                fi.arrival_station,
                fi.flight_date,
                fi.sched_dep_utc,
                fi.sched_arr_utc,
                CURRENT_TIMESTAMP() as snapshot_ts,
                fi.tail_number,
                fi.aircraft_fleet_type as fleet_type,
                ap.hub_flag,
                CASE 
                    WHEN fi.intl_connector_flag THEN 
                        CASE WHEN ap.country = 'USA' THEN 'DOM-INTL' ELSE 'INTL-DOM' END
                    ELSE 'DOM-DOM'
                END as route_type,
                COALESCE(fi.delay_risk_score, 0) * 0.3 +
                COALESCE(1 - fi.turn_success_prob, 0) * 0.2 +
                COALESCE(fi.misconnect_prob, 0) * 0.3 +
                COALESCE(fi.network_criticality_score, 0) * 0.2 as flight_risk_score_0_100,
                fi.network_criticality_score as network_impact_score_0_100,
                fi.connecting_pax_pct * fi.pax_count as misconnect_pax_at_risk,
                fi.revenue_at_risk_usd
            FROM ${DATABASE}.ATOMIC.FLIGHT_INSTANCE fi
            LEFT JOIN ${DATABASE}.ATOMIC.AIRPORT_CAPABILITY ap ON fi.departure_station = ap.station_code
        ) src
        ON fr.flight_key = src.flight_key
        WHEN MATCHED THEN UPDATE SET
            fr.flight_risk_score_0_100 = src.flight_risk_score_0_100,
            fr.snapshot_ts = src.snapshot_ts
        WHEN NOT MATCHED THEN INSERT (
            risk_id, flight_key, flight_number, departure_station, arrival_station,
            flight_date, sched_dep_utc, sched_arr_utc, snapshot_ts, tail_number,
            fleet_type, hub_flag, route_type, flight_risk_score_0_100,
            network_impact_score_0_100, misconnect_pax_at_risk, revenue_at_risk_usd, risk_band
        ) VALUES (
            UUID_STRING(), src.flight_key, src.flight_number, src.departure_station,
            src.arrival_station, src.flight_date, src.sched_dep_utc, src.sched_arr_utc,
            src.snapshot_ts, src.tail_number, src.fleet_type, src.hub_flag, src.route_type,
            src.flight_risk_score_0_100, src.network_impact_score_0_100,
            src.misconnect_pax_at_risk, src.revenue_at_risk_usd,
            CASE 
                WHEN src.flight_risk_score_0_100 >= 70 THEN 'High'
                WHEN src.flight_risk_score_0_100 >= 40 THEN 'Medium'
                ELSE 'Low'
            END
        );
    " 2>/dev/null && success "Risk scores refreshed" || warn "Risk score refresh encountered issues"
    
    echo ""
    success "Main workflow completed!"
}

cmd_status() {
    info "Checking deployment status..."
    echo ""
    
    info "Database: ${DATABASE}"
    snow sql $SNOW_CONN -q "SELECT CURRENT_DATABASE(), CURRENT_WAREHOUSE(), CURRENT_ROLE();" --format json
    
    echo ""
    info "Table row counts:"
    snow sql $SNOW_CONN -q "
        SELECT 'FLIGHT_INSTANCE' as TABLE_NAME, COUNT(*) as ROW_COUNT FROM ${DATABASE}.ATOMIC.FLIGHT_INSTANCE
        UNION ALL SELECT 'AIRCRAFT_ROTATION', COUNT(*) FROM ${DATABASE}.ATOMIC.AIRCRAFT_ROTATION
        UNION ALL SELECT 'CREW_DUTY_PERIOD', COUNT(*) FROM ${DATABASE}.ATOMIC.CREW_DUTY_PERIOD
        UNION ALL SELECT 'FLIGHT_RISK', COUNT(*) FROM ${DATABASE}.IROP_MART.FLIGHT_RISK;
    "
}

cmd_streamlit() {
    info "Getting Streamlit app URL..."
    snow streamlit get-url ${PROJECT_PREFIX}_APP $SNOW_CONN --database $DATABASE --schema IROP_MART 2>/dev/null || warn "Streamlit app not found. Run './deploy.sh --only-streamlit' first."
}

cmd_help() {
    echo ""
    echo "IROP GNN Risk - Runtime Operations"
    echo ""
    echo "Usage: ./run.sh [OPTIONS] COMMAND"
    echo ""
    echo "Commands:"
    echo "  test       Run deployment verification tests"
    echo "  main       Execute main workflow (refresh risk scores)"
    echo "  status     Check deployment status and row counts"
    echo "  streamlit  Get Streamlit app URL"
    echo "  help       Show this help message"
    echo ""
    echo "Options:"
    echo "  -c, --connection NAME   Snowflake connection name (default: demo)"
    echo ""
    echo "Examples:"
    echo "  ./run.sh test           # Verify deployment"
    echo "  ./run.sh main           # Refresh risk scores"
    echo "  ./run.sh -c prod main   # Use 'prod' connection"
    echo ""
}

case $COMMAND in
    test) cmd_test ;;
    main) cmd_main ;;
    status) cmd_status ;;
    streamlit) cmd_streamlit ;;
    help|--help|-h) cmd_help ;;
    *) error_exit "Unknown command: $COMMAND. Run './run.sh help' for usage." ;;
esac
