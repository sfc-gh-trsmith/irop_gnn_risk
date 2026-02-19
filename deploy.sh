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
ENV_PREFIX=""
PROJECT_PREFIX="IROP_GNN_RISK"

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

SNOW_CONN="-c $CONNECTION_NAME"

if [ -n "$ENV_PREFIX" ]; then
    FULL_PREFIX="${ENV_PREFIX}_${PROJECT_PREFIX}"
else
    FULL_PREFIX="${PROJECT_PREFIX}"
fi

DATABASE="${FULL_PREFIX}"
WAREHOUSE="${FULL_PREFIX}_WH"
ROLE="${FULL_PREFIX}_ROLE"
STAGE="${FULL_PREFIX}_STAGE"

ONLY_COMPONENT=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --only-sql) ONLY_COMPONENT="sql"; shift ;;
        --only-data) ONLY_COMPONENT="data"; shift ;;
        --only-streamlit) ONLY_COMPONENT="streamlit"; shift ;;
        --only-notebooks) ONLY_COMPONENT="notebooks"; shift ;;
        --only-cortex) ONLY_COMPONENT="cortex"; shift ;;
        -c|--connection) CONNECTION_NAME="$2"; SNOW_CONN="-c $CONNECTION_NAME"; shift 2 ;;
        -h|--help)
            echo "Usage: ./deploy.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --only-sql        Deploy only database and tables"
            echo "  --only-data       Deploy only synthetic data"
            echo "  --only-streamlit  Deploy only Streamlit app"
            echo "  --only-notebooks  Deploy only notebooks"
            echo "  --only-cortex     Deploy only Cortex objects"
            echo "  -c, --connection  Snowflake connection name (default: demo)"
            echo "  -h, --help        Show this help message"
            exit 0
            ;;
        *) error_exit "Unknown option: $1" ;;
    esac
done

should_run_step() {
    local step_name="$1"
    [ -z "$ONLY_COMPONENT" ] && return 0
    case "$ONLY_COMPONENT" in
        sql) [[ "$step_name" == "sql" ]] ;;
        data) [[ "$step_name" == "data" ]] ;;
        streamlit) [[ "$step_name" == "streamlit" ]] ;;
        notebooks) [[ "$step_name" == "notebooks" ]] ;;
        cortex) [[ "$step_name" == "cortex" ]] ;;
        *) return 1 ;;
    esac
}

echo ""
echo "=============================================="
echo "  IROP GNN Risk - Deployment"
echo "=============================================="
echo ""
info "Database: $DATABASE"
info "Warehouse: $WAREHOUSE"
info "Connection: $CONNECTION_NAME"
echo ""

if should_run_step "sql"; then
    info "Step 1: Creating database and schemas..."
    
    snow sql $SNOW_CONN -q "CREATE ROLE IF NOT EXISTS ${ROLE};"
    snow sql $SNOW_CONN -q "GRANT ROLE ${ROLE} TO ROLE SYSADMIN;"
    
    CURRENT_USER=$(snow sql $SNOW_CONN -q "SELECT CURRENT_USER();" --format json 2>/dev/null | grep -o '"CURRENT_USER()":"[^"]*"' | cut -d'"' -f4)
    snow sql $SNOW_CONN -q "GRANT ROLE ${ROLE} TO USER ${CURRENT_USER};" 2>/dev/null || true
    
    {
        echo "SET IROP_GNN_RISK_DB = '${DATABASE}';"
        echo "SET IROP_GNN_RISK_WH = '${WAREHOUSE}';"
        echo "SET IROP_GNN_RISK_ROLE = '${ROLE}';"
        cat sql/01_database_setup.sql
    } | snow sql $SNOW_CONN -i
    
    success "Database and schemas created"

    info "Step 2: Creating RAW tables..."
    {
        echo "SET IROP_GNN_RISK_DB = '${DATABASE}';"
        echo "SET IROP_GNN_RISK_WH = '${WAREHOUSE}';"
        echo "SET IROP_GNN_RISK_ROLE = '${ROLE}';"
        cat sql/02_raw_tables.sql
    } | snow sql $SNOW_CONN -i
    success "RAW tables created"

    info "Step 3: Creating ATOMIC tables..."
    {
        echo "SET IROP_GNN_RISK_DB = '${DATABASE}';"
        echo "SET IROP_GNN_RISK_WH = '${WAREHOUSE}';"
        echo "SET IROP_GNN_RISK_ROLE = '${ROLE}';"
        cat sql/03_atomic_tables.sql
    } | snow sql $SNOW_CONN -i
    success "ATOMIC tables created"

    info "Step 4: Creating ML_PROCESSING tables..."
    {
        echo "SET IROP_GNN_RISK_DB = '${DATABASE}';"
        echo "SET IROP_GNN_RISK_WH = '${WAREHOUSE}';"
        echo "SET IROP_GNN_RISK_ROLE = '${ROLE}';"
        cat sql/04_ml_processing_tables.sql
    } | snow sql $SNOW_CONN -i
    success "ML_PROCESSING tables created"

    info "Step 5: Creating IROP_MART tables..."
    {
        echo "SET IROP_GNN_RISK_DB = '${DATABASE}';"
        echo "SET IROP_GNN_RISK_WH = '${WAREHOUSE}';"
        echo "SET IROP_GNN_RISK_ROLE = '${ROLE}';"
        cat sql/05_irop_mart_tables.sql
    } | snow sql $SNOW_CONN -i
    success "IROP_MART tables created"

    info "Step 6: Creating simulation UDFs..."
    {
        echo "SET IROP_GNN_RISK_DB = '${DATABASE}';"
        echo "SET IROP_GNN_RISK_WH = '${WAREHOUSE}';"
        echo "SET IROP_GNN_RISK_ROLE = '${ROLE}';"
        cat sql/06_simulation_udfs.sql
    } | snow sql $SNOW_CONN -i
    success "Simulation UDFs created"
fi

if should_run_step "data"; then
    info "Step 7: Loading synthetic data..."
    
    if [ -f "data/generate_data.py" ]; then
        info "Generating synthetic data..."
        python3 data/generate_data.py
    fi
    
    for csv_file in data/*.csv; do
        if [ -f "$csv_file" ]; then
            filename=$(basename "$csv_file" .csv)
            table_name=$(echo "$filename" | tr '[:lower:]' '[:upper:]')
            
            if [[ "$table_name" == "FLIGHTS" ]]; then
                schema="ATOMIC"
                table_name="FLIGHT_INSTANCE"
            elif [[ "$table_name" == "ROTATIONS" ]]; then
                schema="ATOMIC"
                table_name="AIRCRAFT_ROTATION"
            elif [[ "$table_name" == "CREW" ]]; then
                schema="ATOMIC"
                table_name="CREW_DUTY_PERIOD"
            elif [[ "$table_name" == "CREW_ASSIGNMENTS" ]]; then
                schema="ATOMIC"
                table_name="CREW_ASSIGNMENT"
            elif [[ "$table_name" == "PNR" ]]; then
                schema="ATOMIC"
                table_name="PNR_TRIP"
            elif [[ "$table_name" == "AIRPORTS" ]]; then
                schema="ATOMIC"
                table_name="AIRPORT_CAPABILITY"
            elif [[ "$table_name" == "WEATHER" ]]; then
                schema="ATOMIC"
                table_name="WEATHER_ATC"
            elif [[ "$table_name" == "FLIGHT_RISK" ]]; then
                schema="IROP_MART"
            elif [[ "$table_name" == "POLICY_DOCUMENTS" ]]; then
                schema="IROP_MART"
            else
                continue
            fi
            
            info "Loading $csv_file into ${DATABASE}.${schema}.${table_name}..."
            snow sql $SNOW_CONN -q "PUT file://${csv_file} @${DATABASE}.RAW.${STAGE}/data/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;" 2>/dev/null || true
            
            snow sql $SNOW_CONN -q "
                COPY INTO ${DATABASE}.${schema}.${table_name}
                FROM @${DATABASE}.RAW.${STAGE}/data/${filename}.csv
                FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1 NULL_IF = ('', 'NULL'))
                ON_ERROR = CONTINUE;
            " 2>/dev/null || warn "Could not load ${filename}.csv"
        fi
    done
    success "Synthetic data loaded"
fi

if should_run_step "cortex"; then
    info "Step 8: Creating Cortex objects..."
    {
        echo "SET IROP_GNN_RISK_DB = '${DATABASE}';"
        echo "SET IROP_GNN_RISK_WH = '${WAREHOUSE}';"
        echo "SET IROP_GNN_RISK_ROLE = '${ROLE}';"
        cat sql/07_cortex_objects.sql
    } | snow sql $SNOW_CONN -i 2>/dev/null || warn "Cortex Search service creation may require additional setup"
    success "Cortex objects created"
fi

if should_run_step "streamlit"; then
    info "Step 9: Deploying Streamlit app..."
    
    if [ -d "streamlit" ] && [ -f "streamlit/snowflake.yml" ]; then
        cd streamlit
        snow streamlit deploy $SNOW_CONN \
            --database $DATABASE \
            --schema IROP_MART \
            --role $ROLE \
            --replace 2>/dev/null || warn "Streamlit deployment may require manual setup"
        cd ..
        success "Streamlit app deployed"
    else
        warn "Streamlit directory not found or missing snowflake.yml"
    fi
fi

if should_run_step "notebooks"; then
    info "Step 10: Uploading notebooks to stage..."
    
    if [ -d "notebooks" ]; then
        for notebook in notebooks/*.ipynb; do
            if [ -f "$notebook" ]; then
                info "Uploading $notebook..."
                snow sql $SNOW_CONN -q "PUT file://${notebook} @${DATABASE}.RAW.${STAGE}/notebooks/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;" 2>/dev/null || true
            fi
        done
        success "Notebooks uploaded"
    else
        warn "Notebooks directory not found"
    fi
fi

echo ""
echo "=============================================="
success "Deployment complete!"
echo "=============================================="
echo ""
info "Next steps:"
echo "  1. Run './run.sh test' to verify deployment"
echo "  2. Run './run.sh main' to execute the workflow"
echo "  3. Run './run.sh streamlit' to get the Streamlit app URL"
echo ""
