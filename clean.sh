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
GPU_POOL="${PROJECT_PREFIX}_GPU_POOL"
STAGE="${PROJECT_PREFIX}_STAGE"

while [[ $# -gt 0 ]]; do
    case $1 in
        -c|--connection) CONNECTION_NAME="$2"; shift 2 ;;
        -y|--yes) SKIP_CONFIRM=true; shift ;;
        -h|--help)
            echo "Usage: ./clean.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -c, --connection NAME   Snowflake connection name (default: demo)"
            echo "  -y, --yes               Skip confirmation prompt"
            echo "  -h, --help              Show this help message"
            exit 0
            ;;
        *) error_exit "Unknown option: $1" ;;
    esac
done

SNOW_CONN="-c $CONNECTION_NAME"

echo ""
echo "=============================================="
echo "  IROP GNN Risk - Clean Up"
echo "=============================================="
echo ""
warn "This will DELETE all resources:"
echo "  - Database: ${DATABASE}"
echo "  - Warehouse: ${WAREHOUSE}"
echo "  - Role: ${ROLE}"
echo "  - Compute Pool: ${GPU_POOL}"
echo "  - Cortex Search Service: ${DATABASE}.IROP_MART.IROP_GNN_RISK_SEARCH_SVC"
echo ""

if [ "$SKIP_CONFIRM" != "true" ]; then
    read -p "Are you sure you want to continue? (y/N): " confirm
    if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
        info "Cleanup cancelled."
        exit 0
    fi
fi

echo ""
info "Starting cleanup..."

info "Dropping Cortex Search Service..."
snow sql $SNOW_CONN -q "DROP CORTEX SEARCH SERVICE IF EXISTS ${DATABASE}.IROP_MART.IROP_GNN_RISK_SEARCH_SVC;" 2>/dev/null || true

info "Dropping Streamlit app..."
snow sql $SNOW_CONN -q "DROP STREAMLIT IF EXISTS ${DATABASE}.IROP_MART.${PROJECT_PREFIX}_APP;" 2>/dev/null || true

info "Dropping Compute Pool..."
snow sql $SNOW_CONN -q "ALTER COMPUTE POOL IF EXISTS ${GPU_POOL} STOP ALL;" 2>/dev/null || true
snow sql $SNOW_CONN -q "DROP COMPUTE POOL IF EXISTS ${GPU_POOL};" 2>/dev/null || true

info "Dropping Warehouse..."
snow sql $SNOW_CONN -q "DROP WAREHOUSE IF EXISTS ${WAREHOUSE};" 2>/dev/null || true

info "Dropping Database (cascades all schemas, tables, stages)..."
snow sql $SNOW_CONN -q "DROP DATABASE IF EXISTS ${DATABASE};" 2>/dev/null || true

info "Dropping Role..."
snow sql $SNOW_CONN -q "DROP ROLE IF EXISTS ${ROLE};" 2>/dev/null || true

echo ""
success "Cleanup complete!"
echo ""
info "All IROP_GNN_RISK resources have been removed."
echo ""
