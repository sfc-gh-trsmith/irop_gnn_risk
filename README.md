# Delta Network Disruption Intelligence Engine

Snowflake-native graph-based IROP risk and network impact engine for a hub-and-spoke airline, combining an ensemble of specialist ML models with a Heterogeneous GNN, Snowflake ML, Snowpark Container Services, and Cortex-powered IOC copilot.

## Problem Statement

Large hub-and-spoke airlines operate highly interconnected networks where flights, tails, crew, and passengers form a complex graph. IROP recovery relies on slow, leg-centric OR solvers and human intuition, leading to:
- Delayed recognition of cascading risks
- Suboptimal tail and crew swaps
- Missed curfews/slots and preventable misconnects
- Poor customer outcomes at mega-hubs like ATL and on premium international routes

## Solution Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         IROP_GNN_RISK Database                              │
├─────────────┬─────────────┬──────────────────┬─────────────────────────────┤
│    RAW      │   ATOMIC    │  ML_PROCESSING   │         IROP_MART           │
├─────────────┼─────────────┼──────────────────┼─────────────────────────────┤
│ Staged CSVs │ FLIGHTS     │ DELAY_PRED       │ FLIGHT_RISK (final scores)  │
│ Policy Docs │ AIRCRAFT    │ TURN_PRED        │                             │
│ Weather     │ CREW        │ CREW_TIMEOUT     │                             │
│             │ PNR         │ PNR_MISCONNECT   │                             │
│             │ AIRPORTS    │ AOG_RISK         │                             │
│             │             │ GNN_EMBEDDINGS   │                             │
└─────────────┴─────────────┴──────────────────┴─────────────────────────────┘
```

## ML Models

| Model | Target | Algorithm | Output Table |
|-------|--------|-----------|--------------|
| Delay Prediction | `delay_minutes` | XGBoost Regression | `ML_PROCESSING.DELAY_PREDICTIONS` |
| Turn Success | `turn_success_flag` | LightGBM Classifier | `ML_PROCESSING.TURN_PREDICTIONS` |
| Crew Timeout | `timeout_flag` | XGBoost Classifier | `ML_PROCESSING.CREW_TIMEOUT_PREDICTIONS` |
| PNR Misconnect | `misconnect_flag` | XGBoost Classifier | `ML_PROCESSING.PNR_MISCONNECT_PREDICTIONS` |
| AOG Risk | `aog_event_flag` | XGBoost Classifier | `ML_PROCESSING.AOG_RISK_PREDICTIONS` |
| Network Criticality | `network_impact` | PyTorch Geometric HGTConv (GPU) | `ML_PROCESSING.GNN_FLIGHT_EMBEDDINGS` |

## Cortex Intelligence Stack

- **Cortex Search**: RAG over FAR Part 117, MEL manuals, curfew rules, IROP playbooks
- **Cortex Analyst**: Natural language queries over flight risk metrics via semantic model
- **Cortex Agent**: IOC copilot combining structured queries, policy retrieval, and what-if simulation

## Project Structure

```
irop_gnn_risk/
├── deploy.sh                 # Infrastructure & data deployment
├── run.sh                    # Execute ML notebooks & refresh scores
├── clean.sh                  # Teardown all objects
├── sql/
│   ├── 01_database_setup.sql
│   ├── 02_raw_tables.sql
│   ├── 03_atomic_tables.sql
│   ├── 04_ml_processing_tables.sql
│   ├── 05_irop_mart_tables.sql
│   ├── 06_simulation_udfs.sql
│   └── 07_cortex_objects.sql
├── notebooks/
│   ├── 01_delay_prediction.ipynb
│   ├── 02_turn_success.ipynb
│   ├── 03_crew_timeout.ipynb
│   ├── 04_pnr_misconnect.ipynb
│   ├── 05_aog_risk.ipynb
│   └── 06_hgnn_network_criticality.ipynb
├── streamlit/
│   ├── Home.py
│   ├── pages/
│   │   ├── 1_Network_Overview.py
│   │   └── 2_IOC_Copilot.py
│   ├── snowflake.yml
│   └── environment.yml
├── semantic_model/
│   └── irop_gnn_risk_semantic.yaml
├── docs/                     # Policy documents for Cortex Search
│   ├── far_117_excerpts.md
│   ├── mel_manual_excerpts.md
│   ├── curfew_rules.md
│   └── irop_playbook.md
└── data/
    ├── generate_data.py      # Synthetic data generator
    └── *.csv                  # Generated demo data
```

## Quick Start

### Prerequisites
- Snowflake account with ACCOUNTADMIN or equivalent privileges
- `snow` CLI configured with connection named `demo`
- Python 3.8+ (for data generation)

### Deployment

```bash
# 1. Deploy infrastructure, load data, create Cortex objects
./deploy.sh

# 2. Execute ML notebooks and refresh risk scores
./run.sh main

# 3. Open Streamlit app in Snowsight
# Navigate to: Streamlit > IROP_GNN_RISK_APP
```

### Component-Only Deployment

```bash
./deploy.sh --only-sql        # SQL DDL only
./deploy.sh --only-data       # Data loading only
./deploy.sh --only-cortex     # Cortex objects only
./deploy.sh --only-streamlit  # Streamlit app only
./deploy.sh --only-notebooks  # Upload notebooks to stage
```

### Cleanup

```bash
./clean.sh                    # Remove all objects
```

## Demo Scenario

An EDCT delay hits an inbound JFK→ATL leg 15 minutes before departure. Within seconds:

1. **Network Recalculation**: The GNN recalculates risk and network criticality
2. **Risk Flip**: ATL→MCO connector changes from Medium (42) to High (81)
3. **Root Cause**: FAR Part 117 FDP timeout on the downline crew triggered by reduced turn buffer
4. **IOC Copilot**: User asks "Why did ATL→MCO just turn red?"
5. **Explanation**: "FDP timeout risk increased from 0.12 to 0.91 due to reduced turn buffer"
6. **Simulation**: Compare interventions (reserve crew vs. pre-cancel vs. tail swap)

## Target KPIs

- Reduce IROP-related misconnects by **15-20%**
- Network risk repricing in **< 60 seconds**
- Reduce IOC manual triage time by **50%**

## Snowflake Objects Created

| Object | Name |
|--------|------|
| Database | `IROP_GNN_RISK` |
| Warehouse | `IROP_GNN_RISK_WH` |
| Role | `IROP_GNN_RISK_ROLE` |
| Stage | `IROP_GNN_RISK.RAW.IROP_GNN_RISK_STAGE` |
| Cortex Search | `IROP_GNN_RISK_SEARCH_SVC` |
| Cortex Agent | `IROP_GNN_RISK_AGENT` |
| Streamlit App | `IROP_GNN_RISK_APP` |
| GPU Compute Pool | `IROP_GNN_RISK_GPU_POOL` |

## License

Internal Snowflake demo project.
