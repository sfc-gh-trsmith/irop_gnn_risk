# Demo Requirements Document (DRD): Snowcore Airlines Network Disruption Intelligence Engine

GITHUB REPO NAME: delta_network_disruption_intel  
GITHUB REPO DESCRIPTION: Snowflake-native graph-based IROP risk and network impact engine for a hub-and-spoke airline, combining an ensemble of specialist ML models with a Heterogeneous GNN, Snowflake ML Feature Store, Snowpark Container Services, and Cortex-powered IOC copilot.

## 1. Strategic Overview

* **Problem Statement:**  
  Large hub-and-spoke airlines operate highly interconnected networks where flights, tails, crew, and passengers form a complex graph. Today, operational data is fragmented across dispatch, crew, maintenance, and passenger systems, and IROP recovery relies on slow, leg-centric OR solvers and human intuition. This leads to delayed recognition of cascading risks, suboptimal tail and crew swaps, missed curfews/slots, preventable misconnects, and poor customer outcomes—especially at mega-hubs like ATL and on premium international routes.

* **Target Business Goals (KPIs):**  
  * Reduce downstream IROP-related misconnects and pre-cancel events by **15–20%** on targeted banks (e.g., ATL evening peak, JFK/ATL–EU bank).  
  * Improve IOC decision latency so that **network risk reprices in < 60 seconds** after new ATC/ACARS events.  
  * Reduce manual triage time for IOC leads by **50%**, via a single network-criticality view and contrastive “what-if” explainability.  

* **The "Wow" Moment:**  
  In the demo, an EDCT delay hits an inbound JFK–ATL leg 15 minutes before departure. Within seconds, the Snowflake graph engine recalculates risk and network criticality: an ATL–MCO connector flips from Medium (42) to High (81) because the delay now triggers a hard FAR Part 117 FDP timeout on the downline crew. The IOC user asks Cortex: “Why did ATL–MCO just turn red, and what’s the best intervention?” Cortex responds with a precise explanation—“FDP timeout risk increased from 0.12 to 0.91 due to reduced turn buffer”—and simulates options (assign reserve crew vs. pre-cancel vs. tail swap), showing misconnect and revenue-at-risk deltas for each.

---

## 2. User Personas & Stories

| Persona Level | Role Title | Key User Story (Demo Flow) |
| :---- | :---- | :---- |
| **Strategic** | VP, System Operations Control / VP, Network Operations | "As a VP of Network Operations, I want to see a ranked list of flights with the highest **network criticality**—across ATL, JFK, DTW, MSP, LAX, SEA, SLC—so I can focus scarce resources on the interventions that minimize misconnects, preserve premium international banks, and keep us within curfew and crew-legal limits." |
| **Operational** | IOC Flight Manager / Duty Manager | "As an IOC Flight Manager, I want to receive real-time alerts when a seemingly small delay (e.g., +15 minutes EDCT or an APU MEL) causes a **legal crew timeout, curfew violation, or resource conflict** so I can instantly simulate tail swaps, reserve-crew activation, or pre-cancels and choose the least-disruptive recovery plan." |
| **Technical** | Data Scientist / Operations Research Engineer | "As a Data Scientist in the Operations Research team, I want to run ML and GNN models directly on unified flight, tail, crew, PNR, maintenance, and weather data in Snowflake, inspect feature attributions and embeddings, and export calibrated risk and impact scores that can feed our OR solvers and IOC tools without data egress." |

---

## 3. Data Architecture & Snowpark ML (Backend)

*Use the canonical data-layer pattern: `RAW` → `ATOMIC` → `ML_PROCESSING`/`DATA_SCIENCE` → `IROP_MART`.*

* **Structured Data (Inferred Schema):**  

  * `[FLIGHT_INSTANCE]` (Schema: `ATOMIC.FLIGHT_INSTANCE`)  
    * **Grain:** One row per `flight_number` + `flight_date` + `leg_id`.  
    * **Key Columns:**  
      * Identifiers: `flight_key`, `flight_number`, `departure_station`, `arrival_station`, `flight_date`, `leg_id`.  
      * Schedule/actuals: `sched_dep_utc`, `sched_arr_utc`, `act_dep_utc`, `act_arr_utc`, `turn_buffer_minutes`, `current_delay_departure`, `current_delay_arrival`, `gate_id`.  
      * Ops fields: `status` (scheduled/boarding/departed/cancelled), `delay_codes`, `block_time_minutes`, `tail_number`, `aircraft_fleet_type`.  
      * Commercial: `pax_count`, `%connecting_pax`, `elite_pax_count`, `intl_connector_flag`, `revenue_at_risk_usd`.  
      * Derived risk features (from base models): `delay_risk_score`, `turn_success_prob`, `misconnect_prob`, `network_criticality_score`.  

  * `[AIRCRAFT_ROTATION]` (Schema: `ATOMIC.AIRCRAFT_ROTATION`)  
    * **Grain:** One row per `tail_number` + `flight_date` + `sequence_position`.  
    * **Key Columns:**  
      * Identifiers: `rotation_id`, `tail_number`, `flight_key`, `sequence_position`, `prev_flight_key`, `next_flight_key`.  
      * Static: `fleet_type`, `aircraft_age_years`, `owner_flag`, `etops_capable_flag`.  
      * Dynamic: `utilization_hours_24h`, `overnight_location`, `next_maintenance_due_ts`, `maintenance_station_flag`.  
      * Maintenance risk: `mel_apu_flag`, `mel_item_code`, `mel_severity`, `mel_expiry_ts`, `aog_risk_score`.  

  * `[CREW_DUTY_PERIOD]` (Schema: `ATOMIC.CREW_DUTY_PERIOD`)  
    * **Grain:** One row per crew duty period per day (`pairing_id` + `duty_date`).  
    * **Key Columns:**  
      * Identifiers: `duty_id`, `pairing_id`, `crew_base`, `captain_id`, `fo_id`, `fa_count`.  
      * Duty structure: `report_time_utc`, `scheduled_release_time_utc`, `num_segments`, `augmented_crew_flag`.  
      * Legalities: `fdp_limit_minutes`, `fdp_time_used_minutes`, `fdp_remaining_minutes`, `rest_in_last_168_hours_minutes`, `time_zone_span_hours`.  
      * Derived: `crew_timeout_risk_score`, `reserve_crew_available_flag`, `reserve_crew_eta_minutes`.  

  * `[CREW_ASSIGNMENT]` (Schema: `ATOMIC.CREW_ASSIGNMENT`)  
    * **Grain:** One row per flight–duty-period relationship.  
    * **Key Columns:** `flight_key`, `duty_id`, `role` (cockpit/cabin), `leg_sequence_in_duty`.  

  * `[PNR_TRIP]` (Schema: `ATOMIC.PNR_TRIP`)  
    * **Grain:** One row per PNR + journey (OD pair).  
    * **Key Columns:**  
      * Identifiers: `pnr_id`, `trip_id`, `primary_customer_id`.  
      * Journey: `origin`, `destination`, `itinerary_flight_keys` (array), `intl_flag`, `group_size`, `elite_status_level`.  
      * Commercial/experience: `fare_class_bucket`, `rebook_flexibility_index`, `loyalty_value_index`, `estimated_voucher_cost_usd`.  
      * Risk outputs: `pnr_misconnect_prob`, `pnr_reaccom_complexity_score`.  

  * `[AIRPORT_CAPABILITY]` (Schema: `ATOMIC.AIRPORT_CAPABILITY`)  
    * **Grain:** One row per airport + day (or static attributes with SCD2).  
    * **Key Columns:**  
      * Identifiers: `station_code`, `hub_flag`, `country`, `region`.  
      * Infrastructure: `runway_config`, `gate_count`, `widebody_gate_count`, `ground_start_cart_count`, `customs_capacity_per_hour`.  
      * Constraints: `curfew_start_local`, `curfew_end_local`, `slot_controlled_flag`, `mct_dom_dom_minutes`, `mct_dom_intl_minutes`, `mct_intl_dom_minutes`.  
      * Dynamic ops: `gdp_active_flag`, `gdp_avg_delay_minutes`, `atc_congestion_index`, `airport_disruption_index`.  

  * `[WEATHER_ATC_FEED]` (Schema: `RAW.WEATHER_ATC` → `ATOMIC.WEATHER_ATC`)  
    * **Grain:** One row per `(station | sector | route_segment) + 5-minute interval`.  
    * **Key Columns:**  
      * Identifiers: `sector_id`, `station_code`, `valid_time_utc`.  
      * Weather: `convective_index`, `visibility_category`, `crosswind_knots`, `icing_risk_index`.  
      * ATC: `edct_delay_mean`, `holding_probability`, `flow_program_flag`, `airspace_capacity_index`.  

  * `[IROP_RISK_SCORES]` (Schema: `IROP_MART.FLIGHT_RISK`)  
    * **Grain:** One row per `flight_key` + snapshot time.  
    * **Key Columns:**  
      * Identifiers: `flight_key`, `snapshot_ts`.  
      * Composite scores: `flight_risk_score_0_100`, `network_impact_score_0_100`, `crew_legality_component`, `airport_env_component`, `pax_component`, `maintenance_component`.  
      * GNN outputs: `gnn_embedding` (vector), `gnn_network_criticality`, `downline_legs_affected_count`, `misconnect_pax_at_risk`, `revenue_at_risk_usd`.  

* **Unstructured Data (Tribal Knowledge):**  

  * **Source Material:**  
    * Crew legality manuals and **FAR Part 117** policy documents.  
    * Operations manuals for **MEL/CDL** handling, APU usage, and gate/ground equipment restrictions.  
    * Airport curfew and slot regulations (e.g., LHR, CDG, SNA), station operating procedures.  
    * Playbooks and SOPs for IROP recovery (tail swap guidelines, pre-cancel rules, reaccommodation priorities).  

  * **Purpose:**  
    * Indexed with **Cortex Search** to answer qualitative questions like:  
      * “How do we legally extend FDP for an augmented crew crossing time zones?”  
      * “What are the rules for operating with an APU MEL at non-hub outstations?”  
      * “What is the curfew window and slot tolerance at LHR for our evening arrivals?”  
    * Used by **Cortex Agents** to ground the IOC copilot’s recommendations in airline policies and regulatory constraints, not just model outputs.

* **ML Notebook Specification (Snowpark ML & Notebooks):**  

  * **Objective:**  
    Build a **multi-model ensemble** plus GNN stack that:  
    1. Predicts localized risks (delay, turn success, crew timeout, misconnect, AOG) at the flight/tail/crew/PNR level.  
    2. Uses a **Heterogeneous Graph Neural Network (HGNN)** to propagate those risks through the tail–crew–PNR–airport network.  
    3. Produces a final flight-level **Risk Score (0–100)** and **Network Impact Score** for IOC triage.

  * **Base Models and Targets:**  
    * **Departure/Arrival Delay Model**  
      * **Target Variable:** `[target_delay_minutes]` (continuous block-time anomaly).  
      * **Algorithm Choice:** Gradient-boosted trees (XGBoost / Snowflake ML regression).  
      * **Inference Output Table:** `ML_PROCESSING.DELAY_PREDICTIONS` with columns: `flight_key`, `snapshot_ts`, `predicted_delay_minutes`, `delay_risk_score`.  

    * **Turn-Success Model**  
      * **Target Variable:** `[turn_success_flag]` (1 if pushes ≤ X minutes after STD, else 0).  
      * **Algorithm Choice:** Explainable Boosting Machine or LightGBM classifier.  
      * **Inference Output Table:** `ML_PROCESSING.TURN_PREDICTIONS` with `flight_key`, `snapshot_ts`, `turn_success_prob`, `turn_risk_flags`.  

    * **Crew Legality / Timeout Model**  
      * **Target Variable:** `[crew_timeout_flag]` (1 if FDP exceeds legal limit given itinerary, else 0).  
      * **Algorithm Choice:** Survival / time-to-event model (CoxPH or gradient-boosted survival) wrapped in Snowpark ML; fallback to probability classifier.  
      * **Inference Output Table:** `ML_PROCESSING.CREW_TIMEOUT_PREDICTIONS` with `duty_id`, `snapshot_ts`, `timeout_prob`, `time_to_timeout_minutes`.  

    * **Passenger Misconnect Model**  
      * **Target Variable:** `[pnr_misconnect_flag]` at the PNR-trip level.  
      * **Algorithm Choice:** XGBoost / LightGBM classifier with dynamic **MCT** features by connection type (DOM–DOM, DOM–INTL, INTL–DOM).  
      * **Inference Output Table:** `ML_PROCESSING.PNR_MISCONNECT_PREDICTIONS` with `pnr_id`, `trip_id`, `snapshot_ts`, `pnr_misconnect_prob`.  

    * **AOG / MEL Risk Model**  
      * **Target Variable:** `[aog_event_flag]` (probability of AOG within next N hours).  
      * **Algorithm Choice:** NLP model over maintenance logs using Snowpark Python + Document AI / AI_EXTRACT features, then XGBoost classifier.  
      * **Inference Output Table:** `ML_PROCESSING.AOG_RISK_PREDICTIONS` with `tail_number`, `snapshot_ts`, `aog_risk_score`, `critical_mel_flag`.  

  * **GNN Layer (Snowpark Container Services on GPU):**  
    * **Objective:**  
      Implement a **Heterogeneous GNN (HGNN)** using PyTorch Geometric running in **Snowpark Container Services** that:  
      * Builds a dynamic graph with nodes: `FlightInstance`, `Aircraft`, `CrewDutyPeriod`, `CrewMember`, `PNRTrip`, `Airport`, `MaintenanceEvent`, `WeatherCell/ATCSector`.  
      * Edges: `OPERATED_BY`, `NEXT_LEG`, `ASSIGNED_TO`, `CONTAINS`, `BOARDS`, `DEPARTS_FROM`, `ARRIVES_AT`, `OVERLAPS_SECTOR`.  
      * Ingests base-model predictions as node/edge features.  
      * Uses an **attention-based message-passing** mechanism to learn which neighbors (e.g., inbound aircraft, connecting crew, high-value PNRs) drive network impact the most.  

    * **Target Variable (for supervised training):**  
      * `[network_disruption_label]` (e.g., realized downstream misconnects, crew timeouts, cancellations within a defined horizon).  

    * **Inference Output:**  
      * Node embeddings per `flight_key` and `tail_number`.  
      * `gnn_network_criticality` score per `flight_key`.  
      * Written to `ML_PROCESSING.GNN_FLIGHT_EMBEDDINGS` and then surfaced in `IROP_MART.FLIGHT_RISK`.  

  * **Final Risk Scorer (Explainable):**  
    * **Target Variable:** `[flight_risk_score_0_100]`.  
    * **Algorithm Choice:**  
      * Explainable Boosting Machine (EBM) or Shapley-calibrated LightGBM classifier/regressor combining:  
        - Base tabular features (delay, crew, pax, environment).  
        - GNN embeddings & `gnn_network_criticality`.  
    * **Inference Output Table:**  
      * `IROP_MART.FLIGHT_RISK` with transparent component scores and SHAP-attribution fields to support explainability.  

---

## 4. Cortex Intelligence Specifications

### Cortex Analyst (Structured Data / SQL)

* **Semantic Model Scope:**  

  * **Measures:**  
    1. `total_misconnect_pax` – sum of expected misconnecting passengers across selected flights/horizons.  
    2. `total_revenue_at_risk_usd` – aggregated revenue exposure for selected flights/PNRs.  
    3. `avg_flight_risk_score` – average composite risk score for a filtered subset of flights.  
    4. `num_high_risk_flights` – count of flights with `flight_risk_score_0_100 >= 70`.  

  * **Dimensions:**  
    1. `departure_station`, `arrival_station`, `hub_flag` (ATL, JFK, DTW, MSP, etc.).  
    2. `flight_date`, `snapshot_ts_hour` (for intraday trend slices).  
    3. `fleet_type`, `route_type` (DOM–DOM, DOM–INTL, INTL–DOM).  
    4. `risk_band` (Low/Medium/High) and `network_impact_band`.  

* **Golden Query (Verification):**  

  * *User Prompt:*  
    “For tonight’s JFK–LHR and ATL–CDG departure banks, show me the total misconnect passengers and revenue at risk, broken down by origin airport and highlight how many flights are High Risk.”

  * *Expected SQL Operation (Conceptual):*  
    ```sql
    SELECT
        departure_station,
        COUNT(*) AS num_flights,
        SUM(expected_misconnect_pax) AS total_misconnect_pax,
        SUM(revenue_at_risk_usd) AS total_revenue_at_risk_usd,
        SUM(CASE WHEN flight_risk_score_0_100 >= 70 THEN 1 ELSE 0 END) AS num_high_risk_flights
    FROM IROP_MART.FLIGHT_RISK
    WHERE flight_date = CURRENT_DATE
      AND arrival_station IN ('LHR', 'CDG')
      AND departure_station IN ('JFK', 'ATL')
    GROUP BY departure_station;
    ```  

---

### Cortex Search (Unstructured Data / RAG)

* **Service Name:** `IROP_POLICY_SOPS_SEARCH_SERVICE`  

* **Indexing Strategy:**  
  * **Document Attributes:**  
    * `doc_type` (e.g., `FAR_117`, `MEL_MANUAL`, `CURFEW_RULES`, `IROP_PLAYBOOK`).  
    * `station_code` (for station/airport-specific SOPs, e.g., `LHR`, `JFK`, `ATL`).  
    * `fleet_type` (for fleet-specific MEL guidance).  

  * **Usage Pattern:**  
    * Scoped RAG queries conditioned on the flight or station in question:  
      * Example: For a JFK–LHR flight, filter documents where `doc_type IN ('CURFEW_RULES','IROP_PLAYBOOK') AND station_code = 'LHR'`.  

* **Sample RAG Prompt:**  
  “For an evening JFK–LHR flight arriving near the noise curfew at LHR, explain the current curfew rules, any allowed exemptions, and how much buffer time we should preserve to avoid forced cancellation. Summarize in 3 bullets suitable for an IOC Duty Manager.”

---

### Cortex Agents (Reasoning + Tools)

* **Agent Composition:**  
  * Tools:  
    * **Cortex Analyst** – for structured risk and KPI queries.  
    * **Cortex Search** (`IROP_POLICY_SOPS_SEARCH_SERVICE`) – for policy/SOP retrieval.  
    * **Custom Snowpark UDFs/Procedures** – to simulate scenarios (e.g., apply +15 min delay, swap tail, assign reserve crew) and recalculate downstream risk.  

* **Representative Agent Tasks:**  
  1. Given a specific `flight_key`, retrieve current risk scores, SHAP drivers, and downline impact; then summarize in natural language.  
  2. Simulate the effect of:  
     * adding a delay to an inbound leg,  
     * swapping an aircraft between two flights, or  
     * assigning reserve crew to a duty period,  
     and report the change in `flight_risk_score`, `expected_misconnect_pax`, and `total_revenue_at_risk_usd`.  
  3. Validate that recommended actions respect **Part 117**, **MEL/CDL**, and **curfew/slot** rules by cross-checking with Cortex Search over SOPs and regulations.

---

## 5. Streamlit Application UX/UI

* **Layout Strategy:**  

  * **Page 1 (Executive – Network Overview):**  
    * **Purpose:** Give VP / Duty Manager a system-wide **Network Disruption Intelligence** view.  
    * Components:  
      * Global KPI cards:  
        * `# High-Risk Flights (score ≥ 70)`  
        * `Total Misconnect Pax at Risk (next 6 hours)`  
        * `Total Revenue at Risk (USD)`  
      * Network map / graph visualization:  
        * Nodes = airports; edge thickness = number of high-risk flights; color intensity = aggregated network impact.  
      * Ranked list of **Top 10 flights by Network Criticality** with indicators for crew, pax, maintenance, and airport drivers.  

  * **Page 2 (Action – Flight Drill-down & IOC Copilot):**  
    * **Purpose:** IOC Flight Manager triage and simulation workspace.  
    * Layout:  
      * Left pane:  
        * Flight selection table (filter by station: ATL/JFK/etc., by risk band, by route type).  
        * Downline impact tree (e.g., ATL–MCO–LAX chain) with misconnect counts and crew legality flags.  
      * Right pane split:  
        * Upper: Detail panel for selected flight:  
          * Risk score, component breakdown, SHAP drivers.  
          * Key real-world flags: “FDP timeout risk high,” “curfew risk at LHR,” “APU MEL + no ground cart at station.”  
        * Lower: **Chat window (Cortex Agent)** with clear mode toggles:  
          * “Ask about numbers” → routes queries to **Cortex Analyst**.  
          * “Ask about rules/SOPs” → routes queries to **Cortex Search**.  
          * “Simulate options” → calls **custom tools** (e.g., `simulate_tail_swap`, `simulate_reserve_crew_assignment`), writes temporary changes to a sandbox table, re-runs scoring, and returns deltas.  

* **Component Logic:**  

  * **Visualizations:**  
    * **Altair heatmap / matrix**: hub–hub and hub–spoke pairs (e.g., ATL–JFK–LHR) with color by average `network_impact_score`.  
    * **Cascade diagram:** show how a delay on one leg changes risk on subsequent legs (timeline or Sankey-style).  
    * **Funnel for misconnects:** total impacted PNRs → premium cabin PNRs → elite members → hotel/voucher obligations.  

  * **Chat Integration:**  
    * A tabbed or toggled chat widget:  
      * Tab 1 – “IOC Analyst (Numbers)”:  
        * Example prompt: “For ATL between 17:00–21:00 today, which flights have the highest combination of crew timeout risk and misconnect pax?”  
        * The app displays both a textual answer and a filtered table/visual.  
      * Tab 2 – “Ops & Policy (Docs)”:  
        * Example prompt: “What’s the legal basis for this crew timing out after a 15-minute delay, and can we extend FDP?”  
        * The app shows a grounded textual summary and cites the relevant FAR 117 excerpt.  
      * Tab 3 – “What-If Simulator”:  
        * Pre-built buttons for scenarios:  
          * “Add +15m delay to inbound leg.”  
          * “Swap tails with Flight X.”  
          * “Assign reserve crew to this duty period.”  
        * After selection, the app calls the agent/tool, recomputes risk scores, and updates both visuals and an explanation:  
          * “Assigning reserve crew reduces total misconnect pax by 94 and cuts revenue at risk by \$380K; flight risk score drops from 82 → 34.”  

---

## 6. Success Criteria

* **Technical Validator:**  
  * A new ACARS/ATC event (e.g., EDCT delay or MEL update) ingested via Snowpipe Streaming triggers Dynamic Table refresh and model scoring such that the **network view and risk scores refresh in < 60 seconds** for key hubs (ATL, JFK).  
  * Cortex Analyst can reliably translate natural language IOC questions into correct aggregations over `IROP_MART.FLIGHT_RISK` and related dimension tables, returning results and updated Streamlit visuals in **< 3 seconds** for typical filter scopes.

* **Business Validator:**  
  * IOC leaders can identify and act on the **top 3 network-critical flights** for a given disruption scenario (e.g., ATL storm, JFK GDP) in **< 5 minutes**, with clear, contrastive explanations of why those flights matter more than others.  
  * Compared with the current fragmented tooling and manual spreadsheet analysis, the demo workflow demonstrates a **> 50% reduction in time-to-insight** for understanding cascading IROP impact and evaluating tail/crew/passenger recovery options.