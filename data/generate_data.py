#!/usr/bin/env python3
"""
IROP GNN Risk - Synthetic Data Generator
Generates realistic airline operational data for demo purposes.
Seed: 42 for reproducibility
"""
import csv
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = SCRIPT_DIR

HUBS = ['ATL', 'JFK', 'DTW', 'LAX']
SPOKES = ['MCO', 'DFW', 'SEA', 'SLC', 'MSP', 'BOS', 'MIA', 'SFO', 'ORD', 'DEN']
INTL_DESTINATIONS = ['LHR', 'CDG', 'FRA', 'AMS', 'NRT', 'ICN']
ALL_STATIONS = HUBS + SPOKES + INTL_DESTINATIONS

FLEET_TYPES = ['B737-900', 'B757-200', 'A321neo', 'A330-300', 'B767-400', 'A350-900']
NARROW_BODY = ['B737-900', 'B757-200', 'A321neo']
WIDE_BODY = ['A330-300', 'B767-400', 'A350-900']

ELITE_LEVELS = [None, 'SILVER', 'GOLD', 'PLATINUM', 'DIAMOND']
FARE_CLASSES = ['Y', 'B', 'M', 'H', 'Q', 'K', 'L', 'U', 'T', 'X', 'V', 'E', 'N', 'R', 'G', 'S']
DELAY_CODES = ['WX', 'ATC', 'MX', 'CREW', 'PAX', 'CONN', 'GATE', 'FUEL', 'SECURITY', 'OTHER']

BASE_DATE = datetime(2026, 2, 19)

def gen_uuid():
    return str(uuid.uuid4())[:8].upper()

def gen_flight_number(dep, arr):
    base = hash(f"{dep}{arr}") % 9000 + 1000
    return f"DL{base}"

def generate_airports():
    """Generate airport capability data."""
    airports = []
    
    airport_data = {
        'ATL': {'hub': True, 'country': 'USA', 'region': 'Southeast', 'gates': 192, 'wb_gates': 45, 'tz': -5},
        'JFK': {'hub': True, 'country': 'USA', 'region': 'Northeast', 'gates': 128, 'wb_gates': 38, 'tz': -5},
        'DTW': {'hub': True, 'country': 'USA', 'region': 'Midwest', 'gates': 129, 'wb_gates': 22, 'tz': -5},
        'LAX': {'hub': True, 'country': 'USA', 'region': 'West', 'gates': 146, 'wb_gates': 35, 'tz': -8},
        'MCO': {'hub': False, 'country': 'USA', 'region': 'Southeast', 'gates': 129, 'wb_gates': 8, 'tz': -5},
        'DFW': {'hub': False, 'country': 'USA', 'region': 'Southwest', 'gates': 165, 'wb_gates': 20, 'tz': -6},
        'SEA': {'hub': False, 'country': 'USA', 'region': 'Pacific NW', 'gates': 91, 'wb_gates': 15, 'tz': -8},
        'SLC': {'hub': False, 'country': 'USA', 'region': 'Mountain', 'gates': 82, 'wb_gates': 8, 'tz': -7},
        'MSP': {'hub': False, 'country': 'USA', 'region': 'Midwest', 'gates': 117, 'wb_gates': 12, 'tz': -6},
        'BOS': {'hub': False, 'country': 'USA', 'region': 'Northeast', 'gates': 102, 'wb_gates': 18, 'tz': -5},
        'MIA': {'hub': False, 'country': 'USA', 'region': 'Southeast', 'gates': 131, 'wb_gates': 25, 'tz': -5},
        'SFO': {'hub': False, 'country': 'USA', 'region': 'West', 'gates': 115, 'wb_gates': 22, 'tz': -8},
        'ORD': {'hub': False, 'country': 'USA', 'region': 'Midwest', 'gates': 191, 'wb_gates': 28, 'tz': -6},
        'DEN': {'hub': False, 'country': 'USA', 'region': 'Mountain', 'gates': 111, 'wb_gates': 12, 'tz': -7},
        'LHR': {'hub': False, 'country': 'UK', 'region': 'Europe', 'gates': 115, 'wb_gates': 45, 'tz': 0, 
                'curfew_start': '23:00', 'curfew_end': '06:00', 'slot_controlled': True},
        'CDG': {'hub': False, 'country': 'France', 'region': 'Europe', 'gates': 120, 'wb_gates': 40, 'tz': 1,
                'curfew_start': '23:30', 'curfew_end': '06:00', 'slot_controlled': True},
        'FRA': {'hub': False, 'country': 'Germany', 'region': 'Europe', 'gates': 108, 'wb_gates': 38, 'tz': 1},
        'AMS': {'hub': False, 'country': 'Netherlands', 'region': 'Europe', 'gates': 99, 'wb_gates': 30, 'tz': 1},
        'NRT': {'hub': False, 'country': 'Japan', 'region': 'Asia', 'gates': 91, 'wb_gates': 35, 'tz': 9,
                'curfew_start': '23:00', 'curfew_end': '06:00'},
        'ICN': {'hub': False, 'country': 'Korea', 'region': 'Asia', 'gates': 128, 'wb_gates': 42, 'tz': 9},
    }
    
    for code, data in airport_data.items():
        airports.append({
            'station_code': code,
            'hub_flag': data['hub'],
            'country': data['country'],
            'region': data['region'],
            'runway_config': f"{random.randint(2,4)}R/{random.randint(2,4)}L",
            'gate_count': data['gates'],
            'widebody_gate_count': data['wb_gates'],
            'ground_start_cart_count': random.randint(5, 15),
            'customs_capacity_per_hour': random.randint(500, 2000) if data['country'] != 'USA' else None,
            'curfew_start_local': data.get('curfew_start'),
            'curfew_end_local': data.get('curfew_end'),
            'slot_controlled_flag': data.get('slot_controlled', False),
            'mct_dom_dom_minutes': 45 if data['hub'] else 60,
            'mct_dom_intl_minutes': 90,
            'mct_intl_dom_minutes': 120 if code in ['JFK', 'LAX', 'ATL'] else 90,
            'gdp_active_flag': False,
            'gdp_avg_delay_minutes': 0,
            'atc_congestion_index': round(random.uniform(0.2, 0.8), 2),
            'airport_disruption_index': round(random.uniform(0.1, 0.4), 2),
            'timezone_offset_utc': data['tz'],
        })
    
    return airports

def generate_flights(num_flights=150):
    """Generate flight instance data with realistic patterns."""
    flights = []
    flight_keys = []
    
    hub_pairs = [(h1, h2) for h1 in HUBS for h2 in HUBS if h1 != h2]
    hub_spoke = [(h, s) for h in HUBS for s in SPOKES[:6]]
    spoke_hub = [(s, h) for s in SPOKES[:6] for h in HUBS]
    intl_routes = [(h, i) for h in ['JFK', 'ATL', 'LAX'] for i in INTL_DESTINATIONS[:4]]
    intl_return = [(i, h) for i, h in intl_routes]
    
    all_routes = hub_pairs + hub_spoke + spoke_hub + intl_routes + intl_return
    random.shuffle(all_routes)
    
    banks = [
        (6, 9),
        (11, 14),
        (15, 18),
        (18, 22),
    ]
    
    for i in range(num_flights):
        dep, arr = all_routes[i % len(all_routes)]
        is_intl = arr in INTL_DESTINATIONS or dep in INTL_DESTINATIONS
        
        bank_start, bank_end = random.choice(banks)
        dep_hour = random.randint(bank_start, bank_end)
        dep_minute = random.choice([0, 15, 30, 45])
        
        if is_intl:
            block_time = random.randint(420, 660)
            fleet = random.choice(WIDE_BODY)
            pax = random.randint(180, 280)
        else:
            distance_factor = 1 + (0.3 * (abs(hash(f"{dep}{arr}")) % 5))
            block_time = int(90 * distance_factor + random.randint(-15, 30))
            fleet = random.choice(NARROW_BODY if block_time < 180 else FLEET_TYPES)
            pax = random.randint(120, 180) if fleet in NARROW_BODY else random.randint(180, 280)
        
        sched_dep = BASE_DATE.replace(hour=dep_hour, minute=dep_minute)
        sched_arr = sched_dep + timedelta(minutes=block_time)
        
        delay_dep = 0
        delay_arr = 0
        status = 'SCHEDULED'
        delay_code_list = []
        
        if random.random() < 0.35:
            delay_dep = random.choice([5, 10, 15, 20, 25, 30, 45, 60, 90])
            delay_arr = delay_dep + random.randint(-10, 15)
            delay_code_list = random.sample(DELAY_CODES, k=random.randint(1, 2))
            
            if delay_dep > 60:
                status = 'DELAYED'
        
        flight_key = f"{gen_flight_number(dep, arr)}_{BASE_DATE.strftime('%Y%m%d')}_{i:03d}"
        flight_keys.append(flight_key)
        
        connecting_pax_pct = round(random.uniform(0.3, 0.7), 2) if arr in HUBS else round(random.uniform(0.1, 0.3), 2)
        elite_pax = int(pax * random.uniform(0.05, 0.15))
        revenue = pax * random.uniform(150, 800 if is_intl else 350)
        
        turn_buffer = random.randint(35, 90)
        
        flights.append({
            'flight_key': flight_key,
            'flight_number': gen_flight_number(dep, arr),
            'departure_station': dep,
            'arrival_station': arr,
            'flight_date': BASE_DATE.strftime('%Y-%m-%d'),
            'leg_id': 1,
            'sched_dep_utc': sched_dep.strftime('%Y-%m-%d %H:%M:%S'),
            'sched_arr_utc': sched_arr.strftime('%Y-%m-%d %H:%M:%S'),
            'act_dep_utc': (sched_dep + timedelta(minutes=delay_dep)).strftime('%Y-%m-%d %H:%M:%S') if delay_dep else None,
            'act_arr_utc': (sched_arr + timedelta(minutes=delay_arr)).strftime('%Y-%m-%d %H:%M:%S') if delay_arr else None,
            'turn_buffer_minutes': turn_buffer,
            'current_delay_departure': delay_dep,
            'current_delay_arrival': max(0, delay_arr),
            'gate_id': f"{random.choice(['A','B','C','D','E','F','T'])}{random.randint(1,50)}",
            'status': status,
            'delay_codes': str(delay_code_list) if delay_code_list else None,
            'block_time_minutes': block_time,
            'tail_number': None,
            'aircraft_fleet_type': fleet,
            'pax_count': pax,
            'connecting_pax_pct': connecting_pax_pct,
            'elite_pax_count': elite_pax,
            'intl_connector_flag': is_intl or (dep in HUBS and arr in SPOKES and random.random() < 0.2),
            'revenue_at_risk_usd': round(revenue * (delay_dep / 60 + 0.1) if delay_dep else revenue * 0.05, 2),
            'delay_risk_score': round(random.uniform(10, 90), 1) if delay_dep else round(random.uniform(5, 40), 1),
            'turn_success_prob': round(max(0.3, 1 - (delay_dep / 120) - random.uniform(0, 0.2)), 2),
            'misconnect_prob': round(min(0.95, connecting_pax_pct * (delay_dep / 45 + 0.1)), 2),
            'network_criticality_score': round(random.uniform(20, 95), 1),
        })
    
    return flights, flight_keys

def generate_rotations(flights, num_tails=45):
    """Generate aircraft rotation data."""
    rotations = []
    
    tails = [f"N{random.randint(100,999)}D{random.choice(['A','L','N','W'])}" for _ in range(num_tails)]
    
    flight_by_dep = {}
    for f in flights:
        dep = f['departure_station']
        if dep not in flight_by_dep:
            flight_by_dep[dep] = []
        flight_by_dep[dep].append(f)
    
    for dep in flight_by_dep:
        flight_by_dep[dep].sort(key=lambda x: x['sched_dep_utc'])
    
    tail_assignments = {t: [] for t in tails}
    used_flights = set()
    
    for hub in HUBS:
        if hub not in flight_by_dep:
            continue
        hub_flights = [f for f in flight_by_dep[hub] if f['flight_key'] not in used_flights]
        
        for i, flight in enumerate(hub_flights[:len(tails)//2]):
            tail = tails[i % num_tails]
            tail_assignments[tail].append(flight)
            used_flights.add(flight['flight_key'])
    
    for station, station_flights in flight_by_dep.items():
        for flight in station_flights:
            if flight['flight_key'] in used_flights:
                continue
            for tail in tails:
                if len(tail_assignments[tail]) < 4:
                    tail_assignments[tail].append(flight)
                    used_flights.add(flight['flight_key'])
                    break
    
    for tail, assigned_flights in tail_assignments.items():
        if not assigned_flights:
            continue
            
        assigned_flights.sort(key=lambda x: x['sched_dep_utc'])
        fleet = assigned_flights[0]['aircraft_fleet_type']
        
        for i, flight in enumerate(assigned_flights):
            has_mel = random.random() < 0.08
            mel_code = random.choice(['APU', 'PACK', 'IFE', 'LAVATORY', 'GALLEY']) if has_mel else None
            
            flights[flights.index(next(f for f in flights if f['flight_key'] == flight['flight_key']))]['tail_number'] = tail
            
            rotations.append({
                'rotation_id': gen_uuid(),
                'tail_number': tail,
                'flight_key': flight['flight_key'],
                'flight_date': flight['flight_date'],
                'sequence_position': i + 1,
                'prev_flight_key': assigned_flights[i-1]['flight_key'] if i > 0 else None,
                'next_flight_key': assigned_flights[i+1]['flight_key'] if i < len(assigned_flights)-1 else None,
                'fleet_type': fleet,
                'aircraft_age_years': round(random.uniform(2, 18), 1),
                'owner_flag': random.random() < 0.85,
                'etops_capable_flag': fleet in WIDE_BODY,
                'utilization_hours_24h': round(random.uniform(6, 14), 1),
                'overnight_location': assigned_flights[0]['departure_station'],
                'next_maintenance_due_ts': (BASE_DATE + timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d %H:%M:%S'),
                'maintenance_station_flag': flight['departure_station'] in HUBS,
                'mel_apu_flag': mel_code == 'APU',
                'mel_item_code': mel_code,
                'mel_severity': random.choice(['CAT-A', 'CAT-B', 'CAT-C', 'CAT-D']) if mel_code else None,
                'mel_expiry_ts': (BASE_DATE + timedelta(days=random.randint(1, 10))).strftime('%Y-%m-%d %H:%M:%S') if mel_code else None,
                'aog_risk_score': round(random.uniform(0.5, 0.9), 2) if mel_code else round(random.uniform(0.01, 0.15), 2),
            })
    
    return rotations, flights

def generate_crew(flights, num_duties=80):
    """Generate crew duty period data."""
    duties = []
    
    crew_bases = HUBS
    
    captain_ids = [f"CPT{random.randint(10000,99999)}" for _ in range(num_duties)]
    fo_ids = [f"FO{random.randint(10000,99999)}" for _ in range(num_duties)]
    
    flights_by_base = {b: [] for b in crew_bases}
    for f in flights:
        if f['departure_station'] in crew_bases:
            flights_by_base[f['departure_station']].append(f)
    
    for i in range(num_duties):
        base = crew_bases[i % len(crew_bases)]
        base_flights = flights_by_base.get(base, [])
        
        if base_flights:
            first_flight = random.choice(base_flights[:max(1, len(base_flights)//2)])
            report_time = datetime.strptime(first_flight['sched_dep_utc'], '%Y-%m-%d %H:%M:%S') - timedelta(minutes=60)
        else:
            report_time = BASE_DATE.replace(hour=random.randint(5, 18), minute=0)
        
        num_segments = random.randint(2, 5)
        fdp_limit = 600 if num_segments <= 3 else 540
        fdp_used = random.randint(300, fdp_limit - 30)
        
        time_zone_span = random.uniform(0, 4) if random.random() < 0.3 else 0
        augmented = time_zone_span > 3 or fdp_used > 480
        
        timeout_risk = 0.0
        if fdp_used > fdp_limit - 60:
            timeout_risk = min(0.95, (fdp_used - (fdp_limit - 120)) / 120)
        
        release_time = report_time + timedelta(minutes=fdp_used + 30)
        
        duties.append({
            'duty_id': f"DUTY_{gen_uuid()}",
            'pairing_id': f"PAIR_{gen_uuid()}",
            'duty_date': BASE_DATE.strftime('%Y-%m-%d'),
            'crew_base': base,
            'captain_id': captain_ids[i],
            'fo_id': fo_ids[i],
            'fa_count': random.randint(3, 8),
            'report_time_utc': report_time.strftime('%Y-%m-%d %H:%M:%S'),
            'scheduled_release_time_utc': release_time.strftime('%Y-%m-%d %H:%M:%S'),
            'num_segments': num_segments,
            'augmented_crew_flag': augmented,
            'fdp_limit_minutes': fdp_limit,
            'fdp_time_used_minutes': fdp_used,
            'fdp_remaining_minutes': fdp_limit - fdp_used,
            'rest_in_last_168_hours_minutes': random.randint(2400, 4200),
            'time_zone_span_hours': round(time_zone_span, 1),
            'crew_timeout_risk_score': round(timeout_risk, 2),
            'reserve_crew_available_flag': random.random() < 0.6,
            'reserve_crew_eta_minutes': random.randint(30, 180) if random.random() < 0.6 else None,
        })
    
    return duties

def generate_crew_assignments(flights, duties):
    """Generate crew assignment data linking flights to duties."""
    assignments = []
    
    flight_list = flights.copy()
    random.shuffle(flight_list)
    
    duty_flight_count = {d['duty_id']: 0 for d in duties}
    duty_segments = {d['duty_id']: d['num_segments'] for d in duties}
    
    for flight in flight_list:
        available_duties = [d for d in duties 
                          if duty_flight_count[d['duty_id']] < duty_segments[d['duty_id']]]
        
        if not available_duties:
            break
            
        base_match = [d for d in available_duties if d['crew_base'] == flight['departure_station']]
        duty = random.choice(base_match if base_match else available_duties)
        
        duty_flight_count[duty['duty_id']] += 1
        
        assignments.append({
            'assignment_id': gen_uuid(),
            'flight_key': flight['flight_key'],
            'duty_id': duty['duty_id'],
            'role': 'COCKPIT',
            'leg_sequence_in_duty': duty_flight_count[duty['duty_id']],
        })
    
    return assignments

def generate_pnr(flights, num_pnr=2000):
    """Generate PNR trip data."""
    pnrs = []
    
    connecting_flights = [f for f in flights if f['arrival_station'] in HUBS]
    
    for i in range(num_pnr):
        if random.random() < 0.4 and len(connecting_flights) > 1:
            first_leg = random.choice(connecting_flights)
            hub = first_leg['arrival_station']
            second_legs = [f for f in flights 
                          if f['departure_station'] == hub 
                          and f['sched_dep_utc'] > first_leg['sched_arr_utc']]
            
            if second_legs:
                second_leg = random.choice(second_legs)
                origin = first_leg['departure_station']
                destination = second_leg['arrival_station']
                itinerary = [first_leg['flight_key'], second_leg['flight_key']]
                is_intl = destination in INTL_DESTINATIONS or origin in INTL_DESTINATIONS
            else:
                single = random.choice(flights)
                origin = single['departure_station']
                destination = single['arrival_station']
                itinerary = [single['flight_key']]
                is_intl = destination in INTL_DESTINATIONS
        else:
            single = random.choice(flights)
            origin = single['departure_station']
            destination = single['arrival_station']
            itinerary = [single['flight_key']]
            is_intl = destination in INTL_DESTINATIONS
        
        group_size = random.choices([1, 2, 3, 4, 5, 6], weights=[50, 25, 12, 8, 3, 2])[0]
        elite = random.choices(ELITE_LEVELS, weights=[60, 15, 12, 8, 5])[0]
        fare = random.choice(FARE_CLASSES)
        
        misconnect_prob = 0.0
        if len(itinerary) > 1:
            misconnect_prob = round(random.uniform(0.05, 0.45), 2)
        
        pnrs.append({
            'pnr_id': f"PNR{gen_uuid()}",
            'trip_id': f"TRIP_{gen_uuid()}",
            'primary_customer_id': f"CUST{random.randint(100000, 999999)}",
            'origin': origin,
            'destination': destination,
            'itinerary_flight_keys': str(itinerary),
            'intl_flag': is_intl,
            'group_size': group_size,
            'elite_status_level': elite,
            'fare_class_bucket': fare,
            'rebook_flexibility_index': round(random.uniform(0.2, 0.9), 2),
            'loyalty_value_index': round(random.uniform(0.1, 1.0), 2) if elite else round(random.uniform(0.1, 0.4), 2),
            'estimated_voucher_cost_usd': round(random.uniform(100, 500) * group_size, 2) if misconnect_prob > 0.2 else 0,
            'pnr_misconnect_prob': misconnect_prob,
            'pnr_reaccom_complexity_score': round(random.uniform(0.1, 0.9), 2) if len(itinerary) > 1 else round(random.uniform(0.05, 0.3), 2),
        })
    
    return pnrs

def generate_weather(num_records=500):
    """Generate weather and ATC data."""
    weather = []
    
    affected_stations = random.sample(ALL_STATIONS, k=min(8, len(ALL_STATIONS)))
    
    for i in range(num_records):
        station = random.choice(affected_stations) if random.random() < 0.7 else random.choice(ALL_STATIONS)
        interval_offset = (i * 5) % 1440
        valid_time = BASE_DATE + timedelta(minutes=interval_offset)
        
        has_convection = random.random() < 0.15
        has_visibility = random.random() < 0.1
        has_gdp = random.random() < 0.08
        
        weather.append({
            'record_id': gen_uuid(),
            'sector_id': f"ZTL{random.randint(10,99)}" if random.random() < 0.3 else None,
            'station_code': station,
            'valid_time_utc': valid_time.strftime('%Y-%m-%d %H:%M:%S'),
            'convective_index': round(random.uniform(0.6, 1.0), 2) if has_convection else round(random.uniform(0, 0.3), 2),
            'visibility_category': random.choice(['IFR', 'LIFR']) if has_visibility else random.choice(['VFR', 'MVFR']),
            'crosswind_knots': round(random.uniform(15, 35), 1) if random.random() < 0.1 else round(random.uniform(0, 15), 1),
            'icing_risk_index': round(random.uniform(0.5, 0.9), 2) if random.random() < 0.05 else round(random.uniform(0, 0.2), 2),
            'edct_delay_mean': random.randint(15, 60) if has_gdp else 0,
            'holding_probability': round(random.uniform(0.3, 0.8), 2) if has_convection else round(random.uniform(0, 0.15), 2),
            'flow_program_flag': has_gdp,
            'airspace_capacity_index': round(random.uniform(0.4, 0.7), 2) if has_gdp or has_convection else round(random.uniform(0.8, 1.0), 2),
        })
    
    return weather

def generate_flight_risk(flights):
    """Generate IROP mart flight risk data."""
    risks = []
    
    for flight in flights:
        risk_score = float(flight['delay_risk_score']) * 0.3 + \
                    (1 - float(flight['turn_success_prob'])) * 100 * 0.25 + \
                    float(flight['misconnect_prob']) * 100 * 0.25 + \
                    float(flight['network_criticality_score']) * 0.2
        
        risk_score = min(100, max(0, risk_score))
        
        if risk_score >= 70:
            risk_band = 'High'
        elif risk_score >= 40:
            risk_band = 'Medium'
        else:
            risk_band = 'Low'
        
        network_impact = float(flight['network_criticality_score'])
        if network_impact >= 70:
            impact_band = 'High'
        elif network_impact >= 40:
            impact_band = 'Medium'
        else:
            impact_band = 'Low'
        
        is_intl = flight['arrival_station'] in INTL_DESTINATIONS or flight['departure_station'] in INTL_DESTINATIONS
        route_type = 'DOM-INTL' if flight['arrival_station'] in INTL_DESTINATIONS else \
                    ('INTL-DOM' if flight['departure_station'] in INTL_DESTINATIONS else 'DOM-DOM')
        
        risks.append({
            'risk_id': gen_uuid(),
            'flight_key': flight['flight_key'],
            'flight_number': flight['flight_number'],
            'departure_station': flight['departure_station'],
            'arrival_station': flight['arrival_station'],
            'flight_date': flight['flight_date'],
            'sched_dep_utc': flight['sched_dep_utc'],
            'sched_arr_utc': flight['sched_arr_utc'],
            'snapshot_ts': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'tail_number': flight['tail_number'],
            'fleet_type': flight['aircraft_fleet_type'],
            'hub_flag': flight['departure_station'] in HUBS,
            'route_type': route_type,
            'flight_risk_score_0_100': round(risk_score, 1),
            'network_impact_score_0_100': round(network_impact, 1),
            'crew_legality_component': round(random.uniform(0, 30), 1),
            'airport_env_component': round(random.uniform(0, 25), 1),
            'pax_component': round(float(flight['misconnect_prob']) * 100 * 0.5, 1),
            'maintenance_component': round(random.uniform(0, 20), 1),
            'gnn_network_criticality': round(network_impact * random.uniform(0.8, 1.2), 1),
            'gnn_embedding': None,
            'downline_legs_affected_count': random.randint(0, 8),
            'misconnect_pax_at_risk': int(flight['pax_count'] * flight['connecting_pax_pct'] * flight['misconnect_prob']),
            'revenue_at_risk_usd': round(flight['revenue_at_risk_usd'], 2),
            'risk_band': risk_band,
            'network_impact_band': impact_band,
            'shap_attribution': None,
            'risk_drivers': str(['DELAY', 'CREW', 'PAX'][:random.randint(1,3)]) if risk_score > 50 else None,
            'fdp_timeout_risk_flag': random.random() < 0.12,
            'curfew_risk_flag': flight['arrival_station'] in ['LHR', 'CDG', 'NRT'] and random.random() < 0.15,
            'mel_risk_flag': random.random() < 0.08,
            'turn_risk_flag': float(flight['turn_success_prob']) < 0.7,
        })
    
    return risks

def generate_policy_documents():
    """Generate policy documents for Cortex Search."""
    docs = []
    
    docs.append({
        'doc_id': gen_uuid(),
        'doc_type': 'FAR_117',
        'station_code': None,
        'fleet_type': None,
        'title': 'FAR Part 117 - Flight Duty Period Limitations',
        'content': """FAR Part 117 Flight Duty Period (FDP) Limitations and Rest Requirements

FLIGHT DUTY PERIOD LIMITS:
- Unaugmented crew, 2 flight segments: Maximum 9-10 hours depending on start time
- Unaugmented crew, 3+ segments: Maximum 8-9 hours depending on start time
- Augmented crew (Class 1 rest): Up to 17 hours FDP allowed
- Augmented crew (Class 2 rest): Up to 15 hours FDP allowed
- Augmented crew (Class 3 rest): Up to 13 hours FDP allowed

TIME ZONE CONSIDERATIONS:
- Crossing 4+ time zones reduces FDP limits by 30 minutes
- Crossing 6+ time zones reduces FDP limits by 1 hour
- Acclimation period of 48 hours required after crossing 4+ zones

REST REQUIREMENTS:
- Minimum 10 consecutive hours rest opportunity
- Rest must include 8 hours uninterrupted sleep opportunity
- 30 consecutive hours free from duty required per 168-hour period
- 56 hours rest per 168-hour period for unaugmented operations

FDP EXTENSIONS:
- Commander may extend FDP by up to 2 hours if unforeseen circumstances
- Extensions require immediate post-flight rest period extension
- Maximum of 2 extensions per 168-hour period
- Extension must be logged with specific justification

CUMULATIVE FLIGHT TIME LIMITS:
- 100 hours in any 672 consecutive hours
- 1000 hours in any 365-day period
- Flight time includes taxi-out to parking at destination""",
        'effective_date': '2024-01-01',
    })
    
    docs.append({
        'doc_id': gen_uuid(),
        'doc_type': 'MEL_MANUAL',
        'station_code': None,
        'fleet_type': None,
        'title': 'MEL/CDL Operations Manual - APU and Ground Operations',
        'content': """MINIMUM EQUIPMENT LIST (MEL) OPERATIONS GUIDANCE

APU INOPERATIVE PROCEDURES:
- APU MEL Item 49-1: APU may be inoperative provided:
  * External ground power available at origin AND destination
  * Alternate airport has ground power capability
  * Operations not conducted in known or forecast icing conditions
  * Engine start capability via ground cart or cross-bleed confirmed

- APU DEFERRAL RESTRICTIONS:
  * Maximum 10 days deferral for Category B items
  * Operations restricted to stations with ground support
  * Notification required to destination station 60 minutes prior
  * Ground start cart must be confirmed available before dispatch

GROUND START PROCEDURES WITHOUT APU:
1. Confirm ground power unit (GPU) connected and stable
2. Verify external air start unit availability
3. Complete cross-bleed start checklist if no air cart
4. Minimum 90-second stabilization after engine start
5. Do not attempt cross-bleed start if OAT below -10C

STATION LIMITATIONS FOR APU MEL:
- Non-hub outstations may have limited ground support
- Verify ground cart availability via station ops prior to departure
- International destinations require 24-hour advance notification
- Some airports restrict ground equipment during noise curfew hours

CDL ITEMS AFFECTING GROUND OPERATIONS:
- Missing ground service door requires maintenance assist
- Cargo door seal issues may require dry operation only
- Gear doors missing require speed restrictions""",
        'effective_date': '2024-01-01',
    })
    
    docs.append({
        'doc_id': gen_uuid(),
        'doc_type': 'CURFEW_RULES',
        'station_code': 'LHR',
        'fleet_type': None,
        'title': 'London Heathrow (LHR) Curfew and Slot Regulations',
        'content': """LONDON HEATHROW (LHR) NOISE CURFEW AND SLOT REQUIREMENTS

CURFEW WINDOW:
- Night quota period: 23:30 to 06:00 local time
- Scheduled operations prohibited during curfew
- Emergency and humanitarian flights may request exemption

SLOT REQUIREMENTS:
- LHR is fully slot coordinated (Level 3)
- All flights require confirmed slot allocation
- Slot tolerance: +/- 15 minutes from scheduled time
- Slot misuse may result in sanctions and future slot loss

LATE RUNNING PROCEDURES:
- If delay causes arrival after 23:30, contact ATC immediately
- Diversions to LGW, STN, or BHX may be required
- Late arrival fees apply: £10,000 per occurrence minimum
- Repeat violations subject to slot withdrawal proceedings

NOISE QUOTA SYSTEM:
- Each aircraft type assigned QC (Quota Count) value
- Night movements limited by total QC budget
- A350/787 have favorable QC ratings (QC/0.5)
- A330/767 require higher quota allocation (QC/2)

EXEMPTIONS AND WAIVERS:
- Weather diversions: 30-minute grace period applies
- Technical emergencies: Exemption on case-by-case basis
- Scheduled late departures: Must have ACL approval
- Charter and cargo: Generally no curfew exemptions

BUFFER RECOMMENDATIONS:
- Schedule arrivals no later than 22:30 for curfew protection
- Allow 60-minute buffer for transatlantic arrivals
- Winter operations: Add 15-minute additional buffer""",
        'effective_date': '2024-01-01',
    })
    
    docs.append({
        'doc_id': gen_uuid(),
        'doc_type': 'CURFEW_RULES',
        'station_code': 'CDG',
        'fleet_type': None,
        'title': 'Paris Charles de Gaulle (CDG) Curfew and Operations',
        'content': """PARIS CHARLES DE GAULLE (CDG) OPERATIONAL RESTRICTIONS

CURFEW PERIODS:
- Soft curfew: 22:00 to 06:00 (noise surcharges apply)
- Voluntary quiet hours: 00:00 to 05:00
- Chapter 2 aircraft prohibited 22:00-06:00

RUNWAY PREFERENCES:
- Night operations: Preferred runways 26L/26R for departures
- Noise sensitive areas: Avoid overflights of Roissy-en-France
- Weather permitting: Use runway configurations minimizing community impact

LATE ARRIVAL HANDLING:
- Arrivals after 23:30: Increased approach charges apply
- Diversions available to ORY (Orly) with restrictions
- Ground handling limited after 01:00

SLOT COORDINATION:
- CDG is Level 3 coordinated
- Historic slot rights preserved with 80% utilization
- New entrant slots available through COHOR

CONNECTING PASSENGER CONSIDERATIONS:
- MCT for connections: 90 minutes DOM-INTL
- Terminal transfers may require 120 minutes
- Immigration processing times variable: allow 45-60 minutes""",
        'effective_date': '2024-01-01',
    })
    
    docs.append({
        'doc_id': gen_uuid(),
        'doc_type': 'IROP_PLAYBOOK',
        'station_code': None,
        'fleet_type': None,
        'title': 'IROP Recovery Playbook - Tail Swap and Pre-Cancel Guidelines',
        'content': """IRREGULAR OPERATIONS RECOVERY PLAYBOOK

TAIL SWAP DECISION CRITERIA:
Priority Order for Tail Swaps:
1. Eliminate crew legality violations (FDP timeout prevention)
2. Preserve international/long-haul departures
3. Minimize total passenger misconnections
4. Protect high-value customer segments (Diamond, Platinum)
5. Reduce total delay minutes across network

Tail Swap Approval Levels:
- Same hub, same fleet type: IOC Flight Manager authority
- Cross-hub within region: Duty Manager approval required
- Cross-fleet or ETOPS changes: VP Network Operations required
- International route changes: Full IOC leadership consensus

PRE-CANCEL GUIDELINES:
When to Consider Pre-Cancel:
- Delay exceeds 4 hours AND no improvement forecast
- Crew timeout unavoidable AND no reserve crew within 2 hours
- Maintenance issue requires part not available same day
- Weather at destination not forecast to improve

Pre-Cancel Timing:
- Domestic flights: Cancel minimum 3 hours before departure
- International flights: Cancel minimum 5 hours before departure
- Earlier cancellation = better rebooking options

REACCOMMODATION PRIORITIES:
1. Unaccompanied minors and passengers with disabilities
2. Diamond/Platinum members with tight connections
3. International connecting passengers
4. Business class passengers
5. General boarding priority order

CUSTOMER RECOVERY GUIDELINES:
- Meal vouchers: Delays >3 hours
- Hotel accommodation: Delays requiring overnight stay
- Rebooking on other carriers: When next Delta option >24 hours
- Compensation: Per DOT and EU261 requirements

COMMUNICATION REQUIREMENTS:
- Gate announcement: Every 30 minutes during delay
- App/SMS notification: Within 15 minutes of status change
- Elite member direct contact: For delays >2 hours""",
        'effective_date': '2024-01-01',
    })
    
    return docs

def write_csv(filename, data, fieldnames):
    """Write data to CSV file."""
    filepath = OUTPUT_DIR / filename
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        writer.writerows(data)
    print(f"  Written: {filename} ({len(data)} rows)")

def main():
    print("=" * 50)
    print("IROP GNN Risk - Synthetic Data Generator")
    print("=" * 50)
    print(f"\nSeed: 42")
    print(f"Base Date: {BASE_DATE.strftime('%Y-%m-%d')}")
    print(f"Output Directory: {OUTPUT_DIR}\n")
    
    print("Generating airports...")
    airports = generate_airports()
    write_csv('airports.csv', airports, list(airports[0].keys()))
    
    print("Generating flights...")
    flights, flight_keys = generate_flights(150)
    
    print("Generating aircraft rotations...")
    rotations, flights = generate_rotations(flights, 45)
    write_csv('flights.csv', flights, list(flights[0].keys()))
    write_csv('rotations.csv', rotations, list(rotations[0].keys()))
    
    print("Generating crew duty periods...")
    duties = generate_crew(flights, 80)
    write_csv('crew.csv', duties, list(duties[0].keys()))
    
    print("Generating crew assignments...")
    assignments = generate_crew_assignments(flights, duties)
    write_csv('crew_assignments.csv', assignments, list(assignments[0].keys()))
    
    print("Generating PNR trips...")
    pnrs = generate_pnr(flights, 2000)
    write_csv('pnr.csv', pnrs, list(pnrs[0].keys()))
    
    print("Generating weather/ATC data...")
    weather = generate_weather(500)
    write_csv('weather.csv', weather, list(weather[0].keys()))
    
    print("Generating flight risk scores...")
    risks = generate_flight_risk(flights)
    write_csv('flight_risk.csv', risks, list(risks[0].keys()))
    
    print("Generating policy documents...")
    docs = generate_policy_documents()
    write_csv('policy_documents.csv', docs, list(docs[0].keys()))
    
    print("\n" + "=" * 50)
    print("Data generation complete!")
    print("=" * 50)
    
    print(f"\nSummary:")
    print(f"  - Airports: {len(airports)}")
    print(f"  - Flights: {len(flights)}")
    print(f"  - Aircraft Rotations: {len(rotations)}")
    print(f"  - Crew Duty Periods: {len(duties)}")
    print(f"  - Crew Assignments: {len(assignments)}")
    print(f"  - PNR Trips: {len(pnrs)}")
    print(f"  - Weather Records: {len(weather)}")
    print(f"  - Flight Risk Records: {len(risks)}")
    print(f"  - Policy Documents: {len(docs)}")

if __name__ == "__main__":
    main()
