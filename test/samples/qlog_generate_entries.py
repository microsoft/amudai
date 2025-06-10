import json
import random
import datetime
import sys

class DeterministicUUID:
    """A UUID-like object that provides deterministic generation using only random API."""
    
    def __init__(self, hex_string):
        self.hex_string = hex_string
        # Format as standard UUID: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
        self.formatted = f"{hex_string[:8]}-{hex_string[8:12]}-{hex_string[12:16]}-{hex_string[16:20]}-{hex_string[20:]}"
    
    @property
    def hex(self):
        return self.hex_string
    
    def __str__(self):
        return self.formatted

def gen_uuid():
    """
    Generates a UUID-like string using only the random API.
    Returns a DeterministicUUID object that behaves like uuid.UUID.
    All generated UUIDs are deterministic when random is initialized with a known seed.
    """
    # Generate 32 hex characters (128 bits)
    hex_chars = "0123456789abcdef"
    uuid_hex = ""
    
    # Generate first 12 hex chars (48 bits)
    for _ in range(12):
        uuid_hex += hex_chars[random.randint(0, 15)]
    
    # Add version 4 identifier (4 bits)
    uuid_hex += "4"
    
    # Generate next 3 hex chars (12 bits)
    for _ in range(3):
        uuid_hex += hex_chars[random.randint(0, 15)]
    
    # Add variant bits (first 2 bits should be 10, so hex digit is 8, 9, a, or b)
    variant_chars = "89ab"
    uuid_hex += variant_chars[random.randint(0, 3)]
    
    # Generate remaining 15 hex chars (60 bits)
    for _ in range(15):
        uuid_hex += hex_chars[random.randint(0, 15)]
    
    return DeterministicUUID(uuid_hex)


def generate_random_timestamp(start_days_ago=7, end_days_ago=0):
    """Generates a random ISO 8601 timestamp within a date range."""
    now = datetime.datetime(2025, 1, 10, 12, 30, 20)
    start_date = now - \
        datetime.timedelta(days=random.randint(end_days_ago, start_days_ago))
    random_seconds = random.randint(0, 24*60*60 - 1)
    random_microseconds = random.randint(0, 999999)
    final_date = start_date + \
        datetime.timedelta(seconds=random_seconds,
                           microseconds=random_microseconds)
    return final_date.isoformat() + "Z"


def generate_sensor_reading():
    """Generates a single sensor reading object."""
    sensors = [
        ("temp_C", "Celsius", -273.15, -270.0),
        ("mag_field_T", "Tesla", 0.00001, 0.001),
        ("pressure_Pa", "Pascal", 1.0e-10, 1.0e-8),
        ("vibration_hz", "Hertz", 0.1, 10.0),
        ("radiation_mSv", "mSv/hr", 0.001, 0.05)
    ]
    sensor_type, unit, min_val, max_val = random.choice(sensors)
    return {
        "sensor": sensor_type,
        "value": round(random.uniform(min_val, max_val), 6 if isinstance(min_val, float) else 0),
        "unit": unit
    }


def generate_measurement_device():
    """Generates a measurement device object."""
    device_id = f"QMeasure-{random.choice(['XG', 'ZR', 'PL'])}-{random.randint(1, 999):03d}"
    calibration_statuses = [
        "CALIBRATED", "NEEDS_RECALIBRATION", "CALIBRATION_ERROR", "PENDING_CALIBRATION"]
    return {
        "deviceId": device_id,
        "calibrationStatus": random.choice(calibration_statuses),
        "lastCalibrated": generate_random_timestamp(start_days_ago=90, end_days_ago=1),
        "sensorReadings": [generate_sensor_reading() for _ in range(random.randint(2, 4))]
    }


def generate_monitored_pair():
    """Generates a monitored quantum pair object."""
    pair_suffix = f"{random.randint(1,99)}{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}"
    particle_a_id = f"pA-{pair_suffix}{random.choice('XWVUTS')}"
    particle_b_id = f"pB-{pair_suffix}{random.choice('ZYXWVU')}"

    initial_state_vector = [round(random.random(), 3) for _ in range(4)]
    # Normalize (simplified)
    norm = sum(x*x for x in initial_state_vector)**0.5
    initial_state_vector = [
        round(x/norm, 3) if norm > 0 else 0.25 for x in initial_state_vector]

    spins = ["+1/2", "-1/2", "0", "unknown", "+1", "-1"]
    correlation_types = ["Bell_CHSH", "GHZ_State", "W_State", "Hardy_Paradox"]

    linked_anomalies = None
    if random.random() < 0.3:  # 30% chance of having linked anomalies
        linked_anomalies = [
            f"anomaly-prev-{random.randint(1,100):03d}" for _ in range(random.randint(1, 3))]

    return {
        "pairId": f"pair_{pair_suffix}_{random.choice(['alpha', 'beta', 'gamma', 'delta'])}",
        "particleA_id": particle_a_id,
        "particleB_id": particle_b_id,
        "initialStateVector": initial_state_vector,
        "currentState": {
            "spin_A": random.choice(spins),
            "spin_B": random.choice(spins),
            "decoherenceFactor": round(random.uniform(0.01, 0.25), 4),
            "lastMeasurementTimestamp": generate_random_timestamp(start_days_ago=1, end_days_ago=0),
            "measurementDevice": generate_measurement_device()
        },
        "expectedBehavior": {
            "correlationType": random.choice(correlation_types),
            "expectedValue": round(random.uniform(-1.0, 1.0), 3),
            "threshold": round(random.uniform(0.01, 0.2), 3)
        },
        "observedValue": round(random.uniform(-1.0, 1.0), 3),
        "deviation": round(random.random() * 0.5, 3),  # Deviation up to 0.5
        "linkedAnomalies": linked_anomalies
    }


def generate_log_entry(index: int):
    """Generates a single complex log entry."""
    log_id = f"qeads-log-{gen_uuid()}"
    timestamp = generate_random_timestamp()
    service_names = ["QuantumAnomalyDetector",
                     "EntanglementCorrelator", "DecoherenceMonitor", "QubitStabilizer"]
    versions = ["2.7.1-beta", "3.0.0-rc1", "2.6.5", "3.1.0-alpha"]
    host_prefixes = ["qprocessor-node", "analyzer-vm", "data-collector"]
    host_name = f"{random.choice(host_prefixes)}-{random.randint(1,20):02d}.{random.choice(['cluster.internal', 'lab.quantum.net', 'cloud.q-infra.com'])}"
    log_levels = ["INFO", "WARNING", "ERROR", "DEBUG", "CRITICAL"]
    messages = [
        "Potential entanglement instability detected.",
        "Decoherence threshold exceeded for pair.",
        "Anomalous energy signature observed.",
        "System recalibration initiated due to drift.",
        "High-dimensional state vector mismatch."
    ]
    environments = ["production_alpha_quadrant", "staging_sector_gamma",
                    "development_sandbox_1", "testing_cluster_beta"]
    components = ["EntanglementMonitorModule", "SignalProcessor",
                  "DataAggregator", "AnomalyDetectionEngine"]
    functions = ["analyzePairStability()", "processIncomingSignal()",
                 "correlateEntangledStates()", "triggerAlert()"]

    anomaly_score = round(random.uniform(0.5, 1.0), 3)
    # Higher score, more likely confirmed
    is_confirmed = anomaly_score > 0.85 and random.random() > 0.3

    tags_pool = ["instability", "high_energy", "decoherence",
                 "calibration_drift", "spatial_anomaly", "temporal_flux", "multi_particle"]

    mitigation_strategies = ["DynamicFieldReconfiguration",
                             "CoolantSystemBoost", "StateVectorReset", "IsolationProtocol"]
    mitigation_outcomes = ["PENDING", "SUCCESS",
                           "FAILED_RETRY", "IN_PROGRESS", "ABORTED"]

    user_ids = ["dr_aranya_sharma", "prof_elara_vance",
                "tech_jian_li", "sys_monitor_agent"]
    session_prefixes = ["session_quantum_lab",
                        "remote_analysis_sess", "automated_run"]

    client_types = ["AutomatedAnalysisAgent",
                    "ManualInspectionClient", "SystemMonitorDaemon"]

    detail_words = ["flux", "resonance", "cascade", "matrix", "protocol", "threshold", "vector",
                    "amplitude", "calibration", "sequence", "gradient", "oscillation", "spectrum",
                    "interference", "modulation", "correlation", "deviation", "baseline", "signal",
                    "frequency", "magnetic", "temporal", "spatial", "quantum", "entanglement"]

    num_words = random.randint(0, 12)
    detail_text_words = random.sample(detail_words, num_words)

    # Add some punctuation randomly
    delimiters = [" ", " ", " ", ", ", "; ", ": "]
    details_text = ""
    for i, word in enumerate(detail_text_words):
        if random.randint(0, 20) == 1:
            word = str(gen_uuid())
        details_text += word
        if i < len(detail_text_words) - 1:
            details_text += random.choice(delimiters)

    entry = {
        "recordId": index,
        "logId": log_id,
        "timestamp": timestamp,
        "serviceName": random.choice(service_names),
        "serviceVersion": random.choice(versions),
        "hostName": host_name,
        "logLevel": random.choice(log_levels) if anomaly_score < 0.7 else ("WARNING" if anomaly_score < 0.9 else "ERROR"),
        "message": random.choice(messages),
        "details": details_text,
        "correlationId": f"corr-id-{gen_uuid().hex[:12]}",
        "environment": random.choice(environments),
        "source": {
            "component": random.choice(components),
            "function": random.choice(functions),
            "lineNumber": random.randint(50, 1500),
            "ipAddress": f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
        },
        "eventDetails": {
            "eventType": "ANOMALY_CANDIDATE" if not is_confirmed else "CONFIRMED_ANOMALY",
            "anomalyScore": anomaly_score,
            "isConfirmed": is_confirmed,
            "confidence": round(random.uniform(0.75, 0.99), 3) if is_confirmed else round(random.uniform(0.5, 0.9), 3),
            "tags": random.sample(tags_pool, k=random.randint(2, 4)),
            "monitoredPairs": [generate_monitored_pair() for _ in range(random.randint(1, 3))],
            "mitigationAttempt": {
                "attemptId": f"mit-attempt-{gen_uuid().hex[:8]}",
                "strategyUsed": random.choice(mitigation_strategies),
                "parameters": {
                    "fieldStrengthTesla": round(random.uniform(0.001, 0.05), 5) if "FieldReconfiguration" in mitigation_strategies else None,
                    "pulseDurationMs": random.randint(10, 200) if "FieldReconfiguration" in mitigation_strategies else None,
                    "targetRegion": f"Sector_{random.choice('AlphaBetaGammaDelta')}_{random.randint(1,10)}",
                    "coolantFlowRateLPM": round(random.uniform(5.0, 20.0), 1) if "CoolantSystem" in mitigation_strategies else None,
                    "resetTarget": "AllActivePairs" if "Reset" in mitigation_strategies else None
                },
                "outcome": random.choice(mitigation_outcomes),
                # Scheduled for near future
                "scheduledTime": generate_random_timestamp(start_days_ago=0, end_days_ago=-1)
            } if random.random() > 0.4 else None,  # 60% chance of mitigation attempt
            "userData": {
                "userId": random.choice(user_ids),
                "sessionId": f"{random.choice(session_prefixes)}_{gen_uuid().hex[:10]}",
                "permissions": random.sample(["read", "analyze", "mitigate_level_1", "mitigate_level_2", "configure_system"], k=random.randint(1, 3))
            },
            "systemPerformance": {
                "cpuLoad": round(random.uniform(0.1, 0.9), 2),
                "memoryUsageMB": random.randint(2048, 16384),
                "qpuUtilization": round(random.uniform(0.3, 0.95), 2),
                "networkLatencyMs": random.randint(5, 50)
            }
        },
        "clientInfo": {
            "clientType": random.choice(client_types),
            "clientVersion": f"{random.randint(1,3)}.{random.randint(0,5)}.{random.randint(0,9)}",
            "userAgent": f"QADS-Client/{random.choice(versions)} (QuantumOS {random.uniform(5,8):.1f}; {random.choice(['x86_64', 'arm64'])})",
            "originIp": f"192.168.{random.randint(1,254)}.{random.randint(1,254)}"
        } if random.random() > 0.2 else None,  # 80% chance of client info
        "processingTimeMs": random.randint(50, 1500)
    }
    # Clean up None parameters in mitigationAttempt if it exists
    if entry["eventDetails"]["mitigationAttempt"] and entry["eventDetails"]["mitigationAttempt"]["parameters"]:
        entry["eventDetails"]["mitigationAttempt"]["parameters"] = {
            k: v for k, v in entry["eventDetails"]["mitigationAttempt"]["parameters"].items() if v is not None
        }
    return entry


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python generate_logs.py <number_of_entries>")
        sys.exit(1)
    try:
        num_entries_to_generate = int(sys.argv[1])
        if num_entries_to_generate <= 0:
            raise ValueError
    except ValueError:
        print("Please provide a positive integer for the number of entries.")
        sys.exit(1)
        
    random.seed(0x96d7ef1ea0d6)
    for i in range(num_entries_to_generate):
        log_entry = generate_log_entry(i)
        print(json.dumps(log_entry))
