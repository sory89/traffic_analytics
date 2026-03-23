from kafka import KafkaProducer
from faker import Faker
import json
import random
import time
from datetime import datetime, timedelta
import pytz

fake = Faker('fr_FR')

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Routes françaises
roads = ["A1", "A6", "A7", "N118", "D910", "A86", "A104", "N20"]

# Zones urbaines françaises
zones = ["PARIS_CBD", "LA_DEFENSE", "ORLY_AEROPORT", "CDG_AEROPORT", 
         "GARE_DU_NORD", "GARE_DE_LYON", "BOULOGNE", "SAINT_DENIS"]

# Météo en français
weather = ["ENSOLEILLE", "PLUIE", "BROUILLARD", "ORAGE", "NEIGE", "NUAGEUX"]

# Types de véhicules français
vehicle_types = ["VL", "PL", "MOTO", "BUS", "TAXI", "VTC", "CAMION"]

vehicle_cache = []

paris_tz = pytz.timezone("Europe/Paris")

def generate_clean_event():
    vid = f"FR-{fake.bothify(text='??-###-??').upper()}"
    vehicle_cache.append(vid)
    return {
        "vehicle_id": vid,
        "road_id": random.choice(roads),
        "city_zone": random.choice(zones),
        "speed": random.randint(20, 130),
        "congestion_level": random.randint(1, 5),
        "weather": random.choice(weather),
        "vehicle_type": random.choice(vehicle_types),
        "department": random.choice(["75", "92", "93", "94", "78", "91", "95", "77"]),
        "event_time": datetime.now(paris_tz).isoformat()
    }

def generate_dirty_event():
    dirty_type = random.choice([
        "null_speed",
        "negative_speed",
        "extreme_speed",
        "duplicate_vehicle",
        "late_event",
        "future_event",
        "wrong_datatype",
        "schema_drift",
        "corrupt_json"
    ])
    base = generate_clean_event()
    if dirty_type == "null_speed":
        base["speed"] = None
    elif dirty_type == "negative_speed":
        base["speed"] = -40
    elif dirty_type == "extreme_speed":
        base["speed"] = 420
    elif dirty_type == "duplicate_vehicle" and vehicle_cache:
        base["vehicle_id"] = random.choice(vehicle_cache)
    elif dirty_type == "late_event":
        base["event_time"] = (
            datetime.now(paris_tz) - timedelta(minutes=random.randint(10, 120))
        ).isoformat()
    elif dirty_type == "future_event":
        base["event_time"] = (
            datetime.now(paris_tz) + timedelta(minutes=random.randint(5, 60))
        ).isoformat()
    elif dirty_type == "wrong_datatype":
        base["speed"] = "RAPIDE"
    elif dirty_type == "schema_drift":
        base["etat_route"] = random.choice(["BON", "MAUVAIS", "TRAVAUX"])
    elif dirty_type == "corrupt_json":
        return "###EVENEMENT_CORROMPU###"
    return base

print("🚗 Démarrage du producteur de trafic français...")
while True:
    if random.random() < 0.7:
        event = generate_clean_event()
    else:
        event = generate_dirty_event()

    if isinstance(event, str):
        producer.send("traffic-topic", value={"raw": event})
        print("❌ ÉVÉNEMENT CORROMPU ENVOYÉ")
    else:
        producer.send("traffic-topic", value=event)
        print(f"✅ {event['city_zone']} | {event['road_id']} | {event['speed']} km/h | {event['weather']}")

    time.sleep(random.uniform(0.5, 1.5))