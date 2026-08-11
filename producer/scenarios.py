"""Fraud Scenarios Generator — SentinelStream.

Implements synthetic generators for the 6 core fraud scenarios specified in Section 18.
Each function produces GroundTruthEvent objects with hidden ground-truth labels.
"""

from datetime import datetime, timedelta, timezone
import math
import random
from typing import List, Tuple

from producer.schemas import GroundTruthEvent, TransactionEvent

# Standard merchant categories
NORMAL_MERCHANT_CATEGORIES = ["groceries", "food_delivery", "coffee", "utility", "fuel", "ride_share"]
HIGH_RISK_MERCHANT_CATEGORIES = ["electronics", "jewelry", "crypto_exchange", "luxury_goods", "wire_transfer"]

PAYMENT_METHODS = ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NETBANKING"]

INDIAN_CITIES: List[Tuple[str, float, float]] = [
    ("Delhi", 28.6139, 77.2090),
    ("Gurugram", 28.4595, 77.0266),
    ("Mumbai", 19.0760, 72.8777),
    ("Bengaluru", 12.9716, 77.5946),
    ("Hyderabad", 17.3850, 78.4867),
    ("Kolkata", 22.5726, 88.3639),
    ("Chennai", 13.0827, 80.2707),
    ("Pune", 18.5204, 73.8567),
]

INTERNATIONAL_CITIES: List[Tuple[str, float, float]] = [
    ("London", 51.5074, -0.1278),
    ("New York", 40.7128, -74.0060),
    ("Dubai", 25.2048, 55.2708),
    ("Singapore", 1.3521, 103.8198),
]


def haversine_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate Great Circle distance in kilometers between two coordinates."""
    r = 6371.0  # Earth radius in kilometers
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def generate_normal_transaction(
    user_id: str,
    account_id: str,
    device_id: str,
    base_city: Tuple[str, float, float],
    timestamp: datetime,
    avg_amount: float = 1200.0,
    rng: random.Random = random,
) -> GroundTruthEvent:
    """Generate a single normal user transaction."""
    amount = round(rng.lognormvariate(math.log(avg_amount), 0.5), 2)

    # Slight coordinate jitter (+/- 2 km)
    city_name, base_lat, base_lon = base_city
    lat = round(base_lat + rng.uniform(-0.02, 0.02), 4)
    lon = round(base_lon + rng.uniform(-0.02, 0.02), 4)

    tx = TransactionEvent(
        transaction_id=f"tx_n_{rng.randint(1000000, 9999999)}",
        event_time=timestamp,
        user_id=user_id,
        account_id=account_id,
        amount=amount,
        currency="INR",
        merchant_id=f"m_{rng.randint(1000, 9999)}",
        merchant_category=rng.choice(NORMAL_MERCHANT_CATEGORIES),
        payment_method=rng.choice(PAYMENT_METHODS),
        device_id=device_id,
        ip_address=f"10.20.{rng.randint(1, 254)}.{rng.randint(1, 254)}",
        latitude=lat,
        longitude=lon,
        country="IN",
        city=city_name,
    )

    return GroundTruthEvent(
        event=tx,
        is_fraud_ground_truth=False,
        fraud_scenario_type="NORMAL",
        fraud_reason_ground_truth=None,
    )


def generate_scenario_1_high_velocity(
    user_id: str,
    account_id: str,
    device_id: str,
    base_city: Tuple[str, float, float],
    start_time: datetime,
    rng: random.Random = random,
) -> List[GroundTruthEvent]:
    """Scenario 1: Rapid succession of escalating amounts within 5 minutes."""
    events: List[GroundTruthEvent] = []
    city_name, lat, lon = base_city
    escalating_amounts = [500.0, 1200.0, 3500.0, 8500.0, 22000.0, 55000.0]

    current_time = start_time
    for i, amount in enumerate(escalating_amounts):
        current_time += timedelta(seconds=rng.randint(15, 45))
        tx = TransactionEvent(
            transaction_id=f"tx_s1_{rng.randint(1000000, 9999999)}",
            event_time=current_time,
            user_id=user_id,
            account_id=account_id,
            amount=amount,
            currency="INR",
            merchant_id=f"m_{rng.randint(1000, 9999)}",
            merchant_category="electronics" if i > 3 else "groceries",
            payment_method="UPI",
            device_id=device_id,
            ip_address=f"10.20.5.{rng.randint(1, 254)}",
            latitude=lat,
            longitude=lon,
            country="IN",
            city=city_name,
        )
        is_fraud = i >= 2  # First two might be normal, subsequent escalating ones trigger velocity fraud
        events.append(
            GroundTruthEvent(
                event=tx,
                is_fraud_ground_truth=is_fraud,
                fraud_scenario_type="SCENARIO_1_HIGH_VELOCITY",
                fraud_reason_ground_truth=f"High velocity transaction #{i+1} in short time window with escalating amount ₹{amount}",
            )
        )
    return events


def generate_scenario_2_large_amount_anomaly(
    user_id: str,
    account_id: str,
    device_id: str,
    base_city: Tuple[str, float, float],
    timestamp: datetime,
    rng: random.Random = random,
) -> GroundTruthEvent:
    """Scenario 2: Extreme single transaction amount spike vs baseline."""
    city_name, lat, lon = base_city
    extreme_amount = round(rng.uniform(95000.0, 250000.0), 2)

    tx = TransactionEvent(
        transaction_id=f"tx_s2_{rng.randint(1000000, 9999999)}",
        event_time=timestamp,
        user_id=user_id,
        account_id=account_id,
        amount=extreme_amount,
        currency="INR",
        merchant_id=f"m_{rng.randint(1000, 9999)}",
        merchant_category="jewelry",
        payment_method="CREDIT_CARD",
        device_id=device_id,
        ip_address=f"10.20.12.{rng.randint(1, 254)}",
        latitude=lat,
        longitude=lon,
        country="IN",
        city=city_name,
    )

    return GroundTruthEvent(
        event=tx,
        is_fraud_ground_truth=True,
        fraud_scenario_type="SCENARIO_2_LARGE_AMOUNT_ANOMALY",
        fraud_reason_ground_truth=f"Extreme transaction amount ₹{extreme_amount} significantly exceeding user baseline average",
    )


def generate_scenario_3_geographic_anomaly(
    user_id: str,
    account_id: str,
    device_id: str,
    start_city: Tuple[str, float, float],
    start_time: datetime,
    rng: random.Random = random,
) -> List[GroundTruthEvent]:
    """Scenario 3: Impossible travel distance between consecutive transactions."""
    events: List[GroundTruthEvent] = []
    # 1st transaction: Delhi at 10:00 AM
    tx1 = TransactionEvent(
        transaction_id=f"tx_s3_{rng.randint(1000000, 9999999)}",
        event_time=start_time,
        user_id=user_id,
        account_id=account_id,
        amount=1500.0,
        currency="INR",
        merchant_id="m_delhi_1",
        merchant_category="coffee",
        payment_method="UPI",
        device_id=device_id,
        ip_address="10.20.1.10",
        latitude=start_city[1],
        longitude=start_city[2],
        country="IN",
        city=start_city[0],
    )
    events.append(
        GroundTruthEvent(
            event=tx1,
            is_fraud_ground_truth=False,
            fraud_scenario_type="NORMAL",
            fraud_reason_ground_truth=None,
        )
    )

    # 2nd transaction: London 3 minutes later (impossible speed > 10,000 km/h)
    dest_city = ("London", 51.5074, -0.1278)
    tx2_time = start_time + timedelta(minutes=3)
    tx2 = TransactionEvent(
        transaction_id=f"tx_s3_{rng.randint(1000000, 9999999)}",
        event_time=tx2_time,
        user_id=user_id,
        account_id=account_id,
        amount=48000.0,
        currency="INR",
        merchant_id="m_london_99",
        merchant_category="electronics",
        payment_method="CREDIT_CARD",
        device_id=f"dev_foreign_{rng.randint(100, 999)}",
        ip_address="81.2.69.140",
        latitude=dest_city[1],
        longitude=dest_city[2],
        country="GB",
        city=dest_city[0],
    )
    dist_km = haversine_distance_km(start_city[1], start_city[2], dest_city[1], dest_city[2])
    events.append(
        GroundTruthEvent(
            event=tx2,
            is_fraud_ground_truth=True,
            fraud_scenario_type="SCENARIO_3_GEOGRAPHIC_ANOMALY",
            fraud_reason_ground_truth=f"Impossible travel speed detected: {round(dist_km, 1)} km traveled in 3 minutes",
        )
    )

    return events


def generate_scenario_4_new_device_high_value(
    user_id: str,
    account_id: str,
    base_city: Tuple[str, float, float],
    timestamp: datetime,
    rng: random.Random = random,
) -> GroundTruthEvent:
    """Scenario 4: High-value transaction originating from an unrecognized new device."""
    city_name, lat, lon = base_city
    amount = round(rng.uniform(65000.0, 140000.0), 2)
    new_device_id = f"dev_unrecognized_{rng.randint(10000, 99999)}"

    tx = TransactionEvent(
        transaction_id=f"tx_s4_{rng.randint(1000000, 9999999)}",
        event_time=timestamp,
        user_id=user_id,
        account_id=account_id,
        amount=amount,
        currency="INR",
        merchant_id=f"m_{rng.randint(1000, 9999)}",
        merchant_category="electronics",
        payment_method="NETBANKING",
        device_id=new_device_id,
        ip_address=f"185.220.{rng.randint(1, 254)}.{rng.randint(1, 254)}",
        latitude=lat,
        longitude=lon,
        country="IN",
        city=city_name,
    )

    return GroundTruthEvent(
        event=tx,
        is_fraud_ground_truth=True,
        fraud_scenario_type="SCENARIO_4_NEW_DEVICE_HIGH_VALUE",
        fraud_reason_ground_truth=f"First-time device {new_device_id} combined with high monetary amount ₹{amount}",
    )


def generate_scenario_5_merchant_behavior_change(
    user_id: str,
    account_id: str,
    device_id: str,
    base_city: Tuple[str, float, float],
    timestamp: datetime,
    rng: random.Random = random,
) -> GroundTruthEvent:
    """Scenario 5: Sudden shift to high-risk merchant category."""
    city_name, lat, lon = base_city
    amount = round(rng.uniform(45000.0, 90000.0), 2)

    tx = TransactionEvent(
        transaction_id=f"tx_s5_{rng.randint(1000000, 9999999)}",
        event_time=timestamp,
        user_id=user_id,
        account_id=account_id,
        amount=amount,
        currency="INR",
        merchant_id=f"m_crypto_{rng.randint(100, 999)}",
        merchant_category=rng.choice(HIGH_RISK_MERCHANT_CATEGORIES),
        payment_method="NETBANKING",
        device_id=device_id,
        ip_address=f"10.20.88.{rng.randint(1, 254)}",
        latitude=lat,
        longitude=lon,
        country="IN",
        city=city_name,
    )

    return GroundTruthEvent(
        event=tx,
        is_fraud_ground_truth=True,
        fraud_scenario_type="SCENARIO_5_MERCHANT_BEHAVIOR_CHANGE",
        fraud_reason_ground_truth=f"Unusual high-risk merchant category '{tx.merchant_category}' with elevated amount ₹{amount}",
    )


def generate_scenario_6_burst_pattern(
    user_id: str,
    account_id: str,
    device_id: str,
    base_city: Tuple[str, float, float],
    start_time: datetime,
    count: int = 50,
    rng: random.Random = random,
) -> List[GroundTruthEvent]:
    """Scenario 6: Micro-transaction burst (e.g. 50 micro-transactions within 60 seconds)."""
    events: List[GroundTruthEvent] = []
    city_name, lat, lon = base_city

    current_time = start_time
    for i in range(count):
        current_time += timedelta(milliseconds=rng.randint(500, 1200))
        amount = round(rng.uniform(10.0, 99.0), 2)
        tx = TransactionEvent(
            transaction_id=f"tx_s6_{rng.randint(1000000, 9999999)}",
            event_time=current_time,
            user_id=user_id,
            account_id=account_id,
            amount=amount,
            currency="INR",
            merchant_id=f"m_micro_{rng.randint(10, 99)}",
            merchant_category="digital_content",
            payment_method="UPI",
            device_id=device_id,
            ip_address=f"10.20.99.{rng.randint(1, 254)}",
            latitude=lat,
            longitude=lon,
            country="IN",
            city=city_name,
        )
        events.append(
            GroundTruthEvent(
                event=tx,
                is_fraud_ground_truth=True,
                fraud_scenario_type="SCENARIO_6_BURST_PATTERN",
                fraud_reason_ground_truth=f"Automated micro-transaction burst event #{i+1} in high-frequency sequence",
            )
        )
    return events
