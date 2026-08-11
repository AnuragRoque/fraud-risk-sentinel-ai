"""Synthetic Data Generator Engine — SentinelStream.

Generates reproducible normal and fraud transaction streams with ground-truth labels.
Supports seed control, scenario injection, streaming iterators, and CLI output.
"""

import argparse
from datetime import datetime, timedelta, timezone
import json
import random
from typing import Generator, List, Optional

from producer.scenarios import (
    INDIAN_CITIES,
    generate_normal_transaction,
    generate_scenario_1_high_velocity,
    generate_scenario_2_large_amount_anomaly,
    generate_scenario_3_geographic_anomaly,
    generate_scenario_4_new_device_high_value,
    generate_scenario_5_merchant_behavior_change,
    generate_scenario_6_burst_pattern,
)
from producer.schemas import GroundTruthEvent, TransactionEvent


class SyntheticDataGenerator:
    """Engine for generating synthetic financial transaction event streams."""

    def __init__(
        self,
        seed: int = 42,
        num_users: int = 100,
        fraud_rate: float = 0.05,
    ) -> None:
        self.seed = seed
        self.num_users = num_users
        self.fraud_rate = fraud_rate
        self.rng = random.Random(seed)

        # Build synthetic user baseline profiles
        self.user_profiles = []
        for i in range(num_users):
            user_id = f"usr_{10000 + i}"
            account_id = f"acc_{20000 + i}"
            device_id = f"dev_{30000 + i}"
            base_city = self.rng.choice(INDIAN_CITIES)
            avg_amount = round(self.rng.uniform(400.0, 3500.0), 2)
            self.user_profiles.append(
                {
                    "user_id": user_id,
                    "account_id": account_id,
                    "device_id": device_id,
                    "base_city": base_city,
                    "avg_amount": avg_amount,
                }
            )

    def _get_random_user(self) -> dict:
        return self.rng.choice(self.user_profiles)

    def generate_batch(
        self,
        count: int = 100,
        start_time: Optional[datetime] = None,
    ) -> List[GroundTruthEvent]:
        """Generate a batch of synthetic transaction events with ground truth labels."""
        if start_time is None:
            start_time = datetime.now(timezone.utc)
        elif start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)

        events: List[GroundTruthEvent] = []
        current_time = start_time

        while len(events) < count:
            user = self._get_random_user()
            current_time += timedelta(seconds=self.rng.randint(1, 10))

            # Decide whether to inject fraud scenario
            if self.rng.random() < self.fraud_rate:
                scenario_choice = self.rng.randint(1, 6)
                if scenario_choice == 1:
                    scenario_events = generate_scenario_1_high_velocity(
                        user["user_id"], user["account_id"], user["device_id"], user["base_city"], current_time, self.rng
                    )
                    events.extend(scenario_events)
                    current_time = scenario_events[-1].event.event_time
                elif scenario_choice == 2:
                    events.append(
                        generate_scenario_2_large_amount_anomaly(
                            user["user_id"], user["account_id"], user["device_id"], user["base_city"], current_time, self.rng
                        )
                    )
                elif scenario_choice == 3:
                    scenario_events = generate_scenario_3_geographic_anomaly(
                        user["user_id"], user["account_id"], user["device_id"], user["base_city"], current_time, self.rng
                    )
                    events.extend(scenario_events)
                    current_time = scenario_events[-1].event.event_time
                elif scenario_choice == 4:
                    events.append(
                        generate_scenario_4_new_device_high_value(
                            user["user_id"], user["account_id"], user["base_city"], current_time, self.rng
                        )
                    )
                elif scenario_choice == 5:
                    events.append(
                        generate_scenario_5_merchant_behavior_change(
                            user["user_id"], user["account_id"], user["device_id"], user["base_city"], current_time, self.rng
                        )
                    )
                elif scenario_choice == 6:
                    burst_count = self.rng.randint(10, 25)
                    scenario_events = generate_scenario_6_burst_pattern(
                        user["user_id"], user["account_id"], user["device_id"], user["base_city"], current_time, burst_count, self.rng
                    )
                    events.extend(scenario_events)
                    current_time = scenario_events[-1].event.event_time
            else:
                events.append(
                    generate_normal_transaction(
                        user["user_id"],
                        user["account_id"],
                        user["device_id"],
                        user["base_city"],
                        current_time,
                        user["avg_amount"],
                        self.rng,
                    )
                )

        return events[:count]

    def stream_events(
        self,
        count: int = 100,
        start_time: Optional[datetime] = None,
    ) -> Generator[GroundTruthEvent, None, None]:
        """Generator yielding GroundTruthEvent instances sequentially."""
        batch = self.generate_batch(count=count, start_time=start_time)
        for event in batch:
            yield event


def main() -> None:
    """CLI entrypoint for generating synthetic transactions."""
    parser = argparse.ArgumentParser(description="SentinelStream Synthetic Data Generator CLI")
    parser.add_argument("--count", type=int, default=20, help="Number of transaction events to generate")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--fraud-rate", type=float, default=0.1, help="Synthetic fraud injection rate (0.0 - 1.0)")
    parser.add_argument("--output", choices=["console", "json", "streaming_only"], default="console", help="Output format")
    args = parser.parse_args()

    generator = SyntheticDataGenerator(seed=args.seed, fraud_rate=args.fraud_rate)
    events = generator.generate_batch(count=args.count)

    if args.output == "console":
        print(f"=== Generated {len(events)} Synthetic Transactions (Seed: {args.seed}) ===")
        for i, gt in enumerate(events, 1):
            tx = gt.event
            fraud_str = f"[FRAUD: {gt.fraud_scenario_type}]" if gt.is_fraud_ground_truth else "[NORMAL]"
            print(f"{i:03d} | {tx.event_time.strftime('%H:%M:%S')} | {tx.user_id} | {tx.currency} {tx.amount:>9.2f} | {tx.city:<10} | {fraud_str}")
    elif args.output == "json":
        json_output = [gt.to_dict() if hasattr(gt, "to_dict") else json.loads(gt.to_json()) for gt in events]
        print(json.dumps(json_output, indent=2))
    elif args.output == "streaming_only":
        streaming_events = [gt.to_streaming_event().to_dict() for gt in events]
        print(json.dumps(streaming_events, indent=2))


if __name__ == "__main__":
    main()
