#!/usr/bin/env python3
import argparse
import json
import math
import os
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate simulated energy telemetry JSONL files")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD) in UTC")
    parser.add_argument("--days", type=int, default=1, help="Number of days to generate")
    parser.add_argument("--rows-per-device", type=int, default=1440, help="Number of rows per device per day")
    parser.add_argument(
        "--devices",
        default=str(Path("data/reference/devices.csv")),
        help="Path to reference devices CSV",
    )
    parser.add_argument(
        "--raw-base",
        default=str(Path("data/raw")),
        help="Base directory for raw JSONL output",
    )
    return parser.parse_args()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def isoformat_seconds(dt: datetime) -> str:
    # Normalize to UTC and drop fractional seconds, ensure 'Z' suffix
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def generate_day_for_device(
    day_start_utc: datetime,
    rows_per_device: int,
    device_id: str,
    site_id: str,
    capacity_kwh: float,
) -> list[dict]:
    rng = random.Random(f"{device_id}-{day_start_utc.date()}-{rows_per_device}")

    records: list[dict] = []
    interval_seconds = 86_400 / float(rows_per_device)

    # Initialize state of charge with a stable but device/day-specific seed
    soc = rng.uniform(40.0, 85.0)

    for i in range(rows_per_device):
        ts = day_start_utc + timedelta(seconds=i * interval_seconds)
        day_frac = ((ts - day_start_utc).total_seconds() % 86_400) / 86_400.0

        # Diurnal-like pattern: positive during the day, ~0 at night
        # Shifted sine wave so it's >= 0 for ~12 hours
        diurnal = max(0.0, math.sin(2 * math.pi * day_frac - math.pi / 2))

        # Device-specific max power heuristic tied to capacity
        p_max = max(500.0, capacity_kwh * 500.0)  # watts
        noise = rng.gauss(0.0, p_max * 0.05)
        power_w = max(0.0, diurnal * p_max + noise)

        # Energy for the sampling interval
        energy_wh = max(0.0, power_w * (interval_seconds / 3600.0))

        voltage_v = rng.gauss(400.0, 3.0)  # ~400V with small variance
        voltage_v = max(360.0, min(440.0, voltage_v))
        current_a = power_w / voltage_v if voltage_v > 0 else 0.0

        temp_base = 30.0 + 5.0 * math.sin(2 * math.pi * day_frac)
        temperature_c = temp_base + rng.gauss(0.0, 1.0)

        # Slow random walk for SoC
        soc_delta = rng.gauss(0.0, 0.08)
        soc = max(0.0, min(100.0, soc + soc_delta))

        status = "OK" if rng.random() >= 0.01 else "FAULT"

        record = {
            "ts_utc": isoformat_seconds(ts),
            "device_id": device_id,
            "site_id": site_id,
            "power_w": round(power_w, 3),
            "energy_wh": round(energy_wh, 6),
            "voltage_v": round(voltage_v, 3),
            "current_a": round(current_a, 6),
            "temperature_c": round(temperature_c, 3),
            "soc_percent": round(soc, 3),
            "status": status,
        }
        records.append(record)

    return records


def main() -> None:
    args = parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    devices_df = pd.read_csv(args.devices, dtype={"device_id": str, "site_id": str, "device_type": str})

    out_base = Path(args.raw_base)
    ensure_dir(out_base)

    files_written = []

    for day in range(args.days):
        day_start = start_date + timedelta(days=day)
        yyyy = f"{day_start.year:04d}"
        mm = f"{day_start.month:02d}"
        dd = f"{day_start.day:02d}"

        for _, row in devices_df.iterrows():
            device_id = str(row["device_id"]).strip()
            site_id = str(row["site_id"]).strip()
            capacity_kwh = float(row.get("capacity_kwh", 200))

            day_dir = out_base / yyyy / mm / dd / site_id
            ensure_dir(day_dir)
            out_file = day_dir / f"{device_id}.jsonl"

            records = generate_day_for_device(
                day_start_utc=day_start,
                rows_per_device=args.rows_per_device,
                device_id=device_id,
                site_id=site_id,
                capacity_kwh=capacity_kwh,
            )

            with open(out_file, "w", encoding="utf-8") as f:
                for rec in records:
                    f.write(json.dumps(rec) + "\n")

            files_written.append(str(out_file))

    unique_dirs = sorted({str(Path(p).parent) for p in files_written})
    print(f"Wrote {len(files_written)} files across {len(unique_dirs)} directories under '{out_base}'.")


if __name__ == "__main__":
    main()
