"""Calibration script for dynamic priority slider.

Sweeps balance slider s in [0,2] and records served job counts per priority.
Outputs CSV and optional PNG plot.

Usage:
    python scripts/calibrate_priority_slider.py \
        --data fixtures/small_city.json \
        --out artifacts/priority_sweep.csv \
        --plot artifacts/priority_sweep.png

Notes:
- Requires existing database or provided fixture JSON with jobs & locations structure.
- Simplifies by loading jobs from JSON instead of DB to keep calibration isolated.
"""
from __future__ import annotations
import argparse
import json
import csv
import os
from dataclasses import dataclass
from typing import List, Dict, Any

try:
    import matplotlib.pyplot as plt  # optional
except Exception:  # pragma: no cover
    plt = None

import numpy as np

from app.solver_pyvrp import PyVRPSolver
from app.models import Job, Location, Truck, ActionType
# Note: using simple_distance_matrix defined below (no external provider)


@dataclass
class Fixture:
    locations: List[Location]
    jobs: List[Job]


def load_fixture(path: str) -> Fixture:
    with open(path, "r") as f:
        data = json.load(f)
    # Expect structure {"locations": [...], "jobs": [...], "trucks": optional}
    locs = []
    id_map = {}
    for loc in data["locations"]:
        l = Location(id=loc["id"], name=loc["name"], address=loc.get("address", loc["name"]), lat=loc["lat"], lon=loc["lon"])
        locs.append(l)
        id_map[l.id] = l
    jobs = []
    for j in data["jobs"]:
        jobs.append(Job(id=j["id"], location_id=j["location_id"], action=ActionType(j.get("action", "pickup")), priority=j["priority"], date=j.get("date", "2025-09-15")))
    return Fixture(locs, jobs)


def simple_distance_matrix(locs: List[Location]) -> np.ndarray:
    # Fallback Euclidean minutes (speed 30 mph ~ 0.5 miles / minute); treat coords as lat/lon small area
    n = len(locs)
    mat = np.zeros((n, n), dtype=float)
    for i, a in enumerate(locs):
        for j, b in enumerate(locs):
            if i == j or a.lat is None or b.lat is None:
                continue
            dx = (a.lat - b.lat) * 69  # miles approx
            dy = (a.lon - b.lon) * 55  # adjust crude east-west shrink
            dist_miles = (dx * dx + dy * dy) ** 0.5
            minutes = dist_miles / 0.5  # 0.5 miles/minute
            mat[i, j] = minutes
    return mat


def estimate_service_times(locs: List[Location]) -> List[float]:
    # 15 minute default service for all except depot (index 0)
    return [0.0 if idx == 0 else 15.0 for idx, _ in enumerate(locs)]


def run_sweep(fixture: Fixture, out_csv: str, plot_path: str | None):
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    if plot_path:
        os.makedirs(os.path.dirname(plot_path), exist_ok=True)

    # Use first location as depot
    locations = fixture.locations
    jobs = fixture.jobs

    # Trucks: simple single large + one small
    trucks = [
        Truck(id=1, name="T1", max_weight_lb=10000, bed_len_ft=12, bed_width_ft=8, height_limit_ft=10, large_capable=True, large_truck=True),
        Truck(id=2, name="T2", max_weight_lb=8000, bed_len_ft=10, bed_width_ft=7, height_limit_ft=10, large_capable=False, large_truck=False),
    ]

    distance_matrix = simple_distance_matrix(locations)
    service_times = estimate_service_times(locations)

    # Minimal config shim
    class Cfg:  # pragma: no cover - simple struct
        class Solver:
            balance_slider: float = 1.0
        solver = Solver()
    config = Cfg()

    solver = PyVRPSolver(config)

    rows = []
    s_values = [round(x * 0.1, 2) for x in range(0, 21)]
    for s in s_values:
        config.solver.balance_slider = s
        result = solver.solve(trucks, jobs, distance_matrix, service_times, locations, time_limit_seconds=10)
        served_ids = {j for r in result.get("routes", []) for j in [job["job_id"] for job in r.get("jobs", [])]}
        served_P1 = sum(1 for j in jobs if j.priority == 1 and j.id in served_ids)
        served_P2 = sum(1 for j in jobs if j.priority == 2 and j.id in served_ids)
        served_P3 = sum(1 for j in jobs if j.priority == 3 and j.id in served_ids)
        served_Trump = sum(1 for j in jobs if j.priority == 0 and j.id in served_ids)
        total_cost = result.get("total_time", 0) + result.get("total_distance", 0)
        rows.append({
            "s": s,
            "total_cost": total_cost,
            "served_P1": served_P1,
            "served_P2": served_P2,
            "served_P3": served_P3,
            "served_Trump": served_Trump,
            "unassigned": len([j for j in jobs if j.id not in served_ids]),
        })

    with open(out_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    if plt and plot_path:
        fig, ax = plt.subplots(figsize=(7,4))
        ax.plot(s_values, [r["served_P1"] for r in rows], label="P1")
        ax.plot(s_values, [r["served_P2"] for r in rows], label="P2")
        ax.plot(s_values, [r["served_P3"] for r in rows], label="P3")
        ax.set_xlabel("Slider s")
        ax.set_ylabel("Jobs served")
        ax.set_title("Priority Serving vs Slider")
        ax.legend()
        fig.tight_layout()
        fig.savefig(plot_path)
        plt.close(fig)

    print(f"Wrote {out_csv} with {len(rows)} rows")
    if plot_path:
        print(f"Plot saved to {plot_path}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--plot", required=False, default=None)
    args = ap.parse_args()

    fixture = load_fixture(args.data)
    run_sweep(fixture, args.out, args.plot)


if __name__ == "__main__":  # pragma: no cover
    main()
