#!/usr/bin/env python3
"""Fill missing mlb_id values in top_prospects.json.

Searches the MLB Stats API by name for each prospect with a null mlb_id,
picks the best match, and writes the updated file in place.

Usage:
    python prospects/fill_mlb_ids.py [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import time
import unicodedata
from pathlib import Path

import requests

PROSPECTS_FILE = Path("prospects/top_prospects.json")
BASE_URL = "https://statsapi.mlb.com/api/v1"
REQUEST_TIMEOUT = 15


def normalize(name: str) -> str:
    nfd = unicodedata.normalize("NFD", name)
    ascii_str = nfd.encode("ascii", "ignore").decode("ascii")
    return " ".join(ascii_str.lower().replace(".", "").replace("'", "").replace("-", " ").split())


def search_player(name: str) -> list[dict]:
    try:
        r = requests.get(
            f"{BASE_URL}/people/search",
            params={"names": name, "sportId": "11,12,13,14,1"},
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        return r.json().get("people", [])
    except Exception as exc:
        print(f"  API error: {exc}")
        return []


def best_match(name: str, candidates: list[dict]) -> dict | None:
    norm_target = normalize(name)
    # Exact normalized match first
    for c in candidates:
        if normalize(c.get("fullName", "")) == norm_target:
            return c
    # Last-name + first-initial match
    target_parts = norm_target.split()
    if len(target_parts) >= 2:
        for c in candidates:
            cparts = normalize(c.get("fullName", "")).split()
            if len(cparts) >= 2 and cparts[-1] == target_parts[-1] and cparts[0][0] == target_parts[0][0]:
                return c
    # Last-name only if unambiguous
    if len(target_parts) >= 2 and len(candidates) == 1:
        if normalize(candidates[0].get("fullName", "")).split()[-1] == target_parts[-1]:
            return candidates[0]
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Print matches without writing")
    args = parser.parse_args()

    data = json.loads(PROSPECTS_FILE.read_text(encoding="utf-8"))

    missing = [p for p in data if not p.get("mlb_id")]
    already = len(data) - len(missing)
    print(f"{len(data)} prospects total — {already} already have IDs, {len(missing)} to resolve\n")

    filled = 0
    not_found = []

    for prospect in missing:
        name = prospect["name"]
        print(f"Searching: {name} ({prospect.get('team', '?')} {prospect.get('position', '?')}) ...")
        candidates = search_player(name)

        match = best_match(name, candidates)
        if match:
            mlb_id = match["id"]
            full_name = match.get("fullName", name)
            print(f"  ✓ {full_name} → mlb_id={mlb_id}")
            prospect["mlb_id"] = mlb_id
            filled += 1
        else:
            names_found = [c.get("fullName") for c in candidates[:5]]
            print(f"  ✗ No match (candidates: {names_found or 'none'})")
            not_found.append(name)

        time.sleep(0.3)

    print(f"\n{'─' * 50}")
    print(f"Filled: {filled} / {len(missing)}")
    if not_found:
        print(f"Not found ({len(not_found)}):")
        for n in not_found:
            print(f"  - {n}")

    if args.dry_run:
        print("\n--dry-run: no changes written")
        return

    PROSPECTS_FILE.write_text(
        json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"\nWrote {PROSPECTS_FILE}")


if __name__ == "__main__":
    main()
