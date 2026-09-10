import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests

try:
    from src.settings import BRONZE_ROOT, QUERY_PARAMS, SOURCE_URL, STATE_CODES
except ModuleNotFoundError:
    from settings import BRONZE_ROOT, QUERY_PARAMS, SOURCE_URL, STATE_CODES


def fetch_response_data() -> dict:
    try:
        response = requests.get(
            SOURCE_URL,
            params=QUERY_PARAMS.copy(),
            headers={"Accept": "application/json"},
            timeout=30,
        )
        response.raise_for_status()
        response_data = response.json()
    except (requests.RequestException, ValueError) as error:
        raise RuntimeError(f"API request failed: {error}") from error

    if "error" in response_data:
        raise RuntimeError(f"API returned an error: {response_data['error']}")
    return response_data


def validate_batch(response_data: dict) -> list[dict]:
    features = response_data.get("features")
    if not isinstance(features, list):
        raise RuntimeError("API response did not contain a features array")
    if not features:
        raise RuntimeError("API returned an empty CA/OR/WA batch")

    has_california = False
    for feature in features:
        attributes = feature.get("attributes", {})
        if attributes.get("State") == "CA":
            has_california = True
            break

    if not has_california:
        raise RuntimeError("API returned no California records")
    return features


def write_bronze(response_data: dict, run_id: str, output_root: Path = BRONZE_ROOT) -> Path:
    run_path = output_root / run_id
    run_path.mkdir(parents=True, exist_ok=False)
    with open(run_path / "payload.json", "w", encoding="utf-8") as response_file:
        json.dump(response_data, response_file, indent=2)

    metadata = {
        "ingest_run_id": run_id,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source_url": SOURCE_URL,
        "state_filter": list(STATE_CODES),
        "rows_in": len(response_data["features"]),
    }
    with open(run_path / "metadata.json", "w", encoding="utf-8") as metadata_file:
        json.dump(metadata, metadata_file, indent=2)

    return run_path


def main() -> None:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    response_data = fetch_response_data()
    validate_batch(response_data)
    output_path = write_bronze(response_data, run_id)
    print(f"Bronze written to {output_path}")


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1) from error
