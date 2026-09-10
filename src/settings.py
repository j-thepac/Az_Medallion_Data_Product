from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = PROJECT_ROOT / "config.yml"


def load_config() -> dict:
    with CONFIG_PATH.open(encoding="utf-8") as config_file:
        return yaml.safe_load(config_file)


CONFIG = load_config()
DATA_ROOT = PROJECT_ROOT / CONFIG["paths"]["data_root"]
BRONZE_ROOT = DATA_ROOT / CONFIG["paths"]["bronze"]
SILVER_ROOT = DATA_ROOT / CONFIG["paths"]["silver"]
GOLD_ROOT = DATA_ROOT / CONFIG["paths"]["gold"]
QUARANTINE_ROOT = DATA_ROOT / CONFIG["paths"]["quarantine"]
FIXTURE_PATH = PROJECT_ROOT / CONFIG["paths"]["fixture"]
SOURCE_URL = CONFIG["source"]["url"]
STATE_CODES = tuple(CONFIG["source"]["states"])
QUERY_PARAMS = {key: str(value) for key, value in CONFIG["query"].items()}