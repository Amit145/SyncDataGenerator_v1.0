import json
import random
from pathlib import Path


def load_config(path="config/scenario_v1.json"):
    config_path = Path(path)
    if not config_path.is_absolute():
        config_path = Path(__file__).resolve().parent.parent / config_path

    with config_path.open() as f:
        cfg = json.load(f)

    random.seed(cfg["run_settings"]["random_seed"])
    return cfg
