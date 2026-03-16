import json
import random


def load_config(path="config/scenario_v1.json"):
    with open(path) as f:
        cfg = json.load(f)

    random.seed(cfg["run_settings"]["random_seed"])
    return cfg
