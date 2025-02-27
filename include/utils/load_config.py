from typing import Dict, Any

import yaml

def load_config() -> Dict[str, Any]:
    with open("include/config.yaml", "r") as file:
        config = yaml.safe_load(file)

    return config

config = load_config()