from typing import Dict, Any
import io

import yaml

def load_config() -> Dict[str, Any]:
    with open("config.yaml", "r") as file:
        config = yaml.safe_load(file)

    return config