from pathlib import Path
import json

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CLUSTER_CONFIG = PROJECT_ROOT / "configs" / "cluster.json"

def load_json(path: str | Path) -> dict:
    path = Path(path)
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)
    
def load_cluster_config(path: str | Path = DEFAULT_CLUSTER_CONFIG) -> dict:
    config = load_json(path)
    required_sections = ["spark", "minio", "iceberg", "storage"]

    for section in required_sections:
        if section not in config:
            raise ValueError(f"Missing required section '{section}' in cluster configuration.")
        
    return config