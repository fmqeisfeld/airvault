from pathlib import Path
import yaml

CONF_DIR = Path(__file__).parent.parent
CONF_FILE='conf.yaml'
CONF_FILE_PATH = CONF_DIR / CONF_FILE
config_file_path = Path(CONF_FILE_PATH)

if config_file_path.exists():
    with open(config_file_path, "r") as config_file:
        config = yaml.safe_load(config_file)

print(yaml.dump(config, allow_unicode=True, default_flow_style=False))