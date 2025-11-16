import os
from pathlib import Path

import environ

current_dir = Path(__file__).parent

env = environ.Env()
env.read_env(env_file=current_dir / os.getenv("ENV_FILE", ".env_fms22"))
