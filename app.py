import pathlib
import sys

# Allow `import ec_poverty_monitor` when running Streamlit from repo root.
sys.path.insert(0, str(pathlib.Path(__file__).parent / "src"))

# Importing the module runs the Streamlit app.
from ec_poverty_monitor.dashboard.app import *  # noqa: F401,F403
