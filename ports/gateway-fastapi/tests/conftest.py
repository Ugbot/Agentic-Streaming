"""Make the ``gateway_fastapi`` package importable when pytest runs from anywhere."""

import sys
from pathlib import Path

# ports/gateway-fastapi/ holds the gateway_fastapi/ package.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
