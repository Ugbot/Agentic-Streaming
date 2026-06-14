import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))  # the agentic_pipeline package
sys.path.insert(0, str(_ROOT.parent / "pyagentic"))
