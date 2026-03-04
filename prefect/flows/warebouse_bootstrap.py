import sys
from pathlib import Path

FLOWS_ROOT = Path(__file__).resolve().parent
if str(FLOWS_ROOT) not in sys.path:
    sys.path.insert(0, str(FLOWS_ROOT))

from warehouse_bootstrap import warehouse_bootstrap


if __name__ == "__main__":
    warehouse_bootstrap()
