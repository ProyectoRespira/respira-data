from __future__ import annotations

import os
import sys
import time
import urllib.error
import urllib.request


def main() -> int:
    api_url = os.getenv("PREFECT_API_URL", "http://prefect_server:4200/api").rstrip("/")
    health_url = f"{api_url}/health"
    timeout_s = float(os.getenv("PREFECT_WAIT_TIMEOUT_S", "120"))
    interval_s = float(os.getenv("PREFECT_WAIT_INTERVAL_S", "2"))
    deadline = time.monotonic() + timeout_s

    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(health_url, timeout=5) as response:
                if 200 <= response.status < 300:
                    return 0
        except (urllib.error.URLError, TimeoutError):
            pass
        time.sleep(interval_s)

    print(
        f"Timed out waiting for Prefect API health at {health_url} after {timeout_s:.0f}s",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
