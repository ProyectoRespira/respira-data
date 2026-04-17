from __future__ import annotations

import json

from harness import TEST_DIR, dump_results, run_fake_inference


ARTIFACTS_DIR = TEST_DIR / "artifacts"


def main() -> None:
    payload = run_fake_inference()
    paths = dump_results(ARTIFACTS_DIR, payload)

    print("Inferencia local completada.")
    print(json.dumps(payload["meta"], indent=2, ensure_ascii=True))
    print("Archivos generados:")
    for name, path in paths.items():
        print(f"  - {name}: {path}")


if __name__ == "__main__":
    main()
