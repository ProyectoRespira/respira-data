from prefect import flow, task
import subprocess


@task
def dbt_run_bronze() -> None:
    cmd = [
    "bash",
    "-lc",
    "poetry run dbt run --project-dir dbt --profiles-dir dbt --select models/bronze",
]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            "dbt run failed\n\nSTDOUT:\n"
            + result.stdout
            + "\n\nSTDERR:\n"
            + result.stderr
        )


@flow(name="bronze_pipeline")
def bronze_pipeline():
    dbt_run_bronze()


if __name__ == "__main__":
    bronze_pipeline()
