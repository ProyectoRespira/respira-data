from __future__ import annotations

from pipelines.compat import flow, get_flow_context, get_run_logger
from pipelines.config.settings import get_settings
from pipelines.tasks.db import get_engine
from pipelines.tasks.notifications import notify_flow_failure
from pipelines.tasks.social import (
    build_regional_average_message,
    build_x_message,
    extract_social_snapshot,
    post_to_telegram,
    post_to_x,
)


@flow(name="social_broadcast")
def social_broadcast(
    project_code: str = "respira_gold", dry_run: bool | None = None
) -> None:
    logger = get_run_logger()
    settings = get_settings()
    engine = get_engine(settings)

    ctx = get_flow_context()
    ctx.update(
        {
            "project_code": project_code,
            "slack_webhook_url": settings.SLACK_WEBHOOK_URL,
            "flow_name": "social_broadcast",
        }
    )

    try:
        payload = extract_social_snapshot(
            engine,
            project_code=project_code,
            max_age_hours=settings.SOCIAL_DATA_MAX_AGE_HOURS,
            min_stations_per_region=settings.SOCIAL_MIN_STATIONS_PER_REGION,
        )
        regions = payload.get("regions", [])
        if not regions:
            raise RuntimeError("Social snapshot did not include any regions")

        effective_dry_run = (
            settings.SOCIAL_DRY_RUN if dry_run is None else bool(dry_run)
        )
        failures: list[tuple[str, Exception]] = []

        for region in regions:
            region_name = str(region.get("region_name", "unknown_region"))
            try:
                x_message = build_x_message(region)
                post_to_x(settings, x_message, dry_run=effective_dry_run)
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "X post failed for region=%s; continuing with next post",
                    region_name,
                )
                failures.append((f"X ({region_name})", exc))

        for region in regions:
            region_name = str(region.get("region_name", "unknown_region"))
            try:
                telegram_message = build_regional_average_message(region)
                post_to_telegram(
                    settings,
                    telegram_message,
                    dry_run=effective_dry_run,
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "Telegram post failed for region=%s; continuing with next post",
                    region_name,
                )
                failures.append((f"Telegram ({region_name})", exc))

        if failures:
            if len(failures) == 1:
                platform, exc = failures[0]
                raise RuntimeError(f"{platform} posting failed: {exc}") from exc

            failure_message = "; ".join(
                f"{platform} posting failed: {exc}" for platform, exc in failures
            )
            raise RuntimeError(failure_message)

        logger.info(
            "social_broadcast completed for project_code=%s dry_run=%s",
            project_code,
            effective_dry_run,
        )
    except Exception as exc:  # noqa: BLE001
        notify_flow_failure(ctx, str(exc))
        raise
    finally:
        engine.dispose()


if __name__ == "__main__":
    social_broadcast(project_code="respira_gold")
