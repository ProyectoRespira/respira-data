from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import telebot
import tweepy
from sqlalchemy import text

from prefect.cache_policies import NO_CACHE

from pipelines.compat import get_run_logger, task


def _safe_project_table(project_code: str) -> str:
    normalized = project_code.strip().lower()
    if normalized != "respira_gold":
        raise ValueError(f"Unsupported project_code for social broadcast: {project_code}")
    return normalized


@task(name="extract_social_snapshot", cache_policy=NO_CACHE)
def extract_social_snapshot(
    engine,
    project_code: str,
    max_age_hours: int = 6,
    min_stations_per_region: int = 1,
) -> dict[str, Any]:
    logger = get_run_logger()
    schema = _safe_project_table(project_code)

    query = text(
        f"""
        with latest_success_run as (
            select id, as_of
            from {schema}.inference_runs
            where status = 'success'
            order by as_of desc
            limit 1
        ), station_forecast as (
            select
                r.id as region_id,
                r.name as region_name,
                s.id as station_id,
                (
                    select (point->>'value')::double precision
                    from jsonb_array_elements(ir.forecast_12h) as point
                    where point ? 'value'
                    order by (point->>'timestamp')::timestamptz asc nulls last
                    limit 1
                ) as aqi_value
            from {schema}.inference_results ir
            join latest_success_run lsr on lsr.id = ir.inference_run_id
            join {schema}.stations s on s.id = ir.station_id
            join {schema}.regions r on r.id = s.region_id
        )
        select
            lsr.as_of,
            sf.region_id,
            sf.region_name,
            count(*) filter (where sf.aqi_value is not null) as station_count,
            round(avg(sf.aqi_value)::numeric, 0)::int as avg_aqi,
            round(max(sf.aqi_value)::numeric, 0)::int as max_aqi,
            round(min(sf.aqi_value)::numeric, 0)::int as min_aqi
        from station_forecast sf
        cross join latest_success_run lsr
        where sf.aqi_value is not null
        group by lsr.as_of, sf.region_id, sf.region_name
        having count(*) filter (where sf.aqi_value is not null) >= :min_stations
        order by sf.region_name asc
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(query, {"min_stations": int(min_stations_per_region)}).mappings().all()

    if not rows:
        raise RuntimeError("No regional AQI rows were produced from latest inference snapshot")

    as_of = rows[0]["as_of"]
    as_of_utc = as_of if as_of.tzinfo else as_of.replace(tzinfo=timezone.utc)
    age = datetime.now(timezone.utc) - as_of_utc.astimezone(timezone.utc)
    if age > timedelta(hours=max_age_hours):
        raise RuntimeError(
            f"Latest inference snapshot is stale: as_of={as_of_utc.isoformat()} age_hours={age.total_seconds() / 3600:.2f}"
        )

    regions = [
        {
            "region_id": int(row["region_id"]),
            "region_name": str(row["region_name"]),
            "station_count": int(row["station_count"]),
            "avg_aqi": int(row["avg_aqi"]),
            "max_aqi": int(row["max_aqi"]),
            "min_aqi": int(row["min_aqi"]),
        }
        for row in rows
    ]

    logger.info(
        "Extracted social snapshot for project=%s as_of=%s regions=%s",
        project_code,
        as_of_utc.isoformat(),
        len(regions),
    )
    return {"project_code": project_code, "as_of": as_of_utc.isoformat(), "regions": regions}


@task(name="build_regional_average_message")
def build_regional_average_message(payload: dict[str, Any]) -> str:
    regions = payload.get("regions", [])
    if not regions:
        raise RuntimeError("Cannot build social message without regions")

    as_of = payload.get("as_of", "unknown")
    project_code = payload.get("project_code", "respira_gold")

    lines = [
        f"Respira AQI - {project_code}",
        f"Corte: {as_of}",
        "",
    ]

    for region in regions:
        lines.append(
            (
                f"{region['region_name']}: "
                f"prom {region['avg_aqi']} | "
                f"max {region['max_aqi']} | "
                f"min {region['min_aqi']} "
                f"({region['station_count']} estaciones)"
            )
        )

    lines.append("")
    lines.append("Fuente: respira_gold.inference_results + respira_gold.inference_runs")

    return "\n".join(lines)


@task(name="post_to_x", retries=2, retry_delay_seconds=30)
def post_to_x(settings, message: str, dry_run: bool | None = None) -> None:
    logger = get_run_logger()
    enabled = bool(settings.TWITTER_ENABLED)
    effective_dry_run = settings.SOCIAL_DRY_RUN if dry_run is None else bool(dry_run)

    if not enabled:
        logger.info("X posting disabled (TWITTER_ENABLED=false). Skipping.")
        return

    if effective_dry_run:
        logger.info("X dry-run enabled. Message preview:\n%s", message)
        return

    required = {
        "TWITTER_API_KEY": settings.TWITTER_API_KEY,
        "TWITTER_API_SECRET": settings.TWITTER_API_SECRET,
        "TWITTER_ACCESS_TOKEN": settings.TWITTER_ACCESS_TOKEN,
        "TWITTER_ACCESS_TOKEN_SECRET": settings.TWITTER_ACCESS_TOKEN_SECRET,
    }
    missing = [key for key, value in required.items() if not value]
    if missing:
        raise ValueError(f"Missing required X credentials: {', '.join(missing)}")

    client = tweepy.Client(
        bearer_token=settings.TWITTER_BEARER_TOKEN,
        consumer_key=settings.TWITTER_API_KEY,
        consumer_secret=settings.TWITTER_API_SECRET,
        access_token=settings.TWITTER_ACCESS_TOKEN,
        access_token_secret=settings.TWITTER_ACCESS_TOKEN_SECRET,
    )
    response = client.create_tweet(text=message)
    tweet_id = getattr(response, "data", {}) or {}
    logger.info("X post sent successfully. response=%s", tweet_id)


@task(name="post_to_telegram", retries=2, retry_delay_seconds=30)
def post_to_telegram(settings, message: str, dry_run: bool | None = None) -> None:
    logger = get_run_logger()
    enabled = bool(settings.TELEGRAM_ENABLED)
    effective_dry_run = settings.SOCIAL_DRY_RUN if dry_run is None else bool(dry_run)

    if not enabled:
        logger.info("Telegram posting disabled (TELEGRAM_ENABLED=false). Skipping.")
        return

    if effective_dry_run:
        logger.info("Telegram dry-run enabled. Message preview:\n%s", message)
        return

    if not settings.TELEGRAM_BOT_TOKEN or not settings.TELEGRAM_CHAT_ID:
        raise ValueError("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")

    bot = telebot.TeleBot(settings.TELEGRAM_BOT_TOKEN)
    bot.send_message(
        chat_id=settings.TELEGRAM_CHAT_ID,
        text=message,
        disable_web_page_preview=True,
    )
    logger.info("Telegram message sent successfully to chat_id=%s", settings.TELEGRAM_CHAT_ID)
