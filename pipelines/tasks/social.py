from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import telebot
import tweepy
from prefect.cache_policies import NO_CACHE
from sqlalchemy import text

from pipelines.compat import get_run_logger, task


def _safe_project_table(project_code: str) -> str:
    normalized = project_code.strip().lower()
    if normalized != "respira_gold":
        raise ValueError(
            f"Unsupported project_code for social broadcast: {project_code}"
        )
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
        rows = (
            conn.execute(query, {"min_stations": int(min_stations_per_region)})
            .mappings()
            .all()
        )

    if not rows:
        raise RuntimeError(
            "No regional AQI rows were produced from latest inference snapshot"
        )

    as_of = rows[0]["as_of"]
    as_of_utc = as_of if as_of.tzinfo else as_of.replace(tzinfo=UTC)
    age = datetime.now(UTC) - as_of_utc.astimezone(UTC)
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
    return {
        "project_code": project_code,
        "as_of": as_of_utc.isoformat(),
        "regions": regions,
    }


_AQI_LEVELS = [
    (0, 50, "Bueno 😁"),
    (51, 100, "Moderado 🙂"),
    (101, 150, "Insalubre para grupos sensibles 😷"),
    (151, 200, "Insalubre 😶‍🌫️"),
    (201, 300, "Muy insalubre 😰"),
    (301, 500, "Peligroso 💀"),
]

_AQI_MESSAGES = [
    (0, 50, "🟢 ¡Hoy es un excelente día para estar al aire libre! 🏃‍♂️🍃"),
    (
        51,
        100,
        "🟡 Hoy es un buen día para estar al aire libre para la mayoría de las personas. 👍🏻",
    ),
    (101, 150, "🟠 Las personas sensibles deben prestar atención a síntomas 😷"),
    (
        151,
        200,
        "🔴 Hoy es un mal día para estar al aire libre.\nSe recomienda el uso de tapabocas para la población general 💨😷",
    ),
    (
        201,
        300,
        "🟣 Hoy es un muy mal día para estar al aire libre.\nSe recomienda el uso de tapabocas para la población general 🔥😷",
    ),
    (
        301,
        500,
        "⚫ Evite la exposición al aire libre. 💀😷\nAnte la aparición de síntomas, consulte a su médico.",
    ),
]

_AQI_MARKERS = [
    (0, 50, "🟢"),
    (51, 100, "🟡"),
    (101, 150, "🟠"),
    (151, 200, "🔴"),
    (201, 300, "🟣"),
    (301, 500, "⚫"),
]

_X_POST_MAX_LENGTH = 280


def _aqi_label(value: int) -> str:
    for lo, hi, label in _AQI_LEVELS:
        if lo <= value <= hi:
            return label
    return "Fuera del AQI 💀🤯"


def _aqi_message(value: int) -> str:
    for lo, hi, msg in _AQI_MESSAGES:
        if lo <= value <= hi:
            return msg
    return ""


def _aqi_marker(value: int) -> str:
    for lo, hi, marker in _AQI_MARKERS:
        if lo <= value <= hi:
            return marker
    return "⚠️"


def _truncate_text(value: str, max_chars: int) -> str:
    if max_chars <= 0:
        return ""
    if len(value) <= max_chars:
        return value
    if max_chars == 1:
        return value[:1]
    return f"{value[: max_chars - 1].rstrip()}…"


@task(name="build_regional_average_message")
def build_regional_average_message(region: dict[str, Any]) -> str:
    avg = region["avg_aqi"]
    max_ = region["max_aqi"]
    min_ = region["min_aqi"]
    region_name = region["region_name"]

    return (
        f"📊 *Reporte de Calidad del Aire para {region_name} - Pronóstico de 12 horas*\n"
        f"🔹 *AQI Promedio:* {avg} ({_aqi_label(avg)})\n"
        f"🔺 *AQI Máximo:* {max_} ({_aqi_label(max_)})\n"
        f"🔻 *AQI Mínimo:* {min_} ({_aqi_label(min_)})\n\n"
        f"{_aqi_message(avg)}\n\n"
        "🔗 Podés acceder al pronóstico actualizado en tu zona ingresando a www.proyectorespira.net"
    )


@task(name="build_x_message")
def build_x_message(region: dict[str, Any]) -> str:
    avg = region["avg_aqi"]
    max_ = region["max_aqi"]
    min_ = region["min_aqi"]
    region_name = str(region["region_name"])
    full_link = (
        "🔗 Podés ingresar al pronóstico en tu zona en https://proyectorespira.net"
    )
    short_link = "🔗 Pronóstico por zona: https://proyectorespira.net"
    link_only = "https://proyectorespira.net"

    def _format_aqi(value: int) -> str:
        return f"{_aqi_marker(value)} {_aqi_label(value)}"

    def _render(
        name: str,
        title_template: str,
        link_line: str,
    ) -> str:
        return "\n".join(
            (
                title_template.format(region=name),
                f"🔹 AQI Promedio: {avg} ({_format_aqi(avg)})",
                f"🔺 AQI Máximo: {max_} ({_format_aqi(max_)})",
                f"🔻 AQI Mínimo: {min_} ({_format_aqi(min_)})",
                "",
                link_line,
            )
        )

    def _fit_message(title_template: str, link_line: str) -> str:
        message = _render(region_name, title_template, link_line)
        if len(message) <= _X_POST_MAX_LENGTH:
            return message

        base_length = len(message) - len(region_name)
        region_budget = max(12, _X_POST_MAX_LENGTH - base_length)
        compact_region = _truncate_text(region_name, region_budget)
        return _render(compact_region, title_template, link_line)

    variants = (
        (
            "📊 Calidad del Aire para {region} - Próximas 12 hs",
            full_link,
        ),
        (
            "📊 Calidad del Aire para {region} - Próximas 12 hs",
            short_link,
        ),
        (
            "📊 Aire para {region} - Próximas 12 hs",
            short_link,
        ),
        (
            "📊 Aire para {region} - 12 hs",
            link_only,
        ),
    )

    for title_template, link_line in variants:
        message = _fit_message(title_template, link_line)
        if len(message) <= _X_POST_MAX_LENGTH:
            return message

    raise RuntimeError(
        f"Unable to build X message within {_X_POST_MAX_LENGTH} characters"
    )


@task(name="post_to_x", retries=2, retry_delay_seconds=30)
def post_to_x(settings, message: str, dry_run: bool | None = None) -> None:
    logger = get_run_logger()
    enabled = bool(settings.TWITTER_ENABLED)
    effective_dry_run = settings.SOCIAL_DRY_RUN if dry_run is None else bool(dry_run)

    if not enabled:
        logger.info("X posting disabled (TWITTER_ENABLED=false). Skipping.")
        return

    if len(message) > _X_POST_MAX_LENGTH:
        raise ValueError(
            f"X message too long: {len(message)} chars. Limit is {_X_POST_MAX_LENGTH}."
        )

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
        wait_on_rate_limit=True,
    )
    try:
        response = client.create_tweet(text=message)
    except tweepy.TweepyException as exc:
        logger.exception("X API error")
        raise RuntimeError(f"X API error: {exc}") from exc

    logger.info("X post sent successfully. response=%s", response.data)


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
        parse_mode="Markdown",
        disable_web_page_preview=True,
    )
    logger.info(
        "Telegram message sent successfully to chat_id=%s", settings.TELEGRAM_CHAT_ID
    )
