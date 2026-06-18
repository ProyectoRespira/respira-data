from __future__ import annotations

import importlib
import sys
import unittest
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, patch


def _call(task_or_fn, *args, **kwargs):
    fn = getattr(task_or_fn, "fn", task_or_fn)
    return fn(*args, **kwargs)


def _import_social_module():
    sys.modules.pop("pipelines.tasks.social", None)

    telebot_module = SimpleNamespace(TeleBot=MagicMock())

    class DummyTweepyException(Exception):
        pass

    tweepy_module = SimpleNamespace(
        Client=MagicMock(),
        TweepyException=DummyTweepyException,
    )

    prefect_module = ModuleType("prefect")
    cache_policies_module = ModuleType("prefect.cache_policies")
    cache_policies_module.NO_CACHE = object()

    sqlalchemy_module = ModuleType("sqlalchemy")
    sqlalchemy_module.text = lambda value: value

    with patch.dict(
        sys.modules,
        {
            "telebot": telebot_module,
            "tweepy": tweepy_module,
            "prefect": prefect_module,
            "prefect.cache_policies": cache_policies_module,
            "sqlalchemy": sqlalchemy_module,
        },
    ):
        return importlib.import_module("pipelines.tasks.social")


class SocialTasksTestCase(unittest.TestCase):
    def test_build_x_message_matches_short_summary_format(self):
        social = _import_social_module()

        region = {
            "region_name": "Gran Asunción",
            "avg_aqi": 57,
            "max_aqi": 69,
            "min_aqi": 45,
        }

        message = _call(social.build_x_message, region)

        expected = (
            "📊 Calidad del Aire para Gran Asunción - Próximas 12 hs\n"
            "🔹 AQI Promedio: 57 (🟡 Moderado 🙂)\n"
            "🔺 AQI Máximo: 69 (🟡 Moderado 🙂)\n"
            "🔻 AQI Mínimo: 45 (🟢 Bueno 😁)\n"
            "\n"
            "🔗 Podés ingresar al pronóstico en tu zona en https://proyectorespira.net"
        )

        self.assertEqual(message, expected)
        self.assertLessEqual(len(message), 280)

    def test_build_x_message_truncates_long_region_and_stays_within_limit(self):
        social = _import_social_module()

        region = {
            "region_name": "Region Metropolitana " + ("X" * 250),
            "avg_aqi": 175,
            "max_aqi": 190,
            "min_aqi": 160,
        }

        message = _call(social.build_x_message, region)

        self.assertIn("AQI Promedio: 175", message)
        self.assertIn("AQI Máximo: 190", message)
        self.assertIn("AQI Mínimo: 160", message)
        self.assertIn("https://proyectorespira.net", message)
        self.assertLessEqual(len(message), 280)

    def test_post_to_x_rejects_messages_over_280_characters(self):
        social = _import_social_module()
        settings = SimpleNamespace(
            TWITTER_ENABLED=True,
            SOCIAL_DRY_RUN=True,
            TWITTER_BEARER_TOKEN="bearer",
            TWITTER_API_KEY="api-key",
            TWITTER_API_SECRET="api-secret",
            TWITTER_ACCESS_TOKEN="access-token",
            TWITTER_ACCESS_TOKEN_SECRET="access-secret",
        )

        with patch.object(social, "get_run_logger", return_value=MagicMock()):
            with self.assertRaisesRegex(
                ValueError,
                r"X message too long: 281 chars\. Limit is 280\.",
            ):
                _call(social.post_to_x, settings, "x" * 281)

        social.tweepy.Client.assert_not_called()
