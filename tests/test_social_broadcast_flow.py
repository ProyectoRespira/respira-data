from __future__ import annotations

import importlib
import sys
import unittest
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, call, patch


def _call(task_or_fn, *args, **kwargs):
    fn = getattr(task_or_fn, "fn", task_or_fn)
    return fn(*args, **kwargs)


def _import_social_broadcast_module():
    sys.modules.pop("pipelines.flows.social_broadcast", None)

    regions = [
        {
            "region_id": 1,
            "region_name": "Gran Asunción",
            "avg_aqi": 57,
            "max_aqi": 69,
            "min_aqi": 45,
        },
        {
            "region_id": 2,
            "region_name": "Luque",
            "avg_aqi": 88,
            "max_aqi": 102,
            "min_aqi": 70,
        },
    ]
    payload = {
        "project_code": "respira_gold",
        "as_of": "2026-06-18T00:00:00+00:00",
        "regions": regions,
    }
    settings = SimpleNamespace(
        SLACK_WEBHOOK_URL="https://example.test/slack",
        SOCIAL_DRY_RUN=True,
        SOCIAL_DATA_MAX_AGE_HOURS=6,
        SOCIAL_MIN_STATIONS_PER_REGION=1,
    )
    engine = MagicMock()

    settings_module = ModuleType("pipelines.config.settings")
    settings_module.get_settings = MagicMock(return_value=settings)

    db_module = ModuleType("pipelines.tasks.db")
    db_module.get_engine = MagicMock(return_value=engine)

    notifications_module = ModuleType("pipelines.tasks.notifications")
    notifications_module.notify_flow_failure = MagicMock()

    social_module = ModuleType("pipelines.tasks.social")
    social_module.extract_social_snapshot = MagicMock(return_value=payload)
    social_module.build_x_message = MagicMock(
        side_effect=lambda region: f"x:{region['region_name']}"
    )
    social_module.build_regional_average_message = MagicMock(
        side_effect=lambda region: f"telegram:{region['region_name']}"
    )
    social_module.post_to_x = MagicMock()
    social_module.post_to_telegram = MagicMock()

    with patch.dict(
        sys.modules,
        {
            "pipelines.config.settings": settings_module,
            "pipelines.tasks.db": db_module,
            "pipelines.tasks.notifications": notifications_module,
            "pipelines.tasks.social": social_module,
        },
    ):
        module = importlib.import_module("pipelines.flows.social_broadcast")

    return module, social_module, settings, engine, regions


class SocialBroadcastFlowTestCase(unittest.TestCase):
    def test_social_broadcast_posts_each_region_on_both_platforms(self):
        module, social_module, settings, engine, regions = (
            _import_social_broadcast_module()
        )

        _call(module.social_broadcast, project_code="respira_gold", dry_run=True)

        self.assertEqual(
            social_module.build_x_message.call_args_list,
            [call(regions[0]), call(regions[1])],
        )
        self.assertEqual(
            social_module.post_to_x.call_args_list,
            [
                call(settings, "x:Gran Asunción", dry_run=True),
                call(settings, "x:Luque", dry_run=True),
            ],
        )
        self.assertEqual(
            social_module.build_regional_average_message.call_args_list,
            [call(regions[0]), call(regions[1])],
        )
        self.assertEqual(
            social_module.post_to_telegram.call_args_list,
            [
                call(settings, "telegram:Gran Asunción", dry_run=True),
                call(settings, "telegram:Luque", dry_run=True),
            ],
        )
        engine.dispose.assert_called_once_with()
