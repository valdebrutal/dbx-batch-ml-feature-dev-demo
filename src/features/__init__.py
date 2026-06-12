"""Shared Declarative Feature Store catalog and builders.

``SCROLL_CHURN_FEATURES`` lives here so the catalog and the training/prediction
builders share one feature selection.
"""

from __future__ import annotations

# Per-scenario feature selection by name. A different scenario is just a different
# list of names over the same MVs.
SCROLL_CHURN_FEATURES: list[str] = [
    "account_age_days",
    "win_rate_7d",
    "purchases_count_7d",
    "level_ups_7d",
    "current_level",
    "is_clan_member",
    "messages_7d",
    "sessions_7d",
    "avg_session_seconds_7d",
]
