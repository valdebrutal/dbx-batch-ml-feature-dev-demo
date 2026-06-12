"""Shared logging configuration for the demo entry-point scripts.

Each script calls configure_logging() first for uniform output and to quiet the
noisy third-party loggers.
"""

from __future__ import annotations

import logging

# Third-party loggers that flood the task log at INFO; pinned to CRITICAL.
_NOISY_LOGGERS: tuple[str, ...] = (
    "pyspark.sql.connect.logging",
    "pyspark.sql.connect.client",
    "databricks.ml_features.utils.training_scoring_utils",
    "py4j",
    "py4j.clientserver",
)

_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def configure_logging(level: int = logging.INFO) -> None:
    """Set the root logger format/level and silence the noisy third-party loggers.

    Call once at the top of each entry point's ``main()``. ``force=True`` resets any
    handler the serverless runtime installed before our code runs.
    """
    logging.basicConfig(level=level, format=_LOG_FORMAT, force=True)
    for noisy in _NOISY_LOGGERS:
        logging.getLogger(noisy).setLevel(logging.CRITICAL)
