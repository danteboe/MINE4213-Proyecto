"""Fixtures compartidas de los tests de ingesta (sin Spark, sin red)."""

from __future__ import annotations

import pytest

from oceanwatch.common.config import load_config


@pytest.fixture(scope="session")
def cfg() -> dict:
    """Configuración real del proyecto (``conf/config.yaml``)."""
    return load_config()
