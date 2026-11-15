# -*- coding: utf-8 -*-
"""Shared fixtures: a Service on a temporary database/export tree."""

from __future__ import annotations

import pytest

from app import demo
from app.service import Service


@pytest.fixture
def service(tmp_path):
    return Service(tmp_path / "water_quality.sqlite3", tmp_path / "exports")


@pytest.fixture
def seeded(service):
    """The demo dataset: revision 1 with 24 pending readings (seqs 13-36)."""
    return demo.seed(service)
