# -*- coding: utf-8 -*-
"""Tests du score de qualité déterministe (services/quality) — sans réseau."""
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from app.services import quality  # noqa: E402


def test_score_penalises_by_severity():
    analysis = {"issues": [
        {"severity": "critical"}, {"severity": "high"}, {"severity": "low"},
    ], "dataset_info": {"rows": 10, "columns": 3}}
    r = quality.score_from_analysis(analysis)
    assert r["score"] == 100 - (15 + 8 + 1)          # 76
    assert r["issues"] == 3
    assert r["by_severity"]["critical"] == 1


def test_clean_dataset_scores_100():
    r = quality.score_from_analysis({"issues": [], "dataset_info": {"rows": 5, "columns": 2}})
    assert r["score"] == 100 and r["issues"] == 0


def test_score_floored_at_zero():
    analysis = {"issues": [{"severity": "critical"}] * 20, "dataset_info": {}}
    assert quality.score_from_analysis(analysis)["score"] == 0


def test_completeness():
    df = pd.DataFrame({"a": [1, None, 3, 4], "b": [1, 2, 3, 4]})   # 1 manquant / 8 cellules
    assert quality.completeness(df) == 1 - 1 / 8
    assert quality.completeness(pd.DataFrame()) == 1.0
