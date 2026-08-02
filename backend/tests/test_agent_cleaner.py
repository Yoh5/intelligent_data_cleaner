# -*- coding: utf-8 -*-
"""Tests de la boucle agentique de nettoyage (services/agent_cleaner).

Sans clé OpenAI, le conseiller est fail-open (recommended=0) → la boucle tourne
100% offline avec les stratégies rule-based. On vérifie qu'elle AMÉLIORE bien la
qualité des données réelles (missing, doublons, espaces superflus).
"""
import asyncio
import base64
import io
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from app.services import agent_cleaner  # noqa: E402

# CSV volontairement sale : espaces superflus, valeurs manquantes, ligne dupliquée.
_MESSY = (
    "name,age,city\n"
    " Alice ,30,Paris\n"
    "Bob,,Lyon\n"
    "Bob,,Lyon\n"                # doublon exact
    "Carol,42,Marseille\n"
    "David,,Lille\n"
).encode("utf-8")


def _run(content, filename="messy.csv"):
    return asyncio.run(agent_cleaner.run(content, filename, target=90, max_iters=3))


def test_loop_improves_quality():
    r = _run(_MESSY)
    assert r["quality_final"] >= r["quality_start"]
    assert r["issues_after"] <= r["issues_before"]
    assert len(r["iterations"]) >= 1
    assert r["quality_start"] < 100            # le dataset de départ a des problèmes


def test_cleaned_output_is_actually_clean():
    r = _run(_MESSY)
    df = pd.read_csv(io.BytesIO(base64.b64decode(r["cleaned_csv_base64"])))
    assert df.duplicated().sum() == 0                       # doublons retirés
    assert int(df["age"].isna().sum()) == 0                 # manquants imputés
    names = df["name"].astype(str)
    assert (names == names.str.strip()).all()               # espaces retirés
    assert r["rows_after"] <= r["rows_before"]


def test_reproducible_script_generated():
    r = _run(_MESSY)
    assert r["script"] and "import pandas" in r["script"]
    assert len(r["steps"]) >= 1
    assert all("strategy_name" in s for s in r["steps"])


def test_clean_dataset_is_noop():
    clean = b"name,age\nAlice,30\nBob,25\n"
    r = _run(clean, "clean.csv")
    assert r["issues_before"] == 0
    assert r["quality_final"] == 100
    assert r["steps"] == []
