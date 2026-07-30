# -*- coding: utf-8 -*-
"""Tests du conseiller LLM (app.services.advisor) — sans réseau.

Le LLM est monkeypatché : aucun appel réseau, aucune clé requise. On vérifie le
mapping des choix, le respect des bornes et le comportement FAIL-OPEN (repli
rule-based) quand le LLM est absent / en erreur / renvoie du JSON illisible.
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from app.services import advisor  # noqa: E402


# Deux problèmes, chacun avec 3 stratégies candidates
_ISSUES = [
    {"type": "missing", "column": "age", "semantic_type": "numeric", "severity": "high",
     "strategies": [{"name": "médiane", "pros": [], "cons": []},
                    {"name": "moyenne", "pros": [], "cons": []},
                    {"name": "dropna", "pros": [], "cons": []}]},
    {"type": "duplicate", "column": None, "semantic_type": None, "severity": "medium",
     "strategies": [{"name": "keep first", "pros": [], "cons": []},
                    {"name": "keep last", "pros": [], "cons": []}]},
]


def _enable(monkeypatch, fake_llm):
    monkeypatch.setattr(advisor, "_api_key", lambda: "sk-test")
    monkeypatch.setattr(advisor, "_call_llm", fake_llm)


# ── FAIL-OPEN ──────────────────────────────────────────────────

def test_disabled_without_key_returns_empty(monkeypatch):
    monkeypatch.setattr(advisor, "_api_key", lambda: "")
    assert advisor.is_enabled() is False
    assert advisor.advise("d", {}, _ISSUES) == {}


def test_llm_error_is_failopen(monkeypatch):
    def _boom(_):
        raise RuntimeError("network down")
    _enable(monkeypatch, _boom)
    assert advisor.advise("d", {"age": "numeric"}, _ISSUES) == {}


def test_bad_json_is_failopen(monkeypatch):
    _enable(monkeypatch, lambda _: "pas du json")
    assert advisor.advise("d", {}, _ISSUES) == {}


def test_empty_issues_returns_empty(monkeypatch):
    _enable(monkeypatch, lambda _: json.dumps({"choices": []}))
    assert advisor.advise("d", {}, []) == {}


# ── MAPPING DES CHOIX ──────────────────────────────────────────

def test_maps_valid_choices(monkeypatch):
    fake = {"choices": [
        {"issue": 0, "recommended": 2, "rationale": "beaucoup de manquants"},
        {"issue": 1, "recommended": 1, "rationale": "garder le plus récent"},
    ]}
    _enable(monkeypatch, lambda _: json.dumps(fake))
    out = advisor.advise("d", {"age": "numeric"}, _ISSUES)
    assert out[0] == {"recommended": 2, "rationale": "beaucoup de manquants"}
    assert out[1]["recommended"] == 1


def test_out_of_range_strategy_index_ignored(monkeypatch):
    fake = {"choices": [
        {"issue": 0, "recommended": 9, "rationale": "hors bornes"},   # 3 stratégies → 9 invalide
        {"issue": 1, "recommended": 0, "rationale": "ok"},
    ]}
    _enable(monkeypatch, lambda _: json.dumps(fake))
    out = advisor.advise("d", {}, _ISSUES)
    assert 0 not in out          # ignoré → l'appelant garde recommended=0
    assert out[1]["recommended"] == 0


def test_out_of_range_issue_index_ignored(monkeypatch):
    fake = {"choices": [{"issue": 5, "recommended": 0, "rationale": "x"}]}
    _enable(monkeypatch, lambda _: json.dumps(fake))
    assert advisor.advise("d", {}, _ISSUES) == {}


def test_non_integer_choice_ignored(monkeypatch):
    fake = {"choices": [{"issue": "a", "recommended": "b"}, {"issue": 0, "recommended": 1, "rationale": "y"}]}
    _enable(monkeypatch, lambda _: json.dumps(fake))
    out = advisor.advise("d", {}, _ISSUES)
    assert out == {0: {"recommended": 1, "rationale": "y"}}
