"""Le mode démo public : ce qui tourne sur un serveur ouvert à tous.

La route /execute fait tourner un script envoyé par le client (sous-processus, filtre
check_script). Un filtre par liste d'interdictions se contourne : sur un serveur public,
ce serait de l'exécution de code à distance. En mode démo, elle n'existe pas, et aucun
LLM n'est appelé (personne ne consomme le crédit du propriétaire).
"""
import importlib
import sys

import pytest
from fastapi.testclient import TestClient


def _oublier():
    for m in [m for m in sys.modules if m == "app.main" or m.startswith("app.config")]:
        del sys.modules[m]


@pytest.fixture(autouse=True)
def _remettre_l_app_normale(monkeypatch):
    # Ces tests rechargent app.main en mode démo ; sans ce nettoyage, les tests suivants
    # héritaient d'une app SANS /execute (vu sur test_execute_guard).
    yield
    monkeypatch.undo()
    _oublier()
    import app.config
    app.config.get_settings.cache_clear()
    importlib.reload(app.config)
    import app.main  # noqa: F401  — l'app par défaut, pour la suite


def _app(monkeypatch, demo: bool):
    monkeypatch.setenv("DEMO_PUBLIQUE", "true" if demo else "false")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-qui-ne-doit-pas-servir")
    _oublier()
    import app.config
    app.config.get_settings.cache_clear()
    importlib.reload(app.config)
    import app.main
    return app.main


def test_en_demo_publique_l_execution_de_script_n_existe_pas(monkeypatch):
    m = _app(monkeypatch, demo=True)
    client = TestClient(m.app)
    r = client.post("/execute/", files={"file": ("a.csv", b"x\n1\n")}, data={"script": "print(1)"})
    assert r.status_code == 404
    assert m.settings.OPENAI_API_KEY == ""            # la clé est ignorée en démo


def test_hors_demo_la_route_existe(monkeypatch):
    m = _app(monkeypatch, demo=False)
    assert any(getattr(r, "path", "").startswith("/execute") for r in m.app.routes)


def test_le_controle_de_fichier_reste_disponible_en_demo(monkeypatch):
    m = _app(monkeypatch, demo=True)
    assert any("/contract" in getattr(r, "path", "") for r in m.app.routes)
