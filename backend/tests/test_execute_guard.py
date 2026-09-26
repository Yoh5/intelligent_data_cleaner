"""Ce que le serveur accepte d'exécuter, et avec quoi.

`POST /execute/` reçoit un script Python **du client** et le lance. C'est voulu :
l'utilisateur relit, corrige, puis exécute le script que l'application vient de
lui générer. Ce qui ne l'était pas, c'est que ce script héritait de
l'environnement du serveur — `OPENAI_API_KEY` compris — et que `stdout` est
renvoyé dans la réponse. Trois lignes suffisaient pour ressortir la clé.

Ces tests décrivent deux barrières :

1. l'environnement du sous-processus est construit, jamais hérité ;
2. le script doit avoir la forme de ce que le générateur produit — une liste
   blanche d'imports.

La deuxième est une défense en profondeur, **pas un bac à sable**. Du Python
arbitraire reste du Python arbitraire ; ce qui protège vraiment, c'est que le
processus ne détient plus rien qui vaille la peine d'être volé.
"""

import ast
import os

import pytest

from app.services.sandbox import ScriptRefused, check_script, subprocess_env


def test_the_child_environment_carries_no_secret():
    parent = dict(os.environ)
    parent["OPENAI_API_KEY"] = "sk-not-a-real-key"
    parent["AWS_SECRET_ACCESS_KEY"] = "also-not-real"

    child = subprocess_env(parent)

    assert "OPENAI_API_KEY" not in child
    assert "AWS_SECRET_ACCESS_KEY" not in child
    assert not [k for k in child if "KEY" in k or "TOKEN" in k or "SECRET" in k]


def test_the_child_environment_still_lets_python_start():
    """Vider l'environnement n'est utile que si le script tourne encore."""
    child = subprocess_env(dict(os.environ))

    assert child.get("PYTHONIOENCODING") == "utf-8"
    # Sous Windows, un interpréteur privé de SYSTEMROOT ne démarre pas.
    if os.name == "nt":
        assert "SYSTEMROOT" in child


def test_the_script_the_generator_produces_is_accepted():
    """La barrière qui casse le produit qu'elle protège n'est pas une barrière."""
    from app.routers.generate import _build_complete_script

    script = _build_complete_script("ventes.csv", [
        {"step_number": 1, "column": "prix", "issue_type": "missing_values",
         "code": "    df_clean['prix'] = df_clean['prix'].fillna(0)"},
    ])
    ast.parse(script)          # le générateur le valide déjà
    check_script(script)       # et il doit passer ici aussi


@pytest.mark.parametrize("script, why", [
    ("import os\nprint(os.environ['OPENAI_API_KEY'])", "os"),
    ("import subprocess\nsubprocess.run(['whoami'])", "subprocess"),
    ("import socket\nsocket.create_connection(('evil.example', 80))", "socket"),
    ("import urllib.request\nurllib.request.urlopen('https://evil.example')", "urllib"),
    ("from os import environ\nprint(environ)", "os"),
])
def test_a_script_reaching_outside_the_data_work_is_refused(script, why):
    with pytest.raises(ScriptRefused) as refusal:
        check_script(script)
    assert why in str(refusal.value)


@pytest.mark.parametrize("script", [
    "__import__('os').system('whoami')",
    "eval(\"__import__('os').environ\")",
    "exec('import os')",
    "print(getattr(__builtins__, '__import__')('os'))",
    "print(().__class__.__bases__[0].__subclasses__())",
])
def test_the_ways_around_an_import_list_are_refused_too(script):
    with pytest.raises(ScriptRefused):
        check_script(script)


def test_a_script_that_is_not_python_is_refused_before_anything_runs():
    with pytest.raises(ScriptRefused) as refusal:
        check_script("this is not python (")
    assert "syntaxe" in str(refusal.value).lower()


def test_pandas_and_numpy_remain_available():
    check_script(
        "import pandas as pd\n"
        "import numpy as np\n"
        "from pathlib import Path\n"
        "import argparse, logging, sys, warnings\n"
        "from typing import Optional\n"
        "df = pd.DataFrame({'a': [1, 2]})\n"
        "with open('x.csv', 'r', encoding='utf-8') as f:\n"
        "    f.read(1024)\n"
    )


def test_the_endpoint_refuses_the_exfiltration_that_used_to_work(tmp_path, monkeypatch):
    """Le chemin complet, pas seulement la fonction de garde.

    Avant : ce POST renvoyait la clé du serveur dans `logs`.
    """
    from fastapi.testclient import TestClient

    from app.main import app

    monkeypatch.setenv("OPENAI_API_KEY", "sk-canary-value-that-must-not-leak")
    csv = tmp_path / "d.csv"
    csv.write_text("a,b\n1,2\n", encoding="utf-8")

    with TestClient(app) as client:
        response = client.post(
            "/execute/",
            files={"file": ("d.csv", csv.read_bytes(), "text/csv")},
            data={"script": "import os\nprint(os.environ.get('OPENAI_API_KEY'))"},
        )

    assert response.status_code == 400
    assert "sk-canary-value-that-must-not-leak" not in response.text
    assert "os" in response.json()["detail"]


def test_a_real_generated_script_still_runs_with_the_stripped_environment(tmp_path):
    """La question qui compte : est-ce que le produit marche encore ?

    Vider l'environnement d'un sous-processus Python est le genre de
    durcissement qui casse l'interpréteur sur une plateforme et pas sur
    l'autre. Ce test lance vraiment le script, sur un vrai fichier.
    """
    from fastapi.testclient import TestClient

    from app.main import app
    from app.routers.generate import _build_complete_script

    script = _build_complete_script("ventes.csv", [
        {"step_number": 1, "column": "prix", "issue_type": "missing_values",
         "code": "    df_clean['prix'] = df_clean['prix'].fillna(0)"},
    ])
    csv = tmp_path / "ventes.csv"
    csv.write_text("prix,ville\n10,Casablanca\n,Rabat\n", encoding="utf-8")

    with TestClient(app) as client:
        response = client.post(
            "/execute/",
            files={"file": ("ventes.csv", csv.read_bytes(), "text/csv")},
            data={"script": script},
        )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["stats"]["null_before"] == 1 and body["stats"]["null_after"] == 0

    import base64
    cleaned = base64.b64decode(body["file_base64"]).decode("utf-8")
    assert "Casablanca" in cleaned and "0" in cleaned
