"""La barrière en ligne de commande : `python -m app.cli check fichier --usage base`.

Ce qui compte est le CODE DE SORTIE, parce que c'est lui qu'une chaîne de
chargement ou une CI lit : 0 = le contrat est tenu, 1 = il ne l'est pas,
2 = on n'a pas pu en juger (fichier absent, illisible, usage inconnu).
Confondre 1 et 2 ferait passer une panne pour un verdict.

Les tests lancent un vrai sous-processus : un test qui appellerait `main()`
ne verrait ni le code de sortie réel ni l'encodage de la console.
"""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]


def idc(*args, cwd=None):
    env = dict(os.environ, PYTHONPATH=str(BACKEND), PYTHONIOENCODING="utf-8")
    return subprocess.run([sys.executable, "-m", "app.cli", *args], cwd=cwd or BACKEND,
                          env=env, capture_output=True, text=True, encoding="utf-8",
                          timeout=120)


@pytest.fixture
def avec_cle(tmp_path):
    f = tmp_path / "clients.csv"
    f.write_text("id,ville\n" + "\n".join(f"{i},Rabat" for i in range(60)) + "\n",
                 encoding="utf-8")
    return f


@pytest.fixture
def sans_cle(tmp_path):
    f = tmp_path / "ventes.csv"
    # 60 lignes toutes différentes, mais aucune colonne unique à elle seule.
    # (Une première version répétait 7 lignes : après retrait des doublons,
    # `montant` devenait unique et servait de clé — le cas ne testait rien.)
    villes = ["Rabat", "Fes", "Tanger"]
    f.write_text("ville,montant\n"
                 + "\n".join(f"{villes[i % 3]},{i // 3}" for i in range(60)) + "\n",
                 encoding="utf-8")
    return f


def test_contrat_tenu_sort_zero(avec_cle):
    r = idc("check", str(avec_cle), "--usage", "base")
    assert r.returncode == 0, r.stdout + r.stderr
    assert "TENU" in r.stdout and "NON TENU" not in r.stdout


def test_contrat_non_tenu_sort_un_et_dit_pourquoi(sans_cle):
    r = idc("check", str(sans_cle), "--usage", "base")
    assert r.returncode == 1, r.stdout + r.stderr
    assert "NON TENU" in r.stdout
    assert "clé" in r.stdout          # la raison, pas seulement le verdict


def test_le_meme_fichier_passe_pour_un_usage_sans_cle(sans_cle):
    assert idc("check", str(sans_cle), "--usage", "modele").returncode == 0


def test_fichier_absent_sort_deux_pas_un(tmp_path):
    r = idc("check", str(tmp_path / "nexiste_pas.csv"), "--usage", "base")
    assert r.returncode == 2
    assert "introuvable" in r.stderr


def test_usage_inconnu_sort_deux(avec_cle):
    assert idc("check", str(avec_cle), "--usage", "nimporte").returncode == 2


def test_fichier_sans_donnees_sort_deux(tmp_path):
    f = tmp_path / "vide.csv"
    f.write_text("id,ville\n", encoding="utf-8")
    r = idc("check", str(f), "--usage", "base")
    assert r.returncode == 2, r.stdout + r.stderr


def test_json_est_lisible_par_une_machine(sans_cle):
    r = idc("check", str(sans_cle), "--usage", "base", "--json")
    assert r.returncode == 1
    d = json.loads(r.stdout)           # rien d'autre que du JSON sur stdout
    assert d["contrat_tenu"] is False
    assert d["violations"][0]["code"] == "cle_absente"
    assert "csv_base64" not in d       # un verdict, pas les données


def test_sortie_ecrit_les_livrables_seulement_si_demande(avec_cle, tmp_path):
    dossier = tmp_path / "out"
    r = idc("check", str(avec_cle), "--usage", "base", "--sortie", str(dossier))
    assert r.returncode == 0
    assert (dossier / "clients_conforme.csv").exists()
    assert (dossier / "clients_contrat.json").exists()
    assert (dossier / "clients_conforme.parquet").exists()


def test_un_contrat_enregistre_est_reapplique(avec_cle, sans_cle, tmp_path):
    # Le contrat écrit pour un fichier sert de barrière aux livraisons
    # suivantes : un fichier qui n'a pas la colonne clé doit être refusé.
    dossier = tmp_path / "out"
    idc("check", str(avec_cle), "--usage", "base", "--sortie", str(dossier))
    contrat = dossier / "clients_contrat.json"
    assert idc("check", str(avec_cle), "--contrat", str(contrat)).returncode == 0
    r = idc("check", str(sans_cle), "--contrat", str(contrat))
    assert r.returncode == 1
    assert "id" in r.stdout
