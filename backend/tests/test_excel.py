"""Excel : la moitié annoncée du produit, que rien n'exerçait.

Les quatre routes acceptent `.xlsx` et `.xls`, le `CLAUDE.md` annonce
« CSV/Excel », et sur 173 tests **aucun n'ouvrait un classeur**. Le format
marchait — je l'ai vérifié à la main avant d'écrire ce fichier — mais rien ne
le retenait de cesser de marcher, et une capacité annoncée que personne
n'exerce est une capacité qu'on découvre cassée chez l'utilisateur.

Ce que ce fichier tient, et c'est plus fort que « ça répond 200 » :

> **Le même tableau donne le même verdict en .xlsx et en .csv.** Le format est
> un emballage ; s'il change le contrat, les types ou le nombre de lignes, ce
> n'est plus la donnée qui parle.

Avec une exception, et elle est légitime : un CSV n'a pas de types, donc la
colonne de dates y arrive en texte et le nettoyage ajoute une conversion. Le
classeur, lui, porte déjà un vrai `datetime`. Le compte de corrections diffère
donc de un — et ce test **épingle précisément cet écart-là**, pour qu'un autre
écart ne passe pas pour celui-ci.
"""
from __future__ import annotations

import io

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def tableau_sale() -> pd.DataFrame:
    """Un tableau avec les quatre saletés que le produit sait nommer :
    vides, aberrant, doublon, marqueur de vide déguisé en texte."""
    rng = np.random.default_rng(0)
    n = 200
    df = pd.DataFrame({
        "id": range(1, n + 1),
        "age": rng.integers(18, 80, n).astype(float),
        "revenu": rng.normal(2500, 600, n).round(2),
        "ville": rng.choice(["Casablanca", "Rabat", "Fes", "Tanger"], n),
        "inscrit_le": pd.date_range("2024-01-01", periods=n),
        "churn": rng.choice([0, 1], n),
    })
    df.loc[3:9, "revenu"] = np.nan
    df.loc[20, "age"] = 999
    df = pd.concat([df, df.iloc[[5]]], ignore_index=True)
    df.loc[30, "ville"] = "N/A"
    return df


def en_xlsx(df: pd.DataFrame) -> bytes:
    tampon = io.BytesIO()
    df.to_excel(tampon, index=False, engine="openpyxl")
    return tampon.getvalue()


def en_csv(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


@pytest.fixture(scope="module")
def deux_formats() -> dict[str, bytes]:
    df = tableau_sale()
    return {"xlsx": en_xlsx(df), "csv": en_csv(df)}


def poste(chemin: str, nom: str, contenu: bytes, **champs):
    return client.post(
        chemin,
        files={"file": (nom, contenu, "application/octet-stream")},
        data=champs or None,
    )


# --------------------------------------------------------------------------- #
# Chaque route, sur un classeur
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("chemin,champs", [
    ("/analyze/", {}),
    ("/contract-clean/propose", {}),
    ("/auto-clean/", {}),
    ("/prepare/", {"cible": "churn"}),
])
def test_chaque_route_accepte_un_classeur(chemin, champs, deux_formats):
    r = poste(chemin, "sale.xlsx", deux_formats["xlsx"], **champs)
    assert r.status_code == 200, f"{chemin} : {r.text[:400]}"


@pytest.mark.parametrize("chemin,champs", [
    ("/auto-clean/", {}),
    ("/prepare/", {"cible": "churn"}),
])
def test_un_classeur_annonce_mais_illisible_est_refuse_proprement(chemin, champs):
    """Le cas qu'on rencontre pour de vrai : un `.xls` qui est un export HTML
    renommé, un téléchargement coupé. L'extension est juste, le contenu non.

    Ces deux routes laissaient l'exception de pandas remonter : 500, et une
    trace où l'utilisateur apprenait qu'il existe un `_base.py` dans pandas.
    Ce qu'il doit lire, c'est quoi faire de son fichier.
    """
    r = poste(chemin, "menteur.xlsx", b"ceci n'est pas un classeur", **champs)
    assert r.status_code == 400, f"{r.status_code} — {r.text[:200]}"
    detail = r.json()["detail"]
    assert "Traceback" not in detail and "pandas" not in detail.lower()
    assert "menteur.xlsx" in detail and ".csv" in detail


def test_le_refus_ne_recopie_pas_le_contenu_du_fichier():
    """Un message d'erreur part dans les journaux et parfois dans un ticket.
    Y recopier une ligne du fichier, c'est y recopier des données."""
    secret = b"NOM;SALAIRE\nHabiba;41000\n" * 3
    r = poste("/auto-clean/", "paie.xlsx", secret)
    assert r.status_code == 400
    assert "Habiba" not in r.text and "41000" not in r.text


# --------------------------------------------------------------------------- #
# L'équivalence des deux formats
# --------------------------------------------------------------------------- #
def contrat_de(contenu: bytes, nom: str) -> dict:
    r = poste("/contract-clean/propose", nom, contenu)
    assert r.status_code == 200, r.text[:400]
    return r.json()


def test_le_contrat_propose_est_le_meme_dans_les_deux_formats(deux_formats):
    """Le cœur du fichier. Si les types divergent, tout ce qui suit — les
    plages, les formes, les réparations — porte sur autre chose selon que
    l'utilisateur a exporté en CSV ou gardé son classeur."""
    types = {}
    for ext, contenu in deux_formats.items():
        colonnes = contrat_de(contenu, f"sale.{ext}")["contrat"]["colonnes"]
        types[ext] = {c["nom"]: c["type"] for c in colonnes}

    assert types["xlsx"] == types["csv"], (
        "le format change les types inférés :\n"
        f"  xlsx : {types['xlsx']}\n  csv  : {types['csv']}"
    )
    # Et ce ne sont pas tous des chaînes : un contrat qui voit six colonnes de
    # texte « concorderait » sans rien avoir compris.
    assert types["csv"]["inscrit_le"] == "datetime"
    assert types["csv"]["revenu"] == "number"


def test_les_arbitrages_sont_les_memes_dans_les_deux_formats(deux_formats):
    """Les arbitrages sont ce que l'utilisateur apprend sur son fichier. Un
    arbitrage qui n'apparaît que dans un format est une leçon qu'il reçoit
    ou non selon son logiciel de tableur."""
    rendus = {
        ext: {a.split(":")[0].strip() for a in contrat_de(c, f"s.{ext}")["arbitrages"]}
        for ext, c in deux_formats.items()
    }
    assert rendus["xlsx"] == rendus["csv"], rendus


def test_la_preparation_garde_et_ecarte_les_memes_colonnes(deux_formats):
    plans = {}
    for ext, contenu in deux_formats.items():
        p = poste("/prepare/", f"sale.{ext}", contenu, cible="churn").json()["plan"]
        plans[ext] = (sorted(p["numeriques"]), sorted(p["categorielles"]),
                      sorted(r["nom"] for r in p["ecartees"]))
    assert plans["xlsx"] == plans["csv"], plans


# --------------------------------------------------------------------------- #
# Le seul écart admis, épinglé
# --------------------------------------------------------------------------- #
def corrections(contenu: bytes, nom: str) -> set[str]:
    d = poste("/auto-clean/", nom, contenu).json()
    entete = d["script"].split('"""')[1]
    return {l.strip().lstrip("#").strip().lstrip("-").strip()
            for l in entete.splitlines() if l.strip().startswith("#   -")}


def test_le_seul_ecart_entre_les_formats_est_la_date_en_texte(deux_formats):
    """Un CSV ne porte pas ses types : la colonne de dates y arrive en texte,
    et le nettoyage ajoute la conversion. Le classeur porte déjà un vrai
    `datetime`, donc il n'a rien à convertir.

    L'écart est donc juste — mais il doit rester **celui-là**. Épingler sa
    nature, et pas seulement le compte (4 contre 5), est ce qui empêche un
    futur écart de se cacher derrière lui.
    """
    xlsx, csv = corrections(deux_formats["xlsx"], "s.xlsx"), \
        corrections(deux_formats["csv"], "s.csv")

    assert xlsx <= csv, f"le classeur corrige ce que le CSV ignore : {xlsx - csv}"
    manquantes = csv - xlsx
    assert manquantes == {"date_as_string sur inscrit_le"}, (
        f"écart inattendu entre les formats : {manquantes}")


def test_les_deux_formats_corrigent_le_meme_fond(deux_formats):
    """Doublon, aberrant et vides : ils ne dépendent d'aucun format."""
    attendu = {"duplicate_rows", "outliers sur age",
               "missing_values sur revenu", "missing_values sur ville"}
    for ext, contenu in deux_formats.items():
        assert attendu <= corrections(contenu, f"s.{ext}"), ext
