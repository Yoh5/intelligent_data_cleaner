"""Les jeux publics : ce qu'on n'aurait pas pensé à fabriquer.

Ignoré si `corpus-public/` est vide — ces fichiers ne sont pas versionnés (3,5 Mo
et des licences qui diffèrent), et `corpus-public/README.md` donne les URL.

Ce que ce fichier verrouille, et que le corpus fabriqué n'avait pas vu :

- **`?` n'est pas dans la liste du chargeur.** Elle connaît `???` mais pas `?`,
  et UCI Adult l'emploie sur 5,6 % de trois colonnes. La mesure large le
  rattrape ; sans elle, 1 700 vides passaient pour des modalités.
- **Les valeurs extrêmes de Titanic sont vraies.** Un tarif de 512 £ et une
  fratrie de 8 ne sont pas des erreurs de saisie, et un outil qui les écrête par
  défaut détruit les lignes les plus intéressantes du jeu.
"""
import asyncio
from pathlib import Path

import pytest

from app.services import agent_contrat

CORPUS = Path(__file__).resolve().parent.parent.parent / "corpus-public"
FICHIERS = sorted(CORPUS.glob("*.csv")) if CORPUS.exists() else []

pytestmark = pytest.mark.skipif(
    not FICHIERS,
    reason="corpus-public/ vide — voir corpus-public/README.md pour les URL")


@pytest.mark.parametrize("chemin", FICHIERS, ids=[f.name for f in FICHIERS])
def test_un_jeu_public_passe_sans_planter(chemin):
    """Le minimum : aucun fichier réel ne doit faire tomber la passe."""
    r = asyncio.run(agent_contrat.run(chemin.read_bytes(), chemin.name,
                                      usage="modele"))
    assert r["lignes"][1] > 0
    assert r["contrat"]["colonnes"], "un contrat vide n'est pas un contrat"


def _charger(nom: str):
    chemin = CORPUS / nom
    if not chemin.exists():
        pytest.skip(f"{nom} absent")
    return asyncio.run(agent_contrat.run(chemin.read_bytes(), nom, usage="modele"))


def test_uci_adult_voit_les_points_dinterrogation():
    """`?` manque à `DataProfiler.MISSING_PATTERNS`, qui a pourtant `???`."""
    r = _charger("uci-adult.csv")
    vus = [n for n in r["arbitrages"] if "non reconnu" in n and "'?'" in n]
    assert vus, r["arbitrages"]
    assert r["contrat_tenu"], r["enonce"]["non_garanti"]


def test_titanic_refuse_dimputer_age_et_cabine():
    """19,9 % et 77,1 % de vides : les combler fabriquerait la variable."""
    r = _charger("titanic.csv")
    refus = " ".join(r["enonce"]["refuse_exprès"])
    assert "age" in refus and "cabin" in refus, refus
    assert r["contrat_tenu"]


def test_titanic_ne_touche_pas_aux_valeurs_extremes_authentiques():
    """Le tarif de 512 £ est celui de la famille Cardeza, et la fratrie de 8
    celle de la famille Sage. Les écrêter détruirait les lignes qui portent
    l'intérêt du jeu — le défaut `signaler` existe pour ça."""
    import base64
    import io

    import pandas as pd
    r = _charger("titanic.csv")
    d = pd.read_csv(io.BytesIO(base64.b64decode(r["csv_base64"])))
    assert d["fare"].max() > 500, d["fare"].max()
    assert d["sibsp"].max() == 8, d["sibsp"].max()
    assert any("hors de l'intervalle plausible" in n for n in r["arbitrages"])
