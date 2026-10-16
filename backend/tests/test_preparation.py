"""La chaîne de prétraitement, et la seule chose qu'elle doit garantir.

Un test domine ce fichier : **la chaîne ne doit rien savoir du jeu de test.**
C'est la raison pour laquelle `CLAUDE.md` refusait la mise à l'échelle et
l'encodage, et c'est la raison pour laquelle ils sont enfin possibles — non
pas parce que la fuite est devenue acceptable, mais parce qu'on rend la
chaîne au lieu de transformer le fichier.

Ce test ne vérifie pas que le code *dit* séparer avant d'ajuster. Il compare
la moyenne qu'un `StandardScaler` a apprise à la moyenne du jeu
d'entraînement et à celle du fichier entier, et exige qu'elle soit la
première. Une assertion sur le texte du script passerait sur du code qui fuit.
"""
from __future__ import annotations

import subprocess
import sys

import numpy as np
import pandas as pd
import pytest

from app.services.contract import Contrat, ContratColonne
from app.services.contract import TYPES
from app.services.preparation import (BOOLEENS, CATEGORIELS, LIGNES_MIN,
                                      MODALITES_MAX, NUMERIQUES, TEMPORELS,
                                      planifier, script)

sklearn = pytest.importorskip(
    "sklearn", reason="scikit-learn n'est pas une dépendance de l'app : "
                      "seul le script engendré en a besoin")


def jeu(n: int = 300, graine: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(graine)
    df = pd.DataFrame({
        "id": range(1, n + 1),
        "age": rng.integers(18, 80, n).astype(float),
        "revenu": rng.normal(2500, 600, n).round(2),
        "ville": rng.choice(["Casablanca", "Rabat", "Fes", "Tanger"], n),
        "code": [f"REF-{i:05d}" for i in range(n)],
        "constante": ["x"] * n,
        "inscrit_le": pd.date_range("2024-01-01", periods=n).astype(str),
        "churn": rng.choice([0, 1], n),
    })
    df.loc[rng.choice(n, 20, replace=False), "revenu"] = np.nan
    return df


def contrat() -> Contrat:
    return Contrat(
        colonnes=[
            ContratColonne("id", "integer", unique=True),
            ContratColonne("age", "integer"),
            ContratColonne("revenu", "number"),
            ContratColonne("ville", "string"),
            ContratColonne("code", "string"),
            ContratColonne("constante", "string"),
            ContratColonne("inscrit_le", "datetime"),
            ContratColonne("churn", "integer"),
        ],
        cle_primaire=["id"],
    )


# --------------------------------------------------------------------------- #
# LE test
# --------------------------------------------------------------------------- #
def test_la_chaine_nest_ajustee_que_sur_lentrainement(tmp_path):
    """Mesuré, pas affirmé.

    On exécute le script engendré, on lit la moyenne que le `StandardScaler`
    a apprise, et on la compare aux deux moyennes possibles. Si elle vaut
    celle du fichier entier, la chaîne a vu le test.
    """
    df = jeu()
    chemin = tmp_path / "clients.csv"
    df.to_csv(chemin, index=False)
    module = tmp_path / "chaine.py"
    module.write_text(script(planifier(df, contrat(), cible="churn")),
                      encoding="utf-8")

    code = f'''
import sys; sys.path.insert(0, {str(tmp_path)!r})
from chaine import preparer, NUMERIQUES
chaine, (X_train, X_test, y_train, y_test) = preparer({str(chemin)!r})
ct = chaine.named_steps["colonnes"]
echelle = ct.named_transformers_["num"].named_steps["echelle"]
i = NUMERIQUES.index("age")
print("APPRIS", echelle.mean_[i])
print("TRAIN", X_train["age"].mean())
print("TOUT", X_train["age"].sum() + X_test["age"].sum(),
      len(X_train) + len(X_test))
'''
    fini = subprocess.run([sys.executable, "-c", code],
                          capture_output=True, text=True)
    assert fini.returncode == 0, fini.stderr[-800:]
    lignes = dict(l.split(" ", 1) for l in fini.stdout.strip().splitlines())
    appris = float(lignes["APPRIS"])
    train = float(lignes["TRAIN"])
    total, n = (float(x) for x in lignes["TOUT"].split())
    tout = total / n

    assert appris == pytest.approx(train, rel=1e-9), (
        "la moyenne apprise n'est pas celle de l'entraînement")
    assert appris != pytest.approx(tout, rel=1e-6), (
        "la moyenne apprise est celle du fichier entier : la chaîne a vu le "
        "jeu de test, et toute évaluation faite ensuite est optimiste")


def test_le_script_engendre_sexecute(tmp_path):
    """Un script qui ne tourne pas est un script qu'on livre quand même."""
    df = jeu()
    chemin = tmp_path / "d.csv"
    df.to_csv(chemin, index=False)
    module = tmp_path / "chaine.py"
    module.write_text(script(planifier(df, contrat(), cible="churn")),
                      encoding="utf-8")
    fini = subprocess.run([sys.executable, str(module), "--input", str(chemin)],
                          capture_output=True, text=True)
    assert fini.returncode == 0, fini.stderr[-800:]
    assert "ajust" in fini.stdout


# --------------------------------------------------------------------------- #
# Ce que le plan écarte, et pourquoi
# --------------------------------------------------------------------------- #
def test_la_cle_nest_pas_une_variable_explicative():
    plan = planifier(jeu(), contrat(), cible="churn")
    ecartees = {r.nom: r.raison for r in plan.ecartees}
    assert "id" in ecartees and "clé primaire" in ecartees["id"]


def test_la_haute_cardinalite_est_ecartee_avec_son_compte():
    plan = planifier(jeu(), contrat(), cible="churn")
    ecartees = {r.nom: r for r in plan.ecartees}
    assert "code" in ecartees
    assert ecartees["code"].modalites == 300
    assert str(MODALITES_MAX) in ecartees["code"].raison


def test_une_colonne_constante_est_ecartee():
    plan = planifier(jeu(), contrat(), cible="churn")
    assert "constante" in {r.nom for r in plan.ecartees}


def test_une_date_nest_pas_derivee_sans_quon_le_demande():
    """Dériver une date exige de savoir ce que le modèle doit apprendre."""
    plan = planifier(jeu(), contrat(), cible="churn")
    ecartees = {r.nom: r.raison for r in plan.ecartees}
    assert "inscrit_le" in ecartees and "date" in ecartees["inscrit_le"]


def test_chaque_exclusion_porte_une_raison():
    """Une colonne disparue sans explication est une colonne qu'on croira
    avoir utilisée."""
    for role in planifier(jeu(), contrat(), cible="churn").ecartees:
        assert role.raison and len(role.raison) > 20


def test_les_colonnes_retenues_sont_celles_quon_attend():
    plan = planifier(jeu(), contrat(), cible="churn")
    assert plan.numeriques == ["age", "revenu"]
    assert plan.categorielles == ["ville"]
    assert plan.cible == "churn"


# --------------------------------------------------------------------------- #
# Les refus
# --------------------------------------------------------------------------- #
def test_un_fichier_trop_court_le_dit_sans_refuser_la_chaine():
    plan = planifier(jeu(n=20), contrat(), cible="churn")
    assert any(str(LIGNES_MIN) in r for r in plan.refus)
    assert plan.utilisable, "la chaîne reste rendue : c'est l'évaluation qui ne vaut rien"


def test_une_cible_absente_est_dite_et_non_devinee():
    plan = planifier(jeu(), contrat(), cible="inexistante")
    assert plan.cible is None
    assert any("absente" in r for r in plan.refus)


def test_un_fichier_sans_colonne_utilisable_le_dit():
    df = pd.DataFrame({"id": range(1, 101), "code": [f"C{i}" for i in range(100)]})
    ct = Contrat(colonnes=[ContratColonne("id", "integer", unique=True),
                           ContratColonne("code", "string")],
                 cle_primaire=["id"])
    plan = planifier(df, ct)
    assert not plan.utilisable
    assert any("aucune colonne" in r for r in plan.refus)


# --------------------------------------------------------------------------- #
# Le script lui-même
# --------------------------------------------------------------------------- #
def test_le_script_dit_ce_quil_ecarte():
    texte = script(planifier(jeu(), contrat(), cible="churn"))
    for nom in ("id", "code", "constante", "inscrit_le"):
        assert nom in texte, f"{nom} est écartée sans que le script le dise"


def test_aucune_option_ne_permet_dajuster_avant_la_separation():
    """Un paramètre qui permettrait la fuite est un paramètre que quelqu'un
    finirait par passer."""
    texte = script(planifier(jeu(), contrat(), cible="churn"))
    i_sep = texte.index("train_test_split(")
    i_fit = texte.index("chaine.fit(")
    assert i_sep < i_fit, "l'ajustement précède la séparation dans le script"
    assert "fit_transform(X)" not in texte


def test_chaque_type_du_contrat_a_un_role():
    """Un type ajouté à `contract.TYPES` sans rôle ici serait encodé au jugé.

    Le module porte déjà cette assertion à l'import ; ce test la rend visible
    dans la suite plutôt qu'au premier `import` d'un autre module.
    """
    couverts = NUMERIQUES | TEMPORELS | BOOLEENS | CATEGORIELS
    assert set(TYPES) == couverts, (
        f"sans rôle : {sorted(set(TYPES) - couverts)} ; "
        f"en trop : {sorted(couverts - set(TYPES))}")
