"""Tests du contrat de données.

Le test qui porte tout est `test_sortie_de_lagent_est_refusee` : il rejoue le
cas réel où l'ancien score annonçait 100/100 sur des données inutilisables, et
exige que le contrat le refuse. Si ce test passe au vert sans les autres, le
module n'a servi à rien.
"""
import pandas as pd
import pytest

from app.services import contract as C
from app.services import contract_infer as CI


# --------------------------------------------------------------------------- #
# Jeux de données
# --------------------------------------------------------------------------- #
def jeu_sale() -> pd.DataFrame:
    """Reproduit la forme du cas réel : vides textuels, casse, numérique en
    texte, lignes recopiées."""
    import random
    random.seed(7)
    lignes = [{
        "id": i,
        "ville": random.choice(["  Casablanca ", "Rabat", "rabat", "Fes"]),
        "revenu": None if i % 5 == 0 else float(random.randint(1200, 9000)),
        "age": random.choice([str(random.randint(18, 70)), "inconnu"]),
    } for i in range(100)]
    return pd.DataFrame(lignes + lignes[:10])


def contrat_simple() -> C.Contrat:
    return C.Contrat(
        colonnes=[
            C.ContratColonne("id", "integer", nullable=False, unique=True),
            C.ContratColonne("ville", "category", nullable=False),
            C.ContratColonne("revenu", "number", taux_nul_max=0.25),
            C.ContratColonne("age", "integer", taux_nul_max=0.6),
        ],
        cle_primaire=["id"],
    )


# --------------------------------------------------------------------------- #
# Le test qui justifie le module
# --------------------------------------------------------------------------- #
def test_sortie_de_lagent_est_refusee():
    """Imputer le marqueur « inconnu » et laisser Rabat/rabat doit échouer.

    C'est le scénario que `quality.score_from_analysis` notait 100/100, parce
    qu'il comptait les problèmes vus par le profileur plutôt que les garanties
    tenues par la donnée.
    """
    origine = jeu_sale()
    sale = origine.drop_duplicates().copy()
    sale["age"] = sale["age"].fillna("inconnu")          # le piège du mode
    sale["revenu"] = sale["revenu"].fillna(5020.5)       # médiane en masse

    r = C.verify(sale, contrat_simple(), origine=origine)

    codes = {v.code for v in r.violations}
    assert "marqueur_vide_en_valeur" in codes, "« inconnu » doit être vu"
    assert "collision_de_casse" in codes, "Rabat / rabat doit être vu"
    assert "type_inconvertible" in codes, "age en texte ne tient pas integer"
    assert r.bloquantes, "au moins une violation doit être bloquante"
    assert r.score <= C.PLAFOND_BLOQUANT
    assert not r.tient


def test_sortie_conforme_est_acceptee():
    """Et la contre-épreuve : un nettoyage correct doit passer à 100.

    Sans ce test, un contrôle trop sévère passerait pour un contrôle juste.
    C'est lui qui a trouvé que la normalisation des espaces était comptée comme
    une imputation.
    """
    origine = jeu_sale()
    propre = origine.drop_duplicates().copy()
    propre["ville"] = propre["ville"].str.strip().str.title()
    propre["age"] = pd.to_numeric(
        propre["age"].where(propre["age"] != "inconnu"), errors="coerce")

    r = C.verify(propre, contrat_simple(), origine=origine)
    assert r.tient, [v.message for v in r.violations]
    assert r.score == 100


# --------------------------------------------------------------------------- #
# Les deux pièges trouvés en contre-épreuve
# --------------------------------------------------------------------------- #
def test_normalisation_nest_pas_une_imputation():
    """Ramener «  Paris  » à « Paris » concentre la masse : c'est voulu."""
    origine = pd.DataFrame({"v": ["  Paris  "] * 50 + ["Paris"] * 50})
    propre = pd.DataFrame({"v": ["Paris"] * 100})
    ct = C.Contrat(colonnes=[C.ContratColonne("v", "string")], sans_doublons=False)
    r = C.verify(propre, ct, origine=origine)
    assert "masse_ponctuelle" not in {v.code for v in r.violations}


def test_deduplication_nest_pas_une_imputation():
    """Retirer des doublons porteurs de vides baisse le COMPTE de nuls sans
    qu'on ait imputé : on compare donc des taux."""
    base = pd.DataFrame({"x": [1.0, None, 3.0, 4.0]})
    origine = pd.concat([base, base], ignore_index=True)
    propre = base.copy()
    ct = C.Contrat(colonnes=[C.ContratColonne("x", "number", taux_nul_max=0.3)],
                   imputation_max=0.0)
    r = C.verify(propre, ct, origine=origine)
    assert "imputation_excessive" not in {v.code for v in r.violations}


def test_imputation_ponctuelle_est_vue():
    """Mais combler pour de vrai, et sur une seule valeur, doit être vu."""
    origine = pd.DataFrame({"x": [1.0, None, None, None, 5.0]})
    propre = pd.DataFrame({"x": [1.0, 3.0, 3.0, 3.0, 5.0]})
    ct = C.Contrat(colonnes=[C.ContratColonne("x", "number")],
                   imputation_max=0.2, sans_doublons=False)
    r = C.verify(propre, ct, origine=origine)
    codes = {v.code for v in r.violations}
    assert "imputation_excessive" in codes
    assert "masse_ponctuelle" in codes


# --------------------------------------------------------------------------- #
# Contrôles unitaires
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("valeurs,type_,attendu", [
    (["1", "2", "3"], "integer", 1.0),
    (["1", "2.5", "3"], "integer", 2 / 3),
    (["1 200", "3,5"], "number", 1.0),          # format européen
    (["oui", "non", "peut-être"], "boolean", 2 / 3),
    (["2024-01-01", "pas une date"], "datetime", 0.5),
    (["n'importe quoi"], "string", 1.0),
])
def test_part_convertible(valeurs, type_, attendu):
    part, _ = C.part_convertible(pd.Series(valeurs), type_)
    assert part == pytest.approx(attendu, abs=1e-6)


def test_plafond_bloquant_empeche_le_faux_100():
    """Une seule violation bloquante plafonne, même entourée de réussites."""
    df = pd.DataFrame({"k": [1, 1], "a": ["x", "y"]})
    ct = C.Contrat(colonnes=[C.ContratColonne("k", "integer", unique=True),
                             C.ContratColonne("a", "string")],
                   cle_primaire=["k"])
    r = C.verify(df, ct)
    assert r.satisfait, "des garanties tiennent par ailleurs"
    assert r.score <= C.PLAFOND_BLOQUANT


def test_cle_nulle_est_refusee():
    df = pd.DataFrame({"k": [1, None, 3]})
    ct = C.Contrat(colonnes=[C.ContratColonne("k", "integer")], cle_primaire=["k"])
    r = C.verify(df, ct)
    assert "cle_non_unique" in {v.code for v in r.violations}


def test_domaine_et_plage():
    df = pd.DataFrame({"s": ["a", "z"], "n": [5, 500]})
    ct = C.Contrat(colonnes=[
        C.ContratColonne("s", "string", domaine=["a", "b"]),
        C.ContratColonne("n", "integer", minimum=0, maximum=100)],
        sans_doublons=False)
    r = C.verify(df, ct)
    codes = {v.code for v in r.violations}
    assert "hors_domaine" in codes and "hors_plage" in codes


def test_rapport_serialisable():
    r = C.verify(pd.DataFrame({"a": [1]}),
                 C.Contrat(colonnes=[C.ContratColonne("a", "integer")]))
    import json
    json.dumps(r.to_dict())  # ne doit pas lever


def test_contrat_aller_retour_dict():
    ct = contrat_simple()
    assert C.Contrat.from_dict(ct.to_dict()).to_dict() == ct.to_dict()


def test_type_inconnu_refuse():
    with pytest.raises(ValueError):
        C.ContratColonne("x", "flottant")


def test_convertir_produit_le_type_declare():
    s = pd.Series(["1 200", "3,5", "zzz"])
    assert str(C.convertir(s, "number").dtype).startswith("float")
    assert str(C.convertir(pd.Series(["1", "2"]), "integer").dtype) == "Int64"
    assert C.convertir(s, "number").isna().sum() == 1  # « zzz » devient nul


# --------------------------------------------------------------------------- #
# Inférence
# --------------------------------------------------------------------------- #
def _analyse(df: pd.DataFrame):
    """Profile un DataFrame et rend (analyse, df tel que chargé).

    `asyncio.run` et non `get_event_loop` : depuis Python 3.12 ce dernier lève
    hors d'une coroutine.
    """
    import asyncio

    from app.services.profiler import DataProfiler
    p = DataProfiler()
    a = asyncio.run(p.analyze_file(df.to_csv(index=False).encode(), "t.csv"))
    return a, p.df


def test_inference_remesure_les_vides():
    """Le contrat ne doit pas hériter de l'angle mort du profileur.

    `MISSING_PATTERNS` connaît « Unknown » mais pas « inconnu ». Sans remesure,
    le plafond de nullité déclaré est intenable par la donnée propre.

    Les lignes portent un identifiant distinct exprès : sans lui, les 60
    « inconnu » sont 60 lignes **identiques**, que la déduplication réduit à
    une — et 2,4% de vides serait alors la mesure juste de ce que la réparation
    produit. Le piège vaut d'être noté : une colonne isolée ne permet pas de
    distinguer une valeur répétée d'une ligne recopiée.
    """
    df = pd.DataFrame({"id": range(100),
                       "a": ["inconnu"] * 60 + [str(i) for i in range(40)]})
    analyse, charge = _analyse(df)
    ct, notes = CI.inferer(analyse, charge, usage="modele")
    cc = ct.par_nom()["a"]
    assert cc.taux_nul_max > 0.5, "les 60% de « inconnu » doivent compter"
    assert any("non reconnu" in n for n in notes)


def test_les_taux_sont_mesures_apres_deduplication():
    """Un plafond calculé avant la réparation ne doit pas survivre à celle-ci.

    Le cas réel : 220 lignes dont 20 recopiées, `age` vide à 49,1% avant
    déduplication et 51,0% après. Le contrat déclarait 50,1% et sa propre
    réparation le violait.
    """
    base = pd.DataFrame({"id": range(200),
                         "age": ["inconnu"] * 100 + [str(i) for i in range(100)]})
    df = pd.concat([base, base.head(20)], ignore_index=True)
    analyse, charge = _analyse(df)
    ct, notes = CI.inferer(analyse, charge, usage="base")
    out, _ = __import__("app.services.reparer", fromlist=["x"]).appliquer(charge, ct)
    r = C.verify(out, ct, origine=charge)
    assert "taux_nul_depasse" not in {v.code for v in r.violations}, \
        [v.message for v in r.violations]
    assert any("après déduplication" in n for n in notes)


def test_inference_trouve_la_cle_malgre_les_doublons_francs():
    """30 lignes recopiées ne doivent pas disqualifier la vraie clé."""
    base = pd.DataFrame({"id": range(50), "v": range(50)})
    df = pd.concat([base, base.head(10)], ignore_index=True)
    assert CI.candidats_cle(df)[0] == "id"


def test_usage_change_les_budgets():
    df = pd.DataFrame({"id": range(20), "x": [1.0] * 20})
    analyse, charge = _analyse(df)
    modele, _ = CI.inferer(analyse, charge, usage="modele")
    base, _ = CI.inferer(analyse, charge, usage="base")
    assert base.imputation_max == 0.0
    assert modele.imputation_max > base.imputation_max
    assert base.lignes_perdues_max == 0.0


def test_usage_sans_cle_le_signale_quand_il_en_faut():
    # Aucune colonne unique même après déduplication : les 4 lignes sont
    # distinctes, mais chaque colonne ne porte que 2 modalités.
    df = pd.DataFrame({"a": [1, 1, 2, 2], "b": ["x", "y", "x", "y"]})
    analyse, charge = _analyse(df)
    _, notes = CI.inferer(analyse, charge, usage="jointure")
    assert any("identifie" in n for n in notes)


def test_resume_est_lisible():
    txt = CI.resume(contrat_simple())
    assert "clé primaire : id" in txt and "integer" in txt
