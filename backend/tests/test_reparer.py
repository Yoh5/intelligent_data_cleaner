"""Tests des réparations sous contrat.

Chaque test correspond à un défaut réellement observé sur le CSV de sonde, pas à
une hypothèse : l'imputation du mot « inconnu » 247 fois, les deux Rabat, le
tiers de colonne écrasé sur la médiane, la sortie entièrement en texte.
"""
import asyncio

import pandas as pd
import pytest

from app.services import agent_contrat, contract as C, contract_infer as CI
from app.services import reparer as R
from app.services.profiler import DataProfiler


def contrat(**kw) -> C.Contrat:
    base = dict(
        colonnes=[
            C.ContratColonne("id", "integer", nullable=False, unique=True),
            C.ContratColonne("ville", "category", nullable=False),
            C.ContratColonne("age", "integer", taux_nul_max=0.7),
        ],
        cle_primaire=["id"],
    )
    base.update(kw)
    return C.Contrat(**base)


# --------------------------------------------------------------------------- #
# Les quatre défauts observés
# --------------------------------------------------------------------------- #
def test_le_marqueur_vide_devient_un_vide():
    """« inconnu » ne doit plus pouvoir être le mode de sa colonne."""
    df = pd.DataFrame({"a": ["inconnu", "N/A", " - ", "12", None]})
    j = R.Journal()
    out = R.vider_les_marqueurs(df.copy(), j)
    assert out["a"].notna().sum() == 1
    assert any(g.action == "marqueurs_vides" for g in j.faits)


def test_la_casse_est_fusionnee_vers_la_bonne_graphie():
    """'rabat' était plus fréquent que 'Rabat' (104 contre 95) et sortait
    gagnant au seul comptage. À quasi-égalité, la casse tranche."""
    df = pd.DataFrame({"ville": ["rabat"] * 104 + ["Rabat"] * 95})
    ct = C.Contrat(colonnes=[C.ContratColonne("ville", "category")],
                   sans_doublons=False)
    out, j = R.appliquer(df, ct)
    assert set(out["ville"].dropna().unique()) == {"Rabat"}


def test_la_casse_suit_la_frequence_quand_lecart_est_net():
    """Mais une majorité franche reste la majorité : pas de règle esthétique
    qui écrase les données."""
    df = pd.DataFrame({"v": ["sarl"] * 100 + ["SARL"] * 3})
    ct = C.Contrat(colonnes=[C.ContratColonne("v", "string")],
                   sans_doublons=False)
    out, _ = R.appliquer(df, ct)
    assert set(out["v"].dropna().unique()) == {"sarl"}


def test_title_nest_jamais_applique():
    """`.title()` abîmerait les acronymes : on ne choisit que parmi les graphies
    présentes."""
    df = pd.DataFrame({"v": ["iPhone"] * 10})
    ct = C.Contrat(colonnes=[C.ContratColonne("v", "string")], sans_doublons=False)
    out, _ = R.appliquer(df, ct)
    assert out["v"].iloc[0] == "iPhone"


def test_limputation_hors_budget_est_refusee():
    """LE test : combler un tiers d'une colonne doit être refusé, pas fait."""
    df = pd.DataFrame({"x": [1.0, 2.0, 3.0] + [None] * 7})
    ct = C.Contrat(colonnes=[C.ContratColonne("x", "number", taux_nul_max=1.0)],
                   imputation_max=0.20, sans_doublons=False)
    out, j = R.appliquer(df, ct)
    assert out["x"].isna().sum() == 7, "les vides doivent rester vides"
    refus = [g for g in j.refus if g.action == "imputation"]
    assert refus and "budget" in refus[0].detail


def test_limputation_dans_le_budget_est_faite_avec_indicatrice():
    df = pd.DataFrame({"x": [1.0] * 18 + [None] * 2})
    ct = C.Contrat(colonnes=[C.ContratColonne("x", "number")],
                   imputation_max=0.20, sans_doublons=False)
    out, j = R.appliquer(df, ct)
    assert out["x"].isna().sum() == 0
    assert "x_impute" in out.columns, "une valeur inventée doit rester repérable"
    assert out["x_impute"].sum() == 2


def test_pas_dindicatrice_sous_le_seuil():
    df = pd.DataFrame({"x": [1.0] * 99 + [None]})
    ct = C.Contrat(colonnes=[C.ContratColonne("x", "number")],
                   imputation_max=0.20, sans_doublons=False)
    out, _ = R.appliquer(df, ct)
    assert "x_impute" not in out.columns


def test_le_mode_ne_peut_plus_etre_un_marqueur():
    """L'ordre des passes est ce qui protège : les marqueurs sont vidés avant
    que l'imputation ne cherche un mode."""
    df = pd.DataFrame({"c": ["inconnu"] * 6 + ["bleu"] * 13 + [None]})
    ct = C.Contrat(colonnes=[C.ContratColonne("c", "category")],
                   imputation_max=0.5, sans_doublons=False)
    out, _ = R.appliquer(df, ct)
    assert "inconnu" not in set(out["c"].dropna().astype(str))
    assert out["c"].dropna().mode().iloc[0] == "bleu"


def test_la_sortie_est_typee():
    """Le typage est le livrable, pas un effet de bord."""
    df = pd.DataFrame({"id": ["1", "2"], "ville": ["a", "b"], "age": ["30", "40"]})
    out, _ = R.appliquer(df, contrat())
    assert str(out["id"].dtype) == "Int64"
    assert str(out["age"].dtype) == "Int64"


# --------------------------------------------------------------------------- #
# Déduplication
# --------------------------------------------------------------------------- #
def test_doublons_francs_puis_conflits_de_cle():
    """Deux choses différentes : une ligne recopiée, et deux lignes qui
    partagent la clé en divergeant ailleurs."""
    df = pd.DataFrame({"id": ["1", "1", "2", "2"],
                       "ville": ["a", "a", "b", "c"],
                       "age": ["1", "1", "2", "3"]})
    out, j = R.appliquer(df, contrat())
    assert len(out) == 2
    actions = {g.action for g in j.faits}
    assert "doublons" in actions and "cle" in actions
    conflit = [g for g in j.faits if g.action == "cle"][0]
    assert "règle métier" in conflit.detail, "le choix arbitraire doit être dit"


def test_les_lignes_a_cle_vide_sont_retirees():
    df = pd.DataFrame({"id": ["1", None], "ville": ["a", "b"], "age": ["1", "2"]})
    out, j = R.appliquer(df, contrat())
    assert len(out) == 1
    assert any("clé vide" in g.detail for g in j.faits)


# --------------------------------------------------------------------------- #
# Réparer doit satisfaire ce que vérifier exige
# --------------------------------------------------------------------------- #
def test_reparer_satisfait_le_contrat_quil_vise():
    """La boucle est fermée : ce que `reparer` produit doit passer `verify`.

    Si ce test casse, c'est que les deux modules ont divergé — et c'est
    exactement la divergence que l'ancien système avait, entre ce qu'il
    nettoyait et ce qu'il mesurait.
    """
    import random
    random.seed(11)
    lignes = [{
        "id": i,
        "ville": random.choice(["  Casablanca ", "Rabat", "rabat"]),
        "age": random.choice([str(random.randint(18, 70)), "inconnu"]),
    } for i in range(120)]
    df = pd.DataFrame(lignes + lignes[:12])
    ct = contrat()
    out, _ = R.appliquer(df, ct)
    r = C.verify(out, ct, origine=df)
    assert r.tient, [v.message for v in r.violations]


def test_reparer_ne_cree_pas_de_masse_ponctuelle():
    """Une réparation qui respecte son budget ne peut pas violer la clause de
    masse ponctuelle : les deux seuils sont le même nombre."""
    df = pd.DataFrame({"x": [1.0, 2.0, 3.0, 4.0, None]})
    ct = C.Contrat(colonnes=[C.ContratColonne("x", "number")],
                   imputation_max=0.20, sans_doublons=False)
    out, _ = R.appliquer(df, ct)
    r = C.verify(out, ct, origine=df)
    assert "masse_ponctuelle" not in {v.code for v in r.violations}


# --------------------------------------------------------------------------- #
# La passe complète
# --------------------------------------------------------------------------- #
def _csv() -> bytes:
    import random
    random.seed(7)
    lignes = [{
        "id": i,
        "ville": random.choice(["  Casablanca ", "Rabat", "rabat", "Fes"]),
        "revenu": "" if i % 5 == 0 else f"{random.randint(1200, 9000)}",
        "age": random.choice([str(random.randint(18, 70)), "N/A", "inconnu"]),
        "note": "" if i % 3 == 0 else f"{random.uniform(0, 20):.1f}",
    } for i in range(400)]
    return pd.DataFrame(lignes + lignes[:30]).to_csv(index=False).encode()


def test_passe_complete_tient_le_contrat():
    r = asyncio.run(agent_contrat.run(_csv(), "sonde.csv", usage="modele"))
    assert r["contrat_tenu"], r["enonce"]["non_garanti"]
    assert r["score_apres"] == 100
    assert r["score_avant"] < 60
    assert r["lignes"] == [430, 400], "30 doublons francs, rien de plus"


def test_la_passe_enonce_ses_refus():
    """Le livrable n'est pas un score : c'est ce qui est garanti, et ce qui ne
    peut pas l'être."""
    r = asyncio.run(agent_contrat.run(_csv(), "sonde.csv", usage="modele"))
    assert len(r["enonce"]["garanti"]) >= 8
    assert len(r["enonce"]["refuse_exprès"]) == 3, "revenu, age et note"
    assert all("budget" in t for t in r["enonce"]["refuse_exprès"])


def test_lusage_change_le_resultat():
    """La même donnée, deux cibles, deux contrats."""
    a = asyncio.run(agent_contrat.run(_csv(), "s.csv", usage="modele"))
    b = asyncio.run(agent_contrat.run(_csv(), "s.csv", usage="base"))
    assert a["contrat"]["imputation_max"] > b["contrat"]["imputation_max"]
    assert b["contrat"]["lignes_perdues_max"] == 0.0


def test_le_type_est_reinfere_apres_nettoyage():
    """`age` contenait « inconnu » : le profileur la classait en texte, et le
    contrat déclarait une colonne d'âges en `string`."""
    r = asyncio.run(agent_contrat.run(_csv(), "s.csv", usage="modele"))
    types = {c["nom"]: c["type"] for c in r["contrat"]["colonnes"]}
    assert types["age"] == "integer", types
    assert any("marqueurs de vide retirés" in n for n in r["arbitrages"])


def test_la_passe_livre_du_parquet():
    """Le CPV reperd les types à la relecture ; Parquet les conserve."""
    r = asyncio.run(agent_contrat.run(_csv(), "s.csv", usage="modele"))
    assert r["parquet_base64"], "pyarrow absent ? le livrable perdrait ses types"
    import base64
    import io
    d = pd.read_parquet(io.BytesIO(base64.b64decode(r["parquet_base64"])))
    assert str(d["id"].dtype) == "Int64"


def test_le_script_genere_est_valide():
    import ast
    r = asyncio.run(agent_contrat.run(_csv(), "s.csv", usage="modele"))
    ast.parse(r["script"])
    assert "refus" in r["script"] or "refusé" in r["script"].lower()


def test_la_passe_est_serialisable():
    import json
    r = asyncio.run(agent_contrat.run(_csv(), "s.csv", usage="modele"))
    json.dumps(r)


def test_un_contrat_fourni_est_respecte():
    """L'utilisateur doit pouvoir éditer le contrat proposé."""
    ct = C.Contrat(colonnes=[C.ContratColonne("id", "integer", unique=True)],
                   cle_primaire=["id"], imputation_max=0.0)
    r = asyncio.run(agent_contrat.run(_csv(), "s.csv", contrat=ct.to_dict()))
    assert r["contrat"]["imputation_max"] == 0.0
    assert len(r["contrat"]["colonnes"]) == 1


# --------------------------------------------------------------------------- #
# Ce que de VRAIS fichiers ont cassé
# --------------------------------------------------------------------------- #
def test_une_colonne_booleenne_ne_fait_pas_planter_le_profileur():
    """`is_numeric_dtype` répond True pour un booléen.

    La colonne partait donc dans la branche numérique jusqu'au test d'aberrance
    par quantiles, où numpy refuse de soustraire deux booléens. Un fichier de
    17 colonnes dont 8 booléennes faisait planter toute l'analyse — trouvé sur
    `SampleSubmission.csv` du Bias Bounty, pas sur un cas fabriqué.
    """
    from app.services.profiler import DataProfiler
    df = pd.DataFrame({"id": range(40), "defini": [i % 2 == 0 for i in range(40)],
                       "score": [i / 40 for i in range(40)]})
    a = asyncio.run(DataProfiler().analyze_file(
        df.to_csv(index=False).encode(), "b.csv"))
    assert a["columns"]["defini"]["semantic_type"] == "categorical"
    assert all(i["column"] != "defini" or i["issue"] != "outliers"
               for i in a["issues"])


def test_une_colonne_de_codes_ne_devient_pas_numerique():
    """Le cas `StockCode` du jeu Online Retail.

    2 684 références numériques et 424 codes alphanumériques comme `15044B`. Le
    profileur classe ça « numeric-mixed » à 86 %, et le contrat le traduisait en
    entier : la conversion mettait 424 lignes à vide, soit 13,6 % de la colonne,
    et le contrat se trouvait violé par sa propre réparation.

    *Mixte* ne veut pas dire *numérique à 86 %* : ça veut dire que la colonne
    porte des codes, et un code n'est pas une quantité.
    """
    from app.services.profiler import DataProfiler
    codes = [str(10000 + i) for i in range(260)] + [f"{15044 + i}B" for i in range(40)]
    df = pd.DataFrame({"id": range(300), "stockcode": codes})
    prof = DataProfiler()
    analyse = asyncio.run(prof.analyze_file(df.to_csv(index=False).encode(), "t.csv"))
    charge = prof.df
    ct, notes = CI.inferer(analyse, charge, usage="modele")
    assert ct.par_nom()["stockcode"].type == "string", ct.to_dict()
    assert any("ne tient pas le type" in n for n in notes)

    out, _ = R.appliquer(charge, ct)
    assert out["stockcode"].notna().all(), "aucune valeur ne doit être perdue"
    assert C.verify(out, ct, origine=charge).tient


def test_un_fichier_reel_tient_son_contrat():
    """Telco Customer Churn : 7 043 lignes, et `TotalCharges` en texte avec des
    vides. Le cas d'école, et il n'était pas couvert."""
    import pathlib
    f = (pathlib.Path(__file__).parent.parent.parent / "test"
         / "WA_Fn-UseC_-Telco-Customer-Churn.csv")
    if not f.exists():
        pytest.skip("jeu Telco absent")
    r = asyncio.run(agent_contrat.run(f.read_bytes(), f.name, usage="modele"))
    assert r["contrat_tenu"], r["enonce"]["non_garanti"]
    assert r["lignes"][1] == 7043, "aucune ligne ne doit disparaître"
    types = {c["nom"]: c["type"] for c in r["contrat"]["colonnes"]}
    assert types["totalcharges"] in ("number", "integer"), types["totalcharges"]


def test_la_route_refuse_ce_quelle_ne_peut_pas_tenir():
    """Une limite qu'on ne peut pas honorer est pire qu'une limite plus basse."""
    from fastapi.testclient import TestClient

    from app.main import app
    from app.routers import contract_clean as CC
    gros = b"a,b\n" + b"1,2\n" * 10
    c = TestClient(app)
    r = c.post("/contract-clean/",
               files={"file": ("g.csv", gros, "text/csv")})
    assert r.status_code == 200
    assert CC._MAX_MO < 100, "la route ne doit plus promettre 100 Mo"
    with pytest.raises(Exception):
        CC._verifier_taille(b"x" * (CC._MAX + 1))


# --------------------------------------------------------------------------- #
# Valeurs hors plage : le défaut est de ne rien fabriquer
# --------------------------------------------------------------------------- #
def _jeu_avec_extremes(n: int = 400, combien: int = 5) -> pd.DataFrame:
    import random
    random.seed(5)
    revenus = [random.randint(1500, 9000) for _ in range(n)]
    for i in range(combien):
        revenus[i * 17] = random.randint(900_000, 2_000_000)
    return pd.DataFrame({"id": range(n), "revenu": revenus})


def _contrat_avec_plage(df, politique="signaler"):
    prof = DataProfiler()
    analyse = asyncio.run(prof.analyze_file(df.to_csv(index=False).encode(), "r.csv"))
    ct, notes = CI.inferer(analyse, prof.df, usage="modele", proposer_plages=True)
    ct.par_nom()["revenu"].politique_aberrantes = politique
    return ct, notes, prof.df


def test_les_extremes_sont_mesurees_sans_etre_declarees_par_defaut():
    """Déclarer la plage d'office rendrait le contrat intenable par le fichier
    dont il sort — la même erreur que sur le taux de vides. Mais ne rien dire
    laisserait passer en silence ce qu'un humain aurait voulu voir."""
    df = _jeu_avec_extremes()
    prof = DataProfiler()
    analyse = asyncio.run(prof.analyze_file(df.to_csv(index=False).encode(), "r.csv"))
    ct, notes = CI.inferer(analyse, prof.df, usage="modele")
    cc = ct.par_nom()["revenu"]
    assert cc.minimum is None and cc.maximum is None, "rien n'est contraint d'office"
    assert any("intervalle plausible" in n for n in notes), notes


def test_la_politique_par_defaut_ne_touche_a_rien():
    """LE test : signaler, c'est signaler. Pas écrêter discrètement."""
    df = _jeu_avec_extremes()
    ct, _, charge = _contrat_avec_plage(df)
    out, j = R.appliquer(charge, ct)

    assert out["revenu"].max() == charge["revenu"].max(), "aucune valeur modifiée"
    assert out["revenu"].isna().sum() == 0, "aucune valeur vidée"
    refus = [g for g in j.refus if g.action == "aberrantes"]
    assert refus and "trancher" in refus[0].detail
    r = C.verify(out, ct, origine=charge)
    assert "hors_plage" in {v.code for v in r.violations}, "et c'est signalé"


def test_ecreter_le_dit_au_lieu_de_le_taire():
    df = _jeu_avec_extremes()
    ct, _, charge = _contrat_avec_plage(df, "ecreter")
    out, j = R.appliquer(charge, ct)
    fait = [g for g in j.faits if g.action == "aberrantes"]
    assert fait and "fabriquées, pas observées" in fait[0].detail
    assert out["revenu"].max() < 20_000
    assert C.verify(out, ct, origine=charge).tient


def test_ecreter_une_colonne_entiere_ne_leve_pas():
    """Les bornes issues des quartiles sont fractionnaires, la colonne est
    `Int64`, et `to_numeric` conserve ce dtype : écrêter levait un TypeError."""
    df = _jeu_avec_extremes()
    ct, _, charge = _contrat_avec_plage(df, "ecreter")
    out, _ = R.appliquer(charge, ct)
    assert str(out["revenu"].dtype) == "Int64"
    cc = ct.par_nom()["revenu"]
    assert out["revenu"].max() <= cc.maximum, "arrondi vers l'intérieur de la plage"


def test_vider_renvoie_au_budget_dimputation():
    df = _jeu_avec_extremes()
    ct, _, charge = _contrat_avec_plage(df, "vider")
    out, j = R.appliquer(charge, ct)
    fait = [g for g in j.faits if g.action == "aberrantes"]
    assert fait and "budget d'imputation" in fait[0].detail
    assert out["revenu"].max() < 20_000


def test_une_politique_hors_budget_est_refusee():
    """Écrêter la moitié d'une colonne, c'est la dessiner, pas la nettoyer."""
    df = pd.DataFrame({"id": range(40),
                       "x": [1.0] * 20 + [1000.0] * 20})
    ct = C.Contrat(
        colonnes=[C.ContratColonne("id", "integer", unique=True),
                  C.ContratColonne("x", "number", minimum=0, maximum=10,
                                   politique_aberrantes="ecreter")],
        cle_primaire=["id"], imputation_max=0.10)
    out, j = R.appliquer(df, ct)
    refus = [g for g in j.refus if g.action == "aberrantes"]
    assert refus and "budget" in refus[0].detail
    assert out["x"].max() == 1000.0, "rien n'a été touché"


def test_une_politique_inconnue_est_refusee_pas_ignoree():
    df = pd.DataFrame({"x": [1.0] * 20 + [99.0]})
    ct = C.Contrat(colonnes=[C.ContratColonne(
        "x", "number", minimum=0, maximum=10,
        politique_aberrantes="supprimer-tout")], sans_doublons=False)
    _, j = R.appliquer(df, ct)
    assert any("politique inconnue" in g.detail for g in j.refus)


def test_une_colonne_constante_na_pas_de_plage():
    """L'écart interquartile est nul : toute valeur différente de la médiane
    serait déclarée aberrante, ce qui est un verdict et non une mesure."""
    assert CI.plage_plausible(pd.Series([7.0] * 50)) is None


def test_la_plage_est_robuste_aux_extremes_quelle_cherche():
    """Moyenne et écart-type sont déplacés par ce qu'on cherche ; pas les
    quartiles."""
    propre = pd.Series([float(i) for i in range(100)])
    pollue = pd.concat([propre, pd.Series([1e9, 2e9])], ignore_index=True)
    a, b = CI.plage_plausible(propre)
    c, d = CI.plage_plausible(pollue)
    assert abs(b - d) / b < 0.25, (b, d)


def test_une_colonne_booleenne_ne_devient_pas_entiere():
    """Troisième passage du même piège, verrouillé cette fois.

    `is_numeric_dtype` répond `True` pour `bool`. Il a fait planter le profileur
    (quantiles d'aberrance), puis l'inférence de plage (écart interquartile), et
    la correction de la seconde promouvait les drapeaux en entiers. Une colonne
    de booléens doit ressortir `boolean`, et rien ne doit lever.
    """
    df = pd.DataFrame({"id": range(40),
                       "defini": [i % 3 == 0 for i in range(40)],
                       "score": [i / 40 for i in range(40)]})
    prof = DataProfiler()
    analyse = asyncio.run(prof.analyze_file(df.to_csv(index=False).encode(), "b.csv"))
    ct, _ = CI.inferer(analyse, prof.df, usage="modele", proposer_plages=True)
    types = {c.nom: c.type for c in ct.colonnes}
    assert types["defini"] == "boolean", types
    cc = ct.par_nom()["defini"]
    assert cc.minimum is None, "pas de plage sur un drapeau"
    out, _ = R.appliquer(prof.df, ct)
    assert C.verify(out, ct, origine=prof.df).tient


def test_plage_plausible_ne_leve_pas_sur_des_booleens():
    assert CI.plage_plausible(pd.Series([True, False] * 30)) is None


def test_un_fichier_reel_a_colonnes_booleennes_passe():
    """`SampleSubmission.csv` du Bias Bounty : 8 colonnes booléennes sur 17."""
    import pathlib
    f = pathlib.Path("C:/Users/hp/Projet Persos/AI/bias-bounty/SampleSubmission.csv")
    if not f.exists():
        pytest.skip("fichier absent")
    r = asyncio.run(agent_contrat.run(f.read_bytes(), f.name, usage="modele"))
    assert r["contrat_tenu"], r["enonce"]["non_garanti"]
