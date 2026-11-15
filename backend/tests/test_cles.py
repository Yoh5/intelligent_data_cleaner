"""Proposer une clé — sans jamais la décréter.

Une clé est une déclaration sur les fichiers à venir : « ces colonnes
identifieront toujours une ligne ». Un fichier ne peut pas le prouver, il peut
seulement le réfuter. Sur les 34 journaux Zeek réels, 24 échouaient pour
l'usage « base » sur la seule clause de clé, parce que l'outil n'essayait que
`uid`. Il cherche maintenant ce que le fichier ne réfute pas, le **propose**,
et n'adopte d'office qu'une colonne dont le nom dit l'intention.
"""
import asyncio
import gzip
import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

from app.services import agent_contrat, contract as C, contract_infer as I, zeek

BACKEND = Path(__file__).resolve().parents[1]
ZEEK = BACKEND.parent / "corpus-public" / "securite" / "zeek"


def journal(chemin: str, champs: str, types: str, lignes) -> bytes:
    entete = ("#separator \\x09\n#set_separator\t,\n#empty_field\t(empty)\n#unset_field\t-\n"
              f"#path\t{chemin}\n#fields\t{champs}\n#types\t{types}\n")
    return (entete + "\n".join(lignes) + "\n").encode()


# dns.log : plusieurs requêtes par connexion, donc `uid` répété ; deux
# requêtes au même instant sur deux connexions, donc `ts` répété aussi.
DNS = journal("dns", "ts\tuid\tquery\trcode", "time\tstring\tstring\tcount", [
    "1521911721.1\tCaaa\texample.com\t0",
    "1521911721.1\tCbbb\texample.com\t0",
    "1521911722.2\tCaaa\texample.org\t0",
    "1521911723.3\tCbbb\texample.net\t3",
    "1521911724.4\tCccc\texample.com\t0",
])


def passe(contenu: bytes, nom: str, usage="base", **k):
    return asyncio.run(agent_contrat.run(contenu, nom, usage=usage, **k))


# --------------------------------------------------------------------------- #
# La recherche
# --------------------------------------------------------------------------- #
def test_trouve_la_plus_petite_combinaison_et_pas_ses_sur_ensembles():
    df = pd.DataFrame({"jour": ["l", "l", "m", "m"], "poste": ["a", "b", "a", "b"],
                       "bruit": ["x", "y", "y", "x"], "fixe": ["k"] * 4}, dtype=object)
    cles = I.cles_composites(df, list(df.columns))
    assert ["jour", "poste"] in cles
    assert all(len(c) == 2 for c in cles)                 # aucune de taille 3 : elles contiendraient une paire
    assert not any("fixe" in c for c in cles)             # une constante n'identifie rien


def test_ne_propose_rien_que_le_fichier_refute():
    df = pd.DataFrame({"a": ["1", "1", "2"], "b": ["x", "x", "y"], "c": ["p", "p", "q"]}, dtype=object)
    assert I.cles_composites(df, list(df.columns)) == []


def test_passe_a_trois_colonnes_seulement_si_aucune_paire_ne_tient():
    lignes = [(a, b, c) for a in "01" for b in "01" for c in "01"]
    df = pd.DataFrame(lignes, columns=["a", "b", "c"], dtype=object)
    assert I.cles_composites(df, ["a", "b", "c"]) == [["a", "b", "c"]]


def test_une_colonne_au_nom_didentifiant_passe_devant():
    df = pd.DataFrame({"jour": ["l", "l", "m", "m"], "poste": ["a", "b", "a", "b"],
                       "client_id": ["1", "2", "2", "1"]}, dtype=object)
    cles = I.cles_composites(df, list(df.columns))
    assert "client_id" in cles[0]


# --------------------------------------------------------------------------- #
# Un journal Zeek
# --------------------------------------------------------------------------- #
def test_un_journal_sans_cle_recoit_une_proposition_pas_une_cle():
    df, entete = zeek.lire(DNS)
    ct, notes = zeek.contrat(df, entete, "base")
    assert ct.cle_primaire == []                          # rien d'adopté
    assert ct.cles_candidates[0] == ["ts", "uid"]
    assert any("ts + uid" in n and "réfuter" in n for n in notes)
    res = passe(DNS, "dns.log")
    assert not res["contrat_tenu"]                        # une proposition ne tient pas lieu de clé
    assert any(v["code"] == "cle_absente" for v in res["rapport"]["violations"])


def test_la_cle_declaree_fait_tenir_le_contrat():
    res = passe(DNS, "dns.log", cle=["ts", "uid"])
    assert res["contrat_tenu"], res["enonce"]["non_garanti"]
    assert res["contrat"]["cle_primaire"] == ["ts", "uid"]
    assert res["lignes"] == [5, 5]
    assert any("ts+uid identifie chaque ligne" in g for g in res["enonce"]["garanti"])


def test_une_cle_declaree_que_le_fichier_refute_ne_tient_pas():
    # `uid` seul se répète : le déclarer ne doit ni tenir ni jeter des lignes en silence.
    res = passe(DNS, "dns.log", cle=["uid"])
    assert not res["contrat_tenu"]
    assert res["score_apres"] <= C.PLAFOND_BLOQUANT


def test_une_cle_declaree_sur_une_colonne_absente_est_indecidable():
    with pytest.raises(ValueError) as e:
        passe(DNS, "dns.log", cle=["ts", "inconnue"])
    assert "inconnue" in str(e.value)


def test_un_identifiant_unique_est_adopte_une_mesure_unique_est_proposee():
    # files.log : `fuid` identifie un fichier. stats.log : `ts` est unique,
    # mais un horodatage unique peut l'être par accident — proposé, pas adopté.
    fichiers = journal("files", "ts\tfuid\tsource", "time\tstring\tstring", [
        "1.0\tFaaa\tHTTP", "1.0\tFbbb\tHTTP", "2.0\tFccc\tSSL"])
    df, e = zeek.lire(fichiers)
    ct, _ = zeek.contrat(df, e, "base")
    assert ct.cle_primaire == ["fuid"]
    stats = journal("stats", "ts\tpeer\tmem", "time\tstring\tcount", [
        "1.0\tzeek\t70", "2.0\tzeek\t70", "3.0\tzeek\t71"])
    df, e = zeek.lire(stats)
    ct, notes = zeek.contrat(df, e, "base")
    assert ct.cle_primaire == [] and ct.cles_candidates == [["ts"]]


def test_un_texte_unique_par_accident_nest_pas_adopte():
    # Trois lignes : tout y est unique. `referrer` ne devient pas une clé.
    http = journal("http", "ts\tuid\treferrer", "time\tstring\tstring", [
        "1.0\tCa\thttp://a/", "2.0\tCa\thttp://b/", "3.0\tCb\thttp://c/"])
    df, e = zeek.lire(http)
    ct, _ = zeek.contrat(df, e, "base")
    assert ct.cle_primaire == []
    assert ["referrer"] in ct.cles_candidates


def test_un_usage_sans_cle_exigee_ne_cherche_pas():
    df, e = zeek.lire(DNS)
    ct, notes = zeek.contrat(df, e, "tableau")
    assert ct.cles_candidates == [] and not any("composite" in n for n in notes)


# --------------------------------------------------------------------------- #
# Un CSV
# --------------------------------------------------------------------------- #
def test_un_csv_sans_cle_simple_recoit_une_proposition():
    csv = ("jour,poste,valeur\n" + "\n".join(
        f"{j},{p},{v}" for j, p, v in [("lun", "a", 1), ("lun", "b", 1), ("mar", "a", 2),
                                       ("mar", "b", 2), ("mer", "a", 1), ("mer", "b", 2)])).encode()
    res = passe(csv, "releves.csv")
    assert res["contrat"]["cle_primaire"] == []
    assert res["contrat"]["cles_candidates"][0] == ["jour", "poste"]
    assert not res["contrat_tenu"]
    res = passe(csv, "releves.csv", cle=["jour", "poste"])
    assert res["contrat_tenu"], res["enonce"]["non_garanti"]


def test_un_ancien_contrat_se_relit():
    ct = C.Contrat.from_dict({"colonnes": [{"nom": "a"}], "cle_primaire": ["a"]})
    assert ct.cles_candidates == []


# --------------------------------------------------------------------------- #
# La commande
# --------------------------------------------------------------------------- #
def idc(*args):
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    return subprocess.run([sys.executable, "-m", "app.cli", *args], cwd=BACKEND, env=env,
                          capture_output=True, text=True, encoding="utf-8")


def test_commande_la_proposition_saffiche_et_la_cle_se_declare(tmp_path):
    f = tmp_path / "dns.log"
    f.write_bytes(DNS)
    r = idc("check", str(f), "--usage", "base")
    assert r.returncode == 1 and "--cle ts+uid" in r.stdout
    r = idc("check", str(f), "--usage", "base", "--cle", "ts+uid", "--json")
    assert r.returncode == 0, r.stdout + r.stderr
    assert json.loads(r.stdout)["contrat_tenu"] is True
    r = idc("check", str(f), "--usage", "base", "--cle", "ts+nimporte")
    assert r.returncode == 2                               # indécidable, pas un verdict


# --------------------------------------------------------------------------- #
# Les vrais journaux
# --------------------------------------------------------------------------- #
JOURNAUX = sorted(ZEEK.glob("*.log.gz")) if ZEEK.exists() else []


@pytest.mark.skipif(not JOURNAUX, reason="corpus-public/securite/zeek absent — voir le README")
@pytest.mark.parametrize("fichier", JOURNAUX, ids=lambda f: f.name[:-7])
def test_reel_toute_cle_proposee_tient_quand_on_la_declare(fichier):
    """Une proposition que la vérification refuserait serait pire qu'aucune."""
    with gzip.open(fichier, "rb") as g:
        contenu = b"".join(l for _, l in zip(range(5008), g))
    df, e = zeek.lire(contenu)
    ct, _ = zeek.contrat(df, e, "base")
    if ct.cle_primaire:
        cle = ct.cle_primaire
    elif ct.cles_candidates:
        cle = ct.cles_candidates[0]
    else:
        pytest.skip("aucune clé que le fichier ne réfute (lignes identiques, ou une seule ligne)")
    res = passe(contenu, fichier.name, cle=cle)
    assert res["contrat_tenu"], (cle, res["enonce"]["non_garanti"])
    assert res["lignes"][0] == res["lignes"][1]
