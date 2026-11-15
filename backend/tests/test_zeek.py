"""Le format natif de Zeek : un journal qui déclare lui-même son contrat.

Un journal Zeek commence par `#fields` et `#types` : le nom et le type de
chaque colonne, plus la façon dont il écrit « non renseigné » (`#unset_field`).
On n'a donc rien à deviner — le contrat se lit dans le fichier, et la passe
vérifie le journal contre ce qu'il annonce de lui-même.
"""
import asyncio
import base64
import gzip
import io
import ipaddress
import os
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

from app.services import agent_contrat, contract as C, zeek

BACKEND = Path(__file__).resolve().parents[1]
SECURITE = BACKEND.parent / "corpus-public" / "securite"

ENTETE = (
    "#separator \\x09\n#set_separator\t,\n#empty_field\t(empty)\n#unset_field\t-\n"
    "#path\tconn\n#open\t2024-04-12-19-29-13\n"
    "#fields\tts\tuid\tid.orig_h\tid.orig_p\tid.resp_h\tid.resp_p\tproto\tservice\tduration\torig_bytes\tlocal_orig\thistory\n"
    "#types\ttime\tstring\taddr\tport\taddr\tport\tenum\tstring\tinterval\tcount\tbool\tstring\n")
LIGNES = [
    "1521911721.255387\tCMwJl820\t10.164.94.120\t39681\t10.47.3.155\t3389\ttcp\tssl\t0.004266\t97\tT\tShADTdtr",
    "1521911721.411148\tCCVM7z3N\t10.47.25.80\t50817\t10.128.0.218\t23189\ttcp\t-\t0.000486\t0\tT\tSr",
    "1521911721.926018\tCBz9zO1p\t10.47.25.80\t50817\t10.128.0.218\t23189\ttcp\t-\t-\t-\tF\tSR",
    "1521911722.690601\tCXWT872Q\tfe80::215:17ff:fe84:c13f\t143\tff02::2\t0\ticmp\t-\t0.5\t12\tF\tShADTdtR",
    "1521911723.000001\tCa1b2c3d\t10.0.0.1\t65534\t10.0.0.2\t53\tudp\tdns\t0.01\t40\tT\tDd",
]


def journal(lignes=LIGNES, entete=ENTETE) -> bytes:
    return (entete + "\n".join(lignes) + "\n#close\t2024-04-12-19-34-07\n").encode()


def passe(contenu: bytes, nom="conn.log", usage="base"):
    res = asyncio.run(agent_contrat.run(contenu, nom, usage=usage))
    sortie = pd.read_csv(io.BytesIO(base64.b64decode(res["csv_base64"])), dtype=str)
    return res, sortie


# --------------------------------------------------------------------------- #
# Lire
# --------------------------------------------------------------------------- #
def test_reconnait_un_journal_zeek_et_rien_dautre():
    assert zeek.est_zeek(journal())
    assert zeek.est_zeek(gzip.compress(journal()))
    assert not zeek.est_zeek(b"a,b\n1,2\n")
    assert not zeek.est_zeek(gzip.compress(b"a,b\n1,2\n"))


def test_lit_les_lignes_sans_les_commentaires_ni_la_fermeture():
    df, entete = zeek.lire(journal())
    assert len(df) == 5                                   # ni l'en-tête, ni #close
    assert list(df.columns)[:4] == ["ts", "uid", "id_orig_h", "id_orig_p"]
    assert entete.types["id_orig_h"] == "addr" and entete.chemin == "conn"
    assert all(isinstance(v, str) for v in df["id_orig_p"])   # tout en texte
    assert pd.isna(df["service"][1]) and pd.isna(df["duration"][2])   # « - » déclaré non renseigné
    assert df["ts"][0] == "1521911721.255387"             # aucune précision perdue


def test_un_journal_compresse_se_lit_pareil():
    a, _ = zeek.lire(journal())
    b, _ = zeek.lire(gzip.compress(journal()))
    assert a.equals(b)


def test_un_entete_incomplet_est_refuse_en_disant_quoi():
    sans_types = ENTETE[:ENTETE.index("#types")]
    with pytest.raises(ValueError) as e:
        zeek.lire(journal(entete=sans_types))
    assert "#types" in str(e.value)


def test_une_ligne_qui_na_pas_le_bon_nombre_de_champs_est_refusee():
    with pytest.raises(ValueError) as e:
        zeek.lire(journal(LIGNES + ["1521911724.0\tCx\t10.0.0.1"]))
    assert "champs" in str(e.value)


# --------------------------------------------------------------------------- #
# Le contrat vient du fichier
# --------------------------------------------------------------------------- #
def test_le_contrat_traduit_les_types_declares():
    df, entete = zeek.lire(journal())
    ct, notes = zeek.contrat(df, entete, "base")
    c = ct.par_nom()
    assert (c["ts"].type, c["ts"].minimum) == ("number", None)     # double signé
    assert (c["id_orig_p"].type, c["id_orig_p"].minimum, c["id_orig_p"].maximum) == ("integer", 0, 65535)
    assert (c["id_orig_h"].type, c["id_orig_h"].forme) == ("string", "ip")
    assert c["proto"].type == "category" and c["local_orig"].type == "boolean"
    assert (c["orig_bytes"].type, c["orig_bytes"].minimum) == ("integer", 0)
    assert (c["duration"].type, c["duration"].minimum) == ("number", None)
    assert ct.marqueurs_vides == [] and ct.espaces_significatifs
    # la casse d'un champ Zeek n'est jamais une faute de frappe
    assert c["history"].casse_significative and c["uid"].casse_significative
    assert ct.cle_primaire == ["uid"] and ct.cle_exigee
    assert c["service"].nullable and not c["proto"].nullable
    assert any("#types" in n for n in notes)


def test_la_passe_tient_sur_un_journal_sain_et_ne_touche_a_rien():
    res, sortie = passe(journal())
    assert res["contrat_tenu"], res["enonce"]["non_garanti"]
    assert list(sortie["history"]) == ["ShADTdtr", "Sr", "SR", "ShADTdtR", "Dd"]
    assert sortie["id_orig_h"][3] == "fe80::215:17ff:fe84:c13f"      # l'IPv6 n'est pas vidée
    assert sortie["id_resp_p"][4] == "53" and sortie["id_orig_p"][4] == "65534"


def test_le_port_65534_nest_pas_une_valeur_aberrante():
    # Un port a une plage connue ; on ne le juge pas à l'écart interquartile.
    res, _ = passe(journal())
    assert not any("id_orig_p" in a and "plausible" in a for a in res["arbitrages"])


def test_un_port_hors_plage_et_une_adresse_impossible_font_echouer():
    mauvaises = LIGNES + [
        "1521911724.0\tCzzz0001\t999.1.2.3\t70000\t10.0.0.2\t53\tudp\tdns\t0.01\t40\tT\tDd"]
    res, sortie = passe(journal(mauvaises))
    assert not res["contrat_tenu"]
    texte = " ".join(res["enonce"]["non_garanti"])
    assert "id_orig_p" in texte and "id_orig_h" in texte
    assert sortie["id_orig_p"][5] == "70000"              # signalé, jamais écrêté
    assert any("1 valeur(s) au-dessus de 65535" in v and "sous" not in v
               for v in res["enonce"]["non_garanti"])


def test_une_valeur_qui_contredit_le_type_declare_est_bloquante():
    mauvaises = LIGNES + [
        "1521911724.0\tCzzz0002\t10.0.0.1\t80\t10.0.0.2\t53\tudp\tdns\t0.01\tbeaucoup\tT\tDd"]
    res, _ = passe(journal(mauvaises))
    assert not res["contrat_tenu"] and res["score_apres"] <= C.PLAFOND_BLOQUANT
    assert any("orig_bytes" in v for v in res["enonce"]["non_garanti"])


def test_un_uid_repete_nest_pas_pris_pour_cle():
    # dns.log, http.log : plusieurs lignes par connexion, donc par uid.
    lignes = LIGNES + [LIGNES[0].replace("1521911721.255387", "1521911799.0")]
    df, entete = zeek.lire(journal(lignes))
    ct, notes = zeek.contrat(df, entete, "base")
    assert ct.cle_primaire == []


# --------------------------------------------------------------------------- #
# L'adresse : IPv4 ou IPv6, au sens de la bibliothèque standard
# --------------------------------------------------------------------------- #
def test_la_forme_ip_suit_la_bibliotheque_standard():
    motif = re.compile(C.FORMES["ip"])
    cas = ["10.0.0.1", "255.255.255.255", "256.1.1.1", "1.2.3", "999.1.2.3", "::", "::1",
           "fe80::215:17ff:fe84:c13f", "ff02::2", "2001:db8::", "2001:db8:0:0:0:0:2:1",
           "2001:db8::1::2", "1:2:3:4:5:6:7:8", "1:2:3:4:5:6:7:8:9", "1:2:3:4:5:6:7", "::ffff:10.0.0.1",
           "gggg::1", "fe80:::1", "12345::1", "abc", "1::2:3:4:5:6:7", "1:2:3:4:5:6:7::",
           "::2:3:4:5:6:7:8", "1::8", "64:ff9b::192.0.2.33", "1:2:3:4:5:6:1.2.3.4"]
    for a in cas:
        try:
            ipaddress.ip_address(a)
            attendu = True
        except ValueError:
            attendu = False
        assert bool(motif.match(a)) == attendu, a


# --------------------------------------------------------------------------- #
# La commande, et le script rendu
# --------------------------------------------------------------------------- #
def idc(*args):
    env = dict(os.environ, PYTHONPATH=str(BACKEND), PYTHONIOENCODING="utf-8")
    return subprocess.run([sys.executable, "-m", "app.cli", *args], cwd=BACKEND, env=env,
                          capture_output=True, text=True, encoding="utf-8", timeout=180)


def test_commande_check_sur_un_journal_zeek(tmp_path):
    (tmp_path / "conn.log").write_bytes(journal())
    (tmp_path / "conn.log.gz").write_bytes(gzip.compress(journal()))
    assert idc("check", str(tmp_path / "conn.log")).returncode == 0
    assert idc("check", str(tmp_path / "conn.log.gz")).returncode == 0
    (tmp_path / "pas_zeek.log").write_text("ceci n est pas un journal Zeek\n", encoding="utf-8")
    assert idc("check", str(tmp_path / "pas_zeek.log")).returncode == 2


def test_le_script_rendu_relit_le_journal_zeek(tmp_path):
    res, _ = passe(journal())
    (tmp_path / "conn.log").write_bytes(journal())
    (tmp_path / "rejeu.py").write_text(res["script"], encoding="utf-8")
    env = dict(os.environ, PYTHONPATH=str(BACKEND))
    r = subprocess.run([sys.executable, "rejeu.py", "conn.log"], cwd=tmp_path, env=env,
                       capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stderr[-600:]
    assert "contrat tenu : True" in r.stdout


# --------------------------------------------------------------------------- #
# Le vrai journal, quand il est là
# --------------------------------------------------------------------------- #
def test_reel_les_20000_premieres_connexions():
    f = SECURITE / "zeek-conn.log.gz"
    if not f.exists():
        pytest.skip("zeek-conn.log.gz absent — voir corpus-public/README.md")
    with gzip.open(f, "rb") as g:
        tete = b"".join(next(g) for _ in range(20008))
    res, sortie = passe(tete)
    assert res["lignes"] == [20000, 20000]
    assert res["contrat_tenu"], res["enonce"]["non_garanti"]
    assert sortie["history"].nunique() == 119


# --------------------------------------------------------------------------- #
# L'API, celle que l'interface appelle
# --------------------------------------------------------------------------- #
from fastapi.testclient import TestClient      # noqa: E402
from app.main import app                       # noqa: E402

api = TestClient(app)


def test_api_propose_puis_passe_sur_un_journal_zeek():
    f = {"file": ("conn.log", journal(), "text/plain")}
    r = api.post("/contract-clean/propose", files=f, data={"usage": "base"})
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["contrat"]["cle_primaire"] == ["uid"]
    assert d["dataset"]["rows"] == 5 and d["dataset"]["columns"] == 12
    assert any("#types" in a for a in d["arbitrages"])
    r = api.post("/contract-clean/", files={"file": ("conn.log.gz", gzip.compress(journal()), "application/gzip")},
                 data={"usage": "base"})
    assert r.status_code == 200 and r.json()["contrat_tenu"] is True


def test_api_refuse_un_log_qui_nest_pas_zeek():
    r = api.post("/contract-clean/propose", files={"file": ("x.log", b"du texte\n", "text/plain")},
                 data={"usage": "base"})
    assert r.status_code == 400 and "Zeek" in r.json()["detail"]


def test_un_fichier_compresse_piege_est_arrete_a_la_decompression():
    # 60 Mo de zéros tiennent en 60 Ko compressés : sans plafond, un envoi
    # minuscule ferait allouer au serveur ce qu'on veut.
    bombe = gzip.compress(b"#separator \x09\n" + b"0" * (60 * 1024 * 1024))
    assert len(bombe) < 200_000
    with pytest.raises(ValueError) as e:
        zeek.lire(bombe, max_octets=25 * 1024 * 1024)
    assert "décompress" in str(e.value)
    r = api.post("/contract-clean/", files={"file": ("conn.log.gz", bombe, "application/gzip")},
                 data={"usage": "base"})
    assert r.status_code == 413


def test_une_borne_absente_ne_secrit_pas_none():
    res, _ = passe(journal())
    assert not any("None" in g for g in res["enonce"]["garanti"])
    assert any("ne descend jamais sous 0" in g for g in res["enonce"]["garanti"])
    assert any("[0, 65535]" in g for g in res["enonce"]["garanti"])


# --------------------------------------------------------------------------- #
# Les 34 autres journaux : un texte de journal se prend à la lettre
# --------------------------------------------------------------------------- #
# Passer dns, http, ssh, ntp… a montré que la passe réécrivait 6 894 cellules,
# dont 636 dans des journaux déclarés « tenus 100/100 ». Chaque test ci-dessous
# porte une valeur réelle relevée dans ces fichiers.
import csv                                     # noqa: E402

from app.services import reparer               # noqa: E402

ZEEK = SECURITE / "zeek"
NUMERIQUES = {"count", "int", "port", "time", "interval", "double"}


def journal_de(chemin: str, champs: str, types: str, lignes) -> bytes:
    entete = ("#separator \\x09\n#set_separator\t,\n#empty_field\t(empty)\n#unset_field\t-\n"
              f"#path\t{chemin}\n#fields\t{champs}\n#types\t{types}\n")
    return (entete + "\n".join(lignes) + "\n#close\t2024-04-12-19-34-07\n").encode()


def cellules(res) -> list:
    """Le CSV livré, relu **sans** l'interprétation de pandas : `none` y
    redeviendrait un vide et le test ne verrait rien."""
    texte = base64.b64decode(res["csv_base64"]).decode("utf-8")
    return list(csv.reader(io.StringIO(texte, newline="")))[1:]


def test_un_texte_qui_ressemble_a_un_vide_reste_une_valeur():
    # http.log : l'URI « / », vidée 6 175 fois. ssh.log : l'algorithme de
    # compression « none ». ntlm.log : la machine « NULL ». rfb.log : « None ».
    lignes = [f"152191172{i}.0\tC{i}\t{v}" for i, v in enumerate(
        ["/", "none", "NULL", "None", "unknown", "?", "/index.html", "-"])]
    res = asyncio.run(agent_contrat.run(
        journal_de("http", "ts\tuid\turi", "time\tstring\tstring", lignes), "http.log", usage="tableau"))
    assert [l[2] for l in cellules(res)] == ["/", "none", "NULL", "None", "unknown", "?", "/index.html", ""]
    assert res["contrat_tenu"], res["enonce"]["non_garanti"]
    assert not any("reconnues comme vides" in f for f in res["enonce"]["fait"])


def test_les_espaces_de_bord_dun_journal_sont_gardes():
    # loaded_scripts.log : l'indentation dit la profondeur de chargement.
    lignes = ["/opt/zeek/base/init-bare.zeek", "  /opt/zeek/base/bif/const.bif.zeek",
              "    /opt/zeek/base/bif/types.bif.zeek", "Access is denied. "]
    res = asyncio.run(agent_contrat.run(
        journal_de("loaded_scripts", "name", "string", lignes), "loaded_scripts.log", usage="tableau"))
    assert [l[0] for l in cellules(res)] == lignes
    assert not any("détourées" in f for f in res["enonce"]["fait"])


def test_un_intervalle_ou_une_date_negative_ne_sont_pas_des_fautes():
    # ntp.log : `interval` et `time` sont des doubles signés. « Positif »
    # n'était écrit nulle part dans le fichier : c'était une supposition.
    champs, types = "ts\troot_disp\txmt_time\tnum", "time\tinterval\ttime\tcount"
    lignes = ["1521911721.0\t-0.000244\t-2150431849.5\t12", "1521911722.0\t0.5\t1521911722.1\t3"]
    res = asyncio.run(agent_contrat.run(journal_de("ntp", champs, types, lignes), "ntp.log", usage="tableau"))
    assert res["contrat_tenu"], res["enonce"]["non_garanti"]
    # un `count`, lui, est non signé par définition
    faux = journal_de("ntp", champs, types, lignes + ["1521911723.0\t0.1\t1.0\t-4"])
    res = asyncio.run(agent_contrat.run(faux, "ntp.log", usage="tableau"))
    assert any("num" in v and "sous 0" in v for v in res["enonce"]["non_garanti"])


def test_un_nombre_a_17_chiffres_ressort_au_chiffre_pres():
    # ntp.log : -2.1504318496896954e+09 ressortait -2150431849.689696.
    # La lecture rapide de pandas n'est pas exacte au dernier chiffre.
    for texte in ["-2.1504318496896954e+09", "1521911721.2553871", "0.1", "1e22", "123456789012345678"]:
        s = pd.Series([texte, None, "1.5"], dtype=object)
        assert C.convertir(s, "number")[0] == float(texte), texte
        assert C._numerique(s)[0] == float(texte), texte


def test_un_grand_entier_a_cote_dun_vide_garde_ses_chiffres():
    # Passer par un flottant pour loger le vide arrondit au-delà de 2^53.
    s = pd.Series(["9007199254740993", None, "12"], dtype=object)
    c = C.convertir(s, "integer")
    assert int(c[0]) == 9007199254740993 and pd.isna(c[1]) and int(c[2]) == 12


def test_un_contrat_peut_declarer_ses_propres_marqueurs_de_vide():
    df = pd.DataFrame({"n": ["1", "2", "3", "4"], "algo": ["none", "N/A", "zlib", "inconnu"]}, dtype=object)
    ct = C.Contrat(colonnes=[C.ContratColonne("n", "integer"),
                             C.ContratColonne("algo", "string", taux_nul_max=0.5)],
                   marqueurs_vides=["n/a"], imputation_max=0.0)
    propre, _ = reparer.appliquer(df, ct)
    assert propre["algo"].tolist()[0] == "none" and pd.isna(propre["algo"][1])
    assert propre["algo"].tolist()[3] == "inconnu"
    assert C.verify(propre, ct, origine=df).tient
    # sans déclaration, la liste générale s'applique comme avant
    ancien = C.Contrat.from_dict({"colonnes": [{"nom": "n", "type": "integer"}, {"nom": "algo"}], "imputation_max": 0.0})
    assert ancien.marqueurs_vides is None and not ancien.espaces_significatifs
    propre, _ = reparer.appliquer(df, ancien)
    assert propre["algo"].isna().tolist() == [True, True, False, True]


def test_une_valeur_videe_dans_un_journal_reste_bloquante():
    # Le garde-fou ne doit pas s'être relâché : une adresse impossible est
    # vidée par la réparation, et le vérificateur doit toujours le voir.
    lignes = ["1521911721.0\tC1\t10.0.0.1", "1521911722.0\tC2\t999.1.2.3", "1521911723.0\tC3\tnone"]
    res = asyncio.run(agent_contrat.run(
        journal_de("x", "ts\tuid\thote", "time\tstring\taddr", lignes), "x.log", usage="tableau"))
    assert not res["contrat_tenu"] and res["score_apres"] <= C.PLAFOND_BLOQUANT


def _egal(a: str, b: str, type_zeek: str) -> bool:
    from decimal import Decimal, InvalidOperation
    if a == "-":
        return b == ""
    if type_zeek in NUMERIQUES:
        try:
            return Decimal(a) == Decimal(b)
        except InvalidOperation:
            return False
    if type_zeek == "bool":
        return (a, b) in (("T", "True"), ("F", "False"))
    return a == b


JOURNAUX = sorted(ZEEK.glob("*.log.gz")) if ZEEK.exists() else []


@pytest.mark.skipif(not JOURNAUX, reason="corpus-public/securite/zeek absent — voir le README")
@pytest.mark.parametrize("fichier", JOURNAUX, ids=lambda f: f.name[:-7])
def test_reel_chaque_cellule_livree_vaut_la_cellule_recue(fichier):
    """Les 5 000 premières lignes de chaque journal : contrat tenu, et pas une
    cellule changée. Le contrôle relit le fichier brut lui-même et ne consulte
    ni le rapport ni le journal de réparation."""
    with gzip.open(fichier, "rb") as g:
        contenu = b"".join(l for _, l in zip(range(5008), g))
    lignes, types = [], []
    for l in contenu.decode("utf-8", errors="replace").split("\n"):
        if l.startswith("#types"):
            types = l.split("\t")[1:]
        elif l and not l.startswith("#"):
            lignes.append(l.split("\t"))
    res = asyncio.run(agent_contrat.run(contenu, fichier.name, usage="tableau"))
    sortie = cellules(res)
    assert len(sortie) == len(lignes)
    ecarts = [(i, j, a, b) for i, (la, lb) in enumerate(zip(lignes, sortie))
              for j, (a, b) in enumerate(zip(la, lb)) if not _egal(a, b, types[j])]
    assert not ecarts, f"{len(ecarts)} cellule(s) changée(s), par exemple {ecarts[:3]}"
    assert res["contrat_tenu"], res["enonce"]["non_garanti"]
