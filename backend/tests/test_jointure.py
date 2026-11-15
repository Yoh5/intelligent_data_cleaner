"""« Ces deux fichiers sont-ils joignables ? » — avant de lancer la jointure.

Une jointure ne plante jamais : elle rend moins de lignes (clés orphelines),
ou plus (clés répétées des deux côtés), en silence. Le diagnostic mesure donc
ce que la jointure FERAIT, sans la faire, et le dit avant.
"""
import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd

from app.services import jointure as J

BACKEND = Path(__file__).resolve().parents[1]


def ventes(cles):
    return pd.DataFrame({"client": cles, "montant": [str(i) for i in range(len(cles))]})


def clients(cles):
    return pd.DataFrame({"client": cles, "ville": ["Rabat"] * len(cles)})


def test_jointure_propre_n_vers_1():
    d = J.diagnostiquer(ventes(["C1", "C1", "C2"]), clients(["C1", "C2", "C3"]), ["client"], ["client"])
    assert d.joignable and d.problemes == []
    assert d.cardinalite == "n:1"
    assert d.couverture == 1.0
    assert d.lignes_resultat == 3
    assert d.cles_droite_inutilisees == 1          # C3 : une information, pas un défaut


def test_cles_orphelines_font_perdre_des_lignes():
    d = J.diagnostiquer(ventes(["C1", "C9", "C9", "C2"]), clients(["C1", "C2"]), ["client"], ["client"])
    assert not d.joignable
    assert [p["code"] for p in d.problemes] == ["couverture_insuffisante"]
    assert d.orphelines == 2 and d.couverture == 0.5
    assert "C9" in d.exemples_orphelines


def test_un_seuil_de_couverture_plus_bas_laisse_passer():
    g, dr = ventes(["C1", "C2", "C3", "C9"]), clients(["C1", "C2", "C3"])
    assert not J.diagnostiquer(g, dr, ["client"], ["client"]).joignable
    assert J.diagnostiquer(g, dr, ["client"], ["client"], couverture_min=0.7).joignable


def test_n_vers_m_multiplie_les_lignes():
    # C1 : 2 fois à gauche, 3 fois à droite -> 6 lignes au lieu de 2.
    d = J.diagnostiquer(ventes(["C1", "C1", "C2"]), clients(["C1", "C1", "C1", "C2"]), ["client"], ["client"])
    assert not d.joignable
    assert d.cardinalite == "n:m"
    assert d.lignes_resultat == 7
    assert "cardinalite_n_m" in [p["code"] for p in d.problemes]


def test_les_presque_correspondances_sont_nommees():
    # Le cas qui coûte des heures : les clés « se ressemblent » mais ne
    # s'égalent pas — espace, casse, zéro initial mangé par un tableur.
    g = ventes(["C1 ", "c2", "00042", "C3"])
    dr = clients(["C1", "C2", "42", "C3"])
    d = J.diagnostiquer(g, dr, ["client"], ["client"])
    assert not d.joignable
    assert d.orphelines == 3
    assert d.presque == 3                           # toutes rattrapables
    assert ("00042", "42") in d.exemples_presque
    assert any("normalis" in p["message"] for p in d.problemes)


def test_une_cle_vraiment_absente_nest_pas_une_presque_correspondance():
    d = J.diagnostiquer(ventes(["C1", "ZZ"]), clients(["C1"]), ["client"], ["client"])
    assert d.orphelines == 1 and d.presque == 0


def test_les_cles_vides_sont_comptees_a_part():
    d = J.diagnostiquer(ventes(["C1", None, "N/A"]), clients(["C1"]), ["client"], ["client"])
    assert d.cles_vides_gauche == 2
    assert d.orphelines == 2 and not d.joignable


def test_cle_composite():
    g = pd.DataFrame({"pays": ["MA", "MA", "FR"], "code": ["1", "2", "1"]})
    dr = pd.DataFrame({"pays": ["MA", "MA", "FR"], "code": ["1", "2", "1"], "nom": ["a", "b", "c"]})
    d = J.diagnostiquer(g, dr, ["pays", "code"], ["pays", "code"])
    assert d.joignable and d.cardinalite == "1:1"
    # « code » seul n'est pas une clé à droite : n:m
    assert not J.diagnostiquer(g, dr, ["code"], ["code"]).joignable


def test_colonne_absente_est_une_erreur_dusage():
    import pytest
    with pytest.raises(J.CleInconnue) as e:
        J.diagnostiquer(ventes(["C1"]), clients(["C1"]), ["id_client"], ["client"])
    assert "id_client" in str(e.value) and "montant" in str(e.value)   # dit ce qui existe


# --------------------------------------------------------------------------- #
# La commande
# --------------------------------------------------------------------------- #
def idc(*args):
    env = dict(os.environ, PYTHONPATH=str(BACKEND), PYTHONIOENCODING="utf-8")
    return subprocess.run([sys.executable, "-m", "app.cli", *args], cwd=BACKEND, env=env,
                          capture_output=True, text=True, encoding="utf-8", timeout=120)


def ecrire(tmp_path, nom, df):
    p = tmp_path / nom
    df.to_csv(p, index=False)
    return str(p)


def test_commande_join_codes_de_sortie(tmp_path):
    g = ecrire(tmp_path, "ventes.csv", ventes(["C1", "C1", "C2"]))
    ok = ecrire(tmp_path, "clients.csv", clients(["C1", "C2"]))
    ko = ecrire(tmp_path, "clients_incomplets.csv", clients(["C1"]))
    assert idc("join", g, ok, "--sur", "client").returncode == 0
    r = idc("join", g, ko, "--sur", "client")
    assert r.returncode == 1 and "C2" in r.stdout
    assert idc("join", g, ok, "--sur", "nexiste_pas").returncode == 2
    assert idc("join", g, str(tmp_path / "absent.csv"), "--sur", "client").returncode == 2


def test_commande_join_garde_les_zeros_initiaux(tmp_path):
    # Lus par pandas sans précaution, « 01000 » et « 1000 » deviendraient
    # égaux et la jointure paraîtrait parfaite alors que les clés diffèrent.
    g = ecrire(tmp_path, "g.csv", pd.DataFrame({"cp": ["01000", "20000"], "v": ["a", "b"]}))
    d = ecrire(tmp_path, "d.csv", pd.DataFrame({"cp": ["1000", "20000"], "ville": ["x", "y"]}))
    r = idc("join", g, d, "--sur", "cp", "--json")
    assert r.returncode == 1
    j = json.loads(r.stdout)
    assert j["orphelines"] == 1 and j["presque"] == 1


def test_commande_join_cles_de_noms_differents(tmp_path):
    g = ecrire(tmp_path, "g.csv", pd.DataFrame({"Id Client": ["C1", "C2"], "v": ["a", "b"]}))
    d = ecrire(tmp_path, "d.csv", pd.DataFrame({"code": ["C1", "C2"], "ville": ["x", "y"]}))
    assert idc("join", g, d, "--sur", "Id Client=code").returncode == 0


def test_le_nombre_de_lignes_annonce_est_celui_dune_vraie_jointure():
    # Contre-vérification par un autre chemin : pandas fait la jointure,
    # le diagnostic l'avait prédite sans la faire.
    import random
    random.seed(7)
    g = pd.DataFrame({"k": [random.choice("ABCDE") for _ in range(200)], "x": "1"})
    d = pd.DataFrame({"k": [random.choice("BCDEF") for _ in range(50)], "y": "2"})
    diag = J.diagnostiquer(g, d, ["k"], ["k"])
    assert diag.lignes_resultat == len(g.merge(d, on="k", how="inner"))
    assert diag.orphelines == int((g["k"] == "A").sum())


# --------------------------------------------------------------------------- #
# L'API, celle que l'interface appelle
# --------------------------------------------------------------------------- #
from fastapi.testclient import TestClient      # noqa: E402
from app.main import app                       # noqa: E402

api = TestClient(app)


def _csv(df) -> bytes:
    return df.to_csv(index=False).encode()


def _poste_join(g, d, **champs):
    return api.post("/join/", files={"gauche": ("g.csv", _csv(g), "text/csv"),
                                     "droite": ("d.csv", _csv(d), "text/csv")}, data=champs)


def test_api_colonnes_rend_les_noms_tels_que_la_cle_les_attend():
    r = api.post("/join/colonnes", files={"file": ("g.csv", _csv(
        pd.DataFrame({"Id Client": ["C1"], "Montant TTC": ["3"]})), "text/csv")})
    assert r.status_code == 200
    assert r.json() == {"colonnes": ["id_client", "montant_ttc"], "lignes": 1}


def test_api_join_rend_le_diagnostic():
    r = _poste_join(ventes(["C1 ", "C2"]), clients(["C1", "C2"]), sur="client")
    assert r.status_code == 200
    d = r.json()
    assert d["joignable"] is False and d["presque"] == 1
    assert d["exemples_presque"] == [["C1 ", "C1"]]


def test_api_join_cle_composite_et_noms_differents():
    g = pd.DataFrame({"pays": ["MA", "FR"], "num": ["1", "1"]})
    d = pd.DataFrame({"pays": ["MA", "FR"], "code": ["1", "1"], "nom": ["a", "b"]})
    r = _poste_join(g, d, sur="pays,num=code")
    assert r.status_code == 200 and r.json()["joignable"] is True
    assert r.json()["cle_droite"] == ["pays", "code"]


def test_api_join_colonne_absente_est_un_400_qui_dit_quoi_choisir():
    r = _poste_join(ventes(["C1"]), clients(["C1"]), sur="id_client")
    assert r.status_code == 400
    assert "id_client" in r.json()["detail"] and "montant" in r.json()["detail"]


def test_api_join_refuse_un_seuil_absurde_et_un_format_inconnu():
    assert _poste_join(ventes(["C1"]), clients(["C1"]), sur="client",
                       couverture_min="1.5").status_code == 400
    r = api.post("/join/colonnes", files={"file": ("g.txt", b"a,b\n1,2\n", "text/plain")})
    assert r.status_code == 400
