"""Ce que des journaux de sécurité réels ont cassé (15/11/2026).

Trouvés en passant quatre jeux publics — un journal de connexions Zeek et
trois journaux Loghub (OpenSSH, Apache, Linux) — dans la passe sous contrat,
usage « charger en base ». Dans les deux cas le contrat était déclaré tenu à
100/100, sur une sortie qui avait fabriqué ou détruit de l'information :

- une colonne d'heures seules (`06:55:46`) sortait en `2026-11-15 06:55:46` :
  la date du jour de l'exécution, inventée, dans un journal d'authentification ;
- le champ `history` de Zeek, où la casse dit QUI a envoyé le paquet
  (majuscule : l'initiateur ; minuscule : le répondeur), perdait 17 de ses 119
  valeurs, fusionnées « par la casse » comme `Rabat` et `rabat`.
"""
import asyncio
import base64
import io

import pandas as pd

from app.services import agent_contrat, contract as C, reparer as R


def passe(df: pd.DataFrame, usage: str = "base"):
    res = asyncio.run(agent_contrat.run(df.to_csv(index=False).encode(), "j.csv", usage=usage))
    sortie = pd.read_csv(io.BytesIO(base64.b64decode(res["csv_base64"])), dtype=str)
    types = {c["nom"]: c["type"] for c in res["contrat"]["colonnes"]}
    return res, sortie, types


def journal(n=60, **colonnes):
    base = {"id": [str(i) for i in range(n)]}
    base.update({k: [v[i % len(v)] for i in range(n)] for k, v in colonnes.items()})
    return pd.DataFrame(base)


# --------------------------------------------------------------------------- #
# Une date ne s'invente pas
# --------------------------------------------------------------------------- #
def test_une_heure_seule_ne_recoit_pas_la_date_du_jour():
    df = journal(heure=["06:55:46", "06:55:47", "23:10:02"])
    res, sortie, types = passe(df)
    assert types["heure"] != "datetime"
    assert list(sortie["heure"].head(3)) == ["06:55:46", "06:55:47", "23:10:02"]


def test_un_jour_sans_annee_ne_recoit_pas_dannee():
    df = journal(jour=["Dec 10", "Dec 11", "Jan 3"])
    res, sortie, types = passe(df)
    assert types["jour"] != "datetime"
    assert list(sortie["jour"].head(3)) == ["Dec 10", "Dec 11", "Jan 3"]


def test_les_vraies_dates_restent_des_dates():
    for valeurs in (["2024-03-01", "2024-03-02"], ["01/03/2024", "15/03/2024"],
                    ["2024-03-01 10:00:00", "2024-03-02 11:30:00"]):
        _, _, types = passe(journal(quand=valeurs))
        assert types["quand"] == "datetime", valeurs


def test_declarer_datetime_sur_des_heures_ne_fabrique_rien():
    # Même si quelqu'un l'exige dans le contrat : on rend des vides, que le
    # contrôle de type signale, plutôt que des dates au 1er janvier 1900.
    s = pd.Series(["06:55:46", "2024-03-01 10:00:00"])
    conv = C.convertir(s, "datetime")
    assert pd.isna(conv[0]) and conv[1] == pd.Timestamp("2024-03-01 10:00:00")
    part, exemples = C.part_convertible(s, "datetime")
    assert part == 0.5 and exemples == ["06:55:46"]


# --------------------------------------------------------------------------- #
# Une casse qui porte un sens ne se fusionne pas
# --------------------------------------------------------------------------- #
HISTORY = ["ShADTdtr", "ShADTdtR", "Sr", "SR", "ShAdtDTR", "^dtADTR", "^dtADTr", "S"]


def test_les_codes_a_casse_mixte_ne_sont_pas_fusionnes():
    df = journal(history=HISTORY)
    res, sortie, _ = passe(df)
    assert sortie["history"].nunique() == len(HISTORY)
    assert res["contrat_tenu"], res["enonce"]["non_garanti"]
    assert any("history" in r and "casse" in r for r in res["enonce"]["refuse_exprès"])


def test_les_noms_propres_sont_toujours_fusionnes():
    df = journal(ville=["Rabat", "rabat", "RABAT", "Fes"])
    res, sortie, _ = passe(df)
    assert sorted(sortie["ville"].unique()) == ["Fes", "Rabat"]


def test_sans_reparation_les_codes_ne_sont_pas_une_collision():
    # Le vérificateur, seul : des codes à casse mixte ne sont pas une modalité
    # « dédoublée », des graphies ordinaires le sont.
    ct = lambda nom: C.Contrat(colonnes=[C.ContratColonne(nom, "category")])
    codes = pd.DataFrame({"h": HISTORY})
    villes = pd.DataFrame({"v": ["Rabat", "rabat", "Fes"]})
    assert not [v for v in C.verify(codes, ct("h")).violations if v.code == "collision_de_casse"]
    assert [v for v in C.verify(villes, ct("v")).violations if v.code == "collision_de_casse"]


def test_casse_significative_declaree_protege_meme_des_graphies_ordinaires():
    df = pd.DataFrame({"etat": ["OK", "ok", "Ok", "KO"]})
    ct = C.Contrat(colonnes=[C.ContratColonne("etat", "category", casse_significative=True)])
    propre, j = R.appliquer(df, ct)
    assert propre["etat"].nunique() == 4
    assert C.verify(propre, ct, origine=df).tient


def test_un_ancien_contrat_sans_le_champ_se_relit():
    d = C.Contrat(colonnes=[C.ContratColonne("a", "string")]).to_dict()
    d["colonnes"][0].pop("casse_significative")
    assert C.Contrat.from_dict(d).colonnes[0].casse_significative is False


def test_les_dates_jour_dabord_sont_lues_jour_dabord():
    # `15/03/2024` prouve que la colonne est en jour/mois : `01/03/2024` est
    # donc le 1er mars, pas le 3 janvier.
    conv = C.lire_dates(pd.Series(["01/03/2024", "15/03/2024"]))
    assert list(conv) == [pd.Timestamp("2024-03-01"), pd.Timestamp("2024-03-15")]
    conv = C.lire_dates(pd.Series(["03/01/2024", "03/15/2024"]))
    assert list(conv) == [pd.Timestamp("2024-03-01"), pd.Timestamp("2024-03-15")]


def test_une_colonne_de_dates_ambigues_nest_pas_convertie():
    # Rien dans la colonne ne dit si c'est jour/mois ou mois/jour : choisir
    # serait lire de travers la moitié des fichiers. On garde le texte.
    df = journal(quand=["01/03/2024", "02/03/2024", "05/04/2024"])
    res, sortie, types = passe(df)
    assert types["quand"] != "datetime"
    assert list(sortie["quand"].head(3)) == ["01/03/2024", "02/03/2024", "05/04/2024"]
    # ... et le format déclaré lève le doute
    conv = C.lire_dates(pd.Series(["01/03/2024"]), "%d/%m/%Y")
    assert conv[0] == pd.Timestamp("2024-03-01")


# --------------------------------------------------------------------------- #
# Les fichiers réels, quand ils sont là (corpus-public/securite/, non versionné)
# --------------------------------------------------------------------------- #
import pytest                                  # noqa: E402
from pathlib import Path                       # noqa: E402

SECURITE = Path(__file__).resolve().parents[2] / "corpus-public" / "securite"


def _reel(nom):
    f = SECURITE / nom
    if not f.exists():
        pytest.skip(f"{nom} absent — voir corpus-public/README.md")
    res = asyncio.run(agent_contrat.run(f.read_bytes(), nom, usage="base"))
    avant = pd.read_csv(f, dtype=str)
    apres = pd.read_csv(io.BytesIO(base64.b64decode(res["csv_base64"])), dtype=str)
    return res, avant, apres


def test_reel_openssh_garde_ses_heures():
    res, avant, apres = _reel("openssh.csv")
    assert list(apres["time"]) == list(avant["Time"])


def test_reel_zeek_garde_tout_son_champ_history():
    res, avant, apres = _reel("zeek-conn-20k.csv")
    assert apres["history"].nunique() == avant["history"].nunique() == 119
    assert res["contrat_tenu"]
