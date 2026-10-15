"""Le parcours que l'utilisateur fait vraiment : téléverser, puis lancer le script.

`test_execute_guard.py` vérifie que `/execute/` refuse ce qu'il doit refuser.
`test_corpus*.py` vérifient la passe de contrat. **Personne ne vérifiait que le
script produit par `/auto-clean/` nettoie réellement un fichier**, ce qui est
pourtant le produit tel qu'un utilisateur le rencontre : il dépose un CSV, il
récupère un `.py`, il le lance chez lui.

Entre les deux il y a tout ce qui peut casser sans qu'aucun test existant ne
bronche : une stratégie dont le `code_preview` ne s'assemble pas avec la
suivante, un script syntaxiquement valide qui plante à l'exécution sur un
encodage, un nettoyage qui « réussit » en supprimant les quatre cinquièmes des
lignes.

Ce fichier pose donc trois questions par jeu, dans cet ordre :

1. Le script sort-il, et la garde l'accepte-t-elle ?
2. Tourne-t-il jusqu'au bout sur le fichier dont il est issu ?
3. **Reste-t-il des données ?** C'est la question qui compte. Un script qui vide
   le fichier passe les deux premières et ruine l'utilisateur, et c'est le seul
   échec que « le script a tourné » ne peut pas voir.
"""
from __future__ import annotations

import base64
import io
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app.main import app

CORPUS = Path(__file__).resolve().parent.parent.parent / "corpus-public"
PUBLICS = sorted(CORPUS.glob("*.csv")) if CORPUS.exists() else []

#: Quelques fichiers sales écrits ici, pour que le parcours soit couvert même
#: sans le corpus public — qui n'est pas versionné.
SALES: dict[str, str] = {
    "doublons.csv": (
        "commande,client,produit,prix\n"
        "1001,Dupont,Clavier,49.90\n"
        "1002,Martin,Souris,19.50\n"
        "1001,Dupont,Clavier,49.90\n"
        "1003,Bernard,Ecran,189.00\n"
        "1002,Martin,Souris,19.50\n"
        "1004,Petit,Casque,35.00\n"
        "1005,Durand,Webcam,79.00\n"
        "1001,Dupont,Clavier,49.90\n"
    ),
    "manquants.csv": (
        "id,nom,email,age,ville\n"
        "1,Alice,alice@example.com,34,Lyon\n"
        "2,Bob,,41,Paris\n"
        "3,Chloe,chloe@example.com,,Marseille\n"
        "4,David,david@example.com,29,\n"
        "5,Emma,,,Lille\n"
        "6,Farid,farid@example.com,52,Nice\n"
        "7,Gaelle,gaelle@example.com,38,Lyon\n"
        "8,Hugo,,45,Paris\n"
    ),
    "espaces_casse.csv": (
        "  Reference ,  Libelle  ,Categorie\n"
        " REF-001 ,  Vis 4mm  ,quincaillerie\n"
        "REF-002, Ecrou M4 ,QUINCAILLERIE\n"
        " ref-003 ,Rondelle  ,Quincaillerie \n"
        "REF-004,  Boulon,quincaillerie\n"
        "REF-005 , Cheville  , PLOMBERIE \n"
        "ref-006,Joint,plomberie\n"
    ),
    "point_virgule.csv": (
        "id;ville;population\n"
        "1;Casablanca;3359818\n"
        "2;Rabat;577827\n"
        "3;Fes;1112072\n"
        "4;Marrakech;928850\n"
        "5;Tanger;947952\n"
    ),
    "deja_propre.csv": (
        "id,produit,prix\n"
        "1,Clavier,49.90\n"
        "2,Souris,19.50\n"
        "3,Ecran,189.00\n"
        "4,Casque,35.00\n"
        "5,Webcam,79.00\n"
    ),
}


def lignes_de(octets: bytes) -> int:
    """Pas de `sep=None` : le renifleur de pandas lève sur un champ long,
    et on mesurerait le harnais au lieu du produit."""
    dernier = None
    for encodage in ("utf-8-sig", "utf-8", "latin-1"):
        for sep in (",", ";", "	", "|"):
            try:
                df = pd.read_csv(io.BytesIO(octets), encoding=encodage, sep=sep)
            except Exception as erreur:            # noqa: BLE001
                dernier = erreur
                continue
            if df.shape[1] > 1 or sep == "|":
                return len(df)
    raise AssertionError(f"la sortie n'est pas un CSV relisible : {dernier}")


def contenu_rendu(reponse_json: dict) -> bytes:
    """`/execute/` rend du JSON avec le fichier en base64, pas des octets bruts."""
    return base64.b64decode(reponse_json["file_base64"])


def parcours(client: TestClient, nom: str, octets: bytes) -> tuple[dict, bytes | None]:
    """auto-clean puis execute, comme l'interface le fait."""
    auto = client.post("/auto-clean/",
                       files={"file": (nom, octets, "text/csv")})
    assert auto.status_code == 200, f"{nom} : /auto-clean/ a rendu {auto.status_code}"
    corps = auto.json()
    if not corps.get("script"):
        return corps, None
    sortie = client.post("/execute/",
                         files={"file": (nom, octets, "text/csv")},
                         data={"script": corps["script"]})
    assert sortie.status_code == 200, (
        f"{nom} : le script produit par le produit lui-même a été rejeté ou a "
        f"planté ({sortie.status_code}) — {sortie.text[:300]}")
    return corps, contenu_rendu(sortie.json())


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


# --------------------------------------------------------------------------- #
# Fichiers sales écrits ici
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("nom", list(SALES), ids=list(SALES))
def test_le_script_produit_tourne_et_laisse_des_donnees(client, nom):
    octets = SALES[nom].encode("utf-8")
    avant = lignes_de(octets)
    corps, nettoye = parcours(client, nom, octets)

    if nettoye is None:
        assert corps["issues_found"] == 0, (
            f"{nom} : aucun script alors que {corps['issues_found']} problème(s) "
            "ont été trouvés")
        return

    apres = lignes_de(nettoye)
    assert apres > 0, f"{nom} : le nettoyage a vidé le fichier"
    assert apres >= avant * 0.5, (
        f"{nom} : {avant} lignes avant, {apres} après — plus de la moitié des "
        "données est partie, ce qui demande une justification et pas un succès")


def test_les_doublons_sont_vraiment_partis(client):
    """Le seul défaut dont on peut vérifier la disparition sans ambiguïté."""
    octets = SALES["doublons.csv"].encode("utf-8")
    _, nettoye = parcours(client, "doublons.csv", octets)
    assert nettoye is not None, "aucun script produit sur un fichier à doublons"
    df = pd.read_csv(io.BytesIO(nettoye))
    assert len(df) == len(df.drop_duplicates()), (
        "des doublons stricts survivent au script de nettoyage")
    # Et les lignes uniques, elles, sont toutes là.
    assert {"Bernard", "Petit", "Durand"} <= set(df["client"].astype(str))


def test_un_fichier_propre_ne_produit_pas_de_travail(client):
    octets = SALES["deja_propre.csv"].encode("utf-8")
    corps, nettoye = parcours(client, "deja_propre.csv", octets)
    if nettoye is not None:
        assert lignes_de(nettoye) == 5, "un fichier propre a perdu des lignes"


def test_le_point_virgule_est_reconnu(client):
    """Un séparateur mal lu donne une seule colonne et un nettoyage absurde."""
    octets = SALES["point_virgule.csv"].encode("utf-8")
    corps, _ = parcours(client, "point_virgule.csv", octets)
    assert corps["analysis"]["columns"] == 3, (
        f"séparateur mal détecté : {corps['analysis']['columns']} colonne(s) "
        "au lieu de 3")


# --------------------------------------------------------------------------- #
# Jeux réels
# --------------------------------------------------------------------------- #
@pytest.mark.skipif(not PUBLICS,
                    reason="corpus-public/ vide — voir corpus-public/README.md")
@pytest.mark.parametrize("chemin", PUBLICS, ids=[f.name for f in PUBLICS])
def test_un_jeu_reel_survit_au_parcours_complet(client, chemin):
    """Ce que les fichiers fabriqués ne produisent pas : de vrais encodages,
    de vraies colonnes mixtes, de vraies tailles."""
    octets = chemin.read_bytes()
    avant = lignes_de(octets)
    corps, nettoye = parcours(client, chemin.name, octets)
    if nettoye is None:
        return
    apres = lignes_de(nettoye)
    assert apres > 0, f"{chemin.name} : le nettoyage a vidé le fichier"
    assert apres >= avant * 0.5, (
        f"{chemin.name} : {avant} -> {apres} lignes, plus de la moitié perdue")
