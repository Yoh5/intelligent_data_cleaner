"""Le format natif de Zeek : un journal qui déclare lui-même son contrat.

Un journal Zeek (`conn.log`, `dns.log`…) n'est pas un CSV. C'est un texte
tabulé qui s'ouvre sur des lignes `#` : le séparateur, la façon d'écrire
« non renseigné » (`#unset_field`), puis `#fields` et `#types` — le nom et le
**type déclaré** de chaque colonne.

Partout ailleurs dans ce projet, le contrat est *inféré* : on regarde les
données et on propose. Ici il n'y a rien à deviner, et deviner serait une
faute : `id.resp_p` est un `port`, donc un entier entre 0 et 65535, et le juger
à l'écart interquartile (ce que faisait l'inférence : « 65534 hors de
l'intervalle plausible ») n'a aucun sens. On **traduit** donc la déclaration
du fichier, et la passe vérifie le journal contre ce qu'il dit de lui-même.

Trois choses que le format impose et qu'on respecte :

- **le marqueur de vide est celui du fichier**, lu dans `#unset_field` ;
- **la casse n'est jamais une faute de frappe** : `history` vaut `ShADTdtr` ou
  `ShADTdtR` selon qui a envoyé le paquet. Aucune colonne de texte d'un journal
  Zeek ne voit ses graphies fusionnées ;
- **tout est lu en texte** : l'horodatage `1521911721.255387` garde ses six
  décimales, que la lecture en flottant arrondirait ;
- **un texte se prend à la lettre** : pas d'autre marqueur de vide que celui du
  fichier, pas d'espace rogné. Trente-quatre journaux réels ont montré que la
  liste générale vidait l'URI `/` et l'algorithme `none`.

Ce que le fichier ne déclare pas, et qu'on mesure donc : quelles colonnes
peuvent être vides, et si `uid` identifie une ligne (vrai pour `conn.log`,
faux pour `dns.log` ou `http.log`, qui ont plusieurs lignes par connexion).
"""
from __future__ import annotations

import gzip
import io
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import pandas as pd

from app.services import contract_infer
from app.services.contract import Contrat, ContratColonne

_GZIP = b"\x1f\x8b"

#: Les noms que Zeek donne à ses identifiants : `uid` (connexion), `fuid`
#: (fichier), `id`. Une colonne unique qui porte un tel nom est adoptée comme
#: clé ; une colonne unique qui n'en porte pas est seulement proposée.
_IDENTIFIANT = re.compile(r"(^|_)f?u?id$")
#: Les types qui mesurent au lieu de nommer. Un horodatage unique l'est
#: jusqu'au jour où deux événements tombent dans la même microseconde.
_MESURES = frozenset({"time", "interval", "double", "count", "int", "port", "bool"})


@dataclass
class Entete:
    """Ce que le journal déclare avant sa première ligne de données."""
    separateur: str = "\t"
    non_renseigne: str = "-"
    vide: str = "(empty)"
    chemin: str = ""
    champs: List[str] = field(default_factory=list)       # noms d'origine
    types: Dict[str, str] = field(default_factory=dict)   # nom normalisé -> type Zeek


class TropVolumineux(ValueError):
    """Le journal dépasse la taille admise une fois décompressé."""


def _decompresser(contenu: bytes, max_octets: Optional[int] = None) -> bytes:
    """Décompresse, **par morceaux et sous plafond** quand on en donne un.

    Un fichier compressé ne dit pas sa taille réelle : soixante mégaoctets de
    zéros tiennent en soixante kilo-octets. Une route qui décompresserait
    d'un bloc laisserait un envoi minuscule faire allouer au serveur ce qu'il
    veut. La commande, elle, lit des fichiers locaux et ne donne pas de plafond.
    """
    if contenu[:2] != _GZIP:
        if max_octets is not None and len(contenu) > max_octets:
            raise TropVolumineux(f"journal de {len(contenu) / 1048576:.0f} Mo pour un "
                                 f"maximum de {max_octets / 1048576:.0f} Mo")
        return contenu
    if max_octets is None:
        return gzip.decompress(contenu)
    morceaux, total = [], 0
    with gzip.open(io.BytesIO(contenu), "rb") as g:
        while True:
            bloc = g.read(1024 * 1024)
            if not bloc:
                break
            total += len(bloc)
            if total > max_octets:
                raise TropVolumineux(
                    f"journal de plus de {max_octets / 1048576:.0f} Mo une fois "
                    f"décompressé : au-delà de ce qu'une requête traite")
            morceaux.append(bloc)
    return b"".join(morceaux)


def est_zeek(contenu: bytes) -> bool:
    """Un journal Zeek s'ouvre sur `#separator`. On ne décompresse que le début."""
    try:
        if contenu[:2] == _GZIP:
            with gzip.open(io.BytesIO(contenu), "rb") as g:
                debut = g.read(16)
        else:
            debut = contenu[:16]
    except Exception:
        return False
    return debut.startswith(b"#separator")


def _separateur(valeur: str) -> str:
    """`\\x09` -> tabulation : le séparateur est écrit en échappement."""
    valeur = valeur.strip()
    if valeur.startswith("\\x") and len(valeur) == 4:
        return chr(int(valeur[2:], 16))
    return valeur or "\t"


def lire(contenu: bytes, max_octets: Optional[int] = None) -> Tuple[pd.DataFrame, Entete]:
    """Le journal en DataFrame de **texte**, et son en-tête.

    Les noms de colonnes passent par la normalisation du chargeur
    (`id.orig_h` -> `id_orig_h`), pour que le contrat, le rapport et la base
    qui recevra les données parlent des mêmes noms.
    """
    from app.services.profiler import DataProfiler

    texte = _decompresser(contenu, max_octets).decode("utf-8", errors="replace")
    e = Entete()
    types_bruts: List[str] = []
    donnees: List[str] = []
    for ligne in texte.split("\n"):
        ligne = ligne.rstrip("\r")
        if not ligne:
            continue
        if not ligne.startswith("#"):
            donnees.append(ligne)
            continue
        if ligne.startswith("#separator"):
            e.separateur = _separateur(ligne[len("#separator"):])
            continue
        cle, _, reste = ligne.partition(e.separateur)
        if cle == "#unset_field":
            e.non_renseigne = reste
        elif cle == "#empty_field":
            e.vide = reste
        elif cle == "#path":
            e.chemin = reste
        elif cle == "#fields":
            e.champs = reste.split(e.separateur)
        elif cle == "#types":
            types_bruts = reste.split(e.separateur)

    manque = [n for n, v in (("#fields", e.champs), ("#types", types_bruts)) if not v]
    if manque:
        raise ValueError(f"journal Zeek incomplet : ligne(s) {', '.join(manque)} absente(s) "
                         f"de l'en-tête — sans elles le fichier ne déclare pas son contrat")
    if len(e.champs) != len(types_bruts):
        raise ValueError(f"journal Zeek incohérent : {len(e.champs)} champs déclarés "
                         f"pour {len(types_bruts)} types")
    if not donnees:
        raise ValueError("Fichier vide : le journal Zeek ne contient aucune ligne de données")

    n = len(e.champs)
    lignes = [l.split(e.separateur) for l in donnees]
    for i, l in enumerate(lignes):
        if len(l) != n:
            raise ValueError(f"ligne de données {i + 1} : {len(l)} champs pour {n} déclarés "
                             f"— journal tronqué ou séparateur présent dans une valeur")

    noms = [str(c) for c in DataProfiler._sanitize_columns(pd.Index(e.champs))]
    e.types = dict(zip(noms, types_bruts))
    df = pd.DataFrame(lignes, columns=noms, dtype=object)
    # Le non-renseigné DÉCLARÉ devient un vrai vide ; rien d'autre.
    df = df.mask(df == e.non_renseigne)
    return df, e


# --------------------------------------------------------------------------- #
# La déclaration du fichier, traduite en contrat
# --------------------------------------------------------------------------- #
def _colonne(nom: str, type_zeek: str) -> ContratColonne:
    """Un type Zeek -> ce que la colonne doit garantir.

    La casse est déclarée significative partout où le type est du texte : dans
    un journal, une graphie est un fait observé, pas une saisie à normaliser.
    """
    t = type_zeek.strip()
    if t == "port":
        return ContratColonne(nom, "integer", minimum=0, maximum=65535)
    if t == "count":
        return ContratColonne(nom, "integer", minimum=0)
    if t == "int":
        return ContratColonne(nom, "integer")
    if t in ("time", "interval", "double"):
        # Secondes, en nombre : `time` est une date d'époque, qu'on ne convertit
        # pas en calendrier — le fuseau n'est écrit nulle part dans le journal.
        # Aucune borne : ce sont des doubles SIGNÉS. Un `ntp.log` réel porte des
        # dispersions négatives et des horodatages d'avant 1970 ; exiger
        # « positif » le déclarait fautif sur une règle que le fichier n'écrit
        # nulle part. Seuls `count` et `port` sont bornés par leur définition.
        return ContratColonne(nom, "number")
    if t == "bool":
        return ContratColonne(nom, "boolean")
    if t == "addr":
        return ContratColonne(nom, "string", forme="ip", casse_significative=True)
    if t == "enum":
        return ContratColonne(nom, "category", casse_significative=True)
    # string, subnet, set[...], vector[...], et tout type à venir : du texte,
    # intouché.
    return ContratColonne(nom, "string", casse_significative=True)


def contrat(df: pd.DataFrame, entete: Entete,
            usage: str = contract_infer.USAGE_DEFAUT) -> Tuple[Contrat, List[str]]:
    """Le contrat que le journal déclare, complété par ce qu'il faut mesurer."""
    reglage = contract_infer.USAGES.get(usage, contract_infer.USAGES[contract_infer.USAGE_DEFAUT])
    notes = [
        f"journal Zeek « {entete.chemin or '?'} » : les types viennent de la ligne "
        f"#types du fichier, ils ne sont pas inférés. Le non-renseigné est "
        f"« {entete.non_renseigne} », comme le fichier le déclare, et c'est le seul : "
        f"un texte comme « / », « none » ou « NULL » reste une valeur, et les "
        f"espaces en bordure sont gardés.",
    ]
    colonnes: List[ContratColonne] = []
    n = len(df)
    for nom in df.columns:
        cc = _colonne(nom, entete.types.get(nom, "string"))
        # Aucune correction d'aberrante : dans un journal, la valeur extrême
        # est souvent l'incident.
        cc.politique_aberrantes = "signaler"
        taux = float(df[nom].isna().mean()) if n else 0.0
        if taux > 0:
            # Le format ne dit pas quels champs sont facultatifs : on le
            # mesure, et on ne comble rien — un champ non renseigné l'est.
            cc.nullable, cc.taux_nul_max = True, min(1.0, round(taux + 0.01, 4))
        else:
            cc.nullable, cc.taux_nul_max = False, 0.0
        cc.imputation_max = 0.0
        colonnes.append(cc)

    ct = Contrat(colonnes=colonnes, usage=usage,
                 lignes_perdues_max=float(reglage["lignes_perdues_max"]),
                 imputation_max=0.0,
                 cle_exigee=bool(reglage["exige_cle"]),
                 # Un texte de journal se prend à la lettre : `/`, `none` ou
                 # `NULL` y sont des valeurs observées, et l'indentation de
                 # `loaded_scripts` aussi. Le seul vide est celui que le
                 # fichier déclare, déjà posé à la lecture.
                 marqueurs_vides=[], espaces_significatifs=True)
    vides = sorted(c.nom for c in colonnes if c.nullable)
    if vides:
        notes.append(f"champ(s) parfois non renseigné(s), laissés vides et jamais "
                     f"comblés : {', '.join(vides)}.")

    pleines = [c for c in df.columns if n and not df[c].isna().any()]
    uniques = [c for c in pleines if df[c].is_unique]
    # Adoptée d'office : seulement une colonne dont le NOM dit qu'elle
    # identifie (`uid`, `fuid`, `id`) et dont le type n'est pas une mesure.
    # Sur un journal de trois lignes tout est unique ; `referrer` ne devient
    # pas une clé pour autant.
    nommees = sorted((c for c in uniques if _IDENTIFIANT.search(c)
                      and entete.types.get(c, "string").strip() not in _MESURES),
                     key=lambda c: (c != "uid", list(df.columns).index(c)))
    if nommees:
        ct.cle_primaire = [nommees[0]]
        for cc in colonnes:
            if cc.nom == nommees[0]:
                cc.unique = True
    elif ct.cle_exigee:
        notes.append(
            "aucune clé adoptée : `uid` est absent ou répété (normal pour dns.log "
            f"ou http.log, qui ont plusieurs lignes par connexion), alors que "
            f"« {reglage['libelle']} » en exige une.")
        # Ce que le fichier ne réfute pas : une colonne unique sans nom
        # d'identifiant, sinon la plus petite combinaison.
        ct.cles_candidates = ([[c] for c in uniques][:contract_infer.CLE_PROPOSITIONS]
                              if n > 1 else [])
        if not ct.cles_candidates:
            ct.cles_candidates = contract_infer.cles_composites(df, pleines)
        if ct.cles_candidates:
            notes.append(contract_infer.note_cles(ct.cles_candidates, reglage["libelle"]))
    return ct, notes
