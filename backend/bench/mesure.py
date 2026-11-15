"""Comparer une sortie à la vérité, cellule par cellule.

**« Prêt à l'usage » est défini ici, une fois, et sévèrement.** Une cellule est
juste si une machine peut la lire dans le type de sa colonne sans
interprétation :

- un nombre s'écrit avec un point décimal (`1234.5`, pas `1234,5`) ;
- un entier n'a pas de partie décimale (`42`, pas `42.0` : une colonne INTEGER
  le refuserait au chargement) ;
- une date est au format ISO (`2026-03-15`, une heure nulle tolérée) ;
- un code garde ses zéros initiaux, un texte sa graphie exacte ;
- l'inconnu est une cellule **vide** — pas `N/A`, pas une valeur inventée.

On ne lit jamais la sortie avec pandas : sa lecture convertit, et une
conversion est exactement ce qu'on cherche à juger. Le CSV est relu avec le
module `csv`, en texte.
"""
from __future__ import annotations

import csv
import io
import math
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from bench.defauts import Echantillon

_ISO = re.compile(r"^(\d{4})-(\d{2})-(\d{2})(?:[ T]00:00:00(?:\.0+)?)?$")
_ENTIER = re.compile(r"^[+-]?\d+$")
_NOMBRE = re.compile(r"^[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$")
VRAI = {"true", "True", "TRUE"}
FAUX = {"false", "False", "FALSE"}


def lire_csv(octets: bytes) -> List[Dict[str, str]]:
    texte = octets.decode("utf-8-sig", errors="replace")
    return list(csv.DictReader(io.StringIO(texte, newline="")))


def canonique(valeur: Optional[str], role: str) -> Optional[str]:
    """La forme comparable d'une cellule de SORTIE, ou une marque d'échec.

    Renvoie `None` pour une cellule vide, et la chaîne `"\\x00illisible"` pour
    une cellule qui ne tient pas la forme prête-à-l'usage de son rôle.
    """
    if valeur is None or valeur == "":
        return None
    if role in ("nombre",):
        if not _NOMBRE.match(valeur):
            return "\x00illisible"
        x = float(valeur)
        return "\x00illisible" if math.isnan(x) else repr(x)
    if role in ("entier", "id"):
        return valeur if _ENTIER.match(valeur) else "\x00illisible"
    if role == "date":
        m = _ISO.match(valeur)
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else "\x00illisible"
    if role == "booleen":
        if valeur in VRAI:
            return "true"
        if valeur in FAUX:
            return "false"
        return "\x00illisible"
    return valeur                       # code, catégorie, texte : à la lettre


def _verite(valeur: Optional[str], role: str) -> Optional[str]:
    # Selon la version de pandas, une case vidée par `None` dans une colonne
    # objet ressort `None` ou `NaN` (la CI, en pandas 3, rendait « nan »).
    if valeur is None or (isinstance(valeur, float) and math.isnan(valeur)):
        return None
    if role == "nombre":
        return repr(float(valeur))
    return valeur


@dataclass
class Mesure:
    nettoyeur: str
    jeu: str
    defauts: int = 0
    corrigees: int = 0
    saines: int = 0
    cassees: int = 0
    inventees: int = 0
    #: inventées MAIS signalées ligne par ligne par une colonne `<col>_impute`
    inventees_declarees: int = 0
    #: « base » : aucune invention tolérée ; « modele » : inventions tolérées si déclarées
    profil: str = "base"
    doublons_injectes: int = 0
    doublons_restants: int = 0
    lignes_perdues: int = 0
    colonnes_perdues: List[str] = field(default_factory=list)
    #: ce que l'outil dit de sa propre sortie : True « prêt », False « pas prêt », None muet
    verdict_outil: Optional[bool] = None
    verdict_detail: str = ""
    par_defaut: Dict[str, List[int]] = field(default_factory=dict)   # défaut -> [corrigés, total]
    exemples_casses: List[str] = field(default_factory=list)
    exemples_manques: List[str] = field(default_factory=list)
    duree_s: float = 0.0
    erreur: str = ""

    @property
    def manquees(self) -> int:
        return self.defauts - self.corrigees

    @property
    def inventees_silencieuses(self) -> int:
        return self.inventees - self.inventees_declarees

    @property
    def prete(self) -> bool:
        """Le verdict du BANC : la sortie est-elle prête à l'usage annoncé ?

        Pour un chargement en base, rien ne doit être inventé. Pour un modèle,
        combler est un choix légitime à une condition : que chaque valeur
        inventée reste reconnaissable. Une invention silencieuse ne l'est jamais.
        """
        tolerees = self.inventees_declarees if self.profil == "modele" else 0
        return (not self.erreur and self.manquees == 0 and self.cassees == 0
                and self.inventees - tolerees == 0 and self.doublons_restants == 0
                and self.lignes_perdues == 0 and not self.colonnes_perdues)

    @property
    def verdict_juste(self) -> Optional[bool]:
        return None if self.verdict_outil is None else self.verdict_outil == self.prete


def mesurer(ech: Echantillon, sortie: bytes, nettoyeur: str, profil: str = "base") -> Mesure:
    v = ech.verite
    m = Mesure(nettoyeur, v.nom, doublons_injectes=ech.doublons, profil=profil)
    id_col = next(c for c, r in v.roles.items() if r == "id")
    lignes = lire_csv(sortie)
    entetes = list(lignes[0].keys()) if lignes else []
    m.colonnes_perdues = [c for c in v.roles if c not in entetes]

    par_id: Dict[str, Dict[str, str]] = {}
    for ligne in lignes:
        cle = canonique(ligne.get(id_col, ""), "id")
        if cle is None or cle.startswith("\x00"):
            continue
        if cle in par_id:
            m.doublons_restants += 1
            continue
        par_id[cle] = ligne

    defauts = {(i, c): d for i, c, d in ech.defauts}
    for _, d in defauts.items():
        m.par_defaut.setdefault(d, [0, 0])[1] += 1

    for _, rangee in v.table.iterrows():
        ident = rangee[id_col]
        sortie_ligne = par_id.get(ident)
        if sortie_ligne is None:
            m.lignes_perdues += 1
            continue
        for col, role in v.roles.items():
            if col in m.colonnes_perdues:
                continue
            vrai = _verite(rangee[col], role)
            lu = canonique(sortie_ligne.get(col, ""), role)
            juste = lu == vrai
            defaut = defauts.get((ident, col))
            if vrai is None and lu is not None and not (lu or "").startswith("\x00"):
                m.inventees += 1
                declaree = sortie_ligne.get(f"{col}_impute", "") in VRAI
                m.inventees_declarees += declaree
                if declaree and profil == "modele":
                    juste = True        # tolérée pour cet usage : ni manquée, ni cassée
            if defaut:
                m.defauts += 1
                if juste:
                    m.corrigees += 1
                    m.par_defaut[defaut][0] += 1
                elif len(m.exemples_manques) < 6:
                    m.exemples_manques.append(
                        f"{col} [{defaut}] : attendu {vrai!r}, livré {sortie_ligne.get(col, '')!r}")
            else:
                m.saines += 1
                if not juste:
                    m.cassees += 1
                    if len(m.exemples_casses) < 6:
                        m.exemples_casses.append(
                            f"{col} : attendu {vrai!r}, livré {sortie_ligne.get(col, '')!r}")

    # Les doublons non retirés comptent ici, une ligne par copie en trop.
    return m
