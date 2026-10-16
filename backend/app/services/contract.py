"""Contrat de données — ce que la donnée doit garantir, et la vérification de ces
garanties **indépendamment du nettoyeur**.

## Pourquoi ce module existe

L'ancienne fonction objectif (`quality.score_from_analysis`) partait de 100 et
retranchait une pénalité par problème **détecté par notre propre profileur**.
L'agent n'optimisait donc pas « la donnée est utilisable » mais « mon profileur
ne se plaint plus ». Mesuré sur un CSV de 430 lignes, ça donnait 100/100 avec :

- le mot « inconnu » imputé dans 247 lignes sur 400 (c'était le mode de la
  colonne, et `inconnu` n'est pas dans `MISSING_PATTERNS`) ;
- `Rabat` et `rabat` toujours distincts (aucun contrôle de casse n'existe) ;
- un tiers de `note` écrasé sur la médiane, en masse ponctuelle ;
- toutes les colonnes rendues en texte.

Un score ne peut pas arbitrer son propre nettoyage. **Un contrat, si** : il est
déclaré avant, vérifié après, et ses contrôles sont des mesures sur le
DataFrame — ils ne consultent jamais la liste de problèmes du profileur.

## Ce qu'un contrat déclare

Par colonne : un type, la nullité tolérée, l'unicité, un domaine ou une plage.
Globalement : une clé primaire, et un **budget de perte d'information** (lignes
supprimables, taux d'imputation admissible). Ce dernier point est ce qui empêche
de « nettoyer » en détruisant : imputer un tiers d'une variable par sa médiane
n'est pas un nettoyage, et le contrat le refuse par défaut.

## Ce que la vérification rend

`verify()` rend un rapport à deux faces, et les deux comptent :

- **`violations`** — ce qui ne tient pas, chacune mesurée et chiffrée ;
- **`satisfait`** — ce qui tient, énoncé. C'est le livrable qu'un data engineer
  remet : non pas « c'est propre », mais « voici ce qui est désormais garanti ».

Déterministe, sans réseau, sans LLM.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import re

import numpy as np
import pandas as pd

#: Marqueurs de vide rencontrés dans de vraies données. Sur-ensemble de
#: `DataProfiler.MISSING_PATTERNS`, qui connaît `Unknown` mais pas `inconnu` —
#: l'oubli qui a permis d'imputer un marqueur de vide 247 fois en le prenant
#: pour une modalité. Un contrôle de contrat doit être plus large que le
#: chargeur, justement parce que c'est lui qui rattrape ce que le chargeur rate.
MARQUEURS_VIDES = frozenset({
    "", "-", "--", "---", "?", "??", "???", ".", "/",
    "na", "n/a", "n.a.", "nan", "null", "none", "nil", "nd", "n.d.",
    "missing", "unknown", "not available", "not applicable", "undefined",
    "inconnu", "inconnue", "non renseigne", "non renseigné", "non precise",
    "non précisé", "neant", "néant", "vide", "aucun", "aucune", "ras", "sans",
})

def est_vide(texte: pd.Series) -> pd.Series:
    """Masque des cellules qui ne portent qu'un marqueur de vide.

    `texte` est la colonne en chaînes, non détourée.

    **Une tentative d'optimisation a été essayée, puis retirée.** Ce détourage
    appelle une fonction Python par cellule — 4,5 millions d'appels sur un
    fichier de 500 000 lignes, et le poste le plus cher de l'inférence une fois
    les doublons de travail éliminés. Remplacer les trois parcours
    (`strip`, `lower`, `isin`) par une seule passe de `fullmatch` sur une
    alternation de tous les marqueurs paraissait évident.

    Mesuré : 10,6 secondes contre 9,1 — mais l'écart-type d'un passage à l'autre
    est de 0,8 seconde sur cinq répétitions, donc **l'essai n'a rien démontré,
    ni dans un sens ni dans l'autre**. On garde la version simple, faute de gain
    établi. Et on note le chiffre de dispersion, parce que sans lui on relirait
    « 10,6 contre 9,1 » comme un résultat alors que c'est du bruit.
    """
    return texte.str.strip().str.lower().isin(MARQUEURS_VIDES)


#: Jetons booléens acceptés, en minuscules.
VRAI = frozenset({"1", "true", "vrai", "yes", "oui", "y", "o", "t"})
FAUX = frozenset({"0", "false", "faux", "no", "non", "n", "f"})

TYPES = ("integer", "number", "boolean", "datetime", "category", "string")

#: Les formes qu'une colonne de texte peut être tenue de respecter.
#:
#: Un type dit ce qu'une valeur **est** ; une forme dit à quoi elle doit
#: **ressembler**. `source_ip` est du texte pour n'importe quel type, et
#: `999.1.2.3` est du texte parfaitement valide — seule une forme le refuse.
#: C'est le contrôle qui manquait, et c'est celui qui compte sur des données
#: de sécurité : une adresse, un port, un condensat, une adresse MAC.
#:
#: Les motifs sont volontairement stricts. Un motif permissif laisse passer ce
#: qu'il prétend attraper, et une forme qui ne refuse rien est pire qu'absente
#: puisqu'elle rassure.
FORMES: Dict[str, str] = {
    "ipv4": r"^((25[0-5]|2[0-4]\d|[01]?\d?\d)\.){3}(25[0-5]|2[0-4]\d|[01]?\d?\d)$",
    "ipv6": r"^(([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|::|([0-9a-fA-F]{1,4}:){1,7}:|:(:[0-9a-fA-F]{1,4}){1,7})$",
    "mac": r"^([0-9a-fA-F]{2}[:-]){5}[0-9a-fA-F]{2}$",
    "email": r"^[^@\s]+@[^@\s.]+\.[^@\s]{2,}$",
    "uuid": r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
    "url": r"^https?://[^\s/$.?#][^\s]*$",
    "sha256": r"^[0-9a-fA-F]{64}$",
    "md5": r"^[0-9a-fA-F]{32}$",
}

#: Une violation bloquante plafonne le score : on ne peut pas être « à 100 »
#: avec une clé non unique ou une colonne qui refuse son type. C'est ce plafond
#: qui rend impossible le 100/100 observé sur des données inutilisables.
BLOQUANTES = frozenset({
    "colonne_absente", "type_inconvertible", "cle_non_unique",
    "marqueur_vide_en_valeur", "colonne_supprimee",
})
PLAFOND_BLOQUANT = 55

POIDS = {
    "colonne_absente": 25, "type_inconvertible": 20, "cle_non_unique": 20,
    "marqueur_vide_en_valeur": 15, "colonne_supprimee": 20,
    "doublons_lignes": 12, "taux_nul_depasse": 10, "imputation_excessive": 12,
    "masse_ponctuelle": 12, "lignes_perdues": 15, "hors_domaine": 8,
    "hors_plage": 8, "hors_forme": 10, "collision_de_casse": 6, "colonne_inattendue": 2,
}


# --------------------------------------------------------------------------- #
# Déclaration
# --------------------------------------------------------------------------- #
@dataclass
class ContratColonne:
    """Ce qu'une colonne doit garantir."""
    nom: str
    type: str = "string"
    nullable: bool = True
    taux_nul_max: float = 1.0
    unique: bool = False
    domaine: Optional[List[Any]] = None
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    format_date: Optional[str] = None
    #: La forme que chaque valeur non vide doit respecter : un nom de `FORMES`
    #: ou une expression régulière. Une valeur qui ne la respecte pas est une
    #: **violation**, jamais une valeur corrigée en silence.
    forme: Optional[str] = None
    #: Budget d'imputation propre à la colonne ; `None` -> celui du contrat.
    imputation_max: Optional[float] = None
    #: Que faire des valeurs hors de [minimum, maximum] :
    #:
    #: - `"signaler"` (défaut) — on ne touche à rien, la vérification les
    #:   compte et le journal les nomme. C'est le seul choix qui ne fabrique
    #:   rien ;
    #: - `"ecreter"` — ramener au bord le plus proche. **Cela invente des
    #:   valeurs** : trois cents revenus à 1 200 000 deviennent trois cents
    #:   revenus exactement égaux au plafond, et toute statistique calculée
    #:   ensuite porte sur une distribution qu'on a façonnée ;
    #: - `"vider"` — mettre à vide, ce qui est honnête mais déplace le problème
    #:   sur le budget d'imputation.
    #:
    #: Le défaut n'est pas de la timidité. Une valeur aberrante est tantôt une
    #: erreur de saisie, tantôt le client le plus important du fichier, et rien
    #: dans la colonne ne permet de les distinguer. Un outil qui tranche à la
    #: place de l'utilisateur se trompe en silence une fois sur deux.
    politique_aberrantes: str = "signaler"

    def __post_init__(self) -> None:
        if self.type not in TYPES:
            raise ValueError(f"type inconnu : {self.type!r} (attendu : {TYPES})")
        if self.forme is not None and self.forme not in FORMES:
            # Un nom inconnu est traité comme une expression régulière, mais
            # il doit compiler : une regex fautive refuserait tout, ce qui se
            # lirait comme une colonne entièrement invalide.
            try:
                re.compile(self.forme)
            except re.error as erreur:
                raise ValueError(
                    f"forme {self.forme!r} : ni un nom connu {tuple(FORMES)} "
                    f"ni une expression régulière valide ({erreur})") from erreur
        if not self.nullable:
            self.taux_nul_max = 0.0


@dataclass
class Contrat:
    """Ce que le jeu de données entier doit garantir."""
    colonnes: List[ContratColonne] = field(default_factory=list)
    cle_primaire: List[str] = field(default_factory=list)
    sans_doublons: bool = True
    #: Part de lignes qu'on s'autorise à perdre au nettoyage, hors doublons
    #: francs (ceux-là ne sont pas une perte d'information).
    lignes_perdues_max: float = 0.05
    #: Part de cellules qu'on s'autorise à imputer dans une colonne. Au-delà,
    #: la variable n'est plus mesurée, elle est fabriquée.
    imputation_max: float = 0.20
    #: Colonnes hors contrat : tolérées (on ne connaît pas tout) mais listées.
    colonnes_inattendues_interdites: bool = False
    usage: str = ""

    def par_nom(self) -> Dict[str, ContratColonne]:
        return {c.nom: c for c in self.colonnes}

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Contrat":
        cols = [ContratColonne(**c) for c in d.get("colonnes", [])]
        autres = {k: v for k, v in d.items() if k != "colonnes"}
        return Contrat(colonnes=cols, **autres)


@dataclass
class Violation:
    code: str
    colonne: Optional[str]
    message: str
    #: L'ampleur mesurée (taux, compte) — ce qui rend la violation discutable
    #: plutôt qu'assénée.
    mesure: Optional[float] = None
    exemples: List[Any] = field(default_factory=list)

    @property
    def bloquante(self) -> bool:
        return self.code in BLOQUANTES


@dataclass
class Rapport:
    violations: List[Violation] = field(default_factory=list)
    satisfait: List[str] = field(default_factory=list)
    score: int = 0
    lignes: int = 0
    colonnes: int = 0

    @property
    def tient(self) -> bool:
        return not self.violations

    @property
    def bloquantes(self) -> List[Violation]:
        return [v for v in self.violations if v.bloquante]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tient": self.tient,
            "score": self.score,
            "lignes": self.lignes,
            "colonnes": self.colonnes,
            "violations": [
                {**asdict(v), "bloquante": v.bloquante} for v in self.violations
            ],
            "satisfait": self.satisfait,
        }


# --------------------------------------------------------------------------- #
# Conversions : « cette colonne tient-elle son type ? »
# --------------------------------------------------------------------------- #
def _texte(s: pd.Series) -> pd.Series:
    return s.dropna().astype(str).str.strip()


#: Ce qu'on retire avant de lire un nombre : espaces de toute sorte, espace
#: insécable, séparateur de milliers fin, signe pourcent.
_BRUIT_NUMERIQUE = r"[\s  %]"


def _numerique(s: pd.Series) -> pd.Series:
    """Conversion numérique tolérante au format européen (virgule, espace fin).

    Deux optimisations, mesurées plutôt que supposées. Le profil d'un fichier de
    500 000 lignes montrait **6,5 secondes sur 14,5** passées ici, en quatre
    `str.replace` enchaînés — soit quatre parcours Python de la colonne entière,
    et près de huit millions d'appels de lambda.

    1. **Chemin rapide.** La plupart des colonnes numériques sont déjà écrites
       proprement : `to_numeric` les lit sans qu'on touche au texte. On n'entre
       dans la normalisation que si la lecture directe échoue quelque part.
    2. **Un seul passage de nettoyage** par expression régulière au lieu de
       quatre remplacements littéraux.
    """
    # Les booléens EN PREMIER, et convertis. `is_numeric_dtype` répond `True`
    # pour `bool`, si bien qu'une colonne booléenne ressortait telle quelle et
    # faisait lever tout ce qui soustrait ensuite — un quantile, un écart
    # interquartile, un écrêtage. C'est la troisième fois que ce piège se
    # referme dans ce projet : il avait déjà fait planter le profileur sur un
    # fichier de 17 colonnes dont 8 booléennes, puis l'inférence de plage.
    # On le coupe ici, à la racine, plutôt qu'au cas par cas chez les appelants.
    if pd.api.types.is_bool_dtype(s):
        return s.dropna().astype("float64")
    if pd.api.types.is_numeric_dtype(s):
        return s.dropna()
    nn = s.dropna()
    if nn.empty:
        return pd.to_numeric(nn, errors="coerce")

    direct = pd.to_numeric(nn, errors="coerce")
    if bool(direct.notna().all()):
        return direct

    brut = (nn.astype(str)
              .str.replace(_BRUIT_NUMERIQUE, "", regex=True)
              .str.replace(",", ".", regex=False))
    return pd.to_numeric(brut, errors="coerce")


def part_convertible(s: pd.Series, type_: str,
                     format_date: Optional[str] = None) -> Tuple[float, List[Any]]:
    """Part des valeurs non nulles qui tiennent le type, et jusqu'à 3 exemples
    de celles qui ne le tiennent pas.

    On mesure sur les non-nuls : la nullité est un contrôle séparé, et les
    confondre produit un diagnostic qui dit « mauvais type » là où le vrai
    défaut est « trop de vides ».
    """
    nn = s.dropna()
    if nn.empty or type_ in ("string", "category"):
        return 1.0, []

    if type_ in ("integer", "number"):
        conv = _numerique(nn)
        ok = conv.notna()
        if type_ == "integer":
            ok = ok & (np.mod(conv.fillna(0.5), 1) == 0)
    elif type_ == "boolean":
        if pd.api.types.is_bool_dtype(nn):
            ok = pd.Series(True, index=nn.index)
        else:
            ok = _texte(nn).str.lower().isin(VRAI | FAUX)
    elif type_ == "datetime":
        if pd.api.types.is_datetime64_any_dtype(nn):
            ok = pd.Series(True, index=nn.index)
        else:
            ok = pd.to_datetime(_texte(nn), errors="coerce",
                                format=format_date).notna()
    else:  # pragma: no cover - `type` est validé à la construction
        return 1.0, []

    ok = ok.reindex(nn.index, fill_value=False)
    mauvais = nn[~ok]
    return float(ok.mean()), [str(v) for v in mauvais.head(3)]


def convertir(s: pd.Series, type_: str,
              format_date: Optional[str] = None) -> pd.Series:
    """Applique le type déclaré, en laissant NaN ce qui ne tient pas.

    Sert à produire la sortie typée : le typage est le livrable, pas un effet
    de bord. Les valeurs non convertibles deviennent nulles — et c'est alors le
    contrôle de nullité qui les signale, pas un silence.
    """
    if type_ == "number":
        return pd.to_numeric(_numerique(s).reindex(s.index), errors="coerce")
    if type_ == "integer":
        return (pd.to_numeric(_numerique(s).reindex(s.index), errors="coerce")
                .astype("Int64"))
    if type_ == "boolean":
        bas = s.where(s.notna()).astype("object").map(
            lambda v: str(v).strip().lower() if v is not None and v == v else None)
        return bas.map(
            lambda v: True if v in VRAI else (False if v in FAUX else pd.NA)
        ).astype("boolean")
    if type_ == "datetime":
        return pd.to_datetime(s, errors="coerce", format=format_date)
    if type_ == "category":
        return s.astype("object").astype("category")
    return s.astype("object")


# --------------------------------------------------------------------------- #
# Vérification
# --------------------------------------------------------------------------- #
def _est_marqueur_vide(v: Any) -> bool:
    """Un marqueur de vide isolé. Gardé pour les tests et les cas unitaires ;
    les parcours de colonne passent par `_marqueurs`, qui est vectorisé."""
    return isinstance(v, str) and v.strip().lower() in MARQUEURS_VIDES


def _texte_normalise(s: pd.Series) -> Optional[pd.Series]:
    """La colonne en texte détouré et minuscule, ou `None` si c'est inutile.

    Calculée **une seule fois par colonne** et repassée aux contrôles qui en ont
    besoin. Chaque contrôle la recalculait pour son compte : sur un fichier de
    500 000 lignes, la vérification y passait deux secondes à refaire trois fois
    la même chose.

    `None` pour une colonne numérique ou temporelle : aucune de ses valeurs ne
    peut être un marqueur textuel ni entrer en collision de casse, et la
    convertir en texte pour s'en assurer coûte plus cher que tout le reste.
    """
    if (pd.api.types.is_numeric_dtype(s) or pd.api.types.is_bool_dtype(s)
            or pd.api.types.is_datetime64_any_dtype(s)):
        return None
    return s.dropna().astype(str).str.strip()



def _marqueurs(texte: Optional[pd.Series]) -> Optional[pd.Series]:
    """Masque booléen des cellules qui portent un marqueur de vide.

    `isin` sur une série plutôt que `map` d'une fonction Python : le second
    faisait un appel par cellule, soit un demi-million d'appels sur une seule
    colonne d'un fichier de taille ordinaire.
    """
    if texte is None or texte.empty:
        return None
    return est_vide(texte)


def verify(df: pd.DataFrame, contrat: Contrat,
           origine: Optional[pd.DataFrame] = None) -> Rapport:
    """Vérifie le contrat contre `df`, et contre `origine` pour la perte
    d'information.

    Aucun contrôle ici ne consulte le profileur : chacun est une mesure sur le
    DataFrame. C'est la condition pour que le rapport puisse contredire le
    nettoyeur — et donc pour qu'il serve à quelque chose.
    """
    r = Rapport(lignes=len(df), colonnes=len(df.columns))
    n = max(1, len(df))
    attendues = contrat.par_nom()

    for nom, cc in attendues.items():
        if nom not in df.columns:
            r.violations.append(Violation(
                "colonne_absente", nom,
                f"la colonne « {nom} » est absente du résultat"))
            continue

        s = df[nom]
        # Une seule conversion en texte pour toute la colonne, partagée par les
        # contrôles de marqueurs, de domaine et de casse.
        texte_nn = _texte_normalise(s)

        # 1. Le type tient-il ?
        part, exemples = part_convertible(s, cc.type, cc.format_date)
        if part < 1.0:
            r.violations.append(Violation(
                "type_inconvertible", nom,
                f"« {nom} » : {(1 - part) * 100:.1f}% des valeurs non nulles ne "
                f"tiennent pas le type {cc.type}",
                mesure=round(1 - part, 4), exemples=exemples))
        else:
            r.satisfait.append(f"« {nom} » tient entièrement le type {cc.type}")

        # 2. Un marqueur de vide assis dans une cellule non nulle. C'est le
        #    défaut qui passe tous les contrôles de complétude : la cellule est
        #    « remplie », mais avec un mot qui veut dire vide.
        masque = _marqueurs(texte_nn)
        if masque is not None and bool(masque.any()):
            taux = float(masque.sum()) / n
            r.violations.append(Violation(
                "marqueur_vide_en_valeur", nom,
                f"« {nom} » : {int(masque.sum())} cellules ({taux * 100:.1f}%) "
                f"contiennent un marqueur de vide traité comme une valeur",
                mesure=round(taux, 4),
                exemples=sorted(set(texte_nn[masque].head(50)))[:3]))

        # 3. Nullité
        taux_nul = float(s.isna().sum()) / n
        if taux_nul > cc.taux_nul_max + 1e-9:
            r.violations.append(Violation(
                "taux_nul_depasse", nom,
                f"« {nom} » : {taux_nul * 100:.1f}% de valeurs nulles, "
                f"plafond déclaré {cc.taux_nul_max * 100:.1f}%",
                mesure=round(taux_nul, 4)))
        elif cc.taux_nul_max == 0.0:
            r.satisfait.append(f"« {nom} » n'a aucune valeur nulle")

        # 4. Unicité de colonne
        if cc.unique:
            nn2 = s.dropna()
            doublons = int(len(nn2) - nn2.nunique())
            if doublons:
                r.violations.append(Violation(
                    "cle_non_unique", nom,
                    f"« {nom} » est déclarée unique mais porte "
                    f"{doublons} doublon(s)",
                    mesure=float(doublons),
                    exemples=[str(v) for v in
                              nn2[nn2.duplicated()].drop_duplicates().head(3)]))
            else:
                r.satisfait.append(f"« {nom} » est unique")

        # 5. Domaine
        if cc.domaine is not None:
            permis = {str(v) for v in cc.domaine}
            vus = texte_nn if texte_nn is not None else _texte(s)
            dehors = vus[~vus.isin(permis)]
            if len(dehors):
                r.violations.append(Violation(
                    "hors_domaine", nom,
                    f"« {nom} » : {len(dehors)} valeur(s) hors du domaine déclaré",
                    mesure=float(len(dehors)),
                    exemples=sorted(set(dehors.astype(str)))[:3]))
            else:
                r.satisfait.append(f"« {nom} » reste dans son domaine déclaré")

        # 5 bis. Forme — à quoi la valeur doit ressembler.
        #
        # Séparé du domaine exprès : un domaine énumère les valeurs permises,
        # une forme décrit celles qu'on ne peut pas énumérer. On ne liste pas
        # les adresses IP valides.
        if cc.forme is not None:
            motif = re.compile(FORMES.get(cc.forme, cc.forme))
            vus = texte_nn if texte_nn is not None else _texte(s).dropna()
            mauvaises = vus[~vus.astype(str).str.match(motif)]
            if len(mauvaises):
                r.violations.append(Violation(
                    "hors_forme", nom,
                    f"« {nom} » : {len(mauvaises)} valeur(s) ne respectent pas "
                    f"la forme « {cc.forme} »",
                    mesure=float(len(mauvaises)),
                    exemples=sorted(set(mauvaises.astype(str)))[:3]))
            else:
                r.satisfait.append(f"« {nom} » respecte la forme « {cc.forme} »")

        # 6. Plage
        if cc.minimum is not None or cc.maximum is not None:
            v = _numerique(s).dropna()
            bas = int((v < cc.minimum).sum()) if cc.minimum is not None else 0
            haut = int((v > cc.maximum).sum()) if cc.maximum is not None else 0
            if bas or haut:
                r.violations.append(Violation(
                    "hors_plage", nom,
                    f"« {nom} » : {bas} valeur(s) sous {cc.minimum}, "
                    f"{haut} au-dessus de {cc.maximum}",
                    mesure=float(bas + haut)))
            else:
                r.satisfait.append(
                    f"« {nom} » reste dans [{cc.minimum}, {cc.maximum}]")

        # 7. Collision de casse — deux modalités qui ne diffèrent que par la
        #    casse ou les espaces sont une seule modalité comptée deux fois.
        #    Aucun contrôle du profileur ne le voit, et c'est ce qui laissait
        #    passer `Rabat` / `rabat`.
        if cc.type in ("string", "category") and not cc.domaine                 and texte_nn is not None:
            vals = texte_nn.unique()
            if 1 < len(vals) <= 10000:
                groupes: Dict[str, set] = {}
                for v in vals:
                    groupes.setdefault(v.lower(), set()).add(v)
                collisions = [sorted(g) for g in groupes.values() if len(g) > 1]
                if collisions:
                    r.violations.append(Violation(
                        "collision_de_casse", nom,
                        f"« {nom} » : {len(collisions)} modalité(s) dédoublée(s) "
                        f"par la casse ou les espaces",
                        mesure=float(len(collisions)),
                        exemples=[" / ".join(g) for g in collisions[:3]]))

    if contrat.colonnes_inattendues_interdites:
        for nom in df.columns:
            if nom not in attendues:
                r.violations.append(Violation(
                    "colonne_inattendue", nom, f"« {nom} » n'est pas au contrat"))

    # 8. Clé primaire, éventuellement composite
    if contrat.cle_primaire:
        manquantes = [c for c in contrat.cle_primaire if c not in df.columns]
        if manquantes:
            r.violations.append(Violation(
                "colonne_absente", ", ".join(manquantes),
                f"la clé primaire exige {manquantes}, absente(s) du résultat"))
        else:
            cle = df[contrat.cle_primaire]
            doublons = int(cle.duplicated().sum())
            nuls = int(cle.isna().any(axis=1).sum())
            if doublons or nuls:
                detail = []
                if doublons:
                    detail.append(f"{doublons} ligne(s) en doublon de clé")
                if nuls:
                    detail.append(f"{nuls} ligne(s) à clé nulle")
                r.violations.append(Violation(
                    "cle_non_unique", "+".join(contrat.cle_primaire),
                    "clé primaire non valide : " + ", ".join(detail),
                    mesure=float(doublons + nuls)))
            else:
                r.satisfait.append(
                    f"la clé {'+'.join(contrat.cle_primaire)} identifie chaque ligne")

    # 9. Doublons de lignes entières
    if contrat.sans_doublons and len(df):
        d = int(df.duplicated().sum())
        if d:
            r.violations.append(Violation(
                "doublons_lignes", None,
                f"{d} ligne(s) entièrement dupliquée(s)", mesure=float(d)))
        else:
            r.satisfait.append("aucune ligne entièrement dupliquée")

    if origine is not None:
        _perte_information(df, origine, contrat, r)

    r.score = noter(r)
    return r


def _perte_information(df: pd.DataFrame, origine: pd.DataFrame,
                       contrat: Contrat, r: Rapport) -> None:
    """Ce que le nettoyage a coûté — le contrôle que l'ancien score n'avait pas.

    Trois mesures, toutes des comparaisons avant/après :

    - **lignes perdues**, hors doublons francs (les retirer n'est pas une perte) ;
    - **taux de remplissage** par colonne : une colonne dont on a comblé un
      tiers des vides n'est plus mesurée, elle est en partie fabriquée ;
    - **masse ponctuelle** : le remplissage s'est-il concentré sur **une seule**
      valeur ? Combler 33% d'une variable en répartissant sur sa distribution et
      la combler en écrasant tout sur la médiane sont deux gestes différents, et
      seul le second détruit la variance.

    Deux pièges, trouvés en contre-épreuve sur une sortie correcte que ces
    contrôles accusaient à tort :

    - **comparer des comptes entre deux tailles.** Dédupliquer retire des lignes,
      dont certaines portaient des vides : le compte de nuls baisse sans qu'on
      ait imputé quoi que ce soit. On compare donc des **taux**.
    - **prendre une normalisation pour une imputation.** Ramener `"  Casablanca "`
      à `"Casablanca"` concentre 105 lignes sur une étiquette — c'est le résultat
      voulu, pas une masse ponctuelle. Une imputation se reconnaît à la **chute
      du taux de nuls** ; un renommage le laisse intact. Le contrôle de masse ne
      s'arme donc que là où des vides ont effectivement été comblés.
    """
    n_av, n_ap = len(origine), len(df)
    francs = int(origine.duplicated().sum())
    perdues = max(0, n_av - francs - n_ap)
    if n_av and perdues / n_av > contrat.lignes_perdues_max + 1e-9:
        r.violations.append(Violation(
            "lignes_perdues", None,
            f"{perdues} ligne(s) perdues hors doublons "
            f"({perdues / n_av * 100:.1f}%), budget "
            f"{contrat.lignes_perdues_max * 100:.1f}%",
            mesure=round(perdues / n_av, 4)))

    par_nom = contrat.par_nom()
    for nom in origine.columns:
        if nom not in df.columns:
            r.violations.append(Violation(
                "colonne_supprimee", nom,
                f"« {nom} » existait en entrée et a disparu du résultat"))
            continue
        if not n_ap:
            continue

        cc = par_nom.get(nom)
        budget = (cc.imputation_max if cc and cc.imputation_max is not None
                  else contrat.imputation_max)

        # Taux et non comptes : voir le premier piège du docstring.
        comble = max(0.0, float(origine[nom].isna().mean())
                     - float(df[nom].isna().mean()))
        if comble > budget + 1e-9:
            r.violations.append(Violation(
                "imputation_excessive", nom,
                f"« {nom} » : {int(round(comble * n_ap))} valeurs imputées "
                f"({comble * 100:.1f}%), budget {budget * 100:.1f}% — "
                f"au-delà, la variable est fabriquée plutôt que mesurée",
                mesure=round(comble, 4)))

        # Masse ponctuelle : seulement là où des vides ont été comblés, et
        # plafonnée par ce qui a été comblé — au-delà, le gain vient d'un
        # renommage, qui n'est pas une perte d'information.
        if comble <= 1e-9:
            continue
        try:
            av = origine[nom].dropna().astype(str).value_counts()
            ap = df[nom].dropna().astype(str).value_counts()
        except Exception:
            continue
        gains = ((ap - av.reindex(ap.index, fill_value=0)) / n_ap).clip(upper=comble)
        pics = gains[gains > budget + 1e-9].sort_values(ascending=False)
        if len(pics):
            r.violations.append(Violation(
                "masse_ponctuelle", nom,
                f"« {nom} » : le remplissage s'est concentré sur la seule valeur "
                f"« {pics.index[0]} », qui gagne {pics.iloc[0] * 100:.1f}% des "
                f"lignes — distribution écrasée sur un point",
                mesure=round(float(pics.iloc[0]), 4),
                exemples=[str(x) for x in pics.index[:3]]))


def noter(r: Rapport) -> int:
    """0–100, plafonné dès qu'une violation bloquante existe.

    Le plafond est l'essentiel : sans lui, un score agrégé finit toujours par
    noyer un défaut rédhibitoire sous une majorité de contrôles réussis, ce qui
    est précisément comment on obtient 100/100 sur des données inutilisables.
    """
    penalite = sum(POIDS.get(v.code, 5) for v in r.violations)
    score = max(0, 100 - penalite)
    if r.bloquantes:
        score = min(score, PLAFOND_BLOQUANT)
    return int(score)
