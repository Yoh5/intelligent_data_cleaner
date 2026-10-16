"""Proposer un contrat à partir du profil — et de l'usage prévu des données.

## Pourquoi c'est un module séparé

`contract.py` ne doit rien savoir du profileur : c'est ce qui permet à une
vérification de **contredire** le nettoyeur. L'inférence, elle, a besoin du
profil. Les deux responsabilités vivent donc dans deux fichiers, et la
dépendance ne va que dans un sens : `contract_infer` → `contract`.

## Pourquoi l'usage change le contrat

« Prêtes à être utilisées » ne veut rien dire tout seul. Un data engineer ne
nettoie jamais dans l'abstrait : il nettoie **vers** quelque chose, et la cible
décide de ce qui est grave.

- **entraîner un modèle** — le typage est tout, et l'imputation est un poison :
  combler 30 % d'une variable par sa médiane apprend au modèle une régularité
  qu'on a fabriquée. Budget d'imputation serré.
- **charger en base** — la clé primaire et les types sont rédhibitoires, et on
  n'invente **aucune** valeur : une donnée imputée dans un système de référence
  devient indiscernable d'une donnée observée. Budget d'imputation nul.
- **tableau de bord** — les agrégats se font par modalité : deux modalités
  dédoublées par la casse font deux barres au lieu d'une. Les vides se tolèrent.
- **joindre à une autre table** — seule la clé compte, unique et non nulle ;
  une clé en doublon multiplie silencieusement les lignes de la jointure.

## La tension que l'inférence rend explicite

Une colonne vide à 33 % ne peut pas être à la fois « non nullable » et respecter
un budget d'imputation de 20 % : il faudrait fabriquer un tiers de la colonne.
Plutôt que de choisir en silence, l'inférence déclare la colonne **nullable** et
le dit dans `notes`. C'est le genre d'arbitrage qu'un contrat doit porter au
jour, parce que c'est celui qu'un score agrégé cache.

## Ce que l'inférence ne fait pas

Elle ne propose pas de domaine de valeurs par défaut. Un domaine déduit des
données observées est satisfait par construction : il ne contrôle rien sur le
fichier qui l'a produit, et ne sert qu'aux chargements suivants. On ne le
propose donc que sur demande explicite (`proposer_domaines=True`).
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import re

import numpy as np
import pandas as pd

from app.services.contract import (FORMES, MARQUEURS_VIDES, Contrat,
                                   ContratColonne, _numerique, est_vide,
                                   part_convertible)

#: Les quatre usages, et ce que chacun rend rédhibitoire.
USAGES: Dict[str, Dict[str, Any]] = {
    "modele": {
        "libelle": "entraîner un modèle",
        "imputation_max": 0.10,
        "lignes_perdues_max": 0.05,
        "exige_cle": False,
    },
    "base": {
        "libelle": "charger en base de données",
        "imputation_max": 0.0,
        "lignes_perdues_max": 0.0,
        "exige_cle": True,
    },
    "tableau": {
        "libelle": "alimenter un tableau de bord",
        "imputation_max": 0.20,
        "lignes_perdues_max": 0.10,
        "exige_cle": False,
    },
    "jointure": {
        "libelle": "joindre à une autre table",
        "imputation_max": 0.05,
        "lignes_perdues_max": 0.0,
        "exige_cle": True,
    },
}
USAGE_DEFAUT = "modele"

#: Type sémantique du profileur -> type de contrat.
TYPE_DEPUIS_SEMANTIQUE = {
    "numeric": "number",
    "numeric-mixed": "number",
    "datetime": "datetime",
    "datetime-mixed": "datetime",
    "categorical": "category",
    "id": "string",
    "text": "string",
}

#: Indices lexicaux d'identifiant, pour départager deux colonnes également
#: uniques. Le profileur a la même liste mais ne l'atteint jamais pour une clé
#: entière : `_infer_semantic_type` teste `is_numeric_dtype` d'abord, donc une
#: clé primaire en entiers est classée `numeric` et le contrôle de doublons
#: d'identifiant du profileur ne s'exécute pas. On ne s'appuie donc pas sur
#: `semantic_type == "id"` ici.
MOTS_CLE = ("id", "code", "ref", "num", "matricule", "uuid", "cle", "key")


class _Colonne:
    """Les formes d'une colonne dont l'inférence a besoin, calculées une fois.

    Cinq fonctions avaient besoin de la même normalisation — passer la colonne
    en texte, la détourer, la mettre en minuscule — et chacune la refaisait pour
    son compte. Sur 500 000 lignes, l'inférence passait ainsi huit secondes,
    soit la moitié du pipeline, à recalculer quatre fois la même chose.

    `texte` vaut `None` pour une colonne numérique, booléenne ou temporelle :
    aucune de ses valeurs ne peut être un marqueur de vide textuel, et la
    convertir pour s'en assurer coûterait plus que tout le reste.
    """

    __slots__ = ("s", "texte", "vide", "propre", "n", "_chiffres")

    def __init__(self, s: pd.Series) -> None:
        self.s = s
        self.n = len(s)
        if (pd.api.types.is_numeric_dtype(s) or pd.api.types.is_bool_dtype(s)
                or pd.api.types.is_datetime64_any_dtype(s)):
            self.texte = None
            self.vide = s.isna()
        else:
            self.texte = s.astype(str)
            self.vide = s.isna() | est_vide(self.texte)
        #: La colonne débarrassée de ses marqueurs, valeurs non nulles seulement.
        self.propre = s.where(~self.vide).dropna()
        self._chiffres: Optional[pd.Series] = None

    @property
    def taux_vide(self) -> float:
        return float(self.vide.mean()) if self.n else 0.0

    @property
    def chiffres(self) -> pd.Series:
        """La colonne lue comme des nombres, calculée une fois et retenue.

        Trois contrôles en avaient besoin — la détection d'entiers, la
        tenabilité du type, la re-déduction — et chacun relançait la conversion.
        À elle seule, la détection d'entiers reprenait 3,9 secondes sur les 6,5
        de l'inférence, en refaisant des remplacements de chaînes que
        `_numerique` sait déjà éviter.
        """
        if self._chiffres is None:
            self._chiffres = _numerique(self.propre)
        return self._chiffres

    def marqueurs_vus(self, combien: int = 3) -> List[str]:
        if self.texte is None:
            return []
        reperes = self.s[self.vide & self.s.notna()]
        return sorted({str(v).strip() for v in reperes.head(200)})[:combien]


def _entiers(s: pd.Series) -> bool:
    """La colonne ne porte-elle que des entiers (au format près) ?"""
    v = _numerique(s).dropna()
    return bool(len(v)) and bool((v % 1 == 0).all())


def sans_marqueurs(s: pd.Series) -> pd.Series:
    """La colonne avec ses marqueurs de vide ramenés à NaN."""
    if pd.api.types.is_numeric_dtype(s) or pd.api.types.is_datetime64_any_dtype(s):
        return s
    texte = s.astype(str).str.strip().str.lower()
    return s.where(s.notna() & ~texte.isin(MARQUEURS_VIDES))


#: Un zéro initial suivi d'un autre chiffre. `0` et `0.5` sont des nombres ;
#: `01000` et `007` sont des codes.
_ZERO_INITIAL = re.compile(r"^0\d")


def est_un_code(propre: pd.Series) -> Tuple[bool, List[str]]:
    """Des zéros initiaux : c'est un code, pas une quantité.

    **Le piège le plus coûteux du métier, et il est entièrement silencieux.**
    `01000` se convertit en entier sans lever la moindre erreur et devient
    `1000`. Aucun contrôle de type ne le voit — la colonne tient parfaitement
    son type déclaré — et toute jointure sur ce code échoue ensuite sans que
    personne sache pourquoi.

    Un corpus de pathologies l'a trouvé en vingt millisecondes sur le premier
    fichier : codes postaux, identifiants de recensement, matricules, codes
    produits. Le défi Bias Bounty prévient explicitement du cas pour les GEOID,
    dont le FIPS des États commence parfois par zéro — l'Arizona est `04`.

    La règle est étroite exprès : un zéro **suivi d'un autre chiffre**. Elle ne
    déclasse donc ni `0`, ni `0.5`, ni `-0.3`, et elle n'a pas besoin que la
    colonne soit de largeur fixe — `7` et `007` dans la même colonne sont le
    cas le plus dangereux, puisque la conversion les confond.
    """
    if (pd.api.types.is_numeric_dtype(propre)
            or pd.api.types.is_bool_dtype(propre)
            or pd.api.types.is_datetime64_any_dtype(propre)
            or propre.empty):
        return False, []
    texte = propre.astype(str).str.strip()
    masque = texte.str.match(_ZERO_INITIAL)
    if not bool(masque.any()):
        return False, []
    return True, sorted(set(texte[masque].head(50)))[:3]


#: Un nombre entre parenthèses : la convention comptable pour un négatif.
_NEGATIF_COMPTABLE = re.compile(r"^\(\s*[\d\s.,]+\s*\)$")


def negatifs_comptables(propre: pd.Series) -> Tuple[int, List[str]]:
    """Combien de valeurs écrivent un négatif à la mode comptable.

    `(1200)` vaut -1200 dans un export comptable, et vaut 1200 pour quiconque
    retire les parenthèses sans y penser. Un débit devient alors un crédit, le
    total change de signe, et rien ne lève.

    On se contente de **compter et de nommer** : convertir demanderait de
    décider que ce fichier suit bien cette convention, et un identifiant comme
    `(A12)` ou une annotation entre parenthèses ressemblent à s'y méprendre.
    C'est à l'utilisateur de trancher, et il ne peut le faire que si on le lui
    dit.
    """
    if (pd.api.types.is_numeric_dtype(propre)
            or pd.api.types.is_bool_dtype(propre) or propre.empty):
        return 0, []
    texte = propre.astype(str).str.strip()
    masque = texte.str.match(_NEGATIF_COMPTABLE)
    if not bool(masque.any()):
        return 0, []
    return int(masque.sum()), sorted(set(texte[masque].head(50)))[:3]


def type_tenable(s: pd.Series, propose: str) -> Tuple[str, List[str]]:
    """Le type proposé tient-il sur **toutes** les valeurs, ou faut-il reculer ?

    `s` est la colonne **déjà débarrassée de ses marqueurs de vide** (voir
    `_Colonne.propre`).

    Le profileur classe `numeric-mixed` une colonne dont 70 % des valeurs se
    convertissent, et le contrat traduisait ça en type numérique. Sur `StockCode`
    du jeu Online Retail — 2 684 références numériques et **424 codes
    alphanumériques** comme `15044B` — la conversion mettait donc 424 lignes à
    vide, soit 13,6 % de la colonne, et le contrat se trouvait violé par sa
    propre réparation.

    Le fond du problème : *mixte* ne veut pas dire *numérique à 86 %*, ça veut
    dire que la colonne porte des **codes**, et un code n'est pas une quantité.
    Le convertir ne nettoie rien, ça détruit l'information qui dépasse.

    Règle retenue : un type numérique, temporel ou booléen n'est déclaré que si
    la colonne le tient **entièrement**. Sinon on recule sur `string` et on dit
    pourquoi, avec des exemples — c'est à l'utilisateur de trancher s'il veut
    vraiment jeter ces lignes.
    """
    if propose in ("string", "category"):
        return propose, []
    nn = s.dropna()
    if nn.empty:
        return propose, []
    part, exemples = part_convertible(nn, propose)
    if part >= 1.0:
        return propose, []
    return "string", exemples


def _entiers_prepares(prep: "_Colonne") -> bool:
    """`_entiers`, mais sur une colonne déjà préparée."""
    v = prep.chiffres.dropna()
    return bool(len(v)) and bool((v % 1 == 0).all())


def type_apres_nettoyage(s: pd.Series, defaut: str,
                         chiffres: Optional[pd.Series] = None) -> str:
    """Re-déduit le type sur la colonne **déjà** débarrassée de ses marqueurs.

    Même raison que `taux_vide_reel`, et même conséquence si on l'omet :
    `_infer_semantic_type` voyait « inconnu » comme une modalité, classait `age`
    en `text`, et le contrat déclarait donc une colonne d'âges en `string`. La
    sortie restait du texte, et le typage — qui est le livrable — manquait sa
    cible sur la colonne qui en avait le plus besoin.

    On ne remplace le type que si la colonne nettoyée tient **entièrement** un
    type plus précis. Sans ce « entièrement », on requalifierait une colonne de
    commentaires contenant trois nombres.

    **Une seule conversion numérique**, réutilisée pour les deux candidats
    chiffrés : `integer` est inclus dans `number`, donc les tester séparément
    refaisait deux fois le travail le plus cher de l'inférence. Et si la colonne
    ne tient pas `number`, elle ne peut pas tenir `integer` — on saute les deux.
    """
    nn = s.dropna()
    if len(nn) < 3:
        return defaut

    code, _ = est_un_code(nn)
    if code:
        return defaut

    # Le booléen AVANT le chiffre : `_numerique` le convertit maintenant en
    # flottant, donc `True`/`False` passeraient pour des entiers et une colonne
    # de drapeaux serait déclarée `integer`. Elle tiendrait le type, mais le
    # contrat mentirait sur ce que la colonne est — et le livrable qu'on vend
    # est précisément la justesse des types.
    part_bool, _ = part_convertible(nn, "boolean")
    if part_bool >= 1.0:
        return "boolean"

    if chiffres is None:
        chiffres = _numerique(nn)
    if len(chiffres) == len(nn) and bool(chiffres.notna().all()):
        entiers = bool((np.mod(chiffres, 1) == 0).all())
        return "integer" if entiers else "number"

    for candidat in ("datetime",):
        part, _ = part_convertible(nn, candidat)
        if part >= 1.0:
            return candidat
    return defaut


#: Une forme se décide sur **deux** mesures, pas sur un seuil unique.
#:
#: Un seuil seul oblige à trancher arbitrairement : à 75 % d'adresses IPv4,
#: faut-il déclarer la forme ? La question n'a pas de bonne réponse formulée
#: ainsi, parce qu'elle ignore ce que sont les 25 % restants.
#:
#: « Trois quarts d'adresses IPv4, et le reste est deux jetons répétés » —
#: c'est une colonne d'adresses avec des sentinelles, et rien d'autre ne
#: l'explique. « Trois quarts d'adresses IPv4, et le reste est deux cents
#: chaînes différentes » — ce n'est pas une colonne d'adresses, c'est une
#: colonne qui en contient parfois.
#:
#: D'où les deux conditions, toutes deux mesurées et toutes deux citées dans
#: la note : une part suffisante, ET un reste qui se compte sur les doigts.
SEUIL_FORME = 0.70

#: Au-delà, le reste n'est plus un jeu de sentinelles mais du bruit, et une
#: forme déclarée viderait des valeurs qu'on ne comprend pas.
RESTE_MAX_DISTINCT = 3

#: Un plancher de lignes, pour qu'un hasard sur cinq valeurs ne fonde pas une
#: contrainte permanente.
FORME_LIGNES_MIN = 10


def forme_tenable(propre: pd.Series) -> Optional[Tuple[str, float, List[str]]]:
    """La forme que la colonne respecte, s'il y en a une défendable.

    Rend `(nom, taux, reste)` — le taux et les valeurs restantes sont rendus
    pour que la note puisse les citer : une contrainte proposée sans son appui
    est une contrainte qu'on ne peut pas contester.
    """
    nn = propre.dropna().astype(str).str.strip()
    nn = nn[nn != ""]
    if len(nn) < FORME_LIGNES_MIN:
        return None

    meilleure: Optional[Tuple[str, float, List[str]]] = None
    for nom, motif in FORMES.items():
        correspond = nn.str.match(motif)
        taux = float(correspond.mean())
        if taux < SEUIL_FORME:
            continue
        reste = sorted(set(nn[~correspond]))
        if len(reste) > RESTE_MAX_DISTINCT:
            continue
        if meilleure is None or taux > meilleure[1]:
            meilleure = (nom, taux, reste[:3])
    return meilleure


def taux_vide_reel(s: pd.Series) -> float:
    """Taux de vide **mesuré**, marqueurs textuels compris.

    Le profileur rend `missing_rate` à partir de ce que son chargeur a converti
    en NaN, et sa liste `MISSING_PATTERNS` connaît `Unknown` mais pas `inconnu`.
    Inférer le contrat depuis ce taux revient à bâtir la cible sur l'angle mort
    du détecteur : sur la sonde, `age` était annoncée vide à 30% alors qu'elle
    l'est à 61,8%, et le contrat déclarait donc un plafond que la donnée propre
    ne pouvait pas tenir.

    Un contrat ne peut pas hériter des oublis de l'outil qu'il est censé
    arbitrer. On remesure.
    """
    n = len(s)
    if not n:
        return 0.0
    vide = s.isna()
    if not pd.api.types.is_numeric_dtype(s) and not \
            pd.api.types.is_datetime64_any_dtype(s):
        texte = s.astype(str).str.strip().str.lower()
        vide = vide | texte.isin(MARQUEURS_VIDES)
    return float(vide.mean())


#: Multiplicateur de l'écart interquartile. 3 et non 1,5 : la règle de Tukey à
#: 1,5 signale environ 0,7 % d'une loi normale, ce qui sur 500 000 lignes fait
#: 3 500 « aberrantes » parfaitement normales. À 3, on ne retient que ce qui est
#: vraiment loin. C'est aussi le seuil que `DataProfiler` emploie déjà, et deux
#: seuils différents dans le même produit donneraient deux comptes différents
#: pour la même colonne.
IQR_FACTEUR = 3.0


def plage_plausible(chiffres: pd.Series) -> Optional[Tuple[float, float]]:
    """L'intervalle interquartile élargi, ou `None` si la colonne ne s'y prête.

    Mesure **robuste** exprès : moyenne et écart-type sont eux-mêmes déplacés
    par les valeurs extrêmes qu'on cherche, donc une règle à trois sigmas rate
    précisément les cas qui comptent. Les quartiles, eux, ne bougent pas.

    `None` quand l'écart interquartile est nul — colonne constante ou quasi —
    parce qu'alors toute valeur différente de la médiane serait déclarée
    aberrante, ce qui est un verdict et non une mesure.
    """
    # Un drapeau n'a pas de valeur extrême : ses seules valeurs sont ses deux
    # modalités. Le dire ici plutôt que de compter sur l'appelant, parce que
    # cette fonction est appelable seule et que `q3 - q1` sur des booléens lève.
    if pd.api.types.is_bool_dtype(chiffres):
        return None
    v = chiffres.dropna()
    if len(v) < 10:
        return None
    q1, q3 = float(v.quantile(0.25)), float(v.quantile(0.75))
    iqr = q3 - q1
    if iqr <= 0:
        return None
    return q1 - IQR_FACTEUR * iqr, q3 + IQR_FACTEUR * iqr


def compter_aberrantes(chiffres: pd.Series,
                       plage: Tuple[float, float]) -> Tuple[int, List[str]]:
    """Combien de valeurs sortent de la plage, et trois d'entre elles."""
    v = chiffres.dropna()
    dehors = v[(v < plage[0]) | (v > plage[1])]
    extremes = dehors.reindex(dehors.abs().sort_values(ascending=False).index)
    return len(dehors), [f"{x:g}" for x in extremes.head(3)]


def candidats_cle(df: pd.DataFrame,
                  deja_dedupe: Optional[pd.DataFrame] = None) -> List[str]:
    """Colonnes qui identifient chaque ligne, **doublons francs mis de côté**.

    Une ligne entièrement dupliquée n'est pas un défaut de clé, c'est une tâche
    de déduplication. Mesurer l'unicité sur les données brutes ferait donc
    rejeter la vraie clé du jeu — et c'est exactement ce qui se passait : sur la
    sonde, `id` portait 30 doublons issus de 30 lignes recopiées.

    `deja_dedupe` évite de refaire la déduplication quand l'appelant l'a déjà :
    c'est l'opération la plus chère de l'inférence et elle se faisait deux fois.
    """
    d = deja_dedupe if deja_dedupe is not None else df.drop_duplicates()
    trouves = []
    for col in d.columns:
        s = d[col]
        if s.isna().any():
            continue
        # Une clé ne peut pas porter un marqueur de vide. Sans ce filtre, une
        # colonne faite de « inconnu » répétés devient unique après
        # déduplication — les répétitions disparaissent avec les lignes — et se
        # fait sacrer clé primaire.
        if _Colonne(s).taux_vide > 0:
            continue
        if s.nunique(dropna=False) == len(d) and len(d) > 1:
            trouves.append(col)
    trouves.sort(key=lambda c: (
        not any(m in c.lower() for m in MOTS_CLE), list(d.columns).index(c)))
    return trouves


def inferer(analysis: Dict[str, Any], df: pd.DataFrame,
            usage: str = USAGE_DEFAUT,
            proposer_domaines: bool = False,
            proposer_plages: bool = False) -> Tuple[Contrat, List[str]]:
    """Rend un contrat **proposé** et les notes qui expliquent ses arbitrages.

    Le contrat est une proposition : l'utilisateur le modifie avant de lancer le
    nettoyage. C'est la vérification qui est faisant foi, pas l'inférence.
    """
    reglage = USAGES.get(usage, USAGES[USAGE_DEFAUT])
    notes: List[str] = []
    cols_info: Dict[str, Any] = analysis.get("columns", {}) or {}
    budget = float(reglage["imputation_max"])

    # Les taux se mesurent sur la base que la réparation produira, pas sur le
    # fichier brut. Sinon le plafond de nullité est calculé avec un
    # dénominateur que la déduplication va changer : sur un jeu à 220 lignes
    # dont 20 recopiées, le taux de vides de `age` passait de 49,1% avant à
    # 51,0% après, et le contrat déclarait un plafond de 50,1% que sa propre
    # réparation violait. Un contrat qui s'auto-contredit est pire qu'absent.
    # Une seule déduplication, partagée : `candidats_cle` la refaisait pour son
    # compte, ce qui doublait le travail le plus cher de la fonction.
    sans_doublons_francs = df.drop_duplicates() if len(df) else df
    cands = candidats_cle(df, deja_dedupe=sans_doublons_francs) if len(df) else []
    base = sans_doublons_francs
    if cands:
        base = base.drop_duplicates(subset=[cands[0]]).dropna(subset=[cands[0]])
    colonnes_prep = {c: _Colonne(base[c]) for c in base.columns}
    if len(base) != len(df):
        notes.append(
            f"taux mesurés sur les {len(base)} lignes qui resteront après "
            f"déduplication, pas sur les {len(df)} du fichier : un plafond "
            f"calculé avant la réparation ne survit pas à la réparation.")
    n = max(1, len(base))

    colonnes: List[ContratColonne] = []
    for nom, info in cols_info.items():
        sem = info.get("semantic_type", "text")
        type_ = TYPE_DEPUIS_SEMANTIQUE.get(sem, "string")
        prep = colonnes_prep.get(nom)
        if type_ == "number" and prep is not None and _entiers_prepares(prep):
            type_ = "integer"
        # Les codes à zéro initial, avant tout le reste : `01000` tient
        # parfaitement le type entier, et c'est précisément le problème.
        if prep is not None and type_ in ("integer", "number"):
            code, exemples = est_un_code(prep.propre)
            if code:
                notes.append(
                    f"« {nom} » : {', '.join(repr(x) for x in exemples)} "
                    f"commencent par un zéro. C'est un code, pas une quantité — "
                    f"le convertir en nombre donnerait "
                    f"{int(str(exemples[0]).lstrip('0') or 0)} sans la moindre "
                    f"erreur, et toute jointure sur cette colonne échouerait "
                    f"ensuite en silence. Le contrat retient `string`.")
                type_ = "string"

        if prep is not None:
            combien, formes = negatifs_comptables(prep.propre)
            if combien:
                notes.append(
                    f"« {nom} » : {combien} valeur(s) de la forme "
                    f"{', '.join(repr(x) for x in formes)}. En comptabilité, "
                    f"des parenthèses signifient un négatif — retirer les "
                    f"parenthèses sans le signe changerait un débit en crédit. "
                    f"Non converti : la colonne reste du texte tant que vous "
                    f"n'avez pas confirmé la convention.")

        # Reculer avant d'avancer : un type numérique hérité de « numeric-mixed »
        # peut ne pas tenir sur toute la colonne, et le déclarer quand même
        # revient à programmer la destruction des valeurs qui dépassent.
        if prep is not None:
            tenable, hors = type_tenable(prep.propre, type_)
            if tenable != type_:
                notes.append(
                    f"« {nom} » : classée {sem} par le profileur, mais "
                    f"{', '.join(repr(x) for x in hors)} ne tient pas le type "
                    f"{type_}. Le contrat recule sur `string` — convertir "
                    f"mettrait ces valeurs à vide, ce qui détruirait de "
                    f"l'information au lieu de la nettoyer.")
                type_ = tenable

        if prep is not None and type_ in ("string", "category"):
            affine = type_apres_nettoyage(prep.propre, type_, prep.chiffres)
            if affine != type_:
                notes.append(
                    f"« {nom} » : classée {sem} par le profileur, mais une fois "
                    f"les marqueurs de vide retirés elle tient entièrement le "
                    f"type {affine}. Le contrat retient {affine} — sinon le "
                    f"typage, qui est le livrable, raterait sa cible.")
                type_ = affine

        annonce = float(info.get("missing_rate", 0.0) or 0.0)
        mesure = prep.taux_vide if prep is not None else annonce
        taux = max(annonce, mesure)
        if mesure - annonce > 0.02:
            marques = prep.marqueurs_vus()
            notes.append(
                f"« {nom} » : le profileur annonce {annonce * 100:.1f}% de vides, "
                f"la mesure large en trouve {mesure * 100:.1f}% — "
                f"marqueur(s) non reconnu(s) par le chargeur : "
                f"{', '.join(repr(m) for m in marques)}. Le contrat retient la "
                f"mesure, sinon il déclarerait un plafond intenable.")

        if taux <= budget + 1e-9:
            nullable, plafond = False, 0.0
        else:
            # On ne peut pas exiger le remplissage sans dépasser le budget :
            # on le déclare, au lieu de le trancher en silence.
            nullable, plafond = True, min(1.0, round(taux + 0.01, 4))
            notes.append(
                f"« {nom} » est vide à {taux * 100:.1f}% : la combler dépasserait "
                f"le budget d'imputation de {budget * 100:.0f}% retenu pour "
                f"« {reglage['libelle']} ». Déclarée nullable — à décider : "
                f"accepter les vides, relever le budget, ou écarter la colonne.")

        cc = ContratColonne(nom=nom, type=type_, nullable=nullable,
                            taux_nul_max=plafond)

        # La forme : à quoi les valeurs doivent ressembler. Le type dit ce
        # qu'une valeur est ; il ne dit pas que `999.1.2.3` n'est pas une
        # adresse. C'est le contrôle qui manquait, et c'est celui qui compte
        # sur un journal réseau.
        if type_ in ("string", "category") and nom in base.columns:
            trouvee = forme_tenable(base[nom])
            if trouvee is not None:
                forme, taux, reste = trouvee
                cc.forme = forme
                if reste:
                    notes.append(
                        f"« {nom} » : forme « {forme} » proposée — "
                        f"{taux * 100:.1f}% des valeurs la respectent et le "
                        f"reste tient en {len(reste)} jeton(s) répété(s) "
                        f"({', '.join(reste)}), qui deviendront des vides et "
                        f"tomberont sous le budget d'imputation plutôt que "
                        f"d'être corrigés.")
                else:
                    notes.append(
                        f"« {nom} » : forme « {forme} » proposée — toutes les "
                        f"valeurs la respectent ici, la contrainte ne protège "
                        f"donc que les chargements suivants.")

        if proposer_domaines and type_ == "category" and nom in base.columns:
            vus = base[nom].dropna().astype(str).str.strip()
            if 0 < vus.nunique() <= 12:
                cc.domaine = sorted(vus.unique())
                notes.append(
                    f"« {nom} » : domaine proposé depuis les {cc.domaine.__len__()} "
                    f"modalités observées — il est satisfait par construction sur "
                    f"ce fichier, il ne contrôle que les chargements suivants.")

        # Les valeurs extrêmes : toujours MESURÉES, déclarées seulement sur
        # demande.
        #
        # Déclarer la plage par défaut rendrait le contrat intenable par le
        # fichier dont il sort — c'est exactement l'erreur qu'on a déjà faite
        # sur le taux de vides, et elle produit un contrat qui s'auto-contredit.
        # Mais ne rien dire du tout laisserait passer en silence des valeurs
        # qu'un humain aurait voulu voir. Donc : on compte, on le dit dans les
        # arbitrages, et on ne contraint que si on nous le demande.
        if prep is not None and type_ in ("integer", "number"):
            plage = plage_plausible(prep.chiffres)
            if plage is not None:
                dehors, extremes = compter_aberrantes(prep.chiffres, plage)
                if dehors:
                    notes.append(
                        f"« {nom} » : {dehors} valeur(s) hors de l'intervalle "
                        f"plausible [{plage[0]:g}, {plage[1]:g}] "
                        f"({dehors / max(1, len(prep.chiffres)) * 100:.1f}%), "
                        f"dont {', '.join(extremes)}. Non traitées : une valeur "
                        f"extrême est tantôt une erreur de saisie, tantôt le cas "
                        f"le plus important du fichier, et la colonne seule ne "
                        f"permet pas de les distinguer.")
                if proposer_plages:
                    cc.minimum, cc.maximum = plage
                    notes.append(
                        f"« {nom} » : plage déclarée [{plage[0]:g}, "
                        f"{plage[1]:g}]. Le contrat ne tiendra donc pas tant "
                        f"que ces {dehors} valeur(s) n'auront pas été tranchées "
                        f"— c'est le but.")

        colonnes.append(cc)

    contrat = Contrat(
        colonnes=colonnes,
        lignes_perdues_max=float(reglage["lignes_perdues_max"]),
        imputation_max=budget,
        usage=usage,
    )

    if cands:
        contrat.cle_primaire = [cands[0]]
        for cc in contrat.colonnes:
            if cc.nom == cands[0]:
                cc.unique, cc.nullable, cc.taux_nul_max = True, False, 0.0
        if len(cands) > 1:
            notes.append(
                f"clé primaire proposée : « {cands[0]} » — autres candidats "
                f"également uniques : {', '.join(cands[1:4])}.")
    elif reglage["exige_cle"]:
        notes.append(
            f"aucune colonne n'identifie une ligne de façon unique, alors que "
            f"« {reglage['libelle']} » l'exige. Sans clé, une jointure ou un "
            f"chargement incrémental dupliquera des lignes sans le signaler — "
            f"il faut désigner une clé composite.")

    vides = [c.nom for c in colonnes
             if float(cols_info.get(c.nom, {}).get("missing_rate", 0)) > 0.6]
    if vides:
        notes.append(
            f"colonne(s) vides à plus de 60% : {', '.join(vides)}. Les nettoyer "
            f"n'a pas de sens — soit la variable est inexploitable, soit son "
            f"absence est elle-même l'information et mérite un indicateur.")

    notes.append(
        f"budgets pour « {reglage['libelle']} » : au plus "
        f"{budget * 100:.0f}% de cellules imputées par colonne et "
        f"{float(reglage['lignes_perdues_max']) * 100:.0f}% de lignes perdues "
        f"hors doublons francs.")
    return contrat, notes


def resume(contrat: Contrat) -> str:
    """Le contrat en texte, pour qu'un humain le relise avant de lancer."""
    libelle = USAGES.get(contrat.usage, {}).get("libelle", contrat.usage or "—")
    lignes = [f"Contrat — usage : {libelle}", ""]
    if contrat.cle_primaire:
        lignes.append(f"clé primaire : {' + '.join(contrat.cle_primaire)}")
    lignes.append(f"budget imputation : {contrat.imputation_max * 100:.0f}% par colonne")
    lignes.append(f"budget lignes perdues : {contrat.lignes_perdues_max * 100:.0f}%")
    lignes.append("")
    largeur = max((len(c.nom) for c in contrat.colonnes), default=10)
    for c in contrat.colonnes:
        marques = []
        if c.unique:
            marques.append("unique")
        if not c.nullable:
            marques.append("non nul")
        elif c.taux_nul_max < 1.0:
            marques.append(f"nuls <= {c.taux_nul_max * 100:.0f}%")
        if c.domaine:
            marques.append(f"domaine de {len(c.domaine)}")
        if c.minimum is not None or c.maximum is not None:
            marques.append(f"[{c.minimum}, {c.maximum}]")
        lignes.append(f"  {c.nom:<{largeur}}  {c.type:<9} {', '.join(marques)}")
    return "\n".join(lignes)
