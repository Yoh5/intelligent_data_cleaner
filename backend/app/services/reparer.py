"""Réparer en visant le contrat — et refuser ce qui le violerait.

## Ce qui change par rapport à `agent_cleaner`

L'ancienne boucle choisissait une stratégie par problème détecté et l'appliquait.
Elle optimisait donc le silence du profileur, et produisait des données dont le
score disait 100/100 : le mot « inconnu » imputé 247 fois, `Rabat` et `rabat`
encore distincts, un tiers d'une variable écrasé sur sa médiane.

Ici, chaque réparation vise une clause du contrat, dans un ordre qui compte, et
**le budget peut la faire renoncer**. C'est le point : un data engineer qui ne
peut pas tenir une garantie le dit, il ne remplit pas les trous pour faire bonne
figure.

## L'ordre des opérations n'est pas arbitraire

1. **Les marqueurs de vide deviennent des vides.** Avant tout le reste : tant que
   « inconnu » compte pour une modalité, il est le mode de sa colonne et toute
   imputation le propage.
2. **Espaces, puis casse.** Dans cet ordre, sinon `"  Rabat "` et `"rabat"`
   restent deux clés distinctes au moment de choisir la forme canonique.
3. **Les types déclarés.** Ce qui ne tient pas devient vide — et c'est alors le
   contrôle de nullité qui le signale, pas un silence.
4. **Déduplication**, lignes entières d'abord, clé primaire ensuite.
5. **Valeurs hors plage**, avant l'imputation, parce que la politique « vider »
   en crée et que ces vides doivent relever du même budget que les autres.
6. **Imputation, sous budget.** En dernier, parce qu'elle a besoin des types et
   qu'elle ne doit jamais précéder une déduplication (sinon on impute des lignes
   qu'on va jeter).

## Le journal

`appliquer` rend ce qu'il a fait **et ce qu'il a refusé de faire**. La seconde
liste est celle qui a de la valeur : elle dit à l'utilisateur quelles garanties
sa donnée ne peut pas porter, et pourquoi. C'est l'information qu'un score
agrégé détruit.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from app.services.contract import (FORMES, MARQUEURS_VIDES, Contrat,
                                   ContratColonne, convertir)

#: Au-delà de cette part imputée, on ajoute une colonne indicatrice. Imputer
#: sans trace rend une valeur inventée indiscernable d'une valeur observée ;
#: l'indicatrice est la pratique standard et elle coûte une colonne.
SEUIL_INDICATRICE = 0.05


@dataclass
class Geste:
    """Une réparation faite, ou une réparation refusée."""
    action: str
    colonne: Optional[str]
    detail: str
    fait: bool = True
    mesure: Optional[float] = None


@dataclass
class Journal:
    gestes: List[Geste] = field(default_factory=list)

    def fait(self, action: str, colonne: Optional[str], detail: str,
             mesure: Optional[float] = None) -> None:
        self.gestes.append(Geste(action, colonne, detail, True, mesure))

    def refuse(self, action: str, colonne: Optional[str], detail: str,
               mesure: Optional[float] = None) -> None:
        self.gestes.append(Geste(action, colonne, detail, False, mesure))

    @property
    def faits(self) -> List[Geste]:
        return [g for g in self.gestes if g.fait]

    @property
    def refus(self) -> List[Geste]:
        return [g for g in self.gestes if not g.fait]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "faits": [{"action": g.action, "colonne": g.colonne,
                       "detail": g.detail, "mesure": g.mesure}
                      for g in self.faits],
            "refuses": [{"action": g.action, "colonne": g.colonne,
                         "detail": g.detail, "mesure": g.mesure}
                        for g in self.refus],
        }


# --------------------------------------------------------------------------- #
# Les réparations, une par clause
# --------------------------------------------------------------------------- #
def vider_les_marqueurs(df: pd.DataFrame, j: Journal) -> pd.DataFrame:
    """« inconnu », « N/A », « - » deviennent de vrais vides.

    En premier, parce que c'est la réparation dont dépendent toutes les autres :
    un marqueur laissé en place se fait imputer comme une modalité.
    """
    for col in df.columns:
        s = df[col]
        if pd.api.types.is_numeric_dtype(s) or \
                pd.api.types.is_datetime64_any_dtype(s):
            continue
        masque = s.notna() & s.astype(str).str.strip().str.lower().isin(
            MARQUEURS_VIDES)
        n = int(masque.sum())
        if n:
            vus = sorted({str(v).strip() for v in s[masque]})[:3]
            df.loc[masque, col] = np.nan
            j.fait("marqueurs_vides", col,
                   f"{n} cellules reconnues comme vides ({', '.join(repr(v) for v in vus)})",
                   mesure=float(n))
    return df


#: Écart de fréquence en dessous duquel deux graphies sont considérées à
#: égalité, et où la casse tranche plutôt que le comptage.
QUASI_EGALITE = 0.25


def _forme_canonique(variantes: List[str], freq: pd.Series) -> str:
    """Quelle graphie garder parmi `Rabat`, `rabat`, `RABAT`.

    La fréquence d'abord : c'est la forme que les données utilisent le plus. Mais
    sur ce jeu, `rabat` sortait à 104 contre 95 pour `Rabat`, et la sortie
    annonçait une ville nommée « rabat » — défendable au comptage, faux à l'œil,
    et c'est l'œil qui juge un livrable.

    Donc : à quasi-égalité, **la casse tranche**, en préférant l'initiale
    majuscule. On ne force jamais `.title()`, qui abîmerait `SARL` en `Sarl` et
    `iPhone` en `Iphone` : on choisit seulement parmi les graphies réellement
    présentes.
    """
    scores = {v: int(freq.get(v, 0)) for v in variantes}
    plafond = max(scores.values())
    if plafond == 0:
        return sorted(variantes)[0]
    tetes = [v for v, n in scores.items() if n >= plafond * (1 - QUASI_EGALITE)]
    if len(tetes) == 1:
        return tetes[0]

    def rang(v: str) -> tuple:
        majuscule_initiale = v[:1].isupper()
        tout_majuscule = v.isupper()
        # initiale majuscule d'abord, tout-majuscule en dernier, puis fréquence
        return (not majuscule_initiale, tout_majuscule, -scores[v], v)

    return sorted(tetes, key=rang)[0]


def vider_les_hors_forme(df: pd.DataFrame, contrat: Contrat,
                         j: Journal) -> pd.DataFrame:
    """Une valeur qui ne respecte pas sa forme devient un **vide**.

    Pas une valeur corrigée. `999.1.2.3` n'est pas une adresse qu'on peut
    réparer : on ignore ce qu'elle aurait dû être, et la remplacer par
    `0.0.0.0` fabriquerait un fait sur un événement de sécurité. Elle devient
    un vide, et ce vide tombe sous le budget d'imputation comme les autres —
    donc si les adresses invalides sont nombreuses, la réparation **refuse**
    au lieu d'inventer une flotte de machines qui n'existent pas.

    Placée juste après les marqueurs, et pour la même raison : une adresse
    invalide laissée en place est une modalité qui peut empoisonner un mode.
    """
    for cc in contrat.colonnes:
        if cc.forme is None or cc.nom not in df.columns:
            continue
        motif = re.compile(FORMES.get(cc.forme, cc.forme))
        serie = df[cc.nom]
        nn = serie.notna()
        mauvaises = nn & ~serie.astype(str).str.match(motif)
        combien = int(mauvaises.sum())
        if combien:
            exemples = sorted(set(serie[mauvaises].astype(str)))[:3]
            df.loc[mauvaises, cc.nom] = np.nan
            j.fait("vider_hors_forme", cc.nom,
                   f"{combien} valeur(s) hors de la forme « {cc.forme} » "
                   f"vidée(s) — {', '.join(exemples)}",
                   mesure=float(combien))
    return df


def nettoyer_le_texte(df: pd.DataFrame, contrat: Contrat,
                      j: Journal) -> pd.DataFrame:
    """Espaces superflus, puis casse — dans cet ordre.

    Pour la casse, la forme canonique est **la plus fréquente**, pas `.title()` :
    titrer transforme `RABAT` en `Rabat` mais aussi `SARL` en `Sarl` et
    `iPhone` en `Iphone`. On préfère la forme que les données utilisent le plus.
    """
    for cc in contrat.colonnes:
        if cc.type not in ("string", "category") or cc.nom not in df.columns:
            continue
        s = df[cc.nom]
        if pd.api.types.is_numeric_dtype(s):
            continue
        texte = s.where(s.isna(), s.astype(str))
        coupe = texte.where(texte.isna(), texte.str.strip())
        n = int((texte.notna() & (texte != coupe)).sum())
        if n:
            j.fait("espaces", cc.nom, f"{n} valeurs détourées", mesure=float(n))
        df[cc.nom] = coupe

        if cc.domaine:
            continue  # un domaine déclaré fixe déjà la forme attendue
        vus = coupe.dropna()
        groupes: Dict[str, List[str]] = {}
        for v in vus.unique():
            groupes.setdefault(str(v).lower(), []).append(str(v))
        collisions = {k: g for k, g in groupes.items() if len(g) > 1}
        if not collisions:
            continue
        freq = vus.value_counts()
        correspondance = {}
        for variantes in collisions.values():
            canonique = _forme_canonique(variantes, freq)
            for v in variantes:
                if v != canonique:
                    correspondance[v] = canonique
        df[cc.nom] = coupe.replace(correspondance)
        exemples = list(correspondance.items())[:3]
        j.fait("casse", cc.nom,
               f"{len(collisions)} modalité(s) fusionnée(s) : " +
               ", ".join(f"{a!r} -> {b!r}" for a, b in exemples),
               mesure=float(len(collisions)))
    return df


def poser_les_types(df: pd.DataFrame, contrat: Contrat,
                    j: Journal) -> pd.DataFrame:
    """Applique les types déclarés. Ce qui ne tient pas devient vide.

    Le typage est le livrable : sans lui, une sortie « propre » reste du texte et
    le premier `mean()` échoue. Les valeurs perdues à la conversion sont
    comptées et dites — elles ne disparaissent pas en silence.
    """
    for cc in contrat.colonnes:
        if cc.nom not in df.columns:
            continue
        avant = int(df[cc.nom].notna().sum())
        try:
            converti = convertir(df[cc.nom], cc.type, cc.format_date)
        except Exception as e:  # une colonne hostile ne doit pas tuer la passe
            j.refuse("type", cc.nom, f"conversion vers {cc.type} impossible : {e}")
            continue
        perdues = avant - int(converti.notna().sum())
        df[cc.nom] = converti
        if perdues > 0:
            j.fait("type", cc.nom,
                   f"converti en {cc.type} ; {perdues} valeur(s) illisible(s) "
                   f"mises à vide",
                   mesure=float(perdues))
        else:
            j.fait("type", cc.nom, f"converti en {cc.type} sans perte")
    return df


def dedupliquer(df: pd.DataFrame, contrat: Contrat,
                j: Journal) -> pd.DataFrame:
    """Lignes entières, puis clé primaire.

    Les deux ne disent pas la même chose. Une ligne entièrement recopiée est une
    erreur de chargement, et la retirer ne perd rien. Deux lignes qui partagent
    la clé mais diffèrent ailleurs sont un **conflit de données** : on ne peut
    pas en choisir une sans règle métier. On garde la première et on le dit, au
    lieu de laisser croire que la clé était propre.
    """
    if contrat.sans_doublons:
        n = int(df.duplicated().sum())
        if n:
            df = df.drop_duplicates().reset_index(drop=True)
            j.fait("doublons", None, f"{n} ligne(s) entièrement recopiée(s) "
                                     f"retirée(s)", mesure=float(n))

    if contrat.cle_primaire and all(c in df.columns for c in contrat.cle_primaire):
        cle = contrat.cle_primaire
        conflits = int(df.duplicated(subset=cle).sum())
        if conflits:
            df = df.drop_duplicates(subset=cle, keep="first").reset_index(drop=True)
            j.fait("cle", "+".join(cle),
                   f"{conflits} ligne(s) en conflit de clé : première gardée, "
                   f"les autres écartées — ce choix est arbitraire et mérite une "
                   f"règle métier", mesure=float(conflits))
        nuls = int(df[cle].isna().any(axis=1).sum())
        if nuls:
            df = df.dropna(subset=cle).reset_index(drop=True)
            j.fait("cle", "+".join(cle),
                   f"{nuls} ligne(s) à clé vide retirée(s) : sans clé, une ligne "
                   f"ne peut être ni jointe ni mise à jour", mesure=float(nuls))
    return df


def _valeur_de_remplissage(s: pd.Series, cc: ContratColonne):
    """Médiane pour un nombre, mode pour le reste — après les marqueurs vidés.

    L'ordre des passes rend ce choix sûr : `vider_les_marqueurs` est passé avant,
    donc le mode ne peut plus être un marqueur de vide. C'est le piège qui avait
    rempli 247 lignes avec le mot « inconnu ».
    """
    nn = s.dropna()
    if nn.empty:
        return None
    if cc.type in ("integer", "number"):
        v = float(nn.median())
        return int(round(v)) if cc.type == "integer" else v
    if cc.type == "datetime":
        return nn.median()
    modes = nn.mode()
    return modes.iloc[0] if len(modes) else None


def imputer_sous_budget(df: pd.DataFrame, contrat: Contrat,
                        j: Journal) -> pd.DataFrame:
    """Comble les vides **seulement** si le budget le permet.

    C'est la clause qui distingue ce module de l'ancienne boucle. Trois cas :

    - rien à combler : on ne touche pas ;
    - à combler dans le budget : on comble, et au-delà de 5% on ajoute une
      colonne indicatrice pour que la valeur inventée reste reconnaissable ;
    - au-delà du budget : **on refuse**, et on écrit pourquoi. La colonne reste
      vide, le contrat la déclare nullable, et l'utilisateur décide — accepter
      les vides, relever le budget, ou écarter la variable.

    Combler un tiers d'une variable par sa médiane ne la nettoie pas : ça lui
    donne un pic artificiel à la médiane, détruit sa variance, et toute
    corrélation calculée ensuite est en partie un artefact du nettoyage.
    """
    n = max(1, len(df))
    for cc in contrat.colonnes:
        if cc.nom not in df.columns:
            continue
        vides = int(df[cc.nom].isna().sum())
        if not vides:
            continue
        budget = (cc.imputation_max if cc.imputation_max is not None
                  else contrat.imputation_max)
        part = vides / n
        if part > budget + 1e-9:
            j.refuse("imputation", cc.nom,
                     f"{vides} vides ({part:.1%}) : combler dépasserait le budget "
                     f"de {budget:.0%}. La colonne reste vide et le contrat la "
                     f"déclare nullable — la variable serait fabriquée, pas "
                     f"nettoyée", mesure=round(part, 4))
            continue
        valeur = _valeur_de_remplissage(df[cc.nom], cc)
        if valeur is None:
            j.refuse("imputation", cc.nom,
                     "colonne entièrement vide : rien à partir de quoi imputer")
            continue
        if part >= SEUIL_INDICATRICE:
            df[f"{cc.nom}_impute"] = df[cc.nom].isna()
            j.fait("indicatrice", f"{cc.nom}_impute",
                   f"colonne ajoutée : {part:.1%} de « {cc.nom} » est imputé, la "
                   f"trace doit rester lisible")
        df[cc.nom] = df[cc.nom].fillna(valeur)
        j.fait("imputation", cc.nom,
               f"{vides} vides ({part:.1%}) comblés par {valeur!r}, dans le "
               f"budget de {budget:.0%}", mesure=round(part, 4))
    return df


def traiter_les_aberrantes(df: pd.DataFrame, contrat: Contrat,
                           j: Journal) -> pd.DataFrame:
    """Les valeurs hors plage — et pourquoi on n'y touche pas par défaut.

    Écrêter est la réponse réflexe, et c'est une fabrication de valeurs au même
    titre qu'une imputation : ramener trois cents revenus au plafond crée trois
    cents revenus rigoureusement identiques qui n'ont jamais existé, et toute
    statistique calculée ensuite porte sur une distribution qu'on a façonnée.
    La médiane bouge peu, la variance s'effondre, et la corrélation qu'on
    mesurera après est en partie un artefact du nettoyage.

    Supprimer les lignes est pire : on jette des observations entières à cause
    d'une colonne, et c'est souvent **la** colonne intéressante — une fraude,
    une panne, un cas limite se présentent exactement comme des valeurs
    extrêmes.

    Donc par défaut on **signale**. L'utilisateur peut demander `"ecreter"` ou
    `"vider"` dans le contrat, et alors le même budget que l'imputation
    s'applique : au-delà, la colonne n'est plus mesurée, elle est dessinée.
    """
    n = max(1, len(df))
    for cc in contrat.colonnes:
        if cc.nom not in df.columns:
            continue
        if cc.minimum is None and cc.maximum is None:
            continue
        # En flottant explicitement : `to_numeric` conserve le `Int64` d'une
        # colonne entière, et comparer ou écrêter contre une borne fractionnaire
        # issue des quartiles lève alors un TypeError.
        v = pd.to_numeric(df[cc.nom], errors="coerce").astype("float64")
        dehors = pd.Series(False, index=df.index)
        if cc.minimum is not None:
            dehors |= v < cc.minimum
        if cc.maximum is not None:
            dehors |= v > cc.maximum
        combien = int(dehors.sum())
        if not combien:
            continue

        part = combien / n
        bornes = f"[{cc.minimum}, {cc.maximum}]"
        if cc.politique_aberrantes == "signaler":
            j.refuse("aberrantes", cc.nom,
                     f"{combien} valeur(s) hors de {bornes} ({part:.1%}) laissées "
                     f"telles quelles. Écrêter inventerait {combien} valeurs "
                     f"identiques au bord ; supprimer jetterait des lignes "
                     f"entières à cause d'une colonne. Le contrat les signale, "
                     f"c'est à vous de trancher",
                     mesure=round(part, 4))
            continue

        budget = (cc.imputation_max if cc.imputation_max is not None
                  else contrat.imputation_max)
        if part > budget + 1e-9:
            j.refuse("aberrantes", cc.nom,
                     f"{combien} valeur(s) hors de {bornes} ({part:.1%}) : la "
                     f"politique « {cc.politique_aberrantes} » dépasserait le "
                     f"budget de {budget:.0%}. Au-delà, la colonne n'est plus "
                     f"mesurée, elle est dessinée",
                     mesure=round(part, 4))
            continue

        if cc.politique_aberrantes == "ecreter":
            borne = v[dehors].clip(cc.minimum, cc.maximum)
            arrondi = ""
            if pd.api.types.is_integer_dtype(df[cc.nom].dtype):
                # Une plage déduite des quartiles tombe sur des fractions, et
                # l'affecter à une colonne entière lève. On arrondit vers
                # l'intérieur de la plage plutôt que vers le plus proche : un
                # écrêtage qui déborde d'une unité produirait une valeur que le
                # contrat refuse, et la réparation échouerait sur son propre
                # résultat.
                bas = None if cc.minimum is None else int(np.ceil(cc.minimum))
                haut = None if cc.maximum is None else int(np.floor(cc.maximum))
                borne = v[dehors].clip(bas, haut).round().astype("Int64")
                arrondi = " (bornes arrondies vers l'intérieur, colonne entière)"
            df.loc[dehors, cc.nom] = borne
            j.fait("aberrantes", cc.nom,
                   f"{combien} valeur(s) ({part:.1%}) ramenées dans {bornes}"
                   f"{arrondi} — ces valeurs sont désormais fabriquées, pas "
                   f"observées", mesure=round(part, 4))
        elif cc.politique_aberrantes == "vider":
            df.loc[dehors, cc.nom] = pd.NA
            j.fait("aberrantes", cc.nom,
                   f"{combien} valeur(s) ({part:.1%}) hors de {bornes} mises à "
                   f"vide ; elles relèvent maintenant du budget d'imputation",
                   mesure=round(part, 4))
        else:
            j.refuse("aberrantes", cc.nom,
                     f"politique inconnue : {cc.politique_aberrantes!r} "
                     f"(attendu : signaler, ecreter, vider)")
    return df


def appliquer(df: pd.DataFrame, contrat: Contrat) -> tuple[pd.DataFrame, Journal]:
    """Les six passes, dans l'ordre, sur une copie."""
    j = Journal()
    d = df.copy()
    d = vider_les_marqueurs(d, j)
    # Les hors-forme juste après : une adresse invalide laissée en place est
    # une modalité, et une modalité peut empoisonner un mode.
    d = vider_les_hors_forme(d, contrat, j)
    d = nettoyer_le_texte(d, contrat, j)
    d = poser_les_types(d, contrat, j)
    d = dedupliquer(d, contrat, j)
    # Les aberrantes AVANT l'imputation : la politique « vider » crée des vides,
    # et ces vides doivent pouvoir être comblés dans le budget comme les autres.
    d = traiter_les_aberrantes(d, contrat, j)
    d = imputer_sous_budget(d, contrat, j)
    return d, j
