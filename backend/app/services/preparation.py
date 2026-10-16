"""La moitié du prétraitement que ce projet refusait de faire, et pourquoi
elle peut enfin être faite.

## Ce qui était exclu, et la raison qui était bonne

`CLAUDE.md` disait : pas de normalisation, pas d'encodage, pas de mise à
l'échelle, parce qu'**ajuster un transformateur sur un fichier entier fait
fuiter l'information du test dans l'entraînement**. Un `StandardScaler` ajusté
avant la séparation connaît la moyenne et l'écart-type des lignes qu'il est
censé n'avoir jamais vues, et toute évaluation faite ensuite est optimiste
d'une quantité qu'on ne sait pas mesurer.

Cette raison est juste, et elle ne disparaît pas.

## Ce qui change : on ne transforme pas le fichier, on rend la chaîne

Le prétraitement complet n'exige pas de transformer les données ici. Il exige
de produire **la chaîne de transformation**, et de la faire ajuster là où
c'est correct : après la séparation, sur le seul jeu d'entraînement.

C'est exactement ce que ce dépôt fait déjà ailleurs — il n'édite pas le
fichier de l'utilisateur, il écrit un script autonome qu'on peut relire. Ici
le script rend un `Pipeline` scikit-learn, et **la séparation précède
l'ajustement**, dans cet ordre, sans qu'aucun chemin ne permette l'inverse.

Le résultat est un prétraitement entier — vides, types, doublons, encodage,
échelle — dont aucune étape n'est ajustée sur des lignes qu'elle ne doit pas
voir.

## Ce qu'il refuse, et ce que ça coûte de ne pas refuser

Le refus est la même règle que partout dans ce projet : une colonne qu'on ne
sait pas traiter honnêtement est **écartée en le disant**, jamais traitée au
jugé.

- **Haute cardinalité.** Un encodage one-hot sur une colonne à 2 684
  modalités produit 2 684 colonnes. Ce n'est pas une préparation, c'est une
  table creuse que rien ne peut apprendre. Au-delà de `MODALITES_MAX`, la
  colonne sort, avec son compte.
- **La clé.** Un identifiant n'est pas une variable explicative. L'inclure
  donne un modèle qui mémorise la ligne.
- **Une colonne constante.** Variance nulle : elle n'apporte rien et casse
  certains estimateurs.
- **Les dates.** Une date n'est pas un nombre, et la dériver (année, mois,
  jour de semaine, ancienneté) demande de savoir ce que le modèle doit
  apprendre. Le plan le dit plutôt que de choisir à la place de l'utilisateur.

Chaque exclusion porte sa raison en clair, et le script écrit la liste en
commentaire. Une colonne disparue sans explication est une colonne qu'on
croira avoir utilisée.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd

from .contract import TYPES, Contrat, ContratColonne

#: Au-delà, un encodage one-hot crée plus de colonnes qu'il n'en explique.
#: Trente est un choix, pas une loi : il est déclaré ici, passé en paramètre,
#: et écrit dans le script produit, pour qu'il puisse être contesté.
MODALITES_MAX = 30

#: En dessous, la séparation ne veut plus rien dire : vingt lignes de test
#: tirées de cent, c'est une mesure dont l'intervalle couvre tout.
LIGNES_MIN = 50

#: Le vocabulaire des types vient de `contract.TYPES` et n'est pas recopié :
#: une liste dupliquée est une liste qui diverge, et un type inconnu ici
#: tomberait silencieusement dans la branche « catégorielle », où il serait
#: encodé au lieu d'être écarté. `test_chaque_type_du_contrat_a_un_role` tient
#: la correspondance complète.
NUMERIQUES = {"integer", "number"}
TEMPORELS = {"datetime"}
BOOLEENS = {"boolean"}
CATEGORIELS = {"category", "string"}

assert set(TYPES) == NUMERIQUES | TEMPORELS | BOOLEENS | CATEGORIELS, (
    f"contract.TYPES a changé : {sorted(TYPES)} ne correspond plus aux rôles "
    "déclarés ici")


@dataclass
class Role:
    """Ce qu'une colonne devient dans la chaîne, et pourquoi."""
    nom: str
    role: str                  # "numerique" | "categorielle" | "cible" | "ecartee"
    raison: str
    modalites: Optional[int] = None


@dataclass
class Plan:
    """La chaîne proposée, avec tout ce qu'elle ne fait pas."""
    roles: List[Role] = field(default_factory=list)
    cible: Optional[str] = None
    lignes: int = 0
    refus: List[str] = field(default_factory=list)
    modalites_max: int = MODALITES_MAX

    def _noms(self, role: str) -> List[str]:
        return [r.nom for r in self.roles if r.role == role]

    @property
    def numeriques(self) -> List[str]:
        return self._noms("numerique")

    @property
    def categorielles(self) -> List[str]:
        return self._noms("categorielle")

    @property
    def ecartees(self) -> List[Role]:
        return [r for r in self.roles if r.role == "ecartee"]

    @property
    def utilisable(self) -> bool:
        """Une chaîne sans aucune variable explicative n'est pas une chaîne."""
        return bool(self.numeriques or self.categorielles)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "cible": self.cible,
            "lignes": self.lignes,
            "numeriques": self.numeriques,
            "categorielles": self.categorielles,
            "ecartees": [{"nom": r.nom, "raison": r.raison,
                          "modalites": r.modalites} for r in self.ecartees],
            "refus": self.refus,
            "modalites_max": self.modalites_max,
            "utilisable": self.utilisable,
        }


def _modalites(serie: pd.Series) -> int:
    return int(serie.nunique(dropna=True))


def planifier(df: pd.DataFrame, contrat: Contrat, *,
              cible: Optional[str] = None,
              modalites_max: int = MODALITES_MAX) -> Plan:
    """Quel rôle chaque colonne tient, décidé sur le contrat ET sur les données.

    Le contrat donne le type **tenable** — celui que la colonne porte
    entièrement, pas celui que 86 % d'un échantillon suggère. La cardinalité,
    elle, se mesure sur les données : un contrat ne dit pas combien de
    modalités une colonne catégorielle porte, et c'est ce nombre qui décide
    si l'encoder a un sens.
    """
    plan = Plan(cible=cible, lignes=len(df), modalites_max=modalites_max)
    cles = set(contrat.cle_primaire or [])
    par_nom: Dict[str, ContratColonne] = {c.nom: c for c in contrat.colonnes}

    if len(df) < LIGNES_MIN:
        plan.refus.append(
            f"{len(df)} ligne(s) : en dessous de {LIGNES_MIN}, une séparation "
            "entraînement/test ne mesure plus rien d'utile. La chaîne est "
            "rendue quand même, l'évaluation qu'elle permet ne l'est pas.")

    if cible is not None and cible not in df.columns:
        plan.refus.append(
            f"colonne cible {cible!r} absente du fichier : la chaîne est "
            "construite sans cible, donc sans séparation stratifiée.")
        plan.cible = cible = None

    for nom in df.columns:
        colonne = par_nom.get(nom)
        type_ = (colonne.type if colonne else "string").lower()
        serie = df[nom]

        if cible is not None and nom == cible:
            plan.roles.append(Role(nom, "cible", "déclarée comme cible"))
            continue

        if nom in cles:
            plan.roles.append(Role(
                nom, "ecartee",
                "clé primaire : un identifiant n'explique rien, il désigne "
                "une ligne, et l'inclure donne un modèle qui la mémorise"))
            continue

        modalites = _modalites(serie)
        if modalites <= 1:
            plan.roles.append(Role(
                nom, "ecartee",
                "une seule valeur sur tout le fichier : variance nulle, aucune "
                "information, et certains estimateurs refusent la colonne",
                modalites))
            continue

        if type_ in NUMERIQUES:
            plan.roles.append(Role(nom, "numerique",
                                   f"type tenable « {type_} »"))
        elif type_ in BOOLEENS:
            plan.roles.append(Role(nom, "categorielle",
                                   "booléen : deux modalités", modalites))
        elif type_ in TEMPORELS:
            plan.roles.append(Role(
                nom, "ecartee",
                "date : la dériver (année, mois, jour de semaine, ancienneté) "
                "demande de savoir ce que le modèle doit apprendre, et ce "
                "choix n'est pas le nôtre", modalites))
        elif type_ not in CATEGORIELS:
            # Impossible tant que l'assertion du module tient, mais un type
            # ajouté à `contract.TYPES` sans rôle ici serait encodé au jugé.
            plan.roles.append(Role(
                nom, "ecartee",
                f"type « {type_} » sans rôle déclaré dans la préparation : "
                "écartée plutôt que traitée au jugé", modalites))
        elif modalites > modalites_max:
            plan.roles.append(Role(
                nom, "ecartee",
                f"{modalites} modalités pour un plafond de {modalites_max} : "
                "un encodage one-hot produirait plus de colonnes qu'il n'en "
                "explique", modalites))
        else:
            plan.roles.append(Role(nom, "categorielle",
                                   f"{modalites} modalités", modalites))

    if not plan.utilisable:
        plan.refus.append(
            "aucune colonne n'est utilisable comme variable explicative : "
            "il n'y a pas de chaîne à construire, et en rendre une vide "
            "laisserait croire le contraire.")
    return plan


# --------------------------------------------------------------------------- #
# Le script
# --------------------------------------------------------------------------- #
def _liste(noms: List[str]) -> str:
    if not noms:
        return "[]"
    return "[\n    " + ",\n    ".join(repr(n) for n in noms) + ",\n]"


def script(plan: Plan, *, fichier: str = "donnees.csv",
           part_test: float = 0.2, graine: int = 0) -> str:
    """Un script autonome qui construit la chaîne et l'ajuste **après** la
    séparation.

    Il n'y a pas d'option pour l'ajuster avant. Un paramètre qui permettrait
    la fuite serait un paramètre que quelqu'un finirait par passer.
    """
    exclusions = "\n".join(
        f"#   - {r.nom} : {r.raison}" + (f" ({r.modalites} modalités)"
                                         if r.modalites is not None
                                         and "modalités" not in r.raison else "")
        for r in plan.ecartees) or "#   (aucune)"

    refus = "\n".join(f"# ⚠ {r}" for r in plan.refus)
    bloc_refus = (refus + "\n#\n") if refus else ""

    cible = plan.cible
    separation = (
        f"""X = donnees.drop(columns=[{cible!r}])
    y = donnees[{cible!r}]
    # La séparation D'ABORD. Tout ce qui suit n'a le droit de voir que
    # X_train : c'est la seule raison pour laquelle ce script existe.
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size={part_test}, random_state={graine},
        stratify=y if y.nunique() < 20 else None)"""
        if cible else
        f"""X = donnees
    # Aucune cible déclarée : on sépare quand même, pour que la chaîne soit
    # ajustée sur une partie seulement et reste évaluable plus tard.
    X_train, X_test = train_test_split(
        X, test_size={part_test}, random_state={graine})
    y_train = y_test = None""")

    ajustement = ("chaine.fit(X_train, y_train)" if cible
                  else "chaine.fit(X_train)")

    return f'''#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Chaîne de prétraitement — engendrée pour {fichier}.

La séparation précède l'ajustement, et c'est tout l'objet de ce fichier.

Un `StandardScaler` ajusté sur le fichier entier connaît la moyenne et
l'écart-type des lignes qu'il est censé n'avoir jamais vues ; un
`OneHotEncoder` ajusté de même connaît des modalités qui n'apparaissent que
dans le test. L'évaluation qui suit est alors optimiste d'une quantité que
personne ne sait mesurer, et elle a l'air parfaitement normale.

Ici, `train_test_split` passe en premier et `fit` ne voit que `X_train`.
Il n'existe pas d'option pour faire autrement.

Colonnes écartées, et pourquoi :
{exclusions}
#
{bloc_refus}Dépendances : pandas, scikit-learn.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

NUMERIQUES = {_liste(plan.numeriques)}

CATEGORIELLES = {_liste(plan.categorielles)}


def construire() -> Pipeline:
    """La chaîne, non ajustée. Elle ne connaît encore aucune donnée."""
    numerique = Pipeline([
        # La médiane plutôt que la moyenne : une valeur extrême déplace la
        # moyenne et donc toutes les valeurs imputées.
        ("vides", SimpleImputer(strategy="median")),
        ("echelle", StandardScaler()),
    ])
    categorielle = Pipeline([
        ("vides", SimpleImputer(strategy="most_frequent")),
        # `handle_unknown="ignore"` : une modalité vue seulement dans le test
        # ne doit pas faire tomber la prédiction. Elle sort en zéros, ce qui
        # est honnête — on ne sait rien d'elle.
        ("encodage", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
    ])
    return Pipeline([
        ("colonnes", ColumnTransformer(
            [("num", numerique, NUMERIQUES),
             ("cat", categorielle, CATEGORIELLES)],
            # Tout ce qui n'est pas listé est abandonné, et la liste des
            # écartées est en tête de ce fichier. Une colonne qui disparaît
            # sans explication est une colonne qu'on croira avoir utilisée.
            remainder="drop")),
    ])


def preparer(chemin: str | Path):
    donnees = pd.read_csv(chemin)
    manquantes = [c for c in NUMERIQUES + CATEGORIELLES
                  if c not in donnees.columns]
    if manquantes:
        raise SystemExit(
            f"colonnes absentes du fichier : {{manquantes}}. La chaîne a été "
            "construite pour un autre fichier, ou celui-ci a changé.")

    {separation}

    chaine = construire()
    {ajustement}

    return chaine, (X_train, X_test, y_train, y_test)


def main() -> int:
    ap = argparse.ArgumentParser(description="Prépare les données sans fuite.")
    ap.add_argument("--input", "-i", required=True)
    args = ap.parse_args()

    chaine, (X_train, X_test, y_train, y_test) = preparer(args.input)
    avant = X_train.shape[1]
    apres = chaine.transform(X_train).shape[1]
    print(f"entraînement {{len(X_train)}} lignes, test {{len(X_test)}} lignes")
    print(f"{{avant}} colonne(s) en entrée -> {{apres}} après encodage")
    print("La chaîne est ajustée sur l'entraînement seul.")
    print("Pour l'appliquer au test : chaine.transform(X_test)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
'''
