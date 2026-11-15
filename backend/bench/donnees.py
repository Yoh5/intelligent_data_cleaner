"""Les tables de vérité : chaque cellule est connue avant qu'on l'abîme.

Une table de vérité est un DataFrame de **texte canonique** (ce qu'une sortie
prête à l'usage doit contenir, cellule par cellule) et un schéma qui dit le
rôle de chaque colonne. `None` veut dire « la vraie valeur est inconnue » : la
seule sortie juste est alors une cellule vide.

Deux sources :

- `synthetique` — tout est fabriqué, donc tout est contrôlé : les rôles, les
  pièges (codes à zéro initial, valeurs extrêmes vraies), la part d'inconnu ;
- `titanic` — des distributions réelles. On ne garde que les lignes complètes
  sur les colonnes retenues : ce qui reste est la vérité.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas as pd

#: Les rôles qu'une colonne peut tenir, et donc la façon de comparer ses cellules.
ROLES = ("id", "code", "categorie", "nombre", "entier", "date", "booleen", "texte")


@dataclass
class Verite:
    nom: str
    table: pd.DataFrame            # texte canonique ; None = inconnu
    roles: Dict[str, str]          # colonne -> rôle
    #: colonnes où l'inconnu est légitime (on y pose de vrais vides)
    nullables: tuple = ()


VILLES = ("Casablanca", "Rabat", "Marrakech", "Tanger", "Fès", "Agadir")


def synthetique(n: int = 1000, graine: int = 7) -> Verite:
    rng = np.random.default_rng(graine)
    ids = np.arange(1, n + 1)
    # Codes client à cinq chiffres : un sur dix commence par zéro. Une
    # lecture numérique les mange sans erreur.
    codes = [f"{c:05d}" for c in rng.integers(0, 100000, n)]
    villes = rng.choice(VILLES, n, p=(.34, .22, .18, .12, .08, .06))
    montants = np.round(rng.lognormal(6.0, 0.8, n), 2)
    # Dix valeurs extrêmes VRAIES : un gros client n'est pas une faute de frappe.
    gros = rng.choice(n, 10, replace=False)
    montants[gros] = np.round(montants[gros] * 40, 2)
    ages = rng.integers(18, 81, n).astype(object)
    jours = rng.integers(0, 669, n)
    dates = (pd.Timestamp("2025-01-01") + pd.to_timedelta(jours, unit="D")).strftime("%Y-%m-%d")
    actifs = rng.choice(["true", "false"], n, p=(.7, .3))

    table = pd.DataFrame({
        "id": [str(i) for i in ids],
        "code_client": codes,
        "ville": villes,
        "montant": [_nombre(m) for m in montants],
        "age": [str(a) for a in ages],
        "date_commande": list(dates),
        "actif": actifs,
    }, dtype=object)
    # 5 % d'âges réellement inconnus : la vérité est « vide ».
    inconnus = rng.choice(n, n // 20, replace=False)
    table.loc[inconnus, "age"] = None
    roles = {"id": "id", "code_client": "code", "ville": "categorie", "montant": "nombre",
             "age": "entier", "date_commande": "date", "actif": "booleen"}
    return Verite("synthetique", table, roles, nullables=("age",))


def titanic(chemin: Path, graine: int = 7) -> Optional[Verite]:
    """Les lignes complètes de Titanic, sur sept colonnes. `None` si absent."""
    if not chemin.exists():
        return None
    brut = pd.read_csv(chemin, dtype=str, keep_default_na=False)
    brut.columns = [c.strip().lower() for c in brut.columns]
    garder = ["passengerid", "pclass", "name", "sex", "age", "fare", "embarked"]
    d = brut[garder].copy()
    d = d[(d != "").all(axis=1)].reset_index(drop=True)
    # La vérité est canonique : les espaces de bord du fichier source ne sont
    # pas une information, et un nettoyeur qui les retire a raison.
    d = d.apply(lambda s: s.str.strip())
    rng = np.random.default_rng(graine)
    table = pd.DataFrame({
        "passengerid": d["passengerid"],
        "pclass": d["pclass"],
        "name": d["name"],
        "sex": d["sex"],
        "age": [_nombre(float(a)) for a in d["age"]],
        "fare": [_nombre(float(f)) for f in d["fare"]],
        "embarked": d["embarked"],
    }, dtype=object)
    inconnus = rng.choice(len(table), len(table) // 20, replace=False)
    table.loc[inconnus, "age"] = None
    roles = {"passengerid": "id", "pclass": "entier", "name": "texte", "sex": "categorie",
             "age": "nombre", "fare": "nombre", "embarked": "categorie"}
    return Verite("titanic", table, roles, nullables=("age",))


def _nombre(x: float) -> str:
    """Écriture canonique d'un nombre : la plus courte qui le relit à l'identique."""
    s = repr(float(x))
    return s[:-2] if s.endswith(".0") else s
