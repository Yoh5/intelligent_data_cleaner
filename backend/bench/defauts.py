"""Abîmer une table de vérité, en notant chaque coup porté.

Chaque défaut est **nommé** et **localisé** (identifiant de ligne, colonne) :
c'est ce qui permet ensuite de dire, cellule par cellule, si le nettoyeur l'a
réparé, l'a manqué, ou a abîmé une cellule voisine qui allait très bien.

Les défauts sont ceux qu'on rencontre dans de vrais exports — un export Excel
français, une saisie manuelle, un double chargement — et chacun a une seule
réparation juste, connue d'avance :

| défaut      | rôles touchés                       | réparation juste                |
|-------------|-------------------------------------|---------------------------------|
| marqueur    | colonnes où l'inconnu est légitime  | une cellule vide                |
| espaces     | catégorie, texte, code, nombre      | la valeur sans ses espaces      |
| casse       | catégorie                           | la graphie canonique            |
| virgule     | nombre                              | le nombre, point décimal        |
| date_fr     | date                                | la date au format ISO           |
| doublon     | ligne entière                       | une seule ligne                 |

Ce qui n'est **jamais** abîmé sert de piège : les codes à zéro initial, les
identifiants, les booléens, les valeurs extrêmes vraies. Une différence sur ces
cellules ne peut venir que du nettoyeur.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Tuple

import numpy as np
import pandas as pd

from bench.donnees import Verite

MARQUEURS = ("N/A", "inconnu", "?", "-", "null", "NA", "n.d.")

#: Part des cellules éligibles touchées par chaque défaut, et part de lignes doublées.
TAUX = 0.04
TAUX_DOUBLON = 0.03


@dataclass
class Echantillon:
    verite: Verite
    brut: pd.DataFrame                      # ce que le nettoyeur reçoit, en texte
    defauts: List[Tuple[str, str, str]]     # (id, colonne, défaut)
    doublons: int
    octets: bytes = b""
    graine: int = 0
    resume: dict = field(default_factory=dict)


def abimer(v: Verite, graine: int = 11) -> Echantillon:
    rng = np.random.default_rng(graine)
    brut = v.table.copy()
    id_col = next(c for c, r in v.roles.items() if r == "id")
    pris = set()                    # une cellule ne reçoit qu'un défaut
    defauts: List[Tuple[str, str, str]] = []

    def tirer(col: str, eligibles: np.ndarray) -> np.ndarray:
        libres = np.array([i for i in eligibles if (i, col) not in pris], dtype=int)
        k = int(round(len(libres) * TAUX))
        if not k:
            return np.array([], dtype=int)
        choisis = rng.choice(libres, k, replace=False)
        for i in choisis:
            pris.add((int(i), col))
        return choisis

    for col, role in v.roles.items():
        s = v.table[col]
        presents = np.flatnonzero(s.notna().to_numpy())
        absents = np.flatnonzero(s.isna().to_numpy())

        # L'inconnu : la moitié écrite comme une vraie cellule vide (sain),
        # l'autre moitié comme un marqueur (défaut).
        if col in v.nullables and len(absents):
            for j, i in enumerate(absents):
                if j % 2:
                    brut.at[i, col] = MARQUEURS[j % len(MARQUEURS)]
                    pris.add((int(i), col))
                    defauts.append((v.table.at[i, id_col], col, "marqueur"))
                else:
                    brut.at[i, col] = ""

        if role in ("categorie", "texte", "code", "nombre"):
            for i in tirer(col, presents):
                brut.at[i, col] = (" " * int(rng.integers(1, 3))) + str(s.iat[i]) + " "
                defauts.append((v.table.at[i, id_col], col, "espaces"))

        if role == "categorie":
            for i in tirer(col, presents):
                val = str(s.iat[i])
                brut.at[i, col] = val.lower() if rng.random() < .6 else val.upper()
                if brut.at[i, col] != val:
                    defauts.append((v.table.at[i, id_col], col, "casse"))

        if role == "nombre":
            avec_point = np.array([i for i in presents if "." in str(s.iat[i])], dtype=int)
            for i in tirer(col, avec_point):
                brut.at[i, col] = str(s.iat[i]).replace(".", ",")
                defauts.append((v.table.at[i, id_col], col, "virgule"))

        if role == "date":
            for i in tirer(col, presents):
                a, m, j = str(s.iat[i]).split("-")
                brut.at[i, col] = f"{j}/{m}/{a}"
                defauts.append((v.table.at[i, id_col], col, "date_fr"))

    brut = brut.where(brut.notna(), "")

    # Les doublons francs : des lignes entières recopiées, défauts compris.
    k = int(round(len(brut) * TAUX_DOUBLON))
    copies = brut.iloc[rng.choice(len(brut), k, replace=False)]
    brut = pd.concat([brut, copies], ignore_index=True)
    brut = brut.iloc[rng.permutation(len(brut))].reset_index(drop=True)

    resume = {}
    for _, _, d in defauts:
        resume[d] = resume.get(d, 0) + 1
    resume["doublon"] = k
    return Echantillon(v, brut, defauts, k, brut.to_csv(index=False).encode("utf-8"), graine, resume)
