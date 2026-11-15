"""Les nettoyeurs mis à l'épreuve, et ce que chacun dit de lui-même.

Chacun reçoit les mêmes octets et rend un CSV, plus son propre verdict
(« prêt » ou non) quand il en donne un. C'est ce verdict que le banc confronte
ensuite à la vérité.

- `contrat_base` / `contrat_modele` — la passe sous contrat, pour deux usages :
  le chargement en base n'autorise aucune imputation, l'entraînement d'un
  modèle en tolère un peu. Son verdict : « contrat tenu ».
- `ancienne_boucle` — la première version (`/agent-clean`) : elle nettoie, se
  note avec son propre profileur, recommence. Son verdict : « qualité ≥ 90 ».
- `carnet_pandas` — ce qu'on écrit dans un carnet en dix minutes : lecture par
  défaut, doublons retirés, espaces coupés, vides comblés (médiane ou mode).
  Il ne dit rien de lui-même.
"""
from __future__ import annotations

import asyncio
import base64
import io
import time
from typing import Callable, Dict, Optional, Tuple

import pandas as pd

Resultat = Tuple[bytes, Optional[bool], str]


def _sans_llm() -> None:
    """Le conseiller de l'ancienne boucle appelle un LLM s'il trouve une clé.
    Le banc doit être gratuit et reproductible : on le coupe."""
    from app.services import advisor
    advisor._api_key = lambda: ""


def contrat(usage: str) -> Callable[[bytes, str], Resultat]:
    def lancer(octets: bytes, nom: str) -> Resultat:
        from app.services import agent_contrat
        r = asyncio.run(agent_contrat.run(octets, nom, usage=usage))
        return (base64.b64decode(r["csv_base64"]), bool(r["contrat_tenu"]),
                f"contrat {'tenu' if r['contrat_tenu'] else 'non tenu'}, {r['score_apres']}/100")
    return lancer


def ancienne_boucle(octets: bytes, nom: str) -> Resultat:
    _sans_llm()
    from app.services import agent_cleaner
    r = asyncio.run(agent_cleaner.run(octets, nom))
    q = r["quality_final"]
    return base64.b64decode(r["cleaned_csv_base64"]), q >= r["target"], f"qualité {q}/100"


def carnet_pandas(octets: bytes, nom: str) -> Resultat:
    df = pd.read_csv(io.BytesIO(octets))
    df = df.drop_duplicates()
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].str.strip()
    for c in df.columns:
        if df[c].isna().any():
            if pd.api.types.is_numeric_dtype(df[c]):
                df[c] = df[c].fillna(df[c].median())
            else:
                mode = df[c].mode()
                if len(mode):
                    df[c] = df[c].fillna(mode.iloc[0])
    return df.to_csv(index=False).encode("utf-8"), None, "aucun verdict"


#: Le niveau d'exigence auquel chaque nettoyeur est jugé : celui de l'usage
#: qu'il annonce. Ceux qui n'annoncent rien sont jugés pour une base, l'usage
#: le plus exigeant — c'est là que leurs sorties finissent le plus souvent.
PROFILS = {"contrat_modele": "modele"}

NETTOYEURS: Dict[str, Callable[[bytes, str], Resultat]] = {
    "contrat_base": contrat("base"),
    "contrat_modele": contrat("modele"),
    "ancienne_boucle": ancienne_boucle,
    "carnet_pandas": carnet_pandas,
}


def executer(nom: str, octets: bytes, fichier: str) -> Tuple[Resultat, float, str]:
    t = time.perf_counter()
    try:
        return NETTOYEURS[nom](octets, fichier), time.perf_counter() - t, ""
    except Exception as e:  # un nettoyeur qui plante est un résultat, pas un arrêt du banc
        return (b"", None, ""), time.perf_counter() - t, f"{type(e).__name__}: {e}"
