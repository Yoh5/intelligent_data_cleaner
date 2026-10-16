"""`POST /prepare/` — la chaîne de prétraitement, pas les données transformées.

Cette route ne rend **pas** un fichier nettoyé et encodé. Elle rend le script
qui construit la chaîne, parce qu'une chaîne ajustée ici le serait sur le
fichier entier, et c'est précisément la fuite que ce module existe pour
empêcher.

Elle s'appuie sur la passe de contrat : le type d'une colonne est celui que le
contrat déclare **tenable**, pas celui qu'un échantillon suggère. Une colonne
de codes dont 86 % se lisent comme des nombres reste du texte, et la chaîne ne
la met pas à l'échelle.
"""
from __future__ import annotations

from typing import Optional

import pandas as pd
from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from app.routers.auto_clean import _illisible
from app.services import agent_contrat, contract, preparation

router = APIRouter(prefix="/prepare", tags=["prepare"])

EXTENSIONS = {".csv", ".xlsx", ".xls"}
TAILLE_MAX = 25 * 1024 * 1024  # même plafond que la passe de contrat


@router.post("/")
async def preparer(
    file: UploadFile = File(...),
    cible: Optional[str] = Form(None),
    usage: str = Form("modele"),
    modalites_max: int = Form(preparation.MODALITES_MAX),
):
    nom = file.filename or ""
    if nom[nom.rfind("."):].lower() not in EXTENSIONS:
        raise HTTPException(400, f"type de fichier non pris en charge : {nom}")

    contenu = await file.read()
    if len(contenu) > TAILLE_MAX:
        raise HTTPException(
            413, "fichier trop volumineux (25 Mo) : au-delà, la passe demande "
                 "un traitement en arrière-plan, qui est un autre produit")

    # Même filet que `/auto-clean/` : extension juste, contenu illisible.
    # Laisser remonter l'exception rendait un 500 et une trace de pandas.
    try:
        passe = await agent_contrat.run(contenu, nom, usage=usage)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, _illisible(nom, e))
    ct = contract.Contrat.from_dict(passe["contrat"])

    # Le plan se décide sur les données telles qu'elles sont, pas sur la
    # version réparée : c'est le fichier de l'utilisateur que sa chaîne devra
    # lire, et sa cardinalité réelle qui décide si l'encoder a un sens.
    donnees = await _lire(contenu, nom)
    plan = preparation.planifier(donnees, ct, cible=cible,
                                 modalites_max=modalites_max)

    return {
        "plan": plan.to_dict(),
        "script": preparation.script(plan, fichier=nom),
        "filename": f"preparer_{nom.rsplit('.', 1)[0]}.py",
        "contrat_lisible": passe["contrat_lisible"],
        "message": _message(plan),
    }


async def _lire(contenu: bytes, nom: str) -> pd.DataFrame:
    analyse, origine = await agent_contrat._profiler(contenu, nom)
    return origine


def _message(plan: preparation.Plan) -> str:
    if not plan.utilisable:
        return ("Aucune colonne n'est utilisable comme variable explicative. "
                "Rendre une chaîne vide laisserait croire le contraire.")
    morceaux = [
        f"{len(plan.numeriques)} colonne(s) numérique(s), "
        f"{len(plan.categorielles)} catégorielle(s)."
    ]
    if plan.ecartees:
        morceaux.append(f"{len(plan.ecartees)} écartée(s), chacune avec sa raison.")
    morceaux.append("La chaîne est ajustée après la séparation, sur "
                    "l'entraînement seul.")
    return " ".join(morceaux)
