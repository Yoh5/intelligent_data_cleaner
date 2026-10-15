"""Prétraitement sous contrat : deux endpoints, et la raison d'en avoir deux.

`/contract-clean/propose` rend le contrat **proposé** sans toucher aux données,
avec les arbitrages qu'il a dû faire. `/contract-clean/` exécute la passe, en
acceptant un contrat édité.

Les séparer n'est pas une commodité d'API, c'est le cœur du produit. La question
« que vas-tu faire de ces données » n'a pas de bonne réponse par défaut : un
chargement en base interdit d'inventer la moindre valeur, un entraînement de
modèle tolère un peu d'imputation, un tableau de bord veut surtout des modalités
qui ne se dédoublent pas. L'utilisateur voit donc ce qui va être exigé de sa
donnée, le corrige si besoin, et **puis** lance le traitement.

À comparer avec `/agent-clean`, qui itère contre le score de son propre
profileur : il annonçait 100/100 sur un fichier où le mot « inconnu » avait été
imputé dans 247 lignes sur 400.
"""
from typing import Any, Dict, Optional

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from pydantic import BaseModel

from app.services import agent_contrat, contract, contract_infer
from app.services.profiler import DataProfiler

router = APIRouter(prefix="/contract-clean", tags=["contract-clean"])

_ALLOWED = {".csv", ".xlsx", ".xls"}

#: Débit mesuré de la passe complète : **1,8 Mo/s** (500 000 lignes, 21 Mo, en
#: 11,4 s de médiane sur cinq passages, écart-type 0,8 s).
#:
#: Les autres routes annoncent 100 Mo. À ce débit, 100 Mo demanderaient près
#: d'une minute — au-delà de ce qu'une requête HTTP ordinaire tient, donc le
#: client n'aurait rien du tout. Une limite qu'on ne peut pas honorer est pire
#: qu'une limite plus basse qu'on tient : on annonce ce qu'on sait faire, et le
#: message d'erreur dit pourquoi.
#:
#: Au-delà, il faudra un traitement en tâche de fond avec un identifiant de
#: travail — c'est un autre produit, pas un réglage.
_DEBIT_MO_S = 1.8
_MAX_MO = 25
_MAX = _MAX_MO * 1024 * 1024


class Usage(BaseModel):
    """Les usages proposés, avec leur libellé, pour que le client les affiche."""
    cle: str
    libelle: str


def _sans_donnees(nom: str, e: Exception) -> str:
    """Le message d'un fichier qu'on a bien lu, mais qui ne dit rien."""
    if "vide" not in str(e).lower():
        return f"Fichier illisible : {e}"
    return (f"« {nom} » ne contient aucune ligne de données — un en-tête seul, "
            f"ou rien du tout. C'est un état normal avant un premier "
            f"chargement et un incident après, et rien dans le fichier ne "
            f"permet de trancher : aucun contrat n'est donc proposé.")


def _verifier_taille(content: bytes) -> None:
    """Refuse ce qui ne tiendrait pas dans une requête, en disant le temps."""
    mo = len(content) / (1024 * 1024)
    if mo > _MAX_MO:
        raise HTTPException(
            status_code=413,
            detail=(f"Fichier trop volumineux : {mo:.0f} Mo pour un maximum de "
                    f"{_MAX_MO} Mo. La passe traite environ {_DEBIT_MO_S} Mo/s, "
                    f"donc ce fichier demanderait {mo / _DEBIT_MO_S:.0f} s — "
                    f"plus qu'une requête HTTP ne tient."))


def _verifier(file: UploadFile) -> str:
    name = file.filename or ""
    ext = name.lower()[name.rfind("."):] if "." in name else ""
    if ext not in _ALLOWED:
        raise HTTPException(status_code=400, detail=f"Unsupported file type: {ext}")
    return name


@router.get("/usages", response_model=list[Usage])
async def usages() -> list[Usage]:
    return [Usage(cle=k, libelle=v["libelle"])
            for k, v in contract_infer.USAGES.items()]


@router.post("/propose")
async def propose(file: UploadFile = File(...),
                  usage: str = Form(contract_infer.USAGE_DEFAUT),
                  proposer_domaines: bool = Form(False)) -> Dict[str, Any]:
    """Le contrat proposé, sans rien modifier.

    On rend aussi `arbitrages` : les cas où la donnée ne peut pas tenir ce que
    l'usage demanderait (une colonne vide à 62% ne peut pas être comblée dans un
    budget de 10%). Ces phrases sont le vrai contenu de la réponse — c'est là
    que l'utilisateur apprend quelque chose sur son fichier.
    """
    name = _verifier(file)
    content = await file.read()
    _verifier_taille(content)
    if usage not in contract_infer.USAGES:
        raise HTTPException(
            status_code=400,
            detail=f"Usage inconnu : {usage!r} "
                   f"(attendu : {sorted(contract_infer.USAGES)})")
    try:
        p = DataProfiler()
        analyse = await p.analyze_file(content, name, texte_brut=True)
        ct, notes = contract_infer.inferer(
            analyse, p.df, usage=usage, proposer_domaines=proposer_domaines)
        avant = contract.verify(p.df, ct)
        return {
            "usage": usage,
            "libelle": contract_infer.USAGES[usage]["libelle"],
            "contrat": ct.to_dict(),
            "contrat_lisible": contract_infer.resume(ct),
            "arbitrages": notes,
            "etat_initial": avant.to_dict(),
            "dataset": analyse.get("dataset_info", {}),
        }
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=_sans_donnees(name, e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur d'analyse : {e}")


@router.post("/")
async def contract_clean(file: UploadFile = File(...),
                         usage: str = Form(contract_infer.USAGE_DEFAUT),
                         contrat: Optional[str] = Form(None)) -> Dict[str, Any]:
    """Exécute la passe. `contrat` est le JSON rendu par `/propose`, éventuellement
    édité par l'utilisateur ; sans lui, il est inféré de `usage`."""
    name = _verifier(file)
    content = await file.read()
    _verifier_taille(content)
    if contrat is None and usage not in contract_infer.USAGES:
        raise HTTPException(status_code=400, detail=f"Usage inconnu : {usage!r}")

    declare: Optional[Dict[str, Any]] = None
    if contrat:
        import json
        try:
            declare = json.loads(contrat)
            # Validé tout de suite : un contrat mal formé doit échouer ici, avec
            # un 400 qui dit quoi corriger, et pas plus loin dans la passe.
            contract.Contrat.from_dict(declare)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Contrat invalide : {e}")

    try:
        return await agent_contrat.run(content, name, usage=usage, contrat=declare)
    except HTTPException:
        raise
    except ValueError as e:
        # Un fichier sans ligne de données est un état légitime avant le
        # premier chargement et catastrophique après. L'outil ne peut pas
        # distinguer les deux, donc il le dit au lieu de rendre une erreur 500
        # qui ferait croire à une panne de notre côté.
        raise HTTPException(status_code=400, detail=_sans_donnees(name, e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur de traitement : {e}")
