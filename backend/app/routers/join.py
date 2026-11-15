"""« Ces deux fichiers sont-ils joignables ? » — pour l'interface.

Deux routes, pour la même raison que `/contract-clean/` en a deux : on montre
d'abord ce sur quoi on peut joindre (`/join/colonnes`), puis on rend le
diagnostic (`/join/`). Rien n'est modifié ni joint : on dit ce que la jointure
ferait. La logique vit dans `services/jointure.py`, partagée avec la commande.
"""
from typing import Any, Dict

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from app.services import jointure

router = APIRouter(prefix="/join", tags=["join"])

_ALLOWED = {".csv", ".xlsx", ".xls"}
_MAX_MO = 25


async def _lire(file: UploadFile):
    nom = file.filename or ""
    ext = nom.lower()[nom.rfind("."):] if "." in nom else ""
    if ext not in _ALLOWED:
        raise HTTPException(status_code=400, detail=f"Format non pris en charge : {ext or '(aucun)'}")
    contenu = await file.read()
    if len(contenu) > _MAX_MO * 1024 * 1024:
        raise HTTPException(status_code=413,
                            detail=f"« {nom} » dépasse {_MAX_MO} Mo.")
    try:
        return await jointure.lire_en_texte(contenu, nom)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"« {nom} » illisible : {e}")


@router.post("/colonnes")
async def colonnes(file: UploadFile = File(...)) -> Dict[str, Any]:
    """Les colonnes d'un fichier, sous le nom que la clé devra employer."""
    df = await _lire(file)
    return {"colonnes": [str(c) for c in df.columns], "lignes": len(df)}


@router.post("/")
async def diagnostiquer(gauche: UploadFile = File(...), droite: UploadFile = File(...),
                        sur: str = Form(...),
                        couverture_min: float = Form(1.0)) -> Dict[str, Any]:
    """`sur` : colonnes de clé séparées par des virgules, `a=b` si les noms
    diffèrent d'un fichier à l'autre."""
    if not 0.0 <= couverture_min <= 1.0:
        raise HTTPException(status_code=400,
                            detail="couverture_min doit être comprise entre 0 et 1.")
    g, d = await _lire(gauche), await _lire(droite)
    try:
        cg, cd = jointure.lire_la_cle(sur.split(","))
        return jointure.diagnostiquer(g, d, cg, cd, couverture_min=couverture_min).to_dict()
    except jointure.CleInconnue as e:
        raise HTTPException(status_code=400, detail=str(e))
