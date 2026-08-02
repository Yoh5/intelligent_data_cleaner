"""Endpoint agentique : nettoyage autonome piloté par un objectif de qualité.

Contrairement à `/auto-clean` (une passe : analyse → choix → script), `/agent-clean`
lance une VRAIE boucle d'agent : mesure la qualité → nettoie → re-mesure → itère
jusqu'à la cible, en gardant la meilleure version. Renvoie la trace, le CSV nettoyé
et un script pandas reproductible.
"""
from fastapi import APIRouter, File, HTTPException, UploadFile

from app.services import agent_cleaner

router = APIRouter(prefix="/agent-clean", tags=["agent-clean"])

_ALLOWED = {".csv", ".xlsx", ".xls"}


@router.post("/")
async def agent_clean(file: UploadFile = File(...)):
    name = file.filename or ""
    ext = name.lower()[name.rfind("."):] if "." in name else ""
    if ext not in _ALLOWED:
        raise HTTPException(status_code=400, detail=f"Unsupported file type: {ext}")
    content = await file.read()
    if len(content) > 100 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="File too large (max 100MB)")
    try:
        return await agent_cleaner.run(content, name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Agent error: {e}")
