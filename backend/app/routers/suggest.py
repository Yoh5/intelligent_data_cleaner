#!/usr/bin/env python3
"""
Router API pour les suggestions de nettoyage.
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import pandas as pd
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/suggest", tags=["suggestions"])

class IssueInput(BaseModel):
    type: str
    column: Optional[str] = None
    severity: str = "medium"
    description: Optional[str] = None
    semantic_type: Optional[str] = None

class SuggestionRequest(BaseModel):
    dataset_name: str
    column_types: Dict[str, str]
    issues: List[IssueInput]
    sample_data: Optional[Dict[str, List[Any]]] = None

class CleaningStrategy(BaseModel):
    name: str
    description: str
    pros: List[str]
    cons: List[str]
    code_preview: str
    confidence: str


@router.post("/batch")
async def get_suggestions_batch(request: SuggestionRequest):
    """
    Génère des suggestions pour tous les problèmes détectés.
    """
    try:
        logger.info(f"Génération de suggestions pour {len(request.issues)} problèmes")

        results = []
        for issue in request.issues:
            strategies = _generate_strategies_for_issue(issue)
            results.append({
                "issue": {
                    "type": issue.type,
                    "column": issue.column,
                    "severity": issue.severity,
                    "description": issue.description or f"Problème {issue.type}"
                },
                "strategies": strategies,
                "recommended": 0
            })

        return {
            "results": results,
            "total_issues": len(results),
            "total_strategies": sum(len(r["strategies"]) for r in results)
        }

    except Exception as e:
        logger.error(f"Erreur suggestions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _generate_strategies_for_issue(issue: IssueInput) -> List[Dict]:
    """Génère des stratégies pour un problème spécifique."""
    strategies = []
    col = issue.column or "colonne"

    if issue.type in ["missing", "missing_values"]:
        strategies.append({
            "name": "Imputation par médiane",
            "description": f"Remplacer les valeurs manquantes de '{col}' par la médiane",
            "pros": ["Robust aux outliers", "Préserve la distribution"],
            "cons": ["Réduit la variance"],
            "code_preview": f"""if '{col}' in df.columns:
    df['{col}'] = pd.to_numeric(df['{col}'], errors='coerce')
    df['{col}'] = df['{col}'].fillna(df['{col}'].median())""",
            "confidence": "high"
        })

        strategies.append({
            "name": "Imputation par mode",
            "description": f"Remplacer par la valeur la plus fréquente",
            "pros": ["Adapté aux catégoriels"],
            "cons": ["Peu adapté aux numériques continus"],
            "code_preview": f"""if '{col}' in df.columns:
    mode_val = df['{col}'].mode()[0]
    df['{col}'] = df['{col}'].fillna(mode_val)""",
            "confidence": "medium"
        })

    elif issue.type == "duplicate":
        strategies.append({
            "name": "Suppression des doublons",
            "description": "Supprimer les lignes dupliquées",
            "pros": ["Améliore la qualité", "Réduit la taille"],
            "cons": ["Perte potentielle d'informations"],
            "code_preview": "df = df.drop_duplicates()",
            "confidence": "high"
        })

    elif issue.type in ["inconsistent", "mixed_types"]:
        strategies.append({
            "name": "Conversion numérique",
            "description": f"Convertir '{col}' en format numérique",
            "pros": ["Permet les calculs", "Standardise le format"],
            "cons": ["Perte des valeurs textuelles"],
            "code_preview": f"""if '{col}' in df.columns:
    df['{col}'] = df['{col}'].astype(str).str.replace(',', '.')
    df['{col}'] = pd.to_numeric(df['{col}'], errors='coerce')""",
            "confidence": "high"
        })

    else:
        strategies.append({
            "name": "Nettoyage automatique",
            "description": "Appliquer un nettoyage standard",
            "pros": ["Sûr", "Automatique"],
            "cons": ["Générique"],
            "code_preview": f"""# Nettoyage pour {col}
if '{col}' in df.columns:
    df['{col}'] = df['{col}'].astype(str).str.strip()""",
            "confidence": "medium"
        })

    return strategies
