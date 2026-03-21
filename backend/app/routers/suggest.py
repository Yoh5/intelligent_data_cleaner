#!/usr/bin/env python3
"""
Router API pour les suggestions de nettoyage.
La logique métier est dans services/suggestion_engine.py
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import pandas as pd
import io

router = APIRouter(prefix="/suggest", tags=["suggestions"])

class SuggestionRequest(BaseModel):
    dataset_name: str
    column_types: Dict[str, str]
    issues: List[Dict[str, Any]]
    sample_data: Optional[Dict[str, List[Any]]] = None

class CleaningStrategy(BaseModel):
    name: str
    description: str
    pros: List[str]
    cons: List[str]
    code_preview: str
    confidence: str  # 'high', 'medium', 'low'

class BatchSuggestionResponse(BaseModel):
    results: List[Dict[str, Any]]
    total_issues: int
    total_strategies: int

@router.post("/batch", response_model=BatchSuggestionResponse)
async def get_suggestions_batch(request: SuggestionRequest):
    """
    Génère des suggestions pour tous les problèmes détectés.
    """
    try:
        from app.services.suggestion_engine import generate_cleaning_strategy
        
        # Reconstruction d'un DataFrame échantillon si fourni
        df_sample = None
        if request.sample_data:
            try:
                df_sample = pd.DataFrame(request.sample_data)
            except:
                df_sample = None
        
        # Convertir les issues en profils
        profiles = {}
        for issue in request.issues:
            if issue['column']:
                profiles[issue['column']] = {
                    'missing_count': issue.get('count', 0),
                    'semantic_type': issue.get('semantic_type', 'unknown'),
                    'severity': issue.get('severity', 'low')
                }
        
        # Générer les stratégies
        strategy_result = generate_cleaning_strategy(
            profiles, 
            df_sample if df_sample is not None else pd.DataFrame(),
            use_llm=False
        )
        
        # Formater la réponse pour correspondre à l'attendu frontend
        results = []
        for step in strategy_result['steps']:
            results.append({
                'issue': {
                    'type': step['type'],
                    'column': step['column'],
                    'severity': 'high' if step['priority'] < 10 else 'medium'
                },
                'strategies': [{
                    'name': step['description'],
                    'description': step['description'],
                    'pros': ['Automatique', 'Sûr'],
                    'cons': [] if step['priority'] < 10 else ['Peut nécessiter validation'],
                    'code_preview': step['code'],
                    'confidence': 'high' if step['priority'] < 10 else 'medium'
                }],
                'recommended': 0
            })
        
        return {
            'results': results,
            'total_issues': len(results),
            'total_strategies': len(strategy_result['steps'])
        }
        
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Suggestion error: {str(e)}")