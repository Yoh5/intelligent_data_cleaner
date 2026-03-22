#!/usr/bin/env python3
"""
Router API pour les suggestions de nettoyage.
Version optimisée avec gestion robuste des erreurs.
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
import pandas as pd
import numpy as np
import traceback

router = APIRouter(prefix="/suggest", tags=["suggestions"])

class IssueInput(BaseModel):
    type: str
    column: Optional[str] = None
    severity: str = "medium"
    description: Optional[str] = None
    affected_rows: Optional[int] = None
    count: Optional[int] = None
    rate: Optional[float] = None
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
    confidence: str  # 'high', 'medium', 'low'

class BatchSuggestionResponse(BaseModel):
    results: List[Dict[str, Any]]
    total_issues: int
    total_strategies: int
    processing_time_ms: Optional[float] = None


@router.post("/batch", response_model=BatchSuggestionResponse)
async def get_suggestions_batch(request: SuggestionRequest):
    """
    Génère des suggestions pour tous les problèmes détectés.
    Version ultra-robuste avec fallbacks.
    """
    import time
    start_time = time.time()

    try:
        # Reconstruction du DataFrame échantillon
        df_sample = None
        if request.sample_data:
            try:
                # Convertir les données pour pandas
                converted_data = {}
                for col, values in request.sample_data.items():
                    # Gérer les types mixtes
                    converted = []
                    for v in values:
                        if v is None or (isinstance(v, float) and np.isnan(v)):
                            converted.append(None)
                        else:
                            converted.append(v)
                    converted_data[col] = converted

                df_sample = pd.DataFrame(converted_data)
            except Exception as e:
                print(f"Warning: Impossible de reconstruire le DataFrame échantillon: {e}")
                df_sample = pd.DataFrame()

        if df_sample is None:
            df_sample = pd.DataFrame()

        # Générer les stratégies pour chaque issue
        results = []
        total_strategies = 0

        for issue in request.issues:
            try:
                strategies = _generate_strategies_for_issue(
                    issue, 
                    request.column_types.get(issue.column, 'unknown'),
                    df_sample
                )

                results.append({
                    'issue': {
                        'type': issue.type,
                        'column': issue.column,
                        'severity': issue.severity,
                        'description': issue.description or f"Problème {issue.type} dans {issue.column}",
                        'affected_rows': issue.affected_rows,
                        'count': issue.count,
                        'rate': issue.rate
                    },
                    'strategies': strategies,
                    'recommended': 0  # Première stratégie recommandée
                })

                total_strategies += len(strategies)

            except Exception as e:
                print(f"Error generating strategies for issue {issue}: {e}")
                # Fallback: stratégie générique
                results.append({
                    'issue': {
                        'type': issue.type,
                        'column': issue.column,
                        'severity': issue.severity,
                        'description': issue.description or f"Problème {issue.type}",
                    },
                    'strategies': [_get_generic_strategy(issue.type, issue.column)],
                    'recommended': 0
                })
                total_strategies += 1

        processing_time = (time.time() - start_time) * 1000

        return {
            'results': results,
            'total_issues': len(results),
            'total_strategies': total_strategies,
            'processing_time_ms': round(processing_time, 2)
        }

    except Exception as e:
        print(f"ERREUR CRITIQUE SUGGESTIONS: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Suggestion error: {str(e)}")


def _generate_strategies_for_issue(issue: IssueInput, column_type: str, df_sample: pd.DataFrame) -> List[Dict]:
    """Génère des stratégies spécifiques selon le type de problème."""

    strategies = []
    col = issue.column

    if issue.type in ['missing', 'missing_values']:
        # Stratégie 1: Imputation médiane (pour numériques)
        strategies.append({
            'name': 'Imputation par médiane',
            'description': f'Remplacer les valeurs manquantes de "{col}" par la médiane (robust aux outliers)',
            'pros': ['Robustesse', 'Préserve la distribution', 'Ne supprime pas de données'],
            'cons': ['Réduit la variance', 'Peut biaiser si trop de missing'],
            'code_preview': f"""# Imputation médiane pour '{col}'
if '{col}' in df.columns:
    # Conversion sécurisée en numérique
    df['{col}'] = pd.to_numeric(df['{col}'], errors='coerce')

    # Calcul et imputation de la médiane
    median_val = df['{col}'].median()
    if pd.notna(median_val):
        missing_count = df['{col}'].isna().sum()
        df['{col}'] = df['{col}'].fillna(median_val)
        logger.info(f"✓ Imputé {{missing_count}} valeurs par médiane {{median_val:.2f}}")
    else:
        df['{col}'] = df['{col}'].fillna(0)
        logger.warning(f"⚠ Médiane indisponible, remplacement par 0")""",
            'confidence': 'high' if column_type in ['int64', 'float64', 'numeric'] else 'medium'
        })

        # Stratégie 2: Imputation par mode (pour catégoriels)
        strategies.append({
            'name': 'Imputation par mode (valeur la plus fréquente)',
            'description': f'Remplacer les valeurs manquantes de "{col}" par la valeur la plus fréquente',
            'pros': ['Préserve les catégories existantes', 'Simple à comprendre'],
            'cons': ['Peut créer un déséquilibre', 'Moins adapté aux données continues'],
            'code_preview': f"""# Imputation par mode pour '{col}'
if '{col}' in df.columns:
    mode_series = df['{col}'].mode()
    if len(mode_series) > 0:
        mode_val = mode_series[0]
        missing_count = df['{col}'].isna().sum()
        df['{col}'] = df['{col}'].fillna(mode_val)
        logger.info(f"✓ Imputé {{missing_count}} valeurs par mode '{{mode_val}}'")
    else:
        df['{col}'] = df['{col}'].fillna('Unknown')
        logger.info(f"✓ Valeur 'Unknown' utilisée (mode indisponible)")""",
            'confidence': 'high' if column_type == 'object' else 'low'
        })

        # Stratégie 3: Forward fill puis backward fill
        strategies.append({
            'name': 'Remplissage par propagation (ffill/bfill)',
            'description': 'Propager les valeurs valides vers les cases vides adjacentes',
            'pros': ['Préserve l\'ordre temporel', 'Adapté aux séries temporelles'],
            'cons': ['Inefficace si longues séquences de missing', 'Peut propager des erreurs'],
            'code_preview': f"""# Propagation pour '{col}'
if '{col}' in df.columns:
    missing_before = df['{col}'].isna().sum()
    df['{col}'] = df['{col}'].ffill().bfill()
    missing_after = df['{col}'].isna().sum()
    filled = missing_before - missing_after
    logger.info(f"✓ {{filled}} valeurs propagées, {{missing_after}} restantes")""",
            'confidence': 'medium'
        })

    elif issue.type in ['inconsistent', 'mixed_types']:
        # Conversion de type
        strategies.append({
            'name': 'Conversion forcée en numérique',
            'description': f'Convertir "{col}" en type numérique avec gestion des erreurs',
            'pros': ['Permet les calculs statistiques', 'Réduit la taille mémoire'],
            'cons': ['Perte d\'informations textuelles', 'Valeurs invalides -> NaN'],
            'code_preview': f"""# Conversion numérique pour '{col}'
if '{col}' in df.columns:
    # Sauvegarder le type original
    original_dtype = df['{col}'].dtype
    original_non_null = df['{col}'].notna().sum()

    # Nettoyer les formats courants
    df['{col}'] = df['{col}'].astype(str).str.replace(' ', '', regex=False)
    df['{col}'] = df['{col}'].str.replace(',', '.', regex=False)  # Format français
    df['{col}'] = df['{col}'].str.replace('€', '', regex=False)
    df['{col}'] = df['{col}'].str.replace('$', '', regex=False)
    df['{col}'] = df['{col}'].str.replace('%', '', regex=False)

    # Conversion
    df['{col}'] = pd.to_numeric(df['{col}'], errors='coerce')

    new_non_null = df['{col}'].notna().sum()
    lost = original_non_null - new_non_null

    logger.info(f"✓ Converti '{col}': {{original_dtype}} -> float64")
    logger.info(f"  {{new_non_null}}/{{original_non_null}} valeurs conservées ({{lost}} perdues)")""",
            'confidence': 'high'
        })

    elif issue.type in ['duplicate', 'duplicate_ids']:
        strategies.append({
            'name': 'Suppression des doublons',
            'description': f'Regarder et optionnellement supprimer les doublons sur "{col}"',
            'pros': ['Améliore la qualité des données', 'Évite les biais d\'analyse'],
            'cons': ['Perte potentielle de données', 'Vérification manuelle recommandée'],
            'code_preview': f"""# Gestion des doublons pour '{col}'
if '{col}' in df.columns:
    duplicates = df.duplicated(subset=['{col}'], keep=False)
    dup_count = duplicates.sum()

    if dup_count > 0:
        logger.warning(f"⚠ {{dup_count}} doublons détectés dans '{col}'")
        # Afficher quelques exemples
        dup_examples = df[duplicates].head(10)
        logger.info(f"Exemples: {{dup_examples['{col}'].tolist()}}")

        # Option: suppression (décommenter si souhaité)
        # df = df.drop_duplicates(subset=['{col}'], keep='first')
        # logger.info(f"✓ Doublons supprimés, {{len(df)}} lignes restantes")
    else:
        logger.info(f"✓ Aucun doublon dans '{col}'")""",
            'confidence': 'high'
        })

    else:
        # Stratégie générique
        strategies.append(_get_generic_strategy(issue.type, col))

    return strategies


def _get_generic_strategy(issue_type: str, column: Optional[str]) -> Dict:
    """Retourne une stratégie générique quand le type spécifique est inconnu."""
    return {
        'name': 'Nettoyage automatique (générique)',
        'description': f'Appliquer un nettoyage standard sur "{column or "le dataset"}"',
        'pros': ['Sûr', 'Ne supprime pas de données'],
        'cons': ['Peut ne pas résoudre tous les problèmes', 'Nécessite validation'],
        'code_preview': f"""# Nettoyage générique pour '{column or "dataset"}'
if '{column}' in df.columns:
    # Standardisation basique
    df['{column}'] = df['{column}'].astype(str).str.strip()

    # Remplacer valeurs vides
    empty_mask = df['{column}'].isin(['', 'nan', 'None', 'null', 'NA'])
    if empty_mask.any():
        df.loc[empty_mask, '{column}'] = np.nan
        logger.info(f"✓ {{empty_mask.sum()}} valeurs vides standardisées")

    logger.info(f"✓ Nettoyage générique appliqué à '{column}'")""" if column else """# Nettoyage générique du dataset
# Standardisation des noms de colonnes
df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
logger.info(f"✓ Noms de colonnes standardisés")

# Détection des types
for col in df.select_dtypes(include=['object']).columns:
    # Essayer conversion numérique
    try:
        converted = pd.to_numeric(df[col], errors='coerce')
        if converted.notna().sum() / len(df) > 0.8:
            df[col] = converted
            logger.info(f"✓ {{col}} converti en numérique")
    except:
        pass""",
        'confidence': 'low'
    }
