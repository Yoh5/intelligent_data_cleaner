#!/usr/bin/env python3
"""
Router API pour la génération de code de nettoyage.
VERSION CORRIGÉE - Ordre des étapes: Conversion AVANT Imputation
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime
import ast
import textwrap
import traceback

router = APIRouter(prefix="/generate", tags=["code-generation"])

class CleaningStep(BaseModel):
    column: Optional[str] = None
    issue_type: str
    strategy_name: str
    code: str = ""

class CodeGenerationRequest(BaseModel):
    dataset_name: str
    steps: List[CleaningStep]

class CodeGenerationResponse(BaseModel):
    script: str
    filename: str


@router.post("/", response_model=CodeGenerationResponse)
async def generate_cleaning_script(request: CodeGenerationRequest):
    """Génère un script Python validé et exécutable."""
    
    print(f"\n[GENERATE] Requête reçue: {len(request.steps)} étapes")
    for i, step in enumerate(request.steps):
        print(f"  Step {i+1}: {step.issue_type} on {step.column}")

    try:
        if not request.steps:
            raise HTTPException(status_code=400, detail="Aucune étape fournie")

        # CORRECTION: Trier les étapes par priorité (conversion avant imputation)
        sorted_steps = _sort_steps_by_priority(request.steps)
        
        # Validation et correction des étapes
        validated_steps = []
        for i, step in enumerate(sorted_steps):
            try:
                validated_code = _validate_and_fix_code(step.code, step.column, step.issue_type)
                validated_steps.append({
                    'column': step.column,
                    'issue_type': step.issue_type,
                    'strategy_name': step.strategy_name,
                    'code': validated_code,
                    'index': i
                })
                print(f"[GENERATE] Étape {i+1} validée: {step.issue_type} sur {step.column}")
            except Exception as e:
                print(f"[GENERATE] Erreur validation étape {i+1}: {e}")
                validated_steps.append({
                    'column': step.column,
                    'issue_type': step.issue_type,
                    'strategy_name': step.strategy_name,
                    'code': f"# Étape ignorée suite à erreur: {e}\n",
                    'index': i
                })

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"clean_{request.dataset_name.split('.')[0]}_{timestamp}.py"

        # Construction du script
        script = _build_script(request, validated_steps)

        # Validation AST
        try:
            ast.parse(script)
            print("[GENERATE] Syntaxe AST validée")
        except SyntaxError as e:
            print(f"[GENERATE] Erreur de syntaxe: {e}")
            print("[GENERATE] Bascule vers fallback...")
            script = _build_fallback_script(request)
            try:
                ast.parse(script)
            except SyntaxError:
                raise HTTPException(status_code=500, detail="Impossible de générer un script valide")

        print(f"[GENERATE] Script généré: {len(script)} caractères")
        
        return CodeGenerationResponse(script=script, filename=filename)

    except HTTPException:
        raise
    except Exception as e:
        print(f"[GENERATE] Erreur inattendue: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Erreur génération: {str(e)}")


def _sort_steps_by_priority(steps: List[CleaningStep]) -> List[CleaningStep]:
    """
    Trie les étapes pour que la conversion de type soit AVANT l'imputation.
    mixed_types/inconsistent (priorité 1) doit être avant missing (priorité 10)
    """
    def get_priority(step):
        issue_type = step.issue_type.lower()
        if issue_type in ['mixed_types', 'inconsistent', 'type_conversion']:
            return 1  # Conversion en premier
        elif issue_type in ['missing', 'missing_values']:
            return 10  # Imputation après
        elif issue_type == 'outlier':
            return 20  # Outliers après imputation
        else:
            return 5
    
    return sorted(steps, key=get_priority)


def _validate_and_fix_code(code: str, column: Optional[str], issue_type: str) -> str:
    """Valide et corrige le code généré."""
    if not code or not code.strip():
        # Générer un code par défaut selon le type
        if issue_type in ['missing', 'missing_values'] and column:
            return f"""# Imputation pour {column} (APRÈS conversion type si nécessaire)
if df_clean['{column}'].isna().any():
    if pd.api.types.is_numeric_dtype(df_clean['{column}']):
        median_val = df_clean['{column}'].median()
        if pd.notna(median_val):
            df_clean['{column}'] = df_clean['{column}'].fillna(median_val)
            logger.info(f"Imputation médiane {column}: {{median_val:.2f}}")
        else:
            df_clean['{column}'] = df_clean['{column}'].fillna(0)
    else:
        mode_val = df_clean['{column}'].mode()
        if len(mode_val) > 0:
            df_clean['{column}'] = df_clean['{column}'].fillna(mode_val[0])
            logger.info(f"Imputation mode {column}: {{mode_val[0]}}")"""
        
        elif issue_type == 'duplicate':
            return "df_clean = df_clean.drop_duplicates()"
        
        elif issue_type in ['mixed_types', 'inconsistent'] and column:
            return f"""# Conversion type pour {column} (AVANT imputation)
df_clean['{column}'] = df_clean['{column}'].replace(r'^\\s*$', np.nan, regex=True)
df_clean['{column}'] = pd.to_numeric(df_clean['{column}'], errors='coerce')
logger.info(f"Conversion {column} en numérique")"""
        
        else:
            return f"# TODO: {issue_type} sur {column or 'dataset'}"

    code = code.strip()

    # CORRECTION CRITIQUE: Remplacer df['col'] par df_clean['col'] pour éviter de modifier le df original
    if 'df[' in code and 'df_clean[' not in code:
        code = code.replace('df[', 'df_clean[')
    
    # Vérifier que ce n'est pas juste un commentaire
    if code.startswith('#') or code.startswith('print('):
        return f"# WARNING: Code non-exécutable remplacé\npass"

    return code


def _build_script(request: CodeGenerationRequest, validated_steps: List[Dict[str, Any]]) -> str:
    """Construit le script final avec indentation CORRIGÉE."""
    
    steps_code_lines = []
    
    for i, step in enumerate(validated_steps):
        code_block = step['code'].strip()
        if not code_block:
            continue

        # Indenter le code utilisateur correctement (8 espaces pour être dans le try)
        indented_user_code = textwrap.indent(code_block, '        ')
        
        # Assembler le bloc avec bonne variable df_clean
        step_block = f"""    # Step {i+1}: {step['issue_type']} - {step['strategy_name']}
    # Column: {step['column'] or 'N/A'}
    try:
{indented_user_code}
    except Exception as e:
        logger.warning(f"Step {i+1} failed: {{e}}")"""
        
        steps_code_lines.append(step_block)

    steps_code = "\n\n".join(steps_code_lines) if steps_code_lines else "    # Aucune étape spécifique"
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    script = f'''#!/usr/bin/env python3
"""
Script de nettoyage de données généré automatiquement
Source: {request.dataset_name}
Generated: {timestamp}
Steps: {len(request.steps)}
Ordre: Conversion de type → Imputation → Outliers
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_data(filepath: str) -> pd.DataFrame:
    """Charge le dataset avec détection automatique."""
    logger.info(f"Chargement de {{filepath}}")
    
    path = Path(filepath)
    suffix = path.suffix.lower()
    
    if suffix == '.csv':
        for encoding in ['utf-8', 'latin-1', 'iso-8859-1']:
            try:
                df = pd.read_csv(filepath, encoding=encoding)
                logger.info(f"Encoding détecté: {{encoding}}")
                return df
            except UnicodeDecodeError:
                continue
        raise ValueError("Impossible de décoder le fichier CSV")
    
    elif suffix in ['.xlsx', '.xls']:
        return pd.read_excel(filepath)
    
    else:
        raise ValueError(f"Format non supporté: {{suffix}}")

def detect_hidden_missing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Détecte et nettoie les valeurs manquantes cachées avant traitement.
    """
    missing_patterns = ['', ' ', '  ', '   ', 'NA', 'N/A', 'null', 'NULL', 'None', 'NaN']
    
    for col in df.select_dtypes(include=['object']):
        mask = df[col].isin(missing_patterns) | df[col].astype(str).str.strip().eq('')
        if mask.any():
            df.loc[mask, col] = np.nan
            logger.info(f"Converti {{mask.sum()}} valeurs vides en NaN dans '{{col}}'")
    
    return df

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pipeline de nettoyage principal.
    ORDRE CRITIQUE: Conversion type → Imputation → Outliers
    """
    logger.info(f"Données initiales: {{len(df)}} lignes, {{len(df.columns)}} colonnes")
    
    # Backup
    df_clean = df.copy()
    
    # Pré-traitement des valeurs manquantes cachées
    df_clean = detect_hidden_missing(df_clean)
    
    # ETAPES DE NETTOYAGE (triées: conversion avant imputation)
{steps_code}
    
    # Vérification finale
    final_missing = df_clean.isna().sum().sum()
    logger.info(f"Nettoyage terminé: {{final_missing}} valeurs manquantes restantes")
    
    if final_missing > 0:
        cols_with_missing = df_clean.columns[df_clean.isna().any()].tolist()
        logger.warning(f"Colonnes avec valeurs manquantes: {{cols_with_missing}}")
    
    return df_clean

def save_data(df: pd.DataFrame, output_path: str):
    """Sauvegarde avec vérification."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Sauvegarde dans {{output_path}}")
    df.to_csv(output_path, index=False, encoding='utf-8')

def main():
    INPUT_FILE = "{request.dataset_name}"
    OUTPUT_FILE = "cleaned_" + Path(INPUT_FILE).stem + ".csv"
    
    try:
        df_raw = load_data(INPUT_FILE)
        df_clean = clean_data(df_raw)
        save_data(df_clean, OUTPUT_FILE)
        
        print(f"\\n✅ Nettoyage terminé")
        print(f"   Fichier: {{OUTPUT_FILE}}")
        print(f"   Lignes: {{len(df_clean)}}")
        
    except Exception as e:
        logger.error(f"Erreur fatale: {{e}}")
        raise

if __name__ == "__main__":
    main()
'''
    return script


def _build_fallback_script(request: CodeGenerationRequest) -> str:
    """Script de secours minimal."""
    return f'''#!/usr/bin/env python3
import pandas as pd
import numpy as np

def clean_data(input_file: str, output_file: str):
    df = pd.read_csv(input_file)
    
    # Nettoyage basique
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].replace(r'^\\s*$', np.nan, regex=True)
    
    df.to_csv(output_file, index=False)
    print(f"Nettoyage basique: {{len(df)}} lignes")

if __name__ == "__main__":
    clean_data("{request.dataset_name}", "cleaned_{request.dataset_name.split('.')[0]}.csv")
'''