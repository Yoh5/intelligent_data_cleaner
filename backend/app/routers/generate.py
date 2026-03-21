from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
import ast
import textwrap

router = APIRouter(prefix="/generate", tags=["code-generation"])


class CleaningStep(BaseModel):
    column: Optional[str]
    issue_type: str
    strategy_name: str
    code: str


class CodeGenerationRequest(BaseModel):
    dataset_name: str
    steps: List[CleaningStep]


class CodeGenerationResponse(BaseModel):
    script: str
    filename: str


@router.post("/", response_model=CodeGenerationResponse)
async def generate_cleaning_script(request: CodeGenerationRequest):
    """Génère un script Python validé et exécutable."""
    
    # OPTIMIZATION: Valider chaque step avant génération
    validated_steps = []
    for step in request.steps:
        validated_code = _validate_and_fix_code(step.code, step.column, step.issue_type)
        validated_steps.append({
            **step.dict(),
            'code': validated_code
        })
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"clean_{request.dataset_name.split('.')[0]}_{timestamp}.py"
    
    script = _build_script(request, validated_steps)
    
    # OPTIMIZATION: Vérifier syntaxe Python
    try:
        ast.parse(script)
    except SyntaxError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Generated code has syntax error: {str(e)}"
        )
    
    return CodeGenerationResponse(
        script=script,
        filename=filename
    )


def _validate_and_fix_code(code: str, column: Optional[str], issue_type: str) -> str:
    """
    Valide et corrige le code généré pour s'assurer qu'il est exécutable.
    """
    if not code or not code.strip():
        return "# TODO: Add cleaning code here"
    
    # Nettoyer le code
    code = code.strip()
    
    # Vérifier que ce n'est pas juste un commentaire ou un print
    if code.startswith('#') or code.startswith('print('):
        return f"# WARNING: Non-executable code replaced\n# Original: {code}\ndf['{column}'] = df['{column}']"
    
    # Vérifier que df est modifié (assignation)
    if 'df[' not in code and 'df.' not in code and 'df =' not in code:
        # Le code ne modifie probablement pas df, l'encapsuler
        if issue_type == 'missing' and column:
            code = f"df['{column}'] = df['{column}'].fillna(method='ffill')  # Auto-fixed"
        elif issue_type == 'duplicate':
            code = "df = df.drop_duplicates()  # Auto-fixed"
    
    # S'assurer que les conversions numériques utilisent errors='coerce'
    if '.median()' in code or '.mean()' in code:
        if 'pd.to_numeric' not in code and column:
            # Ajouter conversion sécurisée avant l'opération
            code = f"df['{column}'] = pd.to_numeric(df['{column}'], errors='coerce')\n{code}"
    
    return code


def _build_script(request: CodeGenerationRequest, validated_steps: List[dict]) -> str:
    """Construit le script final avec gestion d'erreurs."""
    
    steps_code = "\n\n".join([
        f"""    # Step {i+1}: {step['issue_type']} - {step['strategy_name']}
    # Column: {step['column'] or 'N/A'}
    try:
{indent_code(step['code'], 8)}
    except Exception as e:
        logger.warning(f"Step {i+1} failed: {{e}}")
        # Continue anyway"""
        for i, step in enumerate(validated_steps)
    ])
    
    script = f'''#!/usr/bin/env python3
"""
Script de nettoyage de données généré automatiquement
Source: {request.dataset_name}
Generated: {datetime.now().isoformat()}
Steps: {len(request.steps)}
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging

# Configuration logging
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
        # Détection automatique de l'encoding
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


def detect_and_clean_missing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Détecte et nettoie les valeurs manquantes cachées (strings vides, 'NA', etc.)
    avant le traitement principal.
    """
    missing_patterns = ['', ' ', '  ', 'NA', 'N/A', 'null', 'NULL', 'None', 'NaN']
    
    for col in df.select_dtypes(include=['object']).columns:
        mask = df[col].isin(missing_patterns) | df[col].astype(str).str.strip().eq('')
        if mask.any():
            df.loc[mask, col] = np.nan
            logger.info(f"Converti {{mask.sum()}} valeurs vides en NaN dans '{{col}}'")
    
    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pipeline de nettoyage principal.
    Dataset initial: {len(request.steps)} étapes définies
    """
    logger.info(f"Données initiales: {{len(df)}} lignes, {{len(df.columns)}} colonnes")
    
    # Backup
    df_clean = df.copy()
    
    # OPTIMIZATION: Pré-traitement des valeurs manquantes cachées
    df_clean = detect_and_clean_missing(df_clean)
    
    # Application des étapes
{steps_code}
    
    # Log des résultats
    final_missing = df_clean.isna().sum().sum()
    logger.info(f"Nettoyage terminé: {{final_missing}} valeurs manquantes restantes")
    logger.info(f"Données finales: {{len(df_clean)}} lignes, {{len(df_clean.columns)}} colonnes")
    
    return df_clean


def save_data(df: pd.DataFrame, output_path: str):
    """Sauvegarde avec vérification."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Sauvegarde dans {{output_path}}")
    df.to_csv(output_path, index=False, encoding='utf-8')
    
    # Vérification
    saved_df = pd.read_csv(output_path)
    logger.info(f"Vérification: {{len(saved_df)}} lignes sauvegardées")


def generate_report(df_before: pd.DataFrame, df_after: pd.DataFrame) -> dict:
    """Rapport comparatif détaillé."""
    report = {{
        "lignes_avant": len(df_before),
        "lignes_apres": len(df_after),
        "lignes_supprimees": len(df_before) - len(df_after),
        "colonnes": len(df_after.columns),
        "valeurs_manquantes_avant": int(df_before.isna().sum().sum()),
        "valeurs_manquantes_apres": int(df_after.isna().sum().sum()),
        "modifications_appliquees": {len(request.steps)}
    }}
    
    logger.info("=" * 50)
    logger.info("RAPPORT DE NETTOYAGE")
    for key, value in report.items():
        logger.info(f"  {{key}}: {{value}}")
    logger.info("=" * 50)
    
    return report


def validate_output(df: pd.DataFrame) -> bool:
    """Validation finale des données."""
    issues = []
    
    if df.empty:
        issues.append("Dataset vide après nettoyage")
    
    if df.isna().all().any():
        cols = df.columns[df.isna().all()].tolist()
        issues.append(f"Colonnes entièrement vides: {{cols}}")
    
    if issues:
        for issue in issues:
            logger.error(f"Validation failed: {{issue}}")
        return False
    
    return True


def main():
    # Configuration - MODIFIEZ CES VALEURS
    INPUT_FILE = "{request.dataset_name}"
    OUTPUT_FILE = "cleaned_" + Path(INPUT_FILE).stem + ".csv"
    
    try:
        # 1. Chargement
        df_raw = load_data(INPUT_FILE)
        
        # 2. Nettoyage
        df_clean = clean_data(df_raw)
        
        # 3. Validation
        if not validate_output(df_clean):
            logger.error("Validation échouée - arrêt")
            return
        
        # 4. Rapport
        report = generate_report(df_raw, df_clean)
        
        # 5. Sauvegarde
        save_data(df_clean, OUTPUT_FILE)
        
        print(f"\\n✅ Nettoyage terminé avec succès")
        print(f"   Fichier sortie: {{OUTPUT_FILE}}")
        print(f"   Lignes traitées: {{report['lignes_apres']}}")
        
    except Exception as e:
        logger.error(f"Erreur fatale: {{e}}")
        raise


if __name__ == "__main__":
    main()
'''
    
    return script


def indent_code(code: str, spaces: int) -> str:
    """Indente le code proprement."""
    lines = code.strip().split('\n')
    indent = ' ' * spaces
    return '\n'.join(indent + line for line in lines)