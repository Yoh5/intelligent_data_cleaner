#!/usr/bin/env python3
"""
Générateur de code - Version simplifiée et robuste.
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime
import ast
import textwrap
import logging

logger = logging.getLogger(__name__)

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
    validation_status: Dict[str, Any]


@router.post("/", response_model=CodeGenerationResponse)
async def generate_cleaning_script(request: CodeGenerationRequest):
    """Génère un script Python."""

    try:
        # Valider les étapes
        validated_steps = []
        for i, step in enumerate(request.steps):
            validated_code = _validate_and_fix_code(step.code, step.column, step.issue_type)
            validated_steps.append({
                "column": step.column,
                "issue_type": step.issue_type,
                "strategy_name": step.strategy_name,
                "code": validated_code,
                "step_number": i + 1
            })

        # Générer le script
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"clean_{request.dataset_name.replace('.', '_')}_{timestamp}.py"

        script = _build_script_simple(request, validated_steps)

        # Valider
        syntax_valid, syntax_error = _validate_syntax(script)
        if not syntax_valid:
            logger.error(f"Erreur syntaxe: {syntax_error}")
            raise HTTPException(status_code=500, detail=f"Syntax error: {syntax_error}")

        return CodeGenerationResponse(
            script=script,
            filename=filename,
            validation_status={"syntax_valid": True, "total_steps": len(validated_steps)}
        )

    except Exception as e:
        logger.error(f"Erreur: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _validate_and_fix_code(code: str, column: Optional[str], issue_type: str) -> str:
    if not code:
        return _fallback_code(column, issue_type)

    code = code.strip()

    # Fix pandas deprecated
    code = code.replace(".fillna(method='ffill')", ".ffill()")
    code = code.replace('.fillna(method="ffill")', ".ffill()")
    code = code.replace(".fillna(method='bfill')", ".bfill()")
    code = code.replace('.fillna(method="bfill")', ".bfill()")

    return code


def _fallback_code(column: Optional[str], issue_type: str) -> str:
    if issue_type == "missing" and column:
        return f"""if '{column}' in df.columns:
    df['{column}'] = pd.to_numeric(df['{column}'], errors='coerce')
    df['{column}'] = df['{column}'].fillna(df['{column}'].median())"""
    return "# Etape"


def _build_script_simple(request: CodeGenerationRequest, steps: List[dict]) -> str:
    """Construit le script de manière simple et sûre."""

    # Header
    script = """#!/usr/bin/env python3
# -*- coding: utf-8 -*-"""

    # Imports
    script += """

import pandas as pd
import numpy as np
from pathlib import Path
import logging
import sys
from typing import Optional, Union

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Config
INPUT_FILE: Optional[str] = None
OUTPUT_FILE: Optional[str] = None

def load_data(file_path: Union[str, Path]) -> pd.DataFrame:
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Fichier non trouve: {file_path}")
    logger.info(f"Chargement de {file_path}")
    try:
        df = pd.read_csv(file_path)
    except:
        df = pd.read_csv(file_path, sep=None, engine='python')
    logger.info(f"Charge: {len(df)} lignes, {len(df.columns)} colonnes")
    return df

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("\n" + "="*50)
    logger.info("DEBUT DU NETTOYAGE")
    logger.info("="*50)
    df_clean = df.copy()
    initial_rows = len(df_clean)
"""

    # Ajouter chaque étape
    for step in steps:
        step_num = step["step_number"]
        step_code = step["code"]

        script += f"""
    # ETAPE {step_num}: {step['issue_type']}
    try:
"""
        script += textwrap.indent(step_code, "        ")
        script += f"""
    except Exception as e:
        logger.error(f"Etape {step_num} echouee: {e}")
"""

    # Footer
    script += """
    logger.info("\n" + "="*50)
    logger.info("RESULTAT")
    logger.info("="*50)
    logger.info(f"Dimensions: {initial_rows} -> {len(df_clean)} lignes")
    return df_clean

def save_data(df: pd.DataFrame, output_path: Union[str, Path]) -> None:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.info(f"Sauvegarde: {output_path}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Nettoyage CSV')
    parser.add_argument('--input', '-i', help='Fichier CSV entree')
    parser.add_argument('--output', '-o', help='Fichier CSV sortie')
    args = parser.parse_args()

    input_file = args.input or INPUT_FILE
    if not input_file:
        parser.error("Veuillez specifier un fichier entree")

    output_file = args.output or OUTPUT_FILE or f"cleaned_{Path(input_file).stem}.csv"

    try:
        df_raw = load_data(input_file)
        df_clean = clean_data(df_raw)
        save_data(df_clean, output_file)
        logger.info("\n" + "="*50)
        logger.info("NETTOYAGE TERMINE ✓")
        logger.info("="*50)
    except Exception as e:
        logger.error(f"Erreur fatale: {e}")
        sys.exit(1)
"""

    return script


def _validate_syntax(code: str) -> tuple[bool, Optional[str]]:
    try:
        ast.parse(code)
        return True, None
    except SyntaxError as e:
        return False, f"Ligne {e.lineno}: {e.msg}"
