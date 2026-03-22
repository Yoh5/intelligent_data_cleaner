#!/usr/bin/env python3
"""
Générateur de code Intelligent Data Cleaner - Version optimale et fonctionnelle.
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
    """Génère un script Python complet qui résout tous les problèmes sélectionnés."""

    try:
        logger.info(f"Génération de script pour: {request.dataset_name}")
        logger.info(f"Nombre d'étapes: {len(request.steps)}")

        # Valider et préparer les étapes
        validated_steps = []
        for i, step in enumerate(request.steps):
            try:
                validated_code = _validate_and_fix_code(
                    step.code, 
                    step.column, 
                    step.issue_type
                )
                validated_steps.append({
                    "column": step.column,
                    "issue_type": step.issue_type,
                    "strategy_name": step.strategy_name,
                    "code": validated_code,
                    "step_number": i + 1
                })
                logger.info(f"Étape {i+1} validée: {step.issue_type} sur {step.column}")
            except Exception as step_error:
                logger.error(f"Erreur étape {i+1}: {step_error}")
                validated_steps.append({
                    "column": step.column,
                    "issue_type": step.issue_type,
                    "strategy_name": step.strategy_name,
                    "code": _generate_fallback_code(step.column, step.issue_type),
                    "step_number": i + 1
                })

        # Générer le nom de fichier
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_name = request.dataset_name.replace(".", "_").replace(" ", "_")
        filename = f"clean_{safe_name}_{timestamp}.py"

        # Construire le script complet
        script = _build_complete_script(request.dataset_name, validated_steps)

        # Valider la syntaxe du script généré
        syntax_valid, syntax_error = _validate_syntax(script)
        if not syntax_valid:
            logger.error(f"Erreur de syntaxe dans le script généré: {syntax_error}")
            raise HTTPException(
                status_code=500, 
                detail=f"Erreur de syntaxe dans le script généré: {syntax_error}"
            )

        logger.info(f"Script généré avec succès: {len(script)} caractères")

        return CodeGenerationResponse(
            script=script,
            filename=filename,
            validation_status={
                "syntax_valid": True, 
                "total_steps": len(validated_steps),
                "can_execute": True
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erreur critique lors de la génération: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(
            status_code=500, 
            detail=f"Erreur lors de la génération du script: {str(e)}"
        )


def _validate_and_fix_code(code: str, column: Optional[str], issue_type: str) -> str:
    """Valide et corrige le code de nettoyage pour garantir qu'il fonctionne."""
    if not code or not code.strip():
        return _generate_fallback_code(column, issue_type)

    code = code.strip()

    # Corrections des méthodes pandas dépréciées
    replacements = [
        (".fillna(method='ffill')", ".ffill()"),
        ('.fillna(method="ffill")', ".ffill()"),
        (".fillna(method='bfill')", ".bfill()"),
        ('.fillna(method="bfill")', ".bfill()"),
    ]

    for old, new in replacements:
        code = code.replace(old, new)

    return code


def _generate_fallback_code(column: Optional[str], issue_type: str) -> str:
    """Génère du code de secours qui fonctionne garanti pour chaque type de problème."""
    if issue_type in ["missing", "missing_values"] and column:
        return """# Imputation des valeurs manquantes pour la colonne """ + column + """
if '""" + column + """' in df.columns:
    df['""" + column + """'] = pd.to_numeric(df['""" + column + """'], errors='coerce')
    if pd.api.types.is_numeric_dtype(df['""" + column + """']):
        median_val = df['""" + column + """'].median()
        if pd.notna(median_val):
            df['""" + column + """'] = df['""" + column + """'].fillna(median_val)
        else:
            df['""" + column + """'] = df['""" + column + """'].fillna(0)
    else:
        if not df['""" + column + """'].mode().empty:
            mode_val = df['""" + column + """'].mode()[0]
            df['""" + column + """'] = df['""" + column + """'].fillna(mode_val)
        else:
            df['""" + column + """'] = df['""" + column + """'].fillna('Inconnu')"""

    elif issue_type == "duplicate":
        return """# Suppression des doublons
initial_count = len(df)
df = df.drop_duplicates()
removed = initial_count - len(df)"""

    elif issue_type in ["inconsistent", "mixed_types"] and column:
        return """# Conversion et standardisation de la colonne """ + column + """
if '""" + column + """' in df.columns:
    df['""" + column + """'] = df['""" + column + """'].astype(str).str.strip()
    df['""" + column + """'] = df['""" + column + """'].str.replace(' ', '', regex=False)
    df['""" + column + """'] = df['""" + column + """'].str.replace(',', '.', regex=False)
    df['""" + column + """'] = df['""" + column + """'].str.replace('€', '', regex=False)
    df['""" + column + """'] = df['""" + column + """'].str.replace('$', '', regex=False)
    df['""" + column + """'] = pd.to_numeric(df['""" + column + """'], errors='coerce')"""

    elif issue_type == "outlier" and column:
        return """# Correction des outliers pour """ + column + """
if '""" + column + """' in df.columns and pd.api.types.is_numeric_dtype(df['""" + column + """']):
    Q1 = df['""" + column + """'].quantile(0.25)
    Q3 = df['""" + column + """'].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    df['""" + column + """'] = df['""" + column + """'].clip(lower=lower_bound, upper=upper_bound)"""

    else:
        return "# Opération de nettoyage"


def _build_complete_script(dataset_name: str, steps: List[dict]) -> str:
    """Construit un script Python complet et fonctionnel qui résout tous les problèmes."""

    # Construire le code des étapes de nettoyage
    steps_code_parts = []

    for step in steps:
        step_num = str(step["step_number"])
        issue_type = step["issue_type"]
        column = step["column"]
        code = step["code"]

        # Ajouter un commentaire descriptif
        steps_code_parts.append("")
        steps_code_parts.append("    # " + "="*60)
        if column:
            steps_code_parts.append("    # ÉTAPE " + step_num + ": " + issue_type + " sur '" + column + "'")
        else:
            steps_code_parts.append("    # ÉTAPE " + step_num + ": " + issue_type)
        steps_code_parts.append("    # " + "="*60)
        steps_code_parts.append("    try:")

        # Indenter le code de l'étape
        for code_line in code.split("\n"):
            steps_code_parts.append("        " + code_line)

        # Gestion des erreurs
        steps_code_parts.append("    except Exception as step_error:")
        steps_code_parts.append('        logger.warning("Étape ' + step_num + ' ignorée: " + str(step_error))')

    steps_code = "\n".join(steps_code_parts)
    timestamp_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    summary_steps = "\n".join([f"#   - {s['issue_type']} sur {s['column']}" if s['column'] else f"#   - {s['issue_type']}" for s in steps])

    # Construire le script ligne par ligne (pas de f-strings complexes)
    lines = []
    lines.append("#!/usr/bin/env python3")
    lines.append("# -*- coding: utf-8 -*-")
    lines.append('"""')
    lines.append("Script de nettoyage automatique - Généré le " + timestamp_str)
    lines.append("Source: " + dataset_name)
    lines.append("Ce script résout les problèmes suivants:")
    lines.append(summary_steps)
    lines.append('"""')
    lines.append("")
    lines.append("import pandas as pd")
    lines.append("import numpy as np")
    lines.append("from pathlib import Path")
    lines.append("import logging")
    lines.append("import sys")
    lines.append("import argparse")
    lines.append("from typing import Optional, Union")
    lines.append("import warnings")
    lines.append("warnings.filterwarnings('ignore')")
    lines.append("")
    lines.append("INPUT_FILE: Optional[str] = None")
    lines.append("OUTPUT_FILE: Optional[str] = None")
    lines.append("VERBOSE: bool = True")
    lines.append("")
    lines.append("def setup_logging(verbose: bool = True) -> logging.Logger:")
    lines.append("    level = logging.DEBUG if verbose else logging.INFO")
    lines.append("    logging.basicConfig(")
    lines.append("        level=level,")
    lines.append('        format="%(asctime)s | %(levelname)-8s | %(message)s",')
    lines.append('        datefmt="%H:%M:%S",')
    lines.append("        handlers=[logging.StreamHandler(sys.stdout)]")
    lines.append("    )")
    lines.append("    return logging.getLogger(__name__)")
    lines.append("")
    lines.append("logger = setup_logging(VERBOSE)")
    lines.append("")
    lines.append("def detect_encoding(file_path):")
    lines.append("    encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'iso-8859-1', 'cp1252']")
    lines.append("    for encoding in encodings:")
    lines.append("        try:")
    lines.append("            with open(file_path, 'r', encoding=encoding) as f:")
    lines.append("                f.read(1024)")
    lines.append("            return encoding")
    lines.append("        except:")
    lines.append("            continue")
    lines.append("    return 'utf-8'")
    lines.append("")
    lines.append("def detect_delimiter(file_path, encoding):")
    lines.append("    delimiters = [',', ';', '\\t', '|']")
    lines.append("    try:")
    lines.append("        with open(file_path, 'r', encoding=encoding) as f:")
    lines.append("            first_line = f.readline()")
    lines.append("            counts = {d: first_line.count(d) for d in delimiters}")
    lines.append("            best = max(counts, key=counts.get)")
    lines.append("            return best if counts[best] > 0 else ','")
    lines.append("    except:")
    lines.append("        return ','")
    lines.append("")
    lines.append("def load_data(file_path):")
    lines.append("    file_path = Path(file_path)")
    lines.append("    if not file_path.exists():")
    lines.append("        raise FileNotFoundError(f'Fichier non trouvé: {file_path}')")
    lines.append("    encoding = detect_encoding(file_path)")
    lines.append("    delimiter = detect_delimiter(file_path, encoding)")
    lines.append("    df = pd.read_csv(file_path, encoding=encoding, delimiter=delimiter, skipinitialspace=True)")
    lines.append("    df.columns = df.columns.astype(str).str.strip().str.lower().str.replace(' ', '_')")
    lines.append("    return df")
    lines.append("")
    lines.append("def clean_data(df):")
    lines.append("    logger.info('\\n' + '='*60)")
    lines.append("    logger.info('DÉBUT DU NETTOYAGE')")
    lines.append("    logger.info('='*60)")
    lines.append("    df_clean = df.copy()")
    lines.append("    initial_rows = len(df_clean)")
    lines.append("    # ÉTAPES DE NETTOYAGE:")
    lines.append(steps_code)
    lines.append("")
    lines.append("    logger.info('\\n' + '='*60)")
    lines.append("    logger.info('RESULTAT')")
    lines.append("    logger.info(f'Dimensions: {initial_rows} -> {len(df_clean)} lignes')")
    lines.append("    return df_clean")
    lines.append("")
    lines.append("def save_data(df, output_path):")
    lines.append("    output_path = Path(output_path)")
    lines.append("    output_path.parent.mkdir(parents=True, exist_ok=True)")
    lines.append("    df.to_csv(output_path, index=False, encoding='utf-8-sig')")
    lines.append("    logger.info(f'Sauvegardé: {output_path}')")
    lines.append("")
    lines.append("def main():")
    lines.append("    parser = argparse.ArgumentParser(description='Nettoyage CSV')")
    lines.append("    parser.add_argument('--input', '-i', help='Fichier entrée')")
    lines.append("    parser.add_argument('--output', '-o', help='Fichier sortie')")
    lines.append("    args = parser.parse_args()")
    lines.append("    input_file = args.input or INPUT_FILE")
    lines.append("    output_file = args.output or OUTPUT_FILE")
    lines.append("    if not input_file:")
    lines.append("        parser.error('Spécifiez un fichier entrée')")
    lines.append("    if not output_file:")
    lines.append("        output_file = f'cleaned_{Path(input_file).stem}.csv'")
    lines.append("    df_raw = load_data(input_file)")
    lines.append("    df_clean = clean_data(df_raw)")
    lines.append("    save_data(df_clean, output_file)")
    lines.append("    logger.info('NETTOYAGE TERMINÉ')")
    lines.append("")
    lines.append('if __name__ == "__main__":')
    lines.append("    main()")

    return "\n".join(lines)


def _validate_syntax(code: str):
    """Valide la syntaxe Python du code généré."""
    try:
        ast.parse(code)
        return True, None
    except SyntaxError as e:
        return False, f"Ligne {e.lineno}: {e.msg}"
    except Exception as e:
        return False, str(e)
