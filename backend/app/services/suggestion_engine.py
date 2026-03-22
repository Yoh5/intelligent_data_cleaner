#!/usr/bin/env python3
"""
Moteur de suggestions - Génère des stratégies de nettoyage optimales.
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

@dataclass
class CleaningStep:
    step_type: str
    column: Optional[str]
    description: str
    code_template: str
    priority: int

class SuggestionEngine:
    """
    Génère des étapes de nettoyage optimales basées sur l'analyse des données.
    """

    def __init__(self, profiles: Dict, df_sample: pd.DataFrame):
        self.profiles = profiles
        self.df_sample = df_sample.copy() if df_sample is not None else pd.DataFrame()
        self.steps: List[CleaningStep] = []

    def generate_steps(self) -> List[CleaningStep]:
        """Génère toutes les étapes de nettoyage nécessaires."""

        # Étape 0: Détection des valeurs manquantes cachées (TOUJOURS en premier)
        self._add_hidden_missing_detection()

        for col_name, profile in self.profiles.items():
            semantic = profile.get('semantic_type', 'unknown')
            missing_count = profile.get('missing_count', 0)

            # Étape 1: Conversion de type (avant imputation!)
            if semantic == 'numeric-mixed':
                self._add_numeric_conversion(col_name)
            elif semantic == 'datetime-mixed':
                self._add_datetime_conversion(col_name)

            # Étape 2: Imputation des valeurs manquantes
            if missing_count > 0:
                self._add_imputation(col_name, semantic)

            # Étape 3: Détection et correction des outliers
            if semantic in ['numeric', 'numeric-mixed']:
                self._add_outlier_detection(col_name)

            # Étape 4: Standardisation des catégorielles
            if semantic == 'categorical':
                self._add_categorical_cleaning(col_name)

        # Étape finale: Déduplication
        self._add_deduplication_step()

        # Trier par priorité
        self.steps.sort(key=lambda x: x.priority)
        return self.steps

    def _add_hidden_missing_detection(self):
        """Détecte et convertit les valeurs manquantes cachées."""
        code = """# Détection des valeurs manquantes cachées
for col in df.select_dtypes(include=['object']).columns:
    mask = df[col].isin(['', ' ', 'NA', 'N/A', 'null', 'NULL', 'None', 'missing', '-'])
    if mask.any():
        df.loc[mask, col] = np.nan
        logger.info(f"Converti {mask.sum()} valeurs vides en NaN dans {col}")"""

        self.steps.append(CleaningStep(
            step_type='detect_hidden_missing',
            column=None,
            description="Détection des valeurs manquantes cachées",
            code_template=code,
            priority=0
        ))

    def _add_numeric_conversion(self, col: str):
        """Conversion robuste en numérique."""
        code = f"""# Conversion numérique pour '{col}'
if '{col}' in df.columns:
    df['{col}'] = df['{col}'].astype(str).str.replace(' ', '', regex=False)
    df['{col}'] = df['{col}'].str.replace(',', '.', regex=False)
    df['{col}'] = df['{col}'].str.replace('€', '', regex=False)
    df['{col}'] = df['{col}'].str.replace('$', '', regex=False)
    df['{col}'] = df['{col}'].str.replace('%', '', regex=False)
    df['{col}'] = pd.to_numeric(df['{col}'], errors='coerce')
    logger.info(f"Colonne {col} convertie en numérique")"""

        self.steps.append(CleaningStep(
            step_type='convert_numeric',
            column=col,
            description=f"Conversion de '{col}' en numérique",
            code_template=code,
            priority=1
        ))

    def _add_datetime_conversion(self, col: str):
        """Conversion en datetime."""
        code = f"""# Conversion datetime pour '{col}'
if '{col}' in df.columns:
    df['{col}'] = pd.to_datetime(df['{col}'], errors='coerce', infer_datetime_format=True)
    logger.info(f"Colonne {col} convertie en datetime")"""

        self.steps.append(CleaningStep(
            step_type='convert_datetime',
            column=col,
            description=f"Conversion de '{col}' en datetime",
            code_template=code,
            priority=1
        ))

    def _add_imputation(self, col: str, semantic_type: str):
        """Imputation intelligente selon le type."""
        if semantic_type in ['numeric', 'numeric-mixed']:
            code = f"""# Imputation par médiane pour '{col}'
if '{col}' in df.columns:
    if not pd.api.types.is_numeric_dtype(df['{col}']):
        df['{col}'] = pd.to_numeric(df['{col}'], errors='coerce')
    median_val = df['{col}'].median()
    if pd.notna(median_val):
        df['{col}'] = df['{col}'].fillna(median_val)
        logger.info(f"Imputation médiane sur {col}: {median_val:.2f}")
    else:
        df['{col}'] = df['{col}'].fillna(0)"""
        else:
            code = f"""# Imputation par mode pour '{col}'
if '{col}' in df.columns:
    if not df['{col}'].mode().empty:
        mode_val = df['{col}'].mode()[0]
        df['{col}'] = df['{col}'].fillna(mode_val)
        logger.info(f"Imputation mode sur {col}: {mode_val}")
    else:
        df['{col}'] = df['{col}'].fillna('Inconnu')"""

        self.steps.append(CleaningStep(
            step_type='imputation',
            column=col,
            description=f"Imputation des valeurs manquantes dans '{col}'",
            code_template=code,
            priority=10
        ))

    def _add_outlier_detection(self, col: str):
        """Détection et correction des outliers."""
        code = f"""# Correction des outliers pour '{col}'
if '{col}' in df.columns and pd.api.types.is_numeric_dtype(df['{col}']):
    Q1 = df['{col}'].quantile(0.25)
    Q3 = df['{col}'].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    outliers = ((df['{col}'] < lower_bound) | (df['{col}'] > upper_bound)).sum()
    if outliers > 0:
        df['{col}'] = df['{col}'].clip(lower=lower_bound, upper=upper_bound)
        logger.info(f"{outliers} outliers corrigés dans {col}")"""

        self.steps.append(CleaningStep(
            step_type='outlier_correction',
            column=col,
            description=f"Correction des outliers dans '{col}'",
            code_template=code,
            priority=20
        ))

    def _add_categorical_cleaning(self, col: str):
        """Nettoyage des variables catégorielles."""
        code = f"""# Standardisation de '{col}'
if '{col}' in df.columns:
    df['{col}'] = df['{col}'].astype(str).str.strip()
    df['{col}'] = df['{col}'].str.title()
    logger.info(f"Standardisation de {col} effectuée")"""

        self.steps.append(CleaningStep(
            step_type='categorical_cleaning',
            column=col,
            description=f"Standardisation de '{col}'",
            code_template=code,
            priority=15
        ))

    def _add_deduplication_step(self):
        """Déduplication."""
        code = """# Suppression des doublons
initial_count = len(df)
df = df.drop_duplicates()
removed = initial_count - len(df)
if removed > 0:
    logger.info(f"{removed} doublons supprimés")"""

        self.steps.append(CleaningStep(
            step_type='deduplication',
            column=None,
            description="Suppression des doublons",
            code_template=code,
            priority=100
        ))


def generate_cleaning_strategy(profiles: Dict, df_sample: pd.DataFrame, use_llm: bool = False) -> Dict[str, Any]:
    """Fonction utilitaire pour générer la stratégie complète."""
    engine = SuggestionEngine(profiles, df_sample)
    steps = engine.generate_steps()

    return {
        'steps': [
            {
                'type': s.step_type,
                'column': s.column,
                'description': s.description,
                'code': s.code_template,
                'priority': s.priority
            }
            for s in steps
        ],
        'total_steps': len(steps),
        'summary': f"{len(steps)} étapes de nettoyage générées"
    }
