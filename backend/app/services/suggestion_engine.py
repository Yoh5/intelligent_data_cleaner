#!/usr/bin/env python3
"""
Moteur de suggestions - Logique métier séparée du router.
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

@dataclass
class CleaningStep:
    step_type: str
    column: str
    description: str
    code_template: str
    priority: int
    reason: str
    validation_check: str

class SuggestionEngine:
    """Génère des étapes de nettoyage validées."""

    def __init__(self, profiles: Dict, df_sample: pd.DataFrame):
        self.profiles = profiles
        self.df_sample = df_sample
        self.steps: List[CleaningStep] = []

    def generate_steps(self) -> List[CleaningStep]:
        """Pipeline complet de suggestions."""
        for col_name, profile in self.profiles.items():
            semantic = profile.get('semantic_type', 'unknown')

            # Étape 1: Conversion de type (TOUJOURS en premier pour numeric-mixed)
            if semantic == 'numeric-mixed':
                self._add_type_conversion(col_name, 'numeric')

            # Étape 2: Valeurs manquantes (après conversion de type !)
            if profile.get('missing_count', 0) > 0:
                self._add_imputation(col_name, semantic)

            # Étape 3: Outliers pour colonnes numériques
            if semantic in ['numeric', 'numeric-mixed']:
                self._add_outlier_detection(col_name)

            # Étape 4: Standardisation catégorielle
            if semantic == 'categorical':
                self._add_categorical_cleaning(col_name)

        # Trier par priorité
        self.steps.sort(key=lambda x: x.priority)
        return self.steps

    def _add_type_conversion(self, col: str, target_type: str):
        """Conversion sécurisée avec gestion des erreurs."""
        code = f"""# Conversion {col} en {target_type}
df['{col}'] = pd.to_numeric(df['{col}'], errors='coerce')
logger.info(f"Converti {col} en numérique")"""

        validation = f"pd.api.types.is_numeric_dtype(df['{col}'])"

        self.steps.append(CleaningStep(
            step_type='convert_type',
            column=col,
            description=f"Conversion de {col} en type {target_type}",
            code_template=code,
            priority=1,
            reason="Colonne numérique stockée comme texte avec valeurs vides",
            validation_check=validation
        ))

    def _add_imputation(self, col: str, semantic_type: str):
        """Imputation avec sécurité médiane."""
        if semantic_type in ['numeric', 'numeric-mixed']:
            # CORRECTION: Double vérification pour éviter l'erreur sur strings
            code = f"""# Imputation sécurisée pour {col}
# Conversion explicite (idempotente si déjà converti)
df['{col}'] = pd.to_numeric(df['{col}'], errors='coerce')
if df['{col}'].isna().any():
    median_val = df['{col}'].median()
    if pd.notna(median_val):
        df['{col}'] = df['{col}'].fillna(median_val)
        logger.info(f"Imputation médiane sur {col}: {{median_val:.2f}}")
    else:
        df['{col}'] = df['{col}'].fillna(0)
        logger.warning(f"Imputation avec 0 (toutes valeurs NaN) sur {col}")"""
            validation = f"df['{col}'].isna().sum() == 0"

        elif semantic_type == 'categorical':
            code = f"""# Imputation mode pour {col}
if df['{col}'].isna().any():
    mode_val = df['{col}'].mode()
    if len(mode_val) > 0:
        df['{col}'] = df['{col}'].fillna(mode_val[0])
    else:
        df['{col}'] = df['{col}'].fillna('Inconnu')"""
            validation = f"df['{col}'].isna().sum() == 0"
        else:
            code = f"""# Imputation constante pour {col}
df['{col}'] = df['{col}'].fillna('Non spécifié')"""
            validation = f"df['{col}'].isna().sum() == 0"

        self.steps.append(CleaningStep(
            step_type='impute',
            column=col,
            description=f"Imputation des valeurs manquantes dans {col}",
            code_template=code,
            priority=10 if semantic_type == 'numeric-mixed' else 5,
            reason=f"{self.profiles[col].get('missing_count', 0)} valeurs manquantes détectées",
            validation_check=validation
        ))

    def _add_outlier_detection(self, col: str):
        """Détection outliers avec IQR."""
        code = f"""# Winsorisation IQR pour {col}
Q1 = df['{col}'].quantile(0.25)
Q3 = df['{col}'].quantile(0.75)
IQR = Q3 - Q1
lower = Q1 - 1.5 * IQR
upper = Q3 + 1.5 * IQR

outliers = ((df['{col}'] < lower) | (df['{col}'] > upper)).sum()
if outliers > 0:
    df['{col}'] = df['{col}'].clip(lower, upper)
    logger.info(f"Winsorisation {col}: {{outliers}} outliers corrigés")"""
        
        validation = f"df['{col}'].between(df['{col}'].quantile(0.25) - 1.5*(df['{col}'].quantile(0.75)-df['{col}'].quantile(0.25)), df['{col}'].quantile(0.75) + 1.5*(df['{col}'].quantile(0.75)-df['{col}'].quantile(0.25))).all()"

        self.steps.append(CleaningStep(
            step_type='clip_outliers',
            column=col,
            description=f"Correction des outliers dans {col} (méthode IQR)",
            code_template=code,
            priority=20,
            reason="Distribution avec valeurs extrêmes potentielles",
            validation_check=validation
        ))

    def _add_categorical_cleaning(self, col: str):
        """Nettoyage des catégorielles."""
        code = f"""# Nettoyage catégoriel {col}: trim + title case
df['{col}'] = df['{col}'].astype(str).str.strip()
df['{col}'] = df['{col}'].replace(['nan', 'None', 'null', ''], 'Inconnu')
df['{col}'] = df['{col}'].str.title()"""
        
        validation = f"df['{col}'].astype(str).str.contains('^\\s+$').sum() == 0"

        self.steps.append(CleaningStep(
            step_type='clean_categorical',
            column=col,
            description=f"Nettoyage des valeurs textuelles dans {col}",
            code_template=code,
            priority=15,
            reason="Valeurs avec espaces ou incohérences de format",
            validation_check=validation
        ))

    def generate_summary(self) -> str:
        """Génère un résumé textuel."""
        lines = ["## Plan de nettoyage suggéré", ""]
        for i, step in enumerate(self.steps, 1):
            lines.append(f"**{i}. {step.description}**")
            lines.append(f"   - Type: {step.step_type}")
            lines.append(f"   - Raison: {step.reason}")
            lines.append("")
        return "\n".join(lines)

# Fonction utilitaire pour le router
def generate_cleaning_strategy(profiles: Dict, df_sample: pd.DataFrame, use_llm: bool = False) -> Dict[str, Any]:
    """Génère une stratégie complète."""
    engine = SuggestionEngine(profiles, df_sample)
    steps = engine.generate_steps()

    return {
        'steps': [
            {
                'type': s.step_type,
                'column': s.column,
                'description': s.description,
                'code': s.code_template,
                'priority': s.priority,
                'validation': s.validation_check
            }
            for s in steps
        ],
        'summary': engine.generate_summary(),
        'total_steps': len(steps),
        'critical_steps': len([s for s in steps if s.priority < 10])
    }