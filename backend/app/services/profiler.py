#!/usr/bin/env python3
"""
Profiler optimisé avec détection avancée des valeurs manquantes cachées
et inférence de types robuste.
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

@dataclass
class ColumnProfile:
    name: str
    dtype: str
    nullable: bool
    missing_count: int
    missing_rate: float
    unique_count: int
    sample_values: List[Any]
    semantic_type: Optional[str] = None  # 'numeric', 'categorical', 'text', 'datetime'

class DataProfiler:
    """Profiler avec détection des valeurs manquantes cachées."""

    # Patterns de valeurs manquantes (étendus)
    MISSING_PATTERNS = ['', ' ', '  ', '   ', 'NA', 'N/A', 'n/a', 'na', 
                       'NULL', 'null', 'None', 'none', 'NaN', 'nan', 
                       'MISSING', 'missing', '-', '--', '???', 'Unknown', 'unknown']

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.raw_df = df.copy()
        self.profiles: Dict[str, ColumnProfile] = {}

    def _detect_hidden_missing(self, col: pd.Series) -> pd.Series:
        """Détecte et convertit les valeurs manquantes cachées en NaN."""
        if col.dtype == 'object':
            # Détecter les chaînes vides ou espaces uniquement
            mask = (
                col.isin(self.MISSING_PATTERNS) | 
                col.astype(str).str.strip().eq('') |
                col.astype(str).str.lower().isin([p.lower() for p in self.MISSING_PATTERNS])
            )
            return mask
        return pd.Series(False, index=col.index)

    def _infer_semantic_type(self, col: pd.Series, col_name: str) -> str:
        """Infère le type sémantique avec conversion robuste."""
        if col.dtype == 'object':
            # Tester si numérique caché
            try:
                # Convertir en numérique (coerce transforme erreurs en NaN)
                converted = pd.to_numeric(col.replace('', np.nan).replace(' ', np.nan), 
                                        errors='coerce')
                # Si plus de 70% convertissables → numérique
                if converted.notna().sum() / len(col) > 0.7:
                    return 'numeric-mixed'
            except:
                pass

            # Tester si datetime
            try:
                converted = pd.to_datetime(col, errors='coerce')
                if converted.notna().sum() / len(col) > 0.7:
                    return 'datetime'
            except:
                pass

            # Catégoriel vs Texte
            unique_ratio = col.nunique() / len(col)
            if unique_ratio < 0.05 or col.nunique() < 20:
                return 'categorical'
            else:
                return 'text'

        elif np.issubdtype(col.dtype, np.number):
            return 'numeric'
        elif np.issubdtype(col.dtype, np.datetime64):
            return 'datetime'

        return 'unknown'

    def profile(self) -> Dict[str, ColumnProfile]:
        """Génère les profils avec correction préliminaire des types."""
        for col_name in self.df.columns:
            col = self.df[col_name]

            # Détecter valeurs manquantes cachées
            hidden_missing = self._detect_hidden_missing(col)
            total_missing = col.isna().sum() + hidden_missing.sum()

            # Inférer type sémantique
            semantic = self._infer_semantic_type(col, col_name)

            # Déterminer dtype nettoyé
            if semantic == 'numeric-mixed':
                cleaned_dtype = 'float64'  # Sera converti
            else:
                cleaned_dtype = str(col.dtype)

            self.profiles[col_name] = ColumnProfile(
                name=col_name,
                dtype=cleaned_dtype,
                nullable=total_missing > 0,
                missing_count=int(total_missing),
                missing_rate=total_missing / len(self.df),
                unique_count=col.nunique(),
                sample_values=col.dropna().head(5).tolist(),
                semantic_type=semantic
            )

        return self.profiles

    def get_cleaning_requirements(self) -> List[Dict]:
        """Retourne les besoins de nettoyage détectés."""
        requirements = []

        for name, profile in self.profiles.items():
            # Problème 1: Valeurs manquantes
            if profile.missing_count > 0:
                requirements.append({
                    'column': name,
                    'issue': 'missing_values',
                    'count': profile.missing_count,
                    'rate': profile.missing_rate,
                    'severity': 'high' if profile.missing_rate > 0.1 else 'medium',
                    'semantic_type': profile.semantic_type,
                    'suggestion': self._suggest_missing_strategy(profile)
                })

            # Problème 2: Types mixtes
            if profile.semantic_type == 'numeric-mixed':
                requirements.append({
                    'column': name,
                    'issue': 'mixed_types',
                    'current_dtype': 'object/string',
                    'suggested_dtype': 'numeric',
                    'severity': 'high'
                })

            # Problème 3: Duplicatas potentiels (pour colonnes ID)
            if 'id' in name.lower() and profile.unique_count < len(self.df):
                requirements.append({
                    'column': name,
                    'issue': 'duplicate_ids',
                    'duplicates': len(self.df) - profile.unique_count,
                    'severity': 'critical'
                })

        return requirements

    def _suggest_missing_strategy(self, profile: ColumnProfile) -> str:
        """Suggère stratégie selon type et taux."""
        if profile.semantic_type == 'numeric' or profile.semantic_type == 'numeric-mixed':
            if profile.missing_rate < 0.05:
                return 'median_imputation'
            else:
                return 'mean_imputation'  # ou 'predictive_imputation' pour LLM
        elif profile.semantic_type == 'categorical':
            return 'mode_imputation'
        else:
            return 'constant_imputation'

# Fonction utilitaire
async def analyze_dataset(file_path: str) -> Dict[str, Any]:
    """Analyse complète d'un dataset."""
    df = pd.read_csv(file_path)
    profiler = DataProfiler(df)
    profiles = profiler.profile()
    requirements = profiler.get_cleaning_requirements()

    return {
        'shape': df.shape,
        'columns': {k: vars(v) for k, v in profiles.items()},
        'issues': requirements,
        'summary': {
            'total_missing_cells': sum(p.missing_count for p in profiles.values()),
            'columns_with_issues': len(requirements),
            'recommendation_count': len([r for r in requirements if r['severity'] in ['high', 'critical']])
        }
    }