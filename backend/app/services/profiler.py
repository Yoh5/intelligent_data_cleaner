#!/usr/bin/env python3
"""
Profiler optimisé avec conversion forcée des types NumPy vers Python natifs.
"""
import pandas as pd
import numpy as np
import io
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
    semantic_type: Optional[str] = None

class DataProfiler:
    """Profiler avec détection des valeurs manquantes cachées."""

    MISSING_PATTERNS = ['', ' ', '  ', '   ', 'NA', 'N/A', 'n/a', 'na',
                       'NULL', 'null', 'None', 'none', 'NaN', 'nan',
                       'MISSING', 'missing', '-', '--', '???', 'Unknown', 'unknown']

    def __init__(self, df: Optional[pd.DataFrame] = None):
        self.df = df.copy() if df is not None else None
        self.raw_df = df.copy() if df is not None else None
        self.profiles: Dict[str, ColumnProfile] = {}

    async def analyze_file(self, content: bytes, filename: str) -> Dict[str, Any]:
        """
        Analyse un fichier à partir de son contenu binaire.
        """
        # Détection du format
        if filename.lower().endswith('.csv'):
            for encoding in ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']:
                try:
                    self.df = pd.read_csv(io.BytesIO(content), encoding=encoding)
                    self.raw_df = self.df.copy()
                    break
                except UnicodeDecodeError:
                    continue
            else:
                raise ValueError("Impossible de décoder le fichier CSV")
        elif filename.lower().endswith(('.xlsx', '.xls')):
            self.df = pd.read_excel(io.BytesIO(content))
            self.raw_df = self.df.copy()
        else:
            raise ValueError(f"Format non supporté: {filename}")

        # Lancer l'analyse
        profiles = self.profile()
        requirements = self.get_cleaning_requirements()
        
        # Conversion pour JSON - IMPORTANT: convertir tous les types numpy
        serializable_columns = {}
        for k, v in profiles.items():
            profile_dict = {
                'name': v.name,
                'dtype': str(v.dtype),
                'nullable': bool(v.nullable),  # Convertir numpy.bool_ en bool
                'missing_count': int(v.missing_count),  # Convertir numpy.int64 en int
                'missing_rate': float(v.missing_rate),  # Convertir numpy.float64 en float
                'unique_count': int(v.unique_count),
                'sample_values': [self._convert_to_python(x) for x in v.sample_values],
                'semantic_type': v.semantic_type
            }
            serializable_columns[k] = profile_dict

        return {
            'id': str(pd.Timestamp.now().timestamp()),
            'created_at': pd.Timestamp.now().isoformat(),
            'dataset_info': {
                'filename': filename,
                'rows': int(len(self.df)),  # Convertir en int
                'columns': int(len(self.df.columns)),  # Convertir en int
                'size_bytes': int(len(content)),  # Convertir en int
                'column_types': {col: str(dtype) for col, dtype in self.df.dtypes.items()}
            },
            'issues': requirements,
            'profile_html': None,
            'raw_profile': {
                'shape': [int(x) for x in self.df.shape],  # Convertir shape en list d'int
                'columns': serializable_columns,
                'total_missing_cells': int(sum(p['missing_count'] for p in serializable_columns.values())),
                'columns_with_issues': int(len(requirements))
            }
        }

    def _convert_to_python(self, value: Any) -> Any:
        """
        Convertit récursivement les types numpy en types Python natifs.
        """
        if value is None or pd.isna(value):
            return None
        elif isinstance(value, (np.bool_, bool)):
            return bool(value)
        elif isinstance(value, (np.integer, np.int64, np.int32)):
            return int(value)
        elif isinstance(value, (np.floating, np.float64, np.float32)):
            return float(value)
        elif isinstance(value, np.ndarray):
            return [self._convert_to_python(x) for x in value.tolist()]
        elif isinstance(value, pd.Timestamp):
            return value.isoformat()
        elif isinstance(value, dict):
            return {k: self._convert_to_python(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [self._convert_to_python(x) for x in value]
        else:
            return value

    def _detect_hidden_missing(self, col: pd.Series) -> pd.Series:
        """Détecte et convertit les valeurs manquantes cachées en NaN."""
        if col.dtype == 'object':
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
                converted = pd.to_numeric(col.replace('', np.nan).replace(' ', np.nan),
                                        errors='coerce')
                if converted.notna().sum() / len(col) > 0.7:
                    return 'numeric-mixed'
            except:
                pass

            # Tester si datetime (supprimer le warning en spécifiant infer_datetime_format)
            try:
                # Limiter à un échantillon pour la performance
                sample = col.dropna().head(100)
                if len(sample) > 0:
                    converted = pd.to_datetime(sample, errors='coerce', infer_datetime_format=True)
                    if converted.notna().sum() / len(sample) > 0.7:
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
        if self.df is None:
            raise ValueError("Aucun DataFrame chargé.")
            
        for col_name in self.df.columns:
            col = self.df[col_name]

            # Détecter valeurs manquantes cachées
            hidden_missing = self._detect_hidden_missing(col)
            total_missing = int(col.isna().sum() + hidden_missing.sum())  # Convertir en int

            # Inférer type sémantique
            semantic = self._infer_semantic_type(col, col_name)

            # Déterminer dtype nettoyé
            if semantic == 'numeric-mixed':
                cleaned_dtype = 'float64'
            else:
                cleaned_dtype = str(col.dtype)

            self.profiles[col_name] = ColumnProfile(
                name=col_name,
                dtype=cleaned_dtype,
                nullable=bool(total_missing > 0),  # Convertir explicitement en bool
                missing_count=total_missing,
                missing_rate=float(total_missing / len(self.df)),  # Convertir en float
                unique_count=int(col.nunique()),  # Convertir en int
                sample_values=[self._convert_to_python(x) for x in col.dropna().head(5).tolist()],
                semantic_type=semantic
            )

        return self.profiles

    def get_cleaning_requirements(self) -> List[Dict]:
        """Retourne les besoins de nettoyage détectés avec types Python natifs."""
        requirements = []
        
        if not self.profiles:
            self.profile()

        for name, profile in self.profiles.items():
            # Problème 1: Valeurs manquantes
            if profile.missing_count > 0:
                requirements.append({
                    'column': name,
                    'issue': 'missing_values',
                    'type': 'missing',
                    'count': int(profile.missing_count),
                    'rate': float(profile.missing_rate),
                    'severity': 'high' if profile.missing_rate > 0.1 else 'medium',
                    'semantic_type': profile.semantic_type,
                    'description': f"{int(profile.missing_count)} valeurs manquantes ({float(profile.missing_rate)*100:.1f}%)",
                    'affected_rows': int(profile.missing_count)
                })

            # Problème 2: Types mixtes
            if profile.semantic_type == 'numeric-mixed':
                requirements.append({
                    'column': name,
                    'issue': 'mixed_types',
                    'type': 'inconsistent',
                    'current_dtype': 'object/string',
                    'suggested_dtype': 'numeric',
                    'severity': 'high',
                    'description': f"Colonne {name} contient des nombres et du texte",
                    'affected_rows': None
                })

            # Problème 3: Duplicatas potentiels (pour colonnes ID)
            if 'id' in name.lower() and profile.unique_count < len(self.df):
                requirements.append({
                    'column': name,
                    'issue': 'duplicate_ids',
                    'type': 'duplicate',
                    'duplicates': int(len(self.df) - profile.unique_count),
                    'severity': 'critical',
                    'description': f"Doublons détectés dans la colonne ID {name}",
                    'affected_rows': int(len(self.df) - profile.unique_count)
                })

        return requirements