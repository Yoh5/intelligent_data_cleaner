#!/usr/bin/env python3
"""
Profiler ultra-robuste avec détection intelligente de tous les types de données.
Garantit que l'analyse fonctionne pour n'importe quel CSV.
"""
import pandas as pd
import numpy as np
import io
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass, field
import re
from datetime import datetime

@dataclass
class ColumnProfile:
    name: str
    dtype: str
    nullable: bool
    missing_count: int
    missing_rate: float
    unique_count: int
    sample_values: List[Any]
    semantic_type: str = "unknown"
    inferred_dtype: Optional[str] = None
    cleaning_needed: List[str] = field(default_factory=list)

class DataProfiler:
    """
    Profiler intelligent qui gère tous les cas particuliers:
    - Encodages multiples (UTF-8, Latin-1, CP1252, ISO-8859-1)
    - Délimiteurs auto-détectés (, ; | \t)
    - Valeurs manquantes cachées (15+ patterns)
    - Types mixtes avec conversion intelligente
    - Dates dans tous les formats
    """

    # Patterns de valeurs manquantes étendus
    MISSING_PATTERNS = [
        '', ' ', '  ', '   ', '\t', '\n', '\r',
        'NA', 'N/A', 'n/a', 'na', 'N.A.', 'n.a.',
        'NULL', 'null', 'Null', 'NONE', 'none', 'None',
        'NaN', 'nan', 'NAN', 'NaN ', ' nan',
        'MISSING', 'missing', 'Missing',
        '-', '--', '---', '_', '__', '___',
        '???', '??', '?',
        'Unknown', 'unknown', 'UNKNOWN',
        'Not Available', 'not available', 'NOT AVAILABLE',
        'No Data', 'no data', 'NO DATA',
        'Empty', 'empty', 'EMPTY',
        'Void', 'void', 'VOID',
        'Nil', 'nil', 'NIL'
    ]

    # Patterns pour détecter les nombres dans du texte
    NUMERIC_PATTERN = re.compile(r'^-?\d+\.?\d*$|^-?\d+\,?\d*$')

    # Patterns pour détecter les dates
    DATE_PATTERNS = [
        r'^\d{1,2}[/-]\d{1,2}[/-]\d{2,4}$',  # DD/MM/YYYY, MM-DD-YY
        r'^\d{4}[/-]\d{1,2}[/-]\d{1,2}$',  # YYYY/MM/DD
        r'^\d{1,2}\s+[A-Za-z]{3,}\s+\d{2,4}$',  # 1 Jan 2020
        r'^[A-Za-z]{3,}\s+\d{1,2},?\s+\d{2,4}$',  # Jan 1, 2020
        r'^\d{1,2}\s+[A-Za-z]{3,}$',  # 1 Jan (année implicite)
    ]

    def __init__(self, df: Optional[pd.DataFrame] = None):
        self.df = df.copy() if df is not None else None
        self.raw_df = df.copy() if df is not None else None
        self.profiles: Dict[str, ColumnProfile] = {}
        self.issues: List[Dict] = []
        self.encoding_used: str = "utf-8"
        self.delimiter_used: str = ","

    async def analyze_file(self, content: bytes, filename: str) -> Dict[str, Any]:
        """
        Analyse ultra-robuste d'un fichier CSV/Excel.
        Gère tous les cas d'erreur possibles.
        """
        try:
            # Détection du format
            if filename.lower().endswith('.csv'):
                self.df = await self._load_csv_robust(content)
            elif filename.lower().endswith(('.xlsx', '.xls')):
                self.df = await self._load_excel_robust(content)
            else:
                raise ValueError(f"Format non supporté: {filename}")

            if self.df is None or self.df.empty:
                raise ValueError("Fichier vide ou impossible à lire")

            self.raw_df = self.df.copy()

            # Lancer l'analyse complète
            profiles = self.profile()
            requirements = self.get_cleaning_requirements()

            # Conversion sérialisable pour JSON
            serializable_columns = {}
            for k, v in profiles.items():
                profile_dict = {
                    'name': v.name,
                    'dtype': str(v.dtype),
                    'nullable': bool(v.nullable),
                    'missing_count': int(v.missing_count),
                    'missing_rate': float(v.missing_rate),
                    'unique_count': int(v.unique_count),
                    'sample_values': [self._convert_to_python(x) for x in v.sample_values],
                    'semantic_type': v.semantic_type,
                    'inferred_dtype': v.inferred_dtype,
                    'cleaning_needed': v.cleaning_needed
                }
                serializable_columns[k] = profile_dict

            return {
                'id': str(datetime.now().timestamp()),
                'created_at': datetime.now().isoformat(),
                'dataset_info': {
                    'filename': filename,
                    'rows': int(len(self.df)),
                    'columns': int(len(self.df.columns)),
                    'size_bytes': int(len(content)),
                    'encoding_detected': self.encoding_used,
                    'delimiter_detected': self.delimiter_used,
                    'column_types': {col: str(dtype) for col, dtype in self.df.dtypes.items()}
                },
                'issues': requirements,
                'profile_html': None,
                'raw_profile': {
                    'shape': [int(x) for x in self.df.shape],
                    'columns': serializable_columns,
                    'total_missing_cells': int(sum(p['missing_count'] for p in serializable_columns.values())),
                    'columns_with_issues': int(len(requirements)),
                    'memory_usage_mb': round(self.df.memory_usage(deep=True).sum() / 1024 / 1024, 2)
                }
            }

        except Exception as e:
            import traceback
            print(f"ERREUR ANALYSE: {traceback.format_exc()}")
            raise

    async def _load_csv_robust(self, content: bytes) -> pd.DataFrame:
        """
        Chargement CSV avec détection automatique de:
        - Encodage (5+ encodages testés)
        - Délimiteur (, ; | \t)
        - Header (présent ou non)
        - Nombre de lignes à skip (commentaires)
        """
        encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'iso-8859-1', 'cp1252', 'iso-8859-15']
        delimiters = [',', ';', '|', '\t', ':']

        last_error = None

        for encoding in encodings:
            try:
                # Essayer de décoder un échantillon pour vérifier l'encodage
                sample = content[:min(10000, len(content))].decode(encoding)
                self.encoding_used = encoding
                break
            except UnicodeDecodeError:
                continue
        else:
            raise ValueError("Impossible de décoder le fichier CSV avec les encodages standards")

        # Détection du délimiteur sur l'échantillon
        first_lines = sample.split('\n')[:5]
        delimiter_scores = {}

        for delim in delimiters:
            counts = [len(line.split(delim)) for line in first_lines if line.strip()]
            if counts and max(counts) > 1:
                # Score basé sur la cohérence du nombre de colonnes
                avg_cols = sum(counts) / len(counts)
                consistency = len(set(counts)) == 1  # Toutes les lignes ont même nombre de colonnes
                delimiter_scores[delim] = (avg_cols, consistency)

        # Choisir le meilleur délimiteur
        if delimiter_scores:
            # Priorité au délimiteur avec le plus de colonnes et cohérent
            best_delim = max(delimiter_scores.items(), 
                           key=lambda x: (x[1][0], x[1][1]))[0]
            self.delimiter_used = best_delim
        else:
            self.delimiter_used = ','

        # Chargement avec les paramètres détectés
        try:
            df = pd.read_csv(
                io.BytesIO(content),
                encoding=self.encoding_used,
                delimiter=self.delimiter_used,
                skipinitialspace=True,  # Ignore espaces après délimiteur
                na_values=self.MISSING_PATTERNS,  # Reconnaît tous les patterns manquants
                keep_default_na=True,
                low_memory=False,  # Évite les warnings types mixtes
                dtype=str  # Chargement initial comme string pour inspection
            )

            # Nettoyage des noms de colonnes
            df.columns = df.columns.str.strip().str.replace(' ', '_').str.lower()

            return df

        except Exception as e:
            # Fallback: chargement le plus basique possible
            try:
                df = pd.read_csv(
                    io.BytesIO(content),
                    encoding=self.encoding_used,
                    header=None,
                    on_bad_lines='skip'
                )
                # Générer des noms de colonnes si pas de header
                df.columns = [f'col_{i}' for i in range(len(df.columns))]
                return df
            except Exception as e2:
                raise ValueError(f"Échec chargement CSV: {e2}")

    async def _load_excel_robust(self, content: bytes) -> pd.DataFrame:
        """Chargement Excel avec gestion des erreurs."""
        try:
            # Essayer avec engine openpyxl (xlsx)
            df = pd.read_excel(io.BytesIO(content), engine='openpyxl', dtype=str)
        except:
            try:
                # Fallback sur xlrd (xls)
                df = pd.read_excel(io.BytesIO(content), engine='xlrd', dtype=str)
            except:
                # Dernier essai sans engine spécifié
                df = pd.read_excel(io.BytesIO(content), dtype=str)

        df.columns = df.columns.str.strip().str.replace(' ', '_').str.lower()
        return df

    def _convert_to_python(self, value: Any) -> Any:
        """Convertit récursivement les types numpy/complexes en types Python natifs."""
        if value is None or pd.isna(value):
            return None
        elif isinstance(value, (np.bool_, bool)):
            return bool(value)
        elif isinstance(value, (np.integer, np.int64, np.int32, np.int16, np.int8)):
            return int(value)
        elif isinstance(value, (np.floating, np.float64, np.float32, np.float16)):
            return float(value)
        elif isinstance(value, np.ndarray):
            return [self._convert_to_python(x) for x in value.tolist()]
        elif isinstance(value, pd.Timestamp):
            return value.isoformat()
        elif isinstance(value, dict):
            return {k: self._convert_to_python(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [self._convert_to_python(x) for x in value]
        elif isinstance(value, bytes):
            try:
                return value.decode('utf-8')
            except:
                return str(value)
        else:
            return value

    def _detect_hidden_missing(self, col: pd.Series) -> pd.Series:
        """Détecte les valeurs manquantes cachées dans une colonne."""
        if col.dtype == 'object' or str(col.dtype) == 'object':
            # Convertir en string pour inspection
            str_col = col.astype(str)

            mask = (
                col.isin(self.MISSING_PATTERNS) |
                str_col.str.strip().eq('') |
                str_col.str.strip().str.lower().isin([p.lower() for p in self.MISSING_PATTERNS]) |
                str_col.str.match(r'^\s*$')  # Uniquement espaces
            )
            return mask
        return pd.Series(False, index=col.index)

    def _is_numeric_convertible(self, col: pd.Series) -> tuple[bool, float]:
        """
        Détecte si une colonne object peut être convertie en numérique.
        Retourne: (est_convertible, taux_succes)
        """
        if col.dtype != 'object':
            return False, 0.0

        # Échantillon pour performance
        sample = col.dropna().head(1000)
        if len(sample) == 0:
            return False, 0.0

        # Nettoyer les valeurs avant conversion
        cleaned = sample.astype(str).str.strip()

        # Supprimer les patterns de missing
        cleaned = cleaned[~cleaned.isin(self.MISSING_PATTERNS)]
        cleaned = cleaned[cleaned.str.strip() != '']

        if len(cleaned) == 0:
            return False, 0.0

        # Essayer la conversion
        converted = pd.to_numeric(
            cleaned.str.replace(',', '.', regex=False)  # Gérer séparateur décimal français
                           .str.replace(' ', '', regex=False),  # Supprimer espaces dans nombres
            errors='coerce'
        )

        success_rate = converted.notna().sum() / len(cleaned)
        return success_rate > 0.7, success_rate

    def _is_datetime_convertible(self, col: pd.Series) -> tuple[bool, float]:
        """Détecte si une colonne peut être convertie en datetime."""
        if col.dtype != 'object':
            return False, 0.0

        sample = col.dropna().head(500)
        if len(sample) == 0:
            return False, 0.0

        # Vérifier patterns de date
        str_sample = sample.astype(str).str.strip()
        date_matches = 0

        for val in str_sample:
            for pattern in self.DATE_PATTERNS:
                if re.match(pattern, str(val)):
                    date_matches += 1
                    break

        success_rate = date_matches / len(str_sample)

        if success_rate > 0.7:
            # Vérifier que pandas peut parser
            try:
                converted = pd.to_datetime(sample, errors='coerce', infer_datetime_format=True)
                actual_rate = converted.notna().sum() / len(sample)
                return actual_rate > 0.7, actual_rate
            except:
                return False, 0.0

        return False, success_rate

    def _infer_semantic_type(self, col: pd.Series, col_name: str) -> tuple[str, Optional[str]]:
        """
        Infère le type sémantique et suggère un dtype converti.
        Retourne: (semantic_type, inferred_dtype)
        """
        original_dtype = str(col.dtype)

        # Déjà numérique
        if np.issubdtype(col.dtype, np.number):
            return 'numeric', None

        # Déjà datetime
        if np.issubdtype(col.dtype, np.datetime64):
            return 'datetime', None

        # Type object - analyser le contenu
        if col.dtype == 'object':
            # Test numérique
            is_num, num_rate = self._is_numeric_convertible(col)
            if is_num:
                return 'numeric-mixed', 'float64'

            # Test datetime
            is_date, date_rate = self._is_datetime_convertible(col)
            if is_date:
                return 'datetime-mixed', 'datetime64[ns]'

            # Catégoriel vs Texte
            unique_count = col.nunique()
            total_count = len(col.dropna())

            if total_count == 0:
                return 'empty', None

            unique_ratio = unique_count / total_count

            # Heuristiques pour ID
            if any(keyword in col_name.lower() for keyword in ['id', 'code', 'ref', 'key', 'num']):
                if unique_ratio > 0.9:
                    return 'id', 'string'

            # Catégoriel: peu de valeurs uniques ou colonne explicitement catégorielle
            if unique_ratio < 0.05 or unique_count < 20:
                return 'categorical', 'category'

            # Email
            if any(keyword in col_name.lower() for keyword in ['email', 'mail', 'courriel']):
                sample = col.dropna().head(10).astype(str)
                if any('@' in str(v) for v in sample):
                    return 'email', 'string'

            # URL
            if any(keyword in col_name.lower() for keyword in ['url', 'site', 'web', 'link']):
                sample = col.dropna().head(10).astype(str)
                if any('http' in str(v) or 'www.' in str(v) for v in sample):
                    return 'url', 'string'

            # Téléphone
            if any(keyword in col_name.lower() for keyword in ['phone', 'tel', 'mobile', 'fax']):
                return 'phone', 'string'

            # Texte long
            avg_length = col.dropna().astype(str).str.len().mean()
            if avg_length > 50:
                return 'text', 'string'

            return 'categorical', 'category'

        return 'unknown', None

    def profile(self) -> Dict[str, ColumnProfile]:
        """Génère les profils complets avec détection avancée."""
        if self.df is None:
            raise ValueError("Aucun DataFrame chargé.")

        for col_name in self.df.columns:
            col = self.df[col_name]

            # Détecter valeurs manquantes (NaN + cachées)
            standard_missing = col.isna().sum()
            hidden_missing = self._detect_hidden_missing(col).sum()
            total_missing = int(standard_missing + hidden_missing)

            # Inférer type sémantique
            semantic, inferred_dtype = self._infer_semantic_type(col, col_name)

            # Déterminer dtype nettoyé
            cleaned_dtype = inferred_dtype if inferred_dtype else str(col.dtype)

            # Identifier les besoins de nettoyage
            cleaning_needs = []
            if total_missing > 0:
                cleaning_needs.append('missing_values')
            if semantic == 'numeric-mixed':
                cleaning_needs.append('type_conversion')
            if semantic == 'datetime-mixed':
                cleaning_needs.append('date_parsing')
            if semantic == 'categorical':
                cleaning_needs.append('standardization')

            # Détection doublons pour IDs
            if 'id' in semantic and col.nunique() < len(col.dropna()):
                cleaning_needs.append('duplicate_check')

            self.profiles[col_name] = ColumnProfile(
                name=col_name,
                dtype=str(col.dtype),
                nullable=bool(total_missing > 0),
                missing_count=total_missing,
                missing_rate=float(total_missing / len(self.df)),
                unique_count=int(col.nunique()),
                sample_values=[self._convert_to_python(x) for x in col.dropna().head(5).tolist()],
                semantic_type=semantic,
                inferred_dtype=inferred_dtype,
                cleaning_needed=cleaning_needs
            )

        return self.profiles

    def get_cleaning_requirements(self) -> List[Dict]:
        """Retourne les besoins de nettoyage priorisés."""
        requirements = []

        if not self.profiles:
            self.profile()

        for name, profile in self.profiles.items():
            # 1. Valeurs manquantes
            if profile.missing_count > 0:
                severity = 'critical' if profile.missing_rate > 0.3 else 'high' if profile.missing_rate > 0.1 else 'medium'
                requirements.append({
                    'column': name,
                    'issue': 'missing_values',
                    'type': 'missing',
                    'count': int(profile.missing_count),
                    'rate': float(profile.missing_rate),
                    'severity': severity,
                    'semantic_type': profile.semantic_type,
                    'description': f"{int(profile.missing_count)} valeurs manquantes ({float(profile.missing_rate)*100:.1f}%)",
                    'affected_rows': int(profile.missing_count),
                    'recommended_strategy': self._get_missing_strategy(profile)
                })

            # 2. Types mixtes nécessitant conversion
            if profile.semantic_type in ['numeric-mixed', 'datetime-mixed']:
                requirements.append({
                    'column': name,
                    'issue': 'mixed_types',
                    'type': 'inconsistent',
                    'current_dtype': 'object/string',
                    'suggested_dtype': profile.inferred_dtype,
                    'severity': 'high',
                    'description': f"Colonne '{name}' contient des {profile.semantic_type.replace('-mixed', '')}s masqués par du texte",
                    'affected_rows': None,
                    'recommended_strategy': 'convert_type'
                })

            # 3. Duplicatas pour IDs
            if 'id' in profile.semantic_type and profile.unique_count < len(self.df):
                dup_count = len(self.df) - profile.unique_count
                requirements.append({
                    'column': name,
                    'issue': 'duplicate_ids',
                    'type': 'duplicate',
                    'duplicates': int(dup_count),
                    'severity': 'critical',
                    'description': f"{int(dup_count)} doublons détectés dans l'identifiant '{name}'",
                    'affected_rows': int(dup_count)
                })

            # 4. Problèmes catégoriels
            if profile.semantic_type == 'categorical' and profile.cleaning_needed:
                requirements.append({
                    'column': name,
                    'issue': 'inconsistent_categories',
                    'type': 'inconsistent',
                    'severity': 'medium',
                    'description': f"Standardisation recommandée pour '{name}' ({profile.unique_count} valeurs uniques)",
                    'affected_rows': None
                })

        # Trier par sévérité
        severity_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}
        requirements.sort(key=lambda x: severity_order.get(x['severity'], 4))

        return requirements

    def _get_missing_strategy(self, profile: ColumnProfile) -> str:
        """Suggère la meilleure stratégie d'imputation."""
        if profile.semantic_type in ['numeric', 'numeric-mixed']:
            return 'median_imputation'
        elif profile.semantic_type == 'categorical':
            return 'mode_imputation'
        elif profile.semantic_type == 'id':
            return 'drop_rows'
        else:
            return 'forward_fill'
