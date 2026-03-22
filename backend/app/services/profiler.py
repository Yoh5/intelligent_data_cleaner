#!/usr/bin/env python3
"""
Profiler ultra-robuste pour détecter tous les problèmes de qualité de données.
"""
import pandas as pd
import numpy as np
import io
from typing import Dict, List, Any, Optional
from datetime import datetime

class DataProfiler:
    """Analyse complète d'un dataset."""

    # Patterns de valeurs manquantes
    MISSING_PATTERNS = [
        '', ' ', '  ', '   ', '\t', '\n', 'NA', 'N/A', 'n/a', 'na',
        'NULL', 'null', 'None', 'none', 'NaN', 'nan', 'MISSING', 'missing',
        '-', '--', '---', '???', 'Unknown', 'unknown', 'Not Available'
    ]

    def __init__(self):
        self.df = None
        self.issues = []

    async def analyze_file(self, content: bytes, filename: str) -> Dict[str, Any]:
        """Analyse complète d'un fichier CSV/Excel."""
        try:
            # Chargement avec détection auto
            self.df = await self._load_file(content, filename)

            if self.df is None or self.df.empty:
                raise ValueError("Fichier vide")

            # Analyse colonne par colonne
            columns_info = {}
            for col in self.df.columns:
                columns_info[col] = self._analyze_column(col)

            # Détection des problèmes
            self.issues = self._detect_issues(columns_info)

            return {
                "id": str(datetime.now().timestamp()),
                "created_at": datetime.now().isoformat(),
                "dataset_info": {
                    "filename": filename,
                    "rows": len(self.df),
                    "columns": len(self.df.columns),
                    "size_bytes": len(content)
                },
                "shape": [len(self.df), len(self.df.columns)],
                "columns": columns_info,
                "issues": self.issues,
                "raw_profile": {
                    "dtypes": {col: str(self.df[col].dtype) for col in self.df.columns},
                    "total_missing": int(self.df.isna().sum().sum())
                }
            }

        except Exception as e:
            import traceback
            print(f"ERREUR ANALYSE: {traceback.format_exc()}")
            raise

    async def _load_file(self, content: bytes, filename: str) -> pd.DataFrame:
        """Chargement robuste avec auto-détection."""
        filename = filename.lower()

        if filename.endswith('.csv'):
            # Essayer différents encodages
            for encoding in ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']:
                try:
                    # Détection du délimiteur
                    sample = content[:2000].decode(encoding, errors='ignore')
                    delimiter = ','
                    if sample.count(';') > sample.count(','):
                        delimiter = ';'

                    df = pd.read_csv(
                        io.BytesIO(content),
                        encoding=encoding,
                        delimiter=delimiter,
                        na_values=self.MISSING_PATTERNS,
                        keep_default_na=True,
                        low_memory=False
                    )

                    # Normaliser les noms de colonnes
                    df.columns = df.columns.astype(str).str.strip().str.lower().str.replace(' ', '_')
                    return df

                except Exception as e:
                    continue

            raise ValueError("Impossible de lire le CSV")

        elif filename.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(io.BytesIO(content))
            df.columns = df.columns.astype(str).str.strip().str.lower().str.replace(' ', '_')
            return df

        else:
            raise ValueError("Format non supporté")

    def _analyze_column(self, col: str) -> Dict[str, Any]:
        """Analyse détaillée d'une colonne."""
        series = self.df[col]

        # Type actuel
        dtype = str(series.dtype)

        # Valeurs manquantes
        missing_count = int(series.isna().sum())
        missing_rate = float(missing_count / len(series))

        # Valeurs uniques
        unique_count = int(series.nunique())
        unique_rate = float(unique_count / len(series))

        # Inférence du type sémantique
        semantic_type = self._infer_semantic_type(series, col)

        # Échantillon de valeurs
        sample_values = series.dropna().head(5).tolist()

        return {
            "name": col,
            "dtype": dtype,
            "missing_count": missing_count,
            "missing_rate": missing_rate,
            "unique_count": unique_count,
            "unique_rate": unique_rate,
            "semantic_type": semantic_type,
            "sample_values": sample_values
        }

    def _infer_semantic_type(self, series: pd.Series, col_name: str) -> str:
        """Infère le type sémantique."""
        if pd.api.types.is_datetime64_any_dtype(series):
            return "datetime"

        if pd.api.types.is_numeric_dtype(series):
            return "numeric"

        # Test si numérique caché
        if series.dtype == 'object':
            # Essayer de convertir un échantillon
            sample = series.dropna().head(100)
            if len(sample) > 0:
                converted = pd.to_numeric(sample.astype(str).str.replace(',', '.').str.replace(' ', ''), errors='coerce')
                if converted.notna().sum() / len(sample) > 0.7:
                    return "numeric-mixed"

                # Test datetime
                try:
                    pd.to_datetime(sample, errors='raise')
                    return "datetime-mixed"
                except:
                    pass

        # ID potentiel
        if any(x in col_name.lower() for x in ['id', 'code', 'ref']):
            if series.nunique() / len(series) > 0.9:
                return "id"

        # Catégoriel
        if series.nunique() < 20 or (series.nunique() / len(series)) < 0.05:
            return "categorical"

        return "text"

    def _detect_issues(self, columns_info: Dict) -> List[Dict]:
        """Détecte tous les problèmes."""
        issues = []

        for col, info in columns_info.items():
            # 1. Valeurs manquantes
            if info["missing_count"] > 0:
                severity = "critical" if info["missing_rate"] > 0.3 else "high" if info["missing_rate"] > 0.1 else "medium"
                issues.append({
                    "column": col,
                    "issue": "missing_values",
                    "type": "missing",
                    "severity": severity,
                    "count": info["missing_count"],
                    "rate": info["missing_rate"],
                    "description": f"{info['missing_count']} valeurs manquantes ({info['missing_rate']*100:.1f}%)",
                    "affected_rows": info["missing_count"],
                    "semantic_type": info["semantic_type"]
                })

            # 2. Types mixtes
            if info["semantic_type"] == "numeric-mixed":
                issues.append({
                    "column": col,
                    "issue": "mixed_types",
                    "type": "inconsistent",
                    "severity": "high",
                    "description": f"Colonne '{col}' contient des nombres et du texte",
                    "semantic_type": info["semantic_type"]
                })

            # 3. Doublons potentiels (pour IDs)
            if info["semantic_type"] == "id" and info["unique_count"] < len(self.df):
                dup_count = len(self.df) - info["unique_count"]
                issues.append({
                    "column": col,
                    "issue": "duplicate_ids",
                    "type": "duplicate",
                    "severity": "critical",
                    "description": f"{dup_count} doublons détectés dans l'ID '{col}'",
                    "affected_rows": dup_count
                })

            # 4. Problèmes catégoriels
            if info["semantic_type"] == "categorical" and info["unique_count"] > 10:
                issues.append({
                    "column": col,
                    "issue": "high_cardinality",
                    "type": "inconsistent",
                    "severity": "medium",
                    "description": f"{info['unique_count']} valeurs uniques (cardinalité élevée)",
                    "semantic_type": info["semantic_type"]
                })

        # Trier par sévérité
        severity_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
        issues.sort(key=lambda x: severity_order.get(x["severity"], 4))

        return issues
