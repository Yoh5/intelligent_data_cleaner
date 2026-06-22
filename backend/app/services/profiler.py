#!/usr/bin/env python3
"""
Profiler ultra-robuste pour détecter tous les problèmes de qualité de données.
"""
import io
from datetime import datetime
from typing import Any, Dict, List

import numpy as np
import pandas as pd


def _safe_value(v: Any) -> Any:
    """Convert numpy scalars and Timestamps to JSON-serializable Python types."""
    if v is None:
        return None
    if isinstance(v, float) and (v != v):  # NaN
        return None
    if hasattr(v, "item"):  # numpy scalar
        return v.item()
    if hasattr(v, "isoformat"):  # Timestamp / datetime
        return v.isoformat()
    return v


class DataProfiler:
    """Analyse complète d'un dataset."""

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
            self.df = await self._load_file(content, filename)

            if self.df is None or self.df.empty:
                raise ValueError("Fichier vide")

            columns_info = {}
            for col in self.df.columns:
                columns_info[col] = self._analyze_column(col)

            self.issues = self._detect_issues(columns_info)

            # Sample rows — first 5 rows, fully JSON-serializable
            sample_rows = [
                {k: _safe_value(v) for k, v in row.items()}
                for row in self.df.head(5).to_dict(orient="records")
            ]

            return {
                "id": str(datetime.now().timestamp()),
                "created_at": datetime.now().isoformat(),
                "dataset_info": {
                    "filename": filename,
                    "rows": len(self.df),
                    "columns": len(self.df.columns),
                    "size_bytes": len(content),
                },
                "shape": [len(self.df), len(self.df.columns)],
                "columns": columns_info,
                "issues": self.issues,
                "raw_profile": {
                    "dtypes": {col: str(self.df[col].dtype) for col in self.df.columns},
                    "total_missing": int(self.df.isna().sum().sum()),
                    "sample_rows": sample_rows,
                },
            }

        except Exception as e:
            import traceback
            print(f"ERREUR ANALYSE: {traceback.format_exc()}")
            raise

    async def _load_file(self, content: bytes, filename: str) -> pd.DataFrame:
        """Chargement robuste avec auto-détection."""
        filename_lower = filename.lower()

        if filename_lower.endswith(".csv"):
            for encoding in ["utf-8", "latin-1", "iso-8859-1", "cp1252"]:
                try:
                    sample = content[:2000].decode(encoding, errors="ignore")
                    delimiter = ";" if sample.count(";") > sample.count(",") else ","
                    df = pd.read_csv(
                        io.BytesIO(content),
                        encoding=encoding,
                        delimiter=delimiter,
                        na_values=self.MISSING_PATTERNS,
                        keep_default_na=True,
                        low_memory=False,
                    )
                    df.columns = self._sanitize_columns(df.columns)
                    return df
                except Exception:
                    continue
            raise ValueError("Impossible de lire le CSV")

        elif filename_lower.endswith((".xlsx", ".xls")):
            df = pd.read_excel(io.BytesIO(content))
            df.columns = self._sanitize_columns(df.columns)
            return df

        else:
            raise ValueError("Format non supporté")

    @staticmethod
    def _sanitize_columns(columns) -> pd.Index:
        """Normalize column names to valid, safe Python identifiers."""
        return (
            columns.astype(str)
            .str.strip()
            .str.lower()
            .str.replace(r"['\"]", "", regex=True)       # remove quotes → safe in string literals
            .str.replace(r"\s+", "_", regex=True)        # spaces → underscore
            .str.replace(r"[^a-z0-9_]", "_", regex=True) # other special → underscore
            .str.replace(r"_+", "_", regex=True)          # collapse multiple underscores
            .str.strip("_")                               # strip leading/trailing underscores
        )

    def _analyze_column(self, col: str) -> Dict[str, Any]:
        """Analyse détaillée d'une colonne."""
        series = self.df[col]
        n = len(series)

        dtype = str(series.dtype)
        missing_count = int(series.isna().sum())
        missing_rate = float(missing_count / n) if n > 0 else 0.0
        unique_count = int(series.nunique())
        unique_rate = float(unique_count / n) if n > 0 else 0.0
        semantic_type = self._infer_semantic_type(series, col)

        # Safely convert sample values (handles numpy int64, float64, etc.)
        sample_values = [_safe_value(v) for v in series.dropna().head(5)]

        return {
            "name": col,
            "dtype": dtype,
            "missing_count": missing_count,
            "missing_rate": missing_rate,
            "unique_count": unique_count,
            "unique_rate": unique_rate,
            "semantic_type": semantic_type,
            "sample_values": sample_values,
        }

    def _infer_semantic_type(self, series: pd.Series, col_name: str) -> str:
        """Infère le type sémantique."""
        if pd.api.types.is_datetime64_any_dtype(series):
            return "datetime"

        if pd.api.types.is_numeric_dtype(series):
            return "numeric"

        if series.dtype == "object":
            sample = series.dropna().head(100)
            if len(sample) > 0:
                converted = pd.to_numeric(
                    sample.astype(str).str.replace(",", ".").str.replace(" ", ""),
                    errors="coerce",
                )
                if converted.notna().sum() / len(sample) > 0.7:
                    return "numeric-mixed"

                try:
                    pd.to_datetime(sample, errors="raise")
                    return "datetime-mixed"
                except Exception:
                    pass

        if any(x in col_name.lower() for x in ("id", "code", "ref")):
            n = len(series)
            if n > 0 and series.nunique() / n > 0.9:
                return "id"

        if series.nunique() < 20 or (len(series) > 0 and series.nunique() / len(series) < 0.05):
            return "categorical"

        return "text"

    def _detect_issues(self, columns_info: Dict) -> List[Dict]:
        """Détecte tous les problèmes de qualité de données."""
        issues = []

        # Global duplicate rows
        dup_rows = int(self.df.duplicated().sum())
        if dup_rows > 0:
            issues.append({
                "column": None,
                "issue": "duplicate_rows",
                "type": "duplicate",
                "severity": "high",
                "description": f"{dup_rows} lignes entièrement dupliquées dans le dataset",
                "affected_rows": dup_rows,
                "semantic_type": None,
            })

        for col, info in columns_info.items():
            # 1. Valeurs manquantes
            if info["missing_count"] > 0:
                rate = info["missing_rate"]
                severity = "critical" if rate > 0.3 else "high" if rate > 0.1 else "medium"
                issues.append({
                    "column": col,
                    "issue": "missing_values",
                    "type": "missing",
                    "severity": severity,
                    "count": info["missing_count"],
                    "rate": rate,
                    "description": f"{info['missing_count']} valeurs manquantes ({rate*100:.1f}%)",
                    "affected_rows": info["missing_count"],
                    "semantic_type": info["semantic_type"],
                })

            # 2. Types mixtes
            if info["semantic_type"] == "numeric-mixed":
                issues.append({
                    "column": col,
                    "issue": "mixed_types",
                    "type": "inconsistent",
                    "severity": "high",
                    "description": f"'{col}' contient des nombres et du texte mélangés",
                    "semantic_type": info["semantic_type"],
                })

            # 3. Doublons sur colonne ID
            if info["semantic_type"] == "id" and info["unique_count"] < len(self.df):
                dup_count = len(self.df) - info["unique_count"]
                issues.append({
                    "column": col,
                    "issue": "duplicate_ids",
                    "type": "duplicate",
                    "severity": "critical",
                    "description": f"{dup_count} doublons détectés dans l'identifiant '{col}'",
                    "affected_rows": dup_count,
                })

            # 4. Valeurs aberrantes (outliers) — colonnes numériques
            if info["semantic_type"] == "numeric":
                series = self.df[col].dropna()
                if len(series) >= 10:
                    q1, q3 = float(series.quantile(0.25)), float(series.quantile(0.75))
                    iqr = q3 - q1
                    if iqr > 0:
                        lower, upper = q1 - 3 * iqr, q3 + 3 * iqr
                        outlier_count = int(((series < lower) | (series > upper)).sum())
                        if 0 < outlier_count < len(series) * 0.3:
                            issues.append({
                                "column": col,
                                "issue": "outliers",
                                "type": "outlier",
                                "severity": "medium",
                                "description": f"{outlier_count} valeur(s) aberrante(s) dans '{col}' (règle 3×IQR)",
                                "affected_rows": outlier_count,
                                "semantic_type": "numeric",
                            })

            # 5. Espaces superflus (texte / catégoriel)
            if info["semantic_type"] in ("categorical", "text"):
                series = self.df[col].dropna().astype(str)
                ws_count = int((series != series.str.strip()).sum())
                if ws_count > 0:
                    issues.append({
                        "column": col,
                        "issue": "whitespace",
                        "type": "string_issue",
                        "severity": "low",
                        "description": f"{ws_count} valeur(s) avec espaces superflus dans '{col}'",
                        "affected_rows": ws_count,
                        "semantic_type": info["semantic_type"],
                    })

            # 6. Dates stockées comme texte
            if info["semantic_type"] == "datetime-mixed":
                issues.append({
                    "column": col,
                    "issue": "date_as_string",
                    "type": "inconsistent",
                    "severity": "medium",
                    "description": f"'{col}' contient des dates stockées en texte — conversion nécessaire",
                    "semantic_type": "datetime-mixed",
                })

        severity_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
        issues.sort(key=lambda x: severity_order.get(x.get("severity", "low"), 4))
        return issues
