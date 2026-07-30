from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import logging

from app.services import advisor

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/suggest", tags=["suggestions"])


class IssueInput(BaseModel):
    type: str
    column: Optional[str] = None
    severity: str = "medium"
    description: Optional[str] = None
    semantic_type: Optional[str] = None


class SuggestionRequest(BaseModel):
    dataset_name: str
    column_types: Dict[str, str]
    issues: List[IssueInput]
    sample_data: Optional[Dict[str, List[Any]]] = None


@router.post("/batch")
async def get_suggestions_batch(request: SuggestionRequest):
    try:
        results = []
        advisor_issues = []
        for issue in request.issues:
            strategies = _generate_strategies_for_issue(issue)
            results.append({
                "issue": {
                    "type": issue.type,
                    "column": issue.column,
                    "severity": issue.severity,
                    "description": issue.description or f"Problème {issue.type}",
                },
                "strategies": strategies,
                "recommended": 0,      # défaut rule-based (repli si LLM indispo)
            })
            advisor_issues.append({
                "type": issue.type, "column": issue.column,
                "semantic_type": issue.semantic_type, "severity": issue.severity,
                "strategies": strategies,
            })

        # Couche agentique : le LLM raisonne sur les données réelles pour choisir
        # ET justifier la meilleure stratégie par problème. Fail-open → {} = on
        # conserve recommended=0 (comportement rule-based inchangé).
        advice = advisor.advise(request.dataset_name, request.column_types,
                                advisor_issues, request.sample_data)
        for i, rec in advice.items():
            results[i]["recommended"] = rec["recommended"]
            results[i]["rationale"] = rec["rationale"]

        return {
            "results": results,
            "total_issues": len(results),
            "total_strategies": sum(len(r["strategies"]) for r in results),
            "advisor_used": bool(advice),
        }
    except Exception as e:
        logger.error(f"Erreur suggestions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _generate_strategies_for_issue(issue: IssueInput) -> List[Dict]:
    col = issue.column or "colonne"
    semantic = issue.semantic_type or "numeric"
    strategies = []

    # ── Valeurs manquantes ────────────────────────────────────────────────────
    if issue.type in ("missing", "missing_values"):

        if semantic in ("numeric", "numeric-mixed"):
            strategies.append({
                "name": "Imputation par médiane",
                "description": f"Remplace les NaN de '{col}' par la médiane (robuste aux outliers)",
                "pros": ["Robuste aux valeurs aberrantes", "Préserve la distribution"],
                "cons": ["Réduit légèrement la variance"],
                "code_preview": f"if '{col}' in df.columns:\n"
                                f"    df['{col}'] = pd.to_numeric(df['{col}'], errors='coerce')\n"
                                f"    df['{col}'].fillna(df['{col}'].median(), inplace=True)",
                "confidence": "high",
            })
            strategies.append({
                "name": "Imputation par moyenne",
                "description": f"Remplace les NaN de '{col}' par la moyenne",
                "pros": ["Simple", "Adapté aux distributions symétriques"],
                "cons": ["Sensible aux outliers"],
                "code_preview": f"if '{col}' in df.columns:\n"
                                f"    df['{col}'] = pd.to_numeric(df['{col}'], errors='coerce')\n"
                                f"    df['{col}'].fillna(df['{col}'].mean(), inplace=True)",
                "confidence": "medium",
            })
            strategies.append({
                "name": "Propagation avant (ffill)",
                "description": "Propage la dernière valeur connue vers le bas",
                "pros": ["Conserve la tendance temporelle"],
                "cons": ["Suppose une continuité dans les données"],
                "code_preview": f"if '{col}' in df.columns:\n"
                                f"    df['{col}'] = pd.to_numeric(df['{col}'], errors='coerce')\n"
                                f"    df['{col}'].ffill(inplace=True)\n"
                                f"    df['{col}'].fillna(0, inplace=True)  # fallback si premières lignes NaN",
                "confidence": "medium",
            })
            strategies.append({
                "name": "Supprimer les lignes manquantes",
                "description": f"Supprime les lignes où '{col}' est NaN",
                "pros": ["Données 100% complètes"],
                "cons": ["Perte de données si beaucoup de NaN"],
                "code_preview": f"df.dropna(subset=['{col}'], inplace=True)",
                "confidence": "low",
            })

        elif semantic in ("categorical", "text"):
            strategies.append({
                "name": "Imputation par mode",
                "description": f"Remplace les NaN par la valeur la plus fréquente de '{col}'",
                "pros": ["Respecte la distribution catégorielle"],
                "cons": ["Sur-représente la valeur dominante"],
                "code_preview": f"if '{col}' in df.columns:\n"
                                f"    mode_val = df['{col}'].mode()\n"
                                f"    if not mode_val.empty:\n"
                                f"        df['{col}'].fillna(mode_val[0], inplace=True)",
                "confidence": "high",
            })
            strategies.append({
                "name": "Remplir par 'Inconnu'",
                "description": "Remplace les NaN par la constante 'Inconnu'",
                "pros": ["Transparent", "Aucune perte de lignes"],
                "cons": ["Crée une catégorie artificielle"],
                "code_preview": f"if '{col}' in df.columns:\n"
                                f"    df['{col}'].fillna('Inconnu', inplace=True)",
                "confidence": "medium",
            })
            strategies.append({
                "name": "Supprimer les lignes manquantes",
                "description": f"Supprime les lignes où '{col}' est NaN",
                "pros": ["Données complètes"],
                "cons": ["Perte potentielle de données"],
                "code_preview": f"df.dropna(subset=['{col}'], inplace=True)",
                "confidence": "low",
            })

        elif semantic in ("datetime", "datetime-mixed"):
            strategies.append({
                "name": "Propagation avant (ffill)",
                "description": "Propage la date précédente dans les NaN",
                "pros": ["Naturel pour les séries temporelles"],
                "cons": ["Peut créer des doublons de dates"],
                "code_preview": f"if '{col}' in df.columns:\n"
                                f"    df['{col}'] = pd.to_datetime(df['{col}'], errors='coerce')\n"
                                f"    df['{col}'].ffill(inplace=True)",
                "confidence": "high",
            })
            strategies.append({
                "name": "Supprimer les lignes sans date",
                "description": f"Supprime les lignes où '{col}' est manquante",
                "pros": ["Dates toujours valides"],
                "cons": ["Perte de données"],
                "code_preview": f"df.dropna(subset=['{col}'], inplace=True)",
                "confidence": "medium",
            })

        else:
            strategies.append({
                "name": "Imputation par médiane",
                "description": f"Remplace les NaN de '{col}' par la médiane",
                "pros": ["Robuste aux outliers"],
                "cons": ["Peut ne pas être adapté si colonne non-numérique"],
                "code_preview": f"if '{col}' in df.columns:\n"
                                f"    df['{col}'] = pd.to_numeric(df['{col}'], errors='coerce')\n"
                                f"    df['{col}'].fillna(df['{col}'].median(), inplace=True)",
                "confidence": "medium",
            })

    # ── Types mixtes ──────────────────────────────────────────────────────────
    elif issue.type in ("inconsistent", "mixed_types"):
        strategies.append({
            "name": "Conversion numérique",
            "description": f"Convertit '{col}' en float (gère virgules et espaces)",
            "pros": ["Permet les calculs", "Standardise le format"],
            "cons": ["Les valeurs non-convertibles deviennent NaN"],
            "code_preview": f"if '{col}' in df.columns:\n"
                            f"    df['{col}'] = (\n"
                            f"        df['{col}'].astype(str)\n"
                            f"        .str.replace(',', '.', regex=False)\n"
                            f"        .str.replace(' ', '', regex=False)\n"
                            f"        .str.replace('€', '', regex=False)\n"
                            f"        .str.replace('$', '', regex=False)\n"
                            f"    )\n"
                            f"    df['{col}'] = pd.to_numeric(df['{col}'], errors='coerce')",
            "confidence": "high",
        })
        strategies.append({
            "name": "Conversion en texte propre",
            "description": f"Standardise '{col}' en chaîne de caractères nettoyée",
            "pros": ["Aucune perte de données", "Format uniforme"],
            "cons": ["La colonne reste en type objet (non numérique)"],
            "code_preview": f"if '{col}' in df.columns:\n"
                            f"    df['{col}'] = df['{col}'].astype(str).str.strip()",
            "confidence": "medium",
        })

    # ── Doublons ──────────────────────────────────────────────────────────────
    elif issue.type in ("duplicate", "duplicate_rows", "duplicate_ids"):
        strategies.append({
            "name": "Supprimer les doublons (garder premier)",
            "description": "Conserve la première occurrence de chaque doublon",
            "pros": ["Données uniques", "Préserve l'ordre d'arrivée"],
            "cons": ["Perte potentielle d'informations"],
            "code_preview": "df.drop_duplicates(keep='first', inplace=True)",
            "confidence": "high",
        })
        strategies.append({
            "name": "Supprimer les doublons (garder dernier)",
            "description": "Conserve la dernière occurrence (utile si les lignes récentes prévalent)",
            "pros": ["Données plus récentes conservées"],
            "cons": ["Peut perdre des entrées initiales valides"],
            "code_preview": "df.drop_duplicates(keep='last', inplace=True)",
            "confidence": "medium",
        })

    # ── Valeurs aberrantes ────────────────────────────────────────────────────
    elif issue.type == "outlier":
        strategies.append({
            "name": "Écrêtage IQR (3×)",
            "description": f"Plafonne '{col}' aux bornes Q1−3·IQR / Q3+3·IQR",
            "pros": ["Conserve toutes les lignes", "Atténue l'effet des outliers"],
            "cons": ["Modifie les valeurs extrêmes réelles"],
            "code_preview": f"if '{col}' in df.columns and pd.api.types.is_numeric_dtype(df['{col}']):\n"
                            f"    q1, q3 = df['{col}'].quantile(0.25), df['{col}'].quantile(0.75)\n"
                            f"    iqr = q3 - q1\n"
                            f"    df['{col}'] = df['{col}'].clip(lower=q1 - 3*iqr, upper=q3 + 3*iqr)",
            "confidence": "high",
        })
        strategies.append({
            "name": "Écrêtage percentile (1 % – 99 %)",
            "description": f"Plafonne '{col}' au 1er et 99e percentile",
            "pros": ["Approche intuitive", "Conserve toutes les lignes"],
            "cons": ["Seuils arbitraires"],
            "code_preview": f"if '{col}' in df.columns and pd.api.types.is_numeric_dtype(df['{col}']):\n"
                            f"    p1, p99 = df['{col}'].quantile(0.01), df['{col}'].quantile(0.99)\n"
                            f"    df['{col}'] = df['{col}'].clip(lower=p1, upper=p99)",
            "confidence": "medium",
        })
        strategies.append({
            "name": "Supprimer les lignes aberrantes",
            "description": f"Supprime les lignes dont '{col}' dépasse Q1−3·IQR ou Q3+3·IQR",
            "pros": ["Données 100% dans la plage normale"],
            "cons": ["Perte de lignes potentiellement valides"],
            "code_preview": f"if '{col}' in df.columns and pd.api.types.is_numeric_dtype(df['{col}']):\n"
                            f"    q1, q3 = df['{col}'].quantile(0.25), df['{col}'].quantile(0.75)\n"
                            f"    iqr = q3 - q1\n"
                            f"    mask = (df['{col}'] >= q1 - 3*iqr) & (df['{col}'] <= q3 + 3*iqr)\n"
                            f"    df = df[mask]",
            "confidence": "low",
        })

    # ── Espaces superflus ─────────────────────────────────────────────────────
    elif issue.type in ("whitespace", "string_issue"):
        strategies.append({
            "name": "Supprimer les espaces (strip)",
            "description": f"Retire les espaces en début et fin de '{col}'",
            "pros": ["Non destructif", "Rapide"],
            "cons": ["Ne normalise pas la casse"],
            "code_preview": f"if '{col}' in df.columns:\n"
                            f"    df['{col}'] = df['{col}'].astype(str).str.strip()",
            "confidence": "high",
        })
        strategies.append({
            "name": "Strip + minuscules",
            "description": f"Retire les espaces et met '{col}' en minuscules",
            "pros": ["Normalisation complète", "Élimine les doublons de casse"],
            "cons": ["Perd la casse originale"],
            "code_preview": f"if '{col}' in df.columns:\n"
                            f"    df['{col}'] = df['{col}'].astype(str).str.strip().str.lower()",
            "confidence": "medium",
        })

    # ── Dates stockées en texte ───────────────────────────────────────────────
    elif issue.type == "date_as_string":
        strategies.append({
            "name": "Convertir en datetime",
            "description": f"Parse '{col}' en type datetime Pandas",
            "pros": ["Permet les calculs de dates", "Tri chronologique correct"],
            "cons": ["Les formats non reconnus deviennent NaT"],
            "code_preview": f"if '{col}' in df.columns:\n"
                            f"    df['{col}'] = pd.to_datetime(df['{col}'], errors='coerce', dayfirst=True)",
            "confidence": "high",
        })

    else:
        strategies.append({
            "name": "Nettoyage générique",
            "description": f"Strip et nettoyage de base sur '{col}'",
            "pros": ["Sûr", "Non destructif"],
            "cons": ["Générique"],
            "code_preview": f"if '{col}' in df.columns:\n"
                            f"    df['{col}'] = df['{col}'].astype(str).str.strip()",
            "confidence": "medium",
        })

    return strategies
