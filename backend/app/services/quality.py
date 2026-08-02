"""Score de qualité des données — déterministe, testable, sans réseau.

Sert de **fonction objectif** à la boucle agentique (`agent_cleaner`) : on mesure
la qualité AVANT/APRÈS chaque passe de nettoyage pour savoir si l'agent progresse.

Le score (0–100) part de 100 et retranche une pénalité par problème détecté,
pondérée par la sévérité renvoyée par le `DataProfiler`. C'est transparent (même
notion de « problème » que celle affichée à l'utilisateur) et monotone : corriger
un problème fait monter le score.
"""
from typing import Any, Dict

SEVERITY_PENALTY = {"critical": 15, "high": 8, "medium": 4, "low": 1}


def score_from_analysis(analysis: Dict[str, Any]) -> Dict[str, Any]:
    """Retourne {score, issues, penalty, by_severity, rows, columns} à partir du
    résultat d'un `DataProfiler.analyze_file`."""
    issues = analysis.get("issues", []) or []
    info = analysis.get("dataset_info", {}) or {}
    by_sev = {"critical": 0, "high": 0, "medium": 0, "low": 0}
    penalty = 0
    for it in issues:
        sev = it.get("severity", "low")
        by_sev[sev] = by_sev.get(sev, 0) + 1
        penalty += SEVERITY_PENALTY.get(sev, 1)
    return {
        "score": max(0, 100 - penalty),
        "issues": len(issues),
        "penalty": penalty,
        "by_severity": by_sev,
        "rows": info.get("rows"),
        "columns": info.get("columns"),
    }


def completeness(df) -> float:
    """Part de cellules non manquantes (0..1) — mesure déterministe complémentaire."""
    rows, cols = df.shape
    cells = rows * cols
    if cells == 0:
        return 1.0
    missing = int(df.isna().sum().sum())
    return 1 - missing / cells
