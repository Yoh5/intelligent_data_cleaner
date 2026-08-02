"""Nettoyeur agentique — boucle *perceive → act → observe* pilotée par un objectif.

Fait passer le projet d'un simple conseiller à un **vrai agent** : au lieu de
proposer un nettoyage en un coup, il poursuit un objectif chiffré (le score de
qualité déterministe de `quality.py`) et itère :

    profiler (perçoit les problèmes)
      └─► le conseiller LLM CHOISIT la meilleure stratégie par problème (fail-open)
      └─► applique les correctifs sur le DataFrame (in-process)
      └─► RE-profiler les données nettoyées (observe) → nouveau score
      └─► recommence tant que score < cible ET budget non épuisé
    └─► garde la MEILLEURE version + une trace des passes

Les correctifs exécutés sont les snippets pandas de NOTRE moteur de règles
(`suggest._generate_strategies_for_issue`) — code de confiance, pas du LLM (qui ne
choisit qu'un index). Tout est fail-open : un correctif qui casse est ignoré, une
absence de clé LLM retombe sur la recommandation rule-based (index 0).
"""
import base64
import io
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd

from app.routers.generate import _build_complete_script, _validate_and_fix_code, _validate_syntax
from app.routers.suggest import IssueInput, _generate_strategies_for_issue
from app.services import advisor, quality
from app.services.profiler import DataProfiler

_SAFE_GLOBALS = {"pd": pd, "np": np}


async def _profile(content: bytes, filename: str) -> Tuple[Dict[str, Any], pd.DataFrame]:
    p = DataProfiler()
    analysis = await p.analyze_file(content, filename)
    return analysis, p.df


def _issue_input(issue: Dict[str, Any]) -> IssueInput:
    return IssueInput(
        type=issue.get("type") or issue.get("issue", "unknown"),
        column=issue.get("column"),
        severity=issue.get("severity", "medium"),
        description=issue.get("description"),
        semantic_type=issue.get("semantic_type"),
    )


def _advise(filename: str, analysis: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    """Fait choisir au LLM la meilleure stratégie par problème (fail-open → {})."""
    issues = analysis.get("issues", []) or []
    cols = analysis.get("columns", {}) or {}
    column_types = {name: info.get("semantic_type", "") for name, info in cols.items()}
    sample_data = {name: info.get("sample_values", []) for name, info in cols.items()}
    advisor_issues = []
    for issue in issues:
        ii = _issue_input(issue)
        advisor_issues.append({
            "type": ii.type, "column": ii.column,
            "semantic_type": ii.semantic_type, "severity": ii.severity,
            "strategies": _generate_strategies_for_issue(ii),
        })
    return advisor.advise(filename, column_types, advisor_issues, sample_data)


def _apply(df: pd.DataFrame, strategy: Dict[str, Any]) -> pd.DataFrame:
    """Exécute le snippet pandas de la stratégie sur `df` (in-process). Fail-open :
    si le correctif lève, on garde le df inchangé."""
    ns = {"df": df, "pd": pd, "np": np}
    try:
        exec(strategy["code_preview"], _SAFE_GLOBALS, ns)  # code = moteur de règles (de confiance)
        out = ns.get("df", df)
        return out if isinstance(out, pd.DataFrame) else df
    except Exception:
        return df


def _csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


async def run(content: bytes, filename: str, target: int = 90, max_iters: int = 3) -> Dict[str, Any]:
    """Boucle agentique de nettoyage. Retourne la trace + le CSV nettoyé + un script
    reproductible."""
    analysis, df = await _profile(content, filename)
    start = quality.score_from_analysis(analysis)
    rows_before = analysis.get("dataset_info", {}).get("rows", len(df))
    issues_before = len(analysis.get("issues", []) or [])

    best_df, best_score, best_analysis = df.copy(), start["score"], analysis
    cur_df, cur_analysis = df, analysis
    iterations: List[Dict[str, Any]] = []
    all_steps: List[Dict[str, Any]] = []
    advisor_used = False

    for it in range(1, max(1, max_iters) + 1):
        issues = cur_analysis.get("issues", []) or []
        if not issues:
            break
        advice = _advise(filename, cur_analysis)
        advisor_used = advisor_used or bool(advice)

        for i, issue in enumerate(issues):
            strategies = _generate_strategies_for_issue(_issue_input(issue))
            if not strategies:
                continue
            rec = advice.get(i, {}).get("recommended", 0)
            rec = rec if 0 <= rec < len(strategies) else 0
            chosen = strategies[rec]
            cur_df = _apply(cur_df, chosen)
            all_steps.append({
                "column": issue.get("column"),
                "issue_type": issue.get("issue") or issue.get("type", "unknown"),
                "strategy_name": chosen["name"],
                "rationale": advice.get(i, {}).get("rationale", ""),
                "code": chosen["code_preview"],
            })

        cur_analysis, cur_df = await _profile(_csv_bytes(cur_df), "cleaned.csv")
        score = quality.score_from_analysis(cur_analysis)["score"]
        iterations.append({
            "iter": it,
            "quality": score,
            "issues_remaining": len(cur_analysis.get("issues", []) or []),
        })
        # garde la MEILLEURE version ; à score égal, préfère la version nettoyée
        # (au moins aussi propre que l'original brut)
        if score >= best_score:
            best_df, best_score, best_analysis = cur_df.copy(), score, cur_analysis
        if score >= target:
            break

    script = _assemble_script(filename, all_steps)
    return {
        "quality_start": start["score"],
        "quality_final": best_score,
        "target": target,
        "iterations": iterations,
        "steps": [{k: s[k] for k in ("column", "issue_type", "strategy_name", "rationale")}
                  for s in all_steps],
        "issues_before": issues_before,
        "issues_after": len(best_analysis.get("issues", []) or []),
        "rows_before": rows_before,
        "rows_after": len(best_df),
        "advisor_used": advisor_used,
        "script": script,
        "cleaned_csv_base64": base64.b64encode(_csv_bytes(best_df)).decode(),
        "message": _message(start["score"], best_score, issues_before,
                            len(best_analysis.get("issues", []) or [])),
    }


def _assemble_script(filename: str, steps: List[Dict[str, Any]]) -> str | None:
    """Script pandas reproductible à partir des correctifs appliqués (None si vide
    ou syntaxe invalide)."""
    if not steps:
        return None
    try:
        validated = [{
            "column": s["column"],
            "issue_type": s["issue_type"],
            "strategy_name": s["strategy_name"],
            "rationale": s.get("rationale", ""),
            "code": _validate_and_fix_code(s["code"], s["column"], s["issue_type"]),
            "step_number": idx + 1,
        } for idx, s in enumerate(steps)]
        script = _build_complete_script(filename, validated)
        ok, _ = _validate_syntax(script)
        return script if ok else None
    except Exception:
        return None


def _message(start: int, final: int, issues_before: int, issues_after: int) -> str:
    if issues_before == 0:
        return "Aucun problème détecté — le dataset est déjà propre."
    return (f"Qualité {start}/100 → {final}/100 · "
            f"{issues_before - issues_after} problème(s) résolu(s) sur {issues_before}.")
