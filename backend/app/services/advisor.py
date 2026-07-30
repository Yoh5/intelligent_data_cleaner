"""Conseiller LLM — le cœur *agentique* du nettoyage.

Le moteur de stratégies (`routers/suggest.py`) est déterministe : il propose 2 à 4
stratégies par problème et recommandait toujours la première. Ce module ajoute un
vrai RAISONNEMENT : le LLM lit le contexte réel du dataset (types de colonnes,
problèmes détectés, échantillon) et, pour chaque problème, **choisit la meilleure
stratégie parmi les candidates ET la justifie** en une phrase.

Agent, pas script : il décide en fonction des données, pas d'une règle figée.

**Fail-open total** : sans `OPENAI_API_KEY`, en cas d'erreur réseau/LLM ou de JSON
illisible, `advise()` renvoie `{}` → l'appelant conserve le comportement rule-based
(recommended = 0). L'app n'échoue jamais à cause du LLM.
"""
import json
import logging
import os
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

def _api_key() -> str:
    """Clé OpenAI : variable d'env (tests/prod) sinon settings (.env chargé par
    pydantic). "" → l'app reste 100% rule-based."""
    k = os.getenv("OPENAI_API_KEY", "").strip()
    if k:
        return k
    try:
        from app.config import get_settings
        return (getattr(get_settings(), "OPENAI_API_KEY", "") or "").strip()
    except Exception:
        return ""


def _model() -> str:
    return os.getenv("OPENAI_MODEL", "").strip() or "gpt-4o-mini"


def is_enabled() -> bool:
    """Vrai si une clé OpenAI est disponible (sinon on reste 100% rule-based)."""
    return bool(_api_key())


def _call_llm(prompt: str) -> str:
    """Appel LLM isolé (monkeypatché dans les tests). Retourne le texte brut.
    Lève en cas d'erreur — capturé par advise()."""
    from openai import OpenAI
    client = OpenAI(api_key=_api_key())
    resp = client.chat.completions.create(
        model=_model(),
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
        max_tokens=800,
        temperature=0,
    )
    return resp.choices[0].message.content or ""


def _build_prompt(dataset_name: str, column_types: Dict[str, str],
                  issues: List[Dict[str, Any]], sample_data: Dict[str, List[Any]] = None) -> str:
    cols = ", ".join(f"{c} ({t})" for c, t in (column_types or {}).items()) or "—"
    blocks = []
    for i, iss in enumerate(issues):
        strat_lines = []
        for j, s in enumerate(iss.get("strategies", [])):
            pros = ", ".join(s.get("pros", []) or [])
            cons = ", ".join(s.get("cons", []) or [])
            strat_lines.append(f"    [{j}] {s.get('name', '?')} — pour: {pros} | contre: {cons}")
        blocks.append(
            f"Problème {i} — type={iss.get('type')} colonne='{iss.get('column')}' "
            f"(sémantique={iss.get('semantic_type')}, sévérité={iss.get('severity')})\n"
            + "\n".join(strat_lines)
        )
    issues_block = "\n\n".join(blocks)
    sample = ""
    if sample_data:
        preview = {k: v[:5] for k, v in list(sample_data.items())[:8]}
        sample = f"\nÉchantillon de données :\n{json.dumps(preview, ensure_ascii=False, default=str)[:1200]}\n"

    return (
        "Tu es un ingénieur data. Pour un jeu de données à nettoyer, choisis la MEILLEURE "
        "stratégie parmi les candidates proposées pour CHAQUE problème, en fonction du type "
        "de colonne, de la sévérité et de l'échantillon réel. Justifie en UNE phrase courte.\n\n"
        f"Dataset : {dataset_name}\nColonnes : {cols}\n{sample}\n"
        f"Problèmes et stratégies candidates :\n{issues_block}\n\n"
        "Réponds UNIQUEMENT en JSON :\n"
        '{"choices": [{"issue": <index_problème>, "recommended": <index_stratégie>, '
        '"rationale": "<justification courte>"}]}'
    )


def advise(dataset_name: str, column_types: Dict[str, str],
           issues: List[Dict[str, Any]], sample_data: Dict[str, List[Any]] = None) -> Dict[int, Dict[str, Any]]:
    """Renvoie {index_problème: {"recommended": int, "rationale": str}} d'après le
    raisonnement du LLM. `{}` si LLM indisponible/erreur/JSON illisible (l'appelant
    garde alors recommended=0). Les index de stratégie hors bornes sont ignorés."""
    if not is_enabled() or not issues:
        return {}
    try:
        raw = _call_llm(_build_prompt(dataset_name, column_types, issues, sample_data))
    except Exception as e:  # réseau, quota, auth… — fail-open
        logger.warning("Conseiller LLM indisponible (%s) — repli rule-based", e)
        return {}

    try:
        data = json.loads(raw)
    except (ValueError, TypeError):
        logger.warning("Conseiller LLM : JSON illisible — repli rule-based")
        return {}

    out: Dict[int, Dict[str, Any]] = {}
    for ch in (data.get("choices") if isinstance(data, dict) else None) or []:
        try:
            i = int(ch.get("issue"))
            rec = int(ch.get("recommended"))
        except (TypeError, ValueError):
            continue
        if not (0 <= i < len(issues)):
            continue
        n_strat = len(issues[i].get("strategies", []))
        if not (0 <= rec < n_strat):
            continue
        out[i] = {"recommended": rec, "rationale": str(ch.get("rationale", "")).strip()}
    return out
