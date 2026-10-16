from fastapi import APIRouter, UploadFile, File, HTTPException
from datetime import datetime

from app.services.profiler import DataProfiler
from app.services import advisor
from app.routers.suggest import _generate_strategies_for_issue, IssueInput
from app.routers.generate import _build_complete_script, _validate_and_fix_code, _validate_syntax

router = APIRouter(prefix="/auto-clean", tags=["auto-clean"])

#: Ce que pandas dit quand le contenu ne correspond pas à l'extension. On ne
#: répète pas sa phrase telle quelle : elle parle de moteurs et de `BytesIO`,
#: et elle peut contenir un morceau du fichier — donc des données.
_FORMAT = ("format cannot be determined", "no engine", "corrupt",
           "not a zip file", "unsupported format", "bad magic")


def _illisible(nom: str, e: Exception) -> str:
    """Pourquoi le fichier n'a pas pu s'ouvrir, et quoi faire — sans jamais
    recopier son contenu dans le message."""
    motif = str(e).lower()
    if any(m in motif for m in _FORMAT):
        return (f"« {nom} » porte une extension de classeur mais son contenu "
                f"n'en est pas un : il est tronqué, ou c'est un export au "
                f"format HTML ou CSV renommé. Réenregistrez-le depuis Excel "
                f"en .xlsx, ou déposez-le en .csv sous son vrai nom.")
    if "vide" in motif or "no columns" in motif or "empty" in motif:
        return (f"« {nom} » ne contient aucune ligne de données — un en-tête "
                f"seul, ou rien du tout.")
    if "codec" in motif or "decode" in motif:
        return (f"« {nom} » est dans un encodage qui n'a pas pu être deviné. "
                f"Réenregistrez-le en UTF-8.")
    return f"« {nom} » n'a pas pu être lu : {type(e).__name__}."


@router.post("/")
async def auto_clean(file: UploadFile = File(...)):
    """
    Autopilot: analyze → pick best strategy per issue → generate script, all in one call.
    Returns the same script format as /generate/ plus an analysis summary.
    """
    allowed_extensions = {".csv", ".xlsx", ".xls"}
    file_ext = file.filename.lower()[file.filename.rfind("."):]
    if file_ext not in allowed_extensions:
        raise HTTPException(status_code=400, detail=f"Unsupported file type: {file_ext}")

    content = await file.read()
    if len(content) > 100 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="File too large (max 100MB)")

    profiler = DataProfiler()
    try:
        analysis = await profiler.analyze_file(content, file.filename)
    except HTTPException:
        raise
    except Exception as e:
        # Un classeur tronqué, un .xls qui est en fait du HTML d'export, un
        # CSV dans un encodage que pandas n'ouvre pas : l'extension est
        # correcte et le contenu ne l'est pas. Sans ce filet, l'exception
        # remontait en 500 avec une trace — l'utilisateur apprenait qu'il
        # existe un fichier `_base.py` dans pandas, et rien d'autre.
        raise HTTPException(status_code=400, detail=_illisible(file.filename, e))

    issues = analysis.get("issues", [])
    if not issues:
        return {
            "script": None,
            "filename": None,
            "issues_found": 0,
            "steps_applied": 0,
            "message": "Aucun problème détecté — le dataset est déjà propre.",
            "analysis": {
                "rows": analysis["dataset_info"]["rows"],
                "columns": analysis["dataset_info"]["columns"],
                "issues": [],
            },
        }

    # Stratégies candidates par problème
    per_issue = []
    advisor_issues = []
    for issue in issues:
        issue_input = IssueInput(
            type=issue.get("type") or issue.get("issue", "unknown"),
            column=issue.get("column"),
            severity=issue.get("severity", "medium"),
            description=issue.get("description"),
            semantic_type=issue.get("semantic_type"),
        )
        strategies = _generate_strategies_for_issue(issue_input)
        per_issue.append((issue, strategies))
        advisor_issues.append({
            "type": issue_input.type, "column": issue_input.column,
            "semantic_type": issue_input.semantic_type, "severity": issue_input.severity,
            "strategies": strategies,
        })

    # Couche agentique : le LLM raisonne sur les données réelles et choisit la
    # meilleure stratégie par problème (fail-open → {} = on garde l'index 0).
    # `analysis["columns"]` est un DICT indexé par nom, pas une liste. Écrit ici
    # comme une liste, `for c in cols` itérait sur les clés — des chaînes — et
    # `c.get(...)` levait une AttributeError. La route tombait donc en 500 sur
    # tout fichier portant au moins un problème, c'est-à-dire sur tout fichier
    # qu'on lui donne à nettoyer : seul un fichier déjà propre passait, parce
    # qu'il sort plus haut. `agent_cleaner._advise` fait la même chose
    # correctement depuis le début ; c'est ce bloc-ci qui avait été recopié de
    # travers.
    cols = analysis.get("columns", {}) or {}
    column_types = {nom: info.get("semantic_type", "") for nom, info in cols.items()}
    sample_data = {nom: info.get("sample_values", []) for nom, info in cols.items()}
    advice = advisor.advise(file.filename, column_types, advisor_issues, sample_data)

    validated_steps = []
    for i, (issue, strategies) in enumerate(per_issue):
        if not strategies:
            continue
        rec = advice.get(i, {}).get("recommended", 0)
        chosen = strategies[rec]
        validated_steps.append({
            "column": issue.get("column"),
            "issue_type": issue.get("issue") or issue.get("type", "unknown"),
            "strategy_name": chosen["name"],
            "rationale": advice.get(i, {}).get("rationale", ""),
            "code": _validate_and_fix_code(chosen["code_preview"], issue.get("column"), issue.get("type", "")),
            "step_number": len(validated_steps) + 1,
        })

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = file.filename.replace(".", "_").replace(" ", "_")
    filename = f"auto_clean_{safe_name}_{timestamp}.py"

    script = _build_complete_script(file.filename, validated_steps)
    syntax_valid, syntax_error = _validate_syntax(script)
    if not syntax_valid:
        raise HTTPException(status_code=500, detail=f"Generated script has syntax error: {syntax_error}")

    return {
        "script": script,
        "filename": filename,
        "issues_found": len(issues),
        "steps_applied": len(validated_steps),
        "advisor_used": bool(advice),
        "message": f"{len(validated_steps)} correction(s) appliquée(s) automatiquement.",
        "analysis": {
            "rows": analysis["dataset_info"]["rows"],
            "columns": analysis["dataset_info"]["columns"],
            "issues": issues,
        },
    }
