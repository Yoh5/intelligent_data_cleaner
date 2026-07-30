from fastapi import APIRouter, UploadFile, File, HTTPException
from datetime import datetime

from app.services.profiler import DataProfiler
from app.services import advisor
from app.routers.suggest import _generate_strategies_for_issue, IssueInput
from app.routers.generate import _build_complete_script, _validate_and_fix_code, _validate_syntax

router = APIRouter(prefix="/auto-clean", tags=["auto-clean"])


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
    analysis = await profiler.analyze_file(content, file.filename)

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
    cols = analysis.get("columns", [])
    column_types = {c.get("name"): c.get("semantic_type", "") for c in cols}
    sample_data = {c.get("name"): c.get("sample_values", []) for c in cols}
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
