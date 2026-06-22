import ipaddress
import re
from urllib.parse import urlparse

import httpx
from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.services.profiler import DataProfiler

router = APIRouter(prefix="/analyze", tags=["analysis"])

ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls"}


def _is_safe_url(url: str) -> bool:
    """Block non-http schemes and private/loopback IPs (SSRF prevention)."""
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        host = parsed.hostname or ""
        try:
            addr = ipaddress.ip_address(host)
            if addr.is_private or addr.is_loopback or addr.is_link_local or addr.is_reserved:
                return False
        except ValueError:
            pass  # hostname, not a bare IP
        return True
    except Exception:
        return False


class UrlIngestRequest(BaseModel):
    url: str


@router.post("/url")
async def analyze_from_url(request: UrlIngestRequest):
    """Analyze a CSV/Excel from a URL. Supports direct links, Google Sheets, and public S3."""
    url = request.url.strip()

    if not _is_safe_url(url):
        raise HTTPException(
            status_code=400,
            detail="URL invalide. Seules les URLs http:// et https:// pointant vers des hôtes publics sont acceptées.",
        )

    # Convert Google Sheets edit URL → CSV export URL
    gs_match = re.match(r"https://docs\.google\.com/spreadsheets/d/([^/]+)", url)
    if gs_match:
        sheet_id = gs_match.group(1)
        gid_match = re.search(r"[#&?]gid=(\d+)", url)
        gid = f"&gid={gid_match.group(1)}" if gid_match else ""
        url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv{gid}"
        filename = f"sheet_{sheet_id}.csv"
    else:
        filename = url.split("/")[-1].split("?")[0] or "dataset"
        if not any(filename.lower().endswith(ext) for ext in ALLOWED_EXTENSIONS):
            filename += ".csv"

    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            response = await client.get(url)
    except httpx.TimeoutException:
        raise HTTPException(status_code=408, detail="URL fetch timed out (30s)")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not fetch URL: {str(e)}")

    if response.status_code != 200:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to fetch URL (HTTP {response.status_code}). Vérifiez que le fichier est public.",
        )

    content = response.content
    if len(content) > 100 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="File too large (max 100MB)")

    try:
        profiler = DataProfiler()
        result = await profiler.analyze_file(content, filename)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@router.post("/")
async def analyze_dataset(file: UploadFile = File(...)):
    """Upload a CSV or Excel file and get comprehensive data quality analysis."""
    filename = file.filename or "dataset.csv"
    dot_idx = filename.rfind(".")
    if dot_idx == -1:
        raise HTTPException(status_code=400, detail="Le fichier doit avoir une extension (.csv, .xlsx, .xls)")

    file_ext = filename[dot_idx:].lower()
    if file_ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Format '{file_ext}' non supporté. Utilisez : {', '.join(sorted(ALLOWED_EXTENSIONS))}",
        )

    content = await file.read()
    if len(content) > 100 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="Fichier trop volumineux (max 100MB)")

    try:
        profiler = DataProfiler()
        result = await profiler.analyze_file(content, filename)
        return result
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@router.get("/health")
async def health_check():
    return {"status": "ok", "service": "analyzer"}
