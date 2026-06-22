import re

import httpx
from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.services.profiler import DataProfiler

router = APIRouter(prefix="/analyze", tags=["analysis"])


class UrlIngestRequest(BaseModel):
    url: str


@router.post("/url")
async def analyze_from_url(request: UrlIngestRequest):
    """Analyze a CSV/Excel from a URL. Supports direct links, Google Sheets, and public S3."""
    url = request.url.strip()

    # Convert Google Sheets edit URL → CSV export URL
    gs_match = re.match(r"https://docs\.google\.com/spreadsheets/d/([^/]+)", url)
    if gs_match:
        sheet_id = gs_match.group(1)
        # Preserve gid (sheet tab) if present
        gid_match = re.search(r"[#&?]gid=(\d+)", url)
        gid = f"&gid={gid_match.group(1)}" if gid_match else ""
        url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv{gid}"
        filename = f"sheet_{sheet_id}.csv"
    else:
        filename = url.split("/")[-1].split("?")[0] or "dataset"
        if not any(filename.lower().endswith(ext) for ext in (".csv", ".xlsx", ".xls")):
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
            detail=f"Failed to fetch URL (HTTP {response.status_code}). Make sure the file is publicly accessible.",
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
    """
    Upload a CSV or Excel file and get comprehensive data quality analysis.
    """
    # Validation
    allowed_extensions = {'.csv', '.xlsx', '.xls'}
    file_ext = file.filename.lower()[file.filename.rfind('.'):]

    if file_ext not in allowed_extensions:
        raise HTTPException(
            status_code=400,
            detail=f"File type {file_ext} not supported. Use: {allowed_extensions}"
        )

    try:
        content = await file.read()

        if len(content) > 100 * 1024 * 1024:  # 100MB
            raise HTTPException(status_code=413, detail="File too large")

        # Instancier le profiler sans DataFrame (sera chargé dans analyze_file)
        profiler = DataProfiler()
        result = await profiler.analyze_file(content, file.filename)
        return result

    except Exception as e:
        import traceback
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")

@router.get("/health")
async def health_check():
    return {"status": "ok", "service": "analyzer"}