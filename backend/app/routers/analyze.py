from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse

from app.services.profiler import DataProfiler
from app.models.schemas import AnalysisResult


router = APIRouter(prefix="/analyze", tags=["analysis"])
profiler = DataProfiler()


@router.post("/", response_model=AnalysisResult)
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
        
        result = await profiler.analyze_file(content, file.filename)
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@router.get("/health")
async def health_check():
    return {"status": "ok", "service": "analyzer"}