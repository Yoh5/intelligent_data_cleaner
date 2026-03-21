from pydantic import BaseModel
from typing import Dict, List, Any, Optional
from datetime import datetime


class DatasetInfo(BaseModel):
    filename: str
    rows: int
    columns: int
    size_bytes: int
    column_types: Dict[str, str]


class ColumnStats(BaseModel):
    name: str
    dtype: str
    missing_count: int
    missing_pct: float
    unique_count: int
    sample_values: List[Any]


class IssueDetected(BaseModel):
    type: str  # "missing", "outlier", "inconsistent", "duplicate", "pii"
    severity: str  # "high", "medium", "low"
    column: Optional[str]
    description: str
    affected_rows: Optional[int]


class AnalysisResult(BaseModel):
    id: str
    created_at: datetime
    dataset_info: DatasetInfo
    issues: List[IssueDetected]
    profile_html: Optional[str] = None  # ydata profiling report
    raw_profile: Optional[Dict] = None