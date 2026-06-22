import asyncio
import base64
import functools
import io
import os
import subprocess
import sys
import tempfile

import pandas as pd
from fastapi import APIRouter, File, Form, HTTPException, UploadFile

router = APIRouter(prefix="/execute", tags=["execute"])


@router.post("/")
async def execute_script(
    file: UploadFile = File(...),
    script: str = Form(...),
):
    """
    Run a cleaning script on the uploaded file.
    Uses run_in_executor + subprocess.run for Windows/uvicorn compatibility.
    Returns the cleaned file as Excel if the input was Excel, CSV otherwise.
    """
    content = await file.read()
    filename = file.filename or "dataset.csv"
    ext = os.path.splitext(filename)[1].lower()
    stem = os.path.splitext(filename)[0]

    with tempfile.TemporaryDirectory() as tmpdir:
        script_path = os.path.join(tmpdir, "clean.py")
        output_csv_path = os.path.join(tmpdir, "output.csv")
        output_xlsx_path = os.path.join(tmpdir, "output.xlsx")

        # Convert Excel to CSV so the generated script (which uses read_csv) can read it
        if ext in (".xlsx", ".xls"):
            df_raw = pd.read_excel(io.BytesIO(content))
            input_path = os.path.join(tmpdir, "input.csv")
            df_raw.to_csv(input_path, index=False, encoding="utf-8-sig")
        else:
            input_path = os.path.join(tmpdir, f"input{ext}")
            with open(input_path, "wb") as f:
                f.write(content)

        with open(script_path, "w", encoding="utf-8") as f:
            f.write(script)

        # Run the script in a thread pool — avoids Windows SelectorEventLoop limitations
        cmd = [sys.executable, script_path, "--input", input_path, "--output", output_csv_path]
        loop = asyncio.get_running_loop()
        try:
            result = await loop.run_in_executor(
                None,
                functools.partial(
                    subprocess.run,
                    cmd,
                    timeout=30,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                ),
            )
        except subprocess.TimeoutExpired:
            raise HTTPException(status_code=408, detail="Script execution timed out (30s)")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Execution error: {str(e)}")

        if result.returncode != 0:
            raise HTTPException(
                status_code=422,
                detail=f"Script failed (exit {result.returncode}): {result.stderr[-800:] or 'Unknown error'}",
            )

        if not os.path.exists(output_csv_path):
            raise HTTPException(status_code=500, detail="Script produced no output file")

        # Before/after stats
        stats: dict | None = None
        try:
            df_before = (
                pd.read_excel(io.BytesIO(content))
                if ext in (".xlsx", ".xls")
                else pd.read_csv(io.BytesIO(content))
            )
            df_after = pd.read_csv(output_csv_path)
            nb = int(df_before.isna().sum().sum())
            na = int(df_after.isna().sum().sum())
            stats = {
                "rows_before": len(df_before),
                "rows_after": len(df_after),
                "rows_removed": len(df_before) - len(df_after),
                "null_before": nb,
                "null_after": na,
                "null_fixed": nb - na,
            }
        except Exception:
            pass

        # Output format: return Excel if input was Excel
        if ext in (".xlsx", ".xls"):
            try:
                df_clean = pd.read_csv(output_csv_path)
                df_clean.to_excel(output_xlsx_path, index=False, engine="openpyxl")
                with open(output_xlsx_path, "rb") as f:
                    file_b64 = base64.b64encode(f.read()).decode()
                out_filename = f"cleaned_{stem}.xlsx"
                mimetype = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            except Exception:
                with open(output_csv_path, "rb") as f:
                    file_b64 = base64.b64encode(f.read()).decode()
                out_filename = f"cleaned_{stem}.csv"
                mimetype = "text/csv"
        else:
            with open(output_csv_path, "rb") as f:
                file_b64 = base64.b64encode(f.read()).decode()
            out_filename = f"cleaned_{stem}.csv"
            mimetype = "text/csv"

    return {
        "file_base64": file_b64,
        "filename": out_filename,
        "mimetype": mimetype,
        "stats": stats,
        "logs": result.stdout[-2000:],
    }
