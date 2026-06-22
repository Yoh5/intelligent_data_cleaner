import asyncio
import base64
import io
import os
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
    Run a cleaning script on the uploaded file in an isolated subprocess.
    Returns execution stats and the cleaned CSV (base64-encoded).
    """
    content = await file.read()
    filename = file.filename or "dataset.csv"
    ext = os.path.splitext(filename)[1].lower()

    with tempfile.TemporaryDirectory() as tmpdir:
        input_path = os.path.join(tmpdir, f"input{ext}")
        script_path = os.path.join(tmpdir, "clean.py")
        output_path = os.path.join(tmpdir, "output.csv")

        with open(input_path, "wb") as f:
            f.write(content)

        # If input is Excel, also save a CSV copy the script can read
        if ext in (".xlsx", ".xls"):
            df_raw = pd.read_excel(io.BytesIO(content))
            csv_path = os.path.join(tmpdir, "input.csv")
            df_raw.to_csv(csv_path, index=False, encoding="utf-8-sig")
            input_path = csv_path

        with open(script_path, "w", encoding="utf-8") as f:
            f.write(script)

        try:
            proc = await asyncio.create_subprocess_exec(
                sys.executable, script_path,
                "--input", input_path,
                "--output", output_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout_bytes, stderr_bytes = await asyncio.wait_for(
                proc.communicate(), timeout=30
            )
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except Exception:
                pass
            raise HTTPException(status_code=408, detail="Script execution timed out (30s)")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Execution error: {str(e)}")

        stdout = stdout_bytes.decode("utf-8", errors="replace")
        stderr = stderr_bytes.decode("utf-8", errors="replace")

        if proc.returncode != 0:
            raise HTTPException(
                status_code=422,
                detail=f"Script failed (exit {proc.returncode}): {stderr[-800:] or 'Unknown error'}",
            )

        if not os.path.exists(output_path):
            raise HTTPException(status_code=500, detail="Script did not produce an output file")

        # Compute before/after stats
        stats: dict = {}
        try:
            df_before = pd.read_csv(io.BytesIO(content)) if ext == ".csv" else pd.read_excel(io.BytesIO(content))
            df_after = pd.read_csv(output_path)
            null_before = int(df_before.isna().sum().sum())
            null_after = int(df_after.isna().sum().sum())
            stats = {
                "rows_before": len(df_before),
                "rows_after": len(df_after),
                "rows_removed": len(df_before) - len(df_after),
                "null_before": null_before,
                "null_after": null_after,
                "null_fixed": null_before - null_after,
            }
        except Exception:
            pass

        with open(output_path, "rb") as f:
            csv_b64 = base64.b64encode(f.read()).decode()

    stem = os.path.splitext(filename)[0]
    return {
        "csv_base64": csv_b64,
        "filename": f"cleaned_{stem}.csv",
        "stats": stats,
        "logs": stdout[-2000:],
    }
