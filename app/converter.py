# app/main.py
import io
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse, StreamingResponse

from app.converter import convert_sierra_to_wbs

ALLOWED_EXTS = (".xlsx", ".xls")

app = FastAPI(title="Sierra → WBS Payroll Converter", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten for production if you want
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _ext_ok(filename: str) -> bool:
    if not filename:
        return False
    low = filename.lower()
    return any(low.endswith(ext) for ext in ALLOWED_EXTS)


@app.get("/health")
def health():
    # also confirm template/roster presence to surface config mistakes
    here = Path(__file__).resolve().parents[1]
    template_ok = (here / "wbs_template.xlsx").exists()
    roster_xlsx = (here / "roster.xlsx").exists()
    roster_csv = (here / "roster.csv").exists()
    return JSONResponse({
        "ok": True,
        "ts": datetime.utcnow().isoformat() + "Z",
        "template": template_ok,
        "roster_xlsx": roster_xlsx,
        "roster_csv": roster_csv
    })


@app.post("/process-payroll")
async def process_payroll(file: UploadFile = File(...)):
    if not file or not file.filename:
        raise HTTPException(status_code=400, detail="No file received.")
    if not _ext_ok(file.filename):
        raise HTTPException(status_code=415, detail="Upload .xlsx or .xls")

    try:
        contents = await file.read()
        out_bytes = convert_sierra_to_wbs(contents, sheet_name=None)
        out_name = f"WBS_Payroll_{datetime.utcnow().date().isoformat()}.xlsx"
        return StreamingResponse(
            io.BytesIO(out_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{out_name}"'}
        )
    except ValueError as ve:
        # input/format problems -> 422
        raise HTTPException(status_code=422, detail=str(ve))
    except Exception as e:
        # anything unexpected -> 500 with short message
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")
