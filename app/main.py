# app/main.py
import io
from datetime import datetime
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse, JSONResponse

# ✅ single, one-way import (no circular import)
from app.converter import convert_sierra_to_wbs


app = FastAPI(title="Sierra → WBS Payroll Converter", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten to your frontend domain if needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return JSONResponse({"ok": True, "ts": datetime.utcnow().isoformat() + "Z"})


@app.post("/process-payroll")
async def process_payroll(file: UploadFile = File(...)):
    if not file or not file.filename:
        raise HTTPException(status_code=400, detail="No selected file.")
    name = (file.filename or "").lower()
    if not (name.endswith(".xlsx") or name.endswith(".xls")):
        raise HTTPException(status_code=415, detail="Unsupported file type. Please upload .xlsx or .xls")

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
        # bad/missing columns etc.
        raise HTTPException(status_code=422, detail=str(ve))
    except Exception as e:
        # unhandled
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")
