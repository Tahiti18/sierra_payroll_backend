# app/main.py
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse, Response, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import importlib

app = FastAPI(title="Sierra Payroll Backend")

# CORS: your Netlify app + local dev
ALLOWED_ORIGINS = [
    "https://adorable-madeline-291bb0.netlify.app",
    "http://localhost:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"ok": True}

def _load_converter():
    try:
        mod = importlib.import_module("app.converter")
        fn = getattr(mod, "convert_from_buffers", None)
        return fn
    except Exception:
        return None

_CONVERTER = _load_converter()

@app.post("/api/convert")
async def convert_endpoint(
    sierra_file: UploadFile = File(...),
    roster_file: Optional[UploadFile] = File(None),
):
    if not sierra_file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="sierra_file must be .xlsx")
    if roster_file and not roster_file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="roster_file must be .xlsx")

    sierra_bytes = await sierra_file.read()
    roster_bytes = await roster_file.read() if roster_file else None

    if _CONVERTER is None:
        return JSONResponse(
            {"ok": False, "error": "Converter not found (app/converter.py: convert_from_buffers)."},
            status_code=500,
        )

    try:
        out_bytes, out_name = _CONVERTER(sierra_bytes, roster_bytes)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Conversion failed: {e}")

    if not out_bytes:
        raise HTTPException(status_code=500, detail="Empty result from converter.")

    return Response(
        content=out_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{out_name or "WBS_Payroll.xlsx"}"'},
    )
