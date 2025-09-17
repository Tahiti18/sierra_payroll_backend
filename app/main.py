# app/main.py
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse, Response, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import importlib
import io

app = FastAPI(title="Sierra Payroll Backend")

# --- CORS: allow your Netlify front-end (and localhost for dev) ---
ALLOWED_ORIGINS = [
    "https://adorable-madeline-291bb0.netlify.app",  # your Netlify URL
    "http://localhost:5173",                         # local dev (optional)
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------- Health & debug --------
@app.get("/health")
def health():
    return {"ok": True, "service": "sierra-payroll-backend"}

@app.get("/", response_class=PlainTextResponse)
def root():
    return "Sierra Payroll Backend: OK. Try POST /api/convert"

# -------- Helpers to load converter (so you can keep yours) --------
def _load_converter():
    """
    We try a few import paths so you can keep your converter module wherever you placed it.
    Expected callable signature:
        convert_from_buffers(sierra_xlsx: bytes, roster_xlsx: Optional[bytes]) -> tuple[bytes, str]
    It must return: (output_file_bytes, suggested_filename)
    """
    candidates = [
        "app.converter",
        "converter",
        "server.converter",
    ]
    for modname in candidates:
        try:
            mod = importlib.import_module(modname)
            if hasattr(mod, "convert_from_buffers"):
                return mod.convert_from_buffers
        except Exception:
            continue
    return None

_CONVERTER = _load_converter()

# -------- Main API: convert Sierra -> WBS --------
@app.post("/api/convert")
async def convert_endpoint(
    sierra_file: UploadFile = File(..., description="Sierra Payroll .xlsx"),
    roster_file: Optional[UploadFile] = File(None, description="Roster .xlsx (optional)")
):
    # basic validation
    if not sierra_file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="sierra_file must be an .xlsx file")
    if roster_file and not roster_file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="roster_file must be an .xlsx file")

    # read file bytes
    sierra_bytes = await sierra_file.read()
    roster_bytes = await roster_file.read() if roster_file else None

    # ensure we have a converter
    if _CONVERTER is None:
        # Return a clear error so the front-end shows it
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": "Converter module not found. Expected function 'convert_from_buffers' in app/converter.py (or converter.py)."}
        )

    # run conversion
    try:
        out_bytes, out_name = _CONVERTER(sierra_bytes, roster_bytes)
    except HTTPException:
        raise
    except Exception as e:
        # bubble a readable error to the UI
        raise HTTPException(status_code=500, detail=f"Conversion failed: {e}")

    if not out_bytes:
        raise HTTPException(status_code=500, detail="Converter returned no data.")

    # stream back as a download
    disposition_name = out_name or "WBS_Payroll_Output.xlsx"
    headers = {
        "Content-Disposition": f'attachment; filename="{disposition_name}"'
    }
    return Response(
        content=out_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )
