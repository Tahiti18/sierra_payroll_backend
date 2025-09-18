# app/main.py
import io
import logging
import traceback
from collections import deque
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse, JSONResponse

# ------------------------------------------------------------------------------
# Logging (make sure stack traces go to stdout so Railway shows them)
# ------------------------------------------------------------------------------
logger = logging.getLogger("wbs")
if not logger.handlers:
    handler = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    handler.setFormatter(fmt)
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Keep the last few errors in memory so we can read them from /debug/last-error
_LAST_ERRORS: deque[str] = deque(maxlen=5)


def _remember_error(prefix: str, err: Exception) -> str:
    tb = traceback.format_exc()
    stamp = datetime.utcnow().isoformat() + "Z"
    full = f"[{stamp}] {prefix}: {err.__class__.__name__}: {err}\n{tb}"
    _LAST_ERRORS.append(full)
    logger.error(full)
    return full


def _ext_ok(filename: str) -> bool:
    name = (filename or "").lower()
    return name.endswith(".xlsx") or name.endswith(".xls")


# ------------------------------------------------------------------------------
# Import your real converter (leave all your conversion logic in app/converter.py)
# ------------------------------------------------------------------------------
try:
    # You already have this file in the repo; we just call it.
    from app.converter import convert_sierra_to_wbs  # type: ignore
except Exception as e:  # pragma: no cover
    # If the import fails, log clearly so we know right away.
    _remember_error("IMPORT converter.py failed", e)

    def convert_sierra_to_wbs(_bytes: bytes, sheet_name: Optional[str] = None) -> bytes:
        raise RuntimeError(
            "converter.py import failed earlier; check logs for the traceback."
        )


# ------------------------------------------------------------------------------
# FastAPI app
# ------------------------------------------------------------------------------
app = FastAPI(title="Sierra → WBS Payroll Converter", version="debug-logger-1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],     # tighten later if you want
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ------------------------------------------------------------------------------
# Health + Debug endpoints
# ------------------------------------------------------------------------------
@app.get("/health")
def health():
    return {"ok": True, "ts": datetime.utcnow().isoformat() + "Z"}

@app.get("/debug/last-error")
def debug_last_error():
    """Returns the most recent captured Python traceback (if any)."""
    if not _LAST_ERRORS:
        return {"last_error": None}
    return {"last_error": list(_LAST_ERRORS)[-1]}

@app.get("/debug/errors")
def debug_errors():
    """Returns the recent error ring buffer (up to 5)."""
    return {"errors": list(_LAST_ERRORS)}


# ------------------------------------------------------------------------------
# Main route
# ------------------------------------------------------------------------------
@app.post("/process-payroll")
async def process_payroll(file: UploadFile = File(...)):
    # Basic validation
    if not file or not file.filename:
        raise HTTPException(status_code=400, detail="No selected file.")
    if not _ext_ok(file.filename):
        raise HTTPException(
            status_code=415,
            detail="Unsupported file type. Please upload .xlsx or .xls"
        )

    try:
        contents = await file.read()
    except Exception as e:
        _remember_error("READ upload failed", e)
        raise HTTPException(status_code=400, detail=f"Failed to read upload: {e}")

    # Run conversion with strong error capture
    try:
        out_bytes = convert_sierra_to_wbs(contents, sheet_name=None)
    except HTTPException:
        # If your converter already raised an HTTPException, let it pass through,
        # but also remember it for /debug/last-error.
        _remember_error("Converter HTTPException", HTTPException)
        raise
    except Exception as e:
        # This is the key: we store & log the full traceback and surface a concise message
        _remember_error("Converter crashed", e)
        # Return a friendly payload to the UI
        raise HTTPException(
            status_code=500,
            detail=f"Server crash in converter: {e.__class__.__name__}: {e}"
        )

    # Stream back the Excel
    try:
        out_name = f"WBS_Payroll_{datetime.utcnow().date().isoformat()}.xlsx"
        return StreamingResponse(
            io.BytesIO(out_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{out_name}"'}
        )
    except Exception as e:
        _remember_error("STREAM response failed", e)
        raise HTTPException(status_code=500, detail=f"Failed to stream file: {e}")
