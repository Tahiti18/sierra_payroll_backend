# app/main.py
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, PlainTextResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.requests import Request
from io import BytesIO
import openpyxl

app = FastAPI(title="Sierra Payroll Backend", version="1.0.0")

# ---------- CORS (wide-open during bring-up; lock to Netlify later) ----------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],                 # works with Netlify main + preview deploys + custom domains
    allow_credentials=False,             # must be False when allow_origins == ["*"]
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],  # let browser read attachment filename
)

# ---------- Health / readiness ----------
@app.get("/health")
@app.head("/health")
async def health():
    return {"ok": True}

# ---------- Root (handy for manual sanity checks) ----------
@app.get("/", response_class=HTMLResponse)
async def index():
    return """
<!doctype html>
<html>
  <head><meta charset="utf-8"><title>Sierra Payroll Backend</title></head>
  <body style="font-family:system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial">
    <h3>Sierra Payroll Backend is running.</h3>
    <p>Try <code>/health</code> or POST a file to <code>/api/convert</code>.</p>
  </body>
</html>
"""

# ---------- Upload endpoint (wire-test: echoes workbook back) ----------
@app.post("/api/convert")
async def convert_api(
    sierra_file: UploadFile = File(...),
    roster_file: UploadFile | None = File(None),
):
    # Validate extension early (frontend already enforces this, but be strict)
    if not sierra_file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Sierra file must be .xlsx")

    try:
        # Read uploaded workbook bytes
        sierra_bytes = await sierra_file.read()

        # Load with openpyxl to ensure it's a valid workbook
        wb = openpyxl.load_workbook(BytesIO(sierra_bytes), data_only=True)

        # (For now) return it back so we prove end-to-end file handling is OK.
        # When we drop in the real converter, replace the next 4 lines with:
        #   out_bytes = run_converter(sierra_bytes, roster_bytes_optional)
        #   return StreamingResponse(BytesIO(out_bytes), ...)
        bio = BytesIO()
        wb.save(bio)
        bio.seek(0)

        out_name = "WBS_Payroll.xlsx"
        return StreamingResponse(
            bio,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{out_name}"'},
        )

    except Exception as e:
        # Return a clean 400 so the UI can show a friendly message
        return PlainTextResponse(f"Failed to read Excel: {e}", status_code=400)

# ---------- Nice JSON for unhandled paths (helps the Debug tab) ----------
@app.exception_handler(404)
async def not_found(_req: Request, _exc):
    return JSONResponse({"error": "Not found"}, status_code=404)
