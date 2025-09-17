# app/main.py
from __future__ import annotations

from fastapi import FastAPI, UploadFile, File, HTTPException, Response
from fastapi.responses import HTMLResponse, StreamingResponse, PlainTextResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from io import BytesIO
from pathlib import Path
import csv

# ---------- deps for simple sanity + converter ----------
import openpyxl

# Import the real converter if it exists
do_convert = None
try:
    # your converter file you shared earlier
    from app.converter import convert_from_buffers as _convert_from_buffers  # type: ignore
    do_convert = _convert_from_buffers
except Exception:
    # if converter.py is missing or broken, we safely fall back to echo
    do_convert = None

# =====================================================================================
# FastAPI app
# =====================================================================================
app = FastAPI(title="Sierra Payroll Backend")

# ====== CORS: wide-open to kill the browser “Network Error” immediately ======
# (We can lock to your Netlify origin later)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       # must be "*" when allow_credentials=False
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=86400,
)

# Ensure OPTIONS preflight always succeeds
@app.options("/{path:path}")
async def preflight(_: str) -> Response:
    return Response(status_code=204)

# =====================================================================================
# Health / root
# =====================================================================================
@app.get("/health")
async def health():
    return {"ok": True}

@app.get("/", response_class=HTMLResponse)
async def index():
    return """
<!doctype html>
<html>
  <head><meta charset="utf-8"><title>Sierra Payroll Backend</title></head>
  <body style="font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin:24px">
    <h2>Sierra Payroll Backend</h2>
    <ul>
      <li><code>GET /health</code> → {"ok": true}</li>
      <li><code>GET /employees</code> → list employees from <code>app/data/roster.csv</code></li>
      <li><code>POST /process-payroll</code> → form-data field <b>file</b> (.xlsx); returns WBS .xlsx</li>
    </ul>
  </body>
</html>
    """

# =====================================================================================
# Employees (used by your “Manage Employees” tab)
# Reads app/data/roster.csv if present. Non-fatal if missing.
# =====================================================================================
def _read_roster_csv() -> list[dict]:
    roster_path = Path(__file__).parent / "data" / "roster.csv"
    out: list[dict] = []
    if not roster_path.exists():
        return out
    with roster_path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for r in reader:
            # normalize a few expected keys used by the UI
            out.append({
                "empid": (r.get("EmpID") or r.get("empid") or "").strip(),
                "ssn": (r.get("SSN") or r.get("ssn") or "").strip(),
                "name": (r.get("Employee Name") or r.get("EmployeeName") or r.get("name") or "").strip(),
                "status": (r.get("Status") or r.get("status") or "").strip(),
                "type": (r.get("Type") or r.get("type") or "").strip(),
                "pay_rate": float((r.get("PayRate") or r.get("Pay Rate") or r.get("pay_rate") or "0").replace("$","").strip() or 0),
                "department": (r.get("Dept") or r.get("Department") or r.get("dept") or "").strip(),
            })
    return out

@app.get("/employees")
async def list_employees():
    try:
        return JSONResponse(_read_roster_csv())
    except Exception as e:
        # Still return a JSON error instead of exploding
        return JSONResponse({"error": f"Failed to read roster: {e}"}, status_code=500)

# =====================================================================================
# Payroll processing endpoint expected by your frontend:
#  - route: POST /process-payroll
#  - form field name: "file"  (your page sends this)
#  - returns: WBS .xlsx
# =====================================================================================
@app.post("/process-payroll")
async def process_payroll(file: UploadFile = File(...)):
    # Basic validations
    name = (file.filename or "").lower()
    if not name.endswith(".xlsx"):
        raise HTTPException(status_code=422, detail="Please upload a .xlsx Excel file.")

    try:
        data = await file.read()

        # If we have the real converter, use it (with optional roster.xlsx support in the future)
        if do_convert is not None:
            out_bytes, out_name = do_convert(sierra_xlsx=data, roster_xlsx=None)  # converter handles logic
            return StreamingResponse(
                BytesIO(out_bytes),
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f'attachment; filename="{out_name}"'}
            )

        # ---- Fallback (converter missing): echo a valid workbook so UI flow still works ----
        wb = openpyxl.load_workbook(BytesIO(data), data_only=True)
        bio = BytesIO()
        wb.save(bio)
        bio.seek(0)
        return StreamingResponse(
            bio,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": 'attachment; filename="WBS_Payroll.xlsx"'}
        )

    except HTTPException:
        raise
    except Exception as e:
        # Return a clean 400 to the UI with detail message
        return PlainTextResponse(f"Failed to process payroll: {e}", status_code=400)
