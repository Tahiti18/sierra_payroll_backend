# app/main.py
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from io import BytesIO
from pathlib import Path
import csv
import openpyxl

# ---- Try to import the real converter (must live at app/converter.py) ----
# Expected callable signature: convert(sierra_bytes: bytes, roster_rows: list[dict]) -> bytes
HAS_CONVERTER = False
try:
    from .converter import convert as convert_workbook  # type: ignore
    HAS_CONVERTER = True
except Exception:
    HAS_CONVERTER = False

app = FastAPI(title="Sierra Payroll Backend")

# ---- CORS (allow your Netlify domain; add others if needed) ----
FRONTEND_ORIGINS = [
    "https://adorable-madeline-291bb0.netlify.app",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=FRONTEND_ORIGINS + ["http://localhost", "http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- Health check ----------------------------------------------------------
@app.get("/health")
async def health():
    return {"ok": True}

# ========================================================================== #
#                                EMPLOYEES API                               #
# ========================================================================== #

ROSTER_PATH = (Path(__file__).parent / "data" / "roster.csv").resolve()

REQUIRED_HEADERS = {
    "empid": ["empid", "employee id", "id"],
    "ssn": ["ssn", "social security", "social security number"],
    "employee name": ["employee name", "name"],
    "status": ["status"],
    "type": ["type", "pay type"],
    "payrate": ["payrate", "pay rate", "rate"],
    "dept": ["dept", "department"],
}

def _map_headers(hdr_row: list[str]) -> dict:
    idx = {}
    low = [str(h or "").strip().lower() for h in hdr_row]
    for want, alist in REQUIRED_HEADERS.items():
        for a in alist:
            if a in low:
                idx[want] = low.index(a)
                break
    missing = [k for k in REQUIRED_HEADERS.keys() if k not in idx]
    if missing:
        raise ValueError(f"Roster missing columns: {', '.join(missing)}")
    return idx

def _load_roster() -> list[dict]:
    if not ROSTER_PATH.exists():
        raise FileNotFoundError(f"Roster not found at {ROSTER_PATH}")
    rows: list[dict] = []
    with ROSTER_PATH.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        data = list(reader)
    if not data:
        return rows
    hdr_map = _map_headers(data[0])
    for r in data[1:]:
        if not r or all((c or "").strip() == "" for c in r):
            continue
        def get(k: str) -> str:
            i = hdr_map[k]
            return (r[i] if i < len(r) else "").strip()
        rows.append({
            "EmpID": get("empid"),
            "SSN": get("ssn"),
            "EmployeeName": get("employee name"),
            "Status": get("status"),
            "Type": get("type"),
            "PayRate": get("payrate"),
            "Dept": get("dept"),
        })
    return rows

@app.get("/employees")
async def get_employees():
    try:
        roster = _load_roster()
        return JSONResponse({"count": len(roster), "employees": roster})
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read roster: {e}")

# ========================================================================== #
#                            PAYROLL CONVERSION API                           #
# ========================================================================== #

def _echo_back_if_needed(xlsx_bytes: bytes) -> bytes:
    """
    Safety: if converter module is missing, don't fake payroll;
    raise a 501 so the UI shows a clear message.
    """
    if not HAS_CONVERTER:
        raise HTTPException(
            status_code=501,
            detail="Converter not installed on backend. Add app/converter.py with convert() "
                   "or deploy the latest backend that includes the converter.",
        )
    return xlsx_bytes  # placeholder never used because we raise above

async def _convert_endpoint_logic(sierra_file: UploadFile, roster_file: UploadFile | None):
    if not sierra_file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Sierra file must be .xlsx")

    sierra_bytes = await sierra_file.read()

    # Load roster from CSV by default
    roster_rows = _load_roster()

    # If a roster Excel was uploaded via UI, use it instead
    if roster_file and roster_file.filename and roster_file.filename.lower().endswith((".xlsx", ".xls")):
        try:
            b = await roster_file.read()
            wb = openpyxl.load_workbook(BytesIO(b), data_only=True)
            sh = wb.active
            hdr = [str(c.value or "").strip() for c in next(sh.iter_rows(min_row=1, max_row=1, values_only=False))]
            idx = _map_headers(hdr)
            roster_rows = []
            for row in sh.iter_rows(min_row=2, values_only=True):
                if not row or all((c or "") == "" for c in row):
                    continue
                def gv(k: str) -> str:
                    i = idx[k]
                    return str(row[i] or "").strip()
                roster_rows.append({
                    "EmpID": gv("empid"),
                    "SSN": gv("ssn"),
                    "EmployeeName": gv("employee name"),
                    "Status": gv("status"),
                    "Type": gv("type"),
                    "PayRate": gv("payrate"),
                    "Dept": gv("dept"),
                })
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to parse uploaded roster workbook: {e}")

    # Run real converter if installed
    if HAS_CONVERTER:
        try:
            result_bytes = convert_workbook(sierra_bytes, roster_rows)
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Converter error: {e}")
    else:
        # Clear message – DO NOT fake success
        result_bytes = _echo_back_if_needed(sierra_bytes)  # will raise 501

    filename_out = "WBS_Payroll.xlsx"
    return StreamingResponse(
        BytesIO(result_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename_out}"'},
    )

@app.post("/api/convert")
async def convert_api(
    sierra_file: UploadFile = File(...),
    roster_file: UploadFile | None = File(None),
):
    return await _convert_endpoint_logic(sierra_file, roster_file)

# UI sometimes calls /process-payroll – make it an alias
@app.post("/process-payroll")
async def process_payroll(
    sierra_file: UploadFile = File(...),
    roster_file: UploadFile | None = File(None),
):
    return await _convert_endpoint_logic(sierra_file, roster_file)
