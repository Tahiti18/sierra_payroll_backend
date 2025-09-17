# app/main.py
from __future__ import annotations
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from io import BytesIO
import csv, os

from .converter import convert_from_buffers

APP_ROOT = os.path.dirname(os.path.abspath(__file__))
ROSTER_CSV = os.path.join(APP_ROOT, "data", "roster.csv")

app = FastAPI(title="Sierra Payroll Backend", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # keep wide open while stabilizing
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health():
    return {"ok": True, "service": "sierra-backend", "version": "1.0"}

@app.get("/", response_class=HTMLResponse)
async def index():
    return "<h3>Sierra Payroll Backend is running.</h3><p>Try <code>/health</code>.</p>"

@app.get("/employees")
async def get_employees():
    rows = []
    if os.path.exists(ROSTER_CSV):
        with open(ROSTER_CSV, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                rows.append({
                    "EmpID": r.get("EmpID","").strip(),
                    "SSN": r.get("SSN","").strip(),
                    "name": r.get("Employee Name","").strip(),
                    "status": r.get("Status","").strip(),
                    "type": r.get("Type","").strip(),
                    "payRate": r.get("PayRate","").strip(),
                    "dept": r.get("Dept","").strip(),
                })
    return {"employees": rows, "count": len(rows)}

@app.post("/process-payroll")
async def process_payroll(
    sierra_file: UploadFile = File(...),
    roster_file: UploadFile | None = File(None),
):
    if not sierra_file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Sierra file must be .xlsx")
    try:
        sierra_bytes = await sierra_file.read()
        roster_bytes = await roster_file.read() if roster_file else None
        out_bytes, out_name = convert_from_buffers(sierra_bytes, roster_bytes)
        return StreamingResponse(
            BytesIO(out_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{out_name}"'},
        )
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

# alias for older UI
@app.post("/api/convert")
async def api_convert(sierra_file: UploadFile = File(...), roster_file: UploadFile | None = File(None)):
    return await process_payroll(sierra_file, roster_file)
