# app/main.py
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from io import BytesIO
import csv
import os

# converter lives in app/converter.py (you already have it)
from .converter import convert_from_buffers

app = FastAPI(title="Sierra Payroll Backend")

# ---- CORS: open while we stabilize (no placeholders) ----
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # unblock the browser
    allow_credentials=False,      # must be False when origins="*"
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- health for the front-end “Test Connection” ----
@app.get("/health")
async def health():
    return {"ok": True, "service": "sierra-backend"}

# ---- Employees API (serves roster.csv from repo) ----
@app.get("/employees")
async def employees():
    roster_path = os.path.join(os.path.dirname(__file__), "data", "roster.csv")
    if not os.path.exists(roster_path):
        raise HTTPException(status_code=404, detail="Roster file missing on server")
    out = []
    with open(roster_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            out.append({
                "EmpID": row.get("EmpID",""),
                "SSN": row.get("SSN",""),
                "Employee Name": row.get("Employee Name",""),
                "Status": row.get("Status",""),
                "Type": row.get("Type",""),
                "PayRate": row.get("PayRate",""),
                "Dept": row.get("Dept",""),
            })
    return JSONResponse(out)

# ---- Payroll processing (used by the blue button) ----
@app.post("/process-payroll")
async def process_payroll(
    file: UploadFile | None = File(None),                # some UIs send 'file'
    sierra_file: UploadFile | None = File(None),         # some send 'sierra_file'
    roster_file: UploadFile | None = File(None),
):
    sierra = sierra_file or file
    if not sierra:
        raise HTTPException(status_code=400, detail="Missing Sierra payroll .xlsx upload")
    if not sierra.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Sierra file must be .xlsx")

    try:
        sierra_bytes = await sierra.read()
        roster_bytes = await roster_file.read() if roster_file else None

        # run the real converter
        out_bytes, out_name = convert_from_buffers(sierra_bytes, roster_bytes)

        return StreamingResponse(
            BytesIO(out_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{out_name}"'},
        )
    except HTTPException:
        raise
    except Exception as e:
        # bubble up clear message to the UI
        return PlainTextResponse(f"Conversion failed: {e}", status_code=400)

# ---- compatibility alias for older front-ends ----
@app.post("/api/convert")
async def api_convert(
    file: UploadFile | None = File(None),
    sierra_file: UploadFile | None = File(None),
    roster_file: UploadFile | None = File(None),
):
    return await process_payroll(file=file, sierra_file=sierra_file, roster_file=roster_file)
