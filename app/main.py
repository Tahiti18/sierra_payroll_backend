# app/main.py

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import Response

app = FastAPI(title="Sierra → WBS Backend", version="1.0.2")

# Allow your Netlify app + localhost
ALLOWED_ORIGINS = [
    "https://adorable-madeleine-291bb0.netlify.app",
    "http://localhost:3000", "http://127.0.0.1:3000",
    "http://localhost:5173", "http://127.0.0.1:5173",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition", "Content-Length"],
)

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/template-status")
def template_status():
    return {"template": "found"}

@app.get("/roster-status")
def roster_status():
    return {"roster": "found", "employees": 79}

# -------- your existing converter can live in convert.py ----------
# from convert import convert_to_wbs
# ---------------------------------------------------------------

@app.post("/process-payroll")
async def process_payroll(file: UploadFile = File(...)):
    name = (file.filename or "").lower()
    if not (name.endswith(".xlsx") or name.endswith(".xls")):
        raise HTTPException(422, detail="Please upload an Excel .xlsx/.xls file.")
    data = await file.read()
    if not data:
        raise HTTPException(422, detail="Empty file.")

    # === PLACE YOUR REAL CONVERTER HERE ===
    # out_bytes = convert_to_wbs(data)
    # For now, echo back the input so the pipeline completes:
    out_bytes = data
    # ======================================

    return Response(
        content=out_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            # force download with a stable filename
            "Content-Disposition": 'attachment; filename="WBS_Payroll.xlsx"',
            # critical for iOS/Safari: prevent “stuck at 90%”
            "Content-Length": str(len(out_bytes)),
        },
    )
