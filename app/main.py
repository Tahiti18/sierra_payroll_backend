# app/main.py

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import Response

app = FastAPI(title="Sierra → WBS Backend", version="1.0.2")

# Allow Netlify + local dev
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

@app.post("/process-payroll")
async def process_payroll(file: UploadFile = File(...)):
    name = (file.filename or "").lower()
    if not (name.endswith(".xlsx") or name.endswith(".xls")):
        raise HTTPException(422, detail="Please upload an Excel .xlsx/.xls file.")
    data = await file.read()
    if not data:
        raise HTTPException(422, detail="Empty file.")

    # TODO: replace this with your real converter function
    out_bytes = data

    return Response(
        content=out_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": 'attachment; filename="WBS_Payroll.xlsx"',
            "Content-Length": str(len(out_bytes)),  # <- required to stop iPad/Safari from stalling
        },
    )
