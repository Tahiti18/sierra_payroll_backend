# app/main.py
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from io import BytesIO
import openpyxl

app = FastAPI(title="Sierra Payroll Backend")

# --- CORS: wide-open while we stabilize; we can lock it later to your Netlify origin ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       # later we can change to ["https://<your-netlify>.netlify.app"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- health & root (used by the Netlify “Test Connection” button) ----
@app.get("/health")
async def health():
    return {"ok": True}

@app.get("/", response_class=HTMLResponse)
async def index():
    return "<h3>Sierra Payroll Backend is running.</h3><p>Try <code>/health</code>.</p>"

# ---- (stub) convert endpoint so the upload form has somewhere to post for now ----
@app.post("/api/convert")
async def convert_api(
    sierra_file: UploadFile = File(...),
    roster_file: UploadFile | None = File(None),
):
    if not sierra_file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Sierra file must be .xlsx")
    try:
        data = await sierra_file.read()
        # For now just echo the workbook back to prove the pipe works.
        # We’ll drop in the real converter right after connectivity is green.
        wb = openpyxl.load_workbook(BytesIO(data), data_only=True)
        bio = BytesIO()
        wb.save(bio)
        bio.seek(0)
        return StreamingResponse(
            bio,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": 'attachment; filename="WBS_Payroll.xlsx"'},
        )
    except Exception as e:
        return PlainTextResponse(f"Failed to read Excel: {e}", status_code=400)
