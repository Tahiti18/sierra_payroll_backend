from fastapi import FastAPI, UploadFile, File, Response, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from io import BytesIO
import openpyxl

app = FastAPI(title="Payroll Converter")

# CORS (safe default)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ---------- Simple upload page ----------
@app.get("/", response_class=HTMLResponse)
async def index():
    return """
<!doctype html>
<html>
  <head>
    <meta charset="utf-8"/>
    <title>Payroll Converter</title>
    <style>
      body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 40px; }
      .card { max-width: 760px; margin: 0 auto; padding: 24px; border: 1px solid #e5e7eb; border-radius: 12px; }
      h1 { margin: 0 0 12px; }
      label { display:block; font-weight:600; margin:16px 0 6px; }
      input[type=file] { padding: 10px; border: 1px solid #d1d5db; border-radius: 8px; width: 100%; }
      button { margin-top: 18px; padding: 10px 16px; border-radius: 8px; border: 0; background:#111827; color:#fff; font-weight:600; }
      .hint { color:#6b7280; font-size:13px; }
    </style>
  </head>
  <body>
    <div class="card">
      <h1>Payroll Converter</h1>
      <p class="hint">Upload the Sierra payroll workbook (.xlsx). Optional: upload a Roster workbook.</p>
      <form id="f" action="/api/convert" method="post" enctype="multipart/form-data">
        <label>Sierra file (.xlsx)</label>
        <input name="sierra_file" type="file" accept=".xlsx" required />
        <label>Roster file (optional, .xlsx)</label>
        <input name="roster_file" type="file" accept=".xlsx" />
        <button type="submit">Convert to WBS</button>
      </form>
    </div>
  </body>
</html>
    """

# ---------- Upload API (echo test for now) ----------
@app.post("/api/convert")
async def convert_api(
    sierra_file: UploadFile = File(...),
    roster_file: UploadFile | None = File(None),
):
    # Basic validations
    if not sierra_file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Sierra file must be .xlsx")

    try:
        # Read bytes
        sierra_bytes = await sierra_file.read()

        # Try loading with openpyxl just to ensure it's a real Excel file
        wb = openpyxl.load_workbook(BytesIO(sierra_bytes), data_only=True)
        bio = BytesIO()
        wb.save(bio)
        bio.seek(0)

        # NOTE: Right now we just echo the uploaded workbook back.
        # Next step: replace this block with the real converter and return the WBS workbook.
        filename_out = "WBS_Payroll.xlsx"

        return StreamingResponse(
            bio,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f'attachment; filename="{filename_out}"'
            },
        )
    except Exception as e:
        return PlainTextResponse(f"Failed to read Excel: {e}", status_code=400)
