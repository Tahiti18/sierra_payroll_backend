# app/main.py
import os
import logging
from io import BytesIO
from datetime import datetime, date, timedelta
from typing import Optional, Tuple, Dict, List

import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from starlette.middleware.cors import CORSMiddleware

# =========================
# Logging (helps kill 500s)
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("sierra-backend")

# =========================
# FastAPI + CORS
# =========================
app = FastAPI(title="Sierra Roofing Payroll Backend")

FRONTEND_ORIGIN = os.getenv(
    "FRONTEND_ORIGIN",
    "https://adorable-madeleine-291bb0.netlify.app"
)

# Allow the known frontend plus localhost during debugging
ALLOWED_ORIGINS = {
    FRONTEND_ORIGIN,
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://localhost",
    "http://127.0.0.1"
}
app.add_middleware(
    CORSMiddleware,
    allow_origins=list(ALLOWED_ORIGINS),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# Paths / Files
# =========================
ROSTER_PATH = os.getenv("ROSTER_PATH", "app/data/roster.csv")

# Column name aliases we’ll search for in the uploaded Excel
NAME_ALIASES  = {"name", "employee", "employee name", "worker"}
HOURS_ALIASES = {"hours", "hrs", "total hours", "work hours", "time", "regular", "reg"}
DATE_ALIASES  = {"date", "day", "days", "work date", "worked"}
JOB_ALIASES   = {"job#", "job", "job number"}

# =========================
# Helpers
# =========================
def _normalize(s) -> str:
    return str(s).strip().lower()

def find_col(df: pd.DataFrame, aliases: set[str]) -> Optional[str]:
    """Find a column in df whose normalized header matches (or contains) an alias."""
    norm = {c: _normalize(c) for c in df.columns}
    # exact
    for orig, low in norm.items():
        if low in aliases:
            return orig
    # substring
    for orig, low in norm.items():
        if any(a in low for a in aliases):
            return orig
    return None

def guess_header_row(df: pd.DataFrame) -> int:
    """
    If read_excel guessed the header wrong (None), try to detect the row that
    looks like headers by searching for any alias in a row.
    Returns a 0-based row index to use as header.
    """
    # Look through first 15 rows for something header-like
    check_rows = min(15, len(df))
    alias_pool = NAME_ALIASES | HOURS_ALIASES | DATE_ALIASES | JOB_ALIASES
    for i in range(check_rows):
        row_vals = [_normalize(v) for v in df.iloc[i].tolist()]
        if any(any(a in val for a in alias_pool) for val in row_vals):
            return i
    return 0  # fallback

def to_last_first(name: str) -> str:
    if not isinstance(name, str):
        return ""
    n = name.strip()
    if not n:
        return ""
    if "," in n:     # already "Last, First"
        return n
    parts = n.split()
    if len(parts) >= 2:
        last = parts[-1]
        first = " ".join(parts[:-1])
        return f"{last}, {first}"
    return n

def sunday_for_week(d: date) -> date:
    # Monday=0..Sunday=6 -> Sunday
    return d + timedelta(days=(6 - d.weekday()) % 7)

def compute_pe_from_dates(sierra_dates: pd.Series) -> date:
    dt = pd.to_datetime(sierra_dates, errors="coerce")
    dt = dt.dropna()
    if dt.empty:
        return sunday_for_week(date.today())
    return sunday_for_week(dt.max().date())

def load_roster() -> pd.DataFrame:
    if not os.path.exists(ROSTER_PATH):
        raise HTTPException(status_code=500, detail=f"Roster file not found at {ROSTER_PATH}")
    roster = pd.read_csv(ROSTER_PATH, dtype=str).fillna("")
    required = {"EmpID","SSN","Employee Name","Status","Type","PayRate","Dept"}
    missing = required - set(roster.columns)
    if missing:
        raise HTTPException(status_code=500, detail=f"Roster missing columns: {sorted(missing)}")
    # numeric pay rate
    roster["PayRate"] = pd.to_numeric(roster["PayRate"], errors="coerce").fillna(0.0)
    # unique by Employee Name (first occurrence wins)
    roster = roster.drop_duplicates(subset=["Employee Name"], keep="first")
    return roster

def aggregate_hours(df: pd.DataFrame, name_col: str, hrs_col: str, date_col: Optional[str]) -> Tuple[pd.DataFrame, date]:
    # keep the few columns we need
    use_cols = [name_col, hrs_col] + ([date_col] if date_col else [])
    df = df[use_cols].copy()

    # clean rows
    df[name_col] = df[name_col].astype(str).str.strip()
    df = df[df[name_col] != ""]
    df[hrs_col]  = pd.to_numeric(df[hrs_col], errors="coerce").fillna(0.0)

    if date_col:
        pe = compute_pe_from_dates(df[date_col])
        dt = pd.to_datetime(df[date_col], errors="coerce")
        df["_date"] = dt
        df["_dow"]  = df["_date"].dt.dayofweek  # Mon=0..Sun=6
    else:
        pe = sunday_for_week(date.today())
        df["_date"] = pd.NaT
        df["_dow"]  = pd.NA

    # weekly total hours per employee
    totals = (
        df.groupby(name_col, as_index=False)[hrs_col]
          .sum()
          .rename(columns={name_col: "Name", hrs_col: "TotalHours"})
    )

    # daily Mon..Fri (PC HRS)
    for dow, out_col in [(0,"MON"),(1,"TUE"),(2,"WED"),(3,"THU"),(4,"FRI")]:
        sub = (
            df[(df["_dow"] == dow)]
            .groupby(name_col, as_index=False)[hrs_col].sum()
            .rename(columns={name_col: "Name", hrs_col: out_col})
        )
        totals = totals.merge(sub, how="left", on="Name")

    for c in ["MON","TUE","WED","THU","FRI"]:
        if c not in totals.columns:
            totals[c] = 0.0
        totals[c] = pd.to_numeric(totals[c], errors="coerce").fillna(0.0)

    totals["Reg"] = totals["TotalHours"].clip(upper=40)
    totals["OT"]  = (totals["TotalHours"] - 40).clip(lower=0)
    totals["DT"]  = 0.0  # until we’re given a rule for double-time

    # Normalize to "Last, First" to match roster keys
    totals["Employee Name"] = totals["Name"].astype(str).map(to_last_first)

    return totals, pe

def build_wbs_weekly(roster: pd.DataFrame, agg: pd.DataFrame, pe_date: date) -> BytesIO:
    out = roster.merge(agg, how="left", on="Employee Name", validate="1:1")

    # If we have agg entries that didn't join, tell which are missing in roster
    missing = agg.loc[~agg["Employee Name"].isin(out["Employee Name"]), "Employee Name"].dropna().unique().tolist()
    if missing:
        raise HTTPException(
            status_code=422,
            detail=f"Employees missing from roster.csv: {sorted(set(missing))}"
        )

    for c in ["TotalHours","Reg","OT","DT","MON","TUE","WED","THU","FRI"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0.0)

    totals_hours = (out["Reg"].fillna(0) + out["OT"].fillna(0) + out["DT"].fillna(0))
    out["TotalsCalc"] = out["PayRate"].fillna(0) * totals_hours

    # Salary handling: use PayRate as flat weekly amount, show 40 Reg if hours missing
    is_salary = out["Type"].astype(str).str.upper().eq("S")
    out.loc[is_salary, "TotalsCalc"] = out.loc[is_salary, "PayRate"].fillna(0.0)
    out.loc[is_salary & (totals_hours == 0), "Reg"] = 40.0

    # Dates for header
    rpt_date = pe_date + timedelta(days=3)
    ck_date  = pe_date + timedelta(days=5)

    header_row_labels = [
        "# V","DO NOT EDIT","Version = B90216-00","FmtRev = 2.1",
        f"RunTime = {datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
        "CliUnqId = 055269","CliName = Sierra Roofing and Solar Inc","Freq = W",
        f"PEDate = {pe_date.strftime('%m/%d/%Y')}",
        f"RptDate = {rpt_date.strftime('%m/%d/%Y')}",
        f"CkDate = {ck_date.strftime('%m/%d/%Y')}",
        "EmpType = SSN","DoNotes = 1","PayRates = H+;S+;E+;C+",
        "RateCol = 6","T1 = 7+","CodeBeg = 8","CodeEnd = 26","NoteCol = 27",
        "","","","","","","",""
    ]

    rows: List[List] = []
    pad = [None]*(len(header_row_labels)-3)
    rows.append(["# U","CliUnqID","055269", *pad])
    rows.append(["# N","Client","Sierra Roofing and Solar Inc", *pad])
    rows.append(["# P","Period End",pe_date.strftime("%m/%d/%Y"), *pad])
    rows.append(["# R","Report Due",rpt_date.strftime("%m/%d/%Y"), *pad])
    rows.append(["# C","Check Date",ck_date.strftime("%m/%d/%Y"), *pad])
    rows.append([
        None,None,None,None,None,None,None,
        "REGULAR","OVERTIME","DOUBLETIME","VACATION","SICK","HOLIDAY","BONUS","COMMISSION",
        "PC HRS MON","PC HRS TUE","PC HRS WED","PC HRS THU","PC HRS FRI",
        "PC TTL MON","PC TTL TUE","PC TTL WED","PC TTL THU","PC TTL FRI",
        "TRAVEL AMOUNT","Comments","Totals"
    ])
    rows.append([
        "# E:26","SSN","Employee Name","Status","Type","Pay Rate","Dept",
        "A01","A02","A03","A04","A05","A06","A07","A08",
        "A09","A10","A11","A12","A13","A14","A15","A16","A17",
        "A18","A19","A26"
    ])

    def num(x):
        try:
            return float(x)
        except Exception:
            return 0.0

    # Sorted for consistent output
    for _, r in out.sort_values("Employee Name").iterrows():
        reg, ot, dt = num(r.get("Reg", 0)), num(r.get("OT", 0)), num(r.get("DT", 0))
        mon, tue, wed, thu, fri = (
            num(r.get("MON", 0)), num(r.get("TUE", 0)),
            num(r.get("WED", 0)), num(r.get("THU", 0)), num(r.get("FRI", 0))
        )
        row = [None]*len(header_row_labels)
        row[0] = str(r.get("EmpID", "") or "")
        row[1] = str(r.get("SSN", "") or "")
        row[2] = r.get("Employee Name", "")
        row[3] = r.get("Status", "")
        row[4] = r.get("Type", "")
        row[5] = num(r.get("PayRate", 0))
        row[6] = r.get("Dept", "")
        row[7]  = reg
        row[8]  = ot
        row[9]  = dt
        # A04..A08 (Vacation..Commission) intentionally blank
        row[15] = mon
        row[16] = tue
        row[17] = wed
        row[18] = thu
        row[19] = fri
        # A18, A19 are PC TTL* which remain blank (kept for template alignment)
        row[25] = None  # Travel Amount left blank
        row[26] = None  # Comments blank
        row[27] = round(num(r.get("TotalsCalc", 0)), 2)
        rows.append(row)

    df_weekly = pd.DataFrame(rows, columns=header_row_labels[:len(rows[0])])

    # ----- Write Excel with styles & frozen panes -----
    out_stream = BytesIO()
    with pd.ExcelWriter(out_stream, engine="openpyxl") as writer:
        # First meta/header row (# V)
        pd.DataFrame([header_row_labels], columns=header_row_labels).to_excel(
            writer, index=False, header=False, sheet_name="WEEKLY"
        )
        # Body rows start on next line
        df_weekly.to_excel(writer, index=False, header=False, sheet_name="WEEKLY", startrow=1)

        ws = writer.book["WEEKLY"]

        # Freeze panes: below mapping row and after Dept column (column H)
        # Rows in sheet:
        # 1: #V
        # 2: #U
        # 3: #N
        # 4: #P
        # 5: #R
        # 6: #C
        # 7: Category labels
        # 8: Mapping row "# E:26"
        # -> data begins at row 9, so freeze H9
        ws.freeze_panes = "H9"

        # Column widths so nothing is clipped
        widths = {
            1:10, 2:14, 3:32, 4:10, 5:8, 6:12, 7:12,   # EmpID..Dept
            8:12, 9:12, 10:12,                         # REG/OT/DT
            15:14,16:14,17:14,18:14,19:14,            # PC HRS MON..FRI
            20:14,21:14,22:14,23:14,24:14,            # PC TTL MON..FRI (kept blank)
            25:16, 26:18, 27:12                       # Travel, Comments, Totals
        }
        for c, w in widths.items():
            ws.column_dimensions[ws.cell(row=1, column=c).column_letter].width = w

        # Basic styling on mapping row & category labels
        from openpyxl.styles import Font, Alignment, PatternFill
        bold = Font(bold=True)
        center = Alignment(horizontal="center", vertical="center")
        fill = PatternFill("solid", fgColor="EDEDED")

        # category row (row 7 but +1 because we wrote #V first)
        cat_row = 6 + 1
        for col in range(8, 28):
            cell = ws.cell(row=cat_row, column=col)
            cell.font = bold
            cell.alignment = center
            cell.fill = fill

        # mapping row (# E:26) — row 7+1
        map_row = 7 + 1
        for col in range(1, 28):
            cell = ws.cell(row=map_row, column=col)
            cell.font = bold
            cell.alignment = center
            if col >= 8:
                cell.fill = fill

        # numeric formats (PayRate, REG/OT/DT, daily, Totals)
        num_cols = [6, 8, 9, 10, 15, 16, 17, 18, 19, 27]
        max_row = ws.max_row
        for r in range(map_row+1, max_row+1):
            for c in num_cols:
                ws.cell(row=r, column=c).number_format = '0.00'

    out_stream.seek(0)
    return out_stream

def read_all_sheets_to_frame(raw_excel: bytes) -> pd.DataFrame:
    """
    Load ALL sheets, tolerate weird headers, join rows together.
    This is where most 500s happen; we make this very defensive.
    """
    # Try reading all sheets
    all_frames: List[pd.DataFrame] = []

    try:
        xls = pd.read_excel(BytesIO(raw_excel), sheet_name=None, header=None, engine="openpyxl")
    except Exception as e:
        log.error("Failed to open Excel: %s", e, exc_info=True)
        raise HTTPException(status_code=422, detail=f"Could not read Excel file: {e}")

    for sheet_name, sheet_df in xls.items():
        if sheet_df is None or sheet_df.empty:
            continue

        # Guess which row is the header
        hdr_row = guess_header_row(sheet_df)
        try:
            df = pd.read_excel(BytesIO(raw_excel), sheet_name=sheet_name, header=hdr_row, engine="openpyxl")
        except Exception:
            # fallback to the raw with manual header assignment
            df = sheet_df.copy()
            df.columns = df.iloc[hdr_row].astype(str).tolist()
            df = df.iloc[hdr_row+1:].reset_index(drop=True)

        # Drop completely empty columns, standardize col names
        df = df.loc[:, ~df.columns.astype(str).str.match(r'^\s*$')]

        # Remove rows that are entirely NA
        df = df.dropna(how="all")
        if df.empty:
            continue

        all_frames.append(df)

    if not all_frames:
        raise HTTPException(status_code=422, detail="No usable data found in the Excel file (sheets were empty).")

    # Concatenate all usable frames
    big = pd.concat(all_frames, ignore_index=True)
    return big

# =========================
# Endpoints
# =========================
@app.get("/")
def root():
    return {"message": "Sierra Roofing Backend", "status": "running"}

@app.get("/health")
def health():
    return {"status": "healthy"}

@app.get("/employees")
def employees():
    roster = load_roster()
    # keep UI small and safe
    return [
        {
            "name": r["Employee Name"],
            "ssn": r["SSN"],
            "department": r["Dept"],
            "pay_rate": float(r["PayRate"]) if pd.notna(r["PayRate"]) else 0.0,
        }
        for _, r in roster.iterrows()
    ]

@app.post("/process-payroll")
async def process_payroll(file: UploadFile = File(...)):
    """
    Accept an Excel file, aggregate hours by employee, merge with roster,
    and return a WBS-formatted WEEKLY workbook.
    """
    if not (file.filename or "").lower().endswith((".xlsx",".xls")):
        raise HTTPException(status_code=422, detail="Please upload an Excel file (.xlsx or .xls).")

    try:
        raw = await file.read()
        # Read all sheets defensively, fix headers if needed
        df = read_all_sheets_to_frame(raw)

        # Try to locate key columns
        name_col = find_col(df, NAME_ALIASES)
        hrs_col  = find_col(df, HOURS_ALIASES)
        date_col = find_col(df, DATE_ALIASES)

        if not name_col or not hrs_col:
            detail = {
                "message": "Missing required columns in uploaded file",
                "need_one_of_name": sorted(NAME_ALIASES),
                "need_one_of_hours": sorted(HOURS_ALIASES),
                "got_columns": list(map(str, df.columns))
            }
            raise HTTPException(status_code=422, detail=detail)

        # Aggregate
        agg, pe = aggregate_hours(df, name_col, hrs_col, date_col)

        # Roster
        roster = load_roster()

        # Build WBS
        out_stream = build_wbs_weekly(roster, agg, pe)

        out_name = f"WBS_Payroll_{pe.strftime('%Y-%m-%d')}.xlsx"
        headers = {"Content-Disposition": f'attachment; filename="{out_name}"'}

        return StreamingResponse(
            out_stream,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )

    except HTTPException as he:
        # Reraise with the same status and structured detail
        raise he
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        log.error("Payroll processing error: %s\n%s", e, tb)
        # Return readable, structured error to the frontend instead of a blank 500
        return JSONResponse(
            status_code=500,
            content={"detail": f"{type(e).__name__}: {str(e)}", "traceback": tb.splitlines()}
        )

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    # "app.main:app" so uvicorn works both locally and on Railway
    uvicorn.run("app.main:app", host="0.0.0.0", port=port)
