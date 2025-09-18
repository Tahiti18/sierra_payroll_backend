# server/main.py
import io
from collections import defaultdict, Counter
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse, JSONResponse
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

# =============================================================================
# App + CORS
# =============================================================================
app = FastAPI(title="Sierra → WBS Payroll Converter", version="3.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten to your Netlify origin if desired
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ALLOWED_EXTS = (".xlsx", ".xls")

# Data rows begin at row 9 in the WBS template (row 7 categories, row 8 headers)
WBS_DATA_START_ROW = 9

# Column indexes in the template (1-based)
COL = {
    "EMP_ID": 1,            # "# E:26" / internal ID (unused)
    "SSN": 2,
    "EMP_NAME": 3,
    "STATUS": 4,
    "TYPE": 5,
    "PAY_RATE": 6,
    "DEPT": 7,
    "A01": 8,               # Regular
    "A02": 9,               # OT
    "A03": 10,              # DT
    "A06": 11,              # Vacation (unused unless fed)
    "A07": 12,              # Sick (unused unless fed)
    "A08": 13,              # Holiday (unused unless fed)
    "A04": 14,              # Bonus (unused unless fed)
    "A05": 15,              # Commission (unused unless fed)
    "AH1": 16, "AI1": 17,   # Mon hours / piece $
    "AH2": 18, "AI2": 19,   # Tue
    "AH3": 20, "AI3": 21,   # Wed
    "AH4": 22, "AI4": 23,   # Thu
    "AH5": 24, "AI5": 25,   # Fri
    "ATE": 26,              # Travel amount
    "COMMENTS": 27,
    "TOTALS": 28,
}

# If your Sierra file contains a “Roster” sheet (Employee, SSN, Dept, Type, Pay Rate) we’ll use it
ROSTER_SHEET_NAME = "Roster"
PIECEWORK_SHEET_NAME = "Piecework"  # optional: (Employee, Date, Amount)

# =============================================================================
# Helpers
# =============================================================================
def _ext_ok(filename: str) -> bool:
    fn = (filename or "").lower()
    return any(fn.endswith(e) for e in ALLOWED_EXTS)

def _std(s: str) -> str:
    return (s or "").strip().lower().replace("\n", " ").replace("\r", " ")

def _normalize_name(raw: str) -> str:
    if not isinstance(raw, str):
        raw = "" if pd.isna(raw) else str(raw)
    name = raw.strip()
    if not name:
        return ""
    if "," in name:
        return name
    parts = [p for p in name.split() if p]
    if len(parts) >= 2:
        return f"{parts[-1]}, {' '.join(parts[:-1])}"
    return name

def _to_date(val) -> Optional[date]:
    if pd.isna(val):
        return None
    if isinstance(val, date):
        return val
    if isinstance(val, datetime):
        return val.date()
    try:
        return pd.to_datetime(val).date()
    except Exception:
        # fallback: weekday text → index only
        return None

def _weekday_from_any(val) -> Optional[int]:
    """
    Return 1..5 for Mon..Fri, else None. Accepts a date or a weekday string.
    """
    d = _to_date(val)
    if d:
        wd = d.weekday()  # Mon=0
        return wd + 1 if 0 <= wd <= 4 else None
    # try text
    s = str(val or "").strip().lower()
    maptxt = {
        "m":1,"mon":1,"monday":1,
        "t":2,"tu":2,"tue":2,"tuesday":2,
        "w":3,"wed":3,"wednesday":3,
        "th":4,"thu":4,"thur":4,"thurs":4,"thursday":4,
        "f":5,"fri":5,"friday":5
    }
    return maptxt.get(s)

def _apply_ca_daily_ot(hours: float) -> Dict[str, float]:
    """
    CA daily split:
      0–8   → A01 (REG)
      8–12  → A02 (OT)
      >12   → A03 (DT)
    """
    h = float(hours or 0.0)
    a01 = min(h, 8.0)
    a02 = 0.0
    a03 = 0.0
    if h > 8:
        a02 = min(h - 8.0, 4.0)
    if h > 12:
        a03 = h - 12.0
    return {"A01": a01, "A02": a02, "A03": a03}

def _mode(values: List[float]) -> float:
    vals = [float(v) for v in values if pd.notna(v)]
    if not vals:
        return 0.0
    c = Counter(vals)
    return max(c.items(), key=lambda kv: (kv[1], kv[0]))[0]

def _safe_float(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

# =============================================================================
# Core converter: Sierra → exact WBS template
# =============================================================================
def convert_sierra_to_wbs(input_bytes: bytes, sheet_name: Optional[str] = None) -> bytes:
    # Load Sierra workbook
    xls = pd.ExcelFile(io.BytesIO(input_bytes))
    main_sheet = sheet_name or xls.sheet_names[0]
    df_in = xls.parse(main_sheet)

    if df_in.empty:
        raise ValueError("Input sheet is empty.")

    # Map headers flexibly
    hdr = {
        "name": ["employee", "employee name", "name", "worker", "employee_name"],
        "date": ["date", "day", "work date", "worked date", "days"],
        "hours": ["hours", "hrs", "total hours", "work hours"],
        "rate": ["rate", "pay rate", "hourly rate", "wage", "pay_rate", "salary"],
        "dept": ["department", "dept"],
        "ssn": ["ssn", "social", "social security"],
        "type": ["type", "emp type", "employee type", "pay type"],
    }

    def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
        cols = { _std(c): c for c in df.columns }
        # exact
        for w in candidates:
            k = _std(w)
            if k in cols: return cols[k]
        # relaxed contains
        for w in candidates:
            k = _std(w)
            for kk,vv in cols.items():
                if k in kk: return vv
        return None

    c_name  = _find_col(df_in, hdr["name"])
    c_date  = _find_col(df_in, hdr["date"])
    c_hours = _find_col(df_in, hdr["hours"])
    c_rate  = _find_col(df_in, hdr["rate"])
    c_dept  = _find_col(df_in, hdr["dept"])
    c_ssn   = _find_col(df_in, hdr["ssn"])
    c_type  = _find_col(df_in, hdr["type"])

    for req, col in {"Name":c_name, "Date/Day":c_date, "Hours":c_hours, "Rate":c_rate}.items():
        if not col:
            raise ValueError(f"Missing required column: {req}")

    core = df_in[[c_name, c_date, c_hours, c_rate]].copy()
    core.columns = ["employee", "date", "hours", "rate"]
    if c_dept: core["dept"] = df_in[c_dept]
    else:      core["dept"] = ""
    if c_ssn:  core["ssn"] = df_in[c_ssn]
    else:      core["ssn"] = ""
    if c_type: core["wtype"] = df_in[c_type]
    else:      core["wtype"] = ""

    # Normalize
    core["employee"] = core["employee"].astype(str).map(_normalize_name)
    core["hours"]    = pd.to_numeric(core["hours"], errors="coerce").fillna(0.0).astype(float)
    core["rate"]     = pd.to_numeric(core["rate"],  errors="coerce").fillna(0.0).astype(float)

    # Derive weekday index (1..5 Mon..Fri)
    wd_idx = core["date"].apply(_weekday_from_any)
    core["wd"] = wd_idx

    # Valid rows
    core = core[(core["employee"].str.len() > 0) & (core["hours"] > 0) & core["wd"].notna()]
    if core.empty:
        raise ValueError("No valid rows after cleaning (check Employee/Date/Hours/Days).")

    # Optional Roster sheet
    roster = {}
    if ROSTER_SHEET_NAME in xls.sheet_names:
        try:
            r = xls.parse(ROSTER_SHEET_NAME)
            rn = _find_col(r, ["employee", "employee name", "name"])
            if rn:
                r[rn] = r[rn].astype(str).map(_normalize_name)
                ssn_col = _find_col(r, ["ssn"])
                dep_col = _find_col(r, ["department","dept"])
                typ_col = _find_col(r, ["type","emp type"])
                pr_col  = _find_col(r, ["pay rate","rate","salary"])
                for _, row in r.iterrows():
                    emp = _normalize_name(row.get(rn, ""))
                    if not emp: continue
                    roster[emp] = {
                        "ssn": str(row.get(ssn_col, "") or "") if ssn_col else "",
                        "dept": str(row.get(dep_col, "") or "") if dep_col else "",
                        "type": str(row.get(typ_col, "") or "") if typ_col else "",
                        "rate": float(row.get(pr_col, 0.0) or 0.0) if pr_col else 0.0,
                    }
        except Exception:
            pass

    # Optional Piecework sheet (Employee, Date, Amount) → AI* per weekday
    piece_totals = defaultdict(lambda: defaultdict(float))  # emp -> wd -> amount
    if PIECEWORK_SHEET_NAME in xls.sheet_names:
        try:
            pw = xls.parse(PIECEWORK_SHEET_NAME)
            rn = _find_col(pw, ["employee","employee name","name"])
            dn = _find_col(pw, ["date","day","worked date","days"])
            an = _find_col(pw, ["amount","ttl","total"])
            if rn and dn and an:
                pw[rn] = pw[rn].astype(str).map(_normalize_name)
                for _, row in pw.iterrows():
                    emp = _normalize_name(row.get(rn, ""))
                    wd  = _weekday_from_any(row.get(dn))
                    amt = _safe_float(row.get(an, 0))
                    if emp and wd and 1 <= wd <= 5:
                        piece_totals[emp][wd] += amt
        except Exception:
            pass

    # Aggregate per employee/day
    per_emp_day = core.groupby(["employee","wd"]).agg({
        "hours":"sum",
        "rate": list,           # keep all rates to choose modal later
        "dept": lambda s: next((x for x in s if str(x).strip()), ""),  # first non-empty
        "ssn":  lambda s: next((x for x in s if str(x).strip()), ""),
        "wtype": lambda s: next((x for x in s if str(x).strip()), ""),
    }).reset_index()

    # Build weekly per-employee totals (A01/A02/A03) and AH* daily hours
    weekly_hours = {}  # emp -> dict
    daily_hours  = defaultdict(lambda: defaultdict(float))  # emp -> wd -> hours
    emp_rates    = defaultdict(list)
    emp_dept     = {}
    emp_ssn      = {}
    emp_type     = {}

    for _, row in per_emp_day.iterrows():
        emp = row["employee"]
        wd  = int(row["wd"])
        hrs = float(row["hours"] or 0.0)
        daily_hours[emp][wd] += hrs

        # CA daily split
        dist = _apply_ca_daily_ot(hrs)
        agg = weekly_hours.get(emp, {"A01":0.0,"A02":0.0,"A03":0.0})
        agg["A01"] += dist["A01"]
        agg["A02"] += dist["A02"]
        agg["A03"] += dist["A03"]
        weekly_hours[emp] = agg

        # Collect rate samples (for modal)
        emp_rates[emp].extend(row["rate"] if isinstance(row["rate"], list) else [row["rate"]])

        # Keep first non-empty identity fields
        if emp not in emp_dept or not str(emp_dept[emp]).strip():
            emp_dept[emp] = row["dept"] or ""
        if emp not in emp_ssn or not str(emp_ssn[emp]).strip():
            emp_ssn[emp] = row["ssn"] or ""
        if emp not in emp_type or not str(emp_type[emp]).strip():
            emp_type[emp] = row["wtype"] or ""

    # Sort employees by Dept then Name for stable output
    employees = sorted(weekly_hours.keys(), key=lambda e: (str(emp_dept.get(e,"")), e))

    # Load WBS template (fresh each request; never saved back)
here = Path(__file__).resolve().parent
template_path = here.parent / "wbs_template.xlsx"  # template is in the same folder as main.py
if not template_path.exists():
    raise HTTPException(status_code=500, detail=f"WBS template not found at {template_path}")

    wb = load_workbook(str(template_path))
    ws = wb.active  # the single WEEKLY sheet in your template

    # Clear any prior data rows (if template has sample rows)
    # We clear rows from WBS_DATA_START_ROW down to current max_row where there is any data in the name column.
    max_row = ws.max_row
    if max_row >= WBS_DATA_START_ROW:
        # If template already contains data rows, blank them out (values only) to keep styles
        for r in range(WBS_DATA_START_ROW, max_row+1):
            # if row looks empty already, skip
            if all((ws.cell(row=r, column=c).value in (None, "")) for c in range(1, COL["TOTALS"]+1)):
                continue
            for c in range(1, COL["TOTALS"]+1):
                ws.cell(row=r, column=c).value = None

    # Write employee rows
    current_row = WBS_DATA_START_ROW
    for emp in employees:
        # roster overrides if present
        ssn  = emp_ssn.get(emp, "") or ""
        dept = str(emp_dept.get(emp, "") or "").upper()
        typ  = str(emp_type.get(emp, "") or "").upper()
        rate_modal = _mode(emp_rates.get(emp, []))

        if emp in roster:
            if roster[emp].get("ssn"):  ssn  = roster[emp]["ssn"]
            if roster[emp].get("dept"): dept = roster[emp]["dept"].upper()
            if roster[emp].get("type"): typ  = roster[emp]["type"].upper()
            if roster[emp].get("rate", 0) > 0: rate_modal = float(roster[emp]["rate"])

        # Fallback type if unset: treat rate >= 1000 as salary
        if typ not in ("H", "S"):
            typ = "S" if rate_modal >= 1000 else "H"

        a01 = weekly_hours[emp]["A01"]
        a02 = weekly_hours[emp]["A02"]
        a03 = weekly_hours[emp]["A03"]

        # Daily hours Mon..Fri
        ah = {i: round(float(daily_hours[emp].get(i, 0.0)), 2) for i in range(1,6)}

        # Piece totals (if fed via optional sheet)
        ai = {i: round(float(piece_totals[emp].get(i, 0.0)), 2) for i in range(1,6)}

        # Travel + comments (blank; add your own rules if needed)
        ate = 0.0
        comments = ""

        # Row write
        ws.cell(row=current_row, column=COL["EMP_ID"]).value  = ""                 # leave blank
        ws.cell(row=current_row, column=COL["SSN"]).value     = ssn
        ws.cell(row=current_row, column=COL["EMP_NAME"]).value= emp
        ws.cell(row=current_row, column=COL["STATUS"]).value  = "A"
        ws.cell(row=current_row, column=COL["TYPE"]).value    = typ
        ws.cell(row=current_row, column=COL["PAY_RATE"]).value= round(_safe_float(rate_modal), 2)
        ws.cell(row=current_row, column=COL["DEPT"]).value    = dept

        ws.cell(row=current_row, column=COL["A01"]).value = round(a01, 2)
        ws.cell(row=current_row, column=COL["A02"]).value = round(a02, 2)
        ws.cell(row=current_row, column=COL["A03"]).value = round(a03, 2)

        # Leave A06/A07/A08/A04/A05 blank (unless you feed them later)
        for key in ("A06","A07","A08","A04","A05"):
            ws.cell(row=current_row, column=COL[key]).value = None

        # AH*/AI* daily
        ws.cell(row=current_row, column=COL["AH1"]).value = ah[1] if ah[1] else None
        ws.cell(row=current_row, column=COL["AI1"]).value = ai[1] if ai[1] else None
        ws.cell(row=current_row, column=COL["AH2"]).value = ah[2] if ah[2] else None
        ws.cell(row=current_row, column=COL["AI2"]).value = ai[2] if ai[2] else None
        ws.cell(row=current_row, column=COL["AH3"]).value = ah[3] if ah[3] else None
        ws.cell(row=current_row, column=COL["AI3"]).value = ai[3] if ai[3] else None
        ws.cell(row=current_row, column=COL["AH4"]).value = ah[4] if ah[4] else None
        ws.cell(row=current_row, column=COL["AI4"]).value = ai[4] if ai[4] else None
        ws.cell(row=current_row, column=COL["AH5"]).value = ah[5] if ah[5] else None
        ws.cell(row=current_row, column=COL["AI5"]).value = ai[5] if ai[5] else None

        ws.cell(row=current_row, column=COL["ATE"]).value = ate if ate else None
        ws.cell(row=current_row, column=COL["COMMENTS"]).value = comments or None

        # Totals logic:
        #   Hourly: (A01 + 1.5*A02 + 2*A03) * Pay Rate + AI1..AI5 + ATE
        #   Salaried (Type S): Pay Rate + AI1..AI5 + ATE
        c = lambda key: get_column_letter(COL[key])
        pr = f"{c('PAY_RATE')}{current_row}"
        a01c = f"{c('A01')}{current_row}"
        a02c = f"{c('A02')}{current_row}"
        a03c = f"{c('A03')}{current_row}"
        ai_sum = "+".join([f"{c('AI1')}{current_row}", f"{c('AI2')}{current_row}",
                           f"{c('AI3')}{current_row}", f"{c('AI4')}{current_row}",
                           f"{c('AI5')}{current_row}"])
        ai_sum = f"({ai_sum})"
        atec = f"{c('ATE')}{current_row}"
        # guard empty cells in sum
        ai_sum_safe = f"SUM({c('AI1')}{current_row}:{c('AI5')}{current_row})"

        if typ == "S":
            formula = f"IFERROR({pr} + {ai_sum_safe} + IF({atec}=\"\",0,{atec}), 0)"
        else:
            formula = f"IFERROR(({a01c} + 1.5*{a02c} + 2*{a03c})*{pr} + {ai_sum_safe} + IF({atec}=\"\",0,{atec}), 0)"
        ws.cell(row=current_row, column=COL["TOTALS"]).value = f"={formula}"

        current_row += 1

    last_data_row = current_row - 1

    # Add bottom Grand Totals row (SUM down each numeric column we care about)
    if last_data_row >= WBS_DATA_START_ROW:
        totals_row = current_row
        # Label in the Comments column (like your sample shows right-aligned total band)
        ws.cell(row=totals_row, column=COL["COMMENTS"]).value = "Totals"

        # Sum A01/A02/A03, A04..A08, AH*/AI*, ATE, TOTALS
        cols_to_sum = [
            "A01","A02","A03","A06","A07","A08","A04","A05",
            "AH1","AI1","AH2","AI2","AH3","AI3","AH4","AI4","AH5","AI5",
            "ATE","TOTALS"
        ]
        for key in cols_to_sum:
            col_letter = get_column_letter(COL[key])
            ws.cell(row=totals_row, column=COL[key]).value = f"=SUM({col_letter}{WBS_DATA_START_ROW}:{col_letter}{last_data_row})"

    # Stream result (do NOT save template to disk)
    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    return out.read()

# =============================================================================
# Routes
# =============================================================================
@app.get("/health")
def health():
    return JSONResponse({"ok": True, "ts": datetime.utcnow().isoformat() + "Z"})

@app.post("/process-payroll")
async def process_payroll(file: UploadFile = File(...)):
    if not file or not file.filename:
        raise HTTPException(status_code=400, detail="No selected file.")
    if not _ext_ok(file.filename):
        raise HTTPException(status_code=415, detail="Unsupported file type. Please upload .xlsx or .xls")

    try:
        contents = await file.read()
        out_bytes = convert_sierra_to_wbs(contents, sheet_name=None)
        out_name = f"WBS Payroll {datetime.utcnow().date().isoformat()}.xlsx"
        return StreamingResponse(
            io.BytesIO(out_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename=\"{out_name}\"'}
        )
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")
