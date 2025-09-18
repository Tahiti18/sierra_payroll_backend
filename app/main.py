# app/main.py
import io
from collections import defaultdict, Counter
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse, JSONResponse
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

app = FastAPI(title="Sierra → WBS Payroll Converter", version="3.0.2")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ALLOWED_EXTS = (".xlsx", ".xls")

# WBS template layout (columns are 1-based)
WBS_DATA_START_ROW = 9
COL = {
    "EMP_ID": 1, "SSN": 2, "EMP_NAME": 3, "STATUS": 4, "TYPE": 5, "PAY_RATE": 6, "DEPT": 7,
    "A01": 8, "A02": 9, "A03": 10, "A06": 11, "A07": 12, "A08": 13, "A04": 14, "A05": 15,
    "AH1": 16, "AI1": 17, "AH2": 18, "AI2": 19, "AH3": 20, "AI3": 21, "AH4": 22, "AI4": 23, "AH5": 24, "AI5": 25,
    "ATE": 26, "COMMENTS": 27, "TOTALS": 28,
}

ROSTER_SHEET_NAME = "Roster"
PIECEWORK_SHEET_NAME = "Piecework"

# ---------------- helpers ----------------
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

def _to_date(val):
    if pd.isna(val): return None
    if isinstance(val, date): return val
    if isinstance(val, datetime): return val.date()
    try: return pd.to_datetime(val).date()
    except Exception: return None

def _weekday_from_any(val) -> Optional[int]:
    d = _to_date(val)
    if d:
        wd = d.weekday()  # Mon=0
        return wd + 1 if 0 <= wd <= 4 else None
    s = str(val or "").strip().lower()
    m = {"m":1,"mon":1,"monday":1,"t":2,"tu":2,"tue":2,"tuesday":2,"w":3,"wed":3,"wednesday":3,
         "th":4,"thu":4,"thur":4,"thurs":4,"thursday":4,"f":5,"fri":5,"friday":5}
    return m.get(s)

def _apply_ca_daily_ot(hours: float) -> Dict[str, float]:
    h = float(hours or 0.0)
    a01 = min(h, 8.0); a02 = 0.0; a03 = 0.0
    if h > 8:  a02 = min(h - 8.0, 4.0)
    if h > 12: a03 = h - 12.0
    return {"A01": a01, "A02": a02, "A03": a03}

def _mode(values: List[float]) -> float:
    vals = [float(v) for v in values if pd.notna(v)]
    if not vals: return 0.0
    c = Counter(vals)
    return max(c.items(), key=lambda kv: (kv[1], kv[0]))[0]

def _safe_float(x) -> float:
    try: return float(x)
    except Exception: return 0.0

# -------------- core conversion --------------
def convert_sierra_to_wbs(input_bytes: bytes, sheet_name: Optional[str] = None) -> bytes:
    xls = pd.ExcelFile(io.BytesIO(input_bytes))
    main_sheet = sheet_name or xls.sheet_names[0]
    df_in = xls.parse(main_sheet)
    if df_in.empty:
        raise ValueError("Input sheet is empty.")

    hdr = {
        "name":  ["employee", "employee name", "name", "worker", "employee_name"],
        "date":  ["date", "day", "work date", "worked date", "days"],
        "hours": ["hours", "hrs", "total hours", "work hours"],
        "rate":  ["rate", "pay rate", "hourly rate", "wage", "pay_rate", "salary"],
        "dept":  ["department", "dept"],
        "ssn":   ["ssn", "social", "social security"],
        "type":  ["type", "emp type", "employee type", "pay type"],
    }

    def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
        cols = { _std(c): c for c in df.columns }
        for w in candidates:
            k = _std(w)
            if k in cols: return cols[k]
        for w in candidates:
            k = _std(w)
            for kk, vv in cols.items():
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
    core["dept"]  = df_in[c_dept] if c_dept else ""
    core["ssn"]   = df_in[c_ssn] if c_ssn else ""
    core["wtype"] = df_in[c_type] if c_type else ""

    core["employee"] = core["employee"].astype(str).map(_normalize_name)
    core["hours"]    = pd.to_numeric(core["hours"], errors="coerce").fillna(0.0).astype(float)
    core["rate"]     = pd.to_numeric(core["rate"],  errors="coerce").fillna(0.0).astype(float)
    core["wd"]       = core["date"].apply(_weekday_from_any)

    core = core[(core["employee"].str.len() > 0) & (core["hours"] > 0) & core["wd"].notna()]
    if core.empty:
        raise ValueError("No valid rows after cleaning (check Employee/Date/Hours/Days).")

    # Optional: Roster sheet
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

    # Optional: Piecework sheet
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

    # Aggregate
    per_emp_day = core.groupby(["employee","wd"]).agg({
        "hours":"sum",
        "rate": list,
        "dept":  lambda s: next((x for x in s if str(x).strip()), ""),
        "ssn":   lambda s: next((x for x in s if str(x).strip()), ""),
        "wtype": lambda s: next((x for x in s if str(x).strip()), ""),
    }).reset_index()

    weekly_hours = {}
    daily_hours  = defaultdict(lambda: defaultdict(float))
    emp_rates, emp_dept, emp_ssn, emp_type = defaultdict(list), {}, {}, {}

    for _, row in per_emp_day.iterrows():
        emp = row["employee"]; wd = int(row["wd"]); hrs = float(row["hours"] or 0.0)
        daily_hours[emp][wd] += hrs
        dist = _apply_ca_daily_ot(hrs)
        agg = weekly_hours.get(emp, {"A01":0.0,"A02":0.0,"A03":0.0})
        agg["A01"] += dist["A01"]; agg["A02"] += dist["A02"]; agg["A03"] += dist["A03"]
        weekly_hours[emp] = agg
        emp_rates[emp].extend(row["rate"] if isinstance(row["rate"], list) else [row["rate"]])
        if emp not in emp_dept or not str(emp_dept[emp]).strip(): emp_dept[emp] = row["dept"] or ""
        if emp not in emp_ssn  or not str(emp_ssn[emp]).strip():  emp_ssn[emp]  = row["ssn"] or ""
        if emp not in emp_type or not str(emp_type[emp]).strip(): emp_type[emp] = row["wtype"] or ""

    employees = sorted(weekly_hours.keys(), key=lambda e: (str(emp_dept.get(e,"")), e))

    # Load WBS template from repo root (since app/main.py is in app/)
    here = Path(__file__).resolve().parent
    template_path = here.parent / "wbs_template.xlsx"
    if not template_path.exists():
        raise HTTPException(status_code=500, detail=f"WBS template not found at {template_path}")

    wb = load_workbook(str(template_path))
    ws = wb.active

    # Clear existing data rows but keep styles
    max_row = ws.max_row
    if max_row >= WBS_DATA_START_ROW:
        for r in range(WBS_DATA_START_ROW, max_row+1):
            if all((ws.cell(row=r, column=c).value in (None, "")) for c in range(1, COL["TOTALS"]+1)):
                continue
            for c in range(1, COL["TOTALS"]+1):
                ws.cell(row=r, column=c).value = None

    # Write rows
    current_row = WBS_DATA_START_ROW
    for emp in employees:
        ssn  = emp_ssn.get(emp, "") or ""
        dept = str(emp_dept.get(emp, "") or "").upper()
        typ  = str(emp_type.get(emp, "") or "").upper()
        rate_modal = _mode(emp_rates.get(emp, []))

        if emp in roster:
            if roster[emp].get("ssn"):  ssn  = roster[emp]["ssn"]
            if roster[emp].get("dept"): dept = roster[emp]["dept"].upper()
            if roster[emp].get("type"): typ  = roster[emp]["type"].upper()
            if roster[emp].get("rate", 0) > 0: rate_modal = float(roster[emp]["rate"])

        if typ not in ("H", "S"):
            typ = "S" if rate_modal >= 1000 else "H"

        a01 = weekly_hours[emp]["A01"]; a02 = weekly_hours[emp]["A02"]; a03 = weekly_hours[emp]["A03"]
        ah = {i: round(float(daily_hours[emp].get(i, 0.0)), 2) for i in range(1,6)}
        ai = {i: round(float(piece_totals[emp].get(i, 0.0)), 2) for i in range(1,6)}

        ws.cell(row=current_row, column=COL["EMP_ID"]).value   = ""
        ws.cell(row=current_row, column=COL["SSN"]).value      = ssn
        ws.cell(row=current_row, column=COL["EMP_NAME"]).value = emp
        ws.cell(row=current_row, column=COL["STATUS"]).value   = "A"
        ws.cell(row=current_row, column=COL["TYPE"]).value     = typ
        ws.cell(row=current_row, column=COL["PAY_RATE"]).value = round(_safe_float(rate_modal), 2)
        ws.cell(row=current_row, column=COL["DEPT"]).value     = dept

        ws.cell(row=current_row, column=COL["A01"]).value = round(a01, 2)
        ws.cell(row=current_row, column=COL["A02"]).value = round(a02, 2)
        ws.cell(row=current_row, column=COL["A03"]).value = round(a03, 2)

        for key in ("A06","A07","A08","A04","A05"):
            ws.cell(row=current_row, column=COL[key]).value = None

        for i in range(1,6):
            ws.cell(row=current_row, column=COL[f"AH{i}"]).value = ah[i] if ah[i] else None
            ws.cell(row=current_row, column=COL[f"AI{i}"]).value = ai[i] if ai[i] else None

        ws.cell(row=current_row, column=COL["ATE"]).value = None
        ws.cell(row=current_row, column=COL["COMMENTS"]).value = None

        c = lambda key: get_column_letter(COL[key])
        pr, a01c, a02c, a03c = (f"{c('PAY_RATE')}{current_row}", f"{c('A01')}{current_row}",
                                f"{c('A02')}{current_row}", f"{c('A03')}{current_row}")
        ai_sum = f"SUM({c('AI1')}{current_row}:{c('AI5')}{current_row})"
        atec = f"{c('ATE')}{current_row}"
        if typ == "S":
            formula = f"IFERROR({pr} + {ai_sum} + IF({atec}=\"\",0,{atec}), 0)"
        else:
            formula = f"IFERROR(({a01c} + 1.5*{a02c} + 2*{a03c})*{pr} + {ai_sum} + IF({atec}=\"\",0,{atec}), 0)"
        ws.cell(row=current_row, column=COL["TOTALS"]).value = f"={formula}"

        current_row += 1

    last_data_row = current_row - 1
    if last_data_row >= WBS_DATA_START_ROW:
        totals_row = current_row
        ws.cell(row=totals_row, column=COL["COMMENTS"]).value = "Totals"
        for key in ["A01","A02","A03","A06","A07","A08","A04","A05",
                    "AH1","AI1","AH2","AI2","AH3","AI3","AH4","AI4","AH5","AI5","ATE","TOTALS"]:
            col = get_column_letter(COL[key])
            ws.cell(row=totals_row, column=COL[key]).value = f"=SUM({col}{WBS_DATA_START_ROW}:{col}{last_data_row})"

    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    return out.read()

# --------------- routes ---------------
@app.get("/health")
def health():
    return JSONResponse({"ok": True, "ts": datetime.utcnow().isoformat() + "Z"})

def _ext_ok(filename: str) -> bool:
    fn = (filename or "").lower()
    return any(fn.endswith(e) for e in ALLOWED_EXTS)

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
            headers={"Content-Disposition": f'attachment; filename="{out_name}"'}
        )
    except ValueError as ve:
        # show precise validation failures to frontend and logs
        import traceback, sys
        traceback.print_exc(file=sys.stderr)
        raise HTTPException(status_code=422, detail=str(ve))
    except Exception as e:
        # print full traceback to Railway logs and surface message
        import traceback, sys
        traceback.print_exc(file=sys.stderr)
        raise HTTPException(status_code=500, detail=f"backend processing failed: {e}")
