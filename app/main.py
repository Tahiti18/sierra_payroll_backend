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

app = FastAPI(title="Sierra → WBS Payroll Converter", version="3.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten for prod if needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ALLOWED_EXTS = (".xlsx", ".xls")

# =====================  WBS template layout (1-based)  ======================
WBS_DATA_START_ROW = 9
COL = {
    "EMP_ID": 1, "SSN": 2, "EMP_NAME": 3, "STATUS": 4, "TYPE": 5, "PAY_RATE": 6, "DEPT": 7,
    "A01": 8, "A02": 9, "A03": 10, "A06": 11, "A07": 12, "A08": 13, "A04": 14, "A05": 15,
    "AH1": 16, "AI1": 17, "AH2": 18, "AI2": 19, "AH3": 20, "AI3": 21, "AH4": 22, "AI4": 23, "AH5": 24, "AI5": 25,
    "ATE": 26, "COMMENTS": 27, "TOTALS": 28,
}

ROSTER_FILE = "roster.xlsx"          # repo root
ROSTER_SHEET_NAME = None             # or set a specific sheet name if needed
PIECEWORK_SHEET_NAME = "Piecework"   # optional tab in Sierra file

# ===============================  Helpers  ==================================
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

# ============================  Core Conversion  =============================
def convert_sierra_to_wbs(input_bytes: bytes, sheet_name: Optional[str] = None) -> bytes:
    xls = pd.ExcelFile(io.BytesIO(input_bytes))
    main_sheet = sheet_name or xls.sheet_names[0]
    df_in = xls.parse(main_sheet)
    if df_in.empty:
        raise ValueError("Input sheet is empty.")

    # Sierra header mapping
    hdr = {
        "name":  ["employee", "employee name", "name", "worker", "employee_name"],
        "date":  ["date", "day", "work date", "worked date", "days"],
        "hours": ["hours", "hrs", "total hours", "work hours"],
        "rate":  ["rate", "pay rate", "hourly rate", "wage", "pay_rate", "salary"],
        "dept":  ["department", "dept"],
        "ssn":   ["ssn", "social", "social security"],
        "type":  ["type", "emp type", "employee type", "pay type"],
    }

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

    # Normalize
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

    # Optional piecework totals by weekday
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
                    try:
                        amt = float(row.get(an, 0) or 0)
                    except Exception:
                        amt = 0.0
                    if emp and wd and 1 <= wd <= 5:
                        piece_totals[emp][wd] += amt
        except Exception:
            pass

    # Aggregate per day then weekly
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

    # ----------------- Enrich from roster.xlsx (repo root) ------------------
    here = Path(__file__).resolve().parent
    roster_path = here.parent / ROSTER_FILE
    roster_map: Dict[str, Dict[str, str]] = {}
    if roster_path.exists():
        try:
            rdf = pd.read_excel(roster_path, sheet_name=ROSTER_SHEET_NAME)
            name_col = _find_col(rdf, ["employee","employee name","name"])
            ssn_col  = _find_col(rdf, ["ssn","social","social security","social security number"])
            dept_col = _find_col(rdf, ["department","dept","division"])
            rate_col = _find_col(rdf, ["rate","pay rate","hourly rate","wage","salary"])
            if name_col:
                for _, rr in rdf.iterrows():
                    key = _normalize_name(rr.get(name_col, ""))
                    if not key: continue
                    ssn  = str(rr.get(ssn_col, "") or "") if ssn_col else ""
                    dept = str(rr.get(dept_col, "") or "") if dept_col else ""
                    try:
                        rrate = float(rr.get(rate_col, 0) or 0) if rate_col else 0.0
                    except Exception:
                        rrate = 0.0
                    roster_map[key] = {"ssn": ssn, "dept": dept, "rate": rrate}
        except Exception:
            roster_map = {}

    # ------------------------ Load WBS template ----------------------------
    template_path = here.parent / "wbs_template.xlsx"
    if not template_path.exists():
        raise HTTPException(status_code=500, detail=f"WBS template not found at {template_path}")

    wb = load_workbook(str(template_path))
    ws = wb.active

    # -------------------- Clear old rows (skip merged) ---------------------
    max_row = ws.max_row
    if max_row >= WBS_DATA_START_ROW:
        for r in range(WBS_DATA_START_ROW, max_row + 1):
            # row already blank? skip
            if all((ws.cell(row=r, column=c).value in (None, "")) for c in range(1, COL["TOTALS"] + 1)):
                continue
            for c in range(1, COL["TOTALS"] + 1):
                try:
                    ws.cell(row=r, column=c).value = None
                except AttributeError:
                    # merged cells are read-only in openpyxl; skip them
                    continue

    # ----------------------- Write employee rows ---------------------------
    current_row = WBS_DATA_START_ROW
    for emp in employees:
        ssn  = emp_ssn.get(emp, "") or ""
        dept = str(emp_dept.get(emp, "") or "").upper()
        typ  = str(emp_type.get(emp, "") or "").upper()
        rate_modal = _mode(emp_rates.get(emp, []))

        # roster overrides
        if emp in roster_map:
            r = roster_map[emp]
            if not ssn and r.get("ssn"): ssn = r["ssn"]
            if not dept and r.get("dept"): dept = str(r["dept"]).upper()
            if (rate_modal == 0.0) and r.get("rate", 0) > 0: rate_modal = float(r["rate"])

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

        # zero or blank for unused codes
        for key in ("A06","A07","A08","A04","A05"):
            ws.cell(row=current_row, column=COL[key]).value = None

        for i in range(1,6):
            ws.cell(row=current_row, column=COL[f"AH{i}"]).value = ah[i] if ah[i] else None
            ws.cell(row=current_row, column=COL[f"AI{i}"]).value = ai[i] if ai[i] else None

        ws.cell(row=current_row, column=COL["ATE"]).value = None
        ws.cell(row=current_row, column=COL["COMMENTS"]).value = None

        # --------- Totals formula to match WBS exactly (Hourly vs Salaried)
        # F = Pay Rate, H = A01, I = A02, J = A03, K = A06, L = A07,
        # N = A04, O = A05, Q/S/U/W/Y = AI1..AI5, Z = ATE
        r = current_row
        if typ == "S":
            totals_formula = f"=(F{r}/40*H{r})+(F{r}/40*K{r})+(F{r}/40*L{r})+N{r}+O{r}"
        else:
            totals_formula = (
                f"=(F{r}*H{r})+(F{r}*I{r})+(F{r}*J{r})+(F{r}*K{r})+(F{r}*L{r})+"
                f"Q{r}+S{r}+U{r}+W{r}+Y{r}+Z{r}"
            )
        ws.cell(row=current_row, column=COL["TOTALS"]).value = f"={totals_formula}"

        current_row += 1

    # ---------------------------- Totals row -------------------------------
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

# ================================  API  =====================================
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
        out_name = f"WBS_Payroll_{datetime.utcnow().date().isoformat()}.xlsx"
        return StreamingResponse(
            io.BytesIO(out_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{out_name}"'}
        )
    except ValueError as ve:
        import traceback, sys
        traceback.print_exc(file=sys.stderr)
        raise HTTPException(status_code=422, detail=str(ve))
    except Exception as e:
        import traceback, sys
        traceback.print_exc(file=sys.stderr)
        raise HTTPException(status_code=500, detail=f"backend processing failed: {e}")
