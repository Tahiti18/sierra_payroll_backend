# app/main.py
from __future__ import annotations

import io
import math
from pathlib import Path
from typing import Dict, List

import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import Response
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet

# ------------------------------------------------------------------------------
# App + CORS
# ------------------------------------------------------------------------------
app = FastAPI(title="Sierra → WBS Converter", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # tighten in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------------------------------------------------------------------
# Paths & constants
# ------------------------------------------------------------------------------
HERE = Path(__file__).resolve().parent          # .../app
REPO_ROOT = HERE.parent                         # repo root
TEMPLATE_PATH = REPO_ROOT / "wbs_template.xlsx" # existing template at repo root

DATA_DIR = HERE / "data"
GOLD_ORDER_PATH = DATA_DIR / "gold_master_order.txt"    # one name per line
GOLD_ROSTER_PATH = DATA_DIR / "gold_master_roster.csv"  # employee,ssn,status,type,dept,rate

WBS_DATA_START_ROW = 8  # first data row in the template sheet

# WBS sheet columns (1-based). Adjust if your template changes.
COL = {
    "SSN": 1,              # A
    "EMPLOYEE": 2,         # B (Employee Name)
    "STATUS": 3,           # C
    "TYPE": 4,             # D
    "PAY_RATE": 5,         # E
    "DEPT": 6,             # F
    "REG": 7,              # G  A01
    "OT": 8,               # H  A02
    "DT": 9,               # I  A03
    "VACATION": 10,        # J  A06
    "SICK": 11,            # K  A07
    "HOLIDAY": 12,         # L  A08
    "BONUS": 13,           # M  A04
    "COMMISSION": 14,      # N  A05
    # piecework / travel / notes if your template uses them (safe to write even if hidden)
    "TRAVEL": 45,          # AS
    "NOTES": 46,           # AT
    "TOTALS": 48,          # AV (pink totals at far right)
}

# Sierra header variants → canonical keys (extend if Jeff changes headers)
SIERRA_HEADER_MAP: Dict[str, List[str]] = {
    "employee": ["Employee", "Employee Name", "Name"],
    "status": ["Status"],
    "type": ["Type", "Pay Type"],
    "dept": ["Dept", "Department"],
    "rate": ["Rate", "Pay Rate", "Pay Rate Dept", "Pay Rate Dept "],
    "reg": ["REG", "REGULAR", "A01", "Regular (A01)"],
    "ot": ["OT", "OVERTIME", "A02", "Overtime (A02)"],
    "dt": ["DT", "DOUBLETIME", "A03", "Doubletime (A03)"],
    "vacation": ["VACATION", "A06"],
    "sick": ["SICK", "A07"],
    "holiday": ["HOLIDAY", "A08"],
    "bonus": ["BONUS", "A04"],
    "commission": ["COMMISSION", "A05"],
    "travel": ["TRAVEL AMOUNT", "Travel", "Travel Amount"],
    "notes": ["Notes", "Comments", "Notes and Comments"],
}

NUMERIC_KEYS = [
    "reg", "ot", "dt", "vacation", "sick", "holiday",
    "bonus", "commission", "travel"
]

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
def _norm_name(name: str) -> str:
    """Normalize display names; if already 'Last, First' keep it; try 'First Last' -> 'Last, First'."""
    if not isinstance(name, str):
        return ""
    s = " ".join(name.split()).strip()
    if not s:
        return s
    if "," in s:
        # already 'Last, First'
        parts = [p.strip() for p in s.split(",")]
        s2 = ", ".join(p for p in parts if p)
        return " ".join(s2.split())
    parts = s.split()
    if len(parts) >= 2:
        first = " ".join(parts[:-1])
        last = parts[-1]
        return f"{last}, {first}"
    return s

def _pick_first_present(df: pd.DataFrame, candidates: List[str]) -> str | None:
    # exact or case/space-insensitive
    cols_lower = {c.lower().strip(): c for c in df.columns}
    for wanted in candidates:
        w = wanted.lower().strip()
        if wanted in df.columns:
            return wanted
        if w in cols_lower:
            return cols_lower[w]
    # relaxed contains
    for wanted in candidates:
        w = wanted.lower().strip()
        for c in df.columns:
            if w in c.lower().strip():
                return c
    return None

def _rename_headers(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {}
    for key, cands in SIERRA_HEADER_MAP.items():
        col = _pick_first_present(df, cands)
        if col:
            mapping[col] = key
    return df.rename(columns=mapping)

def _load_gold_order() -> List[str]:
    if not GOLD_ORDER_PATH.exists():
        return []
    lines = GOLD_ORDER_PATH.read_text(encoding="utf-8").splitlines()
    return [_norm_name(x) for x in lines if str(x).strip()]

def _load_gold_roster() -> pd.DataFrame:
    if not GOLD_ROSTER_PATH.exists():
        return pd.DataFrame(columns=["employee","ssn","status","type","dept","rate"])
    df = pd.read_csv(GOLD_ROSTER_PATH, dtype=str).fillna("")
    # expected headers: Employee Name, SSN, Status, Type, Department, Pay Rate
    # normalize to internal names
    rename_map = {}
    for col in df.columns:
        cl = col.lower().strip()
        if cl == "employee name" or cl == "employee":
            rename_map[col] = "employee"
        elif cl == "ssn":
            rename_map[col] = "ssn"
        elif cl == "status":
            rename_map[col] = "status"
        elif cl in ("type", "pay type"):
            rename_map[col] = "type"
        elif cl in ("dept", "department"):
            rename_map[col] = "dept"
        elif cl in ("pay rate", "rate"):
            rename_map[col] = "rate"
    df = df.rename(columns=rename_map)

    if "employee" not in df.columns:
        df["employee"] = ""
    df["employee"] = df["employee"].map(_norm_name)

    if "rate" in df.columns:
        df["rate"] = pd.to_numeric(df["rate"], errors="coerce").fillna(0.0)
    else:
        df["rate"] = 0.0

    for k in ("ssn","status","type","dept"):
        if k not in df.columns:
            df[k] = ""

    return df[["employee","ssn","status","type","dept","rate"]]

def _read_sierra_excel(payload: bytes) -> pd.DataFrame:
    # try default sheet first; if it looks wrong/empty, fallback to first non-empty sheet
    xl = pd.ExcelFile(io.BytesIO(payload))
    sheet_name = xl.sheet_names[0]
    df = xl.parse(sheet_name)
    if df.empty and len(xl.sheet_names) > 1:
        for s in xl.sheet_names:
            tmp = xl.parse(s)
            if not tmp.empty:
                df = tmp
                break
    return df

def _aggregate_sierra(df: pd.DataFrame) -> pd.DataFrame:
    df = _rename_headers(df).copy()
    if "employee" not in df.columns:
        raise ValueError("Missing 'Employee' / 'Employee Name' column in Sierra file.")

    # normalize names
    df["employee"] = df["employee"].map(_norm_name)

    # force numerics
    for k in NUMERIC_KEYS + ["rate"]:
        if k not in df.columns:
            df[k] = 0
        df[k] = pd.to_numeric(df[k], errors="coerce").fillna(0.0)

    # optional text
    if "notes" not in df.columns:
        df["notes"] = ""

    for k in ("status","type","dept"):
        if k not in df.columns:
            df[k] = ""

    # sum duplicates (numbers), keep first text/rate (rate will be overridden by roster if provided)
    agg_map = {k: "sum" for k in NUMERIC_KEYS}
    agg_map.update({"rate": "first", "status": "first", "type": "first", "dept": "first", "notes": "first"})
    grouped = df.groupby("employee", as_index=False, dropna=False).agg(agg_map)
    return grouped

def _enrich_with_roster(sierra: pd.DataFrame, roster: pd.DataFrame) -> pd.DataFrame:
    if roster.empty:
        sierra["ssn"] = ""
        return sierra

    merged = sierra.merge(roster, on="employee", how="left", suffixes=("", "_roster"))

    def coalesce(a, b):
        if b in (None, "", 0, 0.0):
            return a
        if isinstance(b, float) and math.isnan(b):
            return a
        return b

    rows = []
    for _, r in merged.iterrows():
        out = r.copy()
        out["ssn"] = out.get("ssn", "")
        # prefer roster values if present
        for k in ("status","type","dept"):
            out[k] = coalesce(out.get(k), out.get(f"{k}_roster"))
        rr = out.get("rate_roster", 0.0)
        out["rate"] = float(rr) if pd.notna(rr) and float(rr) > 0 else float(out.get("rate", 0.0))
        rows.append(out)

    out_df = pd.DataFrame(rows)
    # drop *_roster columns
    for c in list(out_df.columns):
        if c.endswith("_roster"):
            out_df.drop(columns=c, inplace=True, errors="ignore")
    return out_df

def _apply_gold_order(df: pd.DataFrame, order_list: List[str]) -> pd.DataFrame:
    if not order_list:
        return df
    idx = {name: i for i, name in enumerate(order_list)}
    df["_o"] = df["employee"].map(lambda n: idx.get(n, 10_000_000))
    df = df.sort_values(["_o", "employee"]).drop(columns="_o")
    return df

def _safe_clear_data(ws: Worksheet, start_row: int, last_col: int) -> None:
    max_row = ws.max_row
    if max_row < start_row:
        return
    for r in range(start_row, max_row + 1):
        try:
            # quick empty check
            empty = True
            for c in range(1, last_col + 1):
                v = ws.cell(row=r, column=c).value
                if v not in (None, ""):
                    empty = False
                    break
            if empty:
                continue
            # clear values only (avoid merged write error)
            for c in range(1, last_col + 1):
                try:
                    ws.cell(row=r, column=c).value = None
                except AttributeError:
                    continue
        except Exception:
            continue

def _write_weekly(ws: Worksheet, df: pd.DataFrame) -> None:
    row_idx = WBS_DATA_START_ROW

    def w(col_key: str, value):
        try:
            ws.cell(row=row_idx, column=COL[col_key]).value = value
        except AttributeError:
            pass

    for _, r in df.iterrows():
        # identity
        w("SSN", r.get("ssn", ""))
        w("EMPLOYEE", r.get("employee", ""))
        w("STATUS", r.get("status", ""))
        w("TYPE", r.get("type", ""))
        w("PAY_RATE", round(float(r.get("rate", 0.0) or 0.0), 2))
        w("DEPT", r.get("dept", ""))

        # hours/amount buckets
        for k, ck in [("reg","REG"),("ot","OT"),("dt","DT"),
                      ("vacation","VACATION"),("sick","SICK"),("holiday","HOLIDAY"),
                      ("bonus","BONUS"),("commission","COMMISSION"),
                      ("travel","TRAVEL")]:
            v = float(r.get(k, 0.0) or 0.0)
            w(ck, round(v, 2))

        # total dollars (pink)
        rate = float(r.get("rate", 0.0) or 0.0)
        reg = float(r.get("reg", 0.0) or 0.0)
        ot = float(r.get("ot", 0.0) or 0.0)
        dt = float(r.get("dt", 0.0) or 0.0)
        vacation = float(r.get("vacation", 0.0) or 0.0)
        sick = float(r.get("sick", 0.0) or 0.0)
        holiday = float(r.get("holiday", 0.0) or 0.0)
        bonus = float(r.get("bonus", 0.0) or 0.0)
        commission = float(r.get("commission", 0.0) or 0.0)
        travel = float(r.get("travel", 0.0) or 0.0)

        total_amt = (
            (reg * rate) + (ot * rate * 1.5) + (dt * rate * 2.0)
            + (vacation * rate) + (sick * rate) + (holiday * rate)
            + bonus + commission + travel
        )
        w("TOTALS", round(total_amt, 2))

        row_idx += 1

# ------------------------------------------------------------------------------
# API
# ------------------------------------------------------------------------------
@app.get("/health")
def health():
    return {"ok": True}

@app.post("/process-payroll")
async def process_payroll(file: UploadFile = File(...)) -> Response:
    if not file or not file.filename:
        raise HTTPException(status_code=400, detail="No file uploaded.")
    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=422, detail="Upload an Excel file (.xlsx/.xls).")

    try:
        payload = await file.read()
        sierra_raw = _read_sierra_excel(payload)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Unable to read Excel: {e}")

    try:
        sierra = _aggregate_sierra(sierra_raw)
        roster = _load_gold_roster()
        enriched = _enrich_with_roster(sierra, roster)
        order_list = _load_gold_order()
        final_df = _apply_gold_order(enriched, order_list)

        if not TEMPLATE_PATH.exists():
            raise HTTPException(status_code=500, detail=f"Template missing at {TEMPLATE_PATH}")

        wb = load_workbook(str(TEMPLATE_PATH))
        ws = wb.active  # Weekly sheet

        _safe_clear_data(ws, WBS_DATA_START_ROW, COL["TOTALS"])
        _write_weekly(ws, final_df)

        out = io.BytesIO()
        wb.save(out)
        out.seek(0)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Backend processing failed: {e}")

    headers = {"Content-Disposition": 'attachment; filename="WBS_Payroll.xlsx"'}
    return Response(
        content=out.read(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )

# ------------------------------------------------------------------------------
# Local dev
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8080, reload=False)
