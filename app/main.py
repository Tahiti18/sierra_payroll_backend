# app/main.py
from __future__ import annotations

import io
import math
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import Response
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet

app = FastAPI(title="Sierra → WBS Converter", version="1.0")

# --- CORS (debug-friendly; tighten for prod if needed) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- CONFIG ----------
# Where the template lives (repo root) and gold-master files (app/data)
HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
TEMPLATE_PATH = REPO_ROOT / "wbs_template.xlsx"

DATA_DIR = HERE / "data"
GOLD_ORDER_PATH = DATA_DIR / "gold_master_order.txt"
GOLD_ROSTER_PATH = DATA_DIR / "gold_master_roster.csv"

# WBS layout (column indices begin at 1). Adjust if your template moves.
WBS_DATA_START_ROW = 8  # first employee data row in the template
# Logical “named columns” -> Excel column numbers for the Weekly sheet
COL = {
    "SSN": 1,              # A
    "EMPLOYEE": 2,         # B
    "STATUS": 3,           # C
    "TYPE": 4,             # D
    "PAY_RATE": 5,         # E
    "DEPT": 6,             # F
    "REG": 7,              # G  (A01)
    "OT": 8,               # H  (A02)
    "DT": 9,               # I  (A03)
    "VACATION": 10,        # J  (A06)
    "SICK": 11,            # K  (A07)
    "HOLIDAY": 12,         # L  (A08)
    "BONUS": 13,           # M  (A04)
    "COMMISSION": 14,      # N  (A05)
    # Piecework (Mon..Fri hours and totals) – use as available
    "PC_HRS_MON": 35,      # AI1 (example slot)
    "PC_TTL_MON": 36,      # AJ1
    "PC_HRS_TUE": 37,      # AK1
    "PC_TTL_TUE": 38,      # AL1
    "PC_HRS_WED": 39,      # AM1
    "PC_TTL_WED": 40,      # AN1
    "PC_HRS_THU": 41,      # AO1
    "PC_TTL_THU": 42,      # AP1
    "PC_HRS_FRI": 43,      # AQ1
    "PC_TTL_FRI": 44,      # AR1
    "TRAVEL": 45,          # AS
    "NOTES": 46,           # AT
    "TOTALS": 48,          # AV (pink totals at far right)
}

# Sierra file expected headers -> our canonical names
# If Jeff’s headers vary slightly, add alternates in the lists.
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
    "pc_hrs_mon": ["PC HRS MON", "AH1", "PC HRS MON (AH1)"],
    "pc_ttl_mon": ["PC TTL MON", "AI1"],
    "pc_hrs_tue": ["PC HRS TUE", "AJ2", "PC HRS TUE (AJ2)"],
    "pc_ttl_tue": ["PC TTL TUE", "AK2"],
    "pc_hrs_wed": ["PC HRS WED", "AI3", "PC HRS WED (AI3)"],
    "pc_ttl_wed": ["PC TTL WED", "AJ3"],
    "pc_hrs_thu": ["PC HRS THU", "AH4", "PC HRS THU (AH4)"],
    "pc_ttl_thu": ["PC TTL THU", "AI4"],
    "pc_hrs_fri": ["PC HRS FRI", "AH5", "PC HRS FRI (AH5)"],
    "pc_ttl_fri": ["PC TTL FRI", "AI5"],
    "travel": ["TRAVEL AMOUNT", "ATE", "Travel Amount"],
    "notes": ["Notes", "Comments", "Notes and Comments"],
}

# ---------- helpers ----------

def _normalize_name(name: str) -> str:
    """Normalize employee display names to a consistent 'Last, First' style if possible."""
    if not isinstance(name, str):
        return ""
    s = name.strip()
    # Already in "Last, First"
    if "," in s:
        return " ".join(part.strip() for part in s.split(",")).replace("  ", " ")
    # Try "First Last" -> "Last, First"
    parts = [p for p in s.split() if p]
    if len(parts) >= 2:
        first = " ".join(parts[:-1])
        last = parts[-1]
        return f"{last}, {first}"
    return s


def _pick_first_present(df: pd.DataFrame, candidates: List[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
        # tolerant check (sometimes Excel exports trail/lead spaces)
        for col in df.columns:
            if col.strip().lower() == c.strip().lower():
                return col
    return None


def _rename_headers(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {}
    for key, candidates in SIERRA_HEADER_MAP.items():
        col = _pick_first_present(df, candidates)
        if col is not None:
            mapping[col] = key
    return df.rename(columns=mapping)


def _load_gold_order() -> List[str]:
    if not GOLD_ORDER_PATH.exists():
        return []
    lines = [ln.strip() for ln in GOLD_ORDER_PATH.read_text(encoding="utf-8").splitlines()]
    return [_normalize_name(x) for x in lines if x.strip()]


def _load_gold_roster() -> pd.DataFrame:
    if not GOLD_ROSTER_PATH.exists():
        # Empty frame with expected columns
        return pd.DataFrame(columns=["employee", "ssn", "status", "type", "dept", "rate"])
    df = pd.read_csv(GOLD_ROSTER_PATH, dtype=str).fillna("")
    # normalize and coerce numerics
    df["employee"] = df["employee"].map(_normalize_name)
    for num in ("rate",):
        if num in df.columns:
            df[num] = pd.to_numeric(df[num], errors="coerce").fillna(0.0)
    return df


def _aggregate_sierra(df: pd.DataFrame) -> pd.DataFrame:
    """Rename headers, normalize names, coerce numerics, and aggregate multiple rows per employee."""
    df = _rename_headers(df).copy()

    # Must have at least employee column
    if "employee" not in df.columns:
        raise ValueError("Could not find an 'Employee' column in the uploaded Sierra file.")

    df["employee"] = df["employee"].map(_normalize_name)

    # Default numeric columns
    numeric_keys = [
        "reg", "ot", "dt", "vacation", "sick", "holiday",
        "bonus", "commission",
        "pc_hrs_mon", "pc_ttl_mon",
        "pc_hrs_tue", "pc_ttl_tue",
        "pc_hrs_wed", "pc_ttl_wed",
        "pc_hrs_thu", "pc_ttl_thu",
        "pc_hrs_fri", "pc_ttl_fri",
        "travel",
    ]
    for k in numeric_keys:
        if k not in df.columns:
            df[k] = 0
        df[k] = pd.to_numeric(df[k], errors="coerce").fillna(0.0)

    # Keep text fields if present
    if "notes" not in df.columns:
        df["notes"] = ""

    # Status/Type/Dept/Rate may exist in Sierra; keep but will be overridden by gold roster if present
    if "status" not in df.columns: df["status"] = ""
    if "type" not in df.columns: df["type"] = ""
    if "dept" not in df.columns: df["dept"] = ""
    if "rate" not in df.columns: df["rate"] = 0.0
    df["rate"] = pd.to_numeric(df["rate"], errors="coerce").fillna(0.0)

    # Aggregate duplicates by employee (sum numeric, keep first text)
    agg_map = {k: "sum" for k in numeric_keys + ["rate"]}
    agg_map.update({"status": "first", "type": "first", "dept": "first", "notes": "first"})
    grouped = df.groupby("employee", dropna=False, as_index=False).agg(agg_map)

    return grouped


def _enrich_with_roster(sierra: pd.DataFrame, roster: pd.DataFrame) -> pd.DataFrame:
    """Left-join Sierra employees to gold roster to get SSN/Status/Type/Dept/Rate overrides."""
    # roster columns: employee, ssn, status, type, dept, rate
    if not {"employee"}.issubset(set(roster.columns)):
        # If roster missing, just attach empty columns
        sierra["ssn"] = ""
        return sierra

    merged = sierra.merge(roster, on="employee", how="left", suffixes=("", "_roster"))

    # Prefer roster values where present
    def coalesce(a, b):
        return b if (b not in [None, "", 0, 0.0] and not (isinstance(b, float) and math.isnan(b))) else a

    out_rows = []
    for _, row in merged.iterrows():
        row = row.copy()
        row["ssn"] = row.get("ssn", "")
        for fld in ("status", "type", "dept"):
            row[fld] = coalesce(row.get(fld), row.get(f"{fld}_roster"))
        # pay rate: prefer roster rate if non-zero
        roster_rate = row.get("rate_roster", 0.0)
        row["rate"] = roster_rate if pd.notna(roster_rate) and float(roster_rate) > 0 else row.get("rate", 0.0)
        out_rows.append(row)
    out = pd.DataFrame(out_rows)

    # Clean temporary *_roster columns
    for c in list(out.columns):
        if c.endswith("_roster"):
            out.drop(columns=c, inplace=True, errors="ignore")
    return out


def _apply_gold_order(df: pd.DataFrame, order_list: List[str]) -> pd.DataFrame:
    if not order_list:
        # fallback: keep current order (which is by first appearance in Sierra)
        return df
    order_index = {name: i for i, name in enumerate(order_list)}
    df["_ord"] = df["employee"].map(lambda n: order_index.get(n, 10_000_000))
    df.sort_values(by=["_ord", "employee"], inplace=True)
    df.drop(columns=["_ord"], inplace=True)
    return df


def _safe_clear_data(ws: Worksheet, start_row: int, last_col: int) -> None:
    """Blank out existing data rows without touching merged cell definitions."""
    max_row = ws.max_row
    if max_row < start_row:
        return
    for r in range(start_row, max_row + 1):
        # If entire row is already blank up to last_col, skip
        try:
            row_empty = True
            for c in range(1, last_col + 1):
                cell = ws.cell(row=r, column=c)
                if cell.value not in (None, ""):
                    row_empty = False
                    break
            if row_empty:
                continue
            # Clear values only; avoid merged range write issues by try/except
            for c in range(1, last_col + 1):
                try:
                    ws.cell(row=r, column=c).value = None
                except AttributeError:
                    # merged 'value' is read-only — skip
                    continue
        except Exception:
            # Be defensive; continue clearing other rows
            continue


def _write_weekly(ws: Worksheet, df: pd.DataFrame) -> None:
    """Write the prepared frame to the Weekly sheet according to COL mapping."""
    current_row = WBS_DATA_START_ROW

    def w(row, key, col_key):
        val = row.get(key, "")
        try:
            ws.cell(row=current_row, column=COL[col_key]).value = val
        except AttributeError:
            # handle merged read-only cells gracefully
            pass

    for _, row in df.iterrows():
        # Basic identity columns
        w(row, "ssn", "SSN")
        w(row, "employee", "EMPLOYEE")
        w(row, "status", "STATUS")
        w(row, "type", "TYPE")
        w(row, "rate", "PAY_RATE")
        w(row, "dept", "DEPT")

        # Hours/quantities & amounts
        w(row, "reg", "REG")
        w(row, "ot", "OT")
        w(row, "dt", "DT")
        w(row, "vacation", "VACATION")
        w(row, "sick", "SICK")
        w(row, "holiday", "HOLIDAY")
        w(row, "bonus", "BONUS")
        w(row, "commission", "COMMISSION")

        # Piecework, travel, notes (if present)
        w(row, "pc_hrs_mon", "PC_HRS_MON"); w(row, "pc_ttl_mon", "PC_TTL_MON")
        w(row, "pc_hrs_tue", "PC_HRS_TUE"); w(row, "pc_ttl_tue", "PC_TTL_TUE")
        w(row, "pc_hrs_wed", "PC_HRS_WED"); w(row, "pc_ttl_wed", "PC_TTL_WED")
        w(row, "pc_hrs_thu", "PC_HRS_THU"); w(row, "pc_ttl_thu", "PC_TTL_THU")
        w(row, "pc_hrs_fri", "PC_HRS_FRI"); w(row, "pc_ttl_fri", "PC_TTL_FRI")
        w(row, "travel", "TRAVEL")
        w(row, "notes", "NOTES")

        # Totals (dollars) – pay rate driven
        rate = float(row.get("rate", 0.0) or 0.0)
        reg = float(row.get("reg", 0.0) or 0.0)
        ot = float(row.get("ot", 0.0) or 0.0)
        dt = float(row.get("dt", 0.0) or 0.0)
        vacation = float(row.get("vacation", 0.0) or 0.0)
        sick = float(row.get("sick", 0.0) or 0.0)
        holiday = float(row.get("holiday", 0.0) or 0.0)
        bonus = float(row.get("bonus", 0.0) or 0.0)
        commission = float(row.get("commission", 0.0) or 0.0)
        travel = float(row.get("travel", 0.0) or 0.0)

        total_amt = (reg * rate) + (ot * rate * 1.5) + (dt * rate * 2.0) \
                    + (vacation * rate) + (sick * rate) + (holiday * rate) \
                    + bonus + commission + travel

        try:
            ws.cell(row=current_row, column=COL["TOTALS"]).value = round(total_amt, 2)
        except AttributeError:
            pass

        current_row += 1


# ---------- API ----------

@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/process-payroll")
async def process_payroll(file: UploadFile = File(...)) -> Response:
    # Basic validations
    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=422, detail="Please upload an Excel file (.xlsx or .xls).")

    # Read Sierra file into DataFrame
    try:
        contents = await file.read()
        sierra_df = pd.read_excel(io.BytesIO(contents), engine="openpyxl")
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Could not read Excel: {e}")

    try:
        # Aggregate and normalize Sierra
        sierra_agg = _aggregate_sierra(sierra_df)

        # Enrich with gold roster
        roster_df = _load_gold_roster()
        enriched = _enrich_with_roster(sierra_agg, roster_df)

        # Apply gold order
        order_list = _load_gold_order()
        ordered = _apply_gold_order(enriched, order_list)

        # Load WBS template
        if not TEMPLATE_PATH.exists():
            raise HTTPException(status_code=500, detail=f"WBS template not found at {TEMPLATE_PATH}")
        wb = load_workbook(str(TEMPLATE_PATH))
        ws = wb.active  # Weekly sheet (single-sheet template)

        # Clear old rows safely (preserve styles & merged cells)
        _safe_clear_data(ws, WBS_DATA_START_ROW, COL["TOTALS"])

        # Write rows
        _write_weekly(ws, ordered)

        # Save to bytes
        out_stream = io.BytesIO()
        wb.save(out_stream)
        out_stream.seek(0)
    except HTTPException:
        raise
    except Exception as e:
        # Bubble a concise error to the UI with enough info
        raise HTTPException(status_code=500, detail=f"Backend processing failed: {e}")

    headers = {
        "Content-Disposition": f'attachment; filename="WBS_Payroll.xlsx"'
    }
    return Response(content=out_stream.read(),
                    media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    headers=headers)


# --- local run ---
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8080, reload=False)
