# app/converter.py
import io
from datetime import datetime, date
from typing import Dict, List, Optional

import pandas as pd
from openpyxl import Workbook
from openpyxl.utils import get_column_letter


# ----------------------------- small helpers -----------------------------
ALLOWED_EXTS = (".xlsx", ".xls")


def _std_col(s: str) -> str:
    return (s or "").strip().lower().replace("\n", " ").replace("\r", " ")


def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = { _std_col(c): c for c in df.columns }
    for want in candidates:
        key = _std_col(want)
        if key in cols:
            return cols[key]
    # relaxed contains (headers only)
    for want in candidates:
        key = _std_col(want)
        for k, v in cols.items():
            if key in k:
                return v
    return None


def _require_columns(df: pd.DataFrame, mapping: Dict[str, List[str]]) -> Dict[str, str]:
    resolved, missing = {}, []
    for logical, options in mapping.items():
        col = _find_col(df, options)
        if not col:
            missing.append(f"{logical} (any of: {', '.join(options)})")
        else:
            resolved[logical] = col
    if missing:
        raise ValueError("Missing required columns: " + "; ".join(missing))
    return resolved


def _normalize_name(raw: str) -> str:
    if not raw or not isinstance(raw, str):
        return ""
    name = raw.strip()
    parts = [p for p in name.split() if p]
    if len(parts) == 2:
        # "First Last" -> "Last, First"
        return f"{parts[1]}, {parts[0]}"
    return name


def _to_date(val) -> Optional[date]:
    if pd.isna(val):
        return None
    if isinstance(val, datetime):
        return val.date()
    try:
        return pd.to_datetime(val).date()
    except Exception:
        return None


def _apply_ca_daily_ot(day_hours: float) -> Dict[str, float]:
    """
    California daily OT:
      - first 8: REG
      - next 4 (8–12): OT
      - >12: DT
    """
    h = float(day_hours or 0.0)
    reg = min(h, 8.0)
    ot = 0.0
    dt = 0.0
    if h > 8:
        ot = min(h - 8.0, 4.0)
    if h > 12:
        dt = h - 12.0
    return {"REG": reg, "OT": ot, "DT": dt}


def _money(x: float) -> float:
    # keep exact math; only round at Excel write
    return float(x or 0.0)


# ----------------------------- core converter -----------------------------
def convert_sierra_to_wbs(input_bytes: bytes, sheet_name: Optional[str] = None) -> bytes:
    """
    Reads a Sierra timesheet (Excel), applies CA daily overtime split,
    aggregates to weekly per employee, and writes a fresh WBS-style workbook.

    We intentionally build the workbook from scratch (no template) to avoid
    writing into merged cells.
    """
    excel = pd.ExcelFile(io.BytesIO(input_bytes))
    target_sheet = sheet_name or excel.sheet_names[0]
    df = excel.parse(target_sheet)

    if df.empty:
        raise ValueError("Input sheet is empty.")

    required = {
        "employee": ["employee", "employee name", "name", "worker", "employee_name"],
        "date": ["date", "work date", "day", "worked date"],
        "hours": ["hours", "hrs", "total hours", "work hours"],
        "rate": ["rate", "pay rate", "hourly rate", "wage"],
    }
    optional = {
        "department": ["department", "dept", "division"],
        "ssn": ["ssn", "social", "social security", "social security number"],
        "wtype": ["type", "employee type", "emp type", "pay type"],
        "task": ["task", "earn type", "earning", "code"],
    }

    resolved_req = _require_columns(df, required)
    resolved_opt = {k: _find_col(df, v) for k, v in optional.items()}

    core = df[[resolved_req["employee"], resolved_req["date"], resolved_req["hours"], resolved_req["rate"]]].copy()
    core.columns = ["employee", "date", "hours", "rate"]

    # Attach optionals if present
    core["department"] = df[resolved_opt["department"]] if resolved_opt["department"] else ""
    core["ssn"]        = df[resolved_opt["ssn"]] if resolved_opt["ssn"] else ""
    core["wtype"]      = df[resolved_opt["wtype"]] if resolved_opt["wtype"] else ""
    core["task"]       = df[resolved_opt["task"]] if resolved_opt["task"] else ""

    # Normalize types
    core["employee"] = core["employee"].astype(str).map(_normalize_name)
    core["date"]     = core["date"].map(_to_date)
    core["hours"]    = pd.to_numeric(core["hours"], errors="coerce").fillna(0.0).astype(float)
    core["rate"]     = pd.to_numeric(core["rate"], errors="coerce").fillna(0.0).astype(float)

    # Keep only valid rows
    core = core[(core["employee"].str.len() > 0) & core["date"].notna() & (core["hours"] > 0)]

    if core.empty:
        # Build an empty but valid workbook
        wb = Workbook()
        ws = wb.active
        ws.title = "WEEKLY"
        header1 = ["", "", "WEEKLY PAYROLL", "", "", "", "", "", "", "", "", "", ""]
        header2 = ["Status", "Type", "Employee", "SSN", "Department", "Pay Rate",
                   "REG (A01)", "OT (A02)", "DT (A03)", "REG $", "OT $", "DT $", "TOTAL $"]
        ws.append(header1)
        ws.append(header2)
        bio = io.BytesIO()
        wb.save(bio)
        bio.seek(0)
        return bio.read()

    # Sum per employee/day, then apply CA daily OT split
    day_group = core.groupby(["employee", "date", "rate"], dropna=False)["hours"].sum().reset_index()

    split_rows = []
    for _, row in day_group.iterrows():
        dist = _apply_ca_daily_ot(float(row["hours"]))
        split_rows.append({
            "employee": row["employee"],
            "date": row["date"],
            "rate": float(row["rate"]),
            "REG": dist["REG"],
            "OT": dist["OT"],
            "DT": dist["DT"],
        })
    split_df = pd.DataFrame(split_rows)

    # Weekly totals by employee (and rate)
    weekly = split_df.groupby(["employee", "rate"], dropna=False)[["REG", "OT", "DT"]].sum().reset_index()

    # Dollars
    weekly["REG_$"]   = weekly["REG"] * weekly["rate"]
    weekly["OT_$"]    = weekly["OT"]  * weekly["rate"] * 1.5
    weekly["DT_$"]    = weekly["DT"]  * weekly["rate"] * 2.0
    weekly["TOTAL_$"] = weekly["REG_$"] + weekly["OT_$"] + weekly["DT_$"]

    # Identity columns (first seen)
    id_map = (
        core.groupby("employee")
            .agg({"department": "first", "ssn": "first", "wtype": "first"})
            .reset_index()
    )
    out = pd.merge(weekly, id_map, on="employee", how="left")

    # WBS identity defaults
    out["Status"] = "A"
    out["Type"] = out["wtype"].astype(str).str.upper().map(lambda x: "S" if x.startswith("S") else "H")

    # Final WBS column order
    wbs_cols = [
        "Status",            # A
        "Type",              # H/S
        "employee",          # Last, First
        "ssn",
        "department",
        "rate",
        "REG",
        "OT",
        "DT",
        "REG_$",
        "OT_$",
        "DT_$",
        "TOTAL_$",
    ]
    out = out[wbs_cols].copy()

    # ----------------------------- build Excel -----------------------------
    wb = Workbook()
    ws = wb.active
    ws.title = "WEEKLY"

    header1 = ["", "", "WEEKLY PAYROLL", "", "", "", "", "", "", "", "", "", ""]
    header2 = ["Status", "Type", "Employee", "SSN", "Department", "Pay Rate",
               "REG (A01)", "OT (A02)", "DT (A03)", "REG $", "OT $", "DT $", "TOTAL $"]
    ws.append(header1)
    ws.append(header2)

    for _, r in out.iterrows():
        ws.append([
            r["Status"],
            r["Type"],
            r["employee"],
            r["ssn"],
            r["department"],
            round(_money(r["rate"]), 2),
            round(_money(r["REG"]), 2),
            round(_money(r["OT"]), 2),
            round(_money(r["DT"]), 2),
            round(_money(r["REG_$"]), 2),
            round(_money(r["OT_$"]), 2),
            round(_money(r["DT_$"]), 2),
            round(_money(r["TOTAL_$"]), 2),
        ])

    # Autosize columns
    for col_idx in range(1, 14):
        col = get_column_letter(col_idx)
        max_len = 12
        for cell in ws[col]:
            if cell.value is None:
                continue
            max_len = max(max_len, len(str(cell.value)))
        ws.column_dimensions[col].width = min(max_len + 2, 30)

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.read()
