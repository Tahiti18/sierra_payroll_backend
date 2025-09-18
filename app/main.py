# app/main.py
from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet

app = FastAPI(title="Sierra Payroll → WBS")

# ---------- Repo paths ----------
HERE = Path(__file__).resolve().parent
REPO = HERE.parent
ROSTER_XLSX = REPO / "roster.xlsx"
WBS_TEMPLATE = REPO / "wbs_template.xlsx"
CANON_ORDER_JSON = REPO / "employee_order_2025-09-19.json"  # persisted stable row order (SSNs)


# ---------- Column map / constants ----------
# WBS column indexes (1-based) – adjust if your template differs.
COL = {
    "SSN": 2,
    "EMP_NAME": 3,
    "STATUS": 4,
    "PAY_TYPE": 5,
    "PAY_RATE": 6,
    "DEPT": 7,
    # Earnings buckets (A01..A08 etc.)
    "REG": 8,      # A01
    "OT": 9,       # A02
    "DT": 10,      # A03
    "VAC": 11,     # A06
    "SICK": 12,    # A07
    "HOL": 13,     # A08
    "BONUS": 14,   # A04
    "COMM": 15,    # A05
    # Day columns: PC HRS MON/TUE/... and PC TTL MON/TUE/... may vary per template.
    # If your template has only Friday piece columns, you can ignore the others.
    # Totals column at far right (pink)
    "TOTALS": 28,  # <— set to the actual index of the pink Totals column in your template
}

# First data row in WBS template (header rows above are report meta)
WBS_DATA_START_ROW = 8


# ---------- Helpers ----------
def _is_merged_anchor(ws: Worksheet, row: int, col: int) -> bool:
    """
    Return True if (row, col) is the top-left anchor cell of a merged range.
    We skip writing to anchors to avoid openpyxl 'MergedCell' read-only issues.
    """
    for mr in ws.merged_cells.ranges:
        minr, minc, maxr, maxc = mr.min_row, mr.min_col, mr.max_row, mr.max_col
        if row == minr and col == minc:
            return True
    return False


def _safe_write(ws: Worksheet, row: int, col: int, value):
    """Write value to (row, col) unless it's a merged anchor cell (skip to preserve styles)."""
    try:
        if _is_merged_anchor(ws, row, col):
            return
        ws.cell(row=row, column=col).value = value
    except Exception:
        # As a fallback (very rare), just skip the write. We never want a 500 here.
        pass


def _load_roster() -> pd.DataFrame:
    if not ROSTER_XLSX.exists():
        raise HTTPException(status_code=500, detail=f"Roster file missing at {ROSTER_XLSX}")
    df = pd.read_excel(ROSTER_XLSX)
    # Normalize
    for c in df.columns:
        df.rename(columns={c: str(c).strip().lower()}, inplace=True)
    # Expected roster columns (case-insensitive): name, ssn, dept, rate, status, type
    # Try to map flexibly
    colmap = {}
    for want, options in {
        "name": ["employee", "employee name", "name"],
        "ssn": ["ssn", "social", "social security"],
        "dept": ["dept", "department"],
        "rate": ["rate", "pay rate", "pay_rate"],
        "status": ["status"],
        "type": ["type", "pay type", "pay_type"],
    }.items():
        for o in options:
            if o in df.columns:
                colmap[want] = o
                break
    for k in ["name", "ssn"]:
        if k not in colmap:
            raise HTTPException(status_code=500, detail=f"Roster is missing '{k}' column")
    # Clean SSN
    df["ssn_norm"] = (
        df[colmap["ssn"]]
        .astype(str)
        .str.replace(r"[^0-9]", "", regex=True)
        .str.zfill(9)
    )
    df["name_norm"] = df[colmap["name"]].astype(str).str.strip()
    # Optional fields
    df["dept_out"] = df[colmap["dept"]] if "dept" in colmap else ""
    df["rate_out"] = pd.to_numeric(df[colmap["rate"]], errors="coerce") if "rate" in colmap else 0.0
    df["status_out"] = df[colmap["status"]].astype(str).str.strip() if "status" in colmap else "A"
    df["type_out"] = df[colmap["type"]].astype(str).str.strip().str[:1].str.upper() if "type" in colmap else "H"
    return df[["ssn_norm", "name_norm", "dept_out", "rate_out", "status_out", "type_out"]]


def _canonical_order(ssn_list: List[str]) -> List[str]:
    """
    Keep a persistent, week-to-week stable order of employees by SSN.
    If json exists, use it. If not, save current ssn_list as the canonical order.
    """
    if CANON_ORDER_JSON.exists():
        try:
            saved = json.loads(CANON_ORDER_JSON.read_text())
            saved = [s for s in saved if s in ssn_list]
            # include any new people at the end
            new_ssn = [s for s in ssn_list if s not in saved]
            return saved + new_ssn
        except Exception:
            pass
    # Save the first seen order
    try:
        CANON_ORDER_JSON.write_text(json.dumps(ssn_list, indent=2))
    except Exception:
        pass
    return ssn_list


def _parse_sierra(contents: bytes) -> pd.DataFrame:
    """
    Read Sierra payroll XLSX and return a *normalized* dataframe with one row per time-entry,
    then aggregate it by SSN.
    Expected to find columns for at least: Employee Name, Dept, Pay Rate, REG, OT, DT, VAC, SICK, HOL,
    and possibly day-wise piece/hour columns.
    """
    with io.BytesIO(contents) as bio:
        raw = pd.read_excel(bio)

    # Normalize columns
    cols = {c: str(c).strip().lower() for c in raw.columns}
    raw.rename(columns=cols, inplace=True)

    # Flexible lookup for major buckets
    def pick(*cands) -> Optional[str]:
        for c in cands:
            if c in raw.columns:
                return c
        return None

    name_col = pick("employee", "employee name", "name")
    dept_col = pick("department", "dept")
    rate_col = pick("pay rate", "rate", "pay_rate")
    reg_col = pick("regular", "reg", "a01")
    ot_col = pick("overtime", "ot", "a02")
    dt_col = pick("doubletime", "dt", "a03")
    vac_col = pick("vacation", "vac", "a06")
    sick_col = pick("sick", "a07")
    hol_col = pick("holiday", "hol", "a08")
    bonus_col = pick("bonus", "a04")
    comm_col = pick("commission", "a05")

    for need in [name_col, reg_col]:
        if need is None:
            raise HTTPException(status_code=422, detail="Sierra file is missing required columns")

    # Clean numeric
    num_cols = [c for c in [reg_col, ot_col, dt_col, vac_col, sick_col, hol_col, bonus_col, comm_col] if c]
    for c in num_cols:
        raw[c] = pd.to_numeric(raw[c], errors="coerce").fillna(0.0)

    # Basic normalized dataframe
    df = pd.DataFrame({
        "name_norm": raw[name_col].astype(str).str.strip(),
        "dept_in": raw[dept_col] if dept_col else "",
        "rate_in": pd.to_numeric(raw[rate_col], errors="coerce") if rate_col else 0.0,
        "REG": raw[reg_col] if reg_col else 0.0,
        "OT": raw[ot_col] if ot_col else 0.0,
        "DT": raw[dt_col] if dt_col else 0.0,
        "VAC": raw[vac_col] if vac_col else 0.0,
        "SICK": raw[sick_col] if sick_col else 0.0,
        "HOL": raw[hol_col] if hol_col else 0.0,
        "BONUS": raw[bonus_col] if bonus_col else 0.0,
        "COMM": raw[comm_col] if comm_col else 0.0,
    })

    # Return raw entries (we'll attach SSN from roster and then aggregate)
    return df


def _aggregate_by_ssn(df: pd.DataFrame, roster: pd.DataFrame) -> pd.DataFrame:
    """
    Attach SSN from roster by name, then aggregate sums by SSN.
    If multiple roster rows map to same name, prefer exact match; otherwise first seen.
    """
    # Join on name (left join to keep payroll rows)
    merged = df.merge(
        roster[["ssn_norm", "name_norm", "dept_out", "rate_out", "status_out", "type_out"]],
        how="left",
        on="name_norm",
        suffixes=("", "_r"),
    )

    # If no SSN found for some names, keep them but mark ssn_norm as '' so we can see the problem.
    merged["ssn_norm"] = merged["ssn_norm"].fillna("")

    # Prefer roster dept/rate/type/status when present
    merged["DEPT"] = merged["dept_out"].where(merged["dept_out"].notna() & (merged["dept_out"] != ""), merged["dept_in"])
    merged["RATE"] = merged["rate_out"].fillna(merged["rate_in"]).fillna(0.0)
    merged["STATUS"] = merged["status_out"].fillna("A").str[:1].str.upper()
    merged["TYPE"] = merged["type_out"].fillna("H").str[:1].str.upper()

    # Aggregate by SSN (unknown SSN rows get grouped under empty string; we’ll place them at the end)
    sums = merged.groupby("ssn_norm", dropna=False)[["REG", "OT", "DT", "VAC", "SICK", "HOL", "BONUS", "COMM"]].sum().reset_index()

    # Pick representative name/dept/rate/type/status per SSN (first non-null)
    firsts = (
        merged.sort_values(["ssn_norm", "name_norm"])
        .groupby("ssn_norm", dropna=False)
        .agg({
            "name_norm": "first",
            "DEPT": "first",
            "RATE": "first",
            "STATUS": "first",
            "TYPE": "first",
        })
        .reset_index()
    )

    agg = firsts.merge(sums, on="ssn_norm", how="left")
    return agg


def _order_rows(agg: pd.DataFrame) -> pd.DataFrame:
    ssn_list = agg["ssn_norm"].tolist()
    order = _canonical_order(ssn_list)
    # Unknown SSN ('') always go last
    order_no_empty = [s for s in order if s]
    empty_present = any(s == "" for s in ssn_list)

    sorter = {s: i for i, s in enumerate(order_no_empty)}
    big = 10_000
    agg["__key__"] = agg["ssn_norm"].apply(lambda s: sorter.get(s, big))
    agg = agg.sort_values(["__key__", "name_norm"]).drop(columns="__key__")
    if empty_present:
        # move empty-SSN rows to very end
        unknown = agg[agg["ssn_norm"] == ""]
        known = agg[agg["ssn_norm"] != ""]
        agg = pd.concat([known, unknown], ignore_index=True)
    return agg


def _write_wbs(agg: pd.DataFrame) -> bytes:
    if not WBS_TEMPLATE.exists():
        raise HTTPException(status_code=500, detail=f"WBS template not found at {WBS_TEMPLATE}")

    wb = load_workbook(str(WBS_TEMPLATE))
    ws = wb.active

    # Clear previous data rows (values only, keep styles)
    max_row = ws.max_row
    if max_row >= WBS_DATA_START_ROW:
        for r in range(WBS_DATA_START_ROW, max_row + 1):
            # If the row already looks empty, skip clearing
            row_is_empty = True
            for c in range(1, COL["TOTALS"] + 1):
                v = ws.cell(row=r, column=c).value
                if v not in (None, ""):
                    row_is_empty = False
                    break
            if row_is_empty:
                continue
            for c in range(1, COL["TOTALS"] + 1):
                _safe_write(ws, r, c, None)

    # Write rows
    r = WBS_DATA_START_ROW
    for _, row in agg.iterrows():
        _safe_write(ws, r, COL["SSN"], row["ssn_norm"] if row["ssn_norm"] else "")          # SSN column
        _safe_write(ws, r, COL["EMP_NAME"], row["name_norm"])                               # Employee Name
        _safe_write(ws, r, COL["STATUS"], row["STATUS"])                                    # Status (A/I)
        _safe_write(ws, r, COL["PAY_TYPE"], row["TYPE"])                                    # Pay Type (H/S)
        _safe_write(ws, r, COL["PAY_RATE"], float(row["RATE"] or 0.0))                      # Pay Rate
        _safe_write(ws, r, COL["DEPT"], row["DEPT"] if isinstance(row["DEPT"], str) else "")# Dept

        # Earnings buckets
        _safe_write(ws, r, COL["REG"], float(row["REG"]))
        _safe_write(ws, r, COL["OT"], float(row["OT"]))
        _safe_write(ws, r, COL["DT"], float(row["DT"]))
        _safe_write(ws, r, COL["VAC"], float(row["VAC"]))
        _safe_write(ws, r, COL["SICK"], float(row["SICK"]))
        _safe_write(ws, r, COL["HOL"], float(row["HOL"]))
        _safe_write(ws, r, COL["BONUS"], float(row["BONUS"]))
        _safe_write(ws, r, COL["COMM"], float(row["COMM"]))

        # Totals (pink): sum of all money columns in this row
        total = (
            float(row["REG"]) + float(row["OT"]) + float(row["DT"]) +
            float(row["VAC"]) + float(row["SICK"]) + float(row["HOL"]) +
            float(row["BONUS"]) + float(row["COMM"])
        )
        _safe_write(ws, r, COL["TOTALS"], total)

        r += 1

    # Return workbook bytes
    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    return out.read()


# ---------- API ----------
@app.get("/health")
def health():
    return {"ok": True}


@app.post("/process-payroll")
def process_payroll(file: UploadFile = File(...)):
    try:
        contents = file.file.read()
    except Exception:
        raise HTTPException(status_code=400, detail="Could not read uploaded file")

    if not contents:
        raise HTTPException(status_code=400, detail="Empty upload")

    # Parse Sierra → normalized entries
    raw_df = _parse_sierra(contents)

    # Load roster and attach SSN/rate/dept/status/type
    roster = _load_roster()

    # Aggregate by SSN and enforce canonical order
    agg = _aggregate_by_ssn(raw_df, roster)
    agg = _order_rows(agg)

    # Write WBS file
    xlsx_bytes = _write_wbs(agg)

    filename = "WBS_Payroll.xlsx"
    return StreamingResponse(
        io.BytesIO(xlsx_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
