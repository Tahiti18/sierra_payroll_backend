# app/main.py
from __future__ import annotations

import io
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from openpyxl import load_workbook
from openpyxl.cell.cell import Cell
from openpyxl.worksheet.worksheet import Worksheet

app = FastAPI(title="Sierra → WBS Converter")

# ---------- Repo paths ----------
HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent  # template + roster live in repo root
TEMPLATE_PATH = REPO_ROOT / "wbs_template.xlsx"
ROSTER_XLSX = REPO_ROOT / "roster.xlsx"
ROSTER_CSV = REPO_ROOT / "roster.csv"

# ---------- Template anchors ----------
# We will discover column letters by reading headers in the first 15 rows.
TEMPLATE_HEADERS = [
    "Status", "Type", "Employee", "SSN", "Department", "Pay", "Pay Rate",
    "REG", "OVERTIME", "DOUBLETIME", "VACATION", "SICK", "HOLIDAY",
    "BONUS", "COMMISSION",
    "PC HRS MON", "PC TTL MON",
    "PC HRS TUE", "PC TTL TUE",
    "PC HRS WED", "PC TTL WED",
    "PC HRS THU", "PC TTL THU",
    "PC HRS FRI", "PC TTL FRI",
    "TRAVEL AMOUNT", "Notes", "Comments", "Totals", "TOTALS"
]
# First row of data in your WBS (names begin visually around row 9 in your screenshots)
WBS_DATA_START_ROW = 9

# ---------- Sierra → WBS canonical field names ----------
# We normalize Sierra columns to these keys, then write to WBS in that order.
WBS_ORDER = [
    "status", "type", "employee", "ssn", "dept", "pay_rate",
    "reg", "ot", "dt", "vac", "sick", "hol",
    "bonus", "comm",
    "pc_mon_h", "pc_mon_t",
    "pc_tue_h", "pc_tue_t",
    "pc_wed_h", "pc_wed_t",
    "pc_thu_h", "pc_thu_t",
    "pc_fri_h", "pc_fri_t",
    "travel", "notes", "comments", "totals"
]

NUMERIC_FIELDS = {
    "reg", "ot", "dt", "vac", "sick", "hol",
    "bonus", "comm", "travel",
    "pc_mon_h", "pc_mon_t", "pc_tue_h", "pc_tue_t",
    "pc_wed_h", "pc_wed_t", "pc_thu_h", "pc_thu_t",
    "pc_fri_h", "pc_fri_t",
    "pay_rate", "totals"
}

# Flexible matchers to pull Sierra columns into our canonical fields
SIERRA_MAP: Dict[str, List[str]] = {
    # identity
    "employee": [r"^employee\s*name$", r"^name$", r"^employee$", r"^weekly\s*payroll$"],
    "ssn": [r"^ssn$", r"^social", r"^ss#"],
    "status": [r"^status$"],
    "type": [r"^type$"],
    "dept": [r"^dept", r"^department$"],
    "pay_rate": [r"^pay\s*rate$", r"^rate$"],
    # hours
    "reg": [r"^reg(?:ular)?\b.*\(?.*a01", r"^regular$"],
    "ot": [r"^ot\b.*\(?.*a02", r"^overtime$"],
    "dt": [r"^dt\b.*\(?.*a03", r"^double"],
    "vac": [r"vac", r"a06"],
    "sick": [r"sick", r"a07"],
    "hol": [r"hol", r"a08"],
    # amounts
    "bonus": [r"bonus", r"a04"],
    "comm": [r"comm", r"a05"],
    "travel": [r"travel"],
    # piece counts per day
    "pc_mon_h": [r"pc\s*hrs\s*mon"],
    "pc_mon_t": [r"pc\s*ttl\s*mon"],
    "pc_tue_h": [r"pc\s*hrs\s*tue"],
    "pc_tue_t": [r"pc\s*ttl\s*tue"],
    "pc_wed_h": [r"pc\s*hrs\s*wed"],
    "pc_wed_t": [r"pc\s*ttl\s*wed"],
    "pc_thu_h": [r"pc\s*hrs\s*thu"],
    "pc_thu_t": [r"pc\s*ttl\s*thu"],
    "pc_fri_h": [r"pc\s*hrs\s*fri"],
    "pc_fri_t": [r"pc\s*ttl\s*fri"],
    # free text
    "notes": [r"^notes?$"],
    "comments": [r"^comments?$"],
}

# ---------- Helpers ----------

def normalize_col(s: str) -> str:
    return re.sub(r"\s+", " ", str(s).strip().upper())

def find_first_matching(colnames: List[str], patterns: List[str]) -> Optional[str]:
    for pat in patterns:
        rx = re.compile(pat, re.IGNORECASE)
        for c in colnames:
            if rx.search(str(c)):
                return c
    return None

def digits_only(ssn: str) -> str:
    return re.sub(r"\D", "", str(ssn or ""))

def normalize_name(n: str) -> str:
    n = (n or "").strip()
    return re.sub(r"\s+", " ", n).upper()

def to_float(x) -> float:
    try:
        if pd.isna(x): return 0.0
        return float(str(x).replace(",", ""))
    except Exception:
        return 0.0

def compute_totals_row(row: pd.Series) -> float:
    # Regular pay + OT*1.5 + DT*2 + paid leave hours at 1.0 + bonus + comm + travel
    rate = to_float(row.get("pay_rate"))
    reg = to_float(row.get("reg"))
    ot  = to_float(row.get("ot"))
    dt  = to_float(row.get("dt"))
    vac = to_float(row.get("vac"))
    sick= to_float(row.get("sick"))
    hol = to_float(row.get("hol"))
    bonus = to_float(row.get("bonus"))
    comm  = to_float(row.get("comm"))
    travel = to_float(row.get("travel"))
    return round(rate*(reg + 1.5*ot + 2.0*dt + vac + sick + hol) + bonus + comm + travel, 2)

def load_roster_order() -> List[str]:
    """Return canonical order as a list of SSNs (digits only). Falls back to empty list."""
    order: List[str] = []
    try:
        if ROSTER_XLSX.exists():
            df = pd.read_excel(ROSTER_XLSX)
        elif ROSTER_CSV.exists():
            df = pd.read_csv(ROSTER_CSV)
        else:
            return order
        cols = [normalize_col(c) for c in df.columns]
        # Prefer SSN; if absent, use Employee
        ssn_col = find_first_matching(cols, [r"^SSN$", r"SOCIAL"])
        name_col = find_first_matching(cols, [r"^EMPLOYEE", r"^NAME$"])
        if ssn_col is not None:
            for v in df.iloc[:, cols.index(ssn_col)].tolist():
                s = digits_only(v)
                if s:
                    order.append(s)
        elif name_col is not None:
            for v in df.iloc[:, cols.index(name_col)].tolist():
                order.append(normalize_name(v))
    except Exception:
        pass
    return order

def read_sierra_excel(content: bytes) -> pd.DataFrame:
    """Read Sierra spreadsheet and normalize to our canonical columns."""
    # Try to read first sheet
    df_raw = pd.read_excel(io.BytesIO(content), dtype=str)
    # Normalize header names for matching
    orig_cols = list(df_raw.columns)
    norm_cols = [normalize_col(c) for c in orig_cols]

    # Build normalized frame
    data: Dict[str, List] = {}
    for key in WBS_ORDER:
        data[key] = []

    # Resolve source columns for each canonical key
    src_for: Dict[str, Optional[str]] = {}
    for key, pats in SIERRA_MAP.items():
        col = find_first_matching(norm_cols, pats)
        src_for[key] = col

    # Helper to get a value by canonical key
    def get_value(ix: int, key: str):
        col = src_for.get(key)
        if col is None:
            return "" if key not in NUMERIC_FIELDS else 0.0
        val = df_raw.iloc[ix, norm_cols.index(col)]
        if key in NUMERIC_FIELDS:
            return to_float(val)
        return (val or "")

    # Build rows
    for i in range(len(df_raw)):
        row = {}
        # identity
        row["status"] = get_value(i, "status")
        row["type"] = get_value(i, "type")
        # Name: keep as given; we will later convert to "Last, First" if needed
        row["employee"] = get_value(i, "employee")
        row["ssn"] = digits_only(get_value(i, "ssn"))
        row["dept"] = get_value(i, "dept")
        row["pay_rate"] = get_value(i, "pay_rate")
        # hours + amounts
        for k in ["reg","ot","dt","vac","sick","hol","bonus","comm","travel",
                  "pc_mon_h","pc_mon_t","pc_tue_h","pc_tue_t",
                  "pc_wed_h","pc_wed_t","pc_thu_h","pc_thu_t","pc_fri_h","pc_fri_t"]:
            row[k] = get_value(i, k)
        # placeholders
        row["notes"] = get_value(i, "notes")
        row["comments"] = get_value(i, "comments")
        # totals computed post-aggregation
        row["totals"] = 0.0
        for k in WBS_ORDER:
            data[k].append(row[k])

    df = pd.DataFrame(data)
    # Normalize name now to "Last, First" if it looks like "First Last"
    # (We won't be aggressive; many names already in Last, First.)
    def to_last_first(n: str) -> str:
        n = (n or "").strip()
        if "," in n:
            return n  # already "Last, First"
        parts = [p for p in re.split(r"\s+", n) if p]
        if len(parts) >= 2:
            return f"{parts[-1]}, {' '.join(parts[:-1])}"
        return n

    df["employee"] = df["employee"].fillna("").map(to_last_first)
    return df

def consolidate_employees(df: pd.DataFrame) -> pd.DataFrame:
    """Group by SSN (or name if SSN missing) and sum numeric fields."""
    df = df.copy()

    # Keys: prefer SSN; fallback to normalized name
    df["key_ssn"] = df["ssn"].map(digits_only)
    df["key_name"] = df["employee"].map(normalize_name)
    df["group_key"] = df["key_ssn"]
    df.loc[df["group_key"] == "", "group_key"] = df.loc[df["group_key"] == "", "key_name"]

    # Aggregations
    agg_spec = {k: "sum" for k in NUMERIC_FIELDS if k != "totals"}
    # Non-numeric: take first non-empty
    def first_non_empty(series: pd.Series):
        for v in series:
            if pd.notna(v) and str(v).strip() != "":
                return v
        return ""

    keep_first = ["status","type","employee","ssn","dept","notes","comments"]
    for k in keep_first:
        agg_spec[k] = first_non_empty

    grouped = df.groupby("group_key", dropna=False).agg(agg_spec).reset_index(drop=True)

    # Compute totals per row
    grouped["totals"] = grouped.apply(compute_totals_row, axis=1)

    # Final columns in the canonical order
    return grouped[[*WBS_ORDER]]

def apply_stable_order(df: pd.DataFrame, order_list: List[str]) -> pd.DataFrame:
    """Sort df by canonical roster order (SSN list), append unknowns at bottom."""
    if not order_list:
        # No roster provided: leave as-is
        return df

    # Build a rank per row
    def rank_row(row) -> Tuple[int, int]:
        ssn = digits_only(row["ssn"])
        name = normalize_name(row["employee"])
        try:
            if ssn:
                return (0, order_list.index(ssn))
            else:
                return (1, order_list.index(name))
        except ValueError:
            return (2, 10**9)  # unknowns at the bottom

    ranks = df.apply(rank_row, axis=1, result_type="expand")
    df = df.assign(_b=ranks[0], _r=ranks[1]).sort_values(by=["_b","_r","employee"]).drop(columns=["_b","_r"])
    return df

def discover_template_columns(ws: Worksheet) -> Dict[str, int]:
    """
    Find target column indices by scanning first 15 rows for headers contained in TEMPLATE_HEADERS.
    Returns a dict of header_label -> 1-based column index.
    """
    found: Dict[str, int] = {}
    max_scan_rows = min(15, ws.max_row)
    max_scan_cols = ws.max_column
    for r in range(1, max_scan_rows+1):
        for c in range(1, max_scan_cols+1):
            v = ws.cell(row=r, column=c).value
            if v is None:
                continue
            lab = normalize_col(str(v))
            for h in TEMPLATE_HEADERS:
                if normalize_col(h) == lab and h not in found:
                    found[h] = c
    # Backfill aliases
    if "TOTALS" in found and "Totals" not in found:
        found["Totals"] = found["TOTALS"]
    if "Pay Rate" in found and "Pay" in found and "PAY RATE" not in found:
        found["PAY RATE"] = found["Pay Rate"]
    return found

def col(ws: Worksheet, label_map: Dict[str,int], *labels: str) -> int:
    for lab in labels:
        if lab in label_map:
            return label_map[lab]
    raise KeyError(f"Template column not found for any of: {labels}")

def safe_write(ws: Worksheet, r: int, c: int, value):
    """Avoid writing into merged shadow cells."""
    try:
        cell = ws.cell(row=r, column=c)
        # openpyxl returns MergedCell for non-top-left cells in a merge range; those are read-only
        cell.value = value  # will raise AttributeError if merged shadow
    except AttributeError:
        # Skip—template formatting stays intact
        pass

def clear_previous_data(ws: Worksheet, start_row: int, last_col: int):
    """Clear values (not styles) from previous run below start_row."""
    max_row = ws.max_row
    if max_row < start_row:
        return
    for r in range(start_row, max_row+1):
        # If the row already looks empty (first non-static data cell is blank), skip clears for speed
        row_blank = True
        for c in range(1, last_col+1):
            v = ws.cell(row=r, column=c).value
            if v not in (None, ""):
                row_blank = False
                break
        if row_blank:
            continue
        for c in range(1, last_col+1):
            try:
                ws.cell(row=r, column=c).value = None
            except AttributeError:
                # merged shadow
                continue

def write_to_template(df: pd.DataFrame, template_path: Path, sheet_name: Optional[str] = None) -> bytes:
    if not template_path.exists():
        raise HTTPException(status_code=500, detail=f"WBS template not found at {template_path}")

    wb = load_workbook(str(template_path))
    ws = wb.active if sheet_name is None else wb[sheet_name]

    label_map = discover_template_columns(ws)

    # Column indices in template
    C_STATUS = col(ws, label_map, "Status")
    C_TYPE   = col(ws, label_map, "Type")
    C_EMP    = col(ws, label_map, "Employee", "Employee Name")
    C_SSN    = col(ws, label_map, "SSN")
    C_DEPT   = col(ws, label_map, "Department")
    C_RATE   = col(ws, label_map, "Pay Rate", "PAY RATE", "Pay")
    C_REG    = col(ws, label_map, "REG", "REGULAR")
    C_OT     = col(ws, label_map, "OVERTIME", "OT")
    C_DT     = col(ws, label_map, "DOUBLETIME", "DT")
    C_VAC    = col(ws, label_map, "VACATION")
    C_SICK   = col(ws, label_map, "SICK")
    C_HOL    = col(ws, label_map, "HOLIDAY")
    C_BONUS  = col(ws, label_map, "BONUS")
    C_COMM   = col(ws, label_map, "COMMISSION")
    C_TRAVEL = col(ws, label_map, "TRAVEL AMOUNT")
    C_NOTES  = col(ws, label_map, "Notes")
    C_COMMENTS = col(ws, label_map, "Comments")
    C_TOT    = col(ws, label_map, "TOTALS", "Totals")

    C_PCS = {
        "pc_mon_h": col(ws, label_map, "PC HRS MON"),
        "pc_mon_t": col(ws, label_map, "PC TTL MON"),
        "pc_tue_h": col(ws, label_map, "PC HRS TUE"),
        "pc_tue_t": col(ws, label_map, "PC TTL TUE"),
        "pc_wed_h": col(ws, label_map, "PC HRS WED"),
        "pc_wed_t": col(ws, label_map, "PC TTL WED"),
        "pc_thu_h": col(ws, label_map, "PC HRS THU"),
        "pc_thu_t": col(ws, label_map, "PC TTL THU"),
        "pc_fri_h": col(ws, label_map, "PC HRS FRI"),
        "pc_fri_t": col(ws, label_map, "PC TTL FRI"),
    }

    last_col = max([C_TOT, C_TRAVEL, *C_PCS.values()])

    # Clear prior values but keep styles/merges
    clear_previous_data(ws, WBS_DATA_START_ROW, last_col)

    # Write rows
    r = WBS_DATA_START_ROW
    for _, row in df.iterrows():
        safe_write(ws, r, C_STATUS,   row.get("status"))
        safe_write(ws, r, C_TYPE,     row.get("type"))
        safe_write(ws, r, C_EMP,      row.get("employee"))
        safe_write(ws, r, C_SSN,      row.get("ssn"))
        safe_write(ws, r, C_DEPT,     row.get("dept"))
        safe_write(ws, r, C_RATE,     float(row.get("pay_rate") or 0))
        safe_write(ws, r, C_REG,      float(row.get("reg") or 0))
        safe_write(ws, r, C_OT,       float(row.get("ot") or 0))
        safe_write(ws, r, C_DT,       float(row.get("dt") or 0))
        safe_write(ws, r, C_VAC,      float(row.get("vac") or 0))
        safe_write(ws, r, C_SICK,     float(row.get("sick") or 0))
        safe_write(ws, r, C_HOL,      float(row.get("hol") or 0))
        safe_write(ws, r, C_BONUS,    float(row.get("bonus") or 0))
        safe_write(ws, r, C_COMM,     float(row.get("comm") or 0))
        for k, cidx in C_PCS.items():
            safe_write(ws, r, cidx, float(row.get(k) or 0))
        safe_write(ws, r, C_TRAVEL,   float(row.get("travel") or 0))
        safe_write(ws, r, C_NOTES,    row.get("notes") or "")
        safe_write(ws, r, C_COMMENTS, row.get("comments") or "")
        safe_write(ws, r, C_TOT,      float(row.get("totals") or 0))
        r += 1

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.read()

# ---------- API ----------

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/process-payroll")
async def process_payroll(file: UploadFile = File(...), sheet_name: Optional[str] = None):
    try:
        contents = await file.read()
        # 1) Read Sierra and normalize
        df_raw = read_sierra_excel(contents)

        # 2) Consolidate duplicates (SSN/name) -> one row per employee
        df_cons = consolidate_employees(df_raw)

        # 3) Apply stable order
        order_list = load_roster_order()
        df_sorted = apply_stable_order(df_cons, order_list)

        # 4) Write into template
        out_bytes = write_to_template(df_sorted, TEMPLATE_PATH, sheet_name=sheet_name)

        # Filename like WBS_Payroll_YYYY-MM-DD.xlsx if we can pick from Sierra dates; else generic
        out_name = "WBS_Payroll.xlsx"
        return StreamingResponse(
            io.BytesIO(out_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{out_name}"'},
        )
    except HTTPException:
        raise
    except Exception as e:
        # Bubble a readable error; front-end shows this
        raise HTTPException(status_code=500, detail=f"Processing failed: {e}")
