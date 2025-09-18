# app/main.py
from __future__ import annotations

import io
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet

# --------------------------------------------------------------------------------------
# App
# --------------------------------------------------------------------------------------
app = FastAPI(title="Sierra → WBS Converter", version="2.0.0")

# --------------------------------------------------------------------------------------
# Paths (template + roster in REPO ROOT; this file in app/)
# --------------------------------------------------------------------------------------
HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent

TEMPLATE_PATH = REPO_ROOT / "wbs_template.xlsx"    # must exist
ROSTER_PATH   = REPO_ROOT / "roster.xlsx"          # must exist (for SSNs)
ORDER_PATH    = REPO_ROOT / "employee_order.json"  # persisted SSN order

# --------------------------------------------------------------------------------------
# Template discovery (we auto-find these headers in the first ~15 rows)
# --------------------------------------------------------------------------------------
TEMPLATE_HEADERS = [
    "Status", "Type", "Employee", "Employee Name", "SSN", "Department",
    "Pay", "Pay Rate",
    "REG", "REGULAR",
    "OVERTIME", "OT",
    "DOUBLETIME", "DT",
    "VACATION", "SICK", "HOLIDAY",
    "BONUS", "COMMISSION",
    "PC HRS MON", "PC TTL MON",
    "PC HRS TUE", "PC TTL TUE",
    "PC HRS WED", "PC TTL WED",
    "PC HRS THU", "PC TTL THU",
    "PC HRS FRI", "PC TTL FRI",
    "TRAVEL AMOUNT",
    "Notes", "Comments",
    "Totals", "TOTALS",
]

# First data row in your WBS (rows above are title/meta). Adjust if your template differs.
WBS_DATA_START_ROW = 9

# --------------------------------------------------------------------------------------
# Sierra → Canonical keys (we normalize Sierra columns to these)
# --------------------------------------------------------------------------------------
CANON_KEYS = [
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
    "pay_rate", "reg", "ot", "dt", "vac", "sick", "hol",
    "bonus", "comm", "travel",
    "pc_mon_h", "pc_mon_t", "pc_tue_h", "pc_tue_t",
    "pc_wed_h", "pc_wed_t", "pc_thu_h", "pc_thu_t", "pc_fri_h", "pc_fri_t",
    "totals"
}

# Flexible Sierra header matching
SIERRA_MAP: Dict[str, List[str]] = {
    # identity
    "employee": [r"^employee(\s*name)?$", r"^name$", r"^worker$"],
    "ssn": [r"^ssn$", r"social"],
    "status": [r"^status$"],
    "type": [r"^type$", r"pay\s*type"],
    "dept": [r"^dept", r"^department$"],
    "pay_rate": [r"^pay\s*rate$", r"^rate$"],
    # hours buckets
    "reg": [r"^reg(ular)?(\s*\(?.*a01\)?)?$", r"^a01$"],
    "ot":  [r"^ot(\s*\(?.*a02\)?)?$", r"^overtime$", r"^a02$"],
    "dt":  [r"^dt(\s*\(?.*a03\)?)?$", r"^double", r"^a03$"],
    "vac": [r"vac", r"a06"],
    "sick":[r"sick", r"a07"],
    "hol": [r"hol", r"holiday", r"a08"],
    # amounts
    "bonus": [r"bonus", r"a04"],
    "comm":  [r"comm", r"commission", r"a05"],
    "travel":[r"travel"],
    # piece counts (optional)
    "pc_mon_h": [r"pc\s*hrs\s*mon"], "pc_mon_t": [r"pc\s*ttl\s*mon"],
    "pc_tue_h": [r"pc\s*hrs\s*tue"], "pc_tue_t": [r"pc\s*ttl\s*tue"],
    "pc_wed_h": [r"pc\s*hrs\s*wed"], "pc_wed_t": [r"pc\s*ttl\s*wed"],
    "pc_thu_h": [r"pc\s*hrs\s*thu"], "pc_thu_t": [r"pc\s*ttl\s*thu"],
    "pc_fri_h": [r"pc\s*hrs\s*fri"], "pc_fri_t": [r"pc\s*ttl\s*fri"],
    # free text
    "notes": [r"^notes?$"],
    "comments": [r"^comments?$"],
}

# --------------------------------------------------------------------------------------
# Utilities
# --------------------------------------------------------------------------------------
def norm_col(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "").strip().upper())

def find_col(candidates: List[str], pats: List[str]) -> Optional[str]:
    for pat in pats:
        rx = re.compile(pat, re.I)
        for c in candidates:
            if rx.search(c):
                return c
    return None

def digits_only(x) -> str:
    return re.sub(r"\D", "", str(x or ""))

def to_float(x) -> float:
    if x is None: return 0.0
    try: return float(str(x).replace(",", ""))
    except Exception: return 0.0

def compute_total_row(row: pd.Series) -> float:
    """Hourly: rate*(REG+1.5*OT+2*DT+VAC+SICK+HOL) + BONUS + COMM + TRAVEL
       Salary: base = pay_rate (weekly) + BONUS + COMM + TRAVEL
    """
    pay_type = str(row.get("type") or "").strip().upper()[:1]  # H or S
    rate = to_float(row.get("pay_rate"))
    reg  = to_float(row.get("reg"));  ot = to_float(row.get("ot"));  dt = to_float(row.get("dt"))
    vac  = to_float(row.get("vac"));  sick = to_float(row.get("sick")); hol = to_float(row.get("hol"))
    bonus= to_float(row.get("bonus")); comm = to_float(row.get("comm")); travel = to_float(row.get("travel"))
    if pay_type == "S":
        base = rate
    else:
        base = rate * (reg + 1.5*ot + 2.0*dt + vac + sick + hol)
    return round(base + bonus + comm + travel, 2)

def to_last_first(name: str) -> str:
    n = (name or "").strip()
    if "," in n: return n
    parts = [p for p in re.split(r"\s+", n) if p]
    if len(parts) >= 2:
        return f"{parts[-1]}, {' '.join(parts[:-1])}"
    return n

# --------------------------------------------------------------------------------------
# Load roster + order
# --------------------------------------------------------------------------------------
def load_roster() -> pd.DataFrame:
    if not ROSTER_PATH.exists():
        raise HTTPException(status_code=500, detail=f"Roster not found at {ROSTER_PATH}")
    df = pd.read_excel(ROSTER_PATH)
    cols_norm = {c: norm_col(c) for c in df.columns}
    df.rename(columns=cols_norm, inplace=True)

    def pick(*names) -> Optional[str]:
        for n in names:
            if n in df.columns: return n
        return None

    name_c = pick("EMPLOYEE", "EMPLOYEE NAME", "NAME")
    ssn_c  = pick("SSN", "SOCIAL", "SOCIAL SECURITY")
    dept_c = pick("DEPT", "DEPARTMENT")
    rate_c = pick("PAY RATE", "RATE", "PAY_RATE")
    stat_c = pick("STATUS")
    type_c = pick("TYPE", "PAY TYPE")

    if not name_c or not ssn_c:
        raise HTTPException(status_code=500, detail="Roster must have at least Name and SSN columns")

    out = pd.DataFrame({
        "name": df[name_c].astype(str).str.strip(),
        "ssn": df[ssn_c].astype(str).map(digits_only).str.zfill(9),
        "dept": df[dept_c] if dept_c else "",
        "rate": pd.to_numeric(df[rate_c], errors="coerce").fillna(0.0) if rate_c else 0.0,
        "status": (df[stat_c].astype(str).str.strip().str[:1].str.upper() if stat_c else "A"),
        "type": (df[type_c].astype(str).str.strip().str[:1].str.upper() if type_c else "H"),
    })
    out["name_norm"] = out["name"].map(lambda s: to_last_first(s).upper())
    return out

def load_or_seed_order(ssns: List[str], roster: pd.DataFrame) -> List[str]:
    # Use existing persisted order if present
    if ORDER_PATH.exists():
        try:
            saved = json.loads(ORDER_PATH.read_text())
            # keep only those still present + append any new at end
            keep = [s for s in saved if s in ssns]
            new  = [s for s in ssns if s not in keep and s]
            return keep + new
        except Exception:
            pass
    # Seed by roster SSN order; if empty, seed by current ssns as-is
    base = [s for s in roster["ssn"].tolist() if s in ssns]
    base += [s for s in ssns if s not in base and s]
    try:
        ORDER_PATH.write_text(json.dumps(base, indent=2))
    except Exception:
        pass
    return base

# --------------------------------------------------------------------------------------
# Read Sierra, normalize, attach SSN, aggregate
# --------------------------------------------------------------------------------------
def read_sierra(contents: bytes) -> pd.DataFrame:
    df_raw = pd.read_excel(io.BytesIO(contents))
    # normalize headers
    cols = [norm_col(c) for c in df_raw.columns]
    df_raw.columns = cols

    # map canonical
    src_for: Dict[str, Optional[str]] = {}
    for key, pats in SIERRA_MAP.items():
        src_for[key] = find_col(cols, pats)

    # build normalized rows
    data = {k: [] for k in CANON_KEYS}
    def get(i: int, key: str):
        c = src_for.get(key)
        if c is None:
            return 0.0 if key in NUMERIC_FIELDS else ""
        v = df_raw.iloc[i, cols.index(c)]
        return to_float(v) if key in NUMERIC_FIELDS else (v or "")

    for i in range(len(df_raw)):
        row = {}
        row["status"]   = (get(i, "status") or "")
        row["type"]     = (get(i, "type") or "")
        row["employee"] = to_last_first(get(i, "employee"))
        row["ssn"]      = digits_only(get(i, "ssn"))
        row["dept"]     = (get(i, "dept") or "")
        row["pay_rate"] = get(i, "pay_rate")
        for k in ["reg","ot","dt","vac","sick","hol","bonus","comm","travel",
                  "pc_mon_h","pc_mon_t","pc_tue_h","pc_tue_t",
                  "pc_wed_h","pc_wed_t","pc_thu_h","pc_thu_t","pc_fri_h","pc_fri_t",
                  "notes","comments"]:
            row[k] = get(i, k)
        row["totals"] = 0.0
        for k in CANON_KEYS:
            data[k].append(row[k])
    return pd.DataFrame(data)

def attach_roster_and_aggregate(sierra_df: pd.DataFrame, roster: pd.DataFrame) -> pd.DataFrame:
    # Normalize keys for join
    s_df = sierra_df.copy()
    s_df["name_norm"] = s_df["employee"].map(lambda s: (s or "").upper())
    # Join Sierra→Roster on normalized name (robustness)
    merged = s_df.merge(
        roster[["name_norm","ssn","dept","rate","status","type"]],
        how="left",
        on="name_norm",
        suffixes=("","_r"),
    )

    # Prefer roster identity fields where present
    merged["ssn_out"]   = merged["ssn_r"].fillna(merged["ssn"]).fillna("")
    merged["dept_out"]  = merged["dept_r"].fillna(merged["dept"]).fillna("")
    merged["rate_out"]  = merged["rate_r"].fillna(merged["pay_rate"]).fillna(0.0).map(to_float)
    merged["status_out"]= merged["status_r"].fillna(merged["status"]).fillna("A").astype(str).str[:1].str.upper()
    merged["type_out"]  = merged["type_r"].fillna(merged["type"]).fillna("H").astype(str).str[:1].str.upper()

    # Aggregate by SSN (empty SSN groups by name as fallback)
    merged["group_key"] = merged["ssn_out"]
    mask_empty = merged["group_key"].eq("")
    merged.loc[mask_empty, "group_key"] = merged.loc[mask_empty, "name_norm"]

    # Sum numeric buckets
    sum_cols = ["reg","ot","dt","vac","sick","hol","bonus","comm","travel",
                "pc_mon_h","pc_mon_t","pc_tue_h","pc_tue_t","pc_wed_h","pc_wed_t",
                "pc_thu_h","pc_thu_t","pc_fri_h","pc_fri_t"]
    for c in sum_cols:
        merged[c] = pd.to_numeric(merged[c], errors="coerce").fillna(0.0)

    agg_sum = merged.groupby("group_key", dropna=False)[sum_cols].sum().reset_index()

    # First non-empty identity per group
    keep_first = (merged
                  .sort_values(["group_key","name_norm"])
                  .groupby("group_key", dropna=False)
                  .agg({
                      "employee": "first",
                      "ssn_out": "first",
                      "dept_out": "first",
                      "rate_out": "first",
                      "status_out": "first",
                      "type_out": "first",
                      "notes": "first",
                      "comments": "first",
                  })
                  .reset_index(drop=True))

    agg = keep_first.merge(agg_sum, left_on="group_key", right_on="group_key", how="left")

    # Compute per-row totals
    agg = agg.fillna({"notes":"", "comments":""})
    agg["totals"] = agg.apply(lambda r: compute_total_row(pd.Series({
        "type": r["type_out"], "pay_rate": r["rate_out"],
        "reg": r["reg"], "ot": r["ot"], "dt": r["dt"],
        "vac": r["vac"], "sick": r["sick"], "hol": r["hol"],
        "bonus": r["bonus"], "comm": r["comm"], "travel": r["travel"],
    })), axis=1)

    # Final tidy columns
    final_cols = [
        "employee","ssn_out","status_out","type_out","dept_out","rate_out",
        "reg","ot","dt","vac","sick","hol","bonus","comm",
        "pc_mon_h","pc_mon_t","pc_tue_h","pc_tue_t","pc_wed_h","pc_wed_t",
        "pc_thu_h","pc_thu_t","pc_fri_h","pc_fri_t",
        "travel","notes","comments","totals"
    ]
    agg = agg[final_cols].rename(columns={
        "ssn_out":"ssn", "status_out":"status", "type_out":"type",
        "dept_out":"dept", "rate_out":"pay_rate"
    })
    return agg

def apply_stable_order(df: pd.DataFrame, roster: pd.DataFrame) -> pd.DataFrame:
    ssns = df["ssn"].astype(str).tolist()
    order = load_or_seed_order(ssns, roster)
    order_map = {s:i for i,s in enumerate(order) if s}
    BIG = 10**9
    df = df.assign(
        __k=df["ssn"].map(lambda s: order_map.get(s, BIG)),
        __n=df["employee"].astype(str)
    ).sort_values(["__k","__n"]).drop(columns=["__k","__n"]).reset_index(drop=True)
    # ensure unknown-SSN rows go last
    known = df[df["ssn"].astype(str) != ""]
    unknown = df[df["ssn"].astype(str) == ""]
    return pd.concat([known, unknown], ignore_index=True)

# --------------------------------------------------------------------------------------
# Template I/O
# --------------------------------------------------------------------------------------
def discover_template_columns(ws: Worksheet) -> Dict[str,int]:
    found: Dict[str,int] = {}
    max_r = min(15, ws.max_row)
    max_c = ws.max_column
    for r in range(1, max_r+1):
        for c in range(1, max_c+1):
            v = ws.cell(row=r, column=c).value
            if v is None: continue
            text = norm_col(str(v))
            for hdr in TEMPLATE_HEADERS:
                if norm_col(hdr) == text and hdr not in found:
                    found[hdr] = c
    # simple aliases
    if "Employee" in found and "Employee Name" not in found:
        found["Employee Name"] = found["Employee"]
    if "TOTALS" in found and "Totals" not in found:
        found["Totals"] = found["TOTALS"]
    if "Pay Rate" in found and "Pay" not in found:
        found["Pay"] = found["Pay Rate"]
    return found

def tcol(label_map: Dict[str,int], *labels: str) -> int:
    for lab in labels:
        if lab in label_map:
            return label_map[lab]
    raise HTTPException(status_code=500, detail=f"Template missing headers: {labels}")

def is_merged_shadow(ws: Worksheet, r: int, c: int) -> bool:
    """True if (r,c) is NOT the top-left of a merged range but lies inside one (read-only)."""
    for rng in ws.merged_cells.ranges:
        if rng.min_row <= r <= rng.max_row and rng.min_col <= c <= rng.max_col:
            # shadow unless it's the anchor
            return not (r == rng.min_row and c == rng.min_col)
    return False

def safe_write(ws: Worksheet, r: int, c: int, value):
    try:
        if is_merged_shadow(ws, r, c):
            return  # never write into merged-shadow cells
        ws.cell(row=r, column=c).value = value
    except Exception:
        # never crash the run because of a style quirk
        pass

def clear_previous_data(ws: Worksheet, start_row: int, last_col: int):
    max_row = ws.max_row
    if max_row < start_row: return
    for r in range(start_row, max_row+1):
        row_blank = True
        for c in range(1, last_col+1):
            v = ws.cell(row=r, column=c).value
            if v not in (None, ""):
                row_blank = False
                break
        if row_blank: continue
        for c in range(1, last_col+1):
            safe_write(ws, r, c, None)

def write_to_template(df: pd.DataFrame, template_path: Path, sheet: Optional[str]=None) -> bytes:
    if not template_path.exists():
        raise HTTPException(status_code=500, detail=f"WBS template not found at {template_path}")

    wb = load_workbook(str(template_path))
    ws = wb.active if not sheet else wb[sheet]

    labels = discover_template_columns(ws)

    C_STATUS = tcol(labels, "Status")
    C_TYPE   = tcol(labels, "Type")
    C_EMP    = tcol(labels, "Employee Name", "Employee")
    C_SSN    = tcol(labels, "SSN")
    C_DEPT   = tcol(labels, "Department")
    C_RATE   = tcol(labels, "Pay Rate", "Pay")

    C_REG    = tcol(labels, "REG", "REGULAR")
    C_OT     = tcol(labels, "OVERTIME", "OT")
    C_DT     = tcol(labels, "DOUBLETIME", "DT")
    C_VAC    = tcol(labels, "VACATION")
    C_SICK   = tcol(labels, "SICK")
    C_HOL    = tcol(labels, "HOLIDAY")
    C_BONUS  = tcol(labels, "BONUS")
    C_COMM   = tcol(labels, "COMMISSION")

    # Optional day piece columns (ignore if missing)
    C_PCS = {}
    for k in ["PC HRS MON","PC TTL MON","PC HRS TUE","PC TTL TUE","PC HRS WED","PC TTL WED",
              "PC HRS THU","PC TTL THU","PC HRS FRI","PC TTL FRI"]:
        if k in labels: C_PCS[k] = labels[k]

    C_TRAVEL = tcol(labels, "TRAVEL AMOUNT")
    C_NOTES  = labels.get("Notes")
    C_COMMTS = labels.get("Comments")
    C_TOTALS = tcol(labels, "TOTALS", "Totals")

    last_col = max([C_TOTALS, C_TRAVEL, C_RATE, C_COMM, C_BONUS, C_HOL, *C_PCS.values()] if C_PCS else
                   [C_TOTALS, C_TRAVEL, C_RATE, C_COMM, C_BONUS, C_HOL])

    # Clear previous values, keep styles
    clear_previous_data(ws, WBS_DATA_START_ROW, last_col)

    # Write rows
    r = WBS_DATA_START_ROW
    for _, row in df.iterrows():
        safe_write(ws, r, C_STATUS, str(row.get("status") or "A")[:1].upper())
        safe_write(ws, r, C_TYPE,   str(row.get("type") or "H")[:1].upper())
        safe_write(ws, r, C_EMP,    row.get("employee") or "")
        safe_write(ws, r, C_SSN,    row.get("ssn") or "")
        safe_write(ws, r, C_DEPT,   row.get("dept") or "")
        safe_write(ws, r, C_RATE,   float(row.get("pay_rate") or 0.0))

        safe_write(ws, r, C_REG,    float(row.get("reg") or 0.0))
        safe_write(ws, r, C_OT,     float(row.get("ot") or 0.0))
        safe_write(ws, r, C_DT,     float(row.get("dt") or 0.0))
        safe_write(ws, r, C_VAC,    float(row.get("vac") or 0.0))
        safe_write(ws, r, C_SICK,   float(row.get("sick") or 0.0))
        safe_write(ws, r, C_HOL,    float(row.get("hol") or 0.0))
        safe_write(ws, r, C_BONUS,  float(row.get("bonus") or 0.0))
        safe_write(ws, r, C_COMM,   float(row.get("comm") or 0.0))

        # Day piece columns if present in template
        pcs_map = {
            "PC HRS MON":"pc_mon_h", "PC TTL MON":"pc_mon_t",
            "PC HRS TUE":"pc_tue_h", "PC TTL TUE":"pc_tue_t",
            "PC HRS WED":"pc_wed_h", "PC TTL WED":"pc_wed_t",
            "PC HRS THU":"pc_thu_h", "PC TTL THU":"pc_thu_t",
            "PC HRS FRI":"pc_fri_h", "PC TTL FRI":"pc_fri_t",
        }
        for hdr, canon in pcs_map.items():
            if hdr in C_PCS:
                safe_write(ws, r, C_PCS[hdr], float(row.get(canon) or 0.0))

        safe_write(ws, r, C_TRAVEL, float(row.get("travel") or 0.0))
        if C_NOTES:  safe_write(ws, r, C_NOTES,  row.get("notes") or "")
        if C_COMMTS: safe_write(ws, r, C_COMMTS, row.get("comments") or "")

        # Pink totals (far right)
        safe_write(ws, r, C_TOTALS, float(row.get("totals") or 0.0))
        r += 1

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.read()

# --------------------------------------------------------------------------------------
# API
# --------------------------------------------------------------------------------------
@app.get("/health")
def health():
    return {"ok": True}

@app.post("/process-payroll")
async def process_payroll(file: UploadFile = File(...), sheet_name: Optional[str] = None):
    try:
        contents = await file.read()
        if not contents:
            raise HTTPException(status_code=400, detail="Empty upload")

        # 1) Read Sierra / normalize
        sierra_df = read_sierra(contents)

        # 2) Load roster (SSN/rate/dept/status/type)
        roster = load_roster()

        # 3) Attach roster + aggregate by SSN (dedupe)
        agg = attach_roster_and_aggregate(sierra_df, roster)

        # 4) Apply stable order
        ordered = apply_stable_order(agg, roster)

        # 5) Write to WBS template
        out_bytes = write_to_template(ordered, TEMPLATE_PATH, sheet=sheet_name)

        return StreamingResponse(
            io.BytesIO(out_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": 'attachment; filename="WBS_Payroll.xlsx"'},
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing failed: {e}")
