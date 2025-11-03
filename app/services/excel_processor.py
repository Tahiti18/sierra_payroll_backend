# app/services/excel_processor.py
# Sierra → WBS (FULLY CALCULATED, HEADER-SAFE, BLANK-TEMPLATE TOLERANT)
# - Works with a nearly blank wbs_template.xlsx (will create headers if missing)
# - Reads Sierra daily logs (Days, Name, Hours, Rate)
# - Accepts Days as real dates OR weekday text ("Mon", "Tue", ...), maps to Mon=0..Sun=6
# - Splits hours into REG (≤8/day), OT (8–12/day), DT (>12/day) + overlays WEEKLY >40 into OT
# - Computes Mon–Fri per-day hours and per-day totals (HRS × Pay Rate)
# - Fills Status/Type/Dept/SSN/EmpID from roster.xlsx when available
# - Writes blanks instead of zeros; writes fixed numeric Totals (no Excel formulas)
# - Preserves WBS name order (existing names first), then A–Z by last name
from __future__ import annotations

import io, re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple, List

import pandas as pd
from openpyxl import load_workbook, Workbook
from openpyxl.worksheet.worksheet import Worksheet

# ---------- Paths ----------
HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent.parent
REPO_ROOT = PROJECT_ROOT.parent

WBS_TEMPLATE_PATH = REPO_ROOT / "wbs_template.xlsx"
ROSTER_PATH       = REPO_ROOT / "roster.xlsx"

# ---------- helpers ----------
def _num(s) -> Optional[float]:
    if s is None:
        return None
    if isinstance(s, (int, float)):
        try:
            return float(s)
        except Exception:
            return None
    ss = str(s).strip().replace("$", "").replace(",", "")
    m = re.search(r"-?\d+(\.\d+)?", ss)
    return float(m.group(0)) if m else None

def _safe_int(x) -> Optional[int]:
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass
    try:
        return int(float(str(x).replace(",", "").strip()))
    except Exception:
        return None

def _normalize_name_for_join(name: str) -> Tuple[str, str]:
    if not isinstance(name, str):
        return ("", "")
    s = " ".join(name.replace(",", " ").split()).strip()
    if not s:
        return ("", "")
    parts = s.split(" ")
    if "," in name:
        last, rest = [x.strip() for x in name.split(",", 1)]
        first = rest.split(" ")[0] if rest else ""
    else:
        first, last = parts[0], parts[-1]
    ln = "".join(ch for ch in last.lower() if ch.isalpha())
    fn = "".join(ch for ch in first.lower() if ch.isalpha())
    return ln, fn

def _best_join(left_df: pd.DataFrame, right_df: pd.DataFrame,
               left_name_col: str, right_name_col: str) -> pd.DataFrame:
    L = left_df.copy(); R = right_df.copy()
    L[["__ln", "__fn"]] = L[left_name_col].apply(lambda s: pd.Series(_normalize_name_for_join(str(s))))
    R[["__ln", "__fn"]] = R[right_name_col].apply(lambda s: pd.Series(_normalize_name_for_join(str(s))))
    M = pd.merge(L, R, on=["__ln", "__fn"], how="left", suffixes=("", "_roster"))
    return M.drop(columns=["__ln", "__fn"])

def _last_name_key(full_name: str) -> Tuple[str, str]:
    s = str(full_name or "").strip()
    if not s:
        return ("", "")
    if "," in s:
        last, rest = [x.strip() for x in s.split(",", 1)]
        first = rest.split(" ")[0] if rest else ""
    else:
        parts = s.split()
        first = parts[0]
        last = parts[-1]
    ln = "".join(ch for ch in last.lower() if ch.isalpha())
    fn = "".join(ch for ch in first.lower() if ch.isalpha())
    return (ln, fn)

def _nz_or_blank(v: Optional[float]) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
        return None if abs(f) < 1e-9 else round(f, 3)
    except Exception:
        return None

def _norm_label(s: str) -> str:
    return "".join(ch for ch in str(s).lower() if ch.isalnum())

# ---------- Sierra detection ----------
@dataclass
class SierraLayout:
    header_row: int
    name_idx: int
    hours_idx: int
    rate_idx: Optional[int]
    days_idx: Optional[int]

def _detect_sierra_layout(df: pd.DataFrame) -> Optional[SierraLayout]:
    for r in range(min(60, len(df))):
        row = df.iloc[r].astype(str).str.strip().str.lower()
        if "name" in set(row.values) and "hours" in set(row.values):
            name_idx  = row[row == "name"].index[0]
            hours_idx = row[row == "hours"].index[0]
            rate_idx  = row[row == "rate"].index[0] if any(row == "rate") else None
            days_idx  = row[row == "days"].index[0] if any(row == "days") else None
            return SierraLayout(r, name_idx, hours_idx, rate_idx, days_idx)
    return None

# ---------- Read + aggregate ----------
def _read_sierra_records(xlsx_bytes: bytes) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      agg: DF(Name, Reg_sum, OT_sum, DT_sum, Rate_last)
      per_day: DF(Name, Weekday (0=Mon..6=Sun), Hours, Rate_last)
    """
    bio = io.BytesIO(xlsx_bytes)
    df0 = pd.read_excel(bio, sheet_name=0, header=None)
    layout = _detect_sierra_layout(df0)
    if not layout:
        raise ValueError("Could not detect Sierra header row with 'Name' and 'Hours'.")

    bio.seek(0)
    df = pd.read_excel(bio, sheet_name=0, header=layout.header_row)
    df.columns = [str(c).strip() for c in df.columns]

    name_col  = df.columns[layout.name_idx]
    hours_col = df.columns[layout.hours_idx]
    df = df.rename(columns={name_col: "Name", hours_col: "Hours"})
    if layout.rate_idx is not None:
        rate_name = df.columns[layout.rate_idx]
        if rate_name not in ("Name", "Hours"):
            df = df.rename(columns={rate_name: "Rate"})
    if layout.days_idx is not None:
        days_name = df.columns[layout.days_idx]
        df = df.rename(columns={days_name: "Days"})

    df["Name"] = df["Name"].astype(str).str.strip()
    df = df[df["Name"] != ""].copy()
    df["Hours_num"] = df["Hours"].apply(_num)
    df = df[df["Hours_num"].notnull()].copy()
    df["Rate_num"] = df["Rate"].apply(_num) if "Rate" in df.columns else None

    # Weekday detection: real dates OR weekday text ("Mon", "Tue", ...)
    weekday_map = {
        "monday":0, "mon":0,
        "tuesday":1, "tue":1, "tues":1,
        "wednesday":2, "wed":2,
        "thursday":3, "thu":3, "thur":3, "thurs":3,
        "friday":4, "fri":4,
        "saturday":5, "sat":5,
        "sunday":6, "sun":6
    }

    def to_weekday(val) -> Optional[int]:
        # Try real date
        try:
            d = pd.to_datetime(val, errors="raise")
            return int(d.weekday())
        except Exception:
            pass
        # Try text
        s = str(val).strip().lower()
        letters = "".join(ch for ch in s if ch.isalpha())  # tolerate "Mon." or "Mon – 9/16"
        return weekday_map.get(letters, None)

    if "Days" in df.columns:
        df["Weekday"] = df["Days"].apply(to_weekday)
    else:
        df["Weekday"] = None

    per_day_raw = (df.groupby(["Name", "Weekday"], dropna=False)
                     .agg(Hours=("Hours_num", "sum"),
                          Rate_last=("Rate_num", "last"))
                     .reset_index())

    # Daily split for REG/OT/DT
    def split_daily(h: float) -> Tuple[float, float, float]:
        if h is None:
            return (0.0, 0.0, 0.0)
        reg = min(h, 8.0)
        ot  = min(max(h - 8.0, 0.0), 4.0)
        dt  = max(h - 12.0, 0.0)
        return (reg, ot, dt)

    per_day = per_day_raw.copy()
    per_day[["Reg", "OT", "DT"]] = per_day["Hours"].apply(lambda h: pd.Series(split_daily(h)))

    per_emp = (per_day.groupby("Name", dropna=False)
                        .agg(Reg_sum=("Reg", "sum"),
                             OT_sum=("OT", "sum"),
                             DT_sum=("DT", "sum"),
                             Rate_last=("Rate_last", "last"))
                        .reset_index())

    # Weekly overlay >40
    def weekly_adjust(row):
        total = float((row["Reg_sum"] or 0) + (row["OT_sum"] or 0) + (row["DT_sum"] or 0))
        if total > 40.0:
            extra = total - 40.0
            pull = min(extra, row["Reg_sum"])
            row["Reg_sum"] -= pull
            row["OT_sum"]  += pull
            extra -= pull
            if extra > 0:
                row["OT_sum"] += extra
        return row

    per_emp = per_emp.apply(weekly_adjust, axis=1)
    agg = per_emp[per_emp["Name"].notna() & (per_emp["Name"].astype(str).str.strip() != "")]
    return agg, per_day_raw

# ---------- Roster (OPTIONAL) ----------
def _load_roster() -> pd.DataFrame:
    expected = ["EmpID", "SSN", "Employee Name", "Status", "Type", "PayRate", "Dept"]
    candidates = [
        ROSTER_PATH,
        REPO_ROOT / "roster.xlsx",
        PROJECT_ROOT / "roster.xlsx",
        HERE / "roster.xlsx",
        Path("/roster.xlsx"),
    ]
    path = next((p for p in candidates if p.exists()), None)
    if path is None:
        empty = pd.DataFrame(columns=expected)
        empty["EmpID_clean"] = pd.Series(dtype="Int64")
        empty["SSN_clean"]   = pd.Series(dtype="Int64")
        empty["EmployeeNameRoster"] = pd.Series(dtype="string")
        return empty

    roster = pd.read_excel(path, sheet_name="Roster")
    roster.columns = [str(c).strip() for c in roster.columns]
    missing = set(expected) - set(roster.columns)
    if missing:
        raise ValueError(f"Roster found at {path} but missing columns: {missing}")

    roster["EmpID_clean"] = roster["EmpID"].apply(_safe_int)
    roster["SSN_clean"]   = roster["SSN"].apply(_safe_int)
    roster["EmployeeNameRoster"] = roster["Employee Name"].astype(str)
    return roster

# ---------- assemble computed rows ----------
def _pad_empid(empid: Optional[int]) -> Optional[str]:
    if empid is None:
        return None
    s = str(empid).strip()
    try:
        s = str(int(float(s)))
    except Exception:
        return None
    return s.zfill(10)

def _calc_totals(pay_type: str, reg: float, ot: float, dt: float,
                 rate: Optional[float], default_payrate: Optional[float]) -> float:
    if str(pay_type or "").upper().startswith("S"):
        return float(_num(default_payrate) or 0.0)
    r = _num(rate) or _num(default_payrate) or 0.0
    return float((reg * r) + (ot * 1.5 * r) + (dt * 2.0 * r))

def _build_rows(agg: pd.DataFrame, per_day_hours: pd.DataFrame, roster: pd.DataFrame) -> List[Dict[str, object]]:
    # Per-employee weekday hours pivot (0..6)
    day_pivot = (per_day_hours
                 .assign(Weekday=lambda d: d["Weekday"].apply(lambda x: int(x) if pd.notna(x) else -1))
                 .query("Weekday >= 0 and Weekday <= 6")
                 .pivot_table(index="Name", columns="Weekday", values="Hours", aggfunc="sum", fill_value=0.0))

    joined = _best_join(agg, roster, "Name", "EmployeeNameRoster")
    out: List[Dict[str, object]] = []
    for _, r in joined.iterrows():
        name_src = str(r["Name"]).strip()
        reg = float(r.get("Reg_sum", 0.0) or 0.0)
        ot  = float(r.get("OT_sum", 0.0) or 0.0)
        dt  = float(r.get("DT_sum", 0.0) or 0.0)
        rate_si = r.get("Rate_last")

        empid = r.get("EmpID_clean"); ssn = r.get("SSN_clean")
        name_roster = r.get("EmployeeNameRoster")
        status = r.get("Status", "A"); ptype = r.get("Type", "H")
        payrate_roster = r.get("PayRate"); dept = r.get("Dept")

        name_final = str(name_roster).strip() if pd.notna(name_roster) and str(name_roster).strip() else name_src
        rate_final = _num(payrate_roster) if _num(payrate_roster) is not None else _num(rate_si)
        r_use = _num(rate_final) or 0.0

        # Per-day hours (Mon..Fri indices 0..4)
        h_mon = float(day_pivot.loc[name_src, 0]) if (name_src in day_pivot.index and 0 in day_pivot.columns) else 0.0
        h_tue = float(day_pivot.loc[name_src, 1]) if (name_src in day_pivot.index and 1 in day_pivot.columns) else 0.0
        h_wed = float(day_pivot.loc[name_src, 2]) if (name_src in day_pivot.index and 2 in day_pivot.columns) else 0.0
        h_thu = float(day_pivot.loc[name_src, 3]) if (name_src in day_pivot.index and 3 in day_pivot.columns) else 0.0
        h_fri = float(day_pivot.loc[name_src, 4]) if (name_src in day_pivot.index and 4 in day_pivot.columns) else 0.0

        # Per-day totals
        t_mon = h_mon * r_use
        t_tue = h_tue * r_use
        t_wed = h_wed * r_use
        t_thu = h_thu * r_use
        t_fri = h_fri * r_use

        totals_val = _calc_totals(str(ptype or "H"), reg, ot, dt, rate_final, payrate_roster)

        out.append({
            "EmpID": _pad_empid(empid),
            "SSN": ssn,
            "Employee Name": name_final,
            "Status": status if pd.notna(status) else "A",
            "Type": ptype if pd.notna(ptype) else "H",
            "Dept": dept if pd.notna(dept) else "",
            "Pay Rate": rate_final if rate_final is not None else "",
            "REGULAR": reg,
            "OVERTIME": ot,
            "DOUBLETIME": dt,
            "PC HRS MON": h_mon, "PC HRS TUE": h_tue, "PC HRS WED": h_wed, "PC HRS THU": h_thu, "PC HRS FRI": h_fri,
            "PC TTL MON": t_mon, "PC TTL TUE": t_tue, "PC TTL WED": t_wed, "PC TTL THU": t_thu, "PC TTL FRI": t_fri,
            "Totals": totals_val,
        })
    return out

# ---------- header handling (creates if missing) ----------
REQUIRED_HEADERS = [
    "EmpID", "SSN", "Employee Name", "Status", "Type", "Dept", "Pay Rate",
    "REGULAR", "OVERTIME", "DOUBLETIME",
    "PC HRS MON", "PC TTL MON",
    "PC HRS TUE", "PC TTL TUE",
    "PC HRS WED", "PC TTL WED",
    "PC HRS THU", "PC TTL THU",
    "PC HRS FRI", "PC TTL FRI",
    "Totals"
]

def _ensure_headers(ws: Worksheet, header_row: int) -> Dict[str, int]:
    """
    Ensures a usable header row exists. If the row has no labels,
    this function writes REQUIRED_HEADERS across it.
    Returns a dict of column indices by header label (1-based).
    """
    # Read current headers
    existing = [str(c.value or "").strip() for c in ws[header_row]]
    has_any_label = any(bool(x) for x in existing)

    if not has_any_label:
        # Create required headers from scratch
        for idx, label in enumerate(REQUIRED_HEADERS, start=1):
            ws.cell(row=header_row, column=idx, value=label)
        header_map = {label: i+1 for i, label in enumerate(REQUIRED_HEADERS)}
        return header_map

    # If headers exist, map what we find; create any missing essentials at the end.
    header_map: Dict[str, int] = {}
    # Build quick lookup with normalized labels
    norm_to_idx = {}
    for i, label in enumerate(existing, start=1):
        n = _norm_label(label)
        if n:
            norm_to_idx[n] = i

    def find_or_create(label: str) -> int:
        n = _norm_label(label)
        if n in norm_to_idx:
            return norm_to_idx[n]
        # append at the end
        col = (len(existing) if existing else 0) + 1 + len([k for k in header_map if k not in existing])
        ws.cell(row=header_row, column=col, value=label)
        norm_to_idx[n] = col
        return col

    for label in REQUIRED_HEADERS:
        header_map[label] = find_or_create(label)

    return header_map

# ---------- main ----------
def sierra_excel_to_wbs_bytes(xlsx_bytes: bytes) -> bytes:
    if not WBS_TEMPLATE_PATH.exists():
        # If template truly missing, create a new workbook with WEEKLY and headers
        wb = Workbook()
        ws = wb.active
        ws.title = "WEEKLY"
        header_row = 8
        header_map = _ensure_headers(ws, header_row)
    else:
        wb = load_workbook(WBS_TEMPLATE_PATH)
        ws: Worksheet = wb["WEEKLY"] if "WEEKLY" in wb.sheetnames else next((wb[s] for wb_s in wb.sheetnames for s in [wb_s] if "week" in s.lower()), wb.active)

        # Locate header row: scan rows 6–12; pick row with most labels
        header_row = None
        best_score = -1
        for r in range(6, 13):
            vals = [str(c.value or "").strip() for c in ws[r]]
            score = sum(1 for v in vals if v)
            if score > best_score:
                best_score = score
                header_row = r
        if header_row is None:
            header_row = 8

        # Ensure headers exist (and create missing essentials if needed)
        header_map = _ensure_headers(ws, header_row)

    DATA_START = header_row + 1

    # Read + compute
    agg, per_day_hours = _read_sierra_records(xlsx_bytes)
    roster = _load_roster()
    rows = _build_rows(agg, per_day_hours, roster)

    # Capture existing order (fixed top block)
    existing_order: List[str] = []
    name_col_idx = header_map.get("Employee Name")
    if name_col_idx:
        for rr in range(DATA_START, ws.max_row + 1):
            val = ws.cell(row=rr, column=name_col_idx).value
            if val and str(val).strip():
                existing_order.append(str(val).strip())

    # Clear old data
    if ws.max_row >= DATA_START:
        ws.delete_rows(DATA_START, ws.max_row - DATA_START + 1)

    # Ordering: existing first, others A–Z by last name
    existing_index = {name: idx for idx, name in enumerate(existing_order)} if existing_order else {}

    def sort_key(row: Dict[str, object]) -> Tuple[int, Tuple[str, str]]:
        nm = str(row["Employee Name"] or "").strip()
        pri = existing_index.get(nm, 10**9)
        return (pri, _last_name_key(nm))

    rows_sorted = sorted(rows, key=sort_key)

    # Column indices
    col = header_map  # alias

    # Write rows
    r = DATA_START
    for row in rows_sorted:
        if col.get("EmpID"):        ws.cell(row=r, column=col["EmpID"],        value=row["EmpID"])
        if col.get("SSN"):          ws.cell(row=r, column=col["SSN"],          value=row["SSN"])
        if col.get("Employee Name"):ws.cell(row=r, column=col["Employee Name"],value=row["Employee Name"])
        if col.get("Status"):       ws.cell(row=r, column=col["Status"],       value=row["Status"])
        if col.get("Type"):         ws.cell(row=r, column=col["Type"],         value=row["Type"])
        if col.get("Dept"):         ws.cell(row=r, column=col["Dept"],         value=row["Dept"])
        if col.get("Pay Rate"):     ws.cell(row=r, column=col["Pay Rate"],     value=row["Pay Rate"] if row["Pay Rate"] != "" else None)

        if col.get("REGULAR"):      ws.cell(row=r, column=col["REGULAR"],      value=_nz_or_blank(row["REGULAR"]))
        if col.get("OVERTIME"):     ws.cell(row=r, column=col["OVERTIME"],     value=_nz_or_blank(row["OVERTIME"]))
        if col.get("DOUBLETIME"):   ws.cell(row=r, column=col["DOUBLETIME"],   value=_nz_or_blank(row["DOUBLETIME"]))

        if col.get("PC HRS MON"):   ws.cell(row=r, column=col["PC HRS MON"],   value=_nz_or_blank(row["PC HRS MON"]))
        if col.get("PC HRS TUE"):   ws.cell(row=r, column=col["PC HRS TUE"],   value=_nz_or_blank(row["PC HRS TUE"]))
        if col.get("PC HRS WED"):   ws.cell(row=r, column=col["PC HRS WED"],   value=_nz_or_blank(row["PC HRS WED"]))
        if col.get("PC HRS THU"):   ws.cell(row=r, column=col["PC HRS THU"],   value=_nz_or_blank(row["PC HRS THU"]))
        if col.get("PC HRS FRI"):   ws.cell(row=r, column=col["PC HRS FRI"],   value=_nz_or_blank(row["PC HRS FRI"]))

        if col.get("PC TTL MON"):   ws.cell(row=r, column=col["PC TTL MON"],   value=_nz_or_blank(row["PC TTL MON"]))
        if col.get("PC TTL TUE"):   ws.cell(row=r, column=col["PC TTL TUE"],   value=_nz_or_blank(row["PC TTL TUE"]))
        if col.get("PC TTL WED"):   ws.cell(row=r, column=col["PC TTL WED"],   value=_nz_or_blank(row["PC TTL WED"]))
        if col.get("PC TTL THU"):   ws.cell(row=r, column=col["PC TTL THU"],   value=_nz_or_blank(row["PC TTL THU"]))
        if col.get("PC TTL FRI"):   ws.cell(row=r, column=col["PC TTL FRI"],   value=_nz_or_blank(row["PC TTL FRI"]))

        if col.get("Totals"):       ws.cell(row=r, column=col["Totals"],       value=_nz_or_blank(row["Totals"]))

        r += 1

    # DEBUG sheet
    if "DEBUG" in wb.sheetnames:
        wb.remove(wb["DEBUG"])
    dbg = wb.create_sheet("DEBUG")
    dbg.append([f"Header row: {header_row}"])
    dbg.append(["Headers snapshot (label → col)"])
    for k in REQUIRED_HEADERS:
        dbg.append([k, header_map.get(k, None)])
    dbg.append([])
    dbg.append(["First 20 rows preview (Name, Rate, REG, OT, DT, HRS Mon–Fri, TTL Mon–Fri, Totals)"])
    for row in rows_sorted[:20]:
        dbg.append([
            row["Employee Name"], row["Pay Rate"],
            row["REGULAR"], row["OVERTIME"], row["DOUBLETIME"],
            row["PC HRS MON"], row["PC HRS TUE"], row["PC HRS WED"], row["PC HRS THU"], row["PC HRS FRI"],
            row["PC TTL MON"], row["PC TTL TUE"], row["PC TTL WED"], row["PC TTL THU"], row["PC TTL FRI"],
            row["Totals"]
        ])

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.read()
