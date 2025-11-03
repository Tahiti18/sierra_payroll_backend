"""
Sierra → WBS translator
• Computes hours directly from Start / Lnch St. / Lnch Fnsh / Finish
• Applies California rule: 8h REG, next 4h OT, beyond 12h DT
• Weekly overlay: anything >40h becomes OT
• Pulls pay data from roster.xlsx if present (same folder)
• Writes numeric only, no formulas or colors
"""

from __future__ import annotations
import io, re
from pathlib import Path
from typing import Optional, Tuple, List
import pandas as pd
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

ROSTER_PATH = Path("roster.xlsx")
WBS_TEMPLATE_PATH = Path("wbs_template.xlsx")

# ------------------------------------------------------------ helpers
def _num(x):
    if x is None: return None
    if isinstance(x, (int, float)): return float(x)
    try:
        s = str(x).strip().replace("$","").replace(",","")
        return float(re.search(r"-?\d+(\.\d+)?", s).group()) if re.search(r"-?\d+(\.\d+)?", s) else None
    except: return None

def _safe_int(x):
    try: return int(float(x))
    except: return None

def _hours_from_times(row):
    try:
        st, l1, l2, fn = row["Start"], row["Lnch St."], row["Lnch Fnsh"], row["Finish"]
        to_min = lambda t: None if pd.isna(t) else (int(str(t).split(":")[0])*60 + int(str(t).split(":")[1]))
        times = [to_min(v) for v in (st,l1,l2,fn)]
        if None in times: return 0.0
        work = (times[3]-times[0]) - max(0, times[2]-times[1])
        return max(work/60, 0)
    except: return 0.0

def _split_daily(h):
    reg = min(h, 8.0)
    ot  = min(max(h-8,0), 4)
    dt  = max(h-12,0)
    return reg, ot, dt

# ------------------------------------------------------------ main read
def _read_sierra(xlsx: bytes):
    df = pd.read_excel(io.BytesIO(xlsx))
    cols = [c.strip() for c in df.columns]
    df.columns = cols
    if "Hours" not in cols:
        df["Hours"] = df.apply(_hours_from_times, axis=1)
    df = df[df["Name"].notna()]
    daily = df.groupby(["Name","Days"], dropna=False)["Hours"].sum().reset_index()
    daily[["REG","OT","DT"]] = daily["Hours"].apply(lambda h: pd.Series(_split_daily(h)))
    weekly = daily.groupby("Name", dropna=False)[["REG","OT","DT"]].sum().reset_index()

    def weekly_overlay(row):
        total = row.REG + row.OT + row.DT
        if total > 40:
            extra = total - 40
            shift = min(extra, row.REG)
            row.REG -= shift
            row.OT  += shift
        return row

    weekly = weekly.apply(weekly_overlay, axis=1)
    rate_map = df.groupby("Name")["Rate"].last().apply(_num)
    weekly["Rate"] = weekly["Name"].map(rate_map)
    return weekly

# ------------------------------------------------------------ roster
def _load_roster():
    if not ROSTER_PATH.exists():
        return pd.DataFrame(columns=["Employee Name","SSN","EmpID","Status","Type","Dept","PayRate"])
    try:
        r = pd.read_excel(ROSTER_PATH)
    except: return pd.DataFrame()
    r.columns = [c.strip() for c in r.columns]
    return r

# ------------------------------------------------------------ write
def sierra_excel_to_wbs_bytes(xlsx: bytes) -> bytes:
    agg = _read_sierra(xlsx)
    roster = _load_roster()

    wb = load_workbook(WBS_TEMPLATE_PATH)
    ws = wb.active
    hdr_row = 8
    data_row = hdr_row + 1

    # find headers
    headers = [str(c.value or "").strip().lower() for c in ws[hdr_row]]
    def find(colname): 
        for i,v in enumerate(headers,1):
            if colname.lower() in v: return i
        return None

    empid_col = find("emp id") or find("empid")
    ssn_col = find("ssn")
    name_col = find("employee name") or find("name")
    pay_col = find("pay rate") or find("rate")
    reg_col = find("regular") or find("a01") or pay_col+1
    ot_col  = find("overtime") or find("a02") or reg_col+1
    dt_col  = find("double") or find("a03") or ot_col+1
    tot_col = find("total") or dt_col+1

    # clear old data
    if ws.max_row > data_row:
        ws.delete_rows(data_row, ws.max_row - data_row + 1)

    r = data_row
    for _, row in agg.iterrows():
        name = row.Name
        reg,ot,dt = row.REG,row.OT,row.DT
        rate = _num(row.Rate)
        total = reg*rate + ot*1.5*rate + dt*2*rate
        match = roster[roster["Employee Name"].astype(str).str.strip().str.lower()==name.strip().lower()]
        empid = match["EmpID"].iloc[0] if not match.empty else None
        ssn   = match["SSN"].iloc[0] if not match.empty else None
        payr  = _num(match["PayRate"].iloc[0]) if "PayRate" in match else rate
        status = match["Status"].iloc[0] if "Status" in match else "A"
        ptype  = match["Type"].iloc[0] if "Type" in match else "H"
        dept   = match["Dept"].iloc[0] if "Dept" in match else ""

        if empid_col: ws.cell(r, empid_col, empid)
        if ssn_col:   ws.cell(r, ssn_col, ssn)
        if name_col:  ws.cell(r, name_col, name if not match.empty else f"{name} (NOT FOUND)")
        if pay_col:   ws.cell(r, pay_col, payr)
        if reg_col:   ws.cell(r, reg_col, round(reg,3) if reg else None)
        if ot_col:    ws.cell(r, ot_col, round(ot,3) if ot else None)
        if dt_col:    ws.cell(r, dt_col, round(dt,3) if dt else None)
        if tot_col:   ws.cell(r, tot_col, round(total,2) if total else None)
        r += 1

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.read()
