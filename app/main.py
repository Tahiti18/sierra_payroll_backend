# app/main.py
from __future__ import annotations
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from io import BytesIO
from typing import Dict, Any, Optional, Tuple
from datetime import datetime
import re

import pandas as pd
from openpyxl import load_workbook, Workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment, Font, Border, Side, PatternFill

app = FastAPI(title="Sierra Payroll Backend (Real)")

# CORS: open while stabilizing
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten later if needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ======== GLOBAL ROSTER CACHE (in-memory) ========
ROSTER_BYTES: Optional[bytes] = None
ROSTER_MAP: Dict[str, Dict[str, Any]] = {}  # key = lower(name)

# ======== CONSTANTS ========
WBS_HEADERS = [
    "SSN","Employee Name","Status","Type","Pay Rate","Dept",
    "A01","A02","A03","A06","A07","A08","A04","A05",
    "AH1","AI1","AH2","AI2","AH3","AI3","AH4","AI4","AH5","AI5",
    "ATE","Comments","Totals"
]

# ======== HELPERS ========
def norm(s: Any) -> str:
    return "" if s is None else str(s).strip()

def to_num(v: Any) -> float:
    if v is None or v == "": return 0.0
    if isinstance(v, (int, float)): return float(v)
    try:
        return float(re.sub(r"[^0-9.\-]", "", str(v)))
    except:  # noqa: E722
        return 0.0

def is_date(v: Any) -> bool:
    return isinstance(v, (datetime, pd.Timestamp))

def weekday_headers(day: datetime) -> Tuple[Optional[str], Optional[str]]:
    # Mon..Fri → (AHx, AIx) pairs
    wd = day.weekday()
    return {
        0: ("AH1","AI1"),
        1: ("AH2","AI2"),
        2: ("AH3","AI3"),
        3: ("AH4","AI4"),
        4: ("AH5","AI5"),
    }.get(wd, (None, None))

def build_roster_map_from_bytes(roster_bytes: Optional[bytes]) -> Dict[str, Dict[str, Any]]:
    m: Dict[str, Dict[str, Any]] = {}
    if not roster_bytes: return m
    xr = pd.ExcelFile(BytesIO(roster_bytes))
    df = xr.parse(xr.sheet_names[0])
    cols = {c.lower(): c for c in df.columns}
    def col(*names):
        for n in names:
            if n.lower() in cols: return cols[n.lower()]
        return None
    c_name = col("name","employee name")
    c_ssn  = col("ssn","social security","social security number")
    c_stat = col("status")
    c_type = col("type","pay type")
    c_rate = col("pay rate","rate")
    c_dept = col("dept","department")

    for _, r in df.iterrows():
        name = norm(r.get(c_name,""))
        if not name: continue
        m[name.lower()] = {
            "ssn":    norm(r.get(c_ssn,"")),
            "status": norm(r.get(c_stat,"")),
            "type":   norm(r.get(c_type,"")),
            "pay_rate": to_num(r.get(c_rate,0)),
            "dept":   norm(r.get(c_dept,"")),
        }
    return m

def find_sierra_table(wb) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # Scan sheets to find header row with Days | Name | Hours | Rate | Total (flexible)
    for sh in wb.worksheets:
        max_row, max_col = sh.max_row, sh.max_column
        vals = [[sh.cell(r,c).value for c in range(1,max_col+1)] for r in range(1,max_row+1)]
        fills= [[sh.cell(r,c).fill  for c in range(1,max_col+1)] for r in range(1,max_row+1)]

        hdr_idx = -1
        for i,row in enumerate(vals[:80]):
            low = [norm(x).lower() for x in row]
            if {"days","name","hours","rate","total"} <= set(low):
                hdr_idx = i; break
            if all(k in low for k in ["days","job#","name","hours","rate"]):
                hdr_idx = i; break
        if hdr_idx < 0:
            for i,row in enumerate(vals[:80]):
                low = " ".join([norm(x).lower() for x in row])
                if "days" in low and "name" in low and "hours" in low and "total" in low:
                    hdr_idx = i; break
        if hdr_idx < 0:
            continue

        headers = [norm(v) for v in vals[hdr_idx]]
        table   = vals[hdr_idx+1:]
        df = pd.DataFrame(table, columns=headers)

        # background hex (for future green detection if needed)
        def fill_hex(f):
            try:
                if f.fill_type != "solid": return ""
                rgb = (f.start_color.rgb or "").upper()
                if not rgb: return ""
                if len(rgb)==8: rgb = rgb[-6:]
                return "#"+rgb
            except:  # noqa: E722
                return ""
        bg = [[fill_hex(f) for f in row] for row in fills[hdr_idx+1:]]
        bg_df = pd.DataFrame(bg, columns=headers)
        return df, bg_df

    raise ValueError("Could not detect Sierra header row (need columns like Days | Name | Hours | Rate | Total).")

def convert_sierra_to_wbs(sierra_bytes: bytes, roster_bytes: Optional[bytes]) -> bytes:
    # roster precedence: incoming roster overrides cached; else use cached; else empty
    roster_map = build_roster_map_from_bytes(roster_bytes) if roster_bytes else (ROSTER_MAP or {})
    wb = load_workbook(BytesIO(sierra_bytes), data_only=True)
    df, _bg = find_sierra_table(wb)

    cols = {c.lower(): c for c in df.columns}
    def col(*names):
        for n in names:
            if n and n.lower() in cols: return cols[n.lower()]
        return None

    c_day   = col("days","day")
    c_name  = col("name","employee name")
    c_hours = col("hours","hrs")
    c_rate  = col("rate","pay rate")
    c_total = col("total","amount","gross")
    c_det   = col("job detail","job details","details")

    if c_name is None or c_hours is None or c_total is None:
        raise ValueError("Sierra sheet missing required columns (Name, Hours, Total).")

    # Aggregate by employee
    agg: Dict[str, Dict[str, Any]] = {}
    for _, row in df.iterrows():
        name = norm(row.get(c_name,""))
        if not name:
            continue

        hours = to_num(row.get(c_hours,0))
        rate  = to_num(row.get(c_rate,0))
        total = to_num(row.get(c_total,0))
        detail= norm(row.get(c_det,""))

        # Day parsing (avoid 1899)
        day_v = row.get(c_day,"")
        day: Optional[datetime] = None
        if is_date(day_v):
            try:
                day = pd.to_datetime(day_v).to_pydatetime()
            except Exception:
                day = None
        else:
            try:
                day = datetime.strptime(str(day_v), "%m/%d/%Y")
            except Exception:
                day = None

        key = name.lower()
        if key not in agg:
            ro = roster_map.get(key, {})
            agg[key] = {
                "ssn": ro.get("ssn",""),
                "name": name,
                "status": ro.get("status",""),
                "type": ro.get("type",""),
                "pay_rate": ro.get("pay_rate", rate if rate>0 else 0.0),
                "dept": ro.get("dept",""),
                "A01":0.0,"A02":0.0,"A03":0.0,"A06":0.0,"A07":0.0,"A08":0.0,
                "A04":0.0,"A05":0.0,
                "AH1":0.0,"AI1":0.0,"AH2":0.0,"AI2":0.0,"AH3":0.0,"AI3":0.0,"AH4":0.0,"AI4":0.0,"AH5":0.0,"AI5":0.0,
                "ATE":0.0,
                "comments": "",
                "total": 0.0,
            }
        g = agg[key]

        # fill pay_rate if missing
        if g["pay_rate"] == 0 and rate>0:
            g["pay_rate"] = rate

        # accumulate total $
        g["total"] += total

        txt = detail.lower()
        is_vac = "vacation" in txt
        is_sick= "sick" in txt
        is_hol = "holiday" in txt
        is_bonus = "bonus" in txt
        is_comm  = "commission" in txt
        is_travel= "travel" in txt

        # piecework (hours>0, rate==0, total>0)
        is_piece = (hours>0 and rate==0 and total>0)

        if is_bonus:
            g["A04"] += total
        elif is_comm:
            g["A05"] += total
        elif is_travel:
            g["ATE"] += total
        elif is_vac:
            g["A06"] += hours
        elif is_sick:
            g["A07"] += hours
        elif is_hol:
            g["A08"] += hours
        elif is_piece:
            if day:
                hh, tt = weekday_headers(day)
                if hh and tt:
                    g[hh] += hours
                    g[tt] += total
                else:
                    g["comments"] = (g["comments"] + " " if g["comments"] else "") + f"Piecework ${total:.2f}"
            else:
                g["comments"] = (g["comments"] + " " if g["comments"] else "") + f"Piecework ${total:.2f}"
        else:
            # Regular split: 0–8 REG (A01), 8–12 OT (A02), >12 DT (A03)
            h = max(0.0, hours)
            if h > 12:
                g["A01"] += 8
                g["A02"] += 4
                g["A03"] += h - 12
            elif h > 8:
                g["A01"] += 8
                g["A02"] += h - 8
            else:
                g["A01"] += h

    # Build output workbook with exact headers
    out = Workbook()
    sh = out.active
    sh.title = "Payroll_Output"
    sh.append(WBS_HEADERS)

    for _, g in agg.items():
        sh.append([
            g["ssn"], g["name"], g["status"], g["type"], g["pay_rate"], g["dept"],
            g["A01"], g["A02"], g["A03"], g["A06"], g["A07"], g["A08"], g["A04"], g["A05"],
            g["AH1"], g["AI1"], g["AH2"], g["AI2"], g["AH3"], g["AI3"], g["AH4"], g["AI4"], g["AH5"], g["AI5"],
            g["ATE"], g["comments"], g["total"]
        ])

    # Totals row (blank row before + border)
    last_data = sh.max_row
    totals_row = last_data + 1
    # label in B
    sh.cell(totals_row, 2, "Totals").font = Font(bold=True)

    # number formats & totals per column (money vs hours)
    money_cols = set([5,13,14,16,18,20,22,24,25,27])  # PayRate, A04,A05, AI1..AI5, ATE, Totals
    header_idx = {h:i+1 for i,h in enumerate(WBS_HEADERS)}

    # per-cell formats for all data rows
    if last_data >= 2:
        # hours: A01,A02,A03,A06,A07,A08, AH1,AH2,AH3,AH4,AH5 (not AIx)
        hour_cols = [
            header_idx["A01"], header_idx["A02"], header_idx["A03"],
            header_idx["A06"], header_idx["A07"], header_idx["A08"],
            header_idx["AH1"], header_idx["AH2"], header_idx["AH3"],
            header_idx["AH4"], header_idx["AH5"],
        ]
        for c in hour_cols:
            for r in range(2, last_data+1):
                sh.cell(r, c).number_format = '0.00'
        # money: pay rate, AI1..AI5, A04,A05, ATE, Totals
        money_cols_all = [
            header_idx["Pay Rate"], header_idx["A04"], header_idx["A05"],
            header_idx["AI1"], header_idx["AI2"], header_idx["AI3"],
            header_idx["AI4"], header_idx["AI5"], header_idx["ATE"], header_idx["Totals"]
        ]
        for c in money_cols_all:
            for r in range(2, last_data+1):
                sh.cell(r, c).number_format = '"$"#,##0.00'

    # totals formulas (sum down each numeric col)
    for col in range(5, len(WBS_HEADERS)+1):  # start at Pay Rate for consistency
        L = get_column_letter(col)
        # Sum only numeric columns (we sum anyway; non-numeric will be $0.00 or 0.00)
        sh.cell(totals_row, col, f"=SUM({L}2:{L}{last_data})")
        if col in money_cols or WBS_HEADERS[col-1] == "Totals":
            sh.cell(totals_row, col).number_format = '"$"#,##0.00'
        else:
            sh.cell(totals_row, col).number_format = '0.00'

    # header styling & widths & freeze & border above totals
    header_fill = PatternFill("solid", fgColor="DDDDDD")
    for c in range(1, len(WBS_HEADERS)+1):
        cell = sh.cell(1, c)
        cell.font = Font(bold=True)
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center", vertical="center")
    # widen important columns
    widths = {1:14, 2:28, 3:10, 4:10, 5:12, 6:10}
    for c,w in widths.items():
        sh.column_dimensions[get_column_letter(c)].width = w
    sh.column_dimensions[get_column_letter(WBS_HEADERS.index("Comments")+1)].width = 32
    sh.column_dimensions[get_column_letter(WBS_HEADERS.index("Totals")+1)].width = 14
    # border line on totals row
    thin = Side(style="thin", color="000000")
    for c in range(1, len(WBS_HEADERS)+1):
        sh.cell(totals_row, c).border = Border(top=thin)
    # freeze pane under header
    sh.freeze_panes = "A2"

    bio = BytesIO()
    out.save(bio)
    bio.seek(0)
    return bio.getvalue()

# ======== ROUTES ========
@app.get("/health")
def health():
    return {"ok": True}

@app.post("/roster")
async def upload_roster(roster_file: UploadFile = File(...)):
    global ROSTER_BYTES, ROSTER_MAP
    if not roster_file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Roster file must be .xlsx")
    data = await roster_file.read()
    try:
        ROSTER_MAP = build_roster_map_from_bytes(data)
        ROSTER_BYTES = data
        return {"ok": True, "employees": len(ROSTER_MAP)}
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Failed to parse roster: {e}")

@app.get("/employees")
def get_employees():
    # return ALL employees (no limiting)
    if not ROSTER_MAP:
        return []
    out = []
    for k, v in ROSTER_MAP.items():
        out.append({
            "name": v.get("name") or k,  # k is lower(name)
            "ssn": v.get("ssn",""),
            "department": v.get("dept",""),
            "pay_rate": v.get("pay_rate",0),
            "status": v.get("status",""),
            "type": v.get("type",""),
        })
    # restore proper case for name when cached (store original if present)
    for e in out:
        if e["name"] == e["name"].lower():
            # title-case fallback; better is to store original name in cache
            e["name"] = " ".join([w.capitalize() for w in e["name"].split()])
    return out

@app.post("/process-payroll")
async def process_payroll(
    sierra_file: UploadFile = File(...),
    roster_file: UploadFile | None = File(None),
):
    if not sierra_file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Sierra file must be .xlsx")
    try:
        sierra_bytes = await sierra_file.read()
        roster_bytes = await roster_file.read() if roster_file is not None else None

        # If a roster is included here, update cache so /employees also shows it
        if roster_bytes:
            global ROSTER_BYTES, ROSTER_MAP
            ROSTER_MAP = build_roster_map_from_bytes(roster_bytes)
            ROSTER_BYTES = roster_bytes

        out_bytes = convert_sierra_to_wbs(sierra_bytes, roster_bytes)
        return StreamingResponse(
            BytesIO(out_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": 'attachment; filename="WBS_Payroll.xlsx"'},
        )
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"{type(e).__name__}: {e}"})
