# server/main.py
import io, os, re, unicodedata
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse, JSONResponse
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet

app = FastAPI(title="Sierra → WBS Payroll Converter", version="7.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ALLOWED_EXTS = (".xlsx", ".xls")

# ---------------- file discovery ----------------
BASE_DIR = Path(__file__).resolve().parents[1]
SEARCH_DIRS = [BASE_DIR, BASE_DIR / "app", BASE_DIR / "app" / "data", BASE_DIR / "server"]

def _find_file(basenames: List[str]) -> Optional[Path]:
    env_map = {"wbs_template.xlsx": "WBS_TEMPLATE_PATH", "roster.xlsx": "ROSTER_PATH", "roster.csv": "ROSTER_PATH"}
    for name in basenames:
        env_key = env_map.get(name)
        if env_key:
            p = os.getenv(env_key)
            if p and Path(p).exists():
                return Path(p)
    for d in SEARCH_DIRS:
        for name in basenames:
            p = d / name
            if p.exists():
                return p
    return None

# ---------------- helpers ----------------
def _std(s: str) -> str:
    return (s or "").strip().lower()

def _ext_ok(name: str) -> bool:
    n = (name or "").lower()
    return any(n.endswith(e) for e in ALLOWED_EXTS)

def _clean_space(s: str) -> str:
    return re.sub(r"\s+", " ", str(s).strip())

def _name_parts(name: str):
    name = _clean_space(name)
    if "," in name:
        last, first = name.split(",", 1)
        return last.strip(), first.strip()
    parts = name.split(" ")
    if len(parts) >= 2:
        last = parts[-1].strip()
        first = " ".join(parts[:-1]).strip()
        return last, first
    return name.strip(), ""

def _key_last_first(last: str, first: str) -> str:
    return f"{last},{first}".lower()

def _canon_name(s: str) -> str:
    s = _clean_space(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.lower()

def _to_date(v) -> Optional[date]:
    if pd.isna(v): return None
    try: return pd.to_datetime(v).date()
    except Exception: return None

def _money(x: float) -> float:
    try: return float(x or 0.0)
    except Exception: return 0.0

def _find_col(df: pd.DataFrame, options: List[str]) -> Optional[str]:
    norm = { _std(c): c for c in df.columns }
    for want in options:
        if _std(want) in norm: return norm[_std(want)]
    for want in options:
        for k,c in norm.items():
            if _std(want) in k: return c
    return None

# ---------------- roster & template ----------------
def _load_template_from_disk() -> bytes:
    p = _find_file(["wbs_template.xlsx"])
    if not p:
        raise HTTPException(422, detail="WBS template not found in repo root.")
    return p.read_bytes()

def _load_roster_df() -> Optional[pd.DataFrame]:
    p = _find_file(["roster.xlsx","roster.csv"])
    if not p: return None
    if p.suffix.lower()==".xlsx": df = pd.read_excel(p,dtype=str)
    else: df = pd.read_csv(p,dtype=str)
    if df.empty: return None

    name_col = _find_col(df,["employee name","employee","name"])
    ssn_col  = _find_col(df,["ssn"])
    rate_col = _find_col(df,["payrate","rate","hourly"])
    dept_col = _find_col(df,["dept","department"])
    type_col = _find_col(df,["type"])

    out = pd.DataFrame({
        "employee_disp": df[name_col].astype(str).map(_clean_space),
        "employee_key":  df[name_col].astype(str).map(_canon_name),
        "ssn": df[ssn_col] if ssn_col else "",
        "rate_roster": pd.to_numeric(df[rate_col], errors="coerce") if rate_col else None,
        "department_roster": df[dept_col] if dept_col else "",
        "wtype_roster": df[type_col] if type_col else "",
    })
    return out

# ---------------- build weekly ----------------
def _ca_daily_ot(h: float) -> Dict[str,float]:
    h=float(h or 0.0)
    reg=min(h,8.0); ot=min(max(h-8.0,0.0),4.0); dt=max(h-12.0,0.0)
    return {"REG":reg,"OT":ot,"DT":dt}

def build_weekly_from_sierra(xlsx_bytes: bytes, sheet_name: Optional[str]=None) -> pd.DataFrame:
    excel=pd.ExcelFile(io.BytesIO(xlsx_bytes))
    sheet=sheet_name or excel.sheet_names[0]
    df=excel.parse(sheet)

    emp_col=_find_col(df,["employee","employee name","name"])
    rate_col=_find_col(df,["rate","pay rate","hourly"])
    if not emp_col or not rate_col:
        raise ValueError("Missing employee or rate column")

    base=pd.DataFrame({
        "employee":df[emp_col].astype(str).map(_clean_space),
        "rate":pd.to_numeric(df[rate_col],errors="coerce").fillna(0.0),
    })
    base["emp_key"]=base["employee"].map(_canon_name)

    # detect Mon-Sun
    days=["mon","tue","wed","thu","fri","sat","sun"]
    day_cols={d:_find_col(df,[d,d+"day"]) for d in days if _find_col(df,[d,d+"day"])}

    per_day=[]
    for d,c in day_cols.items():
        hrs=pd.to_numeric(df[c],errors="coerce").fillna(0.0)
        tmp=pd.DataFrame({
            "emp_key":base["emp_key"],
            "employee":base["employee"],
            "date":d,
            "hours":hrs,
            "rate":base["rate"],
        })
        per_day.append(tmp[tmp["hours"]>0])

    if not per_day: raise ValueError("No hours found in Sierra file")
    core=pd.concat(per_day)

    # split daily OT
    parts=[]
    for (k,emp,day),g in core.groupby(["emp_key","employee","date"]):
        tot=g["hours"].sum()
        s=_ca_daily_ot(tot)
        parts.append({"emp_key":k,"employee":emp,"REG":s["REG"],"OT":s["OT"],"DT":s["DT"]})
    split=pd.DataFrame(parts)

    weekly=split.groupby(["emp_key","employee"])[["REG","OT","DT"]].sum().reset_index()

    # dollars
    dollars=defaultdict(lambda:{"REG_$":0,"OT_$":0,"DT_$":0})
    for _,r in core.iterrows():
        s=_ca_daily_ot(r["hours"])
        dollars[r["emp_key"]]["REG_$"]+=s["REG"]*r["rate"]
        dollars[r["emp_key"]]["OT_$"]+=s["OT"]*r["rate"]*1.5
        dollars[r["emp_key"]]["DT_$"]+=s["DT"]*r["rate"]*2

    weekly["rate"]=weekly["emp_key"].map(lambda k:base.loc[base["emp_key"]==k,"rate"].max())
    weekly["REG_$"]=weekly["emp_key"].map(lambda k:round(dollars[k]["REG_$"],2))
    weekly["OT_$"]=weekly["emp_key"].map(lambda k:round(dollars[k]["OT_$"],2))
    weekly["DT_$"]=weekly["emp_key"].map(lambda k:round(dollars[k]["DT_$"],2))
    weekly["TOTAL_$"]=weekly["REG_$"]+weekly["OT_$"]+weekly["DT_$"]
    weekly["ssn"]=""; weekly["Status"]="A"; weekly["Type"]="H"; weekly["department"]=""

    return weekly

# ---------------- template write ----------------
def _find_wbs_header(ws: Worksheet) -> Tuple[int, Dict[str,int]]:
    for r in range(1,ws.max_row+1):
        vals=[_std(str(ws.cell(r,c).value)) for c in range(1,ws.max_column+1)]
        if "employee name" in vals and "a01" in vals:
            col_map={v:c for c,v in enumerate(vals,start=1)}
            return r,col_map
    raise HTTPException(422,"Could not find WBS header row")

def write_into_wbs_template(template_bytes: bytes, weekly: pd.DataFrame) -> bytes:
    wb=load_workbook(io.BytesIO(template_bytes))
    ws=wb["WEEKLY"] if "WEEKLY" in wb.sheetnames else wb.active
    header_row,cols=_find_wbs_header(ws)
    first_data_row=header_row+1

    # clear old
    ws.delete_rows(first_data_row, ws.max_row-first_data_row+1)

    for _,row in weekly.iterrows():
        vals=[""]*ws.max_column
        vals[cols.get("employee name")-1]=row["employee"]
        vals[cols.get("ssn")-1]=row["ssn"]
        vals[cols.get("status")-1]=row["Status"]
        vals[cols.get("type")-1]=row["Type"]
        vals[cols.get("pay rate")-1]=row["rate"]
        vals[cols.get("a01")-1]=row["REG"]
        vals[cols.get("a02")-1]=row["OT"]
        vals[cols.get("a03")-1]=row["DT"]
        vals[cols.get("reg $")-1]=row["REG_$"]
        vals[cols.get("ot $")-1]=row["OT_$"]
        vals[cols.get("dt $")-1]=row["DT_$"]
        vals[cols.get("total $")-1]=row["TOTAL_$"]
        ws.append(vals)

    # totals row
    ws.append([])
    total=weekly[["REG","OT","DT","REG_$","OT_$","DT_$","TOTAL_$"]].sum()
    vals=[""]*ws.max_column
    vals[cols.get("employee name")-1]="TOTAL"
    vals[cols.get("a01")-1]=total["REG"]
    vals[cols.get("a02")-1]=total["OT"]
    vals[cols.get("a03")-1]=total["DT"]
    vals[cols.get("reg $")-1]=total["REG_$"]
    vals[cols.get("ot $")-1]=total["OT_$"]
    vals[cols.get("dt $")-1]=total["DT_$"]
    vals[cols.get("total $")-1]=total["TOTAL_$"]
    ws.append(vals)

    bio=io.BytesIO(); wb.save(bio); bio.seek(0)
    return bio.read()

# ---------------- routes ----------------
@app.get("/health")
def health(): return {"ok":True}

@app.post("/process-payroll")
async def process(file:UploadFile=File(...)):
    if not _ext_ok(file.filename): raise HTTPException(415,"Use .xlsx/.xls")
    data=await file.read()
    weekly=build_weekly_from_sierra(data)
    tmpl=_load_template_from_disk()
    out=write_into_wbs_template(tmpl,weekly)
    return StreamingResponse(io.BytesIO(out),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition":f'attachment; filename=WBS_Payroll_{datetime.utcnow().date()}.xlsx"})
