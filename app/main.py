# app/main.py
from __future__ import annotations

import io
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet
from starlette.responses import StreamingResponse

app = FastAPI(title="Sierra → WBS Converter")

# CORS (frontend on Netlify)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ────────────────────────────────────────────────────────────────────────────────
# HARD-CODED EMPLOYEE ORDER (name, SSN) from your Gold Master: 
# “WBS Payroll 9_19_25 for Marwan 2.xlsx” – WEEKLY sheet
# Keep this list as the single source of truth for row ordering.
EMP_ORDER: List[Tuple[str, str]] = [
    ('Robleza, Dianne', '626946016'),
    ('Shafer, Emily', '622809130'),
    ('Stokes, Symone', '616259695'),
    ('Young, Giana L', '602762103'),
    ('Garcia, Bryan', '616259654'),
    ('Garcia, Miguel A', '681068099'),
    ('Hernandez, Diego', '652143527'),
    ('Pacheco Estrada, Jesus', '645935042'),
    ('Pajarito, Ramon', '685942713'),
    ('Rivas Beltran, Angel M', '358119787'),
    ('Romero Solis, Juan', '836220003'),
    ('Alcaraz, Luis', '432946242'),
    ('Alvarez, Jose', '534908967'),
    ('Arizmendi, Fernando', '613871092'),
    ('Arroyo, Jose', '364725751'),
    ('Bello, Luis', '616226754'),
    ('Bocanegra, Jose', '605908531'),
    ('Bustos, Eric', '603965173'),
    ('Cardoso, Hipolito', '264022781'),
    ('Castaneda, Andy', '611042001'),
    ('Castillo, Moises', '653246578'),
    ('Chavez, Derick J', '610591002'),
    ('Chavez, Endhy', '625379918'),
    ('Contreras, Brian', '137178003'),
    ('Cortez, Kevin', '656253574'),
    ('Cuevas, Marcelo', '625928556'),
    ('Dean, Jake', '615598883'),
    ('Duarte, Esau', '602134323'),
    ('Duarte, Kevin', '616259686'),
    ('Espinoza, Jose', '605486881'),
    ('Esquivel, Kleber', '606450473'),
    ('Flores, Saul', '625928557'),
    ('Garcia, Eduardo', '613909068'),
    ('Garcia, Miguel', '625170362'),
    ('Gomez, Jose', '897981424'),
    ('Gomez, Randel', '625928558'),
    ('Gonzalez, Alejandro', '621318203'),
    ('Gonzalez, Emanuel', '625170363'),
    ('Gonzalez, Miguel', '681068100'),
    ('Hernandez, Diego', '652143527'),
    ('Hernandez, Edgar', '625170364'),
    ('Hernandez, Sergio', '625170365'),
    ('Jose (Luis) Alvarez', '534908967'),
    ('Juan Romero Solis', '836220003'),
    ('Lopez, Alexander', '656253575'),
    ('Lopez, Yair', '625928559'),
    ('Lopez, Zefferino', '625928560'),
    ('Martinez, Alberto', '625170366'),
    ('Martinez, Emiliano', '625928561'),
    ('Martinez, Maciel', '625170367'),
    ('Moreno, Eduardo', '625928562'),
    ('Navarro, Jose', '625928563'),
    ('Pacheco, Jesus', '645935042'),
    ('Padilla, Carlos', '614425738'),
    ('Pajarito, Ramon', '685942713'),
    ('Pelagio, Miguel', '625928564'),
    ('Perez, Edgar', '625928565'),
    ('Ramon, Endhy', '625379918'),
    ('Ramos, Omar', '625928566'),
    ('Rivas, Manuel', '625928567'),
    ('Robledo, Francisco', '613108074'),
    ('Rodriguez, Anthony', '625928568'),
    ('Santos, Efrain', '625928569'),
    ('Santos, Javier', '625928570'),
    ('Solis, Juan Romero', '836220003'),
    ('Stokes, Symone', '616259695'),
    ('Torres, Anthony', '625928571'),
    ('Torrez, Jose', '625928572'),
    ('Vera, Erick', '625928573'),
    ('Vera, Victor', '625928574'),
    ('Zamara, Cesar', '625483371'),
    ('Anolin, Robert M', '552251095'),
    ('Dean, Joe P', '556534609'),
    ('Garrido, Raul', '657554426'),
    ('Magallanes, Julio', '612219002'),
    ('Padilla, Alex', '569697404'),
    ('Pealatere, Francis', '625098739'),
    ('Phein, Saeng Tsing', '624722627'),
    ('Rios, Jose D', '530358447'),
    ('Gomez, Jose', '897981424'),
    ('Nava, Juan M', '636667958'),
    ('Padilla, Carlos', '614425738'),
    ('Robledo, Francisco', '613108074'),
]

# Column map from Sierra → WBS (money/amount buckets)
BUCKETS = {
    "REG": ["A01", "REGULAR"],
    "OT": ["A02", "OVERTIME", "OT"],
    "DT": ["A03", "DOUBLETIME", "DT"],
    "VAC": ["A06", "VACATION", "VAC"],
    "SICK": ["A07", "SICK"],
    "HOLIDAY": ["A08", "HOLIDAY"],
    "BONUS": ["A04", "BONUS"],
    "COMM": ["A05", "COMMISSION", "COMM"],
    # piece-count hours & totals per day (Mon..Fri)
    "PC_HRS_MON": ["AH1"],
    "PC_TTL_MON": ["AI1"],
    "PC_HRS_TUE": ["AH2"],
    "PC_TTL_TUE": ["AI2"],
    "PC_HRS_WED": ["AH3"],
    "PC_TTL_WED": ["AI3"],
    "PC_HRS_THU": ["AH4"],
    "PC_TTL_THU": ["AI4"],
    "PC_HRS_FRI": ["AH5"],
    "PC_TTL_FRI": ["AI5"],
    "TRAVEL": ["ATE", "TRAVEL AMOUNT", "TRAVEL"],
}

WBS_DATA_START_ROW = 9  # first employee row in WEEKLY sheet (based on your template)
WBS_SHEET_NAME = "WEEKLY"

# WBS column indexes (1-based). Update only if your template moves columns.
COL = {
    "SSN": 2,                # B
    "EMP_NAME": 3,           # C
    "STATUS": 4,             # D
    "TYPE": 5,               # E
    "PAY_RATE": 7,           # G  (Pay Rate)
    "DEPT": 8,               # H  (Dept)
    "REG": 10,               # J  (A01)
    "OT": 11,                # K  (A02)
    "DT": 12,                # L  (A03)
    "VAC": 13,               # M  (A06)
    "SICK": 14,              # N  (A07)
    "HOLIDAY": 15,           # O  (A08)
    "BONUS": 16,             # P  (A04)
    "COMM": 17,              # Q  (A05)
    "PC_HRS_MON": 18,        # R  (AH1)
    "PC_TTL_MON": 19,        # S  (AI1)
    "PC_HRS_TUE": 20,        # T
    "PC_TTL_TUE": 21,        # U
    "PC_HRS_WED": 22,        # V
    "PC_TTL_WED": 23,        # W
    "PC_HRS_THU": 24,        # X
    "PC_TTL_THU": 25,        # Y
    "PC_HRS_FRI": 26,        # Z
    "PC_TTL_FRI": 27,        # AA
    "TRAVEL": 28,            # AB (ATE)
    "COMMENTS": 29,          # AC
    "TOTALS": 30,            # AD (leave formulas intact)
}

# ────────────────────────────────────────────────────────────────────────────────
def _norm_col(s: str) -> str:
    return str(s).strip().upper().replace(" ", "").replace("_", "")

def _pick(first_nonempty: List[Optional[str]]) -> str:
    for v in first_nonempty:
        if v and str(v).strip():
            return str(v).strip()
    return ""

def _read_sierra(io_bytes: bytes) -> pd.DataFrame:
    """Read Sierra weekly sheet and normalize columns. Aggregate duplicates."""
    try:
        xl = pd.ExcelFile(io.BytesIO(io_bytes))
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Cannot open Excel: {e}")

    # Prefer WEEKLY; else take first
    sheet = "WEEKLY" if "WEEKLY" in xl.sheet_names else xl.sheet_names[0]
    raw = pd.read_excel(io.BytesIO(io_bytes), sheet_name=sheet, header=None)

    # Find header rows — we look for a row that contains 'Employee' and 'SSN'
    header_idx = None
    for i in range(40):
        row = raw.iloc[i].astype(str).str.lower().tolist()
        if any("employee" in x for x in row) and any("ssn" in x for x in row):
            header_idx = i
            break
    if header_idx is None:
        raise HTTPException(status_code=422, detail="Could not locate header row (looking for 'Employee' and 'SSN').")

    # Sierra usually has a descriptive header row followed by code labels row
    df1 = pd.read_excel(io.BytesIO(io_bytes), sheet_name=sheet, header=header_idx + 1)
    # Promote first data row of df1 as headers
    df1.columns = df1.iloc[0]
    df1 = df1.iloc[1:].reset_index(drop=True)

    # Build a normalized column map
    norm_map: Dict[str, str] = {}
    for c in df1.columns:
        n = _norm_col(str(c))
        norm_map[n] = str(c)

    def col(*cands: List[str]) -> Optional[str]:
        for c in cands:
            n = _norm_col(c)
            # exact
            if n in norm_map:
                return norm_map[n]
            # fuzzy for A01..A08
            for k in norm_map:
                if n == k or n in k:
                    return norm_map[k]
        return None

    # Identify key columns
    cname_emp = col("Employee Name") or col("EMPLOYEENAME")
    cname_ssn = col("SSN")
    cname_status = col("Status")
    cname_type = col("Type")
    cname_prate = col("Pay Rate", "PayRate")
    cname_dept = col("Dept", "Department")

    if cname_emp is None or cname_ssn is None:
        raise HTTPException(status_code=422, detail="Missing required columns (Employee Name and/or SSN).")

    # Prepare bucket columns
    picks: Dict[str, Optional[str]] = {}
    for key, aliases in BUCKETS.items():
        picks[key] = col(*aliases)

    # Select + clean
    use_cols = [c for c in [cname_emp, cname_ssn, cname_status, cname_type, cname_prate, cname_dept] if c]
    for v in picks.values():
        if v and v not in use_cols:
            use_cols.append(v)

    df = df1[use_cols].copy()

    # Ensure numeric on amounts/hours
    num_cols = [c for c in picks.values() if c]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Normalize keys
    df[cname_emp] = df[cname_emp].astype(str).str.strip()
    df[cname_ssn] = df[cname_ssn].astype(str).str.replace(r"\D", "", regex=True)

    # Aggregate duplicates by SSN first; fallback to Name
    key = df[cname_ssn].where(df[cname_ssn].str.len() > 0, df[cname_emp])
    df["_key"] = key

    agg_map = {c: "sum" for c in num_cols}
    # for non-numeric fields, keep first non-empty
    def first_nonempty(series: pd.Series) -> str:
        for v in series:
            if pd.notna(v) and str(v).strip():
                return str(v).strip()
        return ""

    agg_map[cname_emp] = first_nonempty
    agg_map[cname_ssn] = first_nonempty
    if cname_status: agg_map[cname_status] = first_nonempty
    if cname_type: agg_map[cname_type] = first_nonempty
    if cname_prate: agg_map[cname_prate] = first_nonempty
    if cname_dept: agg_map[cname_dept] = first_nonempty

    g = df.groupby("_key", dropna=False).agg(agg_map).reset_index(drop=True)

    # Build normalized output frame with canonical names
    out_cols = [
        ("EMP_NAME", cname_emp),
        ("SSN", cname_ssn),
        ("STATUS", cname_status),
        ("TYPE", cname_type),
        ("PAY_RATE", cname_prate),
        ("DEPT", cname_dept),
    ]
    for k, v in picks.items():
        out_cols.append((k, v))

    norm_rows: List[Dict[str, object]] = []
    for _, row in g.iterrows():
        r: Dict[str, object] = {}
        for k, src in out_cols:
            r[k] = row[src] if src in row and pd.notna(row[src]) else (0 if k.startswith("PC_") or k in {"REG","OT","DT","VAC","SICK","HOLIDAY","BONUS","COMM","TRAVEL"} else "")
        norm_rows.append(r)

    return pd.DataFrame(norm_rows)

# ────────────────────────────────────────────────────────────────────────────────
def _sorted_employees(df: pd.DataFrame) -> List[Dict[str, object]]:
    """Return employees sorted by the hardcoded EMP_ORDER, then any extras."""
    # Build index by SSN then by name
    by_ssn: Dict[str, Dict[str, object]] = {
        str(r.get("SSN", "")).strip(): r for _, r in df.iterrows()
    }
    by_name: Dict[str, Dict[str, object]] = {
        str(r.get("EMP_NAME", "")).strip(): r for _, r in df.iterrows()
    }

    ordered: List[Dict[str, object]] = []
    seen = set()

    for name, ssn in EMP_ORDER:
        r = None
        if ssn and ssn in by_ssn:
            r = by_ssn[ssn]
        elif name in by_name:
            r = by_name[name]
        if r is not None:
            key = r.get("SSN", "") or r.get("EMP_NAME", "")
            if key not in seen:
                ordered.append(r)
                seen.add(key)

    # Append any leftovers (stable by name)
    for _, r in df.sort_values(by=["EMP_NAME"]).iterrows():
        key = r.get("SSN", "") or r.get("EMP_NAME", "")
        if key not in seen:
            ordered.append(r)
            seen.add(key)

    return ordered

def _clear_existing_rows(ws: Worksheet):
    """Clear prior data rows while keeping styles and formulas intact.
    Skips merged/read-only cells safely."""
    max_row = ws.max_row
    if max_row >= WBS_DATA_START_ROW:
        for r in range(WBS_DATA_START_ROW, max_row + 1):
            # If the row already looks empty across data columns, skip it
            try:
                if all((ws.cell(row=r, column=c).value in (None, ""))
                       for c in range(1, COL["TOTALS"] + 1)):
                    continue
            except Exception:
                pass

            for c in range(1, COL["TOTALS"] + 1):
                try:
                    cell = ws.cell(row=r, column=c)
                    # Only clear plain cells; merged cells throw on assignment
                    _ = cell.value  # access to detect read-only
                    cell.value = None
                except Exception:
                    # merged or read-only – leave as-is (keeps formulas/borders)
                    continue

def _write_to_wbs(wb_path: Path, ordered_rows: List[Dict[str, object]]) -> bytes:
    wb = load_workbook(str(wb_path))
    if WBS_SHEET_NAME not in wb.sheetnames:
        raise HTTPException(status_code=500, detail=f"Sheet {WBS_SHEET_NAME} not found in template.")
    ws = wb[WBS_SHEET_NAME]

    _clear_existing_rows(ws)

    r = WBS_DATA_START_ROW
    for row in ordered_rows:
        # text-ish fields
        ws.cell(row=r, column=COL["EMP_NAME"]).value = str(row.get("EMP_NAME", "") or "")
        ssn = str(row.get("SSN", "") or "").replace("-", "")
        ws.cell(row=r, column=COL["SSN"]).value = ssn
        ws.cell(row=r, column=COL["STATUS"]).value = str(row.get("STATUS", "") or "")
        ws.cell(row=r, column=COL["TYPE"]).value = str(row.get("TYPE", "") or "")
        ws.cell(row=r, column=COL["PAY_RATE"]).value = row.get("PAY_RATE", "")
        ws.cell(row=r, column=COL["DEPT"]).value = row.get("DEPT", "")

        # numeric buckets (leave None if 0 to keep sheet tidy)
        for key in ["REG","OT","DT","VAC","SICK","HOLIDAY","BONUS","COMM",
                    "PC_HRS_MON","PC_TTL_MON","PC_HRS_TUE","PC_TTL_TUE",
                    "PC_HRS_WED","PC_TTL_WED","PC_HRS_THU","PC_TTL_THU",
                    "PC_HRS_FRI","PC_TTL_FRI","TRAVEL"]:
            val = row.get(key, 0)
            if pd.isna(val): val = 0
            ws.cell(row=r, column=COL[key]).value = float(val) if val != 0 else None

        # DO NOT touch COL["TOTALS"] – keep template formulas
        r += 1

    # return workbook as bytes
    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()

# ────────────────────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"ok": True}

@app.post("/process-payroll")
async def process_payroll(file: UploadFile = File(...)):
    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=422, detail="Please upload an Excel file (.xlsx or .xls).")

    contents = await file.read()

    # Parse Sierra
    df = _read_sierra(contents)

    # Sort using gold-master order (locked)
    ordered = _sorted_employees(df)

    # Load template from repo root
    here = Path(__file__).resolve().parent
    template_path = here.parent / "wbs_template.xlsx"
    if not template_path.exists():
        raise HTTPException(status_code=500, detail=f"WBS template not found at {template_path}")

    # Write WBS workbook
    xls_bytes = _write_to_wbs(template_path, ordered)

    # Name output using dates if available; fallback to today
    try:
        # If the incoming file has a date in name, re-use it
        base = Path(file.filename).stem
        out_name = f"WBS_{base}.xlsx"
    except Exception:
        out_name = "WBS_Payroll.xlsx"

    return StreamingResponse(
        io.BytesIO(xls_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{out_name}"'},
    )
