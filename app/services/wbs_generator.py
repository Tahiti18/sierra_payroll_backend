# --- in app/services/wbs_generator.py ---

from io import BytesIO
from datetime import timedelta, datetime
import pandas as pd
from fastapi import HTTPException
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, Alignment, PatternFill

def generate_wbs_weekly(roster: pd.DataFrame, agg: pd.DataFrame, pe_date) -> BytesIO:
    # Be tolerant to duplicates → avoids MergeError → 500
    out = roster.merge(agg, how="left", on="Employee Name")

    # If any aggregated names didn’t match the roster, return a clean 422 instead of crashing later
    if "TotalHours" in out.columns:
        missing = out.loc[out["TotalHours"].isna(), "Employee Name"].dropna().unique().tolist()
        if missing:
            raise HTTPException(
                status_code=422,
                detail=f"Employees missing from roster.csv: {sorted(set(missing))}"
            )

    # numeric coercions
    for c in ["TotalHours","Reg","OT","DT","MON","TUE","WED","THU","FRI"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0.0)

    totals_hours = (out.get("Reg",0) + out.get("OT",0) + out.get("DT",0)).fillna(0.0)
    out["TotalsCalc"] = out["PayRate"] * totals_hours
    is_salary = out["Type"].astype(str).str.upper().eq("S")
    out.loc[is_salary, "TotalsCalc"] = out.loc[is_salary, "PayRate"].fillna(0.0)
    out.loc[is_salary & (totals_hours == 0), "Reg"] = 40.0

    # header dates
    rpt_date = pe_date + timedelta(days=3)
    ck_date  = pe_date + timedelta(days=5)

    header_row_labels = [
        "# V","DO NOT EDIT","Version = B90216-00","FmtRev = 2.1",
        f"RunTime = {datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
        "CliUnqId = 055269","CliName = Sierra Roofing and Solar Inc","Freq = W",
        f"PEDate = {pe_date.strftime('%m/%d/%Y')}",
        f"RptDate = {rpt_date.strftime('%m/%d/%Y')}",
        f"CkDate = {ck_date.strftime('%m/%d/%Y')}",
        "EmpType = SSN","DoNotes = 1","PayRates = H+;S+;E+;C+",
        "RateCol = 6","T1 = 7+","CodeBeg = 8","CodeEnd = 26","NoteCol = 27",
        "","","","","","","",""
    ]

    rows = []
    pad = [None]*(len(header_row_labels)-3)
    rows.append(["# U","CliUnqID","055269", *pad])
    rows.append(["# N","Client","Sierra Roofing and Solar Inc", *pad])
    rows.append(["# P","Period End",pe_date.strftime("%m/%d/%Y"), *pad])
    rows.append(["# R","Report Due",rpt_date.strftime("%m/%d/%Y"), *pad])
    rows.append(["# C","Check Date",ck_date.strftime("%m/%d/%Y"), *pad])
    rows.append([
        None,None,None,None,None,None,None,
        "REGULAR","OVERTIME","DOUBLETIME","VACATION","SICK","HOLIDAY","BONUS","COMMISSION",
        "PC HRS MON","PC HRS TUE","PC HRS WED","PC HRS THU","PC HRS FRI",
        "PC TTL MON","PC TTL TUE","PC TTL WED","PC TTL THU","PC TTL FRI",
        "TRAVEL AMOUNT","Comments","Totals"
    ])
    rows.append([
        "# E:26","SSN","Employee Name","Status","Type","Pay Rate","Dept",
        "A01","A02","A03","A04","A05","A06","A07","A08",
        "A09","A10","A11","A12","A13","A14","A15","A16","A17",
        "A18","A19","A26"
    ])

    def fnum(x):
        try: return float(x)
        except: return 0.0

    for _, r in out.sort_values("Employee Name").iterrows():
        row = [None]*len(header_row_labels)
        row[0]  = str(r.get("EmpID") or "")
        row[1]  = str(r.get("SSN") or "")
        row[2]  = r.get("Employee Name") or ""
        row[3]  = r.get("Status") or ""
        row[4]  = r.get("Type") or ""
        row[5]  = fnum(r.get("PayRate"))
        row[6]  = r.get("Dept") or ""
        row[7]  = fnum(r.get("Reg"))
        row[8]  = fnum(r.get("OT"))
        row[9]  = fnum(r.get("DT"))
        row[15] = fnum(r.get("MON"))
        row[16] = fnum(r.get("TUE"))
        row[17] = fnum(r.get("WED"))
        row[18] = fnum(r.get("THU"))
        row[19] = fnum(r.get("FRI"))
        row[27] = round(fnum(r.get("TotalsCalc")), 2)
        rows.append(row)

    df_weekly = pd.DataFrame(rows, columns=header_row_labels)

    # Write Excel
    out_stream = BytesIO()
    with pd.ExcelWriter(out_stream, engine="openpyxl") as writer:
        pd.DataFrame([header_row_labels], columns=header_row_labels).to_excel(
            writer, index=False, header=False, sheet_name="WEEKLY"
        )
        df_weekly.to_excel(writer, index=False, header=False, sheet_name="WEEKLY", startrow=1)

        ws = writer.sheets.get("WEEKLY") or writer.book["WEEKLY"]

        # Freeze panes (keep headings and columns up to Dept)
        ws.freeze_panes = "H9"

        # Safe column width setting
        widths = {
            1:10, 2:14, 3:32, 4:10, 5:8, 6:12, 7:12,
            8:12, 9:12, 10:12,
            15:14,16:14,17:14,18:14,19:14,
            20:14,21:14,22:14,23:14,24:14,
            25:16, 26:18, 27:12
        }
        for c, w in widths.items():
            ws.column_dimensions[get_column_letter(c)].width = w

        # Light styling
        bold = Font(bold=True)
        center = Alignment(horizontal="center", vertical="center")
        fill = PatternFill("solid", fgColor="EDEDED")

        cat_row = 7   # categories line
        for col in range(8, 28):
            cell = ws.cell(row=cat_row, column=col)
            cell.font = bold; cell.alignment = center; cell.fill = fill

        map_row = 8   # “# E:26” mapping line
        for col in range(1, 28):
            cell = ws.cell(row=map_row, column=col)
            cell.font = bold; cell.alignment = center
            if col >= 8: cell.fill = fill

        # number formats
        num_cols = [6, 8, 9, 10, 15, 16, 17, 18, 19, 27]
        for r in range(map_row+1, ws.max_row+1):
            for c in num_cols:
                ws.cell(row=r, column=c).number_format = '0.00'

    out_stream.seek(0)
    return out_stream
