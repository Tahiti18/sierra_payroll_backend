# app/services/wbs_generator.py
import io
import re
from typing import Tuple, Dict, Optional, Any, List

import pandas as pd
from fastapi import HTTPException
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.cell import Cell


def _money(val) -> float:
    try:
        return float(val or 0.0)
    except Exception:
        return 0.0


def _norm(v: Any) -> str:
    s = "" if v is None else str(v)
    s = s.replace("\n", " ").replace("\r", " ")
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


def _find_wbs_header(ws: Worksheet) -> Tuple[int, Dict[str, int]]:
    """
    Locate the header row and map columns. Accepts common header variants.
    """
    req_aliases = {
        "ssn":   ["ssn"],
        "name":  ["employee name", "employee", "name"],
        "status":["status"],
        "type":  ["type"],
        "rate":  ["pay rate", "payrate", "rate"],
        "dept":  ["dept", "department"],
        "a01":   ["a01", "regular", "reg"],
        "a02":   ["a02", "overtime", "ot"],
        "a03":   ["a03", "doubletime", "dt"],
    }
    opt_aliases = {
        "reg_amt":     ["a01 $", "a01$", "reg $", "regular $", "regular amt", "a01 amount"],
        "ot_amt":      ["a02 $", "a02$", "ot $",  "overtime $", "overtime amt", "a02 amount"],
        "dt_amt":      ["a03 $", "a03$", "dt $",  "doubletime $", "doubletime amt", "a03 amount"],
        "total_amt":   ["total $", "total$", "grand total $"],
        "total_plain": ["total", "totals"],  # far-right shaded column
    }

    best_row, best_map, best_score = None, None, -1
    for r in range(1, ws.max_row + 1):
        row_vals = [_norm(ws.cell(r, c).value) for c in range(1, ws.max_column + 1)]
        if sum(1 for v in row_vals if v) < 3:
            continue

        # build lookup
        col_map = {}
        for c, v in enumerate(row_vals, start=1):
            if v and v not in col_map:
                col_map[v] = c

        def pick(aliases: List[str]) -> Optional[int]:
            for a in aliases:
                if a in col_map:
                    return col_map[a]
                for v, c in col_map.items():
                    if a in v:
                        return c
            return None

        tmp = {k: pick(v) for k, v in req_aliases.items()}
        score = sum(1 for v in tmp.values() if v is not None)
        if score > best_score and tmp.get("name") and tmp.get("a01") and tmp.get("a02") and tmp.get("a03"):
            tmp["reg_amt"]     = pick(opt_aliases["reg_amt"])
            tmp["ot_amt"]      = pick(opt_aliases["ot_amt"])
            tmp["dt_amt"]      = pick(opt_aliases["dt_amt"])
            tmp["total_amt"]   = pick(opt_aliases["total_amt"])
            tmp["total_plain"] = pick(opt_aliases["total_plain"])
            best_row, best_map, best_score = r, tmp, score

    if not best_row or not best_map:
        raise HTTPException(422, "WEEKLY header not found. Need columns like 'Employee Name', 'A01', 'A02', 'A03'.")
    return best_row, best_map


def _copy_style(dst: Cell, src: Cell) -> None:
    """
    Clone style attributes from src → dst to preserve WBS look.
    """
    if src.has_style:
        dst.font = src.font
        dst.fill = src.fill
        dst.border = src.border
        dst.alignment = src.alignment
        dst.number_format = src.number_format
        dst.protection = src.protection


def _blank_if_zero(val: Any) -> Optional[float]:
    try:
        v = float(val)
        return None if abs(v) < 1e-9 else v
    except Exception:
        return val


def write_into_wbs_template(template_bytes: bytes, weekly: pd.DataFrame) -> bytes:
    """
    Write ONLY into the 'WEEKLY' sheet, cloning the template's row styles so the
    output matches WBS exactly. Zeros become blanks.
    """
    wb = load_workbook(io.BytesIO(template_bytes))
    ws = wb["WEEKLY"] if "WEEKLY" in wb.sheetnames else wb.active

    header_row, cols = _find_wbs_header(ws)
    first_data_row = header_row + 1

    # 1) Find last existing data row and clear data area (keep styles/merged cells)
    scan_col = cols.get("name") or cols.get("ssn") or 2
    last = ws.max_row
    last_data = first_data_row - 1
    for r in range(first_data_row, last + 1):
        if ws.cell(r, scan_col).value not in (None, ""):
            last_data = r
    if last_data >= first_data_row:
        ws.delete_rows(first_data_row, last_data - first_data_row + 1)

    # 2) Grab a "prototype" row style from the first blank row (immediately under header)
    proto_row_idx = first_data_row
    # If the first row under header is not blank, insert one temporary blank row as prototype
    need_temp_proto = False
    if any(ws.cell(proto_row_idx, c).value not in (None, "") for c in range(1, ws.max_column + 1)):
        ws.insert_rows(proto_row_idx, 1)
        need_temp_proto = True

    # Snapshot styles per column from the prototype row
    proto_styles = {}
    for c in range(1, ws.max_column + 1):
        proto_styles[c] = ws.cell(proto_row_idx, c).copy()

    # Remove the temporary prototype if we inserted one, but keep the style snapshot
    if need_temp_proto:
        ws.delete_rows(proto_row_idx, 1)

    # Helper to set a value at (row, col) and clone style from proto
    def put(r: int, c: int, value: Any):
        cell = ws.cell(r, c)
        # clone style
        _copy_style(cell, proto_styles.get(c, ws.cell(header_row, c)))
        # write blank instead of 0
        cell.value = _blank_if_zero(value)

    # 3) Write each weekly row, cloning styles
    write_row = first_data_row
    for _, row in weekly.iterrows():
        # Ensure we have at least up to the far-right mapped column
        max_target_col = max([v for v in cols.values() if v is not None] + [ws.max_column])
        for c in range(1, max_target_col + 1):
            # Pre-style the entire row (keeps borders/fills across empty columns)
            put(write_row, c, None)

        if cols.get("ssn"):      put(write_row, cols["ssn"],      row.get("ssn", ""))
        if cols.get("name"):     put(write_row, cols["name"],     row.get("employee", ""))
        if cols.get("status"):   put(write_row, cols["status"],   row.get("Status", "A"))
        if cols.get("type"):     put(write_row, cols["type"],     row.get("Type", "H"))
        if cols.get("rate"):     put(write_row, cols["rate"],     row.get("rate", 0.0))
        if cols.get("dept"):     put(write_row, cols["dept"],     row.get("department", ""))

        if cols.get("a01"):      put(write_row, cols["a01"],      row.get("REG", 0.0))
        if cols.get("a02"):      put(write_row, cols["a02"],      row.get("OT", 0.0))
        if cols.get("a03"):      put(write_row, cols["a03"],      row.get("DT", 0.0))

        if cols.get("reg_amt"):  put(write_row, cols["reg_amt"],  row.get("REG_$", 0.0))
        if cols.get("ot_amt"):   put(write_row, cols["ot_amt"],   row.get("OT_$", 0.0))
        if cols.get("dt_amt"):   put(write_row, cols["dt_amt"],   row.get("DT_$", 0.0))

        # Prefer explicit Total $; else the far-right shaded Total/Totals
        total_val = row.get("TOTAL_$", 0.0)
        if cols.get("total_amt"):
            put(write_row, cols["total_amt"], total_val)
        elif cols.get("total_plain"):
            put(write_row, cols["total_plain"], total_val)

        write_row += 1

    # 4) Spacer row (styled like prototype, blank)
    for c in range(1, ws.max_column + 1):
        put(write_row, c, None)
    write_row += 1

    # 5) TOTAL row (clone style + label "TOTAL" in name col; fill sums)
    totals = {
        "REG":     float(weekly["REG"].sum())     if "REG" in weekly else 0.0,
        "OT":      float(weekly["OT"].sum())      if "OT" in weekly else 0.0,
        "DT":      float(weekly["DT"].sum())      if "DT" in weekly else 0.0,
        "REG_$":   float(weekly["REG_$"].sum())   if "REG_$" in weekly else 0.0,
        "OT_$":    float(weekly["OT_$"].sum())    if "OT_$" in weekly else 0.0,
        "DT_$":    float(weekly["DT_$"].sum())    if "DT_$" in weekly else 0.0,
        "TOTAL_$": float(weekly["TOTAL_$"].sum()) if "TOTAL_$" in weekly else 0.0,
    }

    for c in range(1, ws.max_column + 1):
        put(write_row, c, None)

    if cols.get("name"):     put(write_row, cols["name"], "TOTAL")
    if cols.get("a01"):      put(write_row, cols["a01"],  _money(totals["REG"]))
    if cols.get("a02"):      put(write_row, cols["a02"],  _money(totals["OT"]))
    if cols.get("a03"):      put(write_row, cols["a03"],  _money(totals["DT"]))
    if cols.get("reg_amt"):  put(write_row, cols["reg_amt"], _money(totals["REG_$"]))
    if cols.get("ot_amt"):   put(write_row, cols["ot_amt"],  _money(totals["OT_$"]))
    if cols.get("dt_amt"):   put(write_row, cols["dt_amt"],  _money(totals["DT_$"]))

    if cols.get("total_amt"):
        put(write_row, cols["total_amt"], _money(totals["TOTAL_$"]))
    elif cols.get("total_plain"):
        put(write_row, cols["total_plain"], _money(totals["TOTAL_$"]))

    # 6) Save back
    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.read()
