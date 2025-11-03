# app/services/excel_processor.py
# Sierra → WBS Converter (Final, exact spec implementation)

from __future__ import annotations

import io
import re
from datetime import datetime, time, timedelta
from typing import Dict, List, Tuple, Optional

import pandas as pd
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet


# ---------- Constants & Helpers ----------

WDAYS = ["MON", "TUE", "WED", "THU", "FRI"]
WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]

def _normalize_name(full_name: str) -> Tuple[str, str]:
    """Extract first and last name from any format and return '(last, first)' format."""
    if not isinstance(full_name, str):
        return "", ""
    
    # Clean the name
    name = " ".join(full_name.split()).strip()
    if not name:
        return "", ""
    
    # Split into words, keep only alphabetic characters
    words = [re.sub(r'[^a-zA-Z]', '', word) for word in name.split()]
    words = [w for w in words if w]
    
    if not words:
        return "", ""
    
    # Simple heuristic: last word is last name, rest is first name
    if len(words) == 1:
        return words[0].lower(), ""
    else:
        last_name = words[-1].lower()
        first_name = " ".join(words[:-1]).lower()
        return last_name, first_name

def _get_canonical_name(full_name: str) -> str:
    """Convert to 'last, first' format for matching."""
    last, first = _normalize_name(full_name)
    if last and first:
        return f"{last}, {first}"
    elif last:
        return last
    else:
        return full_name.lower().strip()

def _to_time(x) -> Optional[time]:
    """Coerce Excel/time strings to time or None."""
    if pd.isna(x) or x == "" or x is None:
        return None
    
    if isinstance(x, time):
        return x
    
    if isinstance(x, datetime):
        return x.time()
    
    s = str(x).strip()
    if not s:
        return None
    
    # Try common time formats
    for fmt in ("%H:%M", "%I:%M %p", "%H.%M", "%I.%M %p", "%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).time()
        except ValueError:
            continue
    
    # Excel float days (0.5 = 12:00)
    try:
        f = float(s)
        if 0 <= f < 1:
            total_seconds = int(round(f * 24 * 3600))
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            return time(hours, minutes)
    except (ValueError, TypeError):
        pass
    
    return None

def _to_date(x) -> Optional[datetime]:
    """Convert to datetime or None."""
    if pd.isna(x) or x == "" or x is None:
        return None
    
    if isinstance(x, datetime):
        return x
    
    try:
        return pd.to_datetime(x).to_pydatetime()
    except (ValueError, TypeError):
        return None

def _compute_hours(start: Optional[time], finish: Optional[time], 
                   lunch_start: Optional[time], lunch_finish: Optional[time]) -> float:
    """Compute hours = (finish - start) - lunch, clamp ≥ 0."""
    if not start or not finish:
        return 0.0
    
    # Convert to datetime for calculation
    dt_start = datetime(2000, 1, 1, start.hour, start.minute, start.second)
    dt_finish = datetime(2000, 1, 1, finish.hour, finish.minute, finish.second)
    
    # Handle midnight crossing
    if dt_finish < dt_start:
        dt_finish += timedelta(days=1)
    
    total_duration = dt_finish - dt_start
    
    # Subtract lunch break
    if lunch_start and lunch_finish:
        dt_lunch_start = datetime(2000, 1, 1, lunch_start.hour, lunch_start.minute, lunch_start.second)
        dt_lunch_finish = datetime(2000, 1, 1, lunch_finish.hour, lunch_finish.minute, lunch_finish.second)
        
        if dt_lunch_finish < dt_lunch_start:
            dt_lunch_finish += timedelta(days=1)
        
        lunch_duration = dt_lunch_finish - dt_lunch_start
        total_duration -= lunch_duration
    
    hours = max(0.0, total_duration.total_seconds() / 3600.0)
    return round(hours, 2)

def _split_daily_hours(hours: float) -> Tuple[float, float, float]:
    """Daily split: 0–8 REG, 8–12 OT, >12 DT."""
    reg = min(8.0, hours)
    ot = min(4.0, max(0.0, hours - 8.0))
    dt = max(0.0, hours - 12.0)
    return round(reg, 2), round(ot, 2), round(dt, 2)

def _apply_weekly_overlay(reg_total: float, ot_total: float) -> Tuple[float, float]:
    """Weekly overlay: if reg_total > 40, move excess from REG → OT."""
    if reg_total > 40.0:
        excess = round(reg_total - 40.0, 2)
        reg_total = 40.0
        ot_total = round(ot_total + excess, 2)
    return reg_total, ot_total

def _round_to_2dec(value: float) -> float:
    """Round to 2 decimal places."""
    return round(float(value) + 1e-9, 2)

def _blank_if_zero(value: float) -> Optional[float]:
    """Return None if value rounds to 0.00, else rounded value."""
    if value is None:
        return None
    rounded = _round_to_2dec(value)
    return None if abs(rounded) < 0.005 else rounded


# ---------- Roster Loading ----------

def load_roster(roster_path: str) -> List[Dict]:
    """Load roster with exact column mapping."""
    df = pd.read_excel(roster_path, sheet_name=0, engine="openpyxl")
    
    # Map columns exactly
    col_map = {}
    for col in df.columns:
        col_lower = str(col).lower().strip()
        if "employee" in col_lower and "name" in col_lower:
            col_map["name"] = col
        elif "ssn" in col_lower or "social" in col_lower:
            col_map["ssn"] = col
        elif "empid" in col_lower or "emp id" in col_lower:
            col_map["empid"] = col
        elif "status" in col_lower:
            col_map["status"] = col
        elif "type" in col_lower:
            col_map["type"] = col
        elif "payrate" in col_lower or "pay rate" in col_lower:
            col_map["payrate"] = col
        elif "dept" in col_lower or "department" in col_lower:
            col_map["dept"] = col
    
    roster = []
    for _, row in df.iterrows():
        name = str(row[col_map["name"]]).strip() if "name" in col_map else ""
        if not name:
            continue
            
        canonical = _get_canonical_name(name)
        
        # Get pay rate with fallback
        pay_rate = 0.0
        if "payrate" in col_map:
            try:
                pay_rate = float(row[col_map["payrate"]]) if pd.notna(row[col_map["payrate"]]) else 0.0
            except (ValueError, TypeError):
                pay_rate = 0.0
        
        roster.append({
            "original_name": name,
            "canonical_name": canonical,
            "ssn": str(row[col_map["ssn"]]) if "ssn" in col_map and pd.notna(row[col_map["ssn"]]) else "",
            "empid": str(row[col_map["empid"]]) if "empid" in col_map and pd.notna(row[col_map["empid"]]) else "",
            "status": str(row[col_map["status"]]) if "status" in col_map and pd.notna(row[col_map["status"]]) else "A",
            "type": str(row[col_map["type"]]) if "type" in col_map and pd.notna(row[col_map["type"]]) else "H",
            "payrate": _round_to_2dec(pay_rate),
            "dept": str(row[col_map["dept"]]) if "dept" in col_map and pd.notna(row[col_map["dept"]]) else "",
        })
    
    return roster


# ---------- Sierra Parsing ----------

def parse_sierra(sierra_bytes: bytes) -> pd.DataFrame:
    """Parse Sierra workbook with exact column requirements."""
    sio = io.BytesIO(sierra_bytes)
    df = pd.read_excel(sio, sheet_name=0, engine="openpyxl")
    
    # Verify required columns exist
    required_cols = ["Days", "Job#", "Name", "Start", "Lnch St.", "Lnch Fnsh", "Finish", "Hours", "Rate", "Total", "Job Detail"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Sierra sheet missing required columns: {', '.join(missing_cols)}")
    
    results = []
    
    for _, row in df.iterrows():
        name = str(row["Name"]).strip()
        if not name or name == "nan":
            continue
        
        # Compute hours from times (ignore "Hours" column)
        start_time = _to_time(row["Start"])
        finish_time = _to_time(row["Finish"])
        lunch_start = _to_time(row["Lnch St."])
        lunch_finish = _to_time(row["Lnch Fnsh"])
        
        daily_hours = _compute_hours(start_time, finish_time, lunch_start, lunch_finish)
        
        # Get date and weekday
        date_val = _to_date(row["Days"])
        weekday = None
        weekday_name = None
        if date_val:
            weekday = date_val.weekday()  # 0=Monday, 4=Friday
            if 0 <= weekday <= 4:
                weekday_name = WEEKDAYS[weekday]
        
        # Get rate if available
        rate = None
        if pd.notna(row["Rate"]):
            try:
                rate_val = float(row["Rate"])
                if rate_val > 0:
                    rate = _round_to_2dec(rate_val)
            except (ValueError, TypeError):
                pass
        
        results.append({
            "original_name": name,
            "canonical_name": _get_canonical_name(name),
            "date": date_val,
            "weekday": weekday,
            "weekday_name": weekday_name,
            "hours": daily_hours,
            "rate": rate
        })
    
    return pd.DataFrame(results)


# ---------- Hours Computation ----------

class EmployeeWeek:
    __slots__ = ("original_name", "canonical_name", "daily_hours", "reg_daily", "ot_daily", "dt_daily", 
                 "reg_weekly", "ot_weekly", "dt_weekly", "rates", "pc_hours")
    
    def __init__(self, original_name: str):
        self.original_name = original_name
        self.canonical_name = _get_canonical_name(original_name)
        self.daily_hours = {}  # date -> hours
        self.reg_daily = {}    # date -> reg hours
        self.ot_daily = {}     # date -> ot hours  
        self.dt_daily = {}     # date -> dt hours
        self.reg_weekly = 0.0
        self.ot_weekly = 0.0
        self.dt_weekly = 0.0
        self.rates = []        # all observed rates
        self.pc_hours = {day: 0.0 for day in WDAYS}  # PC hours by weekday


def compute_employee_hours(sierra_df: pd.DataFrame) -> Dict[str, EmployeeWeek]:
    """Compute weekly hours for each employee with daily and weekly rules."""
    employees = {}
    
    # First pass: daily calculations
    for _, row in sierra_df.iterrows():
        canonical_name = row["canonical_name"]
        if canonical_name not in employees:
            employees[canonical_name] = EmployeeWeek(row["original_name"])
        
        emp = employees[canonical_name]
        date = row["date"]
        hours = row["hours"]
        
        if date:
            emp.daily_hours[date] = emp.daily_hours.get(date, 0.0) + hours
        
        # Store rate if available
        if row["rate"] is not None:
            emp.rates.append(row["rate"])
    
    # Second pass: apply daily overtime rules
    for emp in employees.values():
        for date, hours in emp.daily_hours.items():
            reg, ot, dt = _split_daily_hours(hours)
            emp.reg_daily[date] = reg
            emp.ot_daily[date] = ot
            emp.dt_daily[date] = dt
            
            emp.reg_weekly += reg
            emp.ot_weekly += ot
            emp.dt_weekly += dt
            
            # Track PC hours for weekdays
            if date and 0 <= date.weekday() <= 4:
                day_key = WDAYS[date.weekday()]
                emp.pc_hours[day_key] += hours
    
    # Third pass: apply weekly overlay
    for emp in employees.values():
        emp.reg_weekly, emp.ot_weekly = _apply_weekly_overlay(emp.reg_weekly, emp.ot_weekly)
        emp.reg_weekly = _round_to_2dec(emp.reg_weekly)
        emp.ot_weekly = _round_to_2dec(emp.ot_weekly)
        emp.dt_weekly = _round_to_2dec(emp.dt_weekly)
        
        # Round PC hours
        for day in WDAYS:
            emp.pc_hours[day] = _round_to_2dec(emp.pc_hours[day])
    
    return employees


# ---------- WBS Template Writing ----------

def _find_wbs_header_row(ws: Worksheet) -> Tuple[int, Dict[str, int]]:
    """Find WBS header row between rows 6-12 and map column headers."""
    header_candidates = [
        "Employee Name", "SSN", "Emp ID", "EmpID", "Status", "Type", "Dept", "Pay Rate",
        "REGULAR", "OVERTIME", "DOUBLETIME", "VACATION", "SICK", "HOLIDAY", "BONUS", "COMMISSION",
        "PC HRS MON", "PC TTL MON", "PC HRS TUE", "PC TTL TUE", "PC HRS WED", "PC TTL WED",
        "PC HRS THU", "PC TTL THU", "PC HRS FRI", "PC TTL FRI", "Totals", "Total"
    ]
    
    best_row = None
    best_score = 0
    best_headers = {}
    
    for row_num in range(6, 13):  # rows 6-12
        row_headers = {}
        score = 0
        
        for col in range(1, ws.max_column + 1):
            cell_value = ws.cell(row=row_num, column=col).value
            if not cell_value:
                continue
                
            header_text = str(cell_value).strip()
            header_lower = header_text.lower()
            
            # Match against our candidates
            for candidate in header_candidates:
                if candidate.lower() == header_lower:
                    row_headers[candidate] = col
                    score += 1
                    break
        
        # Require Employee Name, REGULAR, OVERTIME
        required_found = ("Employee Name" in row_headers and 
                         "REGULAR" in row_headers and 
                         "OVERTIME" in row_headers)
        
        if required_found and score > best_score:
            best_row = row_num
            best_score = score
            best_headers = row_headers
    
    if best_row is None:
        raise RuntimeError("WBS header row not found between rows 6-12 (missing Employee Name/REGULAR/OVERTIME).")
    
    return best_row, best_headers


def _write_cell(ws: Worksheet, row: int, col: int, value, is_numeric: bool = False):
    """Write value to cell, handling zeros as blanks for numeric values."""
    if is_numeric and value is not None:
        value = _blank_if_zero(value)
    
    if value is not None:
        ws.cell(row=row, column=col).value = value


def write_wbs_output(template_path: str, roster: List[Dict], 
                    computed: Dict[str, EmployeeWeek]) -> bytes:
    """Write computed data to WBS template."""
    wb = load_workbook(template_path)
    ws = wb.active
    
    header_row, col_map = _find_wbs_header_row(ws)
    data_start_row = header_row + 1
    
    # Clear existing data rows
    for row in range(data_start_row, ws.max_row + 1):
        for col in range(1, ws.max_column + 1):
            ws.cell(row=row, column=col).value = None
    
    # Build employee list in correct order
    output_employees = []
    
    # 1. Roster employees in order
    roster_employees = []
    for roster_emp in roster:
        canonical = roster_emp["canonical_name"]
        computed_emp = computed.get(canonical)
        roster_employees.append((roster_emp, computed_emp))
    
    # 2. Sierra employees not in roster, sorted alphabetically
    non_roster_employees = []
    for canonical, computed_emp in computed.items():
        if not any(roster_emp["canonical_name"] == canonical for roster_emp, _ in roster_employees):
            non_roster_employees.append(computed_emp)
    
    non_roster_employees.sort(key=lambda x: x.original_name.lower())
    
    # Combine in correct order
    output_employees.extend(roster_employees)
    output_employees.extend([(None, emp) for emp in non_roster_employees])
    
    # Write data
    current_row = data_start_row
    
    for roster_info, computed_emp in output_employees:
        if computed_emp is None:
            # Employee in roster but not in Sierra data
            emp_name = roster_info["original_name"]
            emp_type = roster_info["type"]
            emp_status = roster_info["status"]
            emp_dept = roster_info["dept"]
            emp_ssn = roster_info["ssn"]
            emp_empid = roster_info["empid"]
            pay_rate = roster_info["payrate"]
            reg = ot = dt = 0.0
            pc_hours = {day: 0.0 for day in WDAYS}
        else:
            # Employee has computed data
            if roster_info:
                # Roster employee
                emp_name = roster_info["original_name"]
                emp_type = roster_info["type"]
                emp_status = roster_info["status"]
                emp_dept = roster_info["dept"]
                emp_ssn = roster_info["ssn"]
                emp_empid = roster_info["empid"]
                pay_rate = roster_info["payrate"] if roster_info["payrate"] > 0 else (
                    computed_emp.rates[-1] if computed_emp.rates else 0.0
                )
            else:
                # Non-roster employee
                emp_name = computed_emp.original_name + " (NOT FOUND)"
                emp_type = "H"
                emp_status = "A"
                emp_dept = ""
                emp_ssn = ""
                emp_empid = ""
                pay_rate = computed_emp.rates[-1] if computed_emp.rates else 0.0
            
            reg = computed_emp.reg_weekly
            ot = computed_emp.ot_weekly
            dt = computed_emp.dt_weekly
            pc_hours = computed_emp.pc_hours
        
        # For salaried employees (Type starts with "S"), set hours to 0 and use pay rate as total
        is_salaried = str(emp_type).strip().upper().startswith("S")
        
        # Write employee info
        _write_cell(ws, current_row, col_map["Employee Name"], emp_name)
        if "SSN" in col_map:
            _write_cell(ws, current_row, col_map["SSN"], emp_ssn)
        if "Emp ID" in col_map:
            _write_cell(ws, current_row, col_map["Emp ID"], emp_empid)
        elif "EmpID" in col_map:
            _write_cell(ws, current_row, col_map["EmpID"], emp_empid)
        if "Status" in col_map:
            _write_cell(ws, current_row, col_map["Status"], emp_status)
        if "Type" in col_map:
            _write_cell(ws, current_row, col_map["Type"], emp_type)
        if "Dept" in col_map:
            _write_cell(ws, current_row, col_map["Dept"], emp_dept)
        if "Pay Rate" in col_map:
            _write_cell(ws, current_row, col_map["Pay Rate"], _blank_if_zero(pay_rate), is_numeric=True)
        
        # Write hours buckets
        if is_salaried:
            # Salaried employees get 0 hours
            _write_cell(ws, current_row, col_map["REGULAR"], None, is_numeric=True)
            _write_cell(ws, current_row, col_map["OVERTIME"], None, is_numeric=True)
            _write_cell(ws, current_row, col_map["DOUBLETIME"], None, is_numeric=True)
        else:
            _write_cell(ws, current_row, col_map["REGULAR"], reg, is_numeric=True)
            _write_cell(ws, current_row, col_map["OVERTIME"], ot, is_numeric=True)
            _write_cell(ws, current_row, col_map["DOUBLETIME"], dt, is_numeric=True)
        
        # Write optional pay buckets as blank
        for bucket in ["VACATION", "SICK", "HOLIDAY", "BONUS", "COMMISSION"]:
            if bucket in col_map:
                _write_cell(ws, current_row, col_map[bucket], None, is_numeric=True)
        
        # Write PC columns (only for PC type employees)
        is_pc = str(emp_type).strip().upper() == "PC"
        for day in WDAYS:
            hrs_col = f"PC HRS {day}"
            ttl_col = f"PC TTL {day}"
            
            if hrs_col in col_map:
                pc_hrs = pc_hours[day] if is_pc else 0.0
                _write_cell(ws, current_row, col_map[hrs_col], pc_hrs, is_numeric=True)
            
            if ttl_col in col_map:
                if is_pc and is_pc and pay_rate > 0:
                    pc_ttl = _round_to_2dec(pc_hours[day] * pay_rate)
                else:
                    pc_ttl = 0.0
                _write_cell(ws, current_row, col_map[ttl_col], pc_ttl, is_numeric=True)
        
        # Write totals (right-most Total/Totals column)
        total_columns = []
        for col_name in ["Totals", "Total"]:
            if col_name in col_map:
                total_columns.append((col_name, col_map[col_name]))
        
        if total_columns:
            # Use the right-most total column
            total_col_name, total_col_idx = max(total_columns, key=lambda x: x[1])
            
            if is_salaried:
                total_value = pay_rate  # Use pay rate directly for salaried
            else:
                # Calculate total: REG*Rate + OT*1.5*Rate + DT*2.0*Rate
                total_value = (
                    reg * pay_rate +
                    ot * 1.5 * pay_rate +
                    dt * 2.0 * pay_rate
                )
            
            _write_cell(ws, current_row, total_col_idx, _round_to_2dec(total_value), is_numeric=True)
        
        current_row += 1
    
    # Save to bytes
    output = io.BytesIO()
    wb.save(output)
    return output.getvalue()


# ---------- Public API ----------

def process_excel(sierra_file_bytes: bytes) -> bytes:
    """
    Main entry point for Sierra → WBS conversion.
    """
    # Load roster and template
    roster = load_roster("roster.xlsx")
    
    # Parse Sierra data
    sierra_df = parse_sierra(sierra_file_bytes)
    
    # Compute hours
    computed_hours = compute_employee_hours(sierra_df)
    
    # Write output
    return write_wbs_output("wbs_template.xlsx", roster, computed_hours)
