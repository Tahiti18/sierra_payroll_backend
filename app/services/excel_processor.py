import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
from datetime import datetime, date
from typing import Dict, List, Any, Optional, Tuple
import logging
from decimal import Decimal

logger = logging.getLogger(__name__)

class SierraExcelProcessor:
    """
    Processes Sierra Roofing timesheet Excel files with piecework detection.
    Handles the exact input format: Days, Job#, Name, Start, Lnch St, Lnch Fnsh, Finish, Hours, Rate, Total, Job Detail
    """

    def __init__(self):
        self.piecework_color_threshold = 0.8  # Green color detection threshold

    def process_sierra_file(self, file_path: str) -> Dict[str, Any]:
        """
        Process Sierra payroll Excel file and extract employee time data.

        Args:
            file_path: Path to the Sierra Excel file

        Returns:
            Dictionary containing processed employee data with piecework detection
        """
        try:
            # Load workbook for color detection
            workbook = load_workbook(file_path)
            worksheet = workbook.active

            # Load data with pandas
            df = pd.read_excel(file_path)

            # Clean column names
            df.columns = df.columns.str.strip()

            # Expected columns based on Sierra format
            expected_columns = [
                'Days', 'Job#', 'Name', 'Start', 'Lnch St.', 'Lnch Fnsh', 
                'Finish', 'Hours', 'Rate', 'Total', 'Job Detail'
            ]

            # Validate file structure
            self._validate_file_structure(df, expected_columns)

            # Process employee data
            employee_data = self._extract_employee_data(df, worksheet)

            # Detect piecework entries
            piecework_data = self._detect_piecework(worksheet, employee_data)

            # Calculate totals and validate
            processed_data = self._calculate_employee_totals(employee_data, piecework_data)

            return {
                'success': True,
                'employees': processed_data,
                'summary': {
                    'total_employees': len(processed_data),
                    'piecework_employees': len([e for e in processed_data if e.get('has_piecework', False)]),
                    'total_hours': sum(e.get('total_hours', 0) for e in processed_data),
                    'total_amount': sum(e.get('total_amount', 0) for e in processed_data)
                }
            }

        except Exception as e:
            logger.error(f"Error processing Sierra file: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'employees': []
            }

    def _validate_file_structure(self, df: pd.DataFrame, expected_columns: List[str]) -> None:
        """Validate that the Excel file has the expected Sierra format."""
        if df.empty:
            raise ValueError("Excel file is empty")

        # Check for required columns (flexible matching)
        required_cols = ['Name', 'Hours', 'Rate', 'Total']
        missing_cols = []

        for req_col in required_cols:
            found = False
            for col in df.columns:
                if req_col.lower() in str(col).lower():
                    found = True
                    break
            if not found:
                missing_cols.append(req_col)

        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

    def _extract_employee_data(self, df: pd.DataFrame, worksheet) -> Dict[str, Dict]:
        """Extract employee time entries from the dataframe."""
        employee_data = {}

        for index, row in df.iterrows():
            # Skip empty rows or header rows
            if pd.isna(row.get('Name', '')) or str(row.get('Name', '')).strip() == '':
                continue

            name = str(row['Name']).strip()

            # Skip non-employee rows (signatures, totals, etc.)
            if self._is_non_employee_row(name):
                continue

            # Parse time entry data
            try:
                hours = self._safe_float(row.get('Hours', 0))
                rate = self._safe_float(row.get('Rate', 0))
                total = self._safe_float(row.get('Total', 0))

                # Skip zero entries
                if hours <= 0 and total <= 0:
                    continue

                # Get date information
                date_info = self._extract_date_info(row, index + 2)  # +2 for Excel 1-based + header

                # Initialize employee if not exists
                if name not in employee_data:
                    employee_data[name] = {
                        'name': name,
                        'entries': [],
                        'total_hours': 0,
                        'total_amount': 0
                    }

                # Add time entry
                entry = {
                    'excel_row': index + 2,
                    'date': date_info,
                    'hours': hours,
                    'rate': rate,
                    'total': total,
                    'job_detail': str(row.get('Job Detail', '')).strip(),
                    'start_time': str(row.get('Start', '')).strip(),
                    'finish_time': str(row.get('Finish', '')).strip()
                }

                employee_data[name]['entries'].append(entry)
                employee_data[name]['total_hours'] += hours
                employee_data[name]['total_amount'] += total

            except Exception as e:
                logger.warning(f"Error processing row {index + 2}: {str(e)}")
                continue

        return employee_data

    def _detect_piecework(self, worksheet, employee_data: Dict) -> Dict[str, List]:
        """Detect piecework entries by analyzing cell background colors (green cells)."""
        piecework_entries = {}

        for name, emp_data in employee_data.items():
            piecework_list = []

            for entry in emp_data['entries']:
                row_num = entry['excel_row']

                # Check if any cell in the row has green background
                is_piecework = self._is_row_piecework(worksheet, row_num)

                if is_piecework:
                    piecework_list.append({
                        'entry': entry,
                        'weekday': self._get_weekday(entry['date']),
                        'effective_rate': entry['total'] / entry['hours'] if entry['hours'] > 0 else 0
                    })

            if piecework_list:
                piecework_entries[name] = piecework_list

        return piecework_entries

    def _is_row_piecework(self, worksheet, row_num: int) -> bool:
        """Check if a row contains piecework by examining cell background colors."""
        try:
            # Check cells in the row for green background
            for col in range(1, 15):  # Check first 14 columns
                cell = worksheet.cell(row=row_num, column=col)
                if cell.fill and hasattr(cell.fill, 'fgColor'):
                    # Check if color is green-ish
                    if self._is_green_color(cell.fill):
                        return True
            return False
        except Exception:
            return False

    def _is_green_color(self, fill) -> bool:
        """Check if a fill color is green (piecework indicator)."""
        if not fill or not hasattr(fill, 'fgColor') or not fill.fgColor:
            return False

        try:
            # Get RGB values
            rgb = fill.fgColor.rgb
            if rgb and len(rgb) >= 6:
                # Convert hex to RGB
                r = int(rgb[2:4], 16) / 255.0
                g = int(rgb[4:6], 16) / 255.0  
                b = int(rgb[0:2], 16) / 255.0

                # Check if green component is dominant
                return g > r and g > b and g > 0.5
        except Exception:
            pass

        return False

    def _calculate_employee_totals(self, employee_data: Dict, piecework_data: Dict) -> List[Dict]:
        """Calculate final employee totals with piecework breakdown."""
        processed_employees = []

        for name, emp_data in employee_data.items():
            employee = {
                'name': name,
                'total_hours': emp_data['total_hours'],
                'total_amount': emp_data['total_amount'],
                'has_piecework': name in piecework_data,
                'regular_hours': 0,
                'regular_amount': 0,
                'piecework_hours': 0,
                'piecework_amount': 0,
                'daily_piecework': {
                    'monday': {'hours': 0, 'amount': 0},
                    'tuesday': {'hours': 0, 'amount': 0},
                    'wednesday': {'hours': 0, 'amount': 0},
                    'thursday': {'hours': 0, 'amount': 0},
                    'friday': {'hours': 0, 'amount': 0}
                },
                'entries': emp_data['entries']
            }

            # Process piecework
            if name in piecework_data:
                for pc_entry in piecework_data[name]:
                    entry = pc_entry['entry']
                    weekday = pc_entry['weekday']

                    employee['piecework_hours'] += entry['hours']
                    employee['piecework_amount'] += entry['total']

                    # Add to daily breakdown
                    if weekday in employee['daily_piecework']:
                        employee['daily_piecework'][weekday]['hours'] += entry['hours']
                        employee['daily_piecework'][weekday]['amount'] += entry['total']

            # Calculate regular hours (non-piecework)
            employee['regular_hours'] = employee['total_hours'] - employee['piecework_hours']
            employee['regular_amount'] = employee['total_amount'] - employee['piecework_amount']

            processed_employees.append(employee)

        return processed_employees

    def _safe_float(self, value: Any) -> float:
        """Safely convert value to float."""
        if pd.isna(value):
            return 0.0
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0

    def _is_non_employee_row(self, name: str) -> bool:
        """Check if a row contains non-employee data."""
        name_lower = name.lower()
        skip_keywords = [
            'signature', 'date', 'week of', 'by the signature', 
            'gross', 'total', 'summary', 'report', 'nan', 'none'
        ]
        return any(keyword in name_lower for keyword in skip_keywords)

    def _extract_date_info(self, row: pd.Series, excel_row: int) -> Optional[date]:
        """Extract date information from the row."""
        # Try to find date in Days column or infer from context
        days_value = row.get('Days', '')

        if pd.isna(days_value):
            return None

        try:
            if isinstance(days_value, datetime):
                return days_value.date()
            elif isinstance(days_value, str):
                # Try to parse date string
                for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y']:
                    try:
                        return datetime.strptime(days_value.strip(), fmt).date()
                    except ValueError:
                        continue
        except Exception:
            pass

        return None

    def _get_weekday(self, date_obj: Optional[date]) -> str:
        """Get weekday name from date object."""
        if not date_obj:
            return 'unknown'

        weekdays = {
            0: 'monday', 1: 'tuesday', 2: 'wednesday', 
            3: 'thursday', 4: 'friday', 5: 'saturday', 6: 'sunday'
        }
        return weekdays.get(date_obj.weekday(), 'unknown')
