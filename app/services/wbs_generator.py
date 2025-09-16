"""
WBS Format Generator Service
Generates exact WBS payroll format with proper headers and column mapping
"""
import pandas as pd
from decimal import Decimal
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
from ..models.database import PayrollRecord, Employee

logger = logging.getLogger(__name__)

class WBSFormatGenerator:
    """Generates WBS format Excel files for payroll processing"""

    # WBS Format Constants
    WBS_VERSION = "B90216-00"
    CLIENT_ID = "055269"

    # Column mapping for WBS format
    WBS_COLUMNS = {
        'employee_number': 'A',      # Column A: Employee Number
        'regular_hours': 'A01',      # Column A01: Regular Hours
        'overtime_hours': 'A02',     # Column A02: Overtime Hours
        'pc_hrs_mon': 'AH1',         # Column AH1: Monday Piecework Hours
        'pc_rate_mon': 'AI1',        # Column AI1: Monday Piecework Rate
        'pc_hrs_tue': 'AH2',         # Column AH2: Tuesday Piecework Hours
        'pc_rate_tue': 'AI2',        # Column AI2: Tuesday Piecework Rate
        'pc_hrs_wed': 'AH3',         # Column AH3: Wednesday Piecework Hours
        'pc_rate_wed': 'AI3',        # Column AI3: Wednesday Piecework Rate
        'pc_hrs_thu': 'AH4',         # Column AH4: Thursday Piecework Hours
        'pc_rate_thu': 'AI4',        # Column AI4: Thursday Piecework Rate
        'pc_hrs_fri': 'AH5',         # Column AH5: Friday Piecework Hours
        'pc_rate_fri': 'AI5',        # Column AI5: Friday Piecework Rate
        'travel_time': 'B08',        # Column B08: Travel Time
        'pto_hours': 'E26'           # Column E26: PTO Hours
    }

    # WBS Header structure
    WBS_HEADERS = [
        "# V",    # Version identifier
        "# U",    # User identifier
        "# N",    # Name field
        "# P",    # Pay period
        "# R",    # Regular hours
        "# C",    # Company code
        "# B:8",  # Benefit code 8
        "# E:26"  # Earning code 26
    ]

    def __init__(self):
        """Initialize WBS Format Generator"""
        self.generated_files = []

    def generate_wbs_file(self, payroll_records: List[PayrollRecord], 
                         pay_period_start: datetime, 
                         pay_period_end: datetime,
                         output_path: str) -> str:
        """
        Generate complete WBS format Excel file

        Args:
            payroll_records: List of PayrollRecord objects
            pay_period_start: Start date of pay period
            pay_period_end: End date of pay period
            output_path: Path to save the generated file

        Returns:
            str: Path to generated file
        """
        try:
            logger.info(f"Generating WBS file for {len(payroll_records)} records")

            # Create WBS data structure
            wbs_data = self._build_wbs_data(payroll_records, pay_period_start, pay_period_end)

            # Convert to DataFrame with proper column structure
            df = self._create_wbs_dataframe(wbs_data)

            # Write to Excel with proper formatting
            self._write_wbs_excel(df, output_path, pay_period_start, pay_period_end)

            self.generated_files.append(output_path)
            logger.info(f"WBS file generated successfully: {output_path}")

            return output_path

        except Exception as e:
            logger.error(f"Error generating WBS file: {str(e)}")
            raise

    def _build_wbs_data(self, payroll_records: List[PayrollRecord], 
                       pay_period_start: datetime, 
                       pay_period_end: datetime) -> List[Dict[str, Any]]:
        """Build WBS data structure from payroll records"""
        wbs_data = []

        for record in payroll_records:
            # Basic employee data
            employee_data = {
                'employee_number': record.employee.employee_number,
                'employee_name': f"{record.employee.last_name}, {record.employee.first_name}",
                'ssn': record.employee.ssn,
                'department': record.employee.department,
                'pay_period_start': pay_period_start.strftime('%m/%d/%Y'),
                'pay_period_end': pay_period_end.strftime('%m/%d/%Y')
            }

            # Regular and overtime hours
            employee_data.update({
                'regular_hours': float(record.regular_hours or 0),
                'overtime_hours': float(record.overtime_hours or 0)
            })

            # Piecework hours and rates (daily breakdown)
            piecework_data = {
                'pc_hrs_mon': float(record.pc_hrs_mon or 0),
                'pc_rate_mon': float(record.pc_rate_mon or 0),
                'pc_hrs_tue': float(record.pc_hrs_tue or 0),
                'pc_rate_tue': float(record.pc_rate_tue or 0),
                'pc_hrs_wed': float(record.pc_hrs_wed or 0),
                'pc_rate_wed': float(record.pc_rate_wed or 0),
                'pc_hrs_thu': float(record.pc_hrs_thu or 0),
                'pc_rate_thu': float(record.pc_rate_thu or 0),
                'pc_hrs_fri': float(record.pc_hrs_fri or 0),
                'pc_rate_fri': float(record.pc_rate_fri or 0)
            }
            employee_data.update(piecework_data)

            # Additional time categories
            employee_data.update({
                'travel_time': float(record.travel_time or 0),
                'pto_hours': float(record.pto_hours or 0)
            })

            # Calculate totals for validation
            total_piecework_hours = sum([
                piecework_data['pc_hrs_mon'], piecework_data['pc_hrs_tue'],
                piecework_data['pc_hrs_wed'], piecework_data['pc_hrs_thu'],
                piecework_data['pc_hrs_fri']
            ])

            employee_data['total_piecework_hours'] = total_piecework_hours
            employee_data['total_hours'] = (
                employee_data['regular_hours'] + 
                employee_data['overtime_hours'] + 
                total_piecework_hours +
                employee_data['travel_time']
            )

            wbs_data.append(employee_data)

        return wbs_data

    def _create_wbs_dataframe(self, wbs_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """Create properly structured DataFrame for WBS format"""

        # Define exact column order for WBS format
        wbs_column_order = [
            'employee_number',    # Column A
            'employee_name',      # Employee name
            'ssn',               # Social Security Number
            'department',        # Department
            'pay_period_start',  # Pay period start
            'pay_period_end',    # Pay period end
            'regular_hours',     # A01: Regular Hours
            'overtime_hours',    # A02: Overtime Hours
            'pc_hrs_mon',        # AH1: Monday Piecework Hours
            'pc_rate_mon',       # AI1: Monday Piecework Rate
            'pc_hrs_tue',        # AH2: Tuesday Piecework Hours
            'pc_rate_tue',       # AI2: Tuesday Piecework Rate
            'pc_hrs_wed',        # AH3: Wednesday Piecework Hours
            'pc_rate_wed',       # AI3: Wednesday Piecework Rate
            'pc_hrs_thu',        # AH4: Thursday Piecework Hours
            'pc_rate_thu',       # AI4: Thursday Piecework Rate
            'pc_hrs_fri',        # AH5: Friday Piecework Hours
            'pc_rate_fri',       # AI5: Friday Piecework Rate
            'travel_time',       # B08: Travel Time
            'pto_hours',         # E26: PTO Hours
            'total_hours'        # Calculated total for validation
        ]

        # Create DataFrame with specified column order
        df = pd.DataFrame(wbs_data)

        # Reorder columns to match WBS format
        df = df.reindex(columns=wbs_column_order, fill_value=0)

        # Format numeric columns to proper decimal places
        numeric_columns = [
            'regular_hours', 'overtime_hours', 'travel_time', 'pto_hours',
            'pc_hrs_mon', 'pc_hrs_tue', 'pc_hrs_wed', 'pc_hrs_thu', 'pc_hrs_fri',
            'pc_rate_mon', 'pc_rate_tue', 'pc_rate_wed', 'pc_rate_thu', 'pc_rate_fri',
            'total_hours'
        ]

        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].round(3)  # 3 decimal places for hours

        # Format rate columns to 2 decimal places
        rate_columns = ['pc_rate_mon', 'pc_rate_tue', 'pc_rate_wed', 'pc_rate_thu', 'pc_rate_fri']
        for col in rate_columns:
            if col in df.columns:
                df[col] = df[col].round(2)  # 2 decimal places for rates

        return df

    def _write_wbs_excel(self, df: pd.DataFrame, output_path: str, 
                        pay_period_start: datetime, pay_period_end: datetime):
        """Write DataFrame to Excel with WBS formatting"""

        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:

            # Write header information
            header_df = pd.DataFrame([
                ['# V', self.WBS_VERSION],
                ['# U', self.CLIENT_ID],
                ['# N', 'Sierra Roofing Payroll'],
                ['# P', f"{pay_period_start.strftime('%m/%d/%Y')} - {pay_period_end.strftime('%m/%d/%Y')}"],
                ['# R', 'Regular Hours Processing'],
                ['# C', 'Company: Sierra Roofing'],
                ['# B:8', 'Travel Time Benefits'],
                ['# E:26', 'PTO Earnings'],
                ['', ''],  # Empty row separator
            ])

            # Write headers first
            header_df.to_excel(writer, sheet_name='WBS_Payroll', 
                             index=False, header=False, startrow=0)

            # Write main data starting after headers
            start_row = len(header_df) + 1
            df.to_excel(writer, sheet_name='WBS_Payroll', 
                       index=False, startrow=start_row)

            # Get workbook and worksheet for formatting
            workbook = writer.book
            worksheet = writer.sheets['WBS_Payroll']

            # Format headers with bold and background color
            from openpyxl.styles import Font, PatternFill
            header_font = Font(bold=True)
            header_fill = PatternFill(start_color='D3D3D3', end_color='D3D3D3', fill_type='solid')

            # Apply formatting to header rows
            for row in range(1, len(header_df) + 1):
                for col in range(1, 3):  # Headers are in first 2 columns
                    cell = worksheet.cell(row=row, column=col)
                    cell.font = header_font
                    cell.fill = header_fill

            # Format data header row
            data_header_row = start_row + 1
            for col in range(1, len(df.columns) + 1):
                cell = worksheet.cell(row=data_header_row, column=col)
                cell.font = header_font
                cell.fill = header_fill

            # Auto-adjust column widths
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)  # Cap at 50
                worksheet.column_dimensions[column_letter].width = adjusted_width

    def validate_wbs_data(self, payroll_records: List[PayrollRecord]) -> Dict[str, Any]:
        """Validate payroll data before generating WBS format"""
        validation_results = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'summary': {}
        }

        try:
            total_employees = len(payroll_records)
            total_regular_hours = 0
            total_overtime_hours = 0
            total_piecework_hours = 0
            employees_with_piecework = 0

            for record in payroll_records:
                # Check for required employee data
                if not record.employee.employee_number:
                    validation_results['errors'].append(f"Missing employee number for {record.employee.first_name} {record.employee.last_name}")

                if not record.employee.ssn:
                    validation_results['errors'].append(f"Missing SSN for employee {record.employee.employee_number}")

                # Accumulate hours for summary
                total_regular_hours += float(record.regular_hours or 0)
                total_overtime_hours += float(record.overtime_hours or 0)

                # Calculate piecework totals
                piecework_total = sum([
                    float(record.pc_hrs_mon or 0), float(record.pc_hrs_tue or 0),
                    float(record.pc_hrs_wed or 0), float(record.pc_hrs_thu or 0),
                    float(record.pc_hrs_fri or 0)
                ])
                total_piecework_hours += piecework_total

                if piecework_total > 0:
                    employees_with_piecework += 1

                # Validate piecework rates
                for day in ['mon', 'tue', 'wed', 'thu', 'fri']:
                    hours_attr = f'pc_hrs_{day}'
                    rate_attr = f'pc_rate_{day}'
                    hours = float(getattr(record, hours_attr, 0) or 0)
                    rate = float(getattr(record, rate_attr, 0) or 0)

                    if hours > 0 and rate == 0:
                        validation_results['warnings'].append(
                            f"Employee {record.employee.employee_number} has piecework hours on {day.capitalize()} but no rate"
                        )
                    elif hours == 0 and rate > 0:
                        validation_results['warnings'].append(
                            f"Employee {record.employee.employee_number} has piecework rate on {day.capitalize()} but no hours"
                        )

            # Set validation status
            if validation_results['errors']:
                validation_results['is_valid'] = False

            # Build summary
            validation_results['summary'] = {
                'total_employees': total_employees,
                'total_regular_hours': round(total_regular_hours, 2),
                'total_overtime_hours': round(total_overtime_hours, 2),
                'total_piecework_hours': round(total_piecework_hours, 2),
                'employees_with_piecework': employees_with_piecework,
                'total_hours': round(total_regular_hours + total_overtime_hours + total_piecework_hours, 2)
            }

        except Exception as e:
            validation_results['is_valid'] = False
            validation_results['errors'].append(f"Validation error: {str(e)}")

        return validation_results

    def get_wbs_preview(self, payroll_records: List[PayrollRecord], limit: int = 5) -> Dict[str, Any]:
        """Generate a preview of WBS format data for verification"""
        try:
            # Get first few records for preview
            preview_records = payroll_records[:limit]

            # Build preview data
            from datetime import datetime
            preview_data = self._build_wbs_data(preview_records, 
                                              datetime.now(), 
                                              datetime.now())

            return {
                'success': True,
                'preview_data': preview_data,
                'total_records': len(payroll_records),
                'preview_count': len(preview_data)
            }

        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'preview_data': [],
                'total_records': 0,
                'preview_count': 0
            }
