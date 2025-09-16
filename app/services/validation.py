"""
Validation and Audit Trail System
Comprehensive data validation with audit logging
"""
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from decimal import Decimal
from sqlalchemy.orm import Session

from ..models.database import Employee, PayrollRecord, AuditLog

logger = logging.getLogger(__name__)

class PayrollValidator:
    """Validates payroll data integrity and business rules"""

    def __init__(self, db_session: Session):
        self.db = db_session
        self.validation_errors = []
        self.validation_warnings = []

    def validate_employee_data(self, employee_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate employee data against business rules"""
        results = {
            "is_valid": True,
            "errors": [],
            "warnings": []
        }

        # Required field validation
        required_fields = ["employee_number", "first_name", "last_name", "ssn", "regular_rate"]
        for field in required_fields:
            if not employee_data.get(field):
                results["errors"].append(f"Missing required field: {field}")

        # SSN format validation
        ssn = employee_data.get("ssn", "")
        if ssn and not self._validate_ssn_format(ssn):
            results["errors"].append("Invalid SSN format. Use XXX-XX-XXXX")

        # Rate validation
        regular_rate = employee_data.get("regular_rate", 0)
        if regular_rate and (regular_rate <= 0 or regular_rate > 200):
            results["errors"].append("Regular rate must be between $0.01 and $200.00")

        overtime_rate = employee_data.get("overtime_rate")
        if overtime_rate and (overtime_rate <= 0 or overtime_rate > 300):
            results["errors"].append("Overtime rate must be between $0.01 and $300.00")

        # Check for duplicate employee number
        if employee_data.get("employee_number"):
            existing = self.db.query(Employee).filter(
                Employee.employee_number == employee_data["employee_number"]
            ).first()
            if existing:
                results["errors"].append(f"Employee number {employee_data['employee_number']} already exists")

        # Check for duplicate SSN (active employees only)
        if ssn:
            existing_ssn = self.db.query(Employee).filter(
                Employee.ssn == ssn,
                Employee.is_active == True
            ).first()
            if existing_ssn:
                results["errors"].append(f"SSN already exists for active employee: {existing_ssn.employee_number}")

        if results["errors"]:
            results["is_valid"] = False

        return results

    def validate_payroll_record(self, payroll_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate payroll record data"""
        results = {
            "is_valid": True,
            "errors": [],
            "warnings": []
        }

        # Hours validation
        regular_hours = float(payroll_data.get("regular_hours", 0))
        overtime_hours = float(payroll_data.get("overtime_hours", 0))

        if regular_hours < 0 or regular_hours > 80:
            results["errors"].append("Regular hours must be between 0 and 80")

        if overtime_hours < 0 or overtime_hours > 40:
            results["errors"].append("Overtime hours must be between 0 and 40")

        # Total hours check
        total_regular_overtime = regular_hours + overtime_hours
        if total_regular_overtime > 84:  # 12 hours per day * 7 days
            results["warnings"].append(f"Total regular + overtime hours ({total_regular_overtime}) exceeds typical limits")

        # Piecework validation
        piecework_days = ['mon', 'tue', 'wed', 'thu', 'fri']
        total_piecework_hours = 0

        for day in piecework_days:
            hours = float(payroll_data.get(f"pc_hrs_{day}", 0))
            rate = float(payroll_data.get(f"pc_rate_{day}", 0))

            if hours < 0 or hours > 16:
                results["errors"].append(f"Piecework hours for {day} must be between 0 and 16")

            if hours > 0 and rate <= 0:
                results["errors"].append(f"Piecework rate required for {day} when hours > 0")

            if hours == 0 and rate > 0:
                results["warnings"].append(f"Piecework rate specified for {day} but no hours")

            total_piecework_hours += hours

        # Check for conflicting regular and piecework hours
        if regular_hours > 0 and total_piecework_hours > 0:
            results["warnings"].append("Employee has both regular hours and piecework - verify this is correct")

        # Travel time validation
        travel_time = float(payroll_data.get("travel_time", 0))
        if travel_time < 0 or travel_time > 40:
            results["errors"].append("Travel time must be between 0 and 40 hours")

        # PTO validation
        pto_hours = float(payroll_data.get("pto_hours", 0))
        if pto_hours < 0 or pto_hours > 80:
            results["errors"].append("PTO hours must be between 0 and 80")

        if results["errors"]:
            results["is_valid"] = False

        return results

    def validate_pay_period_consistency(self, pay_period_start: datetime, 
                                      pay_period_end: datetime) -> Dict[str, Any]:
        """Validate pay period dates"""
        results = {
            "is_valid": True,
            "errors": [],
            "warnings": []
        }

        # Date order validation
        if pay_period_start >= pay_period_end:
            results["errors"].append("Pay period start must be before end date")

        # Period length validation (should be 7 days for weekly payroll)
        period_days = (pay_period_end - pay_period_start).days
        if period_days != 6:  # 7-day period (end date is inclusive)
            results["warnings"].append(f"Pay period is {period_days + 1} days (expected 7 for weekly payroll)")

        # Check for overlapping periods
        existing_records = self.db.query(PayrollRecord).filter(
            PayrollRecord.pay_period_start == pay_period_start,
            PayrollRecord.pay_period_end == pay_period_end
        ).count()

        if existing_records > 0:
            results["warnings"].append(f"Found {existing_records} existing records for this pay period")

        if results["errors"]:
            results["is_valid"] = False

        return results

    def _validate_ssn_format(self, ssn: str) -> bool:
        """Validate SSN format (XXX-XX-XXXX)"""
        import re
        pattern = r"^\d{3}-\d{2}-\d{4}$"
        return bool(re.match(pattern, ssn))

    def validate_bulk_import(self, import_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate bulk import data"""
        results = {
            "is_valid": True,
            "total_records": len(import_data),
            "valid_records": 0,
            "invalid_records": 0,
            "errors": [],
            "warnings": []
        }

        employee_numbers_seen = set()
        ssns_seen = set()

        for i, record in enumerate(import_data):
            record_errors = []

            # Check for duplicate employee numbers in batch
            emp_num = record.get("employee_number")
            if emp_num in employee_numbers_seen:
                record_errors.append(f"Duplicate employee number in batch: {emp_num}")
            else:
                employee_numbers_seen.add(emp_num)

            # Check for duplicate SSNs in batch
            ssn = record.get("ssn")
            if ssn in ssns_seen:
                record_errors.append(f"Duplicate SSN in batch: {ssn}")
            else:
                ssns_seen.add(ssn)

            # Validate individual record
            validation_result = self.validate_employee_data(record)
            record_errors.extend(validation_result["errors"])

            if record_errors:
                results["invalid_records"] += 1
                results["errors"].append(f"Row {i + 1}: {'; '.join(record_errors)}")
            else:
                results["valid_records"] += 1

        if results["invalid_records"] > 0:
            results["is_valid"] = False

        return results


class AuditTrailManager:
    """Manages audit trail logging for all system operations"""

    def __init__(self, db_session: Session):
        self.db = db_session

    def log_operation(self, table_name: str, record_id: Optional[int], 
                     action: str, old_values: Optional[Dict[str, Any]], 
                     new_values: Optional[Dict[str, Any]], user_id: str = "system"):
        """Log an operation to the audit trail"""
        try:
            audit_log = AuditLog(
                table_name=table_name,
                record_id=record_id,
                action=action,
                old_values=old_values,
                new_values=new_values,
                user_id=user_id,
                timestamp=datetime.utcnow()
            )

            self.db.add(audit_log)
            self.db.flush()  # Don't commit yet, let the calling function handle it

            logger.info(f"Audit log created: {action} on {table_name} (ID: {record_id})")

        except Exception as e:
            logger.error(f"Error creating audit log: {str(e)}")
            # Don't raise exception - audit logging shouldn't break main operations

    def get_audit_trail(self, table_name: Optional[str] = None, 
                       record_id: Optional[int] = None,
                       user_id: Optional[str] = None,
                       limit: int = 100) -> List[AuditLog]:
        """Retrieve audit trail records with filtering"""
        try:
            query = self.db.query(AuditLog)

            if table_name:
                query = query.filter(AuditLog.table_name == table_name)

            if record_id:
                query = query.filter(AuditLog.record_id == record_id)

            if user_id:
                query = query.filter(AuditLog.user_id == user_id)

            return query.order_by(AuditLog.timestamp.desc()).limit(limit).all()

        except Exception as e:
            logger.error(f"Error retrieving audit trail: {str(e)}")
            return []

    def get_record_history(self, table_name: str, record_id: int) -> List[AuditLog]:
        """Get complete history for a specific record"""
        return self.get_audit_trail(table_name=table_name, record_id=record_id, limit=1000)


class DataQualityChecker:
    """Checks data quality and consistency across the system"""

    def __init__(self, db_session: Session):
        self.db = db_session

    def check_employee_data_quality(self) -> Dict[str, Any]:
        """Check overall employee data quality"""
        results = {
            "total_employees": 0,
            "active_employees": 0,
            "issues": [],
            "warnings": []
        }

        try:
            # Get all employees
            employees = self.db.query(Employee).all()
            results["total_employees"] = len(employees)
            results["active_employees"] = len([e for e in employees if e.is_active])

            # Check for data quality issues
            for employee in employees:
                # Missing or invalid rates
                if not employee.regular_rate or employee.regular_rate <= 0:
                    results["issues"].append(f"Employee {employee.employee_number}: Invalid regular rate")

                # Missing department
                if not employee.department or employee.department.strip() == "":
                    results["issues"].append(f"Employee {employee.employee_number}: Missing department")

                # Suspicious rates (too high or too low)
                if employee.regular_rate and employee.regular_rate < 7.25:  # Below federal minimum wage
                    results["warnings"].append(f"Employee {employee.employee_number}: Rate below minimum wage")

                if employee.regular_rate and employee.regular_rate > 150:  # Very high rate
                    results["warnings"].append(f"Employee {employee.employee_number}: Very high rate ({employee.regular_rate})")

            return results

        except Exception as e:
            logger.error(f"Error checking employee data quality: {str(e)}")
            return {"error": str(e)}

    def check_payroll_data_consistency(self, pay_period_start: datetime, 
                                     pay_period_end: datetime) -> Dict[str, Any]:
        """Check payroll data consistency for a pay period"""
        results = {
            "pay_period": f"{pay_period_start.date()} to {pay_period_end.date()}",
            "total_records": 0,
            "issues": [],
            "warnings": [],
            "summary": {}
        }

        try:
            # Get payroll records for the period
            records = self.db.query(PayrollRecord).filter(
                PayrollRecord.pay_period_start == pay_period_start,
                PayrollRecord.pay_period_end == pay_period_end
            ).all()

            results["total_records"] = len(records)

            if not records:
                results["warnings"].append("No payroll records found for this period")
                return results

            # Check for consistency issues
            total_hours = 0
            piecework_count = 0

            for record in records:
                # Check for negative values
                if record.regular_hours and record.regular_hours < 0:
                    results["issues"].append(f"Employee {record.employee.employee_number}: Negative regular hours")

                if record.overtime_hours and record.overtime_hours < 0:
                    results["issues"].append(f"Employee {record.employee.employee_number}: Negative overtime hours")

                # Calculate totals
                reg_hours = float(record.regular_hours or 0)
                ot_hours = float(record.overtime_hours or 0)
                total_hours += reg_hours + ot_hours

                # Check piecework
                piecework_total = sum([
                    float(record.pc_hrs_mon or 0), float(record.pc_hrs_tue or 0),
                    float(record.pc_hrs_wed or 0), float(record.pc_hrs_thu or 0),
                    float(record.pc_hrs_fri or 0)
                ])

                if piecework_total > 0:
                    piecework_count += 1
                    total_hours += piecework_total

                # Check for excessive hours
                employee_total = reg_hours + ot_hours + piecework_total
                if employee_total > 80:
                    results["warnings"].append(f"Employee {record.employee.employee_number}: High total hours ({employee_total})")

            results["summary"] = {
                "total_hours": round(total_hours, 2),
                "average_hours_per_employee": round(total_hours / len(records), 2) if records else 0,
                "employees_with_piecework": piecework_count
            }

            return results

        except Exception as e:
            logger.error(f"Error checking payroll data consistency: {str(e)}")
            return {"error": str(e)}
