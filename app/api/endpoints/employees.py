"""
Employee Management API Endpoints
Provides CRUD operations for employee data with proper validation
"""
from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime
import logging

from ...db.database import get_db
from ...models.database import Employee, PayRateHistory, AuditLog
from ...core.config import settings
from ...services.excel_processor import SierraExcelProcessor
from ...services.wbs_generator import WBSFormatGenerator

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/employees", tags=["employees"])

# Pydantic models for request/response
from pydantic import BaseModel, Field
from decimal import Decimal

class EmployeeBase(BaseModel):
    employee_number: str = Field(..., description="Unique employee number")
    first_name: str = Field(..., min_length=1, max_length=50)
    last_name: str = Field(..., min_length=1, max_length=50)
    ssn: str = Field(..., regex=r"^\d{3}-\d{2}-\d{4}$", description="Format: XXX-XX-XXXX")
    department: str = Field(..., max_length=100)
    regular_rate: Decimal = Field(..., gt=0, description="Regular hourly rate")
    overtime_rate: Optional[Decimal] = Field(None, gt=0)
    is_active: bool = Field(default=True)

class EmployeeCreate(EmployeeBase):
    pass

class EmployeeUpdate(BaseModel):
    first_name: Optional[str] = Field(None, min_length=1, max_length=50)
    last_name: Optional[str] = Field(None, min_length=1, max_length=50)
    ssn: Optional[str] = Field(None, regex=r"^\d{3}-\d{2}-\d{4}$")
    department: Optional[str] = Field(None, max_length=100)
    regular_rate: Optional[Decimal] = Field(None, gt=0)
    overtime_rate: Optional[Decimal] = Field(None, gt=0)
    is_active: Optional[bool] = None

class EmployeeResponse(EmployeeBase):
    id: int
    created_at: datetime
    updated_at: Optional[datetime]

    class Config:
        from_attributes = True

class PayRateHistoryResponse(BaseModel):
    id: int
    employee_id: int
    rate_type: str
    old_rate: Optional[Decimal]
    new_rate: Decimal
    effective_date: datetime
    created_by: str

    class Config:
        from_attributes = True

class BulkEmployeeUploadResponse(BaseModel):
    success: bool
    total_processed: int
    employees_created: int
    employees_updated: int
    errors: List[str]
    warnings: List[str]


@router.get("/", response_model=List[EmployeeResponse])
async def get_employees(
    skip: int = Query(0, ge=0, description="Number of employees to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of employees to return"),
    active_only: bool = Query(True, description="Return only active employees"),
    department: Optional[str] = Query(None, description="Filter by department"),
    search: Optional[str] = Query(None, description="Search by name or employee number"),
    db: Session = Depends(get_db)
):
    """Get list of employees with filtering and pagination"""
    try:
        query = db.query(Employee)

        # Apply filters
        if active_only:
            query = query.filter(Employee.is_active == True)

        if department:
            query = query.filter(Employee.department.ilike(f"%{department}%"))

        if search:
            search_pattern = f"%{search}%"
            query = query.filter(
                (Employee.first_name.ilike(search_pattern)) |
                (Employee.last_name.ilike(search_pattern)) |
                (Employee.employee_number.ilike(search_pattern))
            )

        # Apply pagination
        employees = query.offset(skip).limit(limit).all()

        logger.info(f"Retrieved {len(employees)} employees")
        return employees

    except Exception as e:
        logger.error(f"Error retrieving employees: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving employees")


@router.get("/{employee_id}", response_model=EmployeeResponse)
async def get_employee(employee_id: int, db: Session = Depends(get_db)):
    """Get specific employee by ID"""
    try:
        employee = db.query(Employee).filter(Employee.id == employee_id).first()

        if not employee:
            raise HTTPException(status_code=404, detail="Employee not found")

        return employee

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving employee {employee_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving employee")


@router.get("/number/{employee_number}", response_model=EmployeeResponse)
async def get_employee_by_number(employee_number: str, db: Session = Depends(get_db)):
    """Get specific employee by employee number"""
    try:
        employee = db.query(Employee).filter(Employee.employee_number == employee_number).first()

        if not employee:
            raise HTTPException(status_code=404, detail="Employee not found")

        return employee

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving employee {employee_number}: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving employee")


@router.post("/", response_model=EmployeeResponse)
async def create_employee(employee_data: EmployeeCreate, db: Session = Depends(get_db)):
    """Create new employee"""
    try:
        # Check if employee number already exists
        existing = db.query(Employee).filter(Employee.employee_number == employee_data.employee_number).first()
        if existing:
            raise HTTPException(status_code=400, detail="Employee number already exists")

        # Check if SSN already exists (for active employees)
        existing_ssn = db.query(Employee).filter(
            Employee.ssn == employee_data.ssn,
            Employee.is_active == True
        ).first()
        if existing_ssn:
            raise HTTPException(status_code=400, detail="SSN already exists for active employee")

        # Create employee
        employee = Employee(**employee_data.dict())
        db.add(employee)
        db.flush()  # Get the ID without committing

        # Create initial pay rate history
        pay_rate_history = PayRateHistory(
            employee_id=employee.id,
            rate_type="regular",
            new_rate=employee_data.regular_rate,
            effective_date=datetime.utcnow(),
            created_by="system"
        )
        db.add(pay_rate_history)

        if employee_data.overtime_rate:
            overtime_history = PayRateHistory(
                employee_id=employee.id,
                rate_type="overtime",
                new_rate=employee_data.overtime_rate,
                effective_date=datetime.utcnow(),
                created_by="system"
            )
            db.add(overtime_history)

        # Create audit log
        audit_log = AuditLog(
            table_name="employees",
            record_id=employee.id,
            action="CREATE",
            old_values=None,
            new_values=employee_data.dict(),
            user_id="system",
            timestamp=datetime.utcnow()
        )
        db.add(audit_log)

        db.commit()
        db.refresh(employee)

        logger.info(f"Created employee {employee.employee_number}: {employee.first_name} {employee.last_name}")
        return employee

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Error creating employee: {str(e)}")
        raise HTTPException(status_code=500, detail="Error creating employee")


@router.put("/{employee_id}", response_model=EmployeeResponse)
async def update_employee(employee_id: int, employee_data: EmployeeUpdate, db: Session = Depends(get_db)):
    """Update existing employee"""
    try:
        employee = db.query(Employee).filter(Employee.id == employee_id).first()
        if not employee:
            raise HTTPException(status_code=404, detail="Employee not found")

        # Store old values for audit
        old_values = {
            "first_name": employee.first_name,
            "last_name": employee.last_name,
            "ssn": employee.ssn,
            "department": employee.department,
            "regular_rate": float(employee.regular_rate),
            "overtime_rate": float(employee.overtime_rate) if employee.overtime_rate else None,
            "is_active": employee.is_active
        }

        # Update fields
        update_data = employee_data.dict(exclude_unset=True)

        # Check for SSN conflicts if SSN is being updated
        if "ssn" in update_data and update_data["ssn"] != employee.ssn:
            existing_ssn = db.query(Employee).filter(
                Employee.ssn == update_data["ssn"],
                Employee.is_active == True,
                Employee.id != employee_id
            ).first()
            if existing_ssn:
                raise HTTPException(status_code=400, detail="SSN already exists for another active employee")

        # Handle rate changes with history tracking
        rate_changes = []

        if "regular_rate" in update_data and update_data["regular_rate"] != employee.regular_rate:
            rate_changes.append({
                "rate_type": "regular",
                "old_rate": employee.regular_rate,
                "new_rate": update_data["regular_rate"]
            })

        if "overtime_rate" in update_data and update_data["overtime_rate"] != employee.overtime_rate:
            rate_changes.append({
                "rate_type": "overtime",
                "old_rate": employee.overtime_rate,
                "new_rate": update_data["overtime_rate"]
            })

        # Apply updates
        for field, value in update_data.items():
            setattr(employee, field, value)

        employee.updated_at = datetime.utcnow()

        # Create pay rate history entries
        for rate_change in rate_changes:
            pay_rate_history = PayRateHistory(
                employee_id=employee.id,
                rate_type=rate_change["rate_type"],
                old_rate=rate_change["old_rate"],
                new_rate=rate_change["new_rate"],
                effective_date=datetime.utcnow(),
                created_by="system"
            )
            db.add(pay_rate_history)

        # Create audit log
        audit_log = AuditLog(
            table_name="employees",
            record_id=employee.id,
            action="UPDATE",
            old_values=old_values,
            new_values=update_data,
            user_id="system",
            timestamp=datetime.utcnow()
        )
        db.add(audit_log)

        db.commit()
        db.refresh(employee)

        logger.info(f"Updated employee {employee.employee_number}")
        return employee

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Error updating employee {employee_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Error updating employee")


@router.delete("/{employee_id}")
async def delete_employee(employee_id: int, permanent: bool = Query(False), db: Session = Depends(get_db)):
    """Delete employee (soft delete by default, permanent if specified)"""
    try:
        employee = db.query(Employee).filter(Employee.id == employee_id).first()
        if not employee:
            raise HTTPException(status_code=404, detail="Employee not found")

        if permanent:
            # Permanent deletion
            old_values = {
                "employee_number": employee.employee_number,
                "first_name": employee.first_name,
                "last_name": employee.last_name,
                "ssn": employee.ssn
            }

            db.delete(employee)
            action = "DELETE_PERMANENT"
            message = f"Permanently deleted employee {employee.employee_number}"
        else:
            # Soft delete
            old_values = {"is_active": employee.is_active}
            employee.is_active = False
            employee.updated_at = datetime.utcnow()
            action = "DELETE_SOFT"
            message = f"Deactivated employee {employee.employee_number}"

        # Create audit log
        audit_log = AuditLog(
            table_name="employees",
            record_id=employee.id,
            action=action,
            old_values=old_values,
            new_values={"is_active": False} if not permanent else None,
            user_id="system",
            timestamp=datetime.utcnow()
        )
        db.add(audit_log)

        db.commit()

        logger.info(message)
        return {"success": True, "message": message}

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Error deleting employee {employee_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Error deleting employee")


@router.get("/{employee_id}/pay-rate-history", response_model=List[PayRateHistoryResponse])
async def get_employee_pay_rate_history(employee_id: int, db: Session = Depends(get_db)):
    """Get pay rate history for specific employee"""
    try:
        employee = db.query(Employee).filter(Employee.id == employee_id).first()
        if not employee:
            raise HTTPException(status_code=404, detail="Employee not found")

        history = db.query(PayRateHistory).filter(
            PayRateHistory.employee_id == employee_id
        ).order_by(PayRateHistory.effective_date.desc()).all()

        return history

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving pay rate history for employee {employee_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving pay rate history")


@router.post("/bulk-upload", response_model=BulkEmployeeUploadResponse)
async def bulk_upload_employees(
    file: UploadFile = File(..., description="Excel file with employee data"),
    update_existing: bool = Query(False, description="Update existing employees if found"),
    db: Session = Depends(get_db)
):
    """Bulk upload employees from Excel file"""
    try:
        if not file.filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(status_code=400, detail="File must be Excel format (.xlsx or .xls)")

        # Save uploaded file temporarily
        import tempfile
        import os

        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as temp_file:
            contents = await file.read()
            temp_file.write(contents)
            temp_file_path = temp_file.name

        try:
            # Process Excel file
            processor = SierraExcelProcessor()
            excel_data = processor.read_excel_file(temp_file_path)

            results = {
                "success": True,
                "total_processed": 0,
                "employees_created": 0,
                "employees_updated": 0,
                "errors": [],
                "warnings": []
            }

            for row_data in excel_data:
                try:
                    results["total_processed"] += 1

                    # Extract employee data from row
                    employee_number = str(row_data.get('employee_number', '')).strip()
                    first_name = str(row_data.get('first_name', '')).strip()
                    last_name = str(row_data.get('last_name', '')).strip()
                    ssn = str(row_data.get('ssn', '')).strip()
                    department = str(row_data.get('department', '')).strip()
                    regular_rate = float(row_data.get('regular_rate', 0))

                    if not all([employee_number, first_name, last_name, ssn]):
                        results["errors"].append(f"Row {results['total_processed']}: Missing required fields")
                        continue

                    # Check if employee exists
                    existing = db.query(Employee).filter(Employee.employee_number == employee_number).first()

                    if existing and update_existing:
                        # Update existing employee
                        existing.first_name = first_name
                        existing.last_name = last_name
                        existing.ssn = ssn
                        existing.department = department
                        existing.regular_rate = regular_rate
                        existing.updated_at = datetime.utcnow()

                        results["employees_updated"] += 1

                    elif existing and not update_existing:
                        results["warnings"].append(f"Employee {employee_number} already exists (skipped)")
                        continue

                    else:
                        # Create new employee
                        employee = Employee(
                            employee_number=employee_number,
                            first_name=first_name,
                            last_name=last_name,
                            ssn=ssn,
                            department=department,
                            regular_rate=regular_rate
                        )
                        db.add(employee)
                        results["employees_created"] += 1

                except Exception as row_error:
                    results["errors"].append(f"Row {results['total_processed']}: {str(row_error)}")
                    continue

            db.commit()

            # Clean up temp file
            os.unlink(temp_file_path)

            logger.info(f"Bulk upload completed: {results['employees_created']} created, {results['employees_updated']} updated")
            return results

        except Exception as processing_error:
            os.unlink(temp_file_path)  # Clean up temp file
            raise processing_error

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in bulk upload: {str(e)}")
        raise HTTPException(status_code=500, detail="Error processing bulk upload")


@router.get("/departments/list")
async def get_departments(db: Session = Depends(get_db)):
    """Get list of all departments"""
    try:
        departments = db.query(Employee.department).filter(
            Employee.department.isnot(None),
            Employee.is_active == True
        ).distinct().all()

        department_list = [dept[0] for dept in departments if dept[0]]
        return {"departments": sorted(department_list)}

    except Exception as e:
        logger.error(f"Error retrieving departments: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving departments")
