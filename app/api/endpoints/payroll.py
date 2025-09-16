"""
Payroll Processing API Endpoints
Handles Sierra Excel upload, processing, and WBS format generation
"""
from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timedelta
import logging
import os
import tempfile

from ...db.database import get_db
from ...models.database import PayrollRecord, Employee, AuditLog
from ...core.config import settings
from ...services.excel_processor import SierraExcelProcessor
from ...services.wbs_generator import WBSFormatGenerator

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/payroll", tags=["payroll"])

# Pydantic models
from pydantic import BaseModel, Field
from decimal import Decimal

class PayrollRecordResponse(BaseModel):
    id: int
    employee_id: int
    pay_period_start: datetime
    pay_period_end: datetime
    regular_hours: Optional[Decimal]
    overtime_hours: Optional[Decimal]
    pc_hrs_mon: Optional[Decimal]
    pc_rate_mon: Optional[Decimal]
    pc_hrs_tue: Optional[Decimal]
    pc_rate_tue: Optional[Decimal]
    pc_hrs_wed: Optional[Decimal]
    pc_rate_wed: Optional[Decimal]
    pc_hrs_thu: Optional[Decimal]
    pc_rate_thu: Optional[Decimal]
    pc_hrs_fri: Optional[Decimal]
    pc_rate_fri: Optional[Decimal]
    travel_time: Optional[Decimal]
    pto_hours: Optional[Decimal]
    created_at: datetime

    class Config:
        from_attributes = True

class PayrollProcessingResponse(BaseModel):
    success: bool
    records_processed: int
    records_created: int
    records_updated: int
    piecework_detected: int
    errors: List[str]
    warnings: List[str]
    pay_period_start: str
    pay_period_end: str

class WBSGenerationResponse(BaseModel):
    success: bool
    file_path: str
    records_count: int
    validation_results: dict
    file_size_bytes: int


@router.post("/upload-sierra-excel", response_model=PayrollProcessingResponse)
async def upload_sierra_excel(
    file: UploadFile = File(..., description="Sierra payroll Excel file"),
    pay_period_start: str = Query(..., description="Pay period start date (YYYY-MM-DD)"),
    pay_period_end: str = Query(..., description="Pay period end date (YYYY-MM-DD)"),
    update_existing: bool = Query(False, description="Update existing records for this pay period"),
    db: Session = Depends(get_db)
):
    """Upload and process Sierra payroll Excel file"""
    try:
        # Validate file format
        if not file.filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(status_code=400, detail="File must be Excel format (.xlsx or .xls)")

        # Parse dates
        try:
            period_start = datetime.strptime(pay_period_start, "%Y-%m-%d")
            period_end = datetime.strptime(pay_period_end, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

        # Save uploaded file temporarily
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as temp_file:
            contents = await file.read()
            temp_file.write(contents)
            temp_file_path = temp_file.name

        try:
            # Process Excel file
            processor = SierraExcelProcessor()
            processed_data = processor.process_sierra_excel(temp_file_path)

            response = PayrollProcessingResponse(
                success=True,
                records_processed=0,
                records_created=0,
                records_updated=0,
                piecework_detected=0,
                errors=[],
                warnings=[],
                pay_period_start=pay_period_start,
                pay_period_end=pay_period_end
            )

            # Process each employee's data
            for employee_data in processed_data:
                try:
                    response.records_processed += 1

                    # Find employee by name (Sierra format uses names, not employee numbers)
                    first_name = employee_data.get('first_name', '').strip()
                    last_name = employee_data.get('last_name', '').strip()

                    employee = db.query(Employee).filter(
                        Employee.first_name.ilike(first_name),
                        Employee.last_name.ilike(last_name),
                        Employee.is_active == True
                    ).first()

                    if not employee:
                        response.errors.append(f"Employee not found: {first_name} {last_name}")
                        continue

                    # Check for existing payroll record
                    existing_record = db.query(PayrollRecord).filter(
                        PayrollRecord.employee_id == employee.id,
                        PayrollRecord.pay_period_start == period_start,
                        PayrollRecord.pay_period_end == period_end
                    ).first()

                    if existing_record and not update_existing:
                        response.warnings.append(f"Payroll record exists for {employee.first_name} {employee.last_name} (skipped)")
                        continue

                    # Detect piecework
                    has_piecework = any([
                        employee_data.get('pc_hrs_mon', 0) > 0,
                        employee_data.get('pc_hrs_tue', 0) > 0,
                        employee_data.get('pc_hrs_wed', 0) > 0,
                        employee_data.get('pc_hrs_thu', 0) > 0,
                        employee_data.get('pc_hrs_fri', 0) > 0
                    ])

                    if has_piecework:
                        response.piecework_detected += 1

                    # Create or update payroll record
                    if existing_record:
                        # Update existing record
                        existing_record.regular_hours = employee_data.get('regular_hours', 0)
                        existing_record.overtime_hours = employee_data.get('overtime_hours', 0)
                        existing_record.pc_hrs_mon = employee_data.get('pc_hrs_mon', 0)
                        existing_record.pc_rate_mon = employee_data.get('pc_rate_mon', 0)
                        existing_record.pc_hrs_tue = employee_data.get('pc_hrs_tue', 0)
                        existing_record.pc_rate_tue = employee_data.get('pc_rate_tue', 0)
                        existing_record.pc_hrs_wed = employee_data.get('pc_hrs_wed', 0)
                        existing_record.pc_rate_wed = employee_data.get('pc_rate_wed', 0)
                        existing_record.pc_hrs_thu = employee_data.get('pc_hrs_thu', 0)
                        existing_record.pc_rate_thu = employee_data.get('pc_rate_thu', 0)
                        existing_record.pc_hrs_fri = employee_data.get('pc_hrs_fri', 0)
                        existing_record.pc_rate_fri = employee_data.get('pc_rate_fri', 0)
                        existing_record.travel_time = employee_data.get('travel_time', 0)
                        existing_record.pto_hours = employee_data.get('pto_hours', 0)
                        existing_record.updated_at = datetime.utcnow()

                        response.records_updated += 1
                    else:
                        # Create new record
                        payroll_record = PayrollRecord(
                            employee_id=employee.id,
                            pay_period_start=period_start,
                            pay_period_end=period_end,
                            regular_hours=employee_data.get('regular_hours', 0),
                            overtime_hours=employee_data.get('overtime_hours', 0),
                            pc_hrs_mon=employee_data.get('pc_hrs_mon', 0),
                            pc_rate_mon=employee_data.get('pc_rate_mon', 0),
                            pc_hrs_tue=employee_data.get('pc_hrs_tue', 0),
                            pc_rate_tue=employee_data.get('pc_rate_tue', 0),
                            pc_hrs_wed=employee_data.get('pc_hrs_wed', 0),
                            pc_rate_wed=employee_data.get('pc_rate_wed', 0),
                            pc_hrs_thu=employee_data.get('pc_hrs_thu', 0),
                            pc_rate_thu=employee_data.get('pc_rate_thu', 0),
                            pc_hrs_fri=employee_data.get('pc_hrs_fri', 0),
                            pc_rate_fri=employee_data.get('pc_rate_fri', 0),
                            travel_time=employee_data.get('travel_time', 0),
                            pto_hours=employee_data.get('pto_hours', 0)
                        )
                        db.add(payroll_record)
                        response.records_created += 1

                    # Create audit log
                    audit_log = AuditLog(
                        table_name="payroll_records",
                        record_id=existing_record.id if existing_record else None,
                        action="UPDATE" if existing_record else "CREATE",
                        old_values=None,
                        new_values={
                            "employee_name": f"{employee.first_name} {employee.last_name}",
                            "pay_period": f"{pay_period_start} to {pay_period_end}",
                            "has_piecework": has_piecework
                        },
                        user_id="system",
                        timestamp=datetime.utcnow()
                    )
                    db.add(audit_log)

                except Exception as row_error:
                    response.errors.append(f"Error processing {first_name} {last_name}: {str(row_error)}")
                    continue

            # Commit all changes
            db.commit()

            # Clean up temp file
            os.unlink(temp_file_path)

            logger.info(f"Sierra Excel processing completed: {response.records_created} created, {response.records_updated} updated")
            return response

        except Exception as processing_error:
            os.unlink(temp_file_path)
            raise processing_error

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing Sierra Excel: {str(e)}")
        raise HTTPException(status_code=500, detail="Error processing Sierra Excel file")


@router.post("/generate-wbs", response_model=WBSGenerationResponse)
async def generate_wbs_format(
    pay_period_start: str = Query(..., description="Pay period start date (YYYY-MM-DD)"),
    pay_period_end: str = Query(..., description="Pay period end date (YYYY-MM-DD)"),
    include_inactive: bool = Query(False, description="Include inactive employees"),
    db: Session = Depends(get_db)
):
    """Generate WBS format Excel file for payroll records"""
    try:
        # Parse dates
        try:
            period_start = datetime.strptime(pay_period_start, "%Y-%m-%d")
            period_end = datetime.strptime(pay_period_end, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

        # Get payroll records for the period
        query = db.query(PayrollRecord).filter(
            PayrollRecord.pay_period_start == period_start,
            PayrollRecord.pay_period_end == period_end
        )

        if not include_inactive:
            query = query.join(Employee).filter(Employee.is_active == True)

        payroll_records = query.all()

        if not payroll_records:
            raise HTTPException(status_code=404, detail="No payroll records found for the specified period")

        # Generate WBS file
        generator = WBSFormatGenerator()

        # Validate data before generation
        validation_results = generator.validate_wbs_data(payroll_records)

        if not validation_results['is_valid']:
            raise HTTPException(
                status_code=400, 
                detail=f"Validation failed: {'; '.join(validation_results['errors'])}"
            )

        # Create output filename
        output_filename = f"WBS_Payroll_{pay_period_start}_to_{pay_period_end}.xlsx"
        output_path = f"/home/user/output/{output_filename}"

        # Generate the file
        generated_file_path = generator.generate_wbs_file(
            payroll_records=payroll_records,
            pay_period_start=period_start,
            pay_period_end=period_end,
            output_path=output_path
        )

        # Get file size
        file_size = os.path.getsize(generated_file_path)

        response = WBSGenerationResponse(
            success=True,
            file_path=generated_file_path,
            records_count=len(payroll_records),
            validation_results=validation_results,
            file_size_bytes=file_size
        )

        logger.info(f"WBS file generated: {output_filename} with {len(payroll_records)} records")
        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating WBS format: {str(e)}")
        raise HTTPException(status_code=500, detail="Error generating WBS format")


@router.get("/records", response_model=List[PayrollRecordResponse])
async def get_payroll_records(
    pay_period_start: Optional[str] = Query(None, description="Filter by pay period start (YYYY-MM-DD)"),
    pay_period_end: Optional[str] = Query(None, description="Filter by pay period end (YYYY-MM-DD)"),
    employee_id: Optional[int] = Query(None, description="Filter by employee ID"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db)
):
    """Get payroll records with filtering"""
    try:
        query = db.query(PayrollRecord)

        # Apply filters
        if pay_period_start:
            period_start = datetime.strptime(pay_period_start, "%Y-%m-%d")
            query = query.filter(PayrollRecord.pay_period_start == period_start)

        if pay_period_end:
            period_end = datetime.strptime(pay_period_end, "%Y-%m-%d")
            query = query.filter(PayrollRecord.pay_period_end == period_end)

        if employee_id:
            query = query.filter(PayrollRecord.employee_id == employee_id)

        # Apply pagination
        records = query.offset(skip).limit(limit).all()

        return records

    except Exception as e:
        logger.error(f"Error retrieving payroll records: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving payroll records")


@router.get("/records/{record_id}", response_model=PayrollRecordResponse)
async def get_payroll_record(record_id: int, db: Session = Depends(get_db)):
    """Get specific payroll record by ID"""
    try:
        record = db.query(PayrollRecord).filter(PayrollRecord.id == record_id).first()

        if not record:
            raise HTTPException(status_code=404, detail="Payroll record not found")

        return record

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving payroll record {record_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving payroll record")


@router.get("/preview-wbs")
async def preview_wbs_data(
    pay_period_start: str = Query(..., description="Pay period start date (YYYY-MM-DD)"),
    pay_period_end: str = Query(..., description="Pay period end date (YYYY-MM-DD)"),
    limit: int = Query(5, ge=1, le=50, description="Number of records to preview"),
    db: Session = Depends(get_db)
):
    """Preview WBS format data before generation"""
    try:
        # Parse dates
        period_start = datetime.strptime(pay_period_start, "%Y-%m-%d")
        period_end = datetime.strptime(pay_period_end, "%Y-%m-%d")

        # Get payroll records
        payroll_records = db.query(PayrollRecord).filter(
            PayrollRecord.pay_period_start == period_start,
            PayrollRecord.pay_period_end == period_end
        ).limit(limit).all()

        if not payroll_records:
            return {"success": False, "message": "No payroll records found for preview"}

        # Generate preview
        generator = WBSFormatGenerator()
        preview_result = generator.get_wbs_preview(payroll_records, limit)

        return preview_result

    except Exception as e:
        logger.error(f"Error generating WBS preview: {str(e)}")
        raise HTTPException(status_code=500, detail="Error generating WBS preview")


@router.get("/summary")
async def get_payroll_summary(
    pay_period_start: Optional[str] = Query(None, description="Pay period start (YYYY-MM-DD)"),
    pay_period_end: Optional[str] = Query(None, description="Pay period end (YYYY-MM-DD)"),
    db: Session = Depends(get_db)
):
    """Get payroll summary statistics"""
    try:
        query = db.query(PayrollRecord)

        # Apply date filters if provided
        if pay_period_start:
            period_start = datetime.strptime(pay_period_start, "%Y-%m-%d")
            query = query.filter(PayrollRecord.pay_period_start == period_start)

        if pay_period_end:
            period_end = datetime.strptime(pay_period_end, "%Y-%m-%d")
            query = query.filter(PayrollRecord.pay_period_end == period_end)

        records = query.all()

        # Calculate summary statistics
        total_records = len(records)
        total_regular_hours = sum(float(r.regular_hours or 0) for r in records)
        total_overtime_hours = sum(float(r.overtime_hours or 0) for r in records)
        total_travel_time = sum(float(r.travel_time or 0) for r in records)
        total_pto_hours = sum(float(r.pto_hours or 0) for r in records)

        # Count piecework employees
        piecework_employees = 0
        total_piecework_hours = 0

        for record in records:
            piecework_total = sum([
                float(record.pc_hrs_mon or 0), float(record.pc_hrs_tue or 0),
                float(record.pc_hrs_wed or 0), float(record.pc_hrs_thu or 0),
                float(record.pc_hrs_fri or 0)
            ])
            if piecework_total > 0:
                piecework_employees += 1
                total_piecework_hours += piecework_total

        summary = {
            "total_records": total_records,
            "total_regular_hours": round(total_regular_hours, 2),
            "total_overtime_hours": round(total_overtime_hours, 2),
            "total_piecework_hours": round(total_piecework_hours, 2),
            "total_travel_time": round(total_travel_time, 2),
            "total_pto_hours": round(total_pto_hours, 2),
            "total_hours": round(total_regular_hours + total_overtime_hours + total_piecework_hours + total_travel_time, 2),
            "piecework_employees": piecework_employees,
            "pay_period_start": pay_period_start,
            "pay_period_end": pay_period_end
        }

        return {"success": True, "summary": summary}

    except Exception as e:
        logger.error(f"Error generating payroll summary: {str(e)}")
        raise HTTPException(status_code=500, detail="Error generating payroll summary")
