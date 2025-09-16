from datetime import datetime, date
from decimal import Decimal
from typing import Optional
from sqlalchemy import Column, Integer, String, DateTime, Date, DECIMAL, Text, Boolean, ForeignKey, Enum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import enum

Base = declarative_base()

class EmployeeStatus(enum.Enum):
    ACTIVE = "A"
    INACTIVE = "I" 
    TEMPORARY = "T"
    CONTRACTOR = "C"

class PayType(enum.Enum):
    HOURLY = "H"
    SALARY = "S" 
    CONTRACT = "C"
    EXEMPT = "E"

class Department(enum.Enum):
    ADMIN = "ADMIN"
    ROOF = "ROOF"
    GUTTR = "GUTTR"
    SOLAR = "SOLAR"

class Employee(Base):
    __tablename__ = "employees"

    id = Column(Integer, primary_key=True, index=True)
    employee_id = Column(String(10), unique=True, index=True)  # WBS ID like 0000662082
    ssn = Column(String(9), unique=True, index=True)  # 9 digits, stored without dashes
    name = Column(String(100), nullable=False)  # "Last, First" format for WBS compatibility
    status = Column(Enum(EmployeeStatus), default=EmployeeStatus.ACTIVE)
    pay_type = Column(Enum(PayType), nullable=False)
    pay_rate = Column(DECIMAL(10, 2), nullable=False)  # Base hourly rate or salary
    department = Column(Enum(Department), nullable=False)
    hire_date = Column(Date)
    termination_date = Column(Date, nullable=True)
    notes = Column(Text, nullable=True)

    # Audit fields
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    pay_rate_history = relationship("PayRateHistory", back_populates="employee")
    payroll_records = relationship("PayrollRecord", back_populates="employee")
    audit_logs = relationship("AuditLog", back_populates="employee")

class PayRateHistory(Base):
    __tablename__ = "pay_rate_history"

    id = Column(Integer, primary_key=True, index=True)
    employee_id = Column(Integer, ForeignKey("employees.id"))
    old_rate = Column(DECIMAL(10, 2), nullable=True)
    new_rate = Column(DECIMAL(10, 2), nullable=False)
    effective_date = Column(Date, nullable=False)
    reason = Column(String(255))
    approved_by = Column(String(100))
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    employee = relationship("Employee", back_populates="pay_rate_history")

class PayrollRecord(Base):
    __tablename__ = "payroll_records"

    id = Column(Integer, primary_key=True, index=True)
    employee_id = Column(Integer, ForeignKey("employees.id"))
    period_start = Column(Date, nullable=False)
    period_end = Column(Date, nullable=False)

    # Regular hours and pay
    regular_hours = Column(DECIMAL(5, 3), default=0)  # A01
    overtime_hours = Column(DECIMAL(5, 3), default=0)  # A02
    doubletime_hours = Column(DECIMAL(5, 3), default=0)  # A03
    vacation_hours = Column(DECIMAL(5, 3), default=0)  # A06
    sick_hours = Column(DECIMAL(5, 3), default=0)  # A07
    holiday_hours = Column(DECIMAL(5, 3), default=0)  # A08

    # Bonus and commission
    bonus_amount = Column(DECIMAL(10, 2), default=0)  # A04
    commission_amount = Column(DECIMAL(10, 2), default=0)  # A05

    # Daily piecework (Monday through Friday)
    pc_hrs_mon = Column(DECIMAL(5, 3), default=0)  # AH1
    pc_ttl_mon = Column(DECIMAL(10, 2), default=0)  # AI1
    pc_hrs_tue = Column(DECIMAL(5, 3), default=0)  # AH2
    pc_ttl_tue = Column(DECIMAL(10, 2), default=0)  # AI2
    pc_hrs_wed = Column(DECIMAL(5, 3), default=0)  # AH3
    pc_ttl_wed = Column(DECIMAL(10, 2), default=0)  # AI3
    pc_hrs_thu = Column(DECIMAL(5, 3), default=0)  # AH4
    pc_ttl_thu = Column(DECIMAL(10, 2), default=0)  # AI4
    pc_hrs_fri = Column(DECIMAL(5, 3), default=0)  # AH5
    pc_ttl_fri = Column(DECIMAL(10, 2), default=0)  # AI5

    # Travel and other
    travel_amount = Column(DECIMAL(10, 2), default=0)  # ATE

    # Totals
    total_hours = Column(DECIMAL(6, 3), default=0)
    total_amount = Column(DECIMAL(12, 2), default=0)  # Totals column

    # Comments and notes
    comments = Column(Text)

    # Processing metadata
    source_file = Column(String(255))  # Original Sierra file name
    processed_at = Column(DateTime, default=datetime.utcnow)
    processed_by = Column(String(100))

    # Audit
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    employee = relationship("Employee", back_populates="payroll_records")

class AuditLog(Base):
    __tablename__ = "audit_logs"

    id = Column(Integer, primary_key=True, index=True)
    employee_id = Column(Integer, ForeignKey("employees.id"), nullable=True)
    action = Column(String(50), nullable=False)  # CREATE, UPDATE, DELETE, PROCESS_PAYROLL
    table_name = Column(String(50))
    record_id = Column(Integer)
    old_values = Column(Text)  # JSON string of old values
    new_values = Column(Text)  # JSON string of new values
    user_id = Column(String(100))
    ip_address = Column(String(45))
    user_agent = Column(Text)
    timestamp = Column(DateTime, default=datetime.utcnow)

    # Relationships
    employee = relationship("Employee", back_populates="audit_logs")
