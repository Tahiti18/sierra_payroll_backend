# Sierra Roofing Payroll Backend

Enterprise-grade FastAPI backend system for automating Sierra Roofing payroll processing. Converts Sierra Excel timesheets to WBS payroll format with piecework detection and comprehensive audit trails.

## Features

- **Employee Management**: Full CRUD operations with pay rate history
- **Sierra Excel Processing**: Automatic piecework detection via green cell highlighting
- **WBS Format Generation**: Exact column mapping (A01, A02, AH1-AH5/AI1-AI5)
- **Validation System**: Comprehensive business rule validation
- **Audit Trail**: Complete operation logging and history
- **Enterprise Security**: Input validation, error handling, and logging

## Quick Deployment to Railway

1. **Create Railway Project**
   ```bash
   npm install -g @railway/cli
   railway login
   railway init
   ```

2. **Deploy Backend**
   ```bash
   # In sierra_payroll_backend directory
   railway up
   ```

3. **Set Environment Variables**
   ```bash
   railway variables set DATABASE_URL=<your_postgresql_url>
   railway variables set SECRET_KEY=<generate_secure_key>
   railway variables set ENVIRONMENT=production
   ```

## Local Development

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set Environment Variables**
   ```bash
   cp .env.example .env
   # Edit .env with your database settings
   ```

3. **Run Application**
   ```bash
   uvicorn app.main:app --reload --port 8000
   ```

4. **Access API Documentation**
   - Interactive docs: http://localhost:8000/docs
   - ReDoc: http://localhost:8000/redoc

## API Endpoints

### Employee Management
- `GET /api/employees` - List employees with filtering
- `POST /api/employees` - Create new employee
- `PUT /api/employees/{id}` - Update employee
- `DELETE /api/employees/{id}` - Delete/deactivate employee
- `POST /api/employees/bulk-upload` - Bulk upload from Excel

### Payroll Processing
- `POST /api/payroll/upload-sierra-excel` - Process Sierra timesheet
- `POST /api/payroll/generate-wbs` - Generate WBS format file
- `GET /api/payroll/records` - List payroll records
- `GET /api/payroll/summary` - Payroll statistics
- `GET /api/payroll/preview-wbs` - Preview WBS data

## Database Schema

### Employee Table
- Employee number, name, SSN, department
- Regular and overtime rates with history tracking
- Active/inactive status for workforce management

### PayrollRecord Table
- Pay period dates and employee reference
- Regular/overtime hours (A01/A02 columns)
- Daily piecework hours/rates (AH1-AH5/AI1-AI5 columns)
- Travel time and PTO hours (B08/E26 columns)

### AuditLog Table
- Complete operation history with before/after values
- User tracking and timestamp logging
- Table and record-level audit trails

## Sierra Excel Format Support

The system processes Sierra's exact format:
- **Column 1**: Days (Mon, Tue, Wed, Thu, Fri)
- **Column 2**: Job# (project identifier)
- **Column 3**: Name (employee full name)
- **Column 4**: Start time
- **Column 5**: Hours worked
- **Column 6**: Rate (hourly or piece rate)
- **Column 7**: Total amount
- **Column 8**: Job Detail description

### Piecework Detection
- Green cell background = piecework entry
- Regular cells = hourly work
- Automatic rate calculation and validation

## WBS Format Generation

Generates exact WBS structure:
- **Headers**: # V, # U, # N, # P, # R, # C, # B:8, # E:26
- **Columns**: A (employee), A01 (regular), A02 (overtime)
- **Piecework**: AH1-AH5 (hours), AI1-AI5 (rates) by day
- **Benefits**: B08 (travel), E26 (PTO)

## Security & Validation

- Input sanitization and validation
- SQL injection prevention
- Business rule enforcement
- Rate and hour limit validation
- Duplicate detection and handling

## Error Handling

- Comprehensive exception handling
- Detailed error messages and logging
- Graceful degradation for non-critical errors
- Health check endpoints for monitoring

## Production Deployment

### Railway (Recommended)
- Automatic HTTPS and domain
- PostgreSQL database included
- Zero-downtime deployments
- Built-in monitoring

### Docker Deployment
```bash
docker build -t sierra-payroll-backend .
docker run -p 8000:8000 -e DATABASE_URL=<url> sierra-payroll-backend
```

### Environment Variables
- `DATABASE_URL`: PostgreSQL connection string
- `SECRET_KEY`: Application secret (generate with `openssl rand -hex 32`)
- `ENVIRONMENT`: `development` or `production`
- `ALLOWED_ORIGINS`: CORS origins (comma-separated)

## Support

For technical support or questions about the Sierra Roofing payroll system, refer to the API documentation at `/docs` endpoint when the application is running.
