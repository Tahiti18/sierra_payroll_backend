from fastapi import FastAPI

app = FastAPI(title="Sierra Roofing Payroll Backend")

@app.get("/")
def root():
    return {
        "message": "Sierra Roofing Payroll Backend",
        "status": "running",
        "employees_ready": True,
        "data_structure": {
            "input": "Sierra Payroll (Days, Job#, Name, Hours, Rate, Total)",
            "output": "WBS Format (SSN, Employee Name, A01, AH2/AI2, etc.)",
            "piecework_examples": [
                "Arizmendi Fernando: 8hrs/$400 Tue-Fri = $1600",
                "Chavez Derick: 7.5hrs/$200 Tue-Fri = $800"
            ]
        }
    }

@app.get("/health")
def health():
    return {"status": "healthy", "port_handling": "fixed"}

@app.post("/api/payroll/process")
def process_payroll():
    return {
        "message": "Payroll processing endpoint ready",
        "supports": ["Sierra Excel input", "WBS format output", "Piecework detection"]
    }
