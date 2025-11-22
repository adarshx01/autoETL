"""
Pydantic models for API request/response validation
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime


# ============================================================================
# Database Models
# ============================================================================

class TableSchema(BaseModel):
    """Table schema information"""
    table_name: str
    columns: Dict[str, str]
    record_count: int


# ============================================================================
# Business Rule Models
# ============================================================================

class ParseRuleRequest(BaseModel):
    """Request to parse natural language business rule"""
    rule_text: str = Field(..., description="Business rule in natural language")
    
    class Config:
        json_schema_extra = {
            "example": {
                "rule_text": "For all active customers, calculate total order amount and store in customer_summary table"
            }
        }


class BusinessRuleResponse(BaseModel):
    """Parsed business rule response"""
    rule_id: str
    rule_name: str
    description: str
    source_tables: List[str]
    target_table: str
    transformation_logic: str
    validation_query: str
    expected_behavior: str


# ============================================================================
# STAGE 1: ETL Models
# ============================================================================

class ETLExecutionRequest(BaseModel):
    """Request to execute ETL transformation"""
    source_table: str = Field(..., description="Source table name from DB1")
    target_table: str = Field(..., description="Target table name for DB2")
    business_rule: str = Field(..., description="Business rule in natural language")
    batch_size: Optional[int] = Field(None, description="Records per batch")
    use_dask: Optional[bool] = Field(None, description="Use Dask for distributed processing")
    user_id: Optional[str] = Field(None, description="User ID for tracking")
    
    class Config:
        json_schema_extra = {
            "example": {
                "source_table": "orders",
                "target_table": "customer_summary",
                "business_rule": "Calculate total orders and revenue per customer, grouped by customer_id",
                "batch_size": 10000,
                "use_dask": True,
                "user_id": "analyst_001"
            }
        }


class ETLJobResponse(BaseModel):
    """Response for ETL job creation"""
    job_id: str
    status: str
    message: str
    source_table: str
    target_table: str
    started_at: str
    estimated_completion_minutes: int


# ============================================================================
# STAGE 2: Validation Models
# ============================================================================

class ValidationRequest(BaseModel):
    """Request to execute validation tests"""
    source_table: str = Field(..., description="Source table to validate against")
    target_table: str = Field(..., description="Target table to validate")
    business_rule_text: Optional[str] = Field(None, description="Business rule in natural language")
    etl_job_id: Optional[str] = Field(None, description="Related ETL job ID to use its rule")
    user_id: Optional[str] = Field(None, description="User ID for tracking")
    
    class Config:
        json_schema_extra = {
            "example": {
                "source_table": "orders",
                "target_table": "customer_summary",
                "business_rule_text": "Verify total orders and revenue calculations per customer",
                "user_id": "analyst_001"
            }
        }


class ValidationJobResponse(BaseModel):
    """Response for validation job creation"""
    job_id: str
    status: str
    message: str
    source_table: str
    target_table: str
    started_at: str
    estimated_completion_minutes: int


class TestResultSummary(BaseModel):
    """Summary of test results"""
    test_id: str
    scenario_name: str
    category: str
    status: str
    execution_time: float
    records_tested: int


# ============================================================================
# Combined Workflow Models
# ============================================================================

class CompleteWorkflowRequest(BaseModel):
    """Request to execute complete ETL + Validation workflow"""
    source_table: str = Field(..., description="Source table from DB1")
    target_table: str = Field(..., description="Target table for DB2")
    business_rule: str = Field(..., description="Business rule in natural language")
    run_etl: bool = Field(True, description="Execute ETL transformation")
    run_validation: bool = Field(True, description="Execute validation tests")
    batch_size: Optional[int] = Field(None, description="Records per batch for ETL")
    user_id: Optional[str] = Field(None, description="User ID for tracking")
    
    class Config:
        json_schema_extra = {
            "example": {
                "source_table": "orders",
                "target_table": "customer_summary",
                "business_rule": "Calculate customer metrics: total orders, revenue, last order date",
                "run_etl": True,
                "run_validation": True,
                "user_id": "analyst_001"
            }
        }


class WorkflowResponse(BaseModel):
    """Response for workflow creation"""
    workflow_id: str
    status: str
    message: str
    stages: Dict[str, str]
    started_at: str


# ============================================================================
# Job Status Models
# ============================================================================

class JobStatus(BaseModel):
    """Generic job status"""
    job_id: str
    job_type: str  # etl, validation, workflow
    status: str  # PENDING, RUNNING, COMPLETED, FAILED
    created_at: str
    started_at: Optional[str]
    completed_at: Optional[str]
    progress_percentage: Optional[int]
    records_processed: Optional[int]
    error_message: Optional[str]
    result_summary: Optional[Dict[str, Any]]


# ============================================================================
# Report Models
# ============================================================================

class ReportSummary(BaseModel):
    """Summary of validation report"""
    job_id: str
    total_tests: int
    passed: int
    failed: int
    errors: int
    pass_rate: float
    execution_time: float
    report_url: str


# ============================================================================
# Metrics Models
# ============================================================================

class ETLMetrics(BaseModel):
    """ETL performance metrics"""
    total_jobs: int
    successful_jobs: int
    failed_jobs: int
    total_records_processed: int
    average_throughput: float
    last_execution: Optional[str]


class ValidationMetrics(BaseModel):
    """Validation test metrics"""
    total_test_runs: int
    total_tests_executed: int
    overall_pass_rate: float
    last_execution: Optional[str]
