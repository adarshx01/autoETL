"""
AutoETL REST API Server
Provides endpoints for ETL execution, validation, and report generation
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import uvicorn
import uuid
from datetime import datetime
import asyncio
import logging

from config.settings import settings
from core.database_manager import DatabaseManager
from core.etl_engine import ETLEngine
from core.metrics_collector import MetricsCollector
from agents.nl_processor import NaturalLanguageProcessor, BusinessRule
from agents.test_generator import TestScenarioGenerator
from agents.validation_agent import ValidationAgent
from reports.report_generator import ReportGenerator
from api.workflow_manager import WorkflowManager
from api.models import *

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="AutoETL API",
    description="Enterprise ETL Validation Agent with Natural Language Processing",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware for web interface
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify allowed origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global instances
workflow_manager = WorkflowManager()
nl_processor = NaturalLanguageProcessor()
metrics_collector = MetricsCollector(enable_redis=settings.ENABLE_METRICS)

# ============================================================================
# Health Check & Status Endpoints
# ============================================================================

@app.get("/", tags=["Health"])
async def root():
    """Root endpoint - API information"""
    return {
        "service": "AutoETL API",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "docs": "/docs",
            "health": "/health",
            "databases": "/api/databases",
            "etl": "/api/etl",
            "validate": "/api/validate"
        }
    }

@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint"""
    try:
        # Check database connections
        source_db = DatabaseManager(settings.DB1_CONNECTION_STRING)
        target_db = DatabaseManager(settings.DB2_CONNECTION_STRING)
        
        source_db.close()
        target_db.close()
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "services": {
                "api": "running",
                "database_source": "connected",
                "database_target": "connected",
                "ai_service": settings.AI_PROVIDER,
                "metrics": settings.ENABLE_METRICS
            }
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail=f"Service unhealthy: {str(e)}")

# ============================================================================
# Database Discovery Endpoints
# ============================================================================

@app.get("/api/databases/source/tables", tags=["Database"])
async def list_source_tables():
    """List all available tables in source database (DB1)"""
    try:
        source_db = DatabaseManager(settings.DB1_CONNECTION_STRING)
        inspector = source_db.engine.dialect.get_inspector(source_db.engine)
        tables = inspector.get_table_names()
        source_db.close()
        
        return {
            "database": "source (DB1)",
            "tables": tables,
            "count": len(tables)
        }
    except Exception as e:
        logger.error(f"Failed to list source tables: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/databases/target/tables", tags=["Database"])
async def list_target_tables():
    """List all available tables in target database (DB2)"""
    try:
        target_db = DatabaseManager(settings.DB2_CONNECTION_STRING)
        inspector = target_db.engine.dialect.get_inspector(target_db.engine)
        tables = inspector.get_table_names()
        target_db.close()
        
        return {
            "database": "target (DB2)",
            "tables": tables,
            "count": len(tables)
        }
    except Exception as e:
        logger.error(f"Failed to list target tables: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/databases/source/tables/{table_name}/schema", tags=["Database"])
async def get_source_table_schema(table_name: str):
    """Get schema information for a specific source table"""
    try:
        source_db = DatabaseManager(settings.DB1_CONNECTION_STRING)
        schema = source_db.get_table_schema(table_name)
        count = source_db.get_table_count(table_name)
        source_db.close()
        
        return {
            "table": table_name,
            "database": "source (DB1)",
            "schema": schema,
            "record_count": count
        }
    except Exception as e:
        logger.error(f"Failed to get schema for {table_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/databases/target/tables/{table_name}/schema", tags=["Database"])
async def get_target_table_schema(table_name: str):
    """Get schema information for a specific target table"""
    try:
        target_db = DatabaseManager(settings.DB2_CONNECTION_STRING)
        schema = target_db.get_table_schema(table_name)
        count = target_db.get_table_count(table_name)
        target_db.close()
        
        return {
            "table": table_name,
            "database": "target (DB2)",
            "schema": schema,
            "record_count": count
        }
    except Exception as e:
        logger.error(f"Failed to get schema for {table_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/databases/source/tables/{table_name}/preview", tags=["Database"])
async def preview_source_table(table_name: str, limit: int = 100):
    """Preview data from source table"""
    try:
        source_db = DatabaseManager(settings.DB1_CONNECTION_STRING)
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        df = source_db.execute_query(query)
        source_db.close()
        
        return {
            "table": table_name,
            "database": "source (DB1)",
            "records": df.to_dict(orient='records'),
            "count": len(df)
        }
    except Exception as e:
        logger.error(f"Failed to preview {table_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# Natural Language Processing Endpoints
# ============================================================================

@app.post("/api/parse-rule", tags=["NLP"], response_model=BusinessRuleResponse)
async def parse_business_rule(request: ParseRuleRequest):
    """Parse natural language business rule into structured format"""
    try:
        logger.info(f"Parsing business rule: {request.rule_text[:100]}...")
        
        parsed_rule = nl_processor.parse_business_rule(request.rule_text)
        
        return BusinessRuleResponse(
            rule_id=parsed_rule.rule_id,
            rule_name=parsed_rule.rule_name,
            description=parsed_rule.description,
            source_tables=parsed_rule.source_tables,
            target_table=parsed_rule.target_table,
            transformation_logic=parsed_rule.transformation_logic,
            validation_query=parsed_rule.validation_query,
            expected_behavior=parsed_rule.expected_behavior
        )
    except Exception as e:
        logger.error(f"Failed to parse rule: {e}")
        raise HTTPException(status_code=500, detail=f"Rule parsing failed: {str(e)}")

# ============================================================================
# STAGE 1: ETL Execution Endpoints
# ============================================================================

@app.post("/api/etl/execute", tags=["Stage 1: ETL"], response_model=ETLJobResponse)
async def execute_etl(request: ETLExecutionRequest, background_tasks: BackgroundTasks):
    """
    STAGE 1: Execute ETL transformation
    - User selects source/target tables
    - Provides business rule in natural language
    - System executes ETL transformation
    """
    try:
        # Create job
        job_id = str(uuid.uuid4())
        
        logger.info(f"Starting ETL job {job_id}")
        logger.info(f"  Source: {request.source_table}")
        logger.info(f"  Target: {request.target_table}")
        logger.info(f"  Rule: {request.business_rule[:100]}...")
        
        # Parse business rule
        parsed_rule = nl_processor.parse_business_rule(request.business_rule)
        
        # Register job
        workflow_manager.register_etl_job(
            job_id=job_id,
            source_table=request.source_table,
            target_table=request.target_table,
            business_rule=parsed_rule,
            user_id=request.user_id
        )
        
        # Execute ETL in background
        background_tasks.add_task(
            workflow_manager.execute_etl_job,
            job_id=job_id,
            source_table=request.source_table,
            target_table=request.target_table,
            parsed_rule=parsed_rule,
            batch_size=request.batch_size or settings.BATCH_SIZE,
            use_dask=request.use_dask if request.use_dask is not None else settings.ENABLE_DASK
        )
        
        return ETLJobResponse(
            job_id=job_id,
            status="RUNNING",
            message="ETL job started successfully",
            source_table=request.source_table,
            target_table=request.target_table,
            started_at=datetime.now().isoformat(),
            estimated_completion_minutes=5
        )
        
    except Exception as e:
        logger.error(f"ETL execution failed: {e}")
        raise HTTPException(status_code=500, detail=f"ETL execution failed: {str(e)}")

@app.get("/api/etl/status/{job_id}", tags=["Stage 1: ETL"])
async def get_etl_status(job_id: str):
    """Get status of ETL job"""
    try:
        status = workflow_manager.get_job_status(job_id)
        
        if not status:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        
        return status
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get job status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/etl/jobs", tags=["Stage 1: ETL"])
async def list_etl_jobs(user_id: Optional[str] = None, status: Optional[str] = None):
    """List all ETL jobs with optional filters"""
    try:
        jobs = workflow_manager.list_jobs(job_type="etl", user_id=user_id, status=status)
        return {
            "jobs": jobs,
            "count": len(jobs)
        }
    except Exception as e:
        logger.error(f"Failed to list jobs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# STAGE 2: Validation Endpoints
# ============================================================================

@app.post("/api/validate/execute", tags=["Stage 2: Validation"], response_model=ValidationJobResponse)
async def execute_validation(request: ValidationRequest, background_tasks: BackgroundTasks):
    """
    STAGE 2: Execute validation tests
    - Can run independently or after Stage 1 ETL
    - Takes natural language business rules
    - Validates data transformation and integrity
    - Generates comprehensive test report
    """
    try:
        # Create validation job
        job_id = str(uuid.uuid4())
        
        logger.info(f"Starting validation job {job_id}")
        logger.info(f"  Source: {request.source_table}")
        logger.info(f"  Target: {request.target_table}")
        
        # Parse business rule if provided as text
        if request.business_rule_text:
            parsed_rule = nl_processor.parse_business_rule(request.business_rule_text)
        elif request.etl_job_id:
            # Get rule from previous ETL job
            etl_job = workflow_manager.get_job_status(request.etl_job_id)
            if not etl_job:
                raise HTTPException(status_code=404, detail=f"ETL job {request.etl_job_id} not found")
            parsed_rule = etl_job.get('business_rule')
        else:
            raise HTTPException(status_code=400, detail="Either business_rule_text or etl_job_id must be provided")
        
        # Register validation job
        workflow_manager.register_validation_job(
            job_id=job_id,
            source_table=request.source_table,
            target_table=request.target_table,
            business_rule=parsed_rule,
            etl_job_id=request.etl_job_id,
            user_id=request.user_id
        )
        
        # Execute validation in background
        background_tasks.add_task(
            workflow_manager.execute_validation_job,
            job_id=job_id,
            source_table=request.source_table,
            target_table=request.target_table,
            parsed_rule=parsed_rule
        )
        
        return ValidationJobResponse(
            job_id=job_id,
            status="RUNNING",
            message="Validation started successfully",
            source_table=request.source_table,
            target_table=request.target_table,
            started_at=datetime.now().isoformat(),
            estimated_completion_minutes=3
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Validation execution failed: {e}")
        raise HTTPException(status_code=500, detail=f"Validation failed: {str(e)}")

@app.get("/api/validate/status/{job_id}", tags=["Stage 2: Validation"])
async def get_validation_status(job_id: str):
    """Get status of validation job"""
    try:
        status = workflow_manager.get_job_status(job_id)
        
        if not status:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        
        return status
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get validation status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/validate/jobs", tags=["Stage 2: Validation"])
async def list_validation_jobs(user_id: Optional[str] = None, status: Optional[str] = None):
    """List all validation jobs with optional filters"""
    try:
        jobs = workflow_manager.list_jobs(job_type="validation", user_id=user_id, status=status)
        return {
            "jobs": jobs,
            "count": len(jobs)
        }
    except Exception as e:
        logger.error(f"Failed to list validation jobs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# Report Endpoints
# ============================================================================

@app.get("/api/reports/{job_id}", tags=["Reports"])
async def get_report(job_id: str):
    """Get HTML report for a validation job"""
    try:
        job_status = workflow_manager.get_job_status(job_id)
        
        if not job_status:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        
        report_path = job_status.get('report_path')
        
        if not report_path:
            raise HTTPException(status_code=404, detail="Report not yet generated")
        
        return FileResponse(
            report_path,
            media_type="text/html",
            filename=f"validation_report_{job_id}.html"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get report: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/reports/{job_id}/results", tags=["Reports"])
async def get_report_results(job_id: str):
    """Get JSON results for a validation job"""
    try:
        job_status = workflow_manager.get_job_status(job_id)
        
        if not job_status:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        
        test_results = job_status.get('test_results', [])
        
        return {
            "job_id": job_id,
            "test_results": [result.to_dict() if hasattr(result, 'to_dict') else result for result in test_results],
            "summary": {
                "total_tests": len(test_results),
                "passed": sum(1 for r in test_results if r.status == 'PASS'),
                "failed": sum(1 for r in test_results if r.status == 'FAIL'),
                "errors": sum(1 for r in test_results if r.status == 'ERROR')
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get report results: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# Metrics Endpoints
# ============================================================================

@app.get("/api/metrics/etl", tags=["Metrics"])
async def get_etl_metrics(rule_name: Optional[str] = None):
    """Get ETL performance metrics"""
    try:
        metrics = metrics_collector.get_etl_metrics_summary(rule_name)
        return metrics
    except Exception as e:
        logger.error(f"Failed to get ETL metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/metrics/validation", tags=["Metrics"])
async def get_validation_metrics(rule_name: Optional[str] = None):
    """Get validation test metrics"""
    try:
        metrics = metrics_collector.get_test_metrics_summary(rule_name)
        return metrics
    except Exception as e:
        logger.error(f"Failed to get validation metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# Combined Workflow Endpoint
# ============================================================================

@app.post("/api/workflow/execute", tags=["Workflow"], response_model=WorkflowResponse)
async def execute_complete_workflow(request: CompleteWorkflowRequest, background_tasks: BackgroundTasks):
    """
    Execute complete workflow: ETL + Validation
    Combines Stage 1 and Stage 2 in a single request
    """
    try:
        workflow_id = str(uuid.uuid4())
        
        logger.info(f"Starting complete workflow {workflow_id}")
        
        # Parse business rule
        parsed_rule = nl_processor.parse_business_rule(request.business_rule)
        
        # Register workflow
        workflow_manager.register_workflow(
            workflow_id=workflow_id,
            source_table=request.source_table,
            target_table=request.target_table,
            business_rule=parsed_rule,
            user_id=request.user_id,
            run_etl=request.run_etl,
            run_validation=request.run_validation
        )
        
        # Execute workflow in background
        background_tasks.add_task(
            workflow_manager.execute_complete_workflow,
            workflow_id=workflow_id,
            source_table=request.source_table,
            target_table=request.target_table,
            parsed_rule=parsed_rule,
            run_etl=request.run_etl,
            run_validation=request.run_validation
        )
        
        return WorkflowResponse(
            workflow_id=workflow_id,
            status="RUNNING",
            message="Workflow started successfully",
            stages={
                "etl": "PENDING" if request.run_etl else "SKIPPED",
                "validation": "PENDING" if request.run_validation else "SKIPPED"
            },
            started_at=datetime.now().isoformat()
        )
        
    except Exception as e:
        logger.error(f"Workflow execution failed: {e}")
        raise HTTPException(status_code=500, detail=f"Workflow failed: {str(e)}")

@app.get("/api/workflow/status/{workflow_id}", tags=["Workflow"])
async def get_workflow_status(workflow_id: str):
    """Get status of complete workflow"""
    try:
        status = workflow_manager.get_workflow_status(workflow_id)
        
        if not status:
            raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")
        
        return status
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get workflow status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# Server Startup
# ============================================================================

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
