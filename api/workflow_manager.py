"""
Workflow Manager
Handles Stage 1 (ETL) and Stage 2 (Validation) workflows
Manages job tracking, execution, and state management
"""

import uuid
import asyncio
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Any
import logging
import json

from config.settings import settings
from core.database_manager import DatabaseManager
from core.etl_engine import ETLEngine
from core.metrics_collector import MetricsCollector
from agents.nl_processor import BusinessRule
from agents.test_generator import TestScenarioGenerator
from agents.validation_agent import ValidationAgent
from reports.report_generator import ReportGenerator

logger = logging.getLogger(__name__)


class WorkflowManager:
    """
    Manages ETL and Validation workflows
    
    Features:
    - Job registration and tracking
    - Stage 1: ETL execution
    - Stage 2: Validation execution
    - Combined workflow orchestration
    - Job history and status management
    """
    
    def __init__(self):
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self.workflows: Dict[str, Dict[str, Any]] = {}
        self.metrics_collector = MetricsCollector(enable_redis=settings.ENABLE_METRICS)
    
    # ========================================================================
    # Job Registration
    # ========================================================================
    
    def register_etl_job(self, job_id: str, source_table: str, target_table: str,
                        business_rule: BusinessRule, user_id: Optional[str] = None):
        """Register a new ETL job"""
        self.jobs[job_id] = {
            'job_id': job_id,
            'job_type': 'etl',
            'status': 'PENDING',
            'source_table': source_table,
            'target_table': target_table,
            'business_rule': business_rule,
            'user_id': user_id,
            'created_at': datetime.now().isoformat(),
            'started_at': None,
            'completed_at': None,
            'progress_percentage': 0,
            'records_processed': 0,
            'error_message': None,
            'result_summary': None
        }
        logger.info(f"Registered ETL job: {job_id}")
    
    def register_validation_job(self, job_id: str, source_table: str, target_table: str,
                               business_rule: BusinessRule, etl_job_id: Optional[str] = None,
                               user_id: Optional[str] = None):
        """Register a new validation job"""
        self.jobs[job_id] = {
            'job_id': job_id,
            'job_type': 'validation',
            'status': 'PENDING',
            'source_table': source_table,
            'target_table': target_table,
            'business_rule': business_rule,
            'etl_job_id': etl_job_id,
            'user_id': user_id,
            'created_at': datetime.now().isoformat(),
            'started_at': None,
            'completed_at': None,
            'progress_percentage': 0,
            'test_results': [],
            'error_message': None,
            'report_path': None,
            'result_summary': None
        }
        logger.info(f"Registered validation job: {job_id}")
    
    def register_workflow(self, workflow_id: str, source_table: str, target_table: str,
                         business_rule: BusinessRule, user_id: Optional[str] = None,
                         run_etl: bool = True, run_validation: bool = True):
        """Register a complete workflow"""
        self.workflows[workflow_id] = {
            'workflow_id': workflow_id,
            'status': 'PENDING',
            'source_table': source_table,
            'target_table': target_table,
            'business_rule': business_rule,
            'user_id': user_id,
            'run_etl': run_etl,
            'run_validation': run_validation,
            'created_at': datetime.now().isoformat(),
            'started_at': None,
            'completed_at': None,
            'etl_job_id': None,
            'validation_job_id': None,
            'stages': {
                'etl': 'PENDING' if run_etl else 'SKIPPED',
                'validation': 'PENDING' if run_validation else 'SKIPPED'
            },
            'error_message': None
        }
        logger.info(f"Registered workflow: {workflow_id}")
    
    # ========================================================================
    # Stage 1: ETL Execution
    # ========================================================================
    
    async def execute_etl_job(self, job_id: str, source_table: str, target_table: str,
                            parsed_rule: BusinessRule, batch_size: int = 10000,
                            use_dask: bool = True):
        """Execute Stage 1: ETL transformation"""
        try:
            # Update job status
            self.jobs[job_id]['status'] = 'RUNNING'
            self.jobs[job_id]['started_at'] = datetime.now().isoformat()
            
            logger.info(f"Executing ETL job {job_id}")
            
            # Initialize components
            source_db = DatabaseManager(settings.DB1_CONNECTION_STRING)
            target_db = DatabaseManager(settings.DB2_CONNECTION_STRING)
            etl_engine = ETLEngine(source_db, target_db)
            
            # Record start in metrics
            self.metrics_collector.record_etl_start(
                job_id=job_id,
                rule_name=parsed_rule.rule_name,
                source_table=source_table,
                target_table=target_table
            )
            
            # Define transformation function
            # In production, this would be dynamically generated from the parsed rule
            def transformation_function(df: pd.DataFrame) -> pd.DataFrame:
                """Dynamic transformation based on business rule"""
                if df.empty:
                    return pd.DataFrame()
                
                # Example: Customer summary transformation
                # This should be generated from parsed_rule.transformation_logic
                try:
                    result = df.groupby('customer_id').agg({
                        'order_id': 'count',
                        'amount': 'sum',
                        'order_date': 'max'
                    }).reset_index()
                    result.columns = ['customer_id', 'total_orders', 'total_amount', 'last_order_date']
                    return result
                except Exception as e:
                    logger.error(f"Transformation failed: {e}")
                    return df
            
            # Execute ETL
            start_time = datetime.now()
            
            etl_engine.extract_transform_load(
                source_table=source_table,
                target_table=target_table,
                transformation_func=transformation_function,
                batch_size=batch_size,
                use_dask=use_dask,
                enable_rollback=True
            )
            
            duration = (datetime.now() - start_time).total_seconds()
            records_processed = etl_engine.metrics['total_records_processed']
            
            # Record completion in metrics
            self.metrics_collector.record_etl_complete(
                job_id=job_id,
                records_processed=records_processed,
                duration=duration,
                success=True,
                batch_count=etl_engine.metrics['successful_batches']
            )
            
            # Update job status
            self.jobs[job_id]['status'] = 'COMPLETED'
            self.jobs[job_id]['completed_at'] = datetime.now().isoformat()
            self.jobs[job_id]['progress_percentage'] = 100
            self.jobs[job_id]['records_processed'] = records_processed
            self.jobs[job_id]['result_summary'] = {
                'records_processed': records_processed,
                'duration_seconds': duration,
                'throughput': records_processed / duration if duration > 0 else 0,
                'successful_batches': etl_engine.metrics['successful_batches'],
                'failed_batches': etl_engine.metrics['failed_batches']
            }
            
            # Close connections
            source_db.close()
            target_db.close()
            
            logger.info(f"ETL job {job_id} completed successfully")
            
        except Exception as e:
            logger.error(f"ETL job {job_id} failed: {e}", exc_info=True)
            
            self.jobs[job_id]['status'] = 'FAILED'
            self.jobs[job_id]['completed_at'] = datetime.now().isoformat()
            self.jobs[job_id]['error_message'] = str(e)
            
            # Record failure in metrics
            if 'duration' in locals():
                self.metrics_collector.record_etl_complete(
                    job_id=job_id,
                    records_processed=0,
                    duration=duration,
                    success=False
                )
    
    # ========================================================================
    # Stage 2: Validation Execution
    # ========================================================================
    
    async def execute_validation_job(self, job_id: str, source_table: str,
                                    target_table: str, parsed_rule: BusinessRule):
        """Execute Stage 2: Validation tests"""
        try:
            # Update job status
            self.jobs[job_id]['status'] = 'RUNNING'
            self.jobs[job_id]['started_at'] = datetime.now().isoformat()
            
            logger.info(f"Executing validation job {job_id}")
            
            # Initialize components
            source_db = DatabaseManager(settings.DB1_CONNECTION_STRING)
            target_db = DatabaseManager(settings.DB2_CONNECTION_STRING)
            
            from agents.nl_processor import NaturalLanguageProcessor
            nl_processor = NaturalLanguageProcessor()
            test_gen = TestScenarioGenerator(nl_processor.llm)
            validation_agent = ValidationAgent(source_db, target_db, test_gen)
            
            # Execute validation
            start_time = datetime.now()
            
            test_results = validation_agent.validate_etl_job(parsed_rule)
            
            duration = (datetime.now() - start_time).total_seconds()
            
            # Calculate statistics
            total_tests = len(test_results)
            passed = sum(1 for r in test_results if r.status == 'PASS')
            failed = sum(1 for r in test_results if r.status == 'FAIL')
            errors = sum(1 for r in test_results if r.status == 'ERROR')
            pass_rate = (passed / total_tests * 100) if total_tests > 0 else 0
            
            # Record metrics
            self.metrics_collector.record_test_execution(
                rule_name=parsed_rule.rule_name,
                total_tests=total_tests,
                passed=passed,
                failed=failed,
                errors=errors,
                duration=duration
            )
            
            # Generate report
            report_gen = ReportGenerator()
            report_html = report_gen.generate_html_report(test_results, parsed_rule)
            
            # Save report
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_filename = f'reports/validation_report_{job_id}_{timestamp}.html'
            
            with open(report_filename, 'w', encoding='utf-8') as f:
                f.write(report_html)
            
            # Update job status
            self.jobs[job_id]['status'] = 'COMPLETED'
            self.jobs[job_id]['completed_at'] = datetime.now().isoformat()
            self.jobs[job_id]['progress_percentage'] = 100
            self.jobs[job_id]['test_results'] = test_results
            self.jobs[job_id]['report_path'] = report_filename
            self.jobs[job_id]['result_summary'] = {
                'total_tests': total_tests,
                'passed': passed,
                'failed': failed,
                'errors': errors,
                'pass_rate': pass_rate,
                'duration_seconds': duration
            }
            
            # Close connections
            source_db.close()
            target_db.close()
            
            logger.info(f"Validation job {job_id} completed successfully")
            
        except Exception as e:
            logger.error(f"Validation job {job_id} failed: {e}", exc_info=True)
            
            self.jobs[job_id]['status'] = 'FAILED'
            self.jobs[job_id]['completed_at'] = datetime.now().isoformat()
            self.jobs[job_id]['error_message'] = str(e)
    
    # ========================================================================
    # Complete Workflow Execution
    # ========================================================================
    
    async def execute_complete_workflow(self, workflow_id: str, source_table: str,
                                       target_table: str, parsed_rule: BusinessRule,
                                       run_etl: bool = True, run_validation: bool = True):
        """Execute complete workflow: ETL + Validation"""
        try:
            # Update workflow status
            self.workflows[workflow_id]['status'] = 'RUNNING'
            self.workflows[workflow_id]['started_at'] = datetime.now().isoformat()
            
            logger.info(f"Executing complete workflow {workflow_id}")
            
            # Stage 1: ETL (if enabled)
            if run_etl:
                etl_job_id = str(uuid.uuid4())
                self.workflows[workflow_id]['etl_job_id'] = etl_job_id
                self.workflows[workflow_id]['stages']['etl'] = 'RUNNING'
                
                self.register_etl_job(etl_job_id, source_table, target_table, parsed_rule)
                await self.execute_etl_job(etl_job_id, source_table, target_table, parsed_rule)
                
                if self.jobs[etl_job_id]['status'] == 'COMPLETED':
                    self.workflows[workflow_id]['stages']['etl'] = 'COMPLETED'
                else:
                    self.workflows[workflow_id]['stages']['etl'] = 'FAILED'
                    raise Exception(f"ETL stage failed: {self.jobs[etl_job_id]['error_message']}")
            
            # Stage 2: Validation (if enabled)
            if run_validation:
                validation_job_id = str(uuid.uuid4())
                self.workflows[workflow_id]['validation_job_id'] = validation_job_id
                self.workflows[workflow_id]['stages']['validation'] = 'RUNNING'
                
                self.register_validation_job(validation_job_id, source_table, target_table, parsed_rule)
                await self.execute_validation_job(validation_job_id, source_table, target_table, parsed_rule)
                
                if self.jobs[validation_job_id]['status'] == 'COMPLETED':
                    self.workflows[workflow_id]['stages']['validation'] = 'COMPLETED'
                else:
                    self.workflows[workflow_id]['stages']['validation'] = 'FAILED'
                    raise Exception(f"Validation stage failed: {self.jobs[validation_job_id]['error_message']}")
            
            # Update workflow status
            self.workflows[workflow_id]['status'] = 'COMPLETED'
            self.workflows[workflow_id]['completed_at'] = datetime.now().isoformat()
            
            logger.info(f"Workflow {workflow_id} completed successfully")
            
        except Exception as e:
            logger.error(f"Workflow {workflow_id} failed: {e}", exc_info=True)
            
            self.workflows[workflow_id]['status'] = 'FAILED'
            self.workflows[workflow_id]['completed_at'] = datetime.now().isoformat()
            self.workflows[workflow_id]['error_message'] = str(e)
    
    # ========================================================================
    # Job Status and Management
    # ========================================================================
    
    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific job"""
        return self.jobs.get(job_id)
    
    def get_workflow_status(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a workflow"""
        workflow = self.workflows.get(workflow_id)
        if not workflow:
            return None
        
        # Enrich with job details if available
        if workflow.get('etl_job_id'):
            workflow['etl_job'] = self.jobs.get(workflow['etl_job_id'])
        
        if workflow.get('validation_job_id'):
            workflow['validation_job'] = self.jobs.get(workflow['validation_job_id'])
        
        return workflow
    
    def list_jobs(self, job_type: Optional[str] = None, user_id: Optional[str] = None,
                 status: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all jobs with optional filters"""
        jobs = list(self.jobs.values())
        
        if job_type:
            jobs = [j for j in jobs if j['job_type'] == job_type]
        
        if user_id:
            jobs = [j for j in jobs if j.get('user_id') == user_id]
        
        if status:
            jobs = [j for j in jobs if j['status'] == status]
        
        # Sort by created_at descending
        jobs.sort(key=lambda x: x['created_at'], reverse=True)
        
        return jobs
    
    def list_workflows(self, user_id: Optional[str] = None,
                      status: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all workflows with optional filters"""
        workflows = list(self.workflows.values())
        
        if user_id:
            workflows = [w for w in workflows if w.get('user_id') == user_id]
        
        if status:
            workflows = [w for w in workflows if w['status'] == status]
        
        # Sort by created_at descending
        workflows.sort(key=lambda x: x['created_at'], reverse=True)
        
        return workflows
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running job"""
        job = self.jobs.get(job_id)
        if not job:
            return False
        
        if job['status'] in ['PENDING', 'RUNNING']:
            job['status'] = 'CANCELLED'
            job['completed_at'] = datetime.now().isoformat()
            logger.info(f"Job {job_id} cancelled")
            return True
        
        return False
    
    def delete_job(self, job_id: str) -> bool:
        """Delete a job from history"""
        if job_id in self.jobs:
            del self.jobs[job_id]
            logger.info(f"Job {job_id} deleted")
            return True
        return False
