import asyncio
from config.settings import settings
from core.database_manager import DatabaseManager
from core.etl_engine import ETLEngine
from core.metrics_collector import MetricsCollector
from agents.nl_processor import NaturalLanguageProcessor
from agents.test_generator import TestScenarioGenerator
from agents.validation_agent import ValidationAgent
from reports.report_generator import ReportGenerator
import pandas as pd
from datetime import datetime
import logging
import uuid
import sys

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('autoetl.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

async def main():
    """
    AutoETL - Enterprise Data Validation Agent
    Main application entry point
    """
    
    logger.info("=" * 80)
    logger.info("🚀 AutoETL - Enterprise Data Validation Agent Starting...")
    logger.info("=" * 80)
    
    try:
        # Initialize components
        logger.info("📊 Initializing database connections...")
        source_db = DatabaseManager(settings.DB1_CONNECTION_STRING)
        target_db = DatabaseManager(settings.DB2_CONNECTION_STRING)
        
        logger.info("🤖 Initializing AI agents...")
        nl_processor = NaturalLanguageProcessor()
        
        logger.info("📈 Initializing metrics collector...")
        metrics = MetricsCollector(enable_redis=settings.ENABLE_METRICS)
        
        # Example: Process a business rule in natural language
        logger.info("📝 Processing business rule in natural language...")
        
        business_rule_text = """
        Calculate comprehensive customer summaries from the orders and order_items tables.

        For each active customer (status = 'active' and is_deleted = false):
        - Calculate total number of orders
        - Calculate total amount spent across all orders
        - Calculate average order value
        - Find the date of their first order
        - Find the date of their last order
        - Count total items purchased
        - Calculate customer lifetime value (sum of all order amounts)
        - Determine preferred payment method (most frequently used)
        - Count cancelled orders
        - Assign customer segment based on total spent:
          * VIP: > $5000
          * Regular: $1000 - $5000
          * New: < $1000

        Store results in customer_summary table with columns:
        customer_id, full_name (first_name + last_name), email, total_orders, 
        total_amount, average_order_value, last_order_date, first_order_date,
        total_items_purchased, customer_lifetime_value, preferred_payment_method,
        total_cancelled_orders, status, customer_segment

        Only include customers who have placed at least one order.
        Filter out customers marked as deleted (is_deleted = true).
        """
        
        # Parse natural language to structured rule
        logger.info("🔄 Converting natural language to structured rule...")
        parsed_rule = nl_processor.parse_business_rule(business_rule_text)
        logger.info(f"✅ Parsed Rule: {parsed_rule.rule_name}")
        logger.info(f"   Source Tables: {', '.join(parsed_rule.source_tables)}")
        logger.info(f"   Target Table: {parsed_rule.target_table}")
        
        # Generate transformation code
        logger.info("🔧 Generating transformation code...")
        transform_code = nl_processor.generate_transformation_code(parsed_rule)
        logger.info("✅ Transformation code generated")
        
        # Execute ETL with metrics tracking
        logger.info("⚙️ Executing ETL pipeline...")
        etl_engine = ETLEngine(source_db, target_db)
        
        # Generate job ID for tracking
        job_id = str(uuid.uuid4())
        
        # Record ETL start in metrics
        metrics.record_etl_start(
            job_id=job_id,
            rule_name=parsed_rule.rule_name,
            source_table=parsed_rule.source_tables[0] if parsed_rule.source_tables else 'unknown',
            target_table=parsed_rule.target_table
        )
        
        # Define transformation function (would be generated dynamically in production)
        def sample_transformation(df: pd.DataFrame) -> pd.DataFrame:
            """
            Sample transformation: Customer order summary
            In production, this would be generated from the AI-parsed rule
            """
            if df.empty:
                return pd.DataFrame(columns=['customer_id', 'total_orders', 'total_amount', 'last_order_date'])
            
            result = df.groupby('customer_id').agg({
                'order_id': 'count',
                'total_amount': 'sum',
                'order_date': 'max'
            }).reset_index()
            result.columns = ['customer_id', 'total_orders', 'total_amount', 'last_order_date']
            return result
        
        # Track ETL start time
        etl_start = datetime.now()
        records_processed = 0
        
        try:
            # Run ETL with advanced features
            etl_engine.extract_transform_load(
                source_table='orders',
                target_table='customer_summary',
                transformation_func=sample_transformation,
                batch_size=settings.BATCH_SIZE,
                use_dask=settings.ENABLE_DASK,
                enable_rollback=True  # Enable snapshot for rollback capability
            )
            
            etl_duration = (datetime.now() - etl_start).total_seconds()
            records_processed = etl_engine.metrics['total_records_processed']
            
            # Record ETL completion
            metrics.record_etl_complete(
                job_id=job_id,
                records_processed=records_processed,
                duration=etl_duration,
                success=True,
                batch_count=etl_engine.metrics['successful_batches']
            )
            
            logger.info(f"✅ ETL completed successfully!")
            logger.info(f"   Records Processed: {records_processed:,}")
            logger.info(f"   Duration: {etl_duration:.2f}s")
            if etl_duration > 0:
                logger.info(f"   Throughput: {records_processed/etl_duration:.2f} records/sec")
            
        except Exception as e:
            etl_duration = (datetime.now() - etl_start).total_seconds()
            metrics.record_etl_complete(job_id, 0, etl_duration, False)
            logger.error(f"❌ ETL failed: {e}")
            raise
        
        # Validate with AI agent
        logger.info("🧪 Starting automated validation testing...")
        test_gen = TestScenarioGenerator(nl_processor.llm)
        validation_agent = ValidationAgent(source_db, target_db, test_gen)
        
        validation_start = datetime.now()
        test_results = validation_agent.validate_etl_job(parsed_rule)
        validation_duration = (datetime.now() - validation_start).total_seconds()
        
        # Calculate test statistics
        total_tests = len(test_results)
        passed = sum(1 for r in test_results if r.status == 'PASS')
        failed = sum(1 for r in test_results if r.status == 'FAIL')
        errors = sum(1 for r in test_results if r.status == 'ERROR')
        pass_rate = (passed / total_tests * 100) if total_tests > 0 else 0
        
        # Record test metrics
        metrics.record_test_execution(
            rule_name=parsed_rule.rule_name,
            total_tests=total_tests,
            passed=passed,
            failed=failed,
            errors=errors,
            duration=validation_duration
        )
        
        logger.info(f"✅ Validation completed!")
        logger.info(f"   Total Tests: {total_tests}")
        logger.info(f"   ✓ Passed: {passed}")
        logger.info(f"   ✗ Failed: {failed}")
        logger.info(f"   ⚠ Errors: {errors}")
        logger.info(f"   Pass Rate: {pass_rate:.1f}%")
        
        # Generate comprehensive report
        logger.info("📊 Generating comprehensive HTML report...")
        report_gen = ReportGenerator()
        report_html = report_gen.generate_html_report(test_results, parsed_rule)
        
        # Save report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f'reports/test_report_{timestamp}.html'
        
        with open(report_filename, 'w', encoding='utf-8') as f:
            f.write(report_html)
        
        logger.info(f"✅ Report generated: {report_filename}")
        
        # Export metrics
        if settings.ENABLE_METRICS:
            logger.info("📈 Exporting metrics...")
            metrics_filename = f'reports/metrics_{timestamp}.json'
            metrics.export_metrics_to_file(metrics_filename)
            logger.info(f"✅ Metrics exported: {metrics_filename}")
        
        # Print summary
        logger.info("=" * 80)
        logger.info("🎉 AutoETL Execution Complete!")
        logger.info("=" * 80)
        logger.info(f"📋 Summary:")
        logger.info(f"   - ETL Records Processed: {records_processed:,}")
        logger.info(f"   - ETL Duration: {etl_duration:.2f}s")
        logger.info(f"   - Tests Executed: {total_tests}")
        logger.info(f"   - Test Pass Rate: {pass_rate:.1f}%")
        logger.info(f"   - Validation Duration: {validation_duration:.2f}s")
        logger.info(f"   - Report: {report_filename}")
        logger.info("=" * 80)
        
        # Close connections
        logger.info("🔒 Closing database connections...")
        source_db.close()
        target_db.close()
        
        if settings.ENABLE_METRICS:
            metrics.close()
        
        logger.info("✅ All systems shut down gracefully")
        
        return {
            'success': True,
            'test_results': test_results,
            'report_path': report_filename,
            'pass_rate': pass_rate
        }
        
    except Exception as e:
        logger.error(f"❌ Fatal error in main execution: {e}", exc_info=True)
        logger.error("=" * 80)
        logger.error("Execution failed. Check logs for details.")
        logger.error("=" * 80)
        return {
            'success': False,
            'error': str(e)
        }

if __name__ == "__main__":
    result = asyncio.run(main())
    
    # Exit with appropriate code
    if result.get('success'):
        sys.exit(0)
    else:
        sys.exit(1)