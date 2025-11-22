"""
Enterprise Metrics Collection and Monitoring System
Tracks ETL performance, data quality, and system health
"""

import redis
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import json
import logging
from config.settings import settings
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)


@dataclass
class ETLMetrics:
    """ETL execution metrics"""
    job_id: str
    rule_name: str
    start_time: str
    end_time: Optional[str]
    duration_seconds: float
    records_processed: int
    records_per_second: float
    status: str
    source_table: str
    target_table: str
    batch_count: int
    success_rate: float
    memory_usage_mb: float
    
    def to_dict(self):
        return asdict(self)


class MetricsCollector:
    """
    Centralized metrics collection system
    
    Features:
    - Real-time performance tracking
    - Historical metrics storage
    - Alert thresholds monitoring
    - Dashboard data aggregation
    - Redis-based caching for fast access
    """
    
    def __init__(self, enable_redis: bool = True):
        self.enable_redis = enable_redis and settings.ENABLE_METRICS
        self.redis_client = None
        
        if self.enable_redis:
            try:
                self.redis_client = redis.Redis(
                    host=settings.REDIS_HOST,
                    port=settings.REDIS_PORT,
                    db=0,
                    decode_responses=True,
                    socket_connect_timeout=5
                )
                self.redis_client.ping()
                logger.info("Redis metrics collection enabled")
            except Exception as e:
                logger.warning(f"Redis connection failed, metrics will be logged only: {e}")
                self.redis_client = None
    
    def record_etl_start(self, job_id: str, rule_name: str, source_table: str, target_table: str):
        """Record ETL job start"""
        metric = {
            'job_id': job_id,
            'rule_name': rule_name,
            'source_table': source_table,
            'target_table': target_table,
            'start_time': datetime.now().isoformat(),
            'status': 'RUNNING'
        }
        
        self._store_metric(f"etl_job:{job_id}", metric, ttl=86400)  # 24 hours
        logger.info(f"ETL job started: {job_id}")
    
    def record_etl_complete(self, job_id: str, records_processed: int, duration: float, 
                           success: bool, batch_count: int = 0):
        """Record ETL job completion"""
        metric_key = f"etl_job:{job_id}"
        existing_metric = self._get_metric(metric_key)
        
        if existing_metric:
            existing_metric.update({
                'end_time': datetime.now().isoformat(),
                'duration_seconds': duration,
                'records_processed': records_processed,
                'records_per_second': records_processed / duration if duration > 0 else 0,
                'status': 'SUCCESS' if success else 'FAILED',
                'batch_count': batch_count
            })
            
            self._store_metric(metric_key, existing_metric, ttl=86400)
            
            # Also store in historical metrics
            history_key = f"etl_history:{existing_metric['rule_name']}"
            self._append_to_list(history_key, existing_metric, max_length=100)
            
            logger.info(f"ETL job completed: {job_id} - {existing_metric['status']}")
    
    def record_test_execution(self, rule_name: str, total_tests: int, passed: int, 
                             failed: int, errors: int, duration: float):
        """Record test execution metrics"""
        metric = {
            'rule_name': rule_name,
            'timestamp': datetime.now().isoformat(),
            'total_tests': total_tests,
            'passed': passed,
            'failed': failed,
            'errors': errors,
            'pass_rate': (passed / total_tests * 100) if total_tests > 0 else 0,
            'duration_seconds': duration
        }
        
        metric_key = f"test_execution:{rule_name}:{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self._store_metric(metric_key, metric, ttl=604800)  # 7 days
        
        # Store in test history
        history_key = f"test_history:{rule_name}"
        self._append_to_list(history_key, metric, max_length=50)
        
        logger.info(f"Test execution recorded: {rule_name} - {passed}/{total_tests} passed")
    
    def record_data_quality_score(self, table_name: str, quality_score: float, 
                                  metrics: Dict[str, Any]):
        """Record data quality metrics"""
        metric = {
            'table_name': table_name,
            'timestamp': datetime.now().isoformat(),
            'quality_score': quality_score,
            'metrics': metrics
        }
        
        metric_key = f"data_quality:{table_name}"
        self._store_metric(metric_key, metric, ttl=86400)
        logger.info(f"Data quality score recorded: {table_name} - {quality_score:.2f}")
    
    def get_etl_metrics_summary(self, rule_name: Optional[str] = None) -> Dict[str, Any]:
        """Get aggregated ETL metrics"""
        history_key = f"etl_history:{rule_name}" if rule_name else "etl_history:*"
        
        if self.redis_client:
            if rule_name:
                history = self._get_list(history_key)
                if history:
                    return self._aggregate_metrics(history)
            else:
                # Get metrics for all rules
                keys = self.redis_client.keys("etl_history:*")
                all_metrics = []
                for key in keys:
                    metrics = self._get_list(key)
                    all_metrics.extend(metrics)
                return self._aggregate_metrics(all_metrics)
        
        return {'status': 'No metrics available'}
    
    def get_test_metrics_summary(self, rule_name: Optional[str] = None) -> Dict[str, Any]:
        """Get aggregated test metrics"""
        history_key = f"test_history:{rule_name}" if rule_name else "test_history:*"
        
        if self.redis_client:
            if rule_name:
                history = self._get_list(history_key)
                if history:
                    return self._aggregate_test_metrics(history)
            else:
                keys = self.redis_client.keys("test_history:*")
                all_metrics = []
                for key in keys:
                    metrics = self._get_list(key)
                    all_metrics.extend(metrics)
                return self._aggregate_test_metrics(all_metrics)
        
        return {'status': 'No metrics available'}
    
    def check_performance_threshold(self, job_id: str, threshold_rps: int = 1000) -> bool:
        """Check if ETL job meets performance threshold"""
        metric = self._get_metric(f"etl_job:{job_id}")
        if metric and 'records_per_second' in metric:
            rps = metric['records_per_second']
            if rps < threshold_rps:
                logger.warning(f"Performance below threshold: {rps:.2f} < {threshold_rps} RPS")
                return False
        return True
    
    def _store_metric(self, key: str, value: Dict, ttl: int = 3600):
        """Store metric in Redis"""
        if self.redis_client:
            try:
                self.redis_client.setex(key, ttl, json.dumps(value))
            except Exception as e:
                logger.error(f"Failed to store metric: {e}")
    
    def _get_metric(self, key: str) -> Optional[Dict]:
        """Get metric from Redis"""
        if self.redis_client:
            try:
                value = self.redis_client.get(key)
                return json.loads(value) if value else None
            except Exception as e:
                logger.error(f"Failed to get metric: {e}")
        return None
    
    def _append_to_list(self, key: str, value: Dict, max_length: int = 100):
        """Append to a Redis list with max length"""
        if self.redis_client:
            try:
                self.redis_client.lpush(key, json.dumps(value))
                self.redis_client.ltrim(key, 0, max_length - 1)
            except Exception as e:
                logger.error(f"Failed to append to list: {e}")
    
    def _get_list(self, key: str) -> list:
        """Get list from Redis"""
        if self.redis_client:
            try:
                values = self.redis_client.lrange(key, 0, -1)
                return [json.loads(v) for v in values]
            except Exception as e:
                logger.error(f"Failed to get list: {e}")
        return []
    
    def _aggregate_metrics(self, metrics: list) -> Dict[str, Any]:
        """Aggregate ETL metrics"""
        if not metrics:
            return {}
        
        total_records = sum(m.get('records_processed', 0) for m in metrics)
        total_duration = sum(m.get('duration_seconds', 0) for m in metrics)
        success_count = sum(1 for m in metrics if m.get('status') == 'SUCCESS')
        
        return {
            'total_jobs': len(metrics),
            'successful_jobs': success_count,
            'failed_jobs': len(metrics) - success_count,
            'success_rate': (success_count / len(metrics) * 100) if metrics else 0,
            'total_records_processed': total_records,
            'total_duration_seconds': total_duration,
            'average_rps': total_records / total_duration if total_duration > 0 else 0,
            'last_execution': metrics[0].get('end_time') if metrics else None
        }
    
    def _aggregate_test_metrics(self, metrics: list) -> Dict[str, Any]:
        """Aggregate test metrics"""
        if not metrics:
            return {}
        
        total_tests = sum(m.get('total_tests', 0) for m in metrics)
        total_passed = sum(m.get('passed', 0) for m in metrics)
        total_failed = sum(m.get('failed', 0) for m in metrics)
        
        return {
            'total_test_runs': len(metrics),
            'total_tests_executed': total_tests,
            'total_passed': total_passed,
            'total_failed': total_failed,
            'overall_pass_rate': (total_passed / total_tests * 100) if total_tests > 0 else 0,
            'last_execution': metrics[0].get('timestamp') if metrics else None
        }
    
    def export_metrics_to_file(self, filepath: str):
        """Export all metrics to a JSON file"""
        all_metrics = {
            'etl_metrics': self.get_etl_metrics_summary(),
            'test_metrics': self.get_test_metrics_summary(),
            'exported_at': datetime.now().isoformat()
        }
        
        with open(filepath, 'w') as f:
            json.dump(all_metrics, f, indent=2)
        
        logger.info(f"Metrics exported to {filepath}")
    
    def close(self):
        """Close Redis connection"""
        if self.redis_client:
            self.redis_client.close()
            logger.info("Redis connection closed")
