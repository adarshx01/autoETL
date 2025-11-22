from sqlalchemy import create_engine, pool, MetaData, Table, inspect, text
from sqlalchemy.orm import sessionmaker, scoped_session
from typing import List, Dict, Any, Optional, Generator
import pandas as pd
from contextlib import contextmanager
import logging
from tenacity import retry, stop_after_attempt, wait_exponential
from datetime import datetime
import hashlib

logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Enterprise-grade database manager with:
    - Connection pooling and health monitoring
    - Automatic retry with exponential backoff
    - Transaction management and rollback support
    - Support for PostgreSQL, MySQL, SQL Server, Oracle
    - Handles millions of records efficiently
    """
    
    def __init__(self, connection_string: str, pool_size: int = 20, max_retries: int = 3):
        self.connection_string = connection_string
        self.max_retries = max_retries
        self.engine = create_engine(
            connection_string,
            poolclass=pool.QueuePool,
            pool_size=pool_size,
            max_overflow=40,
            pool_pre_ping=True,
            pool_recycle=3600,
            echo=False,
            connect_args=self._get_connect_args()
        )
        self.SessionLocal = scoped_session(sessionmaker(bind=self.engine))
        self.metadata = MetaData()
        self._verify_connection()
    
    def _get_connect_args(self) -> Dict:
        """Get database-specific connection arguments"""
        if 'postgresql' in self.connection_string:
            return {'connect_timeout': 10}
        elif 'mysql' in self.connection_string:
            return {'connect_timeout': 10}
        elif 'mssql' in self.connection_string or 'sqlserver' in self.connection_string:
            return {'timeout': 10}
        return {}
    
    def _verify_connection(self):
        """Verify database connection on initialization"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection verified successfully")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
        
    @contextmanager
    def get_session(self):
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            session.close()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def read_table_batch(self, table_name: str, batch_size: int = 10000, 
                         columns: List[str] = None, filters: Dict = None,
                         order_by: str = None) -> Generator[pd.DataFrame, None, None]:
        """
        Read table in batches for memory efficiency
        Supports filtering for targeted data extraction
        Uses streaming to handle millions of records
        
        Args:
            table_name: Name of the table to read
            batch_size: Number of records per batch
            columns: Specific columns to select
            filters: WHERE clause filters as dict
            order_by: Column to order by for consistent batching
        """
        query = f"SELECT {','.join(columns) if columns else '*'} FROM {table_name}"
        
        if filters:
            where_clause = " AND ".join([f"{k}=:{k}" for k in filters.keys()])
            query += f" WHERE {where_clause}"
        
        if order_by:
            query += f" ORDER BY {order_by}"
        
        offset = 0
        total_processed = 0
        
        while True:
            batch_query = f"{query} LIMIT {batch_size} OFFSET {offset}"
            
            try:
                df = pd.read_sql(text(batch_query), self.engine, params=filters or {})
                
                if df.empty:
                    logger.info(f"Batch processing complete. Total records: {total_processed}")
                    break
                    
                total_processed += len(df)
                logger.debug(f"Fetched batch: {len(df)} records (Total: {total_processed})")
                yield df
                offset += batch_size
                
            except Exception as e:
                logger.error(f"Batch read failed at offset {offset}: {e}")
                raise
    
    def execute_query(self, query: str, params: Dict = None) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame"""
        try:
            return pd.read_sql(query, self.engine, params=params)
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
    
    def bulk_insert(self, table_name: str, data: pd.DataFrame, 
                    chunk_size: int = 5000):
        """Optimized bulk insert for large datasets"""
        try:
            data.to_sql(
                table_name, 
                self.engine, 
                if_exists='append', 
                index=False,
                chunksize=chunk_size,
                method='multi'
            )
            logger.info(f"Inserted {len(data)} records into {table_name}")
        except Exception as e:
            logger.error(f"Bulk insert failed: {e}")
            raise
    
    def get_table_schema(self, table_name: str) -> Dict:
        """Get table schema for validation"""
        inspector = inspect(self.engine)
        columns = inspector.get_columns(table_name)
        return {col['name']: str(col['type']) for col in columns}
    
    def get_table_count(self, table_name: str, filters: Dict = None) -> int:
        """Get total record count for a table"""
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        if filters:
            where_clause = " AND ".join([f"{k}=:{k}" for k in filters.keys()])
            query += f" WHERE {where_clause}"
        
        result = pd.read_sql(text(query), self.engine, params=filters or {})
        return int(result['count'].iloc[0])
    
    def get_table_checksum(self, table_name: str, columns: List[str]) -> str:
        """Generate checksum for data validation"""
        query = f"SELECT {','.join(columns)} FROM {table_name} ORDER BY {columns[0]}"
        df = pd.read_sql(text(query), self.engine)
        data_string = df.to_json()
        return hashlib.md5(data_string.encode()).hexdigest()
    
    def create_snapshot(self, table_name: str, snapshot_name: str):
        """Create a snapshot of a table for rollback"""
        with self.get_session() as session:
            session.execute(text(f"CREATE TABLE {snapshot_name} AS SELECT * FROM {table_name}"))
        logger.info(f"Snapshot created: {snapshot_name}")
    
    def rollback_from_snapshot(self, table_name: str, snapshot_name: str):
        """Rollback table to a previous snapshot"""
        with self.get_session() as session:
            session.execute(text(f"TRUNCATE TABLE {table_name}"))
            session.execute(text(f"INSERT INTO {table_name} SELECT * FROM {snapshot_name}"))
        logger.info(f"Rolled back {table_name} from {snapshot_name}")
    
    def close(self):
        """Close all connections"""
        self.engine.dispose()
        logger.info("Database connections closed")


