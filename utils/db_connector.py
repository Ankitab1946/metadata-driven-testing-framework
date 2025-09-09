"""
Database connectivity and operations using SQLAlchemy
"""
import pyodbc
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager
from typing import Dict, List, Any, Optional, Tuple
from utils.logger import framework_logger
from config.config import config

class DatabaseConnector:
    """Database connector class for SQL Server operations"""
    
    def __init__(self):
        self.engines: Dict[str, Any] = {}
        self.sessions: Dict[str, Any] = {}
        self._initialize_connections()
    
    def _initialize_connections(self):
        """Initialize database connections"""
        try:
            # Create engines for server1 and server2
            server1_conn = config.get_db_connection_string("server1")
            server2_conn = config.get_db_connection_string("server2")
            
            self.engines['server1'] = create_engine(
                f"mssql+pyodbc:///?odbc_connect={server1_conn}",
                pool_pre_ping=True,
                pool_recycle=3600,
                connect_args={
                    "timeout": config.database.connection_timeout,
                    "autocommit": True
                }
            )
            
            self.engines['server2'] = create_engine(
                f"mssql+pyodbc:///?odbc_connect={server2_conn}",
                pool_pre_ping=True,
                pool_recycle=3600,
                connect_args={
                    "timeout": config.database.connection_timeout,
                    "autocommit": True
                }
            )
            
            # Create session makers
            self.sessions['server1'] = sessionmaker(bind=self.engines['server1'])
            self.sessions['server2'] = sessionmaker(bind=self.engines['server2'])
            
            framework_logger.info("Database connections initialized successfully")
            
        except Exception as e:
            framework_logger.error(f"Error initializing database connections: {str(e)}")
            raise
    
    def get_engine(self, db_name: str):
        """Get database engine based on DB name"""
        if "server1" in db_name.lower():
            return self.engines['server1']
        elif "server2" in db_name.lower():
            return self.engines['server2']
        else:
            # Default to server1
            return self.engines['server1']
    
    @contextmanager
    def get_session(self, db_name: str):
        """Get database session with context manager"""
        engine_key = "server1" if "server1" in db_name.lower() else "server2"
        session = self.sessions[engine_key]()
        
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            framework_logger.error(f"Database session error: {str(e)}")
            raise
        finally:
            session.close()
    
    def test_connection(self, db_name: str) -> bool:
        """Test database connection"""
        try:
            engine = self.get_engine(db_name)
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                result.fetchone()
            framework_logger.info(f"Database connection test successful for {db_name}")
            return True
        except Exception as e:
            framework_logger.error(f"Database connection test failed for {db_name}: {str(e)}")
            return False
    
    def execute_query(self, db_name: str, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame"""
        try:
            engine = self.get_engine(db_name)
            
            if params:
                df = pd.read_sql_query(text(query), engine, params=params)
            else:
                df = pd.read_sql_query(text(query), engine)
            
            framework_logger.info(f"Query executed successfully on {db_name}")
            return df
            
        except Exception as e:
            framework_logger.error(f"Error executing query on {db_name}: {str(e)}")
            raise
    
    def get_table_schema(self, db_name: str, table_name: str) -> Dict[str, Any]:
        """Get table schema information"""
        try:
            engine = self.get_engine(db_name)
            inspector = inspect(engine)
            
            # Get columns info
            columns = inspector.get_columns(table_name)
            
            # Get primary keys
            pk_constraint = inspector.get_pk_constraint(table_name)
            primary_keys = pk_constraint.get('constrained_columns', [])
            
            # Get foreign keys
            foreign_keys = inspector.get_foreign_keys(table_name)
            
            # Get indexes
            indexes = inspector.get_indexes(table_name)
            
            schema_info = {
                'columns': columns,
                'primary_keys': primary_keys,
                'foreign_keys': foreign_keys,
                'indexes': indexes
            }
            
            framework_logger.info(f"Retrieved schema for table {table_name} on {db_name}")
            return schema_info
            
        except Exception as e:
            framework_logger.error(f"Error getting table schema for {table_name} on {db_name}: {str(e)}")
            raise
    
    def check_table_exists(self, db_name: str, table_name: str) -> bool:
        """Check if table exists in database"""
        try:
            engine = self.get_engine(db_name)
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            
            exists = table_name in tables
            framework_logger.info(f"Table {table_name} {'exists' if exists else 'does not exist'} on {db_name}")
            return exists
            
        except Exception as e:
            framework_logger.error(f"Error checking table existence for {table_name} on {db_name}: {str(e)}")
            return False
    
    def get_row_count(self, db_name: str, table_name: str, where_clause: Optional[str] = None) -> int:
        """Get row count for a table"""
        try:
            query = f"SELECT COUNT(*) as row_count FROM {table_name}"
            if where_clause:
                query += f" WHERE {where_clause}"
            
            result = self.execute_query(db_name, query)
            count = result.iloc[0]['row_count']
            
            framework_logger.info(f"Row count for {table_name} on {db_name}: {count}")
            return count
            
        except Exception as e:
            framework_logger.error(f"Error getting row count for {table_name} on {db_name}: {str(e)}")
            raise
    
    def validate_data_types(self, db_name: str, table_name: str, expected_schema: Dict[str, str]) -> Dict[str, bool]:
        """Validate data types against expected schema"""
        try:
            actual_schema = self.get_table_schema(db_name, table_name)
            validation_results = {}
            
            for column_info in actual_schema['columns']:
                column_name = column_info['name']
                actual_type = str(column_info['type']).upper()
                
                if column_name in expected_schema:
                    expected_type = expected_schema[column_name].upper()
                    validation_results[column_name] = self._compare_data_types(actual_type, expected_type)
                else:
                    validation_results[column_name] = False
            
            framework_logger.info(f"Data type validation completed for {table_name} on {db_name}")
            return validation_results
            
        except Exception as e:
            framework_logger.error(f"Error validating data types for {table_name} on {db_name}: {str(e)}")
            raise
    
    def _compare_data_types(self, actual_type: str, expected_type: str) -> bool:
        """Compare actual and expected data types"""
        # Normalize type names for comparison
        type_mappings = {
            'VARCHAR': ['VARCHAR', 'NVARCHAR', 'TEXT', 'STRING'],
            'INTEGER': ['INTEGER', 'INT', 'BIGINT', 'SMALLINT'],
            'DECIMAL': ['DECIMAL', 'NUMERIC', 'FLOAT', 'REAL'],
            'DATETIME': ['DATETIME', 'TIMESTAMP', 'DATE', 'TIME'],
            'BOOLEAN': ['BOOLEAN', 'BIT']
        }
        
        for standard_type, variants in type_mappings.items():
            if any(variant in actual_type for variant in variants) and any(variant in expected_type for variant in variants):
                return True
        
        return actual_type == expected_type
    
    def check_nullable_constraints(self, db_name: str, table_name: str, column_name: str) -> Tuple[bool, int]:
        """Check nullable constraints for a column"""
        try:
            # Check if column allows nulls in schema
            schema = self.get_table_schema(db_name, table_name)
            column_nullable = None
            
            for col in schema['columns']:
                if col['name'] == column_name:
                    column_nullable = col['nullable']
                    break
            
            # Count null values in the column
            query = f"SELECT COUNT(*) as null_count FROM {table_name} WHERE {column_name} IS NULL"
            result = self.execute_query(db_name, query)
            null_count = result.iloc[0]['null_count']
            
            framework_logger.info(f"Nullable check for {column_name} in {table_name}: nullable={column_nullable}, null_count={null_count}")
            return column_nullable, null_count
            
        except Exception as e:
            framework_logger.error(f"Error checking nullable constraints for {column_name} in {table_name}: {str(e)}")
            raise
    
    def check_unique_constraints(self, db_name: str, table_name: str, column_name: str) -> Tuple[int, int]:
        """Check unique constraints for a column"""
        try:
            # Get total count
            total_query = f"SELECT COUNT(*) as total_count FROM {table_name}"
            total_result = self.execute_query(db_name, total_query)
            total_count = total_result.iloc[0]['total_count']
            
            # Get distinct count
            distinct_query = f"SELECT COUNT(DISTINCT {column_name}) as distinct_count FROM {table_name}"
            distinct_result = self.execute_query(db_name, distinct_query)
            distinct_count = distinct_result.iloc[0]['distinct_count']
            
            framework_logger.info(f"Unique check for {column_name} in {table_name}: total={total_count}, distinct={distinct_count}")
            return total_count, distinct_count
            
        except Exception as e:
            framework_logger.error(f"Error checking unique constraints for {column_name} in {table_name}: {str(e)}")
            raise
    
    def check_range_constraints(self, db_name: str, table_name: str, column_name: str, 
                              range_bottom: Optional[str], range_top: Optional[str]) -> Dict[str, int]:
        """Check range constraints for a column"""
        try:
            results = {}
            
            if range_bottom:
                query = f"SELECT COUNT(*) as below_range FROM {table_name} WHERE {column_name} < {range_bottom}"
                result = self.execute_query(db_name, query)
                results['below_range'] = result.iloc[0]['below_range']
            
            if range_top:
                query = f"SELECT COUNT(*) as above_range FROM {table_name} WHERE {column_name} > {range_top}"
                result = self.execute_query(db_name, query)
                results['above_range'] = result.iloc[0]['above_range']
            
            framework_logger.info(f"Range check for {column_name} in {table_name}: {results}")
            return results
            
        except Exception as e:
            framework_logger.error(f"Error checking range constraints for {column_name} in {table_name}: {str(e)}")
            raise
    
    def check_enumeration_constraints(self, db_name: str, table_name: str, column_name: str, 
                                    allowed_values: List[str]) -> int:
        """Check enumeration constraints for a column"""
        try:
            # Create IN clause with allowed values
            values_str = "', '".join(allowed_values)
            query = f"SELECT COUNT(*) as invalid_count FROM {table_name} WHERE {column_name} NOT IN ('{values_str}')"
            
            result = self.execute_query(db_name, query)
            invalid_count = result.iloc[0]['invalid_count']
            
            framework_logger.info(f"Enumeration check for {column_name} in {table_name}: invalid_count={invalid_count}")
            return invalid_count
            
        except Exception as e:
            framework_logger.error(f"Error checking enumeration constraints for {column_name} in {table_name}: {str(e)}")
            raise
    
    def close_connections(self):
        """Close all database connections"""
        try:
            for engine in self.engines.values():
                engine.dispose()
            framework_logger.info("All database connections closed")
        except Exception as e:
            framework_logger.error(f"Error closing database connections: {str(e)}")

# Global database connector instance
db_connector = DatabaseConnector()
