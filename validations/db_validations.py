"""
Database validation utilities for data quality checks
"""
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
from utils.logger import framework_logger
from utils.db_connector import db_connector
from utils.excel_reader import FeedMetadata, StagingMetadata, EnumerationMetadata

class DatabaseValidator:
    """Database validation utilities"""
    
    def __init__(self):
        self.db_connector = db_connector
    
    def validate_data_types(self, metadata: List[FeedMetadata], selected_feeds: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Validate data types against metadata definitions
        
        Args:
            metadata: List of feed metadata
            selected_feeds: List of specific feeds to validate (None for all)
        
        Returns:
            List of validation results
        """
        results = []
        
        try:
            framework_logger.info("Starting data type validation")
            
            # Filter metadata by selected feeds if specified
            if selected_feeds:
                metadata = [meta for meta in metadata if meta.feed in selected_feeds and (not hasattr(meta, 'skiprow') or str(meta.skiprow).strip() != '#')]
            else:
                # Exclude skipped rows
                metadata = [meta for meta in metadata if not (hasattr(meta, 'skiprow') and str(meta.skiprow).strip() == '#')]
            
            # Group metadata by database and table
            table_groups = {}
            for meta in metadata:
                key = f"{meta.db_name}.{meta.db_table}"
                if key not in table_groups:
                    table_groups[key] = []
                table_groups[key].append(meta)
            
            # Validate each table
            for table_key, table_metadata in table_groups.items():
                db_name, table_name = table_key.split('.', 1)
                
                # Get where clause if available
                where_clause = None
                for meta in table_metadata:
                    if hasattr(meta, 'where_clause') and meta.where_clause:
                        where_clause = meta.where_clause
                        break
                
                result = {
                    'validation_type': 'data_type',
                    'db_name': db_name,
                    'table_name': table_name,
                    'feed_name': table_metadata[0].feed,
                    'column_validations': [],
                    'validation_status': 'PASS',
                    'error_message': None
                }
                
                try:
                    # Get actual table schema
                    actual_schema = self.db_connector.get_table_schema(db_name, table_name)
                    
                    # Create expected schema from metadata
                    expected_schema = {}
                    for meta in table_metadata:
                        expected_schema[meta.field_name] = meta.data_type
                    
                    # Validate data types
                    type_validation_results = self.db_connector.validate_data_types(
                        db_name, table_name, expected_schema
                    )
                    
                    # Process results
                    for column_name, is_valid in type_validation_results.items():
                        column_meta = next((m for m in table_metadata if m.field_name == column_name), None)
                        
                        column_result = {
                            'column_name': column_name,
                            'expected_type': column_meta.data_type if column_meta else 'Unknown',
                            'validation_passed': is_valid,
                            'error_message': None if is_valid else f"Data type mismatch for column {column_name}"
                        }
                        
                        result['column_validations'].append(column_result)
                        
                        if not is_valid:
                            result['validation_status'] = 'FAIL'
                    
                    framework_logger.info(f"Data type validation completed for {table_key}")
                    
                except Exception as e:
                    result['validation_status'] = 'ERROR'
                    result['error_message'] = f"Error validating data types: {str(e)}"
                    framework_logger.error(result['error_message'])
                
                results.append(result)
            
            return results
            
        except Exception as e:
            error_msg = f"Error in data type validation: {str(e)}"
            framework_logger.error(error_msg)
            return [{
                'validation_type': 'data_type',
                'validation_status': 'ERROR',
                'error_message': error_msg
            }]
    
    def validate_nullable_constraints(self, metadata: List[FeedMetadata], selected_feeds: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Validate nullable/mandatory constraints
        
        Args:
            metadata: List of feed metadata
            selected_feeds: List of specific feeds to validate (None for all)
        
        Returns:
            List of validation results
        """
        results = []
        
        try:
            framework_logger.info("Starting nullable constraints validation")
            
            # Filter metadata by selected feeds if specified
            if selected_feeds:
                metadata = [meta for meta in metadata if meta.feed in selected_feeds]
            
            for meta in metadata:
                result = {
                    'validation_type': 'nullable_constraint',
                    'db_name': meta.db_name,
                    'table_name': meta.db_table,
                    'column_name': meta.field_name,
                    'feed_name': meta.feed,
                    'expected_nullable': meta.nullable,
                    'expected_mandatory': meta.mandatory,
                    'actual_nullable': None,
                    'null_count': 0,
                    'validation_status': 'PASS',
                    'error_message': None
                }
                
                try:
                    # Check nullable constraints
                    actual_nullable, null_count = self.db_connector.check_nullable_constraints(
                        meta.db_name, meta.db_table, meta.field_name
                    )
                    
                    result['actual_nullable'] = actual_nullable
                    result['null_count'] = null_count
                    
                    # Validate constraints
                    validation_passed = True
                    error_messages = []
                    
                    # Check if mandatory field has null values
                    if meta.mandatory.upper() == 'Y' and null_count > 0:
                        validation_passed = False
                        error_messages.append(f"Mandatory field has {null_count} null values")
                    
                    # Check if nullable setting matches metadata
                    expected_nullable_bool = meta.nullable.upper() == 'Y'
                    if actual_nullable != expected_nullable_bool:
                        validation_passed = False
                        error_messages.append(f"Nullable setting mismatch: expected {expected_nullable_bool}, got {actual_nullable}")
                    
                    if not validation_passed:
                        result['validation_status'] = 'FAIL'
                        result['error_message'] = "; ".join(error_messages)
                    
                    framework_logger.info(f"Nullable validation completed for {meta.field_name}")
                    
                except Exception as e:
                    result['validation_status'] = 'ERROR'
                    result['error_message'] = f"Error validating nullable constraints: {str(e)}"
                    framework_logger.error(result['error_message'])
                
                results.append(result)
            
            return results
            
        except Exception as e:
            error_msg = f"Error in nullable constraints validation: {str(e)}"
            framework_logger.error(error_msg)
            return [{
                'validation_type': 'nullable_constraint',
                'validation_status': 'ERROR',
                'error_message': error_msg
            }]
    
    def validate_unique_constraints(self, metadata: List[FeedMetadata], selected_feeds: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Validate unique constraints
        
        Args:
            metadata: List of feed metadata
            selected_feeds: List of specific feeds to validate (None for all)
        
        Returns:
            List of validation results
        """
        results = []
        
        try:
            framework_logger.info("Starting unique constraints validation")
            
            # Filter metadata by selected feeds if specified
            if selected_feeds:
                metadata = [meta for meta in metadata if meta.feed in selected_feeds]
            
            # Filter only columns that should be unique
            unique_metadata = [meta for meta in metadata if meta.unique.upper() == 'Y']
            
            for meta in unique_metadata:
                result = {
                    'validation_type': 'unique_constraint',
                    'db_name': meta.db_name,
                    'table_name': meta.db_table,
                    'column_name': meta.field_name,
                    'feed_name': meta.feed,
                    'expected_unique': meta.unique,
                    'total_count': 0,
                    'distinct_count': 0,
                    'duplicate_count': 0,
                    'validation_status': 'PASS',
                    'error_message': None
                }
                
                try:
                    # Check unique constraints
                    total_count, distinct_count = self.db_connector.check_unique_constraints(
                        meta.db_name, meta.db_table, meta.field_name
                    )
                    
                    result['total_count'] = total_count
                    result['distinct_count'] = distinct_count
                    result['duplicate_count'] = total_count - distinct_count
                    
                    # Validate uniqueness
                    if total_count != distinct_count:
                        result['validation_status'] = 'FAIL'
                        result['error_message'] = f"Column should be unique but has {result['duplicate_count']} duplicates"
                    
                    framework_logger.info(f"Unique validation completed for {meta.field_name}")
                    
                except Exception as e:
                    result['validation_status'] = 'ERROR'
                    result['error_message'] = f"Error validating unique constraints: {str(e)}"
                    framework_logger.error(result['error_message'])
                
                results.append(result)
            
            return results
            
        except Exception as e:
            error_msg = f"Error in unique constraints validation: {str(e)}"
            framework_logger.error(error_msg)
            return [{
                'validation_type': 'unique_constraint',
                'validation_status': 'ERROR',
                'error_message': error_msg
            }]
    
    def validate_range_constraints(self, metadata: List[FeedMetadata], selected_feeds: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Validate range constraints
        
        Args:
            metadata: List of feed metadata
            selected_feeds: List of specific feeds to validate (None for all)
        
        Returns:
            List of validation results
        """
        results = []
        
        try:
            framework_logger.info("Starting range constraints validation")
            
            # Filter metadata by selected feeds if specified
            if selected_feeds:
                metadata = [meta for meta in metadata if meta.feed in selected_feeds]
            
            # Filter only columns that have range constraints
            range_metadata = [meta for meta in metadata if meta.range_bottom or meta.range_top]
            
            for meta in range_metadata:
                result = {
                    'validation_type': 'range_constraint',
                    'db_name': meta.db_name,
                    'table_name': meta.db_table,
                    'column_name': meta.field_name,
                    'feed_name': meta.feed,
                    'range_bottom': meta.range_bottom,
                    'range_top': meta.range_top,
                    'below_range_count': 0,
                    'above_range_count': 0,
                    'validation_status': 'PASS',
                    'error_message': None
                }
                
                try:
                    # Check range constraints
                    range_results = self.db_connector.check_range_constraints(
                        meta.db_name, meta.db_table, meta.field_name,
                        meta.range_bottom, meta.range_top
                    )
                    
                    result['below_range_count'] = range_results.get('below_range', 0)
                    result['above_range_count'] = range_results.get('above_range', 0)
                    
                    # Validate range
                    total_violations = result['below_range_count'] + result['above_range_count']
                    if total_violations > 0:
                        result['validation_status'] = 'FAIL'
                        result['error_message'] = f"Range violations: {result['below_range_count']} below range, {result['above_range_count']} above range"
                    
                    framework_logger.info(f"Range validation completed for {meta.field_name}")
                    
                except Exception as e:
                    result['validation_status'] = 'ERROR'
                    result['error_message'] = f"Error validating range constraints: {str(e)}"
                    framework_logger.error(result['error_message'])
                
                results.append(result)
            
            return results
            
        except Exception as e:
            error_msg = f"Error in range constraints validation: {str(e)}"
            framework_logger.error(error_msg)
            return [{
                'validation_type': 'range_constraint',
                'validation_status': 'ERROR',
                'error_message': error_msg
            }]
    
    def validate_enumeration_constraints(self, metadata: List[FeedMetadata], 
                                       enumeration_metadata: List[EnumerationMetadata],
                                       selected_feeds: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Validate enumeration constraints
        
        Args:
            metadata: List of feed metadata
            enumeration_metadata: List of enumeration metadata
            selected_feeds: List of specific feeds to validate (None for all)
        
        Returns:
            List of validation results
        """
        results = []
        
        try:
            framework_logger.info("Starting enumeration constraints validation")
            
            # Filter metadata by selected feeds if specified
            if selected_feeds:
                metadata = [meta for meta in metadata if meta.feed in selected_feeds]
            
            # Filter only columns that have enumeration constraints
            enum_metadata = [meta for meta in metadata if meta.enumeration]
            
            # Create enumeration lookup
            enum_lookup = {}
            for enum_meta in enumeration_metadata:
                if enum_meta.enumeration_name not in enum_lookup:
                    enum_lookup[enum_meta.enumeration_name] = []
                enum_lookup[enum_meta.enumeration_name].append(enum_meta.enum_values)
            
            for meta in enum_metadata:
                result = {
                    'validation_type': 'enumeration_constraint',
                    'db_name': meta.db_name,
                    'table_name': meta.db_table,
                    'column_name': meta.field_name,
                    'feed_name': meta.feed,
                    'enumeration_name': meta.enumeration,
                    'allowed_values': [],
                    'invalid_count': 0,
                    'validation_status': 'PASS',
                    'error_message': None
                }
                
                try:
                    # Get allowed values for this enumeration
                    allowed_values = enum_lookup.get(meta.enumeration, [])
                    result['allowed_values'] = allowed_values
                    
                    if not allowed_values:
                        result['validation_status'] = 'ERROR'
                        result['error_message'] = f"Enumeration {meta.enumeration} not found in metadata"
                        results.append(result)
                        continue
                    
                    # Check enumeration constraints
                    invalid_count = self.db_connector.check_enumeration_constraints(
                        meta.db_name, meta.db_table, meta.field_name, allowed_values
                    )
                    
                    result['invalid_count'] = invalid_count
                    
                    # Validate enumeration
                    if invalid_count > 0:
                        result['validation_status'] = 'FAIL'
                        result['error_message'] = f"Found {invalid_count} values not in allowed enumeration"
                    
                    framework_logger.info(f"Enumeration validation completed for {meta.field_name}")
                    
                except Exception as e:
                    result['validation_status'] = 'ERROR'
                    result['error_message'] = f"Error validating enumeration constraints: {str(e)}"
                    framework_logger.error(result['error_message'])
                
                results.append(result)
            
            return results
            
        except Exception as e:
            error_msg = f"Error in enumeration constraints validation: {str(e)}"
            framework_logger.error(error_msg)
            return [{
                'validation_type': 'enumeration_constraint',
                'validation_status': 'ERROR',
                'error_message': error_msg
            }]
    
    def validate_insert_append_logic(self, metadata: List[FeedMetadata], 
                                   selected_feeds: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Validate insert vs append logic
        
        Args:
            metadata: List of feed metadata
            selected_feeds: List of specific feeds to validate (None for all)
        
        Returns:
            List of validation results
        """
        results = []
        
        try:
            framework_logger.info("Starting insert/append logic validation")
            
            # Filter metadata by selected feeds if specified
            if selected_feeds:
                metadata = [meta for meta in metadata if meta.feed in selected_feeds]
            
            # Group by table for validation
            table_groups = {}
            for meta in metadata:
                key = f"{meta.db_name}.{meta.db_table}"
                if key not in table_groups:
                    table_groups[key] = []
                table_groups[key].append(meta)
            
            for table_key, table_metadata in table_groups.items():
                db_name, table_name = table_key.split('.', 1)
                request_type = table_metadata[0].request  # Assuming all columns in table have same request type
                
                result = {
                    'validation_type': 'insert_append_logic',
                    'db_name': db_name,
                    'table_name': table_name,
                    'feed_name': table_metadata[0].feed,
                    'request_type': request_type,
                    'current_row_count': 0,
                    'expected_behavior': '',
                    'validation_status': 'PASS',
                    'error_message': None
                }
                
                try:
                    # Get current row count
                    current_count = self.db_connector.get_row_count(db_name, table_name)
                    result['current_row_count'] = current_count
                    
                    if request_type.upper() == 'INSERT':
                        result['expected_behavior'] = 'Fresh data load - table should be truncated before insert'
                        # For insert, we would need to check if data is fresh
                        # This could involve checking timestamps or comparing with previous loads
                        
                    elif request_type.upper() == 'APPEND':
                        result['expected_behavior'] = 'Data should be appended to existing data'
                        # For append, we would check if new data is added without removing old data
                        
                    else:
                        result['validation_status'] = 'ERROR'
                        result['error_message'] = f"Unknown request type: {request_type}"
                    
                    framework_logger.info(f"Insert/append validation completed for {table_key}")
                    
                except Exception as e:
                    result['validation_status'] = 'ERROR'
                    result['error_message'] = f"Error validating insert/append logic: {str(e)}"
                    framework_logger.error(result['error_message'])
                
                results.append(result)
            
            return results
            
        except Exception as e:
            error_msg = f"Error in insert/append logic validation: {str(e)}"
            framework_logger.error(error_msg)
            return [{
                'validation_type': 'insert_append_logic',
                'validation_status': 'ERROR',
                'error_message': error_msg
            }]
    
    def validate_count_checks(self, metadata: List[FeedMetadata], 
                            expected_counts: Optional[Dict[str, int]] = None,
                            selected_feeds: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Validate row count checks
        
        Args:
            metadata: List of feed metadata
            expected_counts: Dictionary of expected counts per table
            selected_feeds: List of specific feeds to validate (None for all)
        
        Returns:
            List of validation results
        """
        results = []
        
        try:
            framework_logger.info("Starting count checks validation")
            
            # Filter metadata by selected feeds if specified
            if selected_feeds:
                metadata = [meta for meta in metadata if meta.feed in selected_feeds]
            
            # Group by table for validation
            table_groups = {}
            for meta in metadata:
                key = f"{meta.db_name}.{meta.db_table}"
                if key not in table_groups:
                    table_groups[key] = []
                table_groups[key].append(meta)
            
            for table_key, table_metadata in table_groups.items():
                db_name, table_name = table_key.split('.', 1)
                
                result = {
                    'validation_type': 'count_check',
                    'db_name': db_name,
                    'table_name': table_name,
                    'feed_name': table_metadata[0].feed,
                    'actual_count': 0,
                    'expected_count': None,
                    'validation_status': 'PASS',
                    'error_message': None
                }
                
                try:
                    # Get actual row count
                    actual_count = self.db_connector.get_row_count(db_name, table_name)
                    result['actual_count'] = actual_count
                    
                    # Check against expected count if provided
                    if expected_counts and table_key in expected_counts:
                        expected_count = expected_counts[table_key]
                        result['expected_count'] = expected_count
                        
                        if actual_count != expected_count:
                            result['validation_status'] = 'FAIL'
                            result['error_message'] = f"Count mismatch: expected {expected_count}, got {actual_count}"
                    
                    # Basic validation - check if table has data
                    if actual_count == 0:
                        result['validation_status'] = 'FAIL'
                        result['error_message'] = "Table is empty"
                    
                    framework_logger.info(f"Count check completed for {table_key}: {actual_count} rows")
                    
                except Exception as e:
                    result['validation_status'] = 'ERROR'
                    result['error_message'] = f"Error validating count checks: {str(e)}"
                    framework_logger.error(result['error_message'])
                
                results.append(result)
            
            return results
            
        except Exception as e:
            error_msg = f"Error in count checks validation: {str(e)}"
            framework_logger.error(error_msg)
            return [{
                'validation_type': 'count_check',
                'validation_status': 'ERROR',
                'error_message': error_msg
            }]
    
    def validate_completeness_checks(self, metadata: List[FeedMetadata], 
                                   selected_feeds: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Validate data completeness checks
        
        Args:
            metadata: List of feed metadata
            selected_feeds: List of specific feeds to validate (None for all)
        
        Returns:
            List of validation results
        """
        results = []
        
        try:
            framework_logger.info("Starting completeness checks validation")
            
            # Filter metadata by selected feeds if specified
            if selected_feeds:
                metadata = [meta for meta in metadata if meta.feed in selected_feeds]
            
            # Group by table for validation
            table_groups = {}
            for meta in metadata:
                key = f"{meta.db_name}.{meta.db_table}"
                if key not in table_groups:
                    table_groups[key] = []
                table_groups[key].append(meta)
            
            for table_key, table_metadata in table_groups.items():
                db_name, table_name = table_key.split('.', 1)
                
                result = {
                    'validation_type': 'completeness_check',
                    'db_name': db_name,
                    'table_name': table_name,
                    'feed_name': table_metadata[0].feed,
                    'total_rows': 0,
                    'column_completeness': [],
                    'overall_completeness_score': 0.0,
                    'validation_status': 'PASS',
                    'error_message': None
                }
                
                try:
                    # Get total row count
                    total_rows = self.db_connector.get_row_count(db_name, table_name)
                    result['total_rows'] = total_rows
                    
                    if total_rows == 0:
                        result['validation_status'] = 'FAIL'
                        result['error_message'] = "Table is empty - cannot check completeness"
                        results.append(result)
                        continue
                    
                    # Check completeness for each column
                    completeness_scores = []
                    
                    for meta in table_metadata:
                        # Get null count for column
                        _, null_count = self.db_connector.check_nullable_constraints(
                            db_name, table_name, meta.field_name
                        )
                        
                        completeness_percentage = ((total_rows - null_count) / total_rows) * 100
                        completeness_scores.append(completeness_percentage)
                        
                        column_result = {
                            'column_name': meta.field_name,
                            'total_rows': total_rows,
                            'null_count': null_count,
                            'completeness_percentage': round(completeness_percentage, 2),
                            'is_mandatory': meta.mandatory.upper() == 'Y'
                        }
                        
                        result['column_completeness'].append(column_result)
                    
                    # Calculate overall completeness score
                    overall_score = sum(completeness_scores) / len(completeness_scores)
                    result['overall_completeness_score'] = round(overall_score, 2)
                    
                    # Validate completeness (fail if overall score < 95%)
                    if overall_score < 95.0:
                        result['validation_status'] = 'FAIL'
                        result['error_message'] = f"Low completeness score: {overall_score}%"
                    
                    framework_logger.info(f"Completeness check completed for {table_key}: {overall_score}%")
                    
                except Exception as e:
                    result['validation_status'] = 'ERROR'
                    result['error_message'] = f"Error validating completeness checks: {str(e)}"
                    framework_logger.error(result['error_message'])
                
                results.append(result)
            
            return results
            
        except Exception as e:
            error_msg = f"Error in completeness checks validation: {str(e)}"
            framework_logger.error(error_msg)
            return [{
                'validation_type': 'completeness_check',
                'validation_status': 'ERROR',
                'error_message': error_msg
            }]

# Global database validator instance
db_validator = DatabaseValidator()
