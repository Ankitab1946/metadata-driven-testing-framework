"""
Feed validation tests - File availability, Autosys jobs, and basic feed checks
"""
import pytest
from typing import List, Dict, Any
from validations.file_checks import file_validator
from validations.autosys_checks import autosys_validator
from tests.conftest import assert_validation_result, create_validation_result

@pytest.mark.file_validation
class TestFeedFileValidations:
    """Test class for feed file validations"""
    
    def test_feed_file_availability(self, metadata_reader, selected_feeds, test_result_collector):
        """Test feed file availability in specified paths"""
        # Get feed metadata
        feed_metadata = metadata_reader.feed_metadata
        
        # Filter by selected feeds if specified
        if selected_feeds:
            feed_metadata = [meta for meta in feed_metadata if meta.feed in selected_feeds]
        
        # Get unique feeds to test
        unique_feeds = list(set([meta.feed for meta in feed_metadata]))
        
        for feed_name in unique_feeds:
            # Check feed file availability
            result = file_validator.check_feed_file_availability(
                feed_name=feed_name,
                file_pattern=f"{feed_name}*",
                expected_date=None  # Could be parameterized
            )
            
            # Add to test results
            test_result_collector(result)
            
            # Assert validation
            assert_validation_result(result, 'PASS')
    
    def test_feed_file_freshness(self, metadata_reader, selected_feeds, test_result_collector):
        """Test feed file freshness (within 24 hours)"""
        # Get feed metadata
        feed_metadata = metadata_reader.feed_metadata
        
        # Filter by selected feeds if specified
        if selected_feeds:
            feed_metadata = [meta for meta in feed_metadata if meta.feed in selected_feeds]
        
        # Get unique feeds to test
        unique_feeds = list(set([meta.feed for meta in feed_metadata]))
        
        for feed_name in unique_feeds:
            # First check if file exists
            availability_result = file_validator.check_feed_file_availability(feed_name)
            
            if availability_result['validation_status'] == 'PASS' and availability_result['files_found']:
                latest_file_path = availability_result['files_found'][0]['file_path']
                
                # Check file freshness
                freshness_result = file_validator.check_file_freshness(
                    file_path=latest_file_path,
                    max_age_hours=24
                )
                
                # Add to test results
                test_result_collector(freshness_result)
                
                # Assert validation
                assert_validation_result(freshness_result, 'PASS')
    
    def test_feed_file_size_validation(self, metadata_reader, selected_feeds, test_result_collector):
        """Test feed file size validation"""
        # Get feed metadata
        feed_metadata = metadata_reader.feed_metadata
        
        # Filter by selected feeds if specified
        if selected_feeds:
            feed_metadata = [meta for meta in feed_metadata if meta.feed in selected_feeds]
        
        # Get unique feeds to test
        unique_feeds = list(set([meta.feed for meta in feed_metadata]))
        
        for feed_name in unique_feeds:
            # First check if file exists
            availability_result = file_validator.check_feed_file_availability(feed_name)
            
            if availability_result['validation_status'] == 'PASS' and availability_result['files_found']:
                latest_file_path = availability_result['files_found'][0]['file_path']
                
                # Check file size (minimum 1KB, maximum 1GB)
                size_result = file_validator.validate_file_size(
                    file_path=latest_file_path,
                    min_size_mb=0.001,  # 1KB
                    max_size_mb=1024    # 1GB
                )
                
                # Add to test results
                test_result_collector(size_result)
                
                # Assert validation
                assert_validation_result(size_result, 'PASS')

@pytest.mark.autosys_validation
class TestAutosysValidations:
    """Test class for Autosys job validations"""
    
    def test_autosys_job_availability(self, metadata_reader, selected_feeds, test_result_collector):
        """Test Autosys job availability in specified environment"""
        # Get feed metadata to derive job names
        feed_metadata = metadata_reader.feed_metadata
        
        # Filter by selected feeds if specified
        if selected_feeds:
            feed_metadata = [meta for meta in feed_metadata if meta.feed in selected_feeds]
        
        # Get unique feeds and create job names
        unique_feeds = list(set([meta.feed for meta in feed_metadata]))
        
        for feed_name in unique_feeds:
            # Create job name (convention: FEED_LOAD_<FEED_NAME>)
            job_name = f"FEED_LOAD_{feed_name.upper()}"
            
            # Check job availability
            result = autosys_validator.check_job_availability(job_name)
            
            # Add to test results
            test_result_collector(result)
            
            # Assert validation
            assert_validation_result(result, 'PASS')
    
    def test_autosys_job_success_status(self, metadata_reader, selected_feeds, test_result_collector):
        """Test Autosys job success status"""
        # Get feed metadata to derive job names
        feed_metadata = metadata_reader.feed_metadata
        
        # Filter by selected feeds if specified
        if selected_feeds:
            feed_metadata = [meta for meta in feed_metadata if meta.feed in selected_feeds]
        
        # Get unique feeds and create job names
        unique_feeds = list(set([meta.feed for meta in feed_metadata]))
        
        for feed_name in unique_feeds:
            # Create job name (convention: FEED_LOAD_<FEED_NAME>)
            job_name = f"FEED_LOAD_{feed_name.upper()}"
            
            # Check job status (expecting Success)
            result = autosys_validator.check_job_status(job_name, expected_status='SU')
            
            # Add to test results
            test_result_collector(result)
            
            # Assert validation
            assert_validation_result(result, 'PASS')
    
    def test_autosys_job_failure_log_generation(self, metadata_reader, selected_feeds, test_result_collector):
        """Test log file generation for failed Autosys jobs"""
        # Get feed metadata to derive job names
        feed_metadata = metadata_reader.feed_metadata
        
        # Filter by selected feeds if specified
        if selected_feeds:
            feed_metadata = [meta for meta in feed_metadata if meta.feed in selected_feeds]
        
        # Get unique feeds and create job names
        unique_feeds = list(set([meta.feed for meta in feed_metadata]))
        
        for feed_name in unique_feeds:
            # Create job name (convention: FEED_LOAD_<FEED_NAME>)
            job_name = f"FEED_LOAD_{feed_name.upper()}"
            
            # First check job status
            status_result = autosys_validator.check_job_status(job_name)
            
            # If job failed, check for log file generation
            if status_result.get('current_status') == 'FA':  # Failed
                log_result = file_validator.check_log_file_generation(
                    job_name=job_name,
                    log_type='error'
                )
                
                # Add to test results
                test_result_collector(log_result)
                
                # Assert validation - log should exist for failed jobs
                assert_validation_result(log_result, 'PASS')
            else:
                # Create a result indicating job didn't fail (so no log check needed)
                result = create_validation_result(
                    validation_type='log_generation',
                    status='PASS',
                    job_name=job_name,
                    message=f"Job {job_name} did not fail, no error log expected"
                )
                test_result_collector(result)

@pytest.mark.db_validation
class TestDatabaseLoadValidations:
    """Test class for database load validations"""
    
    def test_feed_data_loaded_after_job_success(self, metadata_reader, database_connector, 
                                              selected_feeds, test_result_collector):
        """Test that feed data is loaded into database after successful Autosys job"""
        # Get feed metadata
        feed_metadata = metadata_reader.feed_metadata
        
        # Filter by selected feeds if specified
        if selected_feeds:
            feed_metadata = [meta for meta in feed_metadata if meta.feed in selected_feeds]
        
        # Group by table for validation
        table_groups = {}
        for meta in feed_metadata:
            key = f"{meta.db_name}.{meta.db_table}"
            if key not in table_groups:
                table_groups[key] = []
            table_groups[key].append(meta)
        
        for table_key, table_metadata in table_groups.items():
            db_name, table_name = table_key.split('.', 1)
            feed_name = table_metadata[0].feed
            
            # Create job name
            job_name = f"FEED_LOAD_{feed_name.upper()}"
            
            # Check job status first
            job_status = autosys_validator.check_job_status(job_name)
            
            if job_status.get('current_status') == 'SU':  # Success
                # Check if table exists and has data
                table_exists = database_connector.check_table_exists(db_name, table_name)
                
                result = {
                    'validation_type': 'data_load_verification',
                    'db_name': db_name,
                    'table_name': table_name,
                    'feed_name': feed_name,
                    'job_name': job_name,
                    'job_status': job_status.get('current_status'),
                    'table_exists': table_exists,
                    'validation_status': 'PASS' if table_exists else 'FAIL',
                    'error_message': None if table_exists else f"Table {table_name} does not exist after successful job"
                }
                
                if table_exists:
                    # Check row count
                    row_count = database_connector.get_row_count(db_name, table_name)
                    result['row_count'] = row_count
                    
                    if row_count == 0:
                        result['validation_status'] = 'FAIL'
                        result['error_message'] = f"Table {table_name} exists but is empty after successful job"
                
                # Add to test results
                test_result_collector(result)
                
                # Assert validation
                assert_validation_result(result, 'PASS')
    
    def test_table_schema_matches_metadata(self, metadata_reader, database_connector, 
                                         selected_feeds, test_result_collector):
        """Test that database table schema matches metadata definitions"""
        # Get feed metadata
        feed_metadata = metadata_reader.feed_metadata
        
        # Filter by selected feeds if specified
        if selected_feeds:
            feed_metadata = [meta for meta in feed_metadata if meta.feed in selected_feeds]
        
        # Group by table for validation
        table_groups = {}
        for meta in feed_metadata:
            key = f"{meta.db_name}.{meta.db_table}"
            if key not in table_groups:
                table_groups[key] = []
            table_groups[key].append(meta)
        
        for table_key, table_metadata in table_groups.items():
            db_name, table_name = table_key.split('.', 1)
            feed_name = table_metadata[0].feed
            
            # Check if table exists
            if database_connector.check_table_exists(db_name, table_name):
                # Get table schema
                try:
                    schema = database_connector.get_table_schema(db_name, table_name)
                    actual_columns = {col['name']: col for col in schema['columns']}
                    
                    # Validate each column from metadata
                    schema_matches = True
                    missing_columns = []
                    type_mismatches = []
                    
                    for meta in table_metadata:
                        column_name = meta.field_name
                        expected_type = meta.data_type
                        
                        if column_name not in actual_columns:
                            missing_columns.append(column_name)
                            schema_matches = False
                        else:
                            actual_type = str(actual_columns[column_name]['type'])
                            # Simplified type comparison
                            if not database_connector._compare_data_types(actual_type, expected_type):
                                type_mismatches.append(f"{column_name}: expected {expected_type}, got {actual_type}")
                                schema_matches = False
                    
                    error_message = None
                    if not schema_matches:
                        error_parts = []
                        if missing_columns:
                            error_parts.append(f"Missing columns: {', '.join(missing_columns)}")
                        if type_mismatches:
                            error_parts.append(f"Type mismatches: {'; '.join(type_mismatches)}")
                        error_message = "; ".join(error_parts)
                    
                    result = {
                        'validation_type': 'schema_validation',
                        'db_name': db_name,
                        'table_name': table_name,
                        'feed_name': feed_name,
                        'schema_matches': schema_matches,
                        'missing_columns': missing_columns,
                        'type_mismatches': type_mismatches,
                        'validation_status': 'PASS' if schema_matches else 'FAIL',
                        'error_message': error_message
                    }
                    
                except Exception as e:
                    result = {
                        'validation_type': 'schema_validation',
                        'db_name': db_name,
                        'table_name': table_name,
                        'feed_name': feed_name,
                        'validation_status': 'ERROR',
                        'error_message': f"Error getting table schema: {str(e)}"
                    }
            else:
                result = {
                    'validation_type': 'schema_validation',
                    'db_name': db_name,
                    'table_name': table_name,
                    'feed_name': feed_name,
                    'validation_status': 'FAIL',
                    'error_message': f"Table {table_name} does not exist"
                }
            
            # Add to test results
            test_result_collector(result)
            
            # Assert validation
            assert_validation_result(result, 'PASS')

@pytest.mark.parametrize("validation_config", [
    {"feed_name": "CUSTOMER_FEED", "expected_min_rows": 1000},
    {"feed_name": "TRANSACTION_FEED", "expected_min_rows": 5000},
    {"feed_name": "PRODUCT_FEED", "expected_min_rows": 100}
])
def test_feed_row_count_validation(validation_config, metadata_reader, database_connector, test_result_collector):
    """Parameterized test for feed row count validation"""
    feed_name = validation_config["feed_name"]
    expected_min_rows = validation_config["expected_min_rows"]
    
    # Get metadata for this feed
    feed_metadata = [meta for meta in metadata_reader.feed_metadata if meta.feed == feed_name]
    
    if not feed_metadata:
        pytest.skip(f"No metadata found for feed: {feed_name}")
    
    # Get table information
    meta = feed_metadata[0]  # Use first metadata record for table info
    db_name = meta.db_name
    table_name = meta.db_table
    
    # Check table exists and get row count
    if database_connector.check_table_exists(db_name, table_name):
        row_count = database_connector.get_row_count(db_name, table_name)
        
        result = {
            'validation_type': 'row_count_validation',
            'db_name': db_name,
            'table_name': table_name,
            'feed_name': feed_name,
            'actual_row_count': row_count,
            'expected_min_rows': expected_min_rows,
            'validation_status': 'PASS' if row_count >= expected_min_rows else 'FAIL',
            'error_message': None if row_count >= expected_min_rows else f"Row count {row_count} is below minimum {expected_min_rows}"
        }
    else:
        result = {
            'validation_type': 'row_count_validation',
            'db_name': db_name,
            'table_name': table_name,
            'feed_name': feed_name,
            'validation_status': 'FAIL',
            'error_message': f"Table {table_name} does not exist"
        }
    
    # Add to test results
    test_result_collector(result)
    
    # Assert validation
    assert_validation_result(result, 'PASS')
