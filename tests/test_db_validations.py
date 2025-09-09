"""
Database validation tests - Data quality, constraints, and business rules
"""
import pytest
from typing import List, Dict, Any
from validations.db_validations import db_validator
from tests.conftest import (
    assert_validation_result, 
    assert_no_null_values, 
    assert_unique_values,
    assert_within_range,
    assert_valid_enumeration
)

@pytest.mark.db_validation
class TestDataTypeValidations:
    """Test class for data type validations"""
    
    def test_data_type_compliance(self, metadata_reader, selected_feeds, test_result_collector):
        """Test data type compliance against metadata"""
        # Get feed metadata
        feed_metadata = metadata_reader.feed_metadata
        
        # Run data type validation
        results = db_validator.validate_data_types(feed_metadata, selected_feeds)
        
        for result in results:
            # Add to test results
            test_result_collector(result)
            
            # Assert validation
            assert_validation_result(result, 'PASS')
            
            # Additional assertions for column-level validations
            if result.get('column_validations'):
                for col_validation in result['column_validations']:
                    if not col_validation['validation_passed']:
                        pytest.fail(f"Data type validation failed for column {col_validation['column_name']}: {col_validation['error_message']}")

@pytest.mark.db_validation
class TestNullableConstraints:
    """Test class for nullable/mandatory constraint validations"""
    
    def test_nullable_constraints(self, metadata_reader, selected_feeds, test_result_collector):
        """Test nullable constraints against metadata"""
        # Get feed metadata
        feed_metadata = metadata_reader.feed_metadata
        
        # Run nullable constraint validation
        results = db_validator.validate_nullable_constraints(feed_metadata, selected_feeds)
        
        for result in results:
            # Add to test results
            test_result_collector(result)
            
            # Assert validation
            assert_validation_result(result, 'PASS')
            
            # Additional assertion for mandatory fields
            if result.get('expected_mandatory') == 'Y':
                assert_no_null_values(result, result.get('column_name'))
    
    def test_mandatory_fields_no_nulls(self, metadata_reader, selected_feeds, test_result_collector):
        """Test that mandatory fields have no null values"""
        # Get feed metadata for mandatory fields only
        feed_metadata = metadata_reader.feed_metadata
        mandatory_metadata = [meta for meta in feed_metadata if meta.mandatory.upper() == 'Y']
        
        # Filter by selected feeds if specified
        if selected_feeds:
            mandatory_metadata = [meta for meta in mandatory_metadata if meta.feed in selected_feeds]
        
        # Run nullable constraint validation for mandatory fields
        results = db_validator.validate_nullable_constraints(mandatory_metadata, selected_feeds)
        
        for result in results:
            # Add to test results
            test_result_collector(result)
            
            # Assert no null values for mandatory fields
            null_count = result.get('null_count', 0)
            if null_count > 0:
                result['validation_status'] = 'FAIL'
                result['error_message'] = f"Mandatory field {result.get('column_name')} has {null_count} null values"
            
            # Assert validation
            assert_validation_result(result, 'PASS')

@pytest.mark.db_validation
class TestUniqueConstraints:
    """Test class for unique constraint validations"""
    
    def test_unique_constraints(self, metadata_reader, selected_feeds, test_result_collector):
        """Test unique constraints against metadata"""
        # Get feed metadata
        feed_metadata = metadata_reader.feed_metadata
        
        # Run unique constraint validation
        results = db_validator.validate_unique_constraints(feed_metadata, selected_feeds)
        
        for result in results:
            # Add to test results
            test_result_collector(result)
            
            # Assert validation
            assert_validation_result(result, 'PASS')
            
            # Additional assertion for unique fields
            assert_unique_values(result, result.get('column_name'))
    
    def test_duplicate_detection(self, metadata_reader, selected_feeds, test_result_collector):
        """Test duplicate detection for unique fields"""
        # Get feed metadata for unique fields only
        feed_metadata = metadata_reader.feed_metadata
        unique_metadata = [meta for meta in feed_metadata if meta.unique.upper() == 'Y']
        
        # Filter by selected feeds if specified
        if selected_feeds:
            unique_metadata = [meta for meta in unique_metadata if meta.feed in selected_feeds]
        
        # Run unique constraint validation
        results = db_validator.validate_unique_constraints(unique_metadata, selected_feeds)
        
        for result in results:
            # Add to test results
            test_result_collector(result)
            
            # Check for duplicates
            duplicate_count = result.get('duplicate_count', 0)
            if duplicate_count > 0:
                result['validation_status'] = 'FAIL'
                result['error_message'] = f"Found {duplicate_count} duplicate values in unique field {result.get('column_name')}"
            
            # Assert validation
            assert_validation_result(result, 'PASS')

@pytest.mark.db_validation
class TestRangeConstraints:
    """Test class for range constraint validations"""
    
    def test_range_constraints(self, metadata_reader, selected_feeds, test_result_collector):
        """Test range constraints against metadata"""
        # Get feed metadata
        feed_metadata = metadata_reader.feed_metadata
        
        # Run range constraint validation
        results = db_validator.validate_range_constraints(feed_metadata, selected_feeds)
        
        for result in results:
            # Add to test results
            test_result_collector(result)
            
            # Assert validation
            assert_validation_result(result, 'PASS')
            
            # Additional assertion for range compliance
            assert_within_range(result, result.get('column_name'))
    
    def test_numeric_range_boundaries(self, metadata_reader, selected_feeds, test_result_collector):
        """Test numeric range boundaries"""
        # Get feed metadata with range constraints
        feed_metadata = metadata_reader.feed_metadata
        range_metadata = [meta for meta in feed_metadata if meta.range_bottom or meta.range_top]
        
        # Filter by selected feeds if specified
        if selected_feeds:
            range_metadata = [meta for meta in range_metadata if meta.feed in selected_feeds]
        
        # Run range constraint validation
        results = db_validator.validate_range_constraints(range_metadata, selected_feeds)
        
        for result in results:
            # Add to test results
            test_result_collector(result)
            
            # Check range violations
            below_range = result.get('below_range_count', 0)
            above_range = result.get('above_range_count', 0)
            
            if below_range > 0 or above_range > 0:
                result['validation_status'] = 'FAIL'
                result['error_message'] = f"Range violations: {below_range} below range, {above_range} above range"
            
            # Assert validation
            assert_validation_result(result, 'PASS')

@pytest.mark.db_validation
class TestEnumerationConstraints:
    """Test class for enumeration constraint validations"""
    
    def test_enumeration_constraints(self, metadata_reader, selected_feeds, test_result_collector):
        """Test enumeration constraints against metadata"""
        # Get feed metadata and enumeration metadata
        feed_metadata = metadata_reader.feed_metadata
        enumeration_metadata = metadata_reader.enumeration_metadata
        
        # Run enumeration constraint validation
        results = db_validator.validate_enumeration_constraints(
            feed_metadata, enumeration_metadata, selected_feeds
        )
        
        for result in results:
            # Add to test results
            test_result_collector(result)
            
            # Assert validation
            assert_validation_result(result, 'PASS')
            
            # Additional assertion for enumeration compliance
            assert_valid_enumeration(result, result.get('column_name'))
    
    def test_invalid_enumeration_values(self, metadata_reader, selected_feeds, test_result_collector):
        """Test detection of invalid enumeration values"""
        # Get feed metadata with enumeration constraints
        feed_metadata = metadata_reader.feed_metadata
        enum_metadata = [meta for meta in feed_metadata if meta.enumeration]
        enumeration_metadata = metadata_reader.enumeration_metadata
        
        # Filter by selected feeds if specified
        if selected_feeds:
            enum_metadata = [meta for meta in enum_metadata if meta.feed in selected_feeds]
        
        # Run enumeration constraint validation
        results = db_validator.validate_enumeration_constraints(
            enum_metadata, enumeration_metadata, selected_feeds
        )
        
        for result in results:
            # Add to test results
            test_result_collector(result)
            
            # Check for invalid values
            invalid_count = result.get('invalid_count', 0)
            if invalid_count > 0:
                result['validation_status'] = 'FAIL'
                result['error_message'] = f"Found {invalid_count} invalid enumeration values in {result.get('column_name')}"
            
            # Assert validation
            assert_validation_result(result, 'PASS')

@pytest.mark.db_validation
class TestInsertAppendLogic:
    """Test class for insert/append logic validations"""
    
    def test_insert_append_logic(self, metadata_reader, selected_feeds, test_result_collector):
        """Test insert vs append logic validation"""
        # Get feed metadata
        feed_metadata = metadata_reader.feed_metadata
        
        # Run insert/append logic validation
        results = db_validator.validate_insert_append_logic(feed_metadata, selected_feeds)
        
        for result in results:
            # Add to test results
            test_result_collector(result)
            
            # Assert validation
            assert_validation_result(result, 'PASS')
    
    def test_fresh_data_load_for_insert(self, metadata_reader, selected_feeds, test_result_collector):
        """Test fresh data load for INSERT request type"""
        # Get feed metadata for INSERT requests
        feed_metadata = metadata_reader.feed_metadata
        insert_metadata = [meta for meta in feed_metadata if meta.request.upper() == 'INSERT']
        
        # Filter by selected feeds if specified
        if selected_feeds:
            insert_metadata = [meta for meta in insert_metadata if meta.feed in selected_feeds]
        
        # Run insert/append logic validation
        results = db_validator.validate_insert_append_logic(insert_metadata, selected_feeds)
        
        for result in results:
            # Add to test results
            test_result_collector(result)
            
            # For INSERT type, verify fresh data load behavior
            if result.get('request_type', '').upper() == 'INSERT':
                # Additional checks could be added here for freshness
                # For now, just ensure the validation passes
                pass
            
            # Assert validation
            assert_validation_result(result, 'PASS')
    
    def test_data_append_for_append(self, metadata_reader, selected_feeds, test_result_collector):
        """Test data append for APPEND request type"""
        # Get feed metadata for APPEND requests
        feed_metadata = metadata_reader.feed_metadata
        append_metadata = [meta for meta in feed_metadata if meta.request.upper() == 'APPEND']
        
        # Filter by selected feeds if specified
        if selected_feeds:
            append_metadata = [meta for meta in append_metadata if meta.feed in selected_feeds]
        
        # Run insert/append logic validation
        results = db_validator.validate_insert_append_logic(append_metadata, selected_feeds)
        
        for result in results:
            # Add to test results
            test_result_collector(result)
            
            # For APPEND type, verify append behavior
            if result.get('request_type', '').upper() == 'APPEND':
                # Additional checks could be added here for append logic
                # For now, just ensure the validation passes
                pass
            
            # Assert validation
            assert_validation_result(result, 'PASS')

@pytest.mark.db_validation
class TestCountChecks:
    """Test class for count check validations"""
    
    def test_count_checks(self, metadata_reader, selected_feeds, test_result_collector):
        """Test row count checks"""
        # Get feed metadata
        feed_metadata = metadata_reader.feed_metadata
        
        # Run count checks validation
        results = db_validator.validate_count_checks(feed_metadata, None, selected_feeds)
        
        for result in results:
            # Add to test results
            test_result_collector(result)
            
            # Assert validation
            assert_validation_result(result, 'PASS')
    
    def test_non_empty_tables(self, metadata_reader, selected_feeds, test_result_collector):
        """Test that tables are not empty"""
        # Get feed metadata
        feed_metadata = metadata_reader.feed_metadata
        
        # Filter by selected feeds if specified
        if selected_feeds:
            feed_metadata = [meta for meta in feed_metadata if meta.feed in selected_feeds]
        
        # Run count checks validation
        results = db_validator.validate_count_checks(feed_metadata, None, selected_feeds)
        
        for result in results:
            # Add to test results
            test_result_collector(result)
            
            # Check that table is not empty
            actual_count = result.get('actual_count', 0)
            if actual_count == 0:
                result['validation_status'] = 'FAIL'
                result['error_message'] = f"Table {result.get('table_name')} is empty"
            
            # Assert validation
            assert_validation_result(result, 'PASS')

@pytest.mark.db_validation
class TestCompletenessChecks:
    """Test class for data completeness validations"""
    
    def test_completeness_checks(self, metadata_reader, selected_feeds, test_result_collector):
        """Test data completeness checks"""
        # Get feed metadata
        feed_metadata = metadata_reader.feed_metadata
        
        # Run completeness checks validation
        results = db_validator.validate_completeness_checks(feed_metadata, selected_feeds)
        
        for result in results:
            # Add to test results
            test_result_collector(result)
            
            # Assert validation
            assert_validation_result(result, 'PASS')
    
    def test_minimum_completeness_threshold(self, metadata_reader, selected_feeds, test_result_collector):
        """Test minimum completeness threshold (95%)"""
        # Get feed metadata
        feed_metadata = metadata_reader.feed_metadata
        
        # Filter by selected feeds if specified
        if selected_feeds:
            feed_metadata = [meta for meta in feed_metadata if meta.feed in selected_feeds]
        
        # Run completeness checks validation
        results = db_validator.validate_completeness_checks(feed_metadata, selected_feeds)
        
        for result in results:
            # Add to test results
            test_result_collector(result)
            
            # Check completeness threshold
            completeness_score = result.get('overall_completeness_score', 0)
            if completeness_score < 95.0:
                result['validation_status'] = 'FAIL'
                result['error_message'] = f"Completeness score {completeness_score}% is below threshold 95%"
            
            # Assert validation
            assert_validation_result(result, 'PASS')
    
    def test_mandatory_field_completeness(self, metadata_reader, selected_feeds, test_result_collector):
        """Test completeness for mandatory fields (should be 100%)"""
        # Get feed metadata
        feed_metadata = metadata_reader.feed_metadata
        
        # Filter by selected feeds if specified
        if selected_feeds:
            feed_metadata = [meta for meta in feed_metadata if meta.feed in selected_feeds]
        
        # Run completeness checks validation
        results = db_validator.validate_completeness_checks(feed_metadata, selected_feeds)
        
        for result in results:
            # Add to test results
            test_result_collector(result)
            
            # Check mandatory field completeness
            column_completeness = result.get('column_completeness', [])
            for col_result in column_completeness:
                if col_result.get('is_mandatory') and col_result.get('completeness_percentage', 0) < 100:
                    result['validation_status'] = 'FAIL'
                    result['error_message'] = f"Mandatory field {col_result['column_name']} has {col_result['completeness_percentage']}% completeness"
                    break
            
            # Assert validation
            assert_validation_result(result, 'PASS')

# Parameterized tests for specific validation scenarios
@pytest.mark.parametrize("validation_scenario", [
    {
        "name": "customer_id_uniqueness",
        "feed": "CUSTOMER_FEED",
        "column": "CUSTOMER_ID",
        "validation_type": "unique"
    },
    {
        "name": "transaction_amount_range",
        "feed": "TRANSACTION_FEED", 
        "column": "AMOUNT",
        "validation_type": "range",
        "min_value": 0,
        "max_value": 1000000
    },
    {
        "name": "status_enumeration",
        "feed": "ORDER_FEED",
        "column": "STATUS",
        "validation_type": "enumeration",
        "allowed_values": ["PENDING", "APPROVED", "REJECTED", "CANCELLED"]
    }
])
def test_specific_validation_scenarios(validation_scenario, metadata_reader, database_connector, test_result_collector):
    """Parameterized test for specific validation scenarios"""
    scenario_name = validation_scenario["name"]
    feed_name = validation_scenario["feed"]
    column_name = validation_scenario["column"]
    validation_type = validation_scenario["validation_type"]
    
    # Get metadata for this feed and column
    feed_metadata = [
        meta for meta in metadata_reader.feed_metadata 
        if meta.feed == feed_name and meta.field_name == column_name
    ]
    
    if not feed_metadata:
        pytest.skip(f"No metadata found for {feed_name}.{column_name}")
    
    meta = feed_metadata[0]
    
    # Perform validation based on type
    if validation_type == "unique":
        total_count, distinct_count = database_connector.check_unique_constraints(
            meta.db_name, meta.db_table, meta.field_name
        )
        
        result = {
            'validation_type': f'specific_{validation_type}',
            'scenario_name': scenario_name,
            'feed_name': feed_name,
            'column_name': column_name,
            'total_count': total_count,
            'distinct_count': distinct_count,
            'duplicate_count': total_count - distinct_count,
            'validation_status': 'PASS' if total_count == distinct_count else 'FAIL',
            'error_message': None if total_count == distinct_count else f"Found {total_count - distinct_count} duplicates"
        }
    
    elif validation_type == "range":
        min_val = validation_scenario.get("min_value")
        max_val = validation_scenario.get("max_value")
        
        range_results = database_connector.check_range_constraints(
            meta.db_name, meta.db_table, meta.field_name, str(min_val), str(max_val)
        )
        
        violations = range_results.get('below_range', 0) + range_results.get('above_range', 0)
        
        result = {
            'validation_type': f'specific_{validation_type}',
            'scenario_name': scenario_name,
            'feed_name': feed_name,
            'column_name': column_name,
            'range_violations': violations,
            'below_range': range_results.get('below_range', 0),
            'above_range': range_results.get('above_range', 0),
            'validation_status': 'PASS' if violations == 0 else 'FAIL',
            'error_message': None if violations == 0 else f"Found {violations} range violations"
        }
    
    elif validation_type == "enumeration":
        allowed_values = validation_scenario.get("allowed_values", [])
        
        invalid_count = database_connector.check_enumeration_constraints(
            meta.db_name, meta.db_table, meta.field_name, allowed_values
        )
        
        result = {
            'validation_type': f'specific_{validation_type}',
            'scenario_name': scenario_name,
            'feed_name': feed_name,
            'column_name': column_name,
            'invalid_count': invalid_count,
            'allowed_values': allowed_values,
            'validation_status': 'PASS' if invalid_count == 0 else 'FAIL',
            'error_message': None if invalid_count == 0 else f"Found {invalid_count} invalid enumeration values"
        }
    
    else:
        result = {
            'validation_type': f'specific_{validation_type}',
            'scenario_name': scenario_name,
            'validation_status': 'ERROR',
            'error_message': f"Unknown validation type: {validation_type}"
        }
    
    # Add to test results
    test_result_collector(result)
    
    # Assert validation
    assert_validation_result(result, 'PASS')
