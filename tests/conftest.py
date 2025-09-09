"""
Pytest configuration and fixtures for the testing framework
"""
import pytest
from pathlib import Path
from typing import Dict, List, Any, Optional
from utils.excel_reader import MetadataReader
from utils.db_connector import db_connector
from utils.logger import framework_logger
from config.config import config
from validations.report_generator import report_generator

# Global test results storage
test_results = []

@pytest.fixture(scope="session")
def metadata_reader():
    """Fixture to provide metadata reader instance"""
    try:
        metadata_file_path = config.paths.metadata_file
        reader = MetadataReader(metadata_file_path)
        metadata = reader.load_all_metadata()
        framework_logger.info("Metadata loaded successfully for test session")
        return reader
    except Exception as e:
        framework_logger.error(f"Failed to load metadata: {str(e)}")
        pytest.fail(f"Failed to load metadata: {str(e)}")

@pytest.fixture(scope="session")
def database_connector():
    """Fixture to provide database connector instance"""
    try:
        # Test database connections
        if not db_connector.test_connection("server1"):
            framework_logger.warning("Server1 connection test failed")
        if not db_connector.test_connection("server2"):
            framework_logger.warning("Server2 connection test failed")
        
        framework_logger.info("Database connector initialized for test session")
        return db_connector
    except Exception as e:
        framework_logger.error(f"Failed to initialize database connector: {str(e)}")
        pytest.fail(f"Failed to initialize database connector: {str(e)}")

@pytest.fixture(scope="session")
def selected_feeds(request):
    """Fixture to get selected feeds from command line or config"""
    # Get feeds from command line option
    feeds = request.config.getoption("--feeds", default=None)
    if feeds:
        selected_feeds_list = [feed.strip() for feed in feeds.split(",")]
        framework_logger.info(f"Selected feeds from command line: {selected_feeds_list}")
        return selected_feeds_list
    
    # If no feeds specified, return None (will validate all feeds)
    framework_logger.info("No specific feeds selected, will validate all feeds")
    return None

@pytest.fixture(scope="session")
def selected_databases(request):
    """Fixture to get selected databases from command line or config"""
    # Get databases from command line option
    databases = request.config.getoption("--databases", default=None)
    if databases:
        selected_db_list = [db.strip() for db in databases.split(",")]
        framework_logger.info(f"Selected databases from command line: {selected_db_list}")
        return selected_db_list
    
    # If no databases specified, return None (will validate all databases)
    framework_logger.info("No specific databases selected, will validate all databases")
    return None

@pytest.fixture(scope="function")
def test_result_collector():
    """Fixture to collect test results for reporting"""
    results = []
    
    def add_result(result: Dict[str, Any]):
        results.append(result)
        test_results.append(result)  # Add to global results
    
    return add_result

@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Setup test environment before running tests"""
    framework_logger.info("Setting up test environment")
    
    # Create necessary directories
    Path(config.paths.report_output_path).mkdir(parents=True, exist_ok=True)
    Path(config.paths.screenshot_path).mkdir(parents=True, exist_ok=True)
    
    # Clear previous test results
    global test_results
    test_results.clear()
    
    framework_logger.info("Test environment setup completed")
    yield
    
    # Cleanup after all tests
    framework_logger.info("Cleaning up test environment")

@pytest.fixture(scope="session", autouse=True)
def generate_final_report(request):
    """Generate final report after all tests complete"""
    yield
    
    # Generate comprehensive report after all tests
    if test_results:
        try:
            framework_logger.info("Generating final comprehensive report")
            report_paths = report_generator.generate_comprehensive_report(
                test_results, 
                "final_validation_report"
            )
            
            framework_logger.info("Final report generated successfully:")
            for format_type, path in report_paths.items():
                framework_logger.info(f"  {format_type.upper()}: {path}")
                
        except Exception as e:
            framework_logger.error(f"Failed to generate final report: {str(e)}")

def pytest_addoption(parser):
    """Add custom command line options"""
    parser.addoption(
        "--feeds",
        action="store",
        default=None,
        help="Comma-separated list of feeds to validate (e.g., --feeds=feed1,feed2)"
    )
    
    parser.addoption(
        "--databases",
        action="store",
        default=None,
        help="Comma-separated list of databases to validate (e.g., --databases=server1,server2)"
    )
    
    parser.addoption(
        "--validation-types",
        action="store",
        default=None,
        help="Comma-separated list of validation types to run (e.g., --validation-types=data_type,nullable)"
    )
    
    parser.addoption(
        "--generate-screenshots",
        action="store_true",
        default=False,
        help="Generate screenshots for failed tests"
    )

def pytest_configure(config):
    """Configure pytest with custom markers"""
    config.addinivalue_line(
        "markers", "file_validation: mark test as file validation"
    )
    config.addinivalue_line(
        "markers", "autosys_validation: mark test as Autosys validation"
    )
    config.addinivalue_line(
        "markers", "db_validation: mark test as database validation"
    )
    config.addinivalue_line(
        "markers", "reconciliation: mark test as reconciliation validation"
    )
    config.addinivalue_line(
        "markers", "pattern_validation: mark test as pattern validation"
    )

def pytest_collection_modifyitems(config, items):
    """Modify test collection based on command line options"""
    validation_types = config.getoption("--validation-types")
    
    if validation_types:
        selected_types = [vtype.strip() for vtype in validation_types.split(",")]
        framework_logger.info(f"Filtering tests for validation types: {selected_types}")
        
        # Filter items based on validation types
        filtered_items = []
        for item in items:
            # Check if test matches selected validation types
            for vtype in selected_types:
                if vtype in item.name or vtype in str(item.fspath):
                    filtered_items.append(item)
                    break
        
        items[:] = filtered_items
        framework_logger.info(f"Filtered to {len(items)} tests")

@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Hook to capture test results"""
    outcome = yield
    report = outcome.get_result()
    
    if report.when == "call":
        # Extract test information
        test_name = item.name
        test_file = str(item.fspath)
        
        # Determine validation type from test name or markers
        validation_type = "unknown"
        if hasattr(item, 'pytestmark'):
            for marker in item.pytestmark:
                if marker.name in ['file_validation', 'autosys_validation', 'db_validation', 
                                 'reconciliation', 'pattern_validation']:
                    validation_type = marker.name
                    break
        
        # Create test result record
        test_result = {
            'test_name': test_name,
            'test_file': test_file,
            'validation_type': validation_type,
            'validation_status': 'PASS' if report.passed else 'FAIL' if report.failed else 'ERROR',
            'duration': report.duration,
            'error_message': str(report.longrepr) if report.longrepr else None,
            'timestamp': call.start
        }
        
        # Add to global results
        test_results.append(test_result)

# Helper functions for tests
def create_validation_result(validation_type: str, status: str, **kwargs) -> Dict[str, Any]:
    """Helper function to create standardized validation result"""
    result = {
        'validation_type': validation_type,
        'validation_status': status,
        'timestamp': pytest.current_timestamp if hasattr(pytest, 'current_timestamp') else None,
        **kwargs
    }
    return result

def assert_validation_result(result: Dict[str, Any], expected_status: str = 'PASS'):
    """Helper function to assert validation results"""
    actual_status = result.get('validation_status')
    error_message = result.get('error_message', '')
    
    if actual_status != expected_status:
        failure_msg = f"Validation failed: expected {expected_status}, got {actual_status}"
        if error_message:
            failure_msg += f"\nError: {error_message}"
        pytest.fail(failure_msg)

# Custom assertion helpers
def assert_no_null_values(result: Dict[str, Any], column_name: str):
    """Assert that a column has no null values"""
    null_count = result.get('null_count', 0)
    if null_count > 0:
        pytest.fail(f"Column {column_name} has {null_count} null values but should be mandatory")

def assert_unique_values(result: Dict[str, Any], column_name: str):
    """Assert that a column has unique values"""
    total_count = result.get('total_count', 0)
    distinct_count = result.get('distinct_count', 0)
    
    if total_count != distinct_count:
        duplicate_count = total_count - distinct_count
        pytest.fail(f"Column {column_name} should be unique but has {duplicate_count} duplicates")

def assert_within_range(result: Dict[str, Any], column_name: str):
    """Assert that column values are within specified range"""
    below_range = result.get('below_range_count', 0)
    above_range = result.get('above_range_count', 0)
    
    if below_range > 0 or above_range > 0:
        pytest.fail(f"Column {column_name} has {below_range} values below range and {above_range} values above range")

def assert_valid_enumeration(result: Dict[str, Any], column_name: str):
    """Assert that column values are within allowed enumeration"""
    invalid_count = result.get('invalid_count', 0)
    
    if invalid_count > 0:
        pytest.fail(f"Column {column_name} has {invalid_count} values not in allowed enumeration")
