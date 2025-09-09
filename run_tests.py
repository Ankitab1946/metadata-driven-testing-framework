"""
Main test runner for the metadata-driven testing framework
"""
import sys
import argparse
from pathlib import Path
import subprocess
from utils.logger import framework_logger
from config.config import config

def main():
    """Main function to run the testing framework"""
    parser = argparse.ArgumentParser(description='Metadata-Driven Testing Framework')
    
    # Test selection arguments
    parser.add_argument('--feeds', type=str, help='Comma-separated list of feeds to validate')
    parser.add_argument('--databases', type=str, help='Comma-separated list of databases to validate')
    parser.add_argument('--validation-types', type=str, help='Comma-separated list of validation types to run')
    
    # Test execution arguments
    parser.add_argument('--parallel', action='store_true', help='Run tests in parallel')
    parser.add_argument('--workers', type=int, default=4, help='Number of parallel workers')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--generate-screenshots', action='store_true', help='Generate screenshots for failed tests')
    
    # Report arguments
    parser.add_argument('--report-format', choices=['html', 'excel', 'json', 'all'], default='all', 
                       help='Report format to generate')
    parser.add_argument('--report-name', type=str, default='validation_report', 
                       help='Name for the generated report')
    
    # Configuration arguments
    parser.add_argument('--metadata-file', type=str, help='Path to metadata Excel file')
    parser.add_argument('--config-file', type=str, help='Path to configuration file')
    
    # Test type selection
    parser.add_argument('--test-type', choices=['all', 'feed', 'db', 'autosys', 'reconciliation'], 
                       default='all', help='Type of tests to run')
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = "DEBUG" if args.verbose else "INFO"
    framework_logger.info("Starting Metadata-Driven Testing Framework")
    
    # Update configuration if provided
    if args.metadata_file:
        config.paths.metadata_file = args.metadata_file
    
    # Validate metadata file exists
    metadata_path = Path(config.paths.metadata_file)
    if not metadata_path.exists():
        framework_logger.error(f"Metadata file not found: {metadata_path}")
        sys.exit(1)
    
    # Build pytest command
    pytest_cmd = ['python', '-m', 'pytest']
    
    # Add test directories based on test type
    if args.test_type == 'all':
        pytest_cmd.append('tests/')
    elif args.test_type == 'feed':
        pytest_cmd.append('tests/test_feed_validations.py')
    elif args.test_type == 'db':
        pytest_cmd.append('tests/test_db_validations.py')
    elif args.test_type == 'autosys':
        pytest_cmd.extend(['-m', 'autosys_validation'])
    elif args.test_type == 'reconciliation':
        pytest_cmd.extend(['-m', 'reconciliation'])
    
    # Add pytest arguments
    if args.verbose:
        pytest_cmd.append('-v')
    
    if args.parallel:
        pytest_cmd.extend(['-n', str(args.workers)])
    
    # Add custom arguments
    if args.feeds:
        pytest_cmd.extend(['--feeds', args.feeds])
    
    if args.databases:
        pytest_cmd.extend(['--databases', args.databases])
    
    if args.validation_types:
        pytest_cmd.extend(['--validation-types', args.validation_types])
    
    if args.generate_screenshots:
        pytest_cmd.append('--generate-screenshots')
    
    # Add HTML report generation
    report_path = Path(config.paths.report_output_path)
    report_path.mkdir(parents=True, exist_ok=True)
    
    html_report_file = report_path / f"{args.report_name}.html"
    pytest_cmd.extend(['--html', str(html_report_file), '--self-contained-html'])
    
    # Add metadata to report
    pytest_cmd.extend(['--metadata', f'Feeds={args.feeds or "All"}'])
    pytest_cmd.extend(['--metadata', f'Databases={args.databases or "All"}'])
    pytest_cmd.extend(['--metadata', f'ValidationTypes={args.validation_types or "All"}'])
    
    framework_logger.info(f"Running command: {' '.join(pytest_cmd)}")
    
    try:
        # Run pytest
        result = subprocess.run(pytest_cmd, capture_output=False, text=True)
        
        if result.returncode == 0:
            framework_logger.info("All tests passed successfully!")
        else:
            framework_logger.warning(f"Some tests failed. Exit code: {result.returncode}")
        
        framework_logger.info(f"HTML report generated: {html_report_file}")
        
        return result.returncode
        
    except Exception as e:
        framework_logger.error(f"Error running tests: {str(e)}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
