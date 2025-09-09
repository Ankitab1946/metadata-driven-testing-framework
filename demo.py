"""
Demo script to showcase the metadata-driven testing framework
"""
import sys
from pathlib import Path
from utils.logger import framework_logger
from utils.excel_reader import MetadataReader
from validations.file_checks import file_validator
from validations.autosys_checks import autosys_validator
from validations.db_validations import db_validator
from validations.report_generator import report_generator
from config.config import config

def run_demo():
    """Run a demonstration of the testing framework"""
    
    framework_logger.info("=" * 60)
    framework_logger.info("METADATA-DRIVEN TESTING FRAMEWORK DEMO")
    framework_logger.info("=" * 60)
    
    # Step 1: Load Metadata
    framework_logger.info("\n1. Loading Metadata from Excel file...")
    try:
        metadata_reader = MetadataReader(config.paths.metadata_file)
        metadata = metadata_reader.load_all_metadata()
        
        framework_logger.info(f"✓ Loaded {len(metadata['feed_metadata'])} feed metadata records")
        framework_logger.info(f"✓ Loaded {len(metadata['staging_metadata'])} staging metadata records")
        framework_logger.info(f"✓ Loaded {len(metadata['enumeration_metadata'])} enumeration records")
        
        # Display sample metadata
        if metadata['feed_metadata']:
            sample_feed = metadata['feed_metadata'][0]
            framework_logger.info(f"  Sample Feed: {sample_feed.feed} -> {sample_feed.db_name}.{sample_feed.db_table}")
        
    except Exception as e:
        framework_logger.error(f"✗ Failed to load metadata: {str(e)}")
        return False
    
    # Step 2: File Validation Demo
    framework_logger.info("\n2. File Validation Demo...")
    try:
        # Get unique feeds for testing
        unique_feeds = metadata_reader.get_unique_feeds()[:2]  # Test first 2 feeds
        
        for feed_name in unique_feeds:
            framework_logger.info(f"  Testing feed: {feed_name}")
            
            # File availability check
            result = file_validator.check_feed_file_availability(feed_name)
            status = "✓" if result['validation_status'] == 'PASS' else "✗"
            framework_logger.info(f"    {status} File availability: {result['validation_status']}")
            
            if result['validation_status'] != 'PASS':
                framework_logger.info(f"      Note: {result.get('error_message', 'File not found (expected in demo)')}")
        
    except Exception as e:
        framework_logger.error(f"✗ File validation demo failed: {str(e)}")
    
    # Step 3: Autosys Job Validation Demo
    framework_logger.info("\n3. Autosys Job Validation Demo...")
    try:
        # Test some sample job names
        sample_jobs = ["FEED_LOAD_CUSTOMER_FEED", "FEED_LOAD_TRANSACTION_FEED", "DATA_VALIDATION_JOB"]
        
        for job_name in sample_jobs:
            framework_logger.info(f"  Testing job: {job_name}")
            
            # Job availability check
            result = autosys_validator.check_job_availability(job_name)
            status = "✓" if result['validation_status'] == 'PASS' else "✗"
            framework_logger.info(f"    {status} Job availability: {result['validation_status']}")
            
            # Job status check
            result = autosys_validator.check_job_status(job_name, 'SU')
            status = "✓" if result['validation_status'] == 'PASS' else "✗"
            framework_logger.info(f"    {status} Job status: {result['validation_status']}")
        
    except Exception as e:
        framework_logger.error(f"✗ Autosys validation demo failed: {str(e)}")
    
    # Step 4: Database Validation Demo (Mock)
    framework_logger.info("\n4. Database Validation Demo...")
    try:
        framework_logger.info("  Note: Using mock database validation (no real DB connection)")
        
        # Create sample validation results
        sample_results = []
        
        for feed_meta in metadata['feed_metadata'][:3]:  # Test first 3 records
            framework_logger.info(f"  Testing: {feed_meta.feed}.{feed_meta.field_name}")
            
            # Mock data type validation
            result = {
                'validation_type': 'data_type',
                'feed_name': feed_meta.feed,
                'column_name': feed_meta.field_name,
                'expected_type': feed_meta.data_type,
                'validation_status': 'PASS',
                'error_message': None
            }
            sample_results.append(result)
            framework_logger.info(f"    ✓ Data type validation: PASS")
            
            # Mock nullable validation if mandatory
            if feed_meta.mandatory.upper() == 'Y':
                result = {
                    'validation_type': 'nullable_constraint',
                    'feed_name': feed_meta.feed,
                    'column_name': feed_meta.field_name,
                    'null_count': 0,
                    'validation_status': 'PASS',
                    'error_message': None
                }
                sample_results.append(result)
                framework_logger.info(f"    ✓ Mandatory field validation: PASS")
            
            # Mock unique validation if required
            if feed_meta.unique.upper() == 'Y':
                result = {
                    'validation_type': 'unique_constraint',
                    'feed_name': feed_meta.feed,
                    'column_name': feed_meta.field_name,
                    'duplicate_count': 0,
                    'validation_status': 'PASS',
                    'error_message': None
                }
                sample_results.append(result)
                framework_logger.info(f"    ✓ Unique constraint validation: PASS")
        
    except Exception as e:
        framework_logger.error(f"✗ Database validation demo failed: {str(e)}")
        sample_results = []
    
    # Step 5: Report Generation Demo
    framework_logger.info("\n5. Report Generation Demo...")
    try:
        # Create some sample test results for report
        demo_results = [
            {
                'validation_type': 'file_availability',
                'feed_name': 'CUSTOMER_FEED',
                'validation_status': 'PASS',
                'error_message': None
            },
            {
                'validation_type': 'autosys_job',
                'feed_name': 'CUSTOMER_FEED',
                'job_name': 'FEED_LOAD_CUSTOMER_FEED',
                'validation_status': 'PASS',
                'error_message': None
            },
            {
                'validation_type': 'data_type',
                'feed_name': 'CUSTOMER_FEED',
                'column_name': 'CUSTOMER_ID',
                'validation_status': 'PASS',
                'error_message': None
            },
            {
                'validation_type': 'nullable_constraint',
                'feed_name': 'TRANSACTION_FEED',
                'column_name': 'AMOUNT',
                'validation_status': 'FAIL',
                'error_message': 'Found 5 null values in mandatory field'
            }
        ]
        
        # Add sample results from database validation
        demo_results.extend(sample_results)
        
        # Generate reports
        report_paths = report_generator.generate_comprehensive_report(
            demo_results, 
            "demo_validation_report"
        )
        
        framework_logger.info("  ✓ Reports generated successfully:")
        for format_type, path in report_paths.items():
            framework_logger.info(f"    - {format_type.upper()}: {path}")
        
    except Exception as e:
        framework_logger.error(f"✗ Report generation demo failed: {str(e)}")
    
    # Step 6: Configuration Demo
    framework_logger.info("\n6. Configuration Demo...")
    try:
        framework_logger.info("  Current Configuration:")
        framework_logger.info(f"    - Metadata file: {config.paths.metadata_file}")
        framework_logger.info(f"    - Feed base path: {config.paths.feed_base_path}")
        framework_logger.info(f"    - Log base path: {config.paths.log_base_path}")
        framework_logger.info(f"    - Report output path: {config.paths.report_output_path}")
        framework_logger.info(f"    - Autosys mock mode: {config.autosys.mock_mode}")
        framework_logger.info(f"    - Autosys environment: {config.autosys.environment}")
        
    except Exception as e:
        framework_logger.error(f"✗ Configuration demo failed: {str(e)}")
    
    # Summary
    framework_logger.info("\n" + "=" * 60)
    framework_logger.info("DEMO COMPLETED SUCCESSFULLY!")
    framework_logger.info("=" * 60)
    framework_logger.info("\nNext Steps:")
    framework_logger.info("1. Update database connection strings in config/config.py")
    framework_logger.info("2. Customize metadata file for your specific feeds and tables")
    framework_logger.info("3. Run actual tests using: python run_tests.py")
    framework_logger.info("4. Check generated reports in the reports/ directory")
    framework_logger.info("\nFor more information, see README.md")
    
    return True

def main():
    """Main function"""
    try:
        success = run_demo()
        return 0 if success else 1
    except KeyboardInterrupt:
        framework_logger.info("\nDemo interrupted by user")
        return 1
    except Exception as e:
        framework_logger.error(f"Demo failed with error: {str(e)}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
