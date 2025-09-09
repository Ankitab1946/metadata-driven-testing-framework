# Metadata-Driven Python Testing Framework

A comprehensive testing framework that uses pytest, Great Expectations, pandas, and SQLAlchemy to perform data validation based on metadata definitions from Excel files.

## Features

- **Metadata-Driven**: All validations are driven by Excel metadata files
- **Comprehensive Validations**: File availability, Autosys jobs, database validations, data quality checks
- **Multiple Report Formats**: HTML, Excel, and JSON reports with screenshots
- **Flexible Configuration**: Support for multiple databases and environments
- **Parallel Execution**: Run tests in parallel for better performance
- **Extensible**: Easy to add new validation types and rules

## Project Structure

```
/
├── config/
│   └── config.py              # Configuration management
├── metadata/
│   ├── Metadata.xlsx          # Sample metadata file
│   └── create_sample_metadata.py
├── tests/
│   ├── conftest.py            # Pytest fixtures and configuration
│   ├── test_feed_validations.py
│   └── test_db_validations.py
├── utils/
│   ├── excel_reader.py        # Excel metadata parser
│   ├── db_connector.py        # Database connectivity
│   └── logger.py              # Logging configuration
├── validations/
│   ├── file_checks.py         # File validation utilities
│   ├── autosys_checks.py      # Autosys job validations
│   ├── db_validations.py      # Database validation utilities
│   └── report_generator.py    # Report generation
├── requirements.txt
├── run_tests.py              # Main test runner
└── README.md
```

## Installation

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Database Connections**:
   Update `config/config.py` or set environment variables:
   ```bash
   export DB_SERVER1_CONNECTION="DRIVER={ODBC Driver 17 for SQL Server};SERVER=server1;DATABASE=database1;Trusted_Connection=yes;"
   export DB_SERVER2_CONNECTION="DRIVER={ODBC Driver 17 for SQL Server};SERVER=server2;DATABASE=database2;Trusted_Connection=yes;"
   ```

3. **Prepare Metadata File**:
   - Use the sample `metadata/Metadata.xlsx` or create your own
   - Ensure all required sheets are present: Feed_to_staging, Staging_to_GRI, Enumeration, etc.

## Metadata File Structure

The framework expects an Excel file with the following sheets:

### 1. Feed_to_staging Sheet
| Column | Description |
|--------|-------------|
| Modules | Module name |
| Feed | Feed name |
| FieldName | Column name |
| DBName | Database name (server1/server2) |
| DB Table | Table name |
| DataType | Expected data type |
| Nullable | Y/N - if column can be null |
| Request | Insert/Append |
| Default | Default value |
| Enumeration | Enumeration name |
| RangeBottom | Minimum value |
| RangeTop | Maximum value |
| Mandatory | Y/N - if column is mandatory |
| Unique | Y/N - if column should be unique |

### 2. Staging_to_GRI Sheet
Similar structure for staging to target transformations.

### 3. Enumeration Sheet
| Column | Description |
|--------|-------------|
| EnumerationName | Name of enumeration |
| EnumValues | Allowed values |

### 4. Additional Sheets
- **Patterns**: Pattern validation rules
- **Reconciliations**: Reconciliation rules (Inter/Intra/Mix)
- **Business Rules**: Custom business validation rules

## Usage

### Basic Usage

```bash
# Run all tests
python run_tests.py

# Run tests for specific feeds
python run_tests.py --feeds "CUSTOMER_FEED,TRANSACTION_FEED"

# Run tests for specific databases
python run_tests.py --databases "server1,server2"

# Run specific validation types
python run_tests.py --validation-types "data_type,nullable,unique"
```

### Advanced Usage

```bash
# Run tests in parallel
python run_tests.py --parallel --workers 4

# Generate screenshots for failures
python run_tests.py --generate-screenshots

# Run only database validations
python run_tests.py --test-type db

# Verbose output
python run_tests.py --verbose

# Custom report name
python run_tests.py --report-name "daily_validation_report"
```

### Using Pytest Directly

```bash
# Run all tests
pytest tests/

# Run with custom options
pytest tests/ --feeds="CUSTOMER_FEED" --html=report.html --self-contained-html

# Run specific test types
pytest tests/test_db_validations.py -v

# Run with markers
pytest -m "db_validation" -v
```

## Validation Types

### 1. File Validations
- **Feed File Availability**: Check if feed files exist in specified paths
- **File Freshness**: Verify files are within acceptable age limits
- **File Size Validation**: Ensure files meet size requirements
- **Log File Generation**: Verify error logs are created for failed jobs

### 2. Autosys Job Validations
- **Job Availability**: Check if jobs exist in Autosys environment
- **Job Status**: Verify job execution status (Success/Failure)
- **Job Completion**: Monitor job completion within timeout
- **Job Dependencies**: Validate job dependency chains

### 3. Database Validations
- **Data Type Compliance**: Verify column data types match metadata
- **Nullable Constraints**: Check mandatory fields have no nulls
- **Unique Constraints**: Validate uniqueness requirements
- **Range Constraints**: Ensure values fall within specified ranges
- **Enumeration Constraints**: Verify values match allowed enumerations
- **Insert/Append Logic**: Validate data load behavior
- **Count Checks**: Verify row counts meet expectations
- **Completeness Checks**: Ensure data completeness thresholds

### 4. Data Quality Checks
- **Duplicate Detection**: Identify duplicate records
- **Referential Integrity**: Check foreign key relationships
- **Business Rules**: Custom business logic validations
- **Pattern Matching**: Validate data patterns
- **Reconciliation**: Inter/Intra/Mix reconciliation rules

## Configuration

### Environment Variables

```bash
# Database connections
DB_SERVER1_CONNECTION="connection_string_1"
DB_SERVER2_CONNECTION="connection_string_2"

# Autosys configuration
AUTOSYS_ENV="DEV"
AUTOSYS_MOCK_MODE="true"

# File paths
METADATA_FILE="metadata/Metadata.xlsx"
FEED_BASE_PATH="/data/feeds"
LOG_BASE_PATH="/data/logs"
```

### Configuration File

Update `config/config.py` for more advanced configuration:

```python
# Database settings
config.database.server1_connection = "your_connection_string"
config.database.connection_timeout = 30

# Autosys settings
config.autosys.mock_mode = False
config.autosys.environment = "PROD"

# Test settings
config.test.parallel_execution = True
config.test.max_workers = 8
```

## Reports

The framework generates comprehensive reports in multiple formats:

### HTML Report
- Interactive dashboard with pass/fail statistics
- Detailed test results with error messages
- Charts and graphs for visual analysis
- Screenshots for failed tests (if enabled)

### Excel Report
- Summary sheet with overall statistics
- Detailed sheets for each validation type
- Color-coded results for easy identification
- Pivot tables for analysis

### JSON Report
- Machine-readable format for integration
- Complete test results and metadata
- Suitable for CI/CD pipelines

## Extending the Framework

### Adding New Validation Types

1. **Create Validation Function**:
   ```python
   def validate_custom_rule(metadata, selected_feeds):
       results = []
       # Your validation logic here
       return results
   ```

2. **Add Test Case**:
   ```python
   @pytest.mark.custom_validation
   def test_custom_validation(metadata_reader, test_result_collector):
       results = validate_custom_rule(metadata_reader.feed_metadata)
       for result in results:
           test_result_collector(result)
           assert_validation_result(result, 'PASS')
   ```

3. **Update Configuration**:
   Add the new validation type to pytest markers and command-line options.

### Adding New Data Sources

1. **Extend MetadataReader**:
   ```python
   def _load_custom_metadata(self, df):
       # Parse custom metadata sheet
       pass
   ```

2. **Create Validation Logic**:
   ```python
   def validate_custom_source(custom_metadata):
       # Validation logic for new source
       pass
   ```

## Troubleshooting

### Common Issues

1. **Database Connection Errors**:
   - Verify connection strings are correct
   - Check network connectivity
   - Ensure proper drivers are installed

2. **Metadata File Errors**:
   - Verify Excel file format and sheet names
   - Check for required columns
   - Ensure data types are correct

3. **Autosys Job Errors**:
   - Set `AUTOSYS_MOCK_MODE=true` for testing
   - Verify Autosys command path
   - Check environment permissions

### Debug Mode

Enable debug logging for detailed troubleshooting:

```bash
python run_tests.py --verbose
```

Or set log level in code:
```python
from utils.logger import TestLogger
logger = TestLogger(log_level="DEBUG")
```

## Best Practices

1. **Metadata Management**:
   - Keep metadata files in version control
   - Use consistent naming conventions
   - Document all validation rules

2. **Test Organization**:
   - Group related tests together
   - Use descriptive test names
   - Add proper documentation

3. **Performance Optimization**:
   - Use parallel execution for large test suites
   - Optimize database queries
   - Cache metadata where possible

4. **Error Handling**:
   - Provide clear error messages
   - Log all validation failures
   - Include context in reports

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Update documentation
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For questions and support:
- Check the troubleshooting section
- Review the example configurations
- Create an issue in the repository
