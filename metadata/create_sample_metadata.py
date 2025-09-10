"""
Script to create sample Metadata.xlsx file with the required structure
"""
import pandas as pd
from pathlib import Path

def create_sample_metadata():
    """Create sample Metadata.xlsx file with all required sheets"""
    
    # Create metadata directory
    metadata_dir = Path("metadata")
    metadata_dir.mkdir(exist_ok=True)
    
    # Sample data for Feed_to_staging sheet
    feed_to_staging_data = [
        {
            'Modules': 'CUSTOMER_MODULE',
            'Feed': 'CUSTOMER_FEED',
            'FieldName': 'CUSTOMER_ID',
            'DBName': 'server1',
            'DB Table': 'CUSTOMER_STG',
            'Where_Clause': '',
            'DataType': 'INTEGER',
            'Nullable': 'N',
            'Request': 'Insert',
            'Default': None,
            'Enumeration': None,
            'RangeBottom': 1,
            'RangeTop': 999999,
            'Mandatory': 'Y',
            'Unique': 'Y'
        },
        {
            'Modules': 'CUSTOMER_MODULE',
            'Feed': 'CUSTOMER_FEED',
            'FieldName': 'CUSTOMER_NAME',
            'DBName': 'server1',
            'DB Table': 'CUSTOMER_STG',
            'DataType': 'VARCHAR',
            'Nullable': 'N',
            'Request': 'Insert',
            'Default': None,
            'Enumeration': None,
            'RangeBottom': None,
            'RangeTop': None,
            'Mandatory': 'Y',
            'Unique': 'N'
        },
        {
            'Modules': 'CUSTOMER_MODULE',
            'Feed': 'CUSTOMER_FEED',
            'FieldName': 'STATUS',
            'DBName': 'server1',
            'DB Table': 'CUSTOMER_STG',
            'DataType': 'VARCHAR',
            'Nullable': 'N',
            'Request': 'Insert',
            'Default': 'ACTIVE',
            'Enumeration': 'STATUS_ENUM',
            'RangeBottom': None,
            'RangeTop': None,
            'Mandatory': 'Y',
            'Unique': 'N'
        },
        {
            'Modules': 'TRANSACTION_MODULE',
            'Feed': 'TRANSACTION_FEED',
            'FieldName': 'TRANSACTION_ID',
            'DBName': 'server2',
            'DB Table': 'TRANSACTION_STG',
            'DataType': 'INTEGER',
            'Nullable': 'N',
            'Request': 'Append',
            'Default': None,
            'Enumeration': None,
            'RangeBottom': 1,
            'RangeTop': 9999999,
            'Mandatory': 'Y',
            'Unique': 'Y'
        },
        {
            'Modules': 'TRANSACTION_MODULE',
            'Feed': 'TRANSACTION_FEED',
            'FieldName': 'AMOUNT',
            'DBName': 'server2',
            'DB Table': 'TRANSACTION_STG',
            'DataType': 'DECIMAL',
            'Nullable': 'N',
            'Request': 'Append',
            'Default': None,
            'Enumeration': None,
            'RangeBottom': 0.01,
            'RangeTop': 1000000,
            'Mandatory': 'Y',
            'Unique': 'N'
        },
        {
            'Modules': 'TRANSACTION_MODULE',
            'Feed': 'TRANSACTION_FEED',
            'FieldName': 'TRANSACTION_TYPE',
            'DBName': 'server2',
            'DB Table': 'TRANSACTION_STG',
            'DataType': 'VARCHAR',
            'Nullable': 'N',
            'Request': 'Append',
            'Default': None,
            'Enumeration': 'TRANSACTION_TYPE_ENUM',
            'RangeBottom': None,
            'RangeTop': None,
            'Mandatory': 'Y',
            'Unique': 'N'
        }
    ]
    
    # Sample data for Staging_to_GRI sheet
    staging_to_gri_data = [
        {
            'Modules': 'CUSTOMER_MODULE',
            'Stg_DBName': 'server1',
            'Stg_DB Table': 'CUSTOMER_STG',
            'Where_Clause_Stg': '',
            'STG_FieldName': 'CUSTOMER_ID',
            'Trg_DBName': 'server1',
            'Trg _DB Table': 'CUSTOMER_MART',
            'Where_Clause_Trg': '',
            'Trg _FieldName': 'CUST_ID',
            'Trg _DataType': 'INTEGER',
            'Nullable': 'N',
            'Request': 'Insert',
            'Default': None,
            'Enumeration': None,
            'RangeBottom': 1,
            'RangeTop': 999999,
            'Mandatory': 'Y',
            'Unique': 'Y'
        },
        {
            'Modules': 'CUSTOMER_MODULE',
            'Stg_DBName': 'server1',
            'Stg_DB Table': 'CUSTOMER_STG',
            'STG_FieldName': 'CUSTOMER_NAME',
            'Trg_DBName': 'server1',
            'Trg _DB Table': 'CUSTOMER_MART',
            'Trg _FieldName': 'CUST_NAME',
            'Trg _DataType': 'VARCHAR',
            'Nullable': 'N',
            'Request': 'Insert',
            'Default': None,
            'Enumeration': None,
            'RangeBottom': None,
            'RangeTop': None,
            'Mandatory': 'Y',
            'Unique': 'N'
        },
        {
            'Modules': 'TRANSACTION_MODULE',
            'Stg_DBName': 'server2',
            'Stg_DB Table': 'TRANSACTION_STG',
            'STG_FieldName': 'TRANSACTION_ID',
            'Trg_DBName': 'server2',
            'Trg _DB Table': 'TRANSACTION_MART',
            'Trg _FieldName': 'TXN_ID',
            'Trg _DataType': 'INTEGER',
            'Nullable': 'N',
            'Request': 'Append',
            'Default': None,
            'Enumeration': None,
            'RangeBottom': 1,
            'RangeTop': 9999999,
            'Mandatory': 'Y',
            'Unique': 'Y'
        }
    ]
    
    # Sample data for Enumeration sheet
    enumeration_data = [
        {'EnumerationName': 'STATUS_ENUM', 'EnumValues': 'ACTIVE'},
        {'EnumerationName': 'STATUS_ENUM', 'EnumValues': 'INACTIVE'},
        {'EnumerationName': 'STATUS_ENUM', 'EnumValues': 'SUSPENDED'},
        {'EnumerationName': 'TRANSACTION_TYPE_ENUM', 'EnumValues': 'DEBIT'},
        {'EnumerationName': 'TRANSACTION_TYPE_ENUM', 'EnumValues': 'CREDIT'},
        {'EnumerationName': 'TRANSACTION_TYPE_ENUM', 'EnumValues': 'TRANSFER'},
        {'EnumerationName': 'TRANSACTION_TYPE_ENUM', 'EnumValues': 'REFUND'}
    ]
    
    # Sample data for Patterns sheet
    patterns_data = [
        {
            'PatternName': 'CUSTOMER_PATTERN_1',
            'Feed': 'CUSTOMER_FEED',
            'Column1': 'CUSTOMER_ID',
            'Column2': 'STATUS',
            'ExpectedPattern': 'ID should be numeric, Status should be ACTIVE for new customers'
        },
        {
            'PatternName': 'TRANSACTION_PATTERN_1',
            'Feed': 'TRANSACTION_FEED',
            'Column1': 'AMOUNT',
            'Column2': 'TRANSACTION_TYPE',
            'ExpectedPattern': 'DEBIT amounts should be positive, CREDIT amounts should be positive'
        }
    ]
    
    # Sample data for Reconciliations sheet
    reconciliations_data = [
        {
            'ReconciliationType': 'Inter',
            'ReconciliationName': 'CUSTOMER_COUNT_CHECK',
            'SourceFeed': 'CUSTOMER_FEED',
            'SourceTable': 'CUSTOMER_STG',
            'SourceColumn': 'CUSTOMER_ID',
            'TargetFeed': 'CUSTOMER_FEED',
            'TargetTable': 'CUSTOMER_STG',
            'TargetColumn': 'CUSTOMER_ID',
            'Operation': 'COUNT',
            'Tolerance': 0
        },
        {
            'ReconciliationType': 'Intra',
            'ReconciliationName': 'TRANSACTION_AMOUNT_SUM',
            'SourceFeed': 'TRANSACTION_FEED',
            'SourceTable': 'TRANSACTION_STG',
            'SourceColumn': 'AMOUNT',
            'TargetFeed': 'SUMMARY_FEED',
            'TargetTable': 'DAILY_SUMMARY',
            'TargetColumn': 'TOTAL_AMOUNT',
            'Operation': 'SUM',
            'Tolerance': 0.01
        }
    ]
    
    # Sample data for Business Rules sheet
    business_rules_data = [
        {
            'RuleName': 'CUSTOMER_AGE_VALIDATION',
            'RuleDescription': 'Customer age should be between 18 and 100',
            'Feed': 'CUSTOMER_FEED',
            'Table': 'CUSTOMER_STG',
            'Column': 'AGE',
            'RuleLogic': 'AGE >= 18 AND AGE <= 100',
            'ErrorMessage': 'Customer age must be between 18 and 100'
        },
        {
            'RuleName': 'TRANSACTION_BUSINESS_HOURS',
            'RuleDescription': 'Transactions should occur during business hours',
            'Feed': 'TRANSACTION_FEED',
            'Table': 'TRANSACTION_STG',
            'Column': 'TRANSACTION_TIME',
            'RuleLogic': 'HOUR(TRANSACTION_TIME) BETWEEN 9 AND 17',
            'ErrorMessage': 'Transactions should occur between 9 AM and 5 PM'
        }
    ]
    
    # Create DataFrames
    feed_to_staging_df = pd.DataFrame(feed_to_staging_data)
    staging_to_gri_df = pd.DataFrame(staging_to_gri_data)
    enumeration_df = pd.DataFrame(enumeration_data)
    patterns_df = pd.DataFrame(patterns_data)
    reconciliations_df = pd.DataFrame(reconciliations_data)
    business_rules_df = pd.DataFrame(business_rules_data)
    
    # Write to Excel file with multiple sheets
    excel_file_path = metadata_dir / "Metadata.xlsx"
    
    with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
        feed_to_staging_df.to_excel(writer, sheet_name='Feed_to_staging', index=False)
        staging_to_gri_df.to_excel(writer, sheet_name='Staging_to_GRI', index=False)
        enumeration_df.to_excel(writer, sheet_name='Enumeration', index=False)
        patterns_df.to_excel(writer, sheet_name='Patterns', index=False)
        reconciliations_df.to_excel(writer, sheet_name='Reconciliations', index=False)
        business_rules_df.to_excel(writer, sheet_name='Business Rules', index=False)
    
    print(f"Sample Metadata.xlsx created at: {excel_file_path}")
    print("\nSheets created:")
    print("1. Feed_to_staging - Feed to staging metadata")
    print("2. Staging_to_GRI - Staging to GRI metadata")
    print("3. Enumeration - Enumeration values")
    print("4. Patterns - Pattern validation rules")
    print("5. Reconciliations - Reconciliation rules")
    print("6. Business Rules - Business validation rules")
    
    return str(excel_file_path)

if __name__ == "__main__":
    create_sample_metadata()
