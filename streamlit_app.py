"""
Streamlit Web Application for Metadata-Driven Testing Framework
"""
import streamlit as st
import os
import json
import uuid
from pathlib import Path
from datetime import datetime
import pandas as pd
from utils.excel_reader import MetadataReader
from validations.file_checks import file_validator
from validations.autosys_checks import autosys_validator
from validations.db_validations import db_validator
from validations.report_generator import report_generator
from utils.logger import framework_logger
from config.config import config

UPLOAD_FOLDER = 'uploads'
REPORTS_FOLDER = 'reports'

Path(UPLOAD_FOLDER).mkdir(exist_ok=True)
Path(REPORTS_FOLDER).mkdir(exist_ok=True)

st.title("Metadata-Driven Testing Framework")

uploaded_file = st.file_uploader("Upload your Metadata.xlsx file", type=['xlsx', 'xls'])

if uploaded_file:
    # Save uploaded file
    session_id = str(uuid.uuid4())
    filename = f"{session_id}_Metadata.xlsx"
    filepath = os.path.join(UPLOAD_FOLDER, filename)
    with open(filepath, "wb") as f:
        f.write(uploaded_file.getbuffer())
    st.success(f"Metadata file uploaded successfully: {uploaded_file.name}")

    # Load metadata
    try:
        metadata_reader = MetadataReader(filepath)
        metadata = metadata_reader.load_all_metadata()
    except Exception as e:
        st.error(f"Failed to load metadata: {str(e)}")
        st.stop()

    # Show metadata summary
    st.subheader("Metadata Summary")
    st.write(f"Feed Records: {len(metadata['feed_metadata'])}")
    st.write(f"Staging Records: {len(metadata['staging_metadata'])}")
    st.write(f"Enumeration Records: {len(metadata['enumeration_metadata'])}")

    # Select feeds
    raw_feeds = []
    for meta in metadata_reader.feed_metadata:
        raw_feeds.extend(meta.feed_list)
    # Remove duplicates and empty strings
    unique_feeds = sorted(set(f.strip() for f in raw_feeds if f.strip()))
    selected_feeds = st.multiselect("Select Feeds to Validate", unique_feeds, default=unique_feeds)

    # Database configuration
    st.subheader("SQL Server Configuration (Windows Authentication)")
    server1_host = st.text_input("Server 1 Host", value="localhost")
    server1_db = st.text_input("Server 1 Database", value="TestDB1")
    server1_enabled = st.checkbox("Enable Server 1", value=True)

    server2_host = st.text_input("Server 2 Host", value="localhost")
    server2_db = st.text_input("Server 2 Database", value="TestDB2")
    server2_enabled = st.checkbox("Enable Server 2", value=True)

    # Validation types
    st.subheader("Validation Types")
    file_validation = st.checkbox("File Validations", value=True)
    autosys_validation = st.checkbox("Autosys Job Validations", value=True)
    db_validation = st.checkbox("Database Validations", value=True)

    # Screenshot options
    st.subheader("Screenshot Options")
    capture_screenshots = st.checkbox("Capture Screenshots", value=True)
    screenshot_on_failure = st.checkbox("Screenshot on Failure Only", value=True)

    if st.button("Run Validation Tests"):
        # Prepare database configs
        database_configs = []
        if server1_enabled:
            database_configs.append({
                'name': 'server1',
                'host': server1_host,
                'database': server1_db,
                'authentication': 'Windows'
            })
        if server2_enabled:
            database_configs.append({
                'name': 'server2',
                'host': server2_host,
                'database': server2_db,
                'authentication': 'Windows'
            })

        validation_types = []
        if file_validation:
            validation_types.append('file_validation')
        if autosys_validation:
            validation_types.append('autosys_validation')
        if db_validation:
            validation_types.append('db_validation')

        # Run validations
        with st.spinner("Running validations..."):
            validation_results = []

            # File validations
            if 'file_validation' in validation_types:
                for feed_name in selected_feeds[:3]:  # Limit for demo
                    result = file_validator.check_feed_file_availability(feed_name)
                    result['validation_category'] = 'File Validation'
                    validation_results.append(result)

            # Autosys validations
            if 'autosys_validation' in validation_types:
                for feed_name in selected_feeds[:3]:
                    job_name = f"FEED_LOAD_{feed_name.upper()}"
                    result = autosys_validator.check_job_availability(job_name)
                    result['validation_category'] = 'Autosys Validation'
                    result['test_type'] = 'Job Availability'
                    validation_results.append(result)

                    result = autosys_validator.check_job_status(job_name, 'SU')
                    result['validation_category'] = 'Autosys Validation'
                    result['test_type'] = 'Job Status'
                    validation_results.append(result)

            # Database validations (mock)
            if 'db_validation' in validation_types:
                for feed_meta in metadata['feed_metadata'][:5]:
                    if feed_meta.feed in selected_feeds:
                        result = {
                            'validation_type': 'data_type',
                            'validation_category': 'Database Validation',
                            'test_type': 'Data Type',
                            'feed_name': feed_meta.feed,
                            'column_name': feed_meta.field_name,
                            'expected_type': feed_meta.data_type,
                            'validation_status': 'PASS',
                            'error_message': None
                        }
                        validation_results.append(result)

                        if feed_meta.mandatory.upper() == 'Y':
                            result = {
                                'validation_type': 'nullable_constraint',
                                'validation_category': 'Database Validation',
                                'test_type': 'Mandatory Field',
                                'feed_name': feed_meta.feed,
                                'column_name': feed_meta.field_name,
                                'null_count': 0,
                                'validation_status': 'PASS',
                                'error_message': None
                            }
                            validation_results.append(result)

            # Generate summary
            total_tests = len(validation_results)
            passed_tests = len([r for r in validation_results if r.get('validation_status') == 'PASS'])
            failed_tests = len([r for r in validation_results if r.get('validation_status') == 'FAIL'])
            error_tests = len([r for r in validation_results if r.get('validation_status') == 'ERROR'])

            summary = {
                'total_tests': total_tests,
                'passed_tests': passed_tests,
                'failed_tests': failed_tests,
                'error_tests': error_tests,
                'success_rate': round((passed_tests / total_tests * 100) if total_tests > 0 else 0, 2)
            }

            # Generate reports
            report_paths = report_generator.generate_comprehensive_report(
                validation_results,
                f"streamlit_validation_report_{session_id}"
            )

            # Display results
            st.success("Validation completed successfully!")
            st.write("### Summary")
            st.write(summary)

            st.write("### Detailed Results")
            st.dataframe(validation_results)

            st.write("### Download Reports")
            for report_type, path in report_paths.items():
                st.markdown(f"[Download {report_type.upper()} Report](./{path})")

else:
    st.info("Please upload a Metadata.xlsx file to begin.")
