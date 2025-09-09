"""
Flask Web Application for Metadata-Driven Testing Framework
"""
import os
import json
import uuid
from pathlib import Path
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_file, redirect, url_for, flash
from werkzeug.utils import secure_filename
import pandas as pd
from utils.excel_reader import MetadataReader
from validations.file_checks import file_validator
from validations.autosys_checks import autosys_validator
from validations.db_validations import db_validator
from validations.report_generator import report_generator
from utils.logger import framework_logger
from config.config import config

app = Flask(__name__)
app.secret_key = 'metadata_testing_framework_secret_key'

# Configuration
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}
MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB max file size

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH

# Create necessary directories
Path(UPLOAD_FOLDER).mkdir(exist_ok=True)
Path('static/css').mkdir(parents=True, exist_ok=True)
Path('static/js').mkdir(parents=True, exist_ok=True)
Path('templates').mkdir(exist_ok=True)

def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/')
def index():
    """Main page"""
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_metadata():
    """Handle metadata file upload"""
    try:
        if 'metadata_file' not in request.files:
            return jsonify({'error': 'No file selected'}), 400
        
        file = request.files['metadata_file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if not allowed_file(file.filename):
            return jsonify({'error': 'Invalid file type. Please upload .xlsx or .xls file'}), 400
        
        # Generate unique filename
        session_id = str(uuid.uuid4())
        filename = f"{session_id}_{secure_filename(file.filename)}"
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        
        # Save file
        file.save(filepath)
        
        # Validate metadata file structure
        try:
            metadata_reader = MetadataReader(filepath)
            metadata = metadata_reader.load_all_metadata()
            
            # Get summary information
            summary = {
                'feed_count': len(metadata['feed_metadata']),
                'staging_count': len(metadata['staging_metadata']),
                'enumeration_count': len(metadata['enumeration_metadata']),
                'unique_feeds': metadata_reader.get_unique_feeds(),
                'unique_modules': metadata_reader.get_unique_modules()
            }
            
            framework_logger.info(f"Metadata uploaded successfully: {filename}")
            
            return jsonify({
                'success': True,
                'session_id': session_id,
                'filename': file.filename,
                'summary': summary
            })
            
        except Exception as e:
            # Remove invalid file
            if os.path.exists(filepath):
                os.remove(filepath)
            return jsonify({'error': f'Invalid metadata file: {str(e)}'}), 400
            
    except Exception as e:
        framework_logger.error(f"Error uploading metadata: {str(e)}")
        return jsonify({'error': f'Upload failed: {str(e)}'}), 500

@app.route('/validate', methods=['POST'])
def run_validation():
    """Run validation tests"""
    try:
        data = request.get_json()
        session_id = data.get('session_id')
        selected_feeds = data.get('selected_feeds', [])
        selected_databases = data.get('selected_databases', [])
        validation_types = data.get('validation_types', [])
        
        if not session_id:
            return jsonify({'error': 'No session ID provided'}), 400
        
        # Find metadata file
        metadata_files = [f for f in os.listdir(UPLOAD_FOLDER) if f.startswith(session_id)]
        if not metadata_files:
            return jsonify({'error': 'Metadata file not found'}), 404
        
        metadata_filepath = os.path.join(UPLOAD_FOLDER, metadata_files[0])
        
        # Load metadata
        metadata_reader = MetadataReader(metadata_filepath)
        metadata = metadata_reader.load_all_metadata()
        
        # Run validations
        validation_results = []
        
        # File validations
        if not validation_types or 'file_validation' in validation_types:
            framework_logger.info("Running file validations...")
            feeds_to_test = selected_feeds if selected_feeds else metadata_reader.get_unique_feeds()
            
            for feed_name in feeds_to_test[:3]:  # Limit to 3 for demo
                result = file_validator.check_feed_file_availability(feed_name)
                result['validation_category'] = 'File Validation'
                validation_results.append(result)
        
        # Autosys validations
        if not validation_types or 'autosys_validation' in validation_types:
            framework_logger.info("Running Autosys validations...")
            feeds_to_test = selected_feeds if selected_feeds else metadata_reader.get_unique_feeds()
            
            for feed_name in feeds_to_test[:3]:  # Limit to 3 for demo
                job_name = f"FEED_LOAD_{feed_name.upper()}"
                
                # Job availability
                result = autosys_validator.check_job_availability(job_name)
                result['validation_category'] = 'Autosys Validation'
                result['test_type'] = 'Job Availability'
                validation_results.append(result)
                
                # Job status
                result = autosys_validator.check_job_status(job_name, 'SU')
                result['validation_category'] = 'Autosys Validation'
                result['test_type'] = 'Job Status'
                validation_results.append(result)
        
        # Database validations (mock)
        if not validation_types or 'db_validation' in validation_types:
            framework_logger.info("Running database validations...")
            
            # Mock database validations
            for feed_meta in metadata['feed_metadata'][:5]:  # Limit to 5 for demo
                if not selected_feeds or feed_meta.feed in selected_feeds:
                    # Data type validation
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
                    
                    # Mandatory field validation
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
        
        # Generate summary statistics
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
            f"web_validation_report_{session_id}"
        )
        
        # Store results for download
        results_data = {
            'session_id': session_id,
            'timestamp': datetime.now().isoformat(),
            'summary': summary,
            'validation_results': validation_results,
            'report_paths': report_paths
        }
        
        results_file = f"results_{session_id}.json"
        with open(os.path.join(UPLOAD_FOLDER, results_file), 'w') as f:
            json.dump(results_data, f, default=str, indent=2)
        
        framework_logger.info(f"Validation completed for session {session_id}")
        
        return jsonify({
            'success': True,
            'summary': summary,
            'validation_results': validation_results,
            'report_paths': {k: os.path.basename(v) for k, v in report_paths.items()}
        })
        
    except Exception as e:
        framework_logger.error(f"Error running validation: {str(e)}")
        return jsonify({'error': f'Validation failed: {str(e)}'}), 500

@app.route('/download/<report_type>/<filename>')
def download_report(report_type, filename):
    """Download generated reports"""
    try:
        report_path = os.path.join('reports', filename)
        if not os.path.exists(report_path):
            return jsonify({'error': 'Report not found'}), 404
        
        return send_file(report_path, as_attachment=True)
        
    except Exception as e:
        framework_logger.error(f"Error downloading report: {str(e)}")
        return jsonify({'error': f'Download failed: {str(e)}'}), 500

@app.route('/api/config')
def get_config():
    """Get current configuration"""
    return jsonify({
        'autosys_mock_mode': config.autosys.mock_mode,
        'autosys_environment': config.autosys.environment,
        'feed_base_path': config.paths.feed_base_path,
        'log_base_path': config.paths.log_base_path
    })

if __name__ == '__main__':
    framework_logger.info("Starting Metadata-Driven Testing Framework Web Application")
    app.run(host='0.0.0.0', port=8000, debug=True)
