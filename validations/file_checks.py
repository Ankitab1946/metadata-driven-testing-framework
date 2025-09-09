"""
File availability and log file validation checks
"""
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from utils.logger import framework_logger
from config.config import config

class FileValidator:
    """File validation utilities"""
    
    def __init__(self):
        self.feed_base_path = Path(config.paths.feed_base_path)
        self.log_base_path = Path(config.paths.log_base_path)
    
    def check_feed_file_availability(self, feed_name: str, file_pattern: Optional[str] = None, 
                                   expected_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Check if feed file is available in the specified path
        
        Args:
            feed_name: Name of the feed
            file_pattern: File pattern to match (e.g., "*.csv", "feed_*.txt")
            expected_date: Expected date in YYYYMMDD format
        
        Returns:
            Dict with validation results
        """
        try:
            framework_logger.info(f"Checking feed file availability for: {feed_name}")
            
            # Construct feed directory path
            feed_dir = self.feed_base_path / feed_name
            
            result = {
                'feed_name': feed_name,
                'feed_directory': str(feed_dir),
                'directory_exists': False,
                'files_found': [],
                'file_count': 0,
                'latest_file': None,
                'latest_file_timestamp': None,
                'validation_status': 'FAIL',
                'error_message': None
            }
            
            # Check if feed directory exists
            if not feed_dir.exists():
                result['error_message'] = f"Feed directory does not exist: {feed_dir}"
                framework_logger.error(result['error_message'])
                return result
            
            result['directory_exists'] = True
            
            # Get file pattern
            if file_pattern is None:
                file_pattern = f"{feed_name}*"
            
            # Find matching files
            matching_files = []
            for file_path in feed_dir.glob(file_pattern):
                if file_path.is_file():
                    file_info = {
                        'file_name': file_path.name,
                        'file_path': str(file_path),
                        'file_size': file_path.stat().st_size,
                        'modified_time': datetime.fromtimestamp(file_path.stat().st_mtime),
                        'created_time': datetime.fromtimestamp(file_path.stat().st_ctime)
                    }
                    matching_files.append(file_info)
            
            # Sort files by modification time (newest first)
            matching_files.sort(key=lambda x: x['modified_time'], reverse=True)
            
            result['files_found'] = matching_files
            result['file_count'] = len(matching_files)
            
            if matching_files:
                result['latest_file'] = matching_files[0]['file_name']
                result['latest_file_timestamp'] = matching_files[0]['modified_time']
                
                # Check if file is from expected date
                if expected_date:
                    expected_dt = datetime.strptime(expected_date, '%Y%m%d')
                    file_date = matching_files[0]['modified_time'].date()
                    
                    if file_date == expected_dt.date():
                        result['validation_status'] = 'PASS'
                        framework_logger.info(f"Feed file found for expected date: {expected_date}")
                    else:
                        result['error_message'] = f"File date {file_date} does not match expected date {expected_dt.date()}"
                        framework_logger.warning(result['error_message'])
                else:
                    result['validation_status'] = 'PASS'
                    framework_logger.info(f"Feed file found: {result['latest_file']}")
            else:
                result['error_message'] = f"No files found matching pattern: {file_pattern}"
                framework_logger.error(result['error_message'])
            
            return result
            
        except Exception as e:
            error_msg = f"Error checking feed file availability for {feed_name}: {str(e)}"
            framework_logger.error(error_msg)
            return {
                'feed_name': feed_name,
                'validation_status': 'ERROR',
                'error_message': error_msg
            }
    
    def check_log_file_generation(self, job_name: str, expected_date: Optional[str] = None,
                                log_type: str = 'error') -> Dict[str, Any]:
        """
        Check if log file is generated for failed jobs
        
        Args:
            job_name: Name of the Autosys job
            expected_date: Expected date in YYYYMMDD format
            log_type: Type of log file ('error', 'info', 'debug')
        
        Returns:
            Dict with validation results
        """
        try:
            framework_logger.info(f"Checking log file generation for job: {job_name}")
            
            # Construct log directory path
            log_dir = self.log_base_path / job_name
            
            result = {
                'job_name': job_name,
                'log_directory': str(log_dir),
                'directory_exists': False,
                'log_files_found': [],
                'log_file_count': 0,
                'latest_log_file': None,
                'latest_log_timestamp': None,
                'validation_status': 'FAIL',
                'error_message': None
            }
            
            # Check if log directory exists
            if not log_dir.exists():
                result['error_message'] = f"Log directory does not exist: {log_dir}"
                framework_logger.error(result['error_message'])
                return result
            
            result['directory_exists'] = True
            
            # Define log file patterns
            log_patterns = {
                'error': f"{job_name}*error*.log",
                'info': f"{job_name}*info*.log",
                'debug': f"{job_name}*debug*.log"
            }
            
            pattern = log_patterns.get(log_type, f"{job_name}*.log")
            
            # Find matching log files
            matching_logs = []
            for log_path in log_dir.glob(pattern):
                if log_path.is_file():
                    log_info = {
                        'file_name': log_path.name,
                        'file_path': str(log_path),
                        'file_size': log_path.stat().st_size,
                        'modified_time': datetime.fromtimestamp(log_path.stat().st_mtime),
                        'created_time': datetime.fromtimestamp(log_path.stat().st_ctime)
                    }
                    matching_logs.append(log_info)
            
            # Sort logs by modification time (newest first)
            matching_logs.sort(key=lambda x: x['modified_time'], reverse=True)
            
            result['log_files_found'] = matching_logs
            result['log_file_count'] = len(matching_logs)
            
            if matching_logs:
                result['latest_log_file'] = matching_logs[0]['file_name']
                result['latest_log_timestamp'] = matching_logs[0]['modified_time']
                
                # Check if log is from expected date
                if expected_date:
                    expected_dt = datetime.strptime(expected_date, '%Y%m%d')
                    log_date = matching_logs[0]['modified_time'].date()
                    
                    if log_date == expected_dt.date():
                        result['validation_status'] = 'PASS'
                        framework_logger.info(f"Log file found for expected date: {expected_date}")
                    else:
                        result['error_message'] = f"Log date {log_date} does not match expected date {expected_dt.date()}"
                        framework_logger.warning(result['error_message'])
                else:
                    result['validation_status'] = 'PASS'
                    framework_logger.info(f"Log file found: {result['latest_log_file']}")
            else:
                result['error_message'] = f"No log files found matching pattern: {pattern}"
                framework_logger.error(result['error_message'])
            
            return result
            
        except Exception as e:
            error_msg = f"Error checking log file generation for {job_name}: {str(e)}"
            framework_logger.error(error_msg)
            return {
                'job_name': job_name,
                'validation_status': 'ERROR',
                'error_message': error_msg
            }
    
    def validate_file_size(self, file_path: str, min_size_mb: Optional[float] = None,
                          max_size_mb: Optional[float] = None) -> Dict[str, Any]:
        """
        Validate file size constraints
        
        Args:
            file_path: Path to the file
            min_size_mb: Minimum expected file size in MB
            max_size_mb: Maximum expected file size in MB
        
        Returns:
            Dict with validation results
        """
        try:
            framework_logger.info(f"Validating file size for: {file_path}")
            
            file_path_obj = Path(file_path)
            
            result = {
                'file_path': file_path,
                'file_exists': False,
                'file_size_bytes': 0,
                'file_size_mb': 0.0,
                'validation_status': 'FAIL',
                'error_message': None
            }
            
            if not file_path_obj.exists():
                result['error_message'] = f"File does not exist: {file_path}"
                framework_logger.error(result['error_message'])
                return result
            
            result['file_exists'] = True
            file_size_bytes = file_path_obj.stat().st_size
            file_size_mb = file_size_bytes / (1024 * 1024)
            
            result['file_size_bytes'] = file_size_bytes
            result['file_size_mb'] = round(file_size_mb, 2)
            
            # Validate size constraints
            size_valid = True
            error_messages = []
            
            if min_size_mb and file_size_mb < min_size_mb:
                size_valid = False
                error_messages.append(f"File size {file_size_mb}MB is below minimum {min_size_mb}MB")
            
            if max_size_mb and file_size_mb > max_size_mb:
                size_valid = False
                error_messages.append(f"File size {file_size_mb}MB exceeds maximum {max_size_mb}MB")
            
            if size_valid:
                result['validation_status'] = 'PASS'
                framework_logger.info(f"File size validation passed: {file_size_mb}MB")
            else:
                result['error_message'] = "; ".join(error_messages)
                framework_logger.error(result['error_message'])
            
            return result
            
        except Exception as e:
            error_msg = f"Error validating file size for {file_path}: {str(e)}"
            framework_logger.error(error_msg)
            return {
                'file_path': file_path,
                'validation_status': 'ERROR',
                'error_message': error_msg
            }
    
    def check_file_freshness(self, file_path: str, max_age_hours: int = 24) -> Dict[str, Any]:
        """
        Check if file is fresh (within specified age)
        
        Args:
            file_path: Path to the file
            max_age_hours: Maximum age in hours
        
        Returns:
            Dict with validation results
        """
        try:
            framework_logger.info(f"Checking file freshness for: {file_path}")
            
            file_path_obj = Path(file_path)
            
            result = {
                'file_path': file_path,
                'file_exists': False,
                'file_age_hours': 0,
                'max_age_hours': max_age_hours,
                'validation_status': 'FAIL',
                'error_message': None
            }
            
            if not file_path_obj.exists():
                result['error_message'] = f"File does not exist: {file_path}"
                framework_logger.error(result['error_message'])
                return result
            
            result['file_exists'] = True
            
            # Calculate file age
            file_mtime = datetime.fromtimestamp(file_path_obj.stat().st_mtime)
            current_time = datetime.now()
            file_age = current_time - file_mtime
            file_age_hours = file_age.total_seconds() / 3600
            
            result['file_age_hours'] = round(file_age_hours, 2)
            result['file_modified_time'] = file_mtime
            
            if file_age_hours <= max_age_hours:
                result['validation_status'] = 'PASS'
                framework_logger.info(f"File freshness validation passed: {file_age_hours} hours old")
            else:
                result['error_message'] = f"File is too old: {file_age_hours} hours (max: {max_age_hours})"
                framework_logger.error(result['error_message'])
            
            return result
            
        except Exception as e:
            error_msg = f"Error checking file freshness for {file_path}: {str(e)}"
            framework_logger.error(error_msg)
            return {
                'file_path': file_path,
                'validation_status': 'ERROR',
                'error_message': error_msg
            }
    
    def batch_validate_files(self, file_validations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Batch validate multiple files
        
        Args:
            file_validations: List of validation configurations
        
        Returns:
            List of validation results
        """
        results = []
        
        for validation_config in file_validations:
            validation_type = validation_config.get('type', 'availability')
            
            if validation_type == 'availability':
                result = self.check_feed_file_availability(
                    validation_config['feed_name'],
                    validation_config.get('file_pattern'),
                    validation_config.get('expected_date')
                )
            elif validation_type == 'log_generation':
                result = self.check_log_file_generation(
                    validation_config['job_name'],
                    validation_config.get('expected_date'),
                    validation_config.get('log_type', 'error')
                )
            elif validation_type == 'size':
                result = self.validate_file_size(
                    validation_config['file_path'],
                    validation_config.get('min_size_mb'),
                    validation_config.get('max_size_mb')
                )
            elif validation_type == 'freshness':
                result = self.check_file_freshness(
                    validation_config['file_path'],
                    validation_config.get('max_age_hours', 24)
                )
            else:
                result = {
                    'validation_status': 'ERROR',
                    'error_message': f"Unknown validation type: {validation_type}"
                }
            
            results.append(result)
        
        return results

# Global file validator instance
file_validator = FileValidator()
