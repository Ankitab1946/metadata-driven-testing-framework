"""
Autosys job availability and status validation checks
"""
import subprocess
import json
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from utils.logger import framework_logger
from config.config import config

class AutosysValidator:
    """Autosys job validation utilities"""
    
    def __init__(self):
        self.autosys_command = config.autosys.command_path
        self.environment = config.autosys.environment
        self.mock_mode = config.autosys.mock_mode
        self.timeout = config.autosys.timeout
    
    def check_job_availability(self, job_name: str) -> Dict[str, Any]:
        """
        Check if Autosys job exists in the specified environment
        
        Args:
            job_name: Name of the Autosys job
        
        Returns:
            Dict with validation results
        """
        try:
            framework_logger.info(f"Checking Autosys job availability: {job_name}")
            
            result = {
                'job_name': job_name,
                'environment': self.environment,
                'job_exists': False,
                'job_definition': None,
                'validation_status': 'FAIL',
                'error_message': None
            }
            
            if self.mock_mode:
                # Mock implementation for testing
                result.update(self._mock_job_availability(job_name))
            else:
                # Real Autosys command execution
                result.update(self._check_real_job_availability(job_name))
            
            return result
            
        except Exception as e:
            error_msg = f"Error checking Autosys job availability for {job_name}: {str(e)}"
            framework_logger.error(error_msg)
            return {
                'job_name': job_name,
                'validation_status': 'ERROR',
                'error_message': error_msg
            }
    
    def check_job_status(self, job_name: str, expected_status: str = 'SU') -> Dict[str, Any]:
        """
        Check Autosys job execution status
        
        Args:
            job_name: Name of the Autosys job
            expected_status: Expected job status (SU=Success, FA=Failure, RU=Running, etc.)
        
        Returns:
            Dict with validation results
        """
        try:
            framework_logger.info(f"Checking Autosys job status: {job_name}")
            
            result = {
                'job_name': job_name,
                'environment': self.environment,
                'current_status': None,
                'expected_status': expected_status,
                'last_run_time': None,
                'next_run_time': None,
                'validation_status': 'FAIL',
                'error_message': None
            }
            
            if self.mock_mode:
                # Mock implementation for testing
                result.update(self._mock_job_status(job_name, expected_status))
            else:
                # Real Autosys command execution
                result.update(self._check_real_job_status(job_name, expected_status))
            
            return result
            
        except Exception as e:
            error_msg = f"Error checking Autosys job status for {job_name}: {str(e)}"
            framework_logger.error(error_msg)
            return {
                'job_name': job_name,
                'validation_status': 'ERROR',
                'error_message': error_msg
            }
    
    def check_job_completion(self, job_name: str, timeout_minutes: int = 60) -> Dict[str, Any]:
        """
        Check if Autosys job has completed within specified timeout
        
        Args:
            job_name: Name of the Autosys job
            timeout_minutes: Timeout in minutes to wait for completion
        
        Returns:
            Dict with validation results
        """
        try:
            framework_logger.info(f"Checking Autosys job completion: {job_name}")
            
            result = {
                'job_name': job_name,
                'environment': self.environment,
                'completed': False,
                'final_status': None,
                'completion_time': None,
                'timeout_minutes': timeout_minutes,
                'validation_status': 'FAIL',
                'error_message': None
            }
            
            if self.mock_mode:
                # Mock implementation for testing
                result.update(self._mock_job_completion(job_name, timeout_minutes))
            else:
                # Real Autosys job completion check
                result.update(self._check_real_job_completion(job_name, timeout_minutes))
            
            return result
            
        except Exception as e:
            error_msg = f"Error checking Autosys job completion for {job_name}: {str(e)}"
            framework_logger.error(error_msg)
            return {
                'job_name': job_name,
                'validation_status': 'ERROR',
                'error_message': error_msg
            }
    
    def _check_real_job_availability(self, job_name: str) -> Dict[str, Any]:
        """Check real Autosys job availability using autorep command"""
        try:
            # Execute autorep command to check job definition
            cmd = [self.autosys_command, '-J', job_name, '-q']
            
            process = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.timeout
            )
            
            if process.returncode == 0:
                # Job exists
                job_definition = process.stdout.strip()
                framework_logger.info(f"Autosys job {job_name} found")
                return {
                    'job_exists': True,
                    'job_definition': job_definition,
                    'validation_status': 'PASS'
                }
            else:
                # Job does not exist
                error_msg = f"Autosys job {job_name} not found: {process.stderr}"
                framework_logger.error(error_msg)
                return {
                    'job_exists': False,
                    'error_message': error_msg
                }
                
        except subprocess.TimeoutExpired:
            error_msg = f"Timeout checking Autosys job {job_name}"
            framework_logger.error(error_msg)
            return {
                'job_exists': False,
                'error_message': error_msg
            }
        except Exception as e:
            error_msg = f"Error executing autorep command: {str(e)}"
            framework_logger.error(error_msg)
            return {
                'job_exists': False,
                'error_message': error_msg
            }
    
    def _check_real_job_status(self, job_name: str, expected_status: str) -> Dict[str, Any]:
        """Check real Autosys job status using autorep command"""
        try:
            # Execute autorep command to check job status
            cmd = [self.autosys_command, '-J', job_name, '-d']
            
            process = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.timeout
            )
            
            if process.returncode == 0:
                # Parse job status from output
                output_lines = process.stdout.strip().split('\n')
                current_status = None
                last_run_time = None
                next_run_time = None
                
                for line in output_lines:
                    if 'Status:' in line:
                        current_status = line.split('Status:')[1].strip()
                    elif 'Last Start:' in line:
                        last_run_time = line.split('Last Start:')[1].strip()
                    elif 'Next Start:' in line:
                        next_run_time = line.split('Next Start:')[1].strip()
                
                validation_status = 'PASS' if current_status == expected_status else 'FAIL'
                error_message = None if validation_status == 'PASS' else f"Expected status {expected_status}, got {current_status}"
                
                framework_logger.info(f"Autosys job {job_name} status: {current_status}")
                return {
                    'current_status': current_status,
                    'last_run_time': last_run_time,
                    'next_run_time': next_run_time,
                    'validation_status': validation_status,
                    'error_message': error_message
                }
            else:
                error_msg = f"Error getting Autosys job status: {process.stderr}"
                framework_logger.error(error_msg)
                return {
                    'error_message': error_msg
                }
                
        except subprocess.TimeoutExpired:
            error_msg = f"Timeout checking Autosys job status for {job_name}"
            framework_logger.error(error_msg)
            return {
                'error_message': error_msg
            }
        except Exception as e:
            error_msg = f"Error executing autorep command: {str(e)}"
            framework_logger.error(error_msg)
            return {
                'error_message': error_msg
            }
    
    def _check_real_job_completion(self, job_name: str, timeout_minutes: int) -> Dict[str, Any]:
        """Check real Autosys job completion with polling"""
        try:
            start_time = datetime.now()
            timeout_time = start_time + timedelta(minutes=timeout_minutes)
            
            while datetime.now() < timeout_time:
                status_result = self._check_real_job_status(job_name, 'SU')
                
                current_status = status_result.get('current_status')
                
                if current_status in ['SU', 'FA', 'TE']:  # Success, Failure, Terminated
                    completion_time = datetime.now()
                    validation_status = 'PASS' if current_status == 'SU' else 'FAIL'
                    error_message = None if current_status == 'SU' else f"Job completed with status: {current_status}"
                    
                    return {
                        'completed': True,
                        'final_status': current_status,
                        'completion_time': completion_time,
                        'validation_status': validation_status,
                        'error_message': error_message
                    }
                
                # Wait before next check
                import time
                time.sleep(30)  # Check every 30 seconds
            
            # Timeout reached
            error_msg = f"Job {job_name} did not complete within {timeout_minutes} minutes"
            framework_logger.error(error_msg)
            return {
                'completed': False,
                'error_message': error_msg
            }
            
        except Exception as e:
            error_msg = f"Error checking job completion: {str(e)}"
            framework_logger.error(error_msg)
            return {
                'completed': False,
                'error_message': error_msg
            }
    
    def _mock_job_availability(self, job_name: str) -> Dict[str, Any]:
        """Mock implementation for job availability check"""
        # Simulate different scenarios based on job name
        mock_jobs = {
            'FEED_LOAD_JOB': {
                'job_exists': True,
                'job_definition': f'insert_job: {job_name}\njob_type: c\ncommand: /scripts/load_feed.sh\nmachine: server1',
                'validation_status': 'PASS'
            },
            'DATA_VALIDATION_JOB': {
                'job_exists': True,
                'job_definition': f'insert_job: {job_name}\njob_type: c\ncommand: /scripts/validate_data.sh\nmachine: server2',
                'validation_status': 'PASS'
            },
            'NONEXISTENT_JOB': {
                'job_exists': False,
                'error_message': f'Job {job_name} not found in Autosys database'
            }
        }
        
        if job_name in mock_jobs:
            result = mock_jobs[job_name]
        else:
            # Default to job exists for unknown jobs
            result = {
                'job_exists': True,
                'job_definition': f'insert_job: {job_name}\njob_type: c\ncommand: /scripts/default.sh\nmachine: server1',
                'validation_status': 'PASS'
            }
        
        framework_logger.info(f"Mock: Autosys job {job_name} availability check completed")
        return result
    
    def _mock_job_status(self, job_name: str, expected_status: str) -> Dict[str, Any]:
        """Mock implementation for job status check"""
        # Simulate different job statuses
        mock_statuses = {
            'FEED_LOAD_JOB': 'SU',
            'DATA_VALIDATION_JOB': 'SU',
            'FAILED_JOB': 'FA',
            'RUNNING_JOB': 'RU'
        }
        
        current_status = mock_statuses.get(job_name, 'SU')  # Default to Success
        validation_status = 'PASS' if current_status == expected_status else 'FAIL'
        error_message = None if validation_status == 'PASS' else f"Expected {expected_status}, got {current_status}"
        
        result = {
            'current_status': current_status,
            'last_run_time': '2024-01-15 10:30:00',
            'next_run_time': '2024-01-16 10:30:00',
            'validation_status': validation_status,
            'error_message': error_message
        }
        
        framework_logger.info(f"Mock: Autosys job {job_name} status check completed")
        return result
    
    def _mock_job_completion(self, job_name: str, timeout_minutes: int) -> Dict[str, Any]:
        """Mock implementation for job completion check"""
        # Simulate job completion scenarios
        completion_scenarios = {
            'FEED_LOAD_JOB': {
                'completed': True,
                'final_status': 'SU',
                'completion_time': datetime.now(),
                'validation_status': 'PASS'
            },
            'FAILED_JOB': {
                'completed': True,
                'final_status': 'FA',
                'completion_time': datetime.now(),
                'validation_status': 'FAIL',
                'error_message': 'Job completed with failure status'
            },
            'TIMEOUT_JOB': {
                'completed': False,
                'error_message': f'Job did not complete within {timeout_minutes} minutes'
            }
        }
        
        if job_name in completion_scenarios:
            result = completion_scenarios[job_name]
        else:
            # Default to successful completion
            result = {
                'completed': True,
                'final_status': 'SU',
                'completion_time': datetime.now(),
                'validation_status': 'PASS'
            }
        
        framework_logger.info(f"Mock: Autosys job {job_name} completion check completed")
        return result
    
    def batch_validate_jobs(self, job_validations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Batch validate multiple Autosys jobs
        
        Args:
            job_validations: List of job validation configurations
        
        Returns:
            List of validation results
        """
        results = []
        
        for validation_config in job_validations:
            validation_type = validation_config.get('type', 'availability')
            job_name = validation_config['job_name']
            
            if validation_type == 'availability':
                result = self.check_job_availability(job_name)
            elif validation_type == 'status':
                expected_status = validation_config.get('expected_status', 'SU')
                result = self.check_job_status(job_name, expected_status)
            elif validation_type == 'completion':
                timeout_minutes = validation_config.get('timeout_minutes', 60)
                result = self.check_job_completion(job_name, timeout_minutes)
            else:
                result = {
                    'job_name': job_name,
                    'validation_status': 'ERROR',
                    'error_message': f"Unknown validation type: {validation_type}"
                }
            
            results.append(result)
        
        return results
    
    def get_job_dependencies(self, job_name: str) -> Dict[str, Any]:
        """
        Get job dependencies (predecessor and successor jobs)
        
        Args:
            job_name: Name of the Autosys job
        
        Returns:
            Dict with dependency information
        """
        try:
            framework_logger.info(f"Getting job dependencies for: {job_name}")
            
            result = {
                'job_name': job_name,
                'predecessors': [],
                'successors': [],
                'validation_status': 'PASS',
                'error_message': None
            }
            
            if self.mock_mode:
                # Mock dependencies
                mock_dependencies = {
                    'FEED_LOAD_JOB': {
                        'predecessors': ['FILE_ARRIVAL_JOB'],
                        'successors': ['DATA_VALIDATION_JOB', 'REPORT_GENERATION_JOB']
                    },
                    'DATA_VALIDATION_JOB': {
                        'predecessors': ['FEED_LOAD_JOB'],
                        'successors': ['ARCHIVE_JOB']
                    }
                }
                
                if job_name in mock_dependencies:
                    result.update(mock_dependencies[job_name])
                
                framework_logger.info(f"Mock: Retrieved dependencies for {job_name}")
            else:
                # Real implementation would use autorep -d command
                # This is a placeholder for real implementation
                pass
            
            return result
            
        except Exception as e:
            error_msg = f"Error getting job dependencies for {job_name}: {str(e)}"
            framework_logger.error(error_msg)
            return {
                'job_name': job_name,
                'validation_status': 'ERROR',
                'error_message': error_msg
            }

# Global Autosys validator instance
autosys_validator = AutosysValidator()
