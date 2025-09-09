"""
Report generation utilities for test results
"""
import os
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
import pandas as pd
from jinja2 import Template
import xlsxwriter
from utils.logger import framework_logger
from config.config import config

class ReportGenerator:
    """Report generation utilities"""
    
    def __init__(self):
        self.report_path = Path(config.paths.report_output_path)
        self.screenshot_path = Path(config.paths.screenshot_path)
        self.report_path.mkdir(parents=True, exist_ok=True)
        self.screenshot_path.mkdir(parents=True, exist_ok=True)
    
    def generate_html_report(self, test_results: List[Dict[str, Any]], 
                           report_name: str = "validation_report") -> str:
        """
        Generate HTML report from test results
        
        Args:
            test_results: List of test validation results
            report_name: Name of the report file
        
        Returns:
            Path to generated HTML report
        """
        try:
            framework_logger.info(f"Generating HTML report: {report_name}")
            
            # Prepare report data
            report_data = self._prepare_report_data(test_results)
            
            # Generate HTML content
            html_content = self._generate_html_content(report_data)
            
            # Save HTML report
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            html_filename = f"{report_name}_{timestamp}.html"
            html_filepath = self.report_path / html_filename
            
            with open(html_filepath, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            framework_logger.info(f"HTML report generated: {html_filepath}")
            return str(html_filepath)
            
        except Exception as e:
            error_msg = f"Error generating HTML report: {str(e)}"
            framework_logger.error(error_msg)
            raise
    
    def generate_excel_report(self, test_results: List[Dict[str, Any]], 
                            report_name: str = "validation_report") -> str:
        """
        Generate Excel report from test results
        
        Args:
            test_results: List of test validation results
            report_name: Name of the report file
        
        Returns:
            Path to generated Excel report
        """
        try:
            framework_logger.info(f"Generating Excel report: {report_name}")
            
            # Prepare report data
            report_data = self._prepare_report_data(test_results)
            
            # Create Excel file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            excel_filename = f"{report_name}_{timestamp}.xlsx"
            excel_filepath = self.report_path / excel_filename
            
            # Create workbook and worksheets
            workbook = xlsxwriter.Workbook(str(excel_filepath))
            
            # Define formats
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#4472C4',
                'font_color': 'white',
                'border': 1
            })
            
            pass_format = workbook.add_format({
                'bg_color': '#C6EFCE',
                'font_color': '#006100',
                'border': 1
            })
            
            fail_format = workbook.add_format({
                'bg_color': '#FFC7CE',
                'font_color': '#9C0006',
                'border': 1
            })
            
            error_format = workbook.add_format({
                'bg_color': '#FFEB9C',
                'font_color': '#9C5700',
                'border': 1
            })
            
            # Create summary worksheet
            self._create_summary_worksheet(workbook, report_data, header_format, pass_format, fail_format, error_format)
            
            # Create detailed results worksheets
            self._create_detailed_worksheets(workbook, test_results, header_format, pass_format, fail_format, error_format)
            
            workbook.close()
            
            framework_logger.info(f"Excel report generated: {excel_filepath}")
            return str(excel_filepath)
            
        except Exception as e:
            error_msg = f"Error generating Excel report: {str(e)}"
            framework_logger.error(error_msg)
            raise
    
    def generate_json_report(self, test_results: List[Dict[str, Any]], 
                           report_name: str = "validation_report") -> str:
        """
        Generate JSON report from test results
        
        Args:
            test_results: List of test validation results
            report_name: Name of the report file
        
        Returns:
            Path to generated JSON report
        """
        try:
            framework_logger.info(f"Generating JSON report: {report_name}")
            
            # Prepare report data
            report_data = self._prepare_report_data(test_results)
            report_data['detailed_results'] = test_results
            
            # Save JSON report
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            json_filename = f"{report_name}_{timestamp}.json"
            json_filepath = self.report_path / json_filename
            
            with open(json_filepath, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, indent=2, default=str)
            
            framework_logger.info(f"JSON report generated: {json_filepath}")
            return str(json_filepath)
            
        except Exception as e:
            error_msg = f"Error generating JSON report: {str(e)}"
            framework_logger.error(error_msg)
            raise
    
    def _prepare_report_data(self, test_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Prepare summary data for reports"""
        total_tests = len(test_results)
        passed_tests = len([r for r in test_results if r.get('validation_status') == 'PASS'])
        failed_tests = len([r for r in test_results if r.get('validation_status') == 'FAIL'])
        error_tests = len([r for r in test_results if r.get('validation_status') == 'ERROR'])
        
        # Group results by validation type
        validation_types = {}
        for result in test_results:
            val_type = result.get('validation_type', 'Unknown')
            if val_type not in validation_types:
                validation_types[val_type] = {'total': 0, 'passed': 0, 'failed': 0, 'errors': 0}
            
            validation_types[val_type]['total'] += 1
            status = result.get('validation_status', 'ERROR')
            if status == 'PASS':
                validation_types[val_type]['passed'] += 1
            elif status == 'FAIL':
                validation_types[val_type]['failed'] += 1
            else:
                validation_types[val_type]['errors'] += 1
        
        # Group results by feed
        feed_results = {}
        for result in test_results:
            feed_name = result.get('feed_name', 'Unknown')
            if feed_name not in feed_results:
                feed_results[feed_name] = {'total': 0, 'passed': 0, 'failed': 0, 'errors': 0}
            
            feed_results[feed_name]['total'] += 1
            status = result.get('validation_status', 'ERROR')
            if status == 'PASS':
                feed_results[feed_name]['passed'] += 1
            elif status == 'FAIL':
                feed_results[feed_name]['failed'] += 1
            else:
                feed_results[feed_name]['errors'] += 1
        
        return {
            'report_generated_at': datetime.now(),
            'total_tests': total_tests,
            'passed_tests': passed_tests,
            'failed_tests': failed_tests,
            'error_tests': error_tests,
            'success_rate': round((passed_tests / total_tests * 100) if total_tests > 0 else 0, 2),
            'validation_types': validation_types,
            'feed_results': feed_results
        }
    
    def _generate_html_content(self, report_data: Dict[str, Any]) -> str:
        """Generate HTML content for the report"""
        html_template = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Validation Report</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .header {
            text-align: center;
            margin-bottom: 30px;
            padding-bottom: 20px;
            border-bottom: 2px solid #4472C4;
        }
        .header h1 {
            color: #4472C4;
            margin: 0;
        }
        .summary {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .summary-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            text-align: center;
        }
        .summary-card h3 {
            margin: 0 0 10px 0;
            font-size: 2em;
        }
        .summary-card p {
            margin: 0;
            font-size: 1.1em;
        }
        .pass { background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); }
        .fail { background: linear-gradient(135deg, #fc466b 0%, #3f5efb 100%); }
        .error { background: linear-gradient(135deg, #FDBB2D 0%, #22C1C3 100%); }
        
        .section {
            margin-bottom: 30px;
        }
        .section h2 {
            color: #333;
            border-bottom: 2px solid #4472C4;
            padding-bottom: 10px;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 15px;
        }
        th, td {
            padding: 12px;
            text-align: left;
            border: 1px solid #ddd;
        }
        th {
            background-color: #4472C4;
            color: white;
            font-weight: bold;
        }
        tr:nth-child(even) {
            background-color: #f9f9f9;
        }
        .status-pass { color: #006100; font-weight: bold; }
        .status-fail { color: #9C0006; font-weight: bold; }
        .status-error { color: #9C5700; font-weight: bold; }
        
        .progress-bar {
            width: 100%;
            height: 20px;
            background-color: #f0f0f0;
            border-radius: 10px;
            overflow: hidden;
            margin: 10px 0;
        }
        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, #11998e 0%, #38ef7d 100%);
            transition: width 0.3s ease;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Data Validation Report</h1>
            <p>Generated on: {{ report_data.report_generated_at.strftime('%Y-%m-%d %H:%M:%S') }}</p>
        </div>
        
        <div class="summary">
            <div class="summary-card">
                <h3>{{ report_data.total_tests }}</h3>
                <p>Total Tests</p>
            </div>
            <div class="summary-card pass">
                <h3>{{ report_data.passed_tests }}</h3>
                <p>Passed</p>
            </div>
            <div class="summary-card fail">
                <h3>{{ report_data.failed_tests }}</h3>
                <p>Failed</p>
            </div>
            <div class="summary-card error">
                <h3>{{ report_data.error_tests }}</h3>
                <p>Errors</p>
            </div>
        </div>
        
        <div class="section">
            <h2>Overall Success Rate</h2>
            <div class="progress-bar">
                <div class="progress-fill" style="width: {{ report_data.success_rate }}%"></div>
            </div>
            <p><strong>{{ report_data.success_rate }}%</strong> of tests passed successfully</p>
        </div>
        
        <div class="section">
            <h2>Results by Validation Type</h2>
            <table>
                <thead>
                    <tr>
                        <th>Validation Type</th>
                        <th>Total</th>
                        <th>Passed</th>
                        <th>Failed</th>
                        <th>Errors</th>
                        <th>Success Rate</th>
                    </tr>
                </thead>
                <tbody>
                    {% for val_type, stats in report_data.validation_types.items() %}
                    <tr>
                        <td>{{ val_type }}</td>
                        <td>{{ stats.total }}</td>
                        <td class="status-pass">{{ stats.passed }}</td>
                        <td class="status-fail">{{ stats.failed }}</td>
                        <td class="status-error">{{ stats.errors }}</td>
                        <td>{{ "%.1f"|format((stats.passed / stats.total * 100) if stats.total > 0 else 0) }}%</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
        
        <div class="section">
            <h2>Results by Feed</h2>
            <table>
                <thead>
                    <tr>
                        <th>Feed Name</th>
                        <th>Total</th>
                        <th>Passed</th>
                        <th>Failed</th>
                        <th>Errors</th>
                        <th>Success Rate</th>
                    </tr>
                </thead>
                <tbody>
                    {% for feed_name, stats in report_data.feed_results.items() %}
                    <tr>
                        <td>{{ feed_name }}</td>
                        <td>{{ stats.total }}</td>
                        <td class="status-pass">{{ stats.passed }}</td>
                        <td class="status-fail">{{ stats.failed }}</td>
                        <td class="status-error">{{ stats.errors }}</td>
                        <td>{{ "%.1f"|format((stats.passed / stats.total * 100) if stats.total > 0 else 0) }}%</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
    </div>
</body>
</html>
        """
        
        template = Template(html_template)
        return template.render(report_data=report_data)
    
    def _create_summary_worksheet(self, workbook, report_data, header_format, pass_format, fail_format, error_format):
        """Create summary worksheet in Excel report"""
        worksheet = workbook.add_worksheet('Summary')
        
        # Write summary data
        worksheet.write('A1', 'Data Validation Report Summary', header_format)
        worksheet.write('A2', f"Generated on: {report_data['report_generated_at']}")
        
        # Overall statistics
        worksheet.write('A4', 'Overall Statistics', header_format)
        worksheet.write('A5', 'Total Tests')
        worksheet.write('B5', report_data['total_tests'])
        worksheet.write('A6', 'Passed Tests')
        worksheet.write('B6', report_data['passed_tests'], pass_format)
        worksheet.write('A7', 'Failed Tests')
        worksheet.write('B7', report_data['failed_tests'], fail_format)
        worksheet.write('A8', 'Error Tests')
        worksheet.write('B8', report_data['error_tests'], error_format)
        worksheet.write('A9', 'Success Rate')
        worksheet.write('B9', f"{report_data['success_rate']}%")
        
        # Validation types summary
        row = 11
        worksheet.write(f'A{row}', 'Results by Validation Type', header_format)
        row += 1
        
        headers = ['Validation Type', 'Total', 'Passed', 'Failed', 'Errors', 'Success Rate']
        for col, header in enumerate(headers):
            worksheet.write(row, col, header, header_format)
        row += 1
        
        for val_type, stats in report_data['validation_types'].items():
            worksheet.write(row, 0, val_type)
            worksheet.write(row, 1, stats['total'])
            worksheet.write(row, 2, stats['passed'], pass_format)
            worksheet.write(row, 3, stats['failed'], fail_format)
            worksheet.write(row, 4, stats['errors'], error_format)
            success_rate = (stats['passed'] / stats['total'] * 100) if stats['total'] > 0 else 0
            worksheet.write(row, 5, f"{success_rate:.1f}%")
            row += 1
        
        # Feed results summary
        row += 2
        worksheet.write(f'A{row}', 'Results by Feed', header_format)
        row += 1
        
        headers = ['Feed Name', 'Total', 'Passed', 'Failed', 'Errors', 'Success Rate']
        for col, header in enumerate(headers):
            worksheet.write(row, col, header, header_format)
        row += 1
        
        for feed_name, stats in report_data['feed_results'].items():
            worksheet.write(row, 0, feed_name)
            worksheet.write(row, 1, stats['total'])
            worksheet.write(row, 2, stats['passed'], pass_format)
            worksheet.write(row, 3, stats['failed'], fail_format)
            worksheet.write(row, 4, stats['errors'], error_format)
            success_rate = (stats['passed'] / stats['total'] * 100) if stats['total'] > 0 else 0
            worksheet.write(row, 5, f"{success_rate:.1f}%")
            row += 1
        
        # Auto-adjust column widths
        worksheet.set_column('A:F', 20)
    
    def _create_detailed_worksheets(self, workbook, test_results, header_format, pass_format, fail_format, error_format):
        """Create detailed results worksheets in Excel report"""
        # Group results by validation type
        validation_groups = {}
        for result in test_results:
            val_type = result.get('validation_type', 'Unknown')
            if val_type not in validation_groups:
                validation_groups[val_type] = []
            validation_groups[val_type].append(result)
        
        # Create worksheet for each validation type
        for val_type, results in validation_groups.items():
            # Clean worksheet name (Excel has restrictions)
            sheet_name = val_type.replace('_', ' ').title()[:31]
            worksheet = workbook.add_worksheet(sheet_name)
            
            # Write headers
            headers = self._get_headers_for_validation_type(val_type)
            for col, header in enumerate(headers):
                worksheet.write(0, col, header, header_format)
            
            # Write data
            for row, result in enumerate(results, start=1):
                for col, header in enumerate(headers):
                    value = result.get(header.lower().replace(' ', '_'), '')
                    
                    # Apply formatting based on status
                    if header == 'Validation Status':
                        if value == 'PASS':
                            worksheet.write(row, col, value, pass_format)
                        elif value == 'FAIL':
                            worksheet.write(row, col, value, fail_format)
                        else:
                            worksheet.write(row, col, value, error_format)
                    else:
                        worksheet.write(row, col, str(value))
            
            # Auto-adjust column widths
            worksheet.set_column(0, len(headers)-1, 15)
    
    def _get_headers_for_validation_type(self, validation_type: str) -> List[str]:
        """Get appropriate headers for each validation type"""
        common_headers = ['Feed Name', 'DB Name', 'Table Name', 'Validation Status', 'Error Message']
        
        type_specific_headers = {
            'data_type': ['Column Name', 'Expected Type'],
            'nullable_constraint': ['Column Name', 'Expected Nullable', 'Null Count'],
            'unique_constraint': ['Column Name', 'Total Count', 'Distinct Count', 'Duplicate Count'],
            'range_constraint': ['Column Name', 'Range Bottom', 'Range Top', 'Below Range Count', 'Above Range Count'],
            'enumeration_constraint': ['Column Name', 'Enumeration Name', 'Invalid Count'],
            'count_check': ['Actual Count', 'Expected Count'],
            'completeness_check': ['Total Rows', 'Overall Completeness Score'],
            'file_availability': ['File Pattern', 'File Count', 'Latest File'],
            'autosys_job': ['Job Name', 'Current Status', 'Expected Status']
        }
        
        specific_headers = type_specific_headers.get(validation_type, [])
        return specific_headers + common_headers
    
    def capture_screenshot(self, screenshot_name: str, element_selector: Optional[str] = None) -> str:
        """
        Capture screenshot for report (placeholder implementation)
        
        Args:
            screenshot_name: Name of the screenshot
            element_selector: CSS selector for specific element (optional)
        
        Returns:
            Path to captured screenshot
        """
        try:
            # This is a placeholder implementation
            # In real scenario, this would use Selenium or similar tool
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_filename = f"{screenshot_name}_{timestamp}.png"
            screenshot_filepath = self.screenshot_path / screenshot_filename
            
            # Create a placeholder image file
            with open(screenshot_filepath, 'w') as f:
                f.write("Placeholder screenshot file")
            
            framework_logger.info(f"Screenshot captured: {screenshot_filepath}")
            return str(screenshot_filepath)
            
        except Exception as e:
            error_msg = f"Error capturing screenshot: {str(e)}"
            framework_logger.error(error_msg)
            return ""
    
    def generate_comprehensive_report(self, test_results: List[Dict[str, Any]], 
                                    report_name: str = "comprehensive_validation_report") -> Dict[str, str]:
        """
        Generate comprehensive report in multiple formats
        
        Args:
            test_results: List of test validation results
            report_name: Name of the report
        
        Returns:
            Dictionary with paths to generated reports
        """
        try:
            framework_logger.info(f"Generating comprehensive report: {report_name}")
            
            report_paths = {}
            
            # Generate HTML report
            html_path = self.generate_html_report(test_results, report_name)
            report_paths['html'] = html_path
            
            # Generate Excel report
            excel_path = self.generate_excel_report(test_results, report_name)
            report_paths['excel'] = excel_path
            
            # Generate JSON report
            json_path = self.generate_json_report(test_results, report_name)
            report_paths['json'] = json_path
            
            framework_logger.info(f"Comprehensive report generated successfully")
            return report_paths
            
        except Exception as e:
            error_msg = f"Error generating comprehensive report: {str(e)}"
            framework_logger.error(error_msg)
            raise

# Global report generator instance
report_generator = ReportGenerator()
