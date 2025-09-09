"""
Configuration management for the metadata-driven testing framework
"""
import os
from typing import Dict, Any, Optional
from pydantic import BaseModel
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class DatabaseConfig(BaseModel):
    """Database configuration model"""
    server1_connection: str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=server1;DATABASE=database1;Trusted_Connection=yes;"
    server2_connection: str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=server2;DATABASE=database2;Trusted_Connection=yes;"
    connection_timeout: int = 30
    query_timeout: int = 300

class AutosysConfig(BaseModel):
    """Autosys configuration model"""
    environment: str = "DEV"
    command_path: str = "/opt/CA/WorkloadAutomationAE/bin/autorep"
    timeout: int = 60
    mock_mode: bool = True  # Set to False when real Autosys is available

class PathConfig(BaseModel):
    """File path configuration model"""
    metadata_file: str = "metadata/Metadata.xlsx"
    feed_base_path: str = "/data/feeds"
    log_base_path: str = "/data/logs"
    report_output_path: str = "reports"
    screenshot_path: str = "reports/screenshots"

class TestConfig(BaseModel):
    """Test execution configuration"""
    parallel_execution: bool = False
    max_workers: int = 4
    retry_count: int = 3
    retry_delay: int = 5
    generate_screenshots: bool = True
    detailed_logging: bool = True

class Config:
    """Main configuration class"""
    
    def __init__(self):
        self.database = DatabaseConfig()
        self.autosys = AutosysConfig()
        self.paths = PathConfig()
        self.test = TestConfig()
        
        # Override with environment variables if available
        self._load_env_overrides()
    
    def _load_env_overrides(self):
        """Load configuration overrides from environment variables"""
        # Database overrides
        if os.getenv("DB_SERVER1_CONNECTION"):
            self.database.server1_connection = os.getenv("DB_SERVER1_CONNECTION")
        if os.getenv("DB_SERVER2_CONNECTION"):
            self.database.server2_connection = os.getenv("DB_SERVER2_CONNECTION")
        
        # Autosys overrides
        if os.getenv("AUTOSYS_ENV"):
            self.autosys.environment = os.getenv("AUTOSYS_ENV")
        if os.getenv("AUTOSYS_MOCK_MODE"):
            self.autosys.mock_mode = os.getenv("AUTOSYS_MOCK_MODE").lower() == "true"
        
        # Path overrides
        if os.getenv("METADATA_FILE"):
            self.paths.metadata_file = os.getenv("METADATA_FILE")
        if os.getenv("FEED_BASE_PATH"):
            self.paths.feed_base_path = os.getenv("FEED_BASE_PATH")
        if os.getenv("LOG_BASE_PATH"):
            self.paths.log_base_path = os.getenv("LOG_BASE_PATH")
    
    def get_db_connection_string(self, db_name: str) -> str:
        """Get database connection string based on DB name"""
        if "server1" in db_name.lower():
            return self.database.server1_connection
        elif "server2" in db_name.lower():
            return self.database.server2_connection
        else:
            # Default to server1
            return self.database.server1_connection
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        return {
            "database": self.database.dict(),
            "autosys": self.autosys.dict(),
            "paths": self.paths.dict(),
            "test": self.test.dict()
        }

# Global configuration instance
config = Config()
