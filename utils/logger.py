"""
Centralized logging configuration for the testing framework
"""
import sys
from pathlib import Path
from loguru import logger
from typing import Optional

class TestLogger:
    """Centralized logger for the testing framework"""
    
    def __init__(self, log_level: str = "INFO", log_file: Optional[str] = None):
        self.log_level = log_level
        self.log_file = log_file
        self._setup_logger()
    
    def _setup_logger(self):
        """Setup logger configuration"""
        # Remove default logger
        logger.remove()
        
        # Console logger with colors
        logger.add(
            sys.stdout,
            level=self.log_level,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
            colorize=True
        )
        
        # File logger if specified
        if self.log_file:
            log_path = Path(self.log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            logger.add(
                self.log_file,
                level=self.log_level,
                format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
                rotation="10 MB",
                retention="7 days",
                compression="zip"
            )
    
    def get_logger(self, name: str = "TestFramework"):
        """Get logger instance with specified name"""
        return logger.bind(name=name)

# Global logger instance
test_logger = TestLogger(log_file="logs/test_framework.log")
framework_logger = test_logger.get_logger("Framework")
