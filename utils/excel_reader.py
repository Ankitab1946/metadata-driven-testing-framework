"""
Excel metadata reader for parsing Metadata.xlsx file
"""
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from utils.logger import framework_logger

@dataclass
class FeedMetadata:
    """Feed to staging metadata structure"""
    modules: str
    feed: str  # This will store the raw feed string (possibly multiple paths)
    feed_list: list  # New field to store parsed list of feed file paths
    field_name: str
    db_name: str
    db_table: str
    data_type: str
    nullable: str
    request: str
    default: Optional[str]
    enumeration: Optional[str]
    range_bottom: Optional[str]
    range_top: Optional[str]
    mandatory: str
    unique: str


@dataclass
class StagingMetadata:
    """Staging to GRI metadata structure"""
    modules: str
    stg_db_name: str
    stg_db_table: str
    where_clause_stg: Optional[str]
    stg_field_name: str
    trg_db_name: str
    trg_db_table: str
    where_clause_trg: Optional[str]
    trg_field_name: str
    trg_data_type: str
    nullable: str
    request: str
    default: Optional[str]
    enumeration: Optional[str]
    range_bottom: Optional[str]
    range_top: Optional[str]
    mandatory: str
    unique: str

@dataclass
class EnumerationMetadata:
    """Enumeration metadata structure"""
    enumeration_name: str
    enum_values: str

@dataclass
class PatternMetadata:
    """Pattern metadata structure"""
    pattern_name: str
    columns: List[str]
    expected_patterns: List[Dict[str, Any]]

@dataclass
class ReconciliationMetadata:
    """Reconciliation metadata structure"""
    rec_type: str  # Inter, Intra, Mix
    rec_name: str
    source_feed: str
    source_table: str
    source_column: str
    target_feed: str
    target_table: str
    target_column: str
    operation: str  # SUM, COUNT, AVG, etc.
    tolerance: Optional[float]

class MetadataReader:
    """Excel metadata reader class"""
    
    def __init__(self, metadata_file_path: str):
        self.metadata_file_path = Path(metadata_file_path)
        self.feed_metadata: List[FeedMetadata] = []
        self.staging_metadata: List[StagingMetadata] = []
        self.enumeration_metadata: List[EnumerationMetadata] = []
        self.pattern_metadata: List[PatternMetadata] = []
        self.reconciliation_metadata: List[ReconciliationMetadata] = []
        
        if not self.metadata_file_path.exists():
            framework_logger.error(f"Metadata file not found: {self.metadata_file_path}")
            raise FileNotFoundError(f"Metadata file not found: {self.metadata_file_path}")
    
    def load_all_metadata(self) -> Dict[str, Any]:
        """Load all metadata from Excel file"""
        try:
            framework_logger.info(f"Loading metadata from: {self.metadata_file_path}")
            
            # Read all sheets
            excel_data = pd.read_excel(self.metadata_file_path, sheet_name=None, engine='openpyxl')
            
            # Load each sheet
            self._load_feed_metadata(excel_data.get('Feed_to_staging'))
            self._load_staging_metadata(excel_data.get('Staging_to_GRI'))
            self._load_enumeration_metadata(excel_data.get('Enumeration'))
            self._load_pattern_metadata(excel_data.get('Patterns'))
            self._load_reconciliation_metadata(excel_data.get('Reconciliations'))
            
            framework_logger.info("Successfully loaded all metadata")
            
            return {
                'feed_metadata': self.feed_metadata,
                'staging_metadata': self.staging_metadata,
                'enumeration_metadata': self.enumeration_metadata,
                'pattern_metadata': self.pattern_metadata,
                'reconciliation_metadata': self.reconciliation_metadata
            }
            
        except Exception as e:
            framework_logger.error(f"Error loading metadata: {str(e)}")
            raise
    
    def _load_feed_metadata(self, df: Optional[pd.DataFrame]):
        """Load Feed_to_staging sheet metadata"""
        if df is None:
            framework_logger.warning("Feed_to_staging sheet not found")
            return
        
        try:
            # Clean column names
            df.columns = df.columns.str.strip()
            
            import re
            
            for _, row in df.iterrows():
                if pd.isna(row.get('Modules')) or pd.isna(row.get('Feed')):
                    continue
                
                feed_raw = str(row.get('Feed', ''))
                # Split feed string by comma, pipe, semicolon
                feed_list = [f.strip() for f in re.split(r'[,\|;]+', feed_raw) if f.strip()]
                
                feed_meta = FeedMetadata(
                    modules=str(row.get('Modules', '')),
                    feed=feed_raw,
                    feed_list=feed_list,
                    field_name=str(row.get('FieldName', '')),
                    db_name=str(row.get('DBName', '')),
                    db_table=str(row.get('DB Table', '')),
                    data_type=str(row.get('DataType', '')),
                    nullable=str(row.get('Nullable', 'N')),
                    request=str(row.get('Request', 'Insert')),
                    default=str(row.get('Default', '')) if pd.notna(row.get('Default')) else None,
                    enumeration=str(row.get('Enumeration', '')) if pd.notna(row.get('Enumeration')) else None,
                    range_bottom=str(row.get('RangeBottom', '')) if pd.notna(row.get('RangeBottom')) else None,
                    range_top=str(row.get('RangeTop', '')) if pd.notna(row.get('RangeTop')) else None,
                    mandatory=str(row.get('Mandatory', 'N')),
                    unique=str(row.get('Unique', 'N'))
                )
                self.feed_metadata.append(feed_meta)
            
            framework_logger.info(f"Loaded {len(self.feed_metadata)} feed metadata records")
            
        except Exception as e:
            framework_logger.error(f"Error loading feed metadata: {str(e)}")
            raise
    
    def _load_staging_metadata(self, df: Optional[pd.DataFrame]):
        """Load Staging_to_GRI sheet metadata"""
        if df is None:
            framework_logger.warning("Staging_to_GRI sheet not found")
            return
        
        try:
            # Clean column names
            df.columns = df.columns.str.strip()
            
            for _, row in df.iterrows():
                if pd.isna(row.get('Modules')):
                    continue
                
                staging_meta = StagingMetadata(
                    modules=str(row.get('Modules', '')),
                    stg_db_name=str(row.get('Stg_DBName', '')),
                    stg_db_table=str(row.get('Stg_DB Table', '')),
                    stg_field_name=str(row.get('STG_FieldName', '')),
                    trg_db_name=str(row.get('Trg_DBName', '')),
                    trg_db_table=str(row.get('Trg _DB Table', '')),
                    trg_field_name=str(row.get('Trg _FieldName', '')),
                    trg_data_type=str(row.get('Trg _DataType', '')),
                    nullable=str(row.get('Nullable', 'N')),
                    request=str(row.get('Request', 'Insert')),
                    default=str(row.get('Default', '')) if pd.notna(row.get('Default')) else None,
                    enumeration=str(row.get('Enumeration', '')) if pd.notna(row.get('Enumeration')) else None,
                    range_bottom=str(row.get('RangeBottom', '')) if pd.notna(row.get('RangeBottom')) else None,
                    range_top=str(row.get('RangeTop', '')) if pd.notna(row.get('RangeTop')) else None,
                    mandatory=str(row.get('Mandatory', 'N')),
                    unique=str(row.get('Unique', 'N'))
                )
                self.staging_metadata.append(staging_meta)
            
            framework_logger.info(f"Loaded {len(self.staging_metadata)} staging metadata records")
            
        except Exception as e:
            framework_logger.error(f"Error loading staging metadata: {str(e)}")
            raise
    
    def _load_enumeration_metadata(self, df: Optional[pd.DataFrame]):
        """Load Enumeration sheet metadata"""
        if df is None:
            framework_logger.warning("Enumeration sheet not found")
            return
        
        try:
            # Clean column names
            df.columns = df.columns.str.strip()
            
            for _, row in df.iterrows():
                if pd.isna(row.get('EnumerationName')):
                    continue
                
                enum_meta = EnumerationMetadata(
                    enumeration_name=str(row.get('EnumerationName', '')),
                    enum_values=str(row.get('EnumValues', ''))
                )
                self.enumeration_metadata.append(enum_meta)
            
            framework_logger.info(f"Loaded {len(self.enumeration_metadata)} enumeration metadata records")
            
        except Exception as e:
            framework_logger.error(f"Error loading enumeration metadata: {str(e)}")
            raise
    
    def _load_pattern_metadata(self, df: Optional[pd.DataFrame]):
        """Load Patterns sheet metadata"""
        if df is None:
            framework_logger.warning("Patterns sheet not found")
            return
        
        try:
            # This is a simplified implementation
            # In real scenario, pattern structure might be more complex
            framework_logger.info("Pattern metadata loading - placeholder implementation")
            
        except Exception as e:
            framework_logger.error(f"Error loading pattern metadata: {str(e)}")
            raise
    
    def _load_reconciliation_metadata(self, df: Optional[pd.DataFrame]):
        """Load Reconciliations sheet metadata"""
        if df is None:
            framework_logger.warning("Reconciliations sheet not found")
            return
        
        try:
            # This is a simplified implementation
            # In real scenario, reconciliation structure might be more complex
            framework_logger.info("Reconciliation metadata loading - placeholder implementation")
            
        except Exception as e:
            framework_logger.error(f"Error loading reconciliation metadata: {str(e)}")
            raise
    
    def get_feed_metadata_by_feed(self, feed_name: str) -> List[FeedMetadata]:
        """Get feed metadata for specific feed"""
        return [meta for meta in self.feed_metadata if meta.feed == feed_name]
    
    def get_staging_metadata_by_module(self, module_name: str) -> List[StagingMetadata]:
        """Get staging metadata for specific module"""
        return [meta for meta in self.staging_metadata if meta.modules == module_name]
    
    def get_enumeration_values(self, enum_name: str) -> List[str]:
        """Get enumeration values for specific enumeration name"""
        values = []
        for meta in self.enumeration_metadata:
            if meta.enumeration_name == enum_name:
                values.append(meta.enum_values)
        return values
    
    def get_unique_feeds(self) -> List[str]:
        """Get list of unique feeds"""
        return list(set([meta.feed for meta in self.feed_metadata]))
    
    def get_unique_modules(self) -> List[str]:
        """Get list of unique modules"""
        modules = set()
        modules.update([meta.modules for meta in self.feed_metadata])
        modules.update([meta.modules for meta in self.staging_metadata])
        return list(modules)
