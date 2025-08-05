"""
Data Quality Validation System for YouNiverse Dataset Enrichment

This module implements comprehensive data quality validation including:
- Junction point validation for data source transitions
- Statistical progression validation and outlier detection  
- Cross-source data consistency checks
- Weekly temporal validation and granularity enforcement
- Data quality flag assignment logic

Author: data-analytics-expert
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Union, Any
from dataclasses import dataclass
from enum import Enum
import json
from scipy import stats
from sklearn.preprocessing import StandardScaler

from ..core.config import Config
from ..core.exceptions import ValidationError, DataProcessingError
from ..utils.data_utils import DataUtils
from ..utils.date_utils import DateUtils


class ValidationSeverity(Enum):
    """Validation error severity levels"""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


class ValidationCategory(Enum):
    """Categories of validation checks"""
    TEMPORAL = "temporal"
    STATISTICAL = "statistical"
    COMPLETENESS = "completeness"
    CONSISTENCY = "consistency"
    JUNCTION_POINT = "junction_point"
    SCHEMA = "schema"


@dataclass
class ValidationRule:
    """Individual validation rule configuration"""
    name: str
    category: ValidationCategory
    severity: ValidationSeverity
    enabled: bool = True
    parameters: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.parameters is None:
            self.parameters = {}


@dataclass
class ValidationResult:
    """Result of a validation check"""
    rule_name: str
    passed: bool
    severity: ValidationSeverity
    message: str
    affected_records: int = 0
    affected_channels: List[str] = None
    details: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.affected_channels is None:
            self.affected_channels = []
        if self.details is None:
            self.details = {}


@dataclass
class QualityMetrics:
    """Data quality metrics summary"""
    total_records: int = 0
    valid_records: int = 0
    quality_score: float = 0.0
    completeness_score: float = 0.0
    consistency_score: float = 0.0
    temporal_score: float = 0.0
    validation_errors: int = 0
    critical_errors: int = 0
    quality_flags: Dict[str, int] = None
    
    def __post_init__(self):
        if self.quality_flags is None:
            self.quality_flags = {}


class DataValidator:
    """
    Comprehensive data quality validation system
    
    Implements multi-layered validation approach:
    1. Schema and data type validation
    2. Temporal continuity and granularity validation
    3. Statistical progression and outlier detection
    4. Junction point validation for data source transitions
    5. Cross-source consistency checks
    6. Data completeness assessment
    """
    
    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.data_utils = DataUtils()
        self.date_utils = DateUtils()
        
        # Load validation rules from configuration
        self.validation_rules = self._load_validation_rules()
        
        # Quality thresholds from configuration
        self.quality_thresholds = self._load_quality_thresholds()
        
        # Initialize validation statistics
        self.metrics = QualityMetrics()
        self.validation_results: List[ValidationResult] = []
        
    def validate_dataset(self, df: pd.DataFrame, 
                        validation_level: str = "comprehensive") -> Tuple[pd.DataFrame, QualityMetrics]:
        """
        Perform comprehensive dataset validation
        
        Args:
            df: Dataset to validate
            validation_level: "basic", "standard", or "comprehensive"
            
        Returns:
            Tuple of (validated_dataframe, quality_metrics)
        """
        self.logger.info(f"Starting {validation_level} validation on {len(df)} records")
        start_time = datetime.now()
        
        # Reset validation state
        self.validation_results = []
        self.metrics = QualityMetrics(total_records=len(df))
        
        validated_df = df.copy()
        
        try:
            # Phase 1: Schema validation
            validated_df = self._validate_schema(validated_df)
            
            # Phase 2: Temporal validation
            validated_df = self._validate_temporal_consistency(validated_df)
            
            # Phase 3: Statistical validation
            if validation_level in ["standard", "comprehensive"]:
                validated_df = self._validate_statistical_progression(validated_df)
            
            # Phase 4: Junction point validation
            if validation_level == "comprehensive":
                validated_df = self._validate_junction_points(validated_df)
            
            # Phase 5: Cross-source consistency
            if validation_level == "comprehensive":
                validated_df = self._validate_cross_source_consistency(validated_df)
            
            # Phase 6: Completeness validation
            validated_df = self._validate_completeness(validated_df)
            
            # Phase 7: Calculate final quality metrics
            self._calculate_quality_metrics(validated_df)
            
            validation_time = (datetime.now() - start_time).total_seconds()
            self.logger.info(f"Validation completed in {validation_time:.2f} seconds")
            self.logger.info(f"Quality score: {self.metrics.quality_score:.3f}")
            
            return validated_df, self.metrics
            
        except Exception as e:
            self.logger.error(f"Validation failed: {str(e)}")
            raise ValidationError(f"Dataset validation failed: {str(e)}")
    
    def _load_validation_rules(self) -> Dict[str, ValidationRule]:
        """Load validation rules from configuration"""
        rules = {}
        
        # Temporal validation rules
        rules["weekly_granularity"] = ValidationRule(
            name="weekly_granularity",
            category=ValidationCategory.TEMPORAL,
            severity=ValidationSeverity.HIGH,
            parameters={
                "tolerance_days": 3,
                "expected_interval_days": 7
            }
        )
        
        rules["date_range"] = ValidationRule(
            name="date_range",
            category=ValidationCategory.TEMPORAL,
            severity=ValidationSeverity.CRITICAL,
            parameters={
                "min_date": "2015-01-01",
                "max_date": "2025-12-31"
            }
        )
        
        # Statistical validation rules
        rules["growth_rate_limits"] = ValidationRule(
            name="growth_rate_limits",
            category=ValidationCategory.STATISTICAL,
            severity=ValidationSeverity.MEDIUM,
            parameters={
                "max_subscriber_growth_rate": 0.2,
                "max_view_growth_rate": 0.5,
                "max_video_uploads_per_week": 50
            }
        )
        
        rules["outlier_detection"] = ValidationRule(
            name="outlier_detection",
            category=ValidationCategory.STATISTICAL,
            severity=ValidationSeverity.LOW,
            parameters={
                "z_score_threshold": 3.0,
                "iqr_multiplier": 1.5
            }
        )
        
        # Junction point validation rules
        rules["junction_continuity"] = ValidationRule(
            name="junction_continuity",
            category=ValidationCategory.JUNCTION_POINT,
            severity=ValidationSeverity.HIGH,
            parameters={
                "september_2019_tolerance": 15.0,
                "august_2022_tolerance": 10.0,
                "overlap_required": True
            }
        )
        
        # Completeness validation rules
        rules["required_fields"] = ValidationRule(
            name="required_fields",
            category=ValidationCategory.COMPLETENESS,
            severity=ValidationSeverity.CRITICAL,
            parameters={
                "critical_fields": ["channel", "datetime", "views", "subs"],
                "important_fields": ["category", "videos"],
                "optional_fields": ["channel_name", "total_longform_views"]
            }
        )
        
        return rules
    
    def _load_quality_thresholds(self) -> Dict[str, float]:
        """Load quality thresholds from configuration"""
        return {
            "excellent": 0.95,
            "good": 0.85,
            "acceptable": 0.75,
            "poor": 0.60,
            "critical": 0.50
        }
    
    def _validate_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate dataset schema and data types"""
        
        rule = self.validation_rules["required_fields"]
        if not rule.enabled:
            return df
        
        # Check for required fields
        critical_fields = rule.parameters["critical_fields"]
        important_fields = rule.parameters["important_fields"]
        
        missing_critical = set(critical_fields) - set(df.columns)
        missing_important = set(important_fields) - set(df.columns)
        
        if missing_critical:
            result = ValidationResult(
                rule_name="required_fields",
                passed=False,
                severity=ValidationSeverity.CRITICAL,
                message=f"Missing critical fields: {missing_critical}",
                details={"missing_fields": list(missing_critical)}
            )
            self.validation_results.append(result)
            raise ValidationError(f"Missing critical fields: {missing_critical}")
        
        if missing_important:
            result = ValidationResult(
                rule_name="required_fields",
                passed=False,
                severity=ValidationSeverity.HIGH,
                message=f"Missing important fields: {missing_important}",
                details={"missing_fields": list(missing_important)}
            )
            self.validation_results.append(result)
        
        # Validate data types
        type_errors = 0
        
        # Numeric fields should be numeric
        numeric_fields = ["views", "delta_views", "subs", "delta_subs", "videos", "delta_videos"]
        for field in numeric_fields:
            if field in df.columns:
                non_numeric = pd.to_numeric(df[field], errors='coerce').isna().sum()
                if non_numeric > 0:
                    type_errors += non_numeric
                    df[field] = pd.to_numeric(df[field], errors='coerce').fillna(0)
        
        # Datetime field should be datetime
        if 'datetime' in df.columns:
            invalid_dates = pd.to_datetime(df['datetime'], errors='coerce').isna().sum()
            if invalid_dates > 0:
                type_errors += invalid_dates
                df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
        
        if type_errors > 0:
            result = ValidationResult(
                rule_name="data_types",
                passed=False,
                severity=ValidationSeverity.MEDIUM,
                message=f"Fixed {type_errors} data type issues",
                affected_records=type_errors
            )
            self.validation_results.append(result)
        
        return df
    
    def _validate_temporal_consistency(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate temporal consistency and weekly granularity"""
        
        # Weekly granularity validation
        granularity_rule = self.validation_rules["weekly_granularity"]
        if granularity_rule.enabled:
            df = self._check_weekly_granularity(df, granularity_rule)
        
        # Date range validation
        date_range_rule = self.validation_rules["date_range"]
        if date_range_rule.enabled:
            df = self._check_date_range(df, date_range_rule)
        
        return df
    
    def _check_weekly_granularity(self, df: pd.DataFrame, rule: ValidationRule) -> pd.DataFrame:
        """Check that records follow weekly granularity"""
        
        tolerance_days = rule.parameters["tolerance_days"]
        violations = 0
        
        for channel_id in df['channel'].unique():
            channel_data = df[df['channel'] == channel_id].sort_values('datetime')
            
            if len(channel_data) > 1:
                date_diffs = channel_data['datetime'].diff().dt.days
                # Skip first NaN value
                date_diffs = date_diffs.dropna()
                
                # Check for deviations from 7-day intervals
                non_weekly = date_diffs[(date_diffs < 7 - tolerance_days) | 
                                      (date_diffs > 7 + tolerance_days)]
                
                if len(non_weekly) > 0:
                    violations += len(non_weekly)
        
        if violations > 0:
            result = ValidationResult(
                rule_name="weekly_granularity",
                passed=False,
                severity=rule.severity,
                message=f"Found {violations} records violating weekly granularity",
                affected_records=violations
            )
            self.validation_results.append(result)
        
        return df
    
    def _check_date_range(self, df: pd.DataFrame, rule: ValidationRule) -> pd.DataFrame:
        """Check that all dates fall within expected range"""
        
        min_date = pd.to_datetime(rule.parameters["min_date"])
        max_date = pd.to_datetime(rule.parameters["max_date"])
        
        out_of_range = (df['datetime'] < min_date) | (df['datetime'] > max_date)
        violations = out_of_range.sum()
        
        if violations > 0:
            result = ValidationResult(
                rule_name="date_range",
                passed=False,
                severity=rule.severity,
                message=f"Found {violations} records with dates outside valid range",
                affected_records=violations,
                details={
                    "min_date": min_date.isoformat(),
                    "max_date": max_date.isoformat(),
                    "earliest_found": df['datetime'].min().isoformat(),
                    "latest_found": df['datetime'].max().isoformat()
                }
            )
            self.validation_results.append(result)
        
        return df
    
    def _validate_statistical_progression(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate statistical progression patterns and detect outliers"""
        
        # Growth rate validation
        growth_rule = self.validation_rules["growth_rate_limits"]
        if growth_rule.enabled:
            df = self._check_growth_rates(df, growth_rule)
        
        # Outlier detection
        outlier_rule = self.validation_rules["outlier_detection"]
        if outlier_rule.enabled:
            df = self._detect_statistical_outliers(df, outlier_rule)
        
        return df
    
    def _check_growth_rates(self, df: pd.DataFrame, rule: ValidationRule) -> pd.DataFrame:
        """Check for unrealistic growth rates"""
        
        max_sub_growth = rule.parameters["max_subscriber_growth_rate"]
        max_view_growth = rule.parameters["max_view_growth_rate"]
        max_video_uploads = rule.parameters["max_video_uploads_per_week"]
        
        violations = 0
        
        for channel_id in df['channel'].unique():
            channel_data = df[df['channel'] == channel_id].sort_values('datetime')
            
            if len(channel_data) > 1:
                # Check subscriber growth rate
                sub_growth_rates = channel_data['delta_subs'] / (channel_data['subs'].shift(1) + 1)
                excessive_sub_growth = sub_growth_rates > max_sub_growth
                violations += excessive_sub_growth.sum()
                
                # Check view growth rate
                view_growth_rates = channel_data['delta_views'] / (channel_data['views'].shift(1) + 1)
                excessive_view_growth = view_growth_rates > max_view_growth
                violations += excessive_view_growth.sum()
                
                # Check video upload rate
                excessive_uploads = channel_data['delta_videos'] > max_video_uploads
                violations += excessive_uploads.sum()
        
        if violations > 0:
            result = ValidationResult(
                rule_name="growth_rate_limits",
                passed=False,
                severity=rule.severity,
                message=f"Found {violations} records with excessive growth rates",
                affected_records=violations
            )
            self.validation_results.append(result)
        
        return df
    
    def _detect_statistical_outliers(self, df: pd.DataFrame, rule: ValidationRule) -> pd.DataFrame:
        """Detect statistical outliers using Z-score and IQR methods"""
        
        z_threshold = rule.parameters["z_score_threshold"]
        iqr_multiplier = rule.parameters["iqr_multiplier"]
        
        outliers_detected = 0
        
        for channel_id in df['channel'].unique():
            channel_data = df[df['channel'] == channel_id].copy()
            
            if len(channel_data) < 4:  # Need minimum data for statistical analysis
                continue
            
            # Detect outliers in delta metrics using Z-score
            for metric in ['delta_views', 'delta_subs']:
                if metric in channel_data.columns:
                    values = channel_data[metric].values
                    z_scores = np.abs(stats.zscore(values))
                    outliers = z_scores > z_threshold
                    
                    if outliers.any():
                        outlier_indices = channel_data[outliers].index
                        df.loc[outlier_indices, 'data_quality_flag'] = 'fair'
                        outliers_detected += outliers.sum()
            
            # Detect outliers using IQR method
            for metric in ['views', 'subs', 'videos']:
                if metric in channel_data.columns:
                    Q1 = channel_data[metric].quantile(0.25)
                    Q3 = channel_data[metric].quantile(0.75)
                    IQR = Q3 - Q1
                    lower_bound = Q1 - iqr_multiplier * IQR
                    upper_bound = Q3 + iqr_multiplier * IQR
                    
                    outliers = (channel_data[metric] < lower_bound) | (channel_data[metric] > upper_bound)
                    
                    if outliers.any():
                        outlier_indices = channel_data[outliers].index
                        df.loc[outlier_indices, 'data_quality_flag'] = 'fair'
                        outliers_detected += outliers.sum()
        
        if outliers_detected > 0:
            result = ValidationResult(
                rule_name="outlier_detection",
                passed=False,
                severity=rule.severity,
                message=f"Detected {outliers_detected} statistical outliers",
                affected_records=outliers_detected
            )
            self.validation_results.append(result)
        
        return df
    
    def _validate_junction_points(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate continuity at critical junction points"""
        
        rule = self.validation_rules["junction_continuity"]
        if not rule.enabled:
            return df
        
        # Define junction points
        junction_points = [
            {
                'name': 'September 2019',
                'date': datetime(2019, 9, 1),
                'tolerance': rule.parameters["september_2019_tolerance"]
            },
            {
                'name': 'August 2022', 
                'date': datetime(2022, 8, 1),
                'tolerance': rule.parameters["august_2022_tolerance"]
            }
        ]
        
        total_violations = 0
        
        for junction in junction_points:
            violations = self._check_junction_continuity(df, junction)
            total_violations += violations
        
        if total_violations > 0:
            result = ValidationResult(
                rule_name="junction_continuity",
                passed=False,
                severity=rule.severity,
                message=f"Found {total_violations} junction point violations",
                affected_records=total_violations
            )
            self.validation_results.append(result)
        
        return df
    
    def _check_junction_continuity(self, df: pd.DataFrame, junction: Dict) -> int:
        """Check continuity at a specific junction point"""
        
        junction_date = junction['date']
        tolerance_pct = junction['tolerance']
        
        # Define window around junction point (±2 weeks)
        window_start = junction_date - timedelta(weeks=2)
        window_end = junction_date + timedelta(weeks=2)
        
        junction_records = df[
            (df['datetime'] >= window_start) & 
            (df['datetime'] <= window_end)
        ]
        
        violations = 0
        
        for channel_id in junction_records['channel'].unique():
            channel_junction = junction_records[
                junction_records['channel'] == channel_id
            ].sort_values('datetime')
            
            if len(channel_junction) >= 2:
                # Check for reasonable continuity in key metrics
                for metric in ['subs', 'views']:
                    values = channel_junction[metric].values
                    
                    for i in range(1, len(values)):
                        if values[i-1] > 0:  # Avoid division by zero
                            delta_pct = abs(values[i] - values[i-1]) / values[i-1] * 100
                            
                            if delta_pct > tolerance_pct:
                                violations += 1
                                # Flag records with continuity issues
                                mask = (df['channel'] == channel_id) & \
                                       (df['datetime'] >= window_start) & \
                                       (df['datetime'] <= window_end)
                                df.loc[mask, 'data_quality_flag'] = 'fair'
        
        return violations
    
    def _validate_cross_source_consistency(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate consistency across different data sources"""
        
        source_inconsistencies = 0
        
        # Check for inconsistencies between adjacent records from different sources
        for channel_id in df['channel'].unique():
            channel_data = df[df['channel'] == channel_id].sort_values('datetime')
            
            if len(channel_data) > 1:
                for i in range(1, len(channel_data)):
                    current_record = channel_data.iloc[i]
                    previous_record = channel_data.iloc[i-1]
                    
                    # If sources are different, check for consistency
                    if current_record['collection_source'] != previous_record['collection_source']:
                        consistency_score = self._calculate_consistency_score(
                            previous_record, current_record
                        )
                        
                        if consistency_score < 0.8:  # 80% consistency threshold
                            source_inconsistencies += 1
                            df.loc[current_record.name, 'data_quality_flag'] = 'fair'
        
        if source_inconsistencies > 0:
            result = ValidationResult(
                rule_name="cross_source_consistency",
                passed=False,
                severity=ValidationSeverity.MEDIUM,
                message=f"Found {source_inconsistencies} cross-source inconsistencies",
                affected_records=source_inconsistencies
            )
            self.validation_results.append(result)
        
        return df
    
    def _calculate_consistency_score(self, record1: pd.Series, record2: pd.Series) -> float:
        """Calculate consistency score between two records"""
        
        # Calculate relative differences in key metrics
        metrics = ['subs', 'views', 'videos']
        differences = []
        
        for metric in metrics:
            if record1[metric] > 0:
                rel_diff = abs(record2[metric] - record1[metric]) / record1[metric]
                differences.append(min(rel_diff, 1.0))  # Cap at 100% difference
        
        # Return average consistency (1 - average_difference)
        if differences:
            return 1.0 - np.mean(differences)
        return 1.0
    
    def _validate_completeness(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate data completeness across required fields"""
        
        rule = self.validation_rules["required_fields"]
        if not rule.enabled:
            return df
        
        critical_fields = rule.parameters["critical_fields"]
        important_fields = rule.parameters["important_fields"]
        
        # Check completeness for critical fields
        critical_missing = 0
        for field in critical_fields:
            if field in df.columns:
                missing_count = df[field].isna().sum()
                critical_missing += missing_count
        
        # Check completeness for important fields
        important_missing = 0
        for field in important_fields:
            if field in df.columns:
                missing_count = df[field].isna().sum()
                important_missing += missing_count
        
        if critical_missing > 0:
            result = ValidationResult(
                rule_name="critical_completeness",
                passed=False,
                severity=ValidationSeverity.CRITICAL,
                message=f"Found {critical_missing} missing values in critical fields",
                affected_records=critical_missing
            )
            self.validation_results.append(result)
        
        if important_missing > 0:
            result = ValidationResult(
                rule_name="important_completeness",
                passed=False,
                severity=ValidationSeverity.HIGH,
                message=f"Found {important_missing} missing values in important fields",
                affected_records=important_missing
            )
            self.validation_results.append(result)
        
        return df
    
    def _calculate_quality_metrics(self, df: pd.DataFrame) -> None:
        """Calculate comprehensive quality metrics"""
        
        # Count validation errors by severity
        critical_errors = sum(1 for r in self.validation_results 
                            if r.severity == ValidationSeverity.CRITICAL and not r.passed)
        high_errors = sum(1 for r in self.validation_results 
                         if r.severity == ValidationSeverity.HIGH and not r.passed)
        medium_errors = sum(1 for r in self.validation_results 
                          if r.severity == ValidationSeverity.MEDIUM and not r.passed)
        low_errors = sum(1 for r in self.validation_results 
                        if r.severity == ValidationSeverity.LOW and not r.passed)
        
        self.metrics.validation_errors = critical_errors + high_errors + medium_errors + low_errors
        self.metrics.critical_errors = critical_errors
        
        # Calculate quality scores
        total_records = len(df)
        valid_records = total_records - sum(r.affected_records for r in self.validation_results 
                                          if not r.passed)
        
        self.metrics.valid_records = max(valid_records, 0)
        
        # Overall quality score (weighted by severity)
        error_penalty = (critical_errors * 0.4 + high_errors * 0.3 + 
                        medium_errors * 0.2 + low_errors * 0.1)
        max_penalty = total_records * 0.1  # Max 10% penalty
        normalized_penalty = min(error_penalty / total_records, 0.1)
        
        self.metrics.quality_score = max(1.0 - normalized_penalty, 0.0)
        
        # Completeness score
        total_fields = len([col for col in df.columns if col in 
                          ['channel', 'datetime', 'views', 'subs', 'videos', 'category']])
        non_null_ratio = df[['channel', 'datetime', 'views', 'subs']].notna().all(axis=1).mean()
        self.metrics.completeness_score = non_null_ratio
        
        # Consistency score (based on cross-source validation)
        consistency_errors = sum(r.affected_records for r in self.validation_results 
                               if r.rule_name == "cross_source_consistency" and not r.passed)
        self.metrics.consistency_score = max(1.0 - consistency_errors / total_records, 0.0)
        
        # Temporal score (based on granularity validation)
        temporal_errors = sum(r.affected_records for r in self.validation_results 
                            if r.rule_name in ["weekly_granularity", "date_range"] and not r.passed)
        self.metrics.temporal_score = max(1.0 - temporal_errors / total_records, 0.0)
        
        # Quality flags distribution
        if 'data_quality_flag' in df.columns:
            self.metrics.quality_flags = df['data_quality_flag'].value_counts().to_dict()
    
    def generate_validation_report(self, output_path: str = None) -> Dict[str, Any]:
        """Generate comprehensive validation report"""
        
        report = {
            "validation_summary": {
                "total_rules_checked": len(self.validation_rules),
                "rules_passed": sum(1 for r in self.validation_results if r.passed),
                "rules_failed": sum(1 for r in self.validation_results if not r.passed),
                "total_records_affected": sum(r.affected_records for r in self.validation_results if not r.passed)
            },
            "quality_metrics": {
                "overall_quality_score": self.metrics.quality_score,
                "completeness_score": self.metrics.completeness_score,
                "consistency_score": self.metrics.consistency_score,
                "temporal_score": self.metrics.temporal_score,
                "total_records": self.metrics.total_records,
                "valid_records": self.metrics.valid_records,
                "validation_errors": self.metrics.validation_errors,
                "critical_errors": self.metrics.critical_errors
            },
            "validation_results": [
                {
                    "rule_name": r.rule_name,
                    "passed": r.passed,
                    "severity": r.severity.value,
                    "message": r.message,
                    "affected_records": r.affected_records,
                    "affected_channels": len(r.affected_channels),
                    "details": r.details
                }
                for r in self.validation_results
            ],
            "quality_flags_distribution": self.metrics.quality_flags,
            "recommendations": self._generate_recommendations()
        }
        
        if output_path:
            with open(output_path, 'w') as f:
                json.dump(report, f, indent=2)
            self.logger.info(f"Validation report saved to: {output_path}")
        
        return report
    
    def _generate_recommendations(self) -> List[str]:
        """Generate recommendations based on validation results"""
        
        recommendations = []
        
        # Check for critical issues
        critical_results = [r for r in self.validation_results 
                          if r.severity == ValidationSeverity.CRITICAL and not r.passed]
        
        if critical_results:
            recommendations.append(
                "CRITICAL: Address missing required fields and data type issues immediately"
            )
        
        # Check quality score
        if self.metrics.quality_score < 0.7:
            recommendations.append(
                "Overall data quality is below acceptable threshold. Consider data cleaning."
            )
        
        # Check junction point issues
        junction_issues = [r for r in self.validation_results 
                         if r.rule_name == "junction_continuity" and not r.passed]
        
        if junction_issues:
            recommendations.append(
                "Junction point validation failed. Review data source transitions for continuity."
            )
        
        # Check temporal issues
        temporal_issues = [r for r in self.validation_results 
                         if r.rule_name == "weekly_granularity" and not r.passed]
        
        if temporal_issues:
            recommendations.append(
                "Weekly granularity violations detected. Consider temporal data normalization."
            )
        
        if not recommendations:
            recommendations.append("Data quality meets acceptable standards.")
        
        return recommendations