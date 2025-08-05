"""
Processing Metadata Model for YouNiverse Dataset Enrichment

This module defines comprehensive metadata tracking for the 5-phase data
collection and processing pipeline, enabling full traceability, quality
assessment, and performance monitoring.

Author: data-analytics-expert
"""

from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Dict, List, Optional, Any, Union
from enum import Enum
import json


class ProcessingPhase(Enum):
    """Processing phase enumeration"""
    PHASE_1_API_METADATA = "phase_1_api_metadata"
    PHASE_2A_WAYBACK_CHARTS = "phase_2a_wayback_charts"
    PHASE_2B_WAYBACK_UPLOADS = "phase_2b_wayback_uploads"
    PHASE_3_VIEWSTATS = "phase_3_viewstats"
    PHASE_4_FINAL_API = "phase_4_final_api"
    PHASE_5_INTEGRATION = "phase_5_integration"


class ProcessingStatus(Enum):
    """Processing status enumeration"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    SKIPPED = "skipped"


class DataSource(Enum):
    """Data source enumeration"""
    YOUTUBE_API = "youtube_api"
    WAYBACK_MACHINE = "wayback_machine"
    VIEWSTATS_API = "viewstats_api"
    SOCIAL_BLADE = "social_blade"
    INTERPOLATION = "interpolation"
    MANUAL_CORRECTION = "manual_correction"


@dataclass
class SourceMetadata:
    """Metadata for individual data source"""
    source: DataSource
    url: Optional[str] = None
    timestamp: Optional[datetime] = None
    response_code: Optional[int] = None
    data_points_collected: int = 0
    success_rate: float = 0.0
    error_messages: List[str] = field(default_factory=list)
    rate_limit_info: Dict[str, Any] = field(default_factory=dict)
    snapshot_info: Dict[str, Any] = field(default_factory=dict)
    
    def add_error(self, error_message: str):
        """Add error message to source metadata"""
        if error_message not in self.error_messages:
            self.error_messages.append(error_message)
    
    def update_success_rate(self, successful_requests: int, total_requests: int):
        """Update success rate based on request statistics"""
        if total_requests > 0:
            self.success_rate = successful_requests / total_requests


@dataclass
class PhaseMetrics:
    """Metrics for individual processing phase"""
    phase: ProcessingPhase
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    status: ProcessingStatus = ProcessingStatus.PENDING
    channels_processed: int = 0
    channels_successful: int = 0
    channels_failed: int = 0
    records_created: int = 0
    records_updated: int = 0
    records_validated: int = 0
    quality_scores: Dict[str, int] = field(default_factory=dict)
    error_summary: Dict[str, int] = field(default_factory=dict)
    performance_metrics: Dict[str, float] = field(default_factory=dict)
    
    def __post_init__(self):
        if not self.quality_scores:
            self.quality_scores = {
                "excellent": 0,
                "good": 0,
                "fair": 0,
                "poor": 0,
                "missing": 0
            }
    
    @property
    def duration_seconds(self) -> float:
        """Calculate phase duration in seconds"""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate for the phase"""
        if self.channels_processed > 0:
            return self.channels_successful / self.channels_processed
        return 0.0
    
    def start_phase(self):
        """Mark phase as started"""
        self.start_time = datetime.now()
        self.status = ProcessingStatus.IN_PROGRESS
    
    def complete_phase(self):
        """Mark phase as completed"""
        self.end_time = datetime.now()
        self.status = ProcessingStatus.COMPLETED
    
    def fail_phase(self, error_message: str = ""):
        """Mark phase as failed"""
        self.end_time = datetime.now()
        self.status = ProcessingStatus.FAILED
        if error_message:
            self.error_summary["phase_failure"] = self.error_summary.get("phase_failure", 0) + 1


@dataclass
class ValidationMetrics:
    """Validation and quality assessment metrics"""
    junction_point_validations: Dict[str, bool] = field(default_factory=dict)
    statistical_outliers_detected: int = 0
    temporal_inconsistencies: int = 0
    cross_source_conflicts: int = 0
    interpolated_data_points: int = 0
    manual_reviews_required: int = 0
    overall_quality_score: float = 0.0
    completeness_score: float = 0.0
    consistency_score: float = 0.0
    temporal_score: float = 0.0
    validation_errors: List[Dict[str, Any]] = field(default_factory=list)
    
    def add_validation_error(self, error_type: str, message: str, 
                           affected_channels: List[str] = None):
        """Add validation error to metrics"""
        error_record = {
            "error_type": error_type,
            "message": message,
            "timestamp": datetime.now().isoformat(),
            "affected_channels": affected_channels or [],
            "channel_count": len(affected_channels) if affected_channels else 0
        }
        self.validation_errors.append(error_record)
    
    def update_junction_validation(self, junction_name: str, success: bool):
        """Update junction point validation result"""
        self.junction_point_validations[junction_name] = success


@dataclass
class CollectionContext:
    """Context information for data collection"""
    execution_id: str
    start_date: date
    end_date: date
    target_channels: int
    execution_environment: str = "production"
    configuration_version: str = "1.0.0"
    data_schema_version: str = "enhanced_v1"
    quality_rules_version: str = "1.0.0"
    collection_parameters: Dict[str, Any] = field(default_factory=dict)
    resource_constraints: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        if not self.collection_parameters:
            self.collection_parameters = {
                "batch_size": 1000,
                "parallel_workers": 16,
                "checkpoint_interval": 10000,
                "retry_attempts": 3,
                "rate_limit_delay": 1.0
            }
        
        if not self.resource_constraints:
            self.resource_constraints = {
                "max_memory_gb": 16,
                "max_processing_hours": 24,
                "api_quota_limits": {},
                "storage_requirements_gb": 10
            }


@dataclass
class ProcessingMetadata:
    """
    Comprehensive processing metadata for YouNiverse Dataset Enrichment
    
    Tracks all aspects of the 5-phase data collection and processing pipeline
    including performance metrics, quality assessments, validation results,
    and operational metadata for full traceability and monitoring.
    """
    
    # Core identification and context
    execution_id: str
    collection_context: CollectionContext
    created_timestamp: datetime = field(default_factory=datetime.now)
    last_updated: datetime = field(default_factory=datetime.now)
    
    # Phase-specific metrics
    phase_metrics: Dict[ProcessingPhase, PhaseMetrics] = field(default_factory=dict)
    
    # Source-specific metadata
    source_metadata: Dict[DataSource, SourceMetadata] = field(default_factory=dict)
    
    # Validation and quality metrics
    validation_metrics: ValidationMetrics = field(default_factory=ValidationMetrics)
    
    # Overall processing statistics
    total_channels_processed: int = 0
    total_records_created: int = 0
    total_processing_time_seconds: float = 0.0
    overall_success_rate: float = 0.0
    
    # Resource utilization
    peak_memory_usage_gb: float = 0.0
    total_api_requests: int = 0
    total_data_downloaded_gb: float = 0.0
    storage_usage_gb: float = 0.0
    
    # Error tracking and diagnostics
    critical_errors: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[Dict[str, Any]] = field(default_factory=list)
    performance_bottlenecks: List[Dict[str, Any]] = field(default_factory=list)
    
    # Final dataset characteristics
    final_dataset_stats: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Initialize phase metrics for all phases"""
        for phase in ProcessingPhase:
            if phase not in self.phase_metrics:
                self.phase_metrics[phase] = PhaseMetrics(phase=phase)
    
    def start_phase(self, phase: ProcessingPhase):
        """Start tracking a processing phase"""
        self.phase_metrics[phase].start_phase()
        self.last_updated = datetime.now()
    
    def complete_phase(self, phase: ProcessingPhase):
        """Complete tracking a processing phase"""
        self.phase_metrics[phase].complete_phase()
        self.last_updated = datetime.now()
        self._update_overall_metrics()
    
    def fail_phase(self, phase: ProcessingPhase, error_message: str = ""):
        """Mark a phase as failed"""
        self.phase_metrics[phase].fail_phase(error_message)
        self.last_updated = datetime.now()
        if error_message:
            self.add_critical_error(f"Phase {phase.value} failed", error_message)
    
    def update_phase_metrics(self, phase: ProcessingPhase, 
                           channels_processed: int = 0,
                           channels_successful: int = 0,
                           records_created: int = 0,
                           records_updated: int = 0):
        """Update metrics for a specific phase"""
        metrics = self.phase_metrics[phase]
        metrics.channels_processed += channels_processed
        metrics.channels_successful += channels_successful
        metrics.records_created += records_created
        metrics.records_updated += records_updated
        self.last_updated = datetime.now()
    
    def add_source_metadata(self, source: DataSource, metadata: SourceMetadata):
        """Add metadata for a data source"""
        self.source_metadata[source] = metadata
        self.last_updated = datetime.now()
    
    def add_critical_error(self, error_type: str, message: str, 
                          context: Dict[str, Any] = None):
        """Add critical error to tracking"""
        error_record = {
            "error_type": error_type,
            "message": message,
            "timestamp": datetime.now().isoformat(),
            "context": context or {}
        }
        self.critical_errors.append(error_record)
        self.last_updated = datetime.now()
    
    def add_warning(self, warning_type: str, message: str,
                   context: Dict[str, Any] = None):
        """Add warning to tracking"""
        warning_record = {
            "warning_type": warning_type,
            "message": message,
            "timestamp": datetime.now().isoformat(),
            "context": context or {}
        }
        self.warnings.append(warning_record)
        self.last_updated = datetime.now()
    
    def add_performance_bottleneck(self, bottleneck_type: str, description: str,
                                  impact_metrics: Dict[str, Any] = None):
        """Add performance bottleneck information"""
        bottleneck_record = {
            "bottleneck_type": bottleneck_type,
            "description": description,
            "timestamp": datetime.now().isoformat(),
            "impact_metrics": impact_metrics or {}
        }
        self.performance_bottlenecks.append(bottleneck_record)
        self.last_updated = datetime.now()
    
    def update_resource_usage(self, memory_gb: float = None, 
                            api_requests: int = None,
                            data_downloaded_gb: float = None,
                            storage_gb: float = None):
        """Update resource utilization metrics"""
        if memory_gb is not None:
            self.peak_memory_usage_gb = max(self.peak_memory_usage_gb, memory_gb)
        if api_requests is not None:
            self.total_api_requests += api_requests
        if data_downloaded_gb is not None:
            self.total_data_downloaded_gb += data_downloaded_gb
        if storage_gb is not None:
            self.storage_usage_gb = storage_gb
        self.last_updated = datetime.now()
    
    def update_final_dataset_stats(self, stats: Dict[str, Any]):
        """Update final dataset statistics"""
        self.final_dataset_stats.update(stats)
        self.last_updated = datetime.now()
    
    def _update_overall_metrics(self):
        """Update overall processing metrics from phase metrics"""
        total_channels = 0
        total_records = 0
        total_time = 0.0
        successful_channels = 0
        
        for phase_metrics in self.phase_metrics.values():
            total_channels += phase_metrics.channels_processed
            total_records += phase_metrics.records_created
            total_time += phase_metrics.duration_seconds
            successful_channels += phase_metrics.channels_successful
        
        self.total_channels_processed = total_channels
        self.total_records_created = total_records
        self.total_processing_time_seconds = total_time
        
        if total_channels > 0:
            self.overall_success_rate = successful_channels / total_channels
    
    def get_processing_summary(self) -> Dict[str, Any]:
        """Get comprehensive processing summary"""
        return {
            "execution_id": self.execution_id,
            "collection_context": {
                "start_date": self.collection_context.start_date.isoformat(),
                "end_date": self.collection_context.end_date.isoformat(),
                "target_channels": self.collection_context.target_channels,
                "execution_environment": self.collection_context.execution_environment
            },
            "processing_timeline": {
                "created": self.created_timestamp.isoformat(),
                "last_updated": self.last_updated.isoformat(),
                "total_duration_hours": self.total_processing_time_seconds / 3600
            },
            "overall_metrics": {
                "channels_processed": self.total_channels_processed,
                "records_created": self.total_records_created,
                "success_rate": round(self.overall_success_rate, 3),
                "api_requests": self.total_api_requests,
                "data_downloaded_gb": round(self.total_data_downloaded_gb, 2),
                "peak_memory_gb": round(self.peak_memory_usage_gb, 2)
            },
            "phase_summary": {
                phase.value: {
                    "status": metrics.status.value,
                    "channels_processed": metrics.channels_processed,
                    "success_rate": round(metrics.success_rate, 3),
                    "duration_minutes": round(metrics.duration_seconds / 60, 1),
                    "records_created": metrics.records_created
                }
                for phase, metrics in self.phase_metrics.items()
            },
            "quality_metrics": {
                "overall_quality_score": round(self.validation_metrics.overall_quality_score, 3),
                "completeness_score": round(self.validation_metrics.completeness_score, 3),
                "consistency_score": round(self.validation_metrics.consistency_score, 3),
                "junction_validations": self.validation_metrics.junction_point_validations,
                "validation_errors": len(self.validation_metrics.validation_errors)
            },
            "error_summary": {
                "critical_errors": len(self.critical_errors),
                "warnings": len(self.warnings),
                "performance_bottlenecks": len(self.performance_bottlenecks)
            },
            "final_dataset": self.final_dataset_stats
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metadata to dictionary representation"""
        return {
            "execution_id": self.execution_id,
            "collection_context": {
                "execution_id": self.collection_context.execution_id,
                "start_date": self.collection_context.start_date.isoformat(),
                "end_date": self.collection_context.end_date.isoformat(),
                "target_channels": self.collection_context.target_channels,
                "execution_environment": self.collection_context.execution_environment,
                "configuration_version": self.collection_context.configuration_version,
                "data_schema_version": self.collection_context.data_schema_version,
                "quality_rules_version": self.collection_context.quality_rules_version,
                "collection_parameters": self.collection_context.collection_parameters,
                "resource_constraints": self.collection_context.resource_constraints
            },
            "created_timestamp": self.created_timestamp.isoformat(),
            "last_updated": self.last_updated.isoformat(),
            "phase_metrics": {
                phase.value: {
                    "phase": metrics.phase.value,
                    "start_time": metrics.start_time.isoformat() if metrics.start_time else None,
                    "end_time": metrics.end_time.isoformat() if metrics.end_time else None,
                    "status": metrics.status.value,
                    "channels_processed": metrics.channels_processed,
                    "channels_successful": metrics.channels_successful,
                    "channels_failed": metrics.channels_failed,
                    "records_created": metrics.records_created,
                    "records_updated": metrics.records_updated,
                    "records_validated": metrics.records_validated,
                    "quality_scores": metrics.quality_scores,
                    "error_summary": metrics.error_summary,
                    "performance_metrics": metrics.performance_metrics,
                    "duration_seconds": metrics.duration_seconds,
                    "success_rate": metrics.success_rate
                }
                for phase, metrics in self.phase_metrics.items()
            },
            "source_metadata": {
                source.value: {
                    "source": metadata.source.value,
                    "url": metadata.url,
                    "timestamp": metadata.timestamp.isoformat() if metadata.timestamp else None,
                    "response_code": metadata.response_code,
                    "data_points_collected": metadata.data_points_collected,
                    "success_rate": metadata.success_rate,
                    "error_messages": metadata.error_messages,
                    "rate_limit_info": metadata.rate_limit_info,
                    "snapshot_info": metadata.snapshot_info
                }
                for source, metadata in self.source_metadata.items()
            },
            "validation_metrics": {
                "junction_point_validations": self.validation_metrics.junction_point_validations,
                "statistical_outliers_detected": self.validation_metrics.statistical_outliers_detected,
                "temporal_inconsistencies": self.validation_metrics.temporal_inconsistencies,
                "cross_source_conflicts": self.validation_metrics.cross_source_conflicts,
                "interpolated_data_points": self.validation_metrics.interpolated_data_points,
                "manual_reviews_required": self.validation_metrics.manual_reviews_required,
                "overall_quality_score": self.validation_metrics.overall_quality_score,
                "completeness_score": self.validation_metrics.completeness_score,
                "consistency_score": self.validation_metrics.consistency_score,
                "temporal_score": self.validation_metrics.temporal_score,
                "validation_errors": self.validation_metrics.validation_errors
            },
            "overall_metrics": {
                "total_channels_processed": self.total_channels_processed,
                "total_records_created": self.total_records_created,
                "total_processing_time_seconds": self.total_processing_time_seconds,
                "overall_success_rate": self.overall_success_rate,
                "peak_memory_usage_gb": self.peak_memory_usage_gb,
                "total_api_requests": self.total_api_requests,
                "total_data_downloaded_gb": self.total_data_downloaded_gb,
                "storage_usage_gb": self.storage_usage_gb
            },
            "error_tracking": {
                "critical_errors": self.critical_errors,
                "warnings": self.warnings,
                "performance_bottlenecks": self.performance_bottlenecks
            },
            "final_dataset_stats": self.final_dataset_stats
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ProcessingMetadata':
        """Create ProcessingMetadata from dictionary representation"""
        
        # Parse collection context
        context_data = data["collection_context"]
        context = CollectionContext(
            execution_id=context_data["execution_id"],
            start_date=datetime.fromisoformat(context_data["start_date"]).date(),
            end_date=datetime.fromisoformat(context_data["end_date"]).date(),
            target_channels=context_data["target_channels"],
            execution_environment=context_data.get("execution_environment", "production"),
            configuration_version=context_data.get("configuration_version", "1.0.0"),
            data_schema_version=context_data.get("data_schema_version", "enhanced_v1"),
            quality_rules_version=context_data.get("quality_rules_version", "1.0.0"),
            collection_parameters=context_data.get("collection_parameters", {}),
            resource_constraints=context_data.get("resource_constraints", {})
        )
        
        # Parse validation metrics
        validation_data = data.get("validation_metrics", {})
        validation_metrics = ValidationMetrics(
            junction_point_validations=validation_data.get("junction_point_validations", {}),
            statistical_outliers_detected=validation_data.get("statistical_outliers_detected", 0),
            temporal_inconsistencies=validation_data.get("temporal_inconsistencies", 0),
            cross_source_conflicts=validation_data.get("cross_source_conflicts", 0),
            interpolated_data_points=validation_data.get("interpolated_data_points", 0),
            manual_reviews_required=validation_data.get("manual_reviews_required", 0),
            overall_quality_score=validation_data.get("overall_quality_score", 0.0),
            completeness_score=validation_data.get("completeness_score", 0.0),
            consistency_score=validation_data.get("consistency_score", 0.0),
            temporal_score=validation_data.get("temporal_score", 0.0),
            validation_errors=validation_data.get("validation_errors", [])
        )
        
        # Create main object
        metadata = cls(
            execution_id=data["execution_id"],
            collection_context=context,
            created_timestamp=datetime.fromisoformat(data["created_timestamp"]),
            last_updated=datetime.fromisoformat(data["last_updated"]),
            validation_metrics=validation_metrics,
            total_channels_processed=data.get("overall_metrics", {}).get("total_channels_processed", 0),
            total_records_created=data.get("overall_metrics", {}).get("total_records_created", 0),
            total_processing_time_seconds=data.get("overall_metrics", {}).get("total_processing_time_seconds", 0.0),
            overall_success_rate=data.get("overall_metrics", {}).get("overall_success_rate", 0.0),
            peak_memory_usage_gb=data.get("overall_metrics", {}).get("peak_memory_usage_gb", 0.0),
            total_api_requests=data.get("overall_metrics", {}).get("total_api_requests", 0),
            total_data_downloaded_gb=data.get("overall_metrics", {}).get("total_data_downloaded_gb", 0.0),
            storage_usage_gb=data.get("overall_metrics", {}).get("storage_usage_gb", 0.0),
            critical_errors=data.get("error_tracking", {}).get("critical_errors", []),
            warnings=data.get("error_tracking", {}).get("warnings", []),
            performance_bottlenecks=data.get("error_tracking", {}).get("performance_bottlenecks", []),
            final_dataset_stats=data.get("final_dataset_stats", {})
        )
        
        # Parse phase metrics
        phase_data = data.get("phase_metrics", {})
        for phase_name, metrics_data in phase_data.items():
            phase = ProcessingPhase(phase_name)
            metrics = PhaseMetrics(
                phase=phase,
                start_time=datetime.fromisoformat(metrics_data["start_time"]) if metrics_data.get("start_time") else None,
                end_time=datetime.fromisoformat(metrics_data["end_time"]) if metrics_data.get("end_time") else None,
                status=ProcessingStatus(metrics_data.get("status", "pending")),
                channels_processed=metrics_data.get("channels_processed", 0),
                channels_successful=metrics_data.get("channels_successful", 0),
                channels_failed=metrics_data.get("channels_failed", 0),
                records_created=metrics_data.get("records_created", 0),
                records_updated=metrics_data.get("records_updated", 0),
                records_validated=metrics_data.get("records_validated", 0),
                quality_scores=metrics_data.get("quality_scores", {}),
                error_summary=metrics_data.get("error_summary", {}),
                performance_metrics=metrics_data.get("performance_metrics", {})
            )
            metadata.phase_metrics[phase] = metrics
        
        # Parse source metadata
        source_data = data.get("source_metadata", {})
        for source_name, source_metadata_data in source_data.items():
            source = DataSource(source_name)
            source_metadata = SourceMetadata(
                source=source,
                url=source_metadata_data.get("url"),
                timestamp=datetime.fromisoformat(source_metadata_data["timestamp"]) if source_metadata_data.get("timestamp") else None,
                response_code=source_metadata_data.get("response_code"),
                data_points_collected=source_metadata_data.get("data_points_collected", 0),
                success_rate=source_metadata_data.get("success_rate", 0.0),
                error_messages=source_metadata_data.get("error_messages", []),
                rate_limit_info=source_metadata_data.get("rate_limit_info", {}),
                snapshot_info=source_metadata_data.get("snapshot_info", {})
            )
            metadata.source_metadata[source] = source_metadata
        
        return metadata
    
    def to_json(self) -> str:
        """Convert metadata to JSON representation"""
        return json.dumps(self.to_dict(), indent=2)
    
    @classmethod  
    def from_json(cls, json_str: str) -> 'ProcessingMetadata':
        """Create ProcessingMetadata from JSON representation"""
        data = json.loads(json_str)
        return cls.from_dict(data)