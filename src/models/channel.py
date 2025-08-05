"""
Channel Data Model for YouNiverse Dataset Enrichment

This module defines the channel data model supporting enhanced metadata
and analytics capabilities for the comprehensive 10-year dataset.

Author: data-analytics-expert
"""

from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Optional, Dict, List, Any
from enum import Enum
import json


class ChannelStatus(Enum):
    """Channel status enumeration"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    TERMINATED = "terminated"
    PRIVATE = "private"
    UNKNOWN = "unknown"


class ChannelCategory(Enum):
    """YouTube channel category enumeration"""
    AUTOS_VEHICLES = "Autos & Vehicles"
    COMEDY = "Comedy"
    EDUCATION = "Education"
    ENTERTAINMENT = "Entertainment"
    FILM_ANIMATION = "Film & Animation"
    GAMING = "Gaming"
    HOWTO_STYLE = "Howto & Style"
    MUSIC = "Music"
    NEWS_POLITICS = "News & Politics"
    NONPROFITS_ACTIVISM = "Nonprofits & Activism"
    PEOPLE_BLOGS = "People & Blogs"
    PETS_ANIMALS = "Pets & Animals"
    SCIENCE_TECHNOLOGY = "Science & Technology"
    SPORTS = "Sports"
    TRAVEL_EVENTS = "Travel & Events"
    OTHER = "Other"


@dataclass
class ChannelMetrics:
    """Current channel metrics snapshot"""
    subscriber_count: int = 0
    total_view_count: int = 0
    video_count: int = 0
    total_longform_views: int = 0
    total_shorts_views: int = 0
    average_views_per_video: float = 0.0
    subscriber_to_view_ratio: float = 0.0
    last_updated: Optional[datetime] = None
    
    def calculate_derived_metrics(self):
        """Calculate derived metrics from base counts"""
        if self.video_count > 0:
            self.average_views_per_video = self.total_view_count / self.video_count
        
        if self.total_view_count > 0:
            self.subscriber_to_view_ratio = self.subscriber_count / self.total_view_count


@dataclass
class ChannelHistory:
    """Historical data tracking for channel"""
    first_data_point: Optional[date] = None
    last_data_point: Optional[date] = None
    total_data_points: int = 0
    data_coverage_percentage: float = 0.0
    junction_points_validated: List[str] = field(default_factory=list)
    collection_sources_used: List[str] = field(default_factory=list)
    quality_score: float = 0.0
    
    def update_coverage(self, start_date: date, end_date: date, expected_weeks: int):
        """Update data coverage statistics"""
        if self.total_data_points > 0 and expected_weeks > 0:
            self.data_coverage_percentage = min(self.total_data_points / expected_weeks, 1.0)


@dataclass
class Channel:
    """
    Comprehensive channel data model
    
    Supports enhanced metadata fields for 10-year dataset analytics
    including current metrics, historical tracking, and quality assessment.
    """
    
    # Core identification
    channel_id: str
    channel_name: str = ""
    channel_handle: str = ""  # @username format
    
    # Classification
    category: ChannelCategory = ChannelCategory.OTHER
    country: str = ""
    language: str = ""
    
    # Status and metadata
    status: ChannelStatus = ChannelStatus.UNKNOWN
    created_date: Optional[date] = None
    description: str = ""
    keywords: List[str] = field(default_factory=list)
    
    # Current metrics
    current_metrics: ChannelMetrics = field(default_factory=ChannelMetrics)
    
    # Historical data tracking
    history: ChannelHistory = field(default_factory=ChannelHistory)
    
    # Data collection metadata
    collection_metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Quality and validation
    data_quality_score: float = 0.0
    validation_flags: List[str] = field(default_factory=list)
    last_validated: Optional[datetime] = None
    
    # Processing status
    processing_status: str = "pending"  # pending, processing, completed, failed
    processing_notes: str = ""
    last_updated: Optional[datetime] = None
    
    def __post_init__(self):
        """Initialize derived fields after object creation"""
        if not self.channel_name and self.channel_id:
            self.channel_name = f"Channel_{self.channel_id[:8]}"
        
        if not self.last_updated:
            self.last_updated = datetime.now()
    
    def update_current_metrics(self, 
                              subscriber_count: int,
                              total_view_count: int,
                              video_count: int,
                              longform_views: int = 0,
                              shorts_views: int = 0):
        """Update current channel metrics"""
        self.current_metrics.subscriber_count = subscriber_count
        self.current_metrics.total_view_count = total_view_count
        self.current_metrics.video_count = video_count
        self.current_metrics.total_longform_views = longform_views
        self.current_metrics.total_shorts_views = shorts_views
        self.current_metrics.last_updated = datetime.now()
        
        # Calculate derived metrics
        self.current_metrics.calculate_derived_metrics()
        
        # Update last updated timestamp
        self.last_updated = datetime.now()
    
    def add_validation_flag(self, flag: str, details: str = ""):
        """Add a validation flag to the channel"""
        if flag not in self.validation_flags:
            self.validation_flags.append(flag)
        
        # Store flag details in collection metadata
        if "validation_details" not in self.collection_metadata:
            self.collection_metadata["validation_details"] = {}
        
        self.collection_metadata["validation_details"][flag] = {
            "timestamp": datetime.now().isoformat(),
            "details": details
        }
    
    def remove_validation_flag(self, flag: str):
        """Remove a validation flag from the channel"""
        if flag in self.validation_flags:
            self.validation_flags.remove(flag)
        
        # Remove from details as well
        if ("validation_details" in self.collection_metadata and 
            flag in self.collection_metadata["validation_details"]):
            del self.collection_metadata["validation_details"][flag]
    
    def update_data_quality_score(self, score: float):
        """Update the overall data quality score"""
        self.data_quality_score = max(0.0, min(1.0, score))  # Clamp between 0 and 1
        self.last_validated = datetime.now()
    
    def add_collection_source(self, source: str, details: Dict[str, Any] = None):
        """Add information about a data collection source"""
        if "collection_sources" not in self.collection_metadata:
            self.collection_metadata["collection_sources"] = {}
        
        self.collection_metadata["collection_sources"][source] = {
            "timestamp": datetime.now().isoformat(),
            "details": details or {}
        }
        
        # Update history tracking
        if source not in self.history.collection_sources_used:
            self.history.collection_sources_used.append(source)
    
    def update_historical_coverage(self, 
                                 first_date: date,
                                 last_date: date,
                                 total_points: int,
                                 expected_points: int):
        """Update historical data coverage information"""
        self.history.first_data_point = first_date
        self.history.last_data_point = last_date
        self.history.total_data_points = total_points
        
        if expected_points > 0:
            self.history.data_coverage_percentage = min(total_points / expected_points, 1.0)
    
    def validate_junction_point(self, junction_name: str, success: bool):
        """Record junction point validation result"""
        if success and junction_name not in self.history.junction_points_validated:
            self.history.junction_points_validated.append(junction_name)
        elif not success and junction_name in self.history.junction_points_validated:
            self.history.junction_points_validated.remove(junction_name)
        
        # Add validation flag if failed
        flag = f"junction_point_{junction_name.lower().replace(' ', '_')}"
        if not success:
            self.add_validation_flag(flag, f"Junction point {junction_name} validation failed")
        else:
            self.remove_validation_flag(flag)
    
    def get_quality_summary(self) -> Dict[str, Any]:
        """Get a summary of channel data quality"""
        return {
            "channel_id": self.channel_id,
            "channel_name": self.channel_name,
            "status": self.status.value,
            "data_quality_score": self.data_quality_score,
            "data_coverage_percentage": self.history.data_coverage_percentage,
            "total_data_points": self.history.total_data_points,
            "validation_flags": self.validation_flags,
            "collection_sources": len(self.history.collection_sources_used),
            "junction_points_validated": len(self.history.junction_points_validated),
            "last_updated": self.last_updated.isoformat() if self.last_updated else None
        }
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get a summary of current channel metrics"""
        return {
            "channel_id": self.channel_id,
            "channel_name": self.channel_name,
            "category": self.category.value,
            "subscriber_count": self.current_metrics.subscriber_count,
            "total_view_count": self.current_metrics.total_view_count,
            "video_count": self.current_metrics.video_count,
            "total_longform_views": self.current_metrics.total_longform_views,
            "total_shorts_views": self.current_metrics.total_shorts_views,
            "average_views_per_video": self.current_metrics.average_views_per_video,
            "subscriber_to_view_ratio": self.current_metrics.subscriber_to_view_ratio,
            "last_updated": self.current_metrics.last_updated.isoformat() if self.current_metrics.last_updated else None
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert channel to dictionary representation"""
        return {
            "channel_id": self.channel_id,
            "channel_name": self.channel_name,
            "channel_handle": self.channel_handle,
            "category": self.category.value,
            "country": self.country,
            "language": self.language,
            "status": self.status.value,
            "created_date": self.created_date.isoformat() if self.created_date else None,
            "description": self.description,
            "keywords": self.keywords,
            "current_metrics": {
                "subscriber_count": self.current_metrics.subscriber_count,
                "total_view_count": self.current_metrics.total_view_count,
                "video_count": self.current_metrics.video_count,
                "total_longform_views": self.current_metrics.total_longform_views,
                "total_shorts_views": self.current_metrics.total_shorts_views,
                "average_views_per_video": self.current_metrics.average_views_per_video,
                "subscriber_to_view_ratio": self.current_metrics.subscriber_to_view_ratio,
                "last_updated": self.current_metrics.last_updated.isoformat() if self.current_metrics.last_updated else None
            },
            "history": {
                "first_data_point": self.history.first_data_point.isoformat() if self.history.first_data_point else None,
                "last_data_point": self.history.last_data_point.isoformat() if self.history.last_data_point else None,
                "total_data_points": self.history.total_data_points,
                "data_coverage_percentage": self.history.data_coverage_percentage,
                "junction_points_validated": self.history.junction_points_validated,
                "collection_sources_used": self.history.collection_sources_used,
                "quality_score": self.history.quality_score
            },
            "collection_metadata": self.collection_metadata,
            "data_quality_score": self.data_quality_score,
            "validation_flags": self.validation_flags,
            "last_validated": self.last_validated.isoformat() if self.last_validated else None,
            "processing_status": self.processing_status,
            "processing_notes": self.processing_notes,
            "last_updated": self.last_updated.isoformat() if self.last_updated else None
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Channel':
        """Create channel from dictionary representation"""
        
        # Parse dates
        created_date = None
        if data.get("created_date"):
            created_date = datetime.fromisoformat(data["created_date"]).date()
        
        last_validated = None
        if data.get("last_validated"):
            last_validated = datetime.fromisoformat(data["last_validated"])
        
        last_updated = None
        if data.get("last_updated"):
            last_updated = datetime.fromisoformat(data["last_updated"])
        
        # Create current metrics
        metrics_data = data.get("current_metrics", {})
        current_metrics = ChannelMetrics(
            subscriber_count=metrics_data.get("subscriber_count", 0),
            total_view_count=metrics_data.get("total_view_count", 0),
            video_count=metrics_data.get("video_count", 0),
            total_longform_views=metrics_data.get("total_longform_views", 0),
            total_shorts_views=metrics_data.get("total_shorts_views", 0),
            average_views_per_video=metrics_data.get("average_views_per_video", 0.0),
            subscriber_to_view_ratio=metrics_data.get("subscriber_to_view_ratio", 0.0),
            last_updated=datetime.fromisoformat(metrics_data["last_updated"]) if metrics_data.get("last_updated") else None
        )
        
        # Create history
        history_data = data.get("history", {})
        history = ChannelHistory(
            first_data_point=datetime.fromisoformat(history_data["first_data_point"]).date() if history_data.get("first_data_point") else None,
            last_data_point=datetime.fromisoformat(history_data["last_data_point"]).date() if history_data.get("last_data_point") else None,
            total_data_points=history_data.get("total_data_points", 0),
            data_coverage_percentage=history_data.get("data_coverage_percentage", 0.0),
            junction_points_validated=history_data.get("junction_points_validated", []),
            collection_sources_used=history_data.get("collection_sources_used", []),
            quality_score=history_data.get("quality_score", 0.0)
        )
        
        # Create channel
        return cls(
            channel_id=data["channel_id"],
            channel_name=data.get("channel_name", ""),
            channel_handle=data.get("channel_handle", ""),
            category=ChannelCategory(data.get("category", "Other")),
            country=data.get("country", ""),
            language=data.get("language", ""),
            status=ChannelStatus(data.get("status", "unknown")),
            created_date=created_date,
            description=data.get("description", ""),
            keywords=data.get("keywords", []),
            current_metrics=current_metrics,
            history=history,
            collection_metadata=data.get("collection_metadata", {}),
            data_quality_score=data.get("data_quality_score", 0.0),
            validation_flags=data.get("validation_flags", []),
            last_validated=last_validated,
            processing_status=data.get("processing_status", "pending"),
            processing_notes=data.get("processing_notes", ""),
            last_updated=last_updated
        )
    
    def to_json(self) -> str:
        """Convert channel to JSON representation"""
        return json.dumps(self.to_dict(), indent=2)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Channel':
        """Create channel from JSON representation"""
        data = json.loads(json_str)
        return cls.from_dict(data)