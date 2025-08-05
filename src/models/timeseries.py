"""
Timeseries Record Model for YouNiverse Dataset Enrichment

This module defines the timeseries record data model supporting enhanced
metrics and weekly granularity for the comprehensive 10-year timeline.

Author: data-analytics-expert
"""

from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Optional, Dict, List, Any
from enum import Enum
import json


class CollectionSource(Enum):
    """Data collection source enumeration"""
    API = "API"
    WAYBACK_CHARTS = "WAYBACK_CHARTS"
    WAYBACK_UPLOADS = "WAYBACK_UPLOADS"
    VIEWSTATS = "VIEWSTATS"
    FINAL_API = "FINAL_API"


class DataQualityFlag(Enum):
    """Data quality flag enumeration"""
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    MISSING = "missing"


class ActivityStatus(Enum):
    """Channel activity status enumeration"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    DORMANT = "dormant"
    UNKNOWN = "unknown"


@dataclass
class TimeseriesRecord:
    """
    Enhanced timeseries record for YouNiverse dataset
    
    Supports all original fields plus enhanced analytics fields
    for comprehensive 10-year dataset analysis.
    """
    
    # Core identification and temporal fields
    channel: str
    datetime: datetime
    
    # Original schema fields (preserved exactly)
    category: str = ""
    views: int = 0
    delta_views: int = 0
    subs: int = 0
    delta_subs: int = 0
    videos: int = 0
    delta_videos: int = 0
    activity: str = "unknown"
    
    # Enhanced schema fields
    total_longform_views: int = 0
    total_shorts_views: int = 0
    channel_name: str = ""
    collection_source: CollectionSource = CollectionSource.API
    data_quality_flag: DataQualityFlag = DataQualityFlag.GOOD
    
    # Additional metadata for analytics
    week_start_date: Optional[date] = None
    week_number: int = 0
    year: int = 0
    quarter: int = 0
    
    # Quality and validation metadata
    validation_flags: List[str] = field(default_factory=list)
    processing_notes: str = ""
    created_timestamp: Optional[datetime] = None
    modified_timestamp: Optional[datetime] = None
    
    def __post_init__(self):
        """Initialize derived fields after object creation"""
        if self.datetime:
            # Set week start date (normalize to Sunday)
            days_since_sunday = self.datetime.weekday() + 1
            if days_since_sunday == 7:
                days_since_sunday = 0
            self.week_start_date = (self.datetime - 
                                  pd.Timedelta(days=days_since_sunday)).date()
            
            # Set temporal metadata
            self.year = self.datetime.year
            self.quarter = ((self.datetime.month - 1) // 3) + 1
            self.week_number = self.datetime.isocalendar()[1]
        
        if not self.created_timestamp:
            self.created_timestamp = datetime.now()
        
        if not self.modified_timestamp:
            self.modified_timestamp = datetime.now()
    
    def update_metrics(self,
                      views: int = None,
                      subs: int = None,
                      videos: int = None,
                      longform_views: int = None,
                      shorts_views: int = None):
        """Update metrics and calculate deltas"""
        
        # Update views if provided
        if views is not None:
            self.delta_views = views - self.views
            self.views = views
        
        # Update subscribers if provided
        if subs is not None:
            self.delta_subs = subs - self.subs
            self.subs = subs
        
        # Update videos if provided
        if videos is not None:
            self.delta_videos = videos - self.videos
            self.videos = videos
        
        # Update enhanced view metrics
        if longform_views is not None:
            self.total_longform_views = longform_views
        
        if shorts_views is not None:
            self.total_shorts_views = shorts_views
        
        # Update modification timestamp
        self.modified_timestamp = datetime.now()
    
    def calculate_growth_rates(self, previous_record: 'TimeseriesRecord') -> Dict[str, float]:
        """Calculate growth rates compared to previous record"""
        growth_rates = {}
        
        # Subscriber growth rate
        if previous_record.subs > 0:
            growth_rates['subscriber_growth_rate'] = (self.subs - previous_record.subs) / previous_record.subs
        else:
            growth_rates['subscriber_growth_rate'] = 0.0
        
        # View growth rate
        if previous_record.views > 0:
            growth_rates['view_growth_rate'] = (self.views - previous_record.views) / previous_record.views
        else:
            growth_rates['view_growth_rate'] = 0.0
        
        # Video growth rate
        growth_rates['video_growth_rate'] = self.videos - previous_record.videos
        
        return growth_rates
    
    def get_engagement_metrics(self) -> Dict[str, float]:
        """Calculate engagement metrics"""
        metrics = {}
        
        # Views per subscriber
        if self.subs > 0:
            metrics['views_per_subscriber'] = self.views / self.subs
        else:
            metrics['views_per_subscriber'] = 0.0
        
        # Views per video
        if self.videos > 0:
            metrics['views_per_video'] = self.views / self.videos
        else:
            metrics['views_per_video'] = 0.0
        
        # Shorts vs longform ratio
        total_enhanced_views = self.total_longform_views + self.total_shorts_views
        if total_enhanced_views > 0:
            metrics['shorts_ratio'] = self.total_shorts_views / total_enhanced_views
            metrics['longform_ratio'] = self.total_longform_views / total_enhanced_views
        else:
            metrics['shorts_ratio'] = 0.0
            metrics['longform_ratio'] = 0.0
        
        return metrics
    
    def add_validation_flag(self, flag: str):
        """Add a validation flag to the record"""
        if flag not in self.validation_flags:
            self.validation_flags.append(flag)
            self.modified_timestamp = datetime.now()
    
    def remove_validation_flag(self, flag: str):
        """Remove a validation flag from the record"""
        if flag in self.validation_flags:
            self.validation_flags.remove(flag)
            self.modified_timestamp = datetime.now()
    
    def is_junction_point(self) -> bool:
        """Check if this record is near a critical junction point"""
        sep_2019 = datetime(2019, 9, 1)
        aug_2022 = datetime(2022, 8, 1)
        
        # Check if within 2 weeks of junction points
        for junction_date in [sep_2019, aug_2022]:
            time_diff = abs((self.datetime - junction_date).days)
            if time_diff <= 14:  # Within 2 weeks
                return True
        
        return False
    
    def validate_data_consistency(self) -> List[str]:
        """Validate internal data consistency"""
        issues = []
        
        # Check for negative core metrics
        if self.views < 0:
            issues.append("Negative view count")
        if self.subs < 0:
            issues.append("Negative subscriber count")
        if self.videos < 0:
            issues.append("Negative video count")
        
        # Check enhanced views consistency
        total_enhanced = self.total_longform_views + self.total_shorts_views
        if total_enhanced > 0 and self.views > 0:
            if total_enhanced > self.views * 1.1:  # Allow 10% variance
                issues.append("Enhanced view counts exceed total views")
        
        # Check delta consistency (deltas should be reasonable)
        if abs(self.delta_views) > self.views:
            issues.append("Delta views exceeds total views")
        
        if abs(self.delta_subs) > self.subs:
            issues.append("Delta subscribers exceeds total subscribers")
        
        return issues
    
    def to_original_format(self) -> Dict[str, Any]:
        """Convert to original dataset format for compatibility"""
        return {
            'channel': self.channel,
            'category': self.category,
            'datetime': self.datetime.isoformat(),
            'views': self.views,
            'delta_views': self.delta_views,
            'subs': self.subs,
            'delta_subs': self.delta_subs,
            'videos': self.videos,
            'delta_videos': self.delta_videos,
            'activity': self.activity
        }
    
    def to_enhanced_format(self) -> Dict[str, Any]:
        """Convert to enhanced dataset format"""
        return {
            'channel': self.channel,
            'category': self.category,
            'datetime': self.datetime.isoformat(),
            'views': self.views,
            'delta_views': self.delta_views,
            'subs': self.subs,
            'delta_subs': self.delta_subs,
            'videos': self.videos,
            'delta_videos': self.delta_videos,
            'activity': self.activity,
            'total_longform_views': self.total_longform_views,
            'total_shorts_views': self.total_shorts_views,
            'channel_name': self.channel_name,
            'collection_source': self.collection_source.value,
            'data_quality_flag': self.data_quality_flag.value
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to complete dictionary representation"""
        return {
            'channel': self.channel,
            'datetime': self.datetime.isoformat(),
            'category': self.category,
            'views': self.views,
            'delta_views': self.delta_views,
            'subs': self.subs,
            'delta_subs': self.delta_subs,
            'videos': self.videos,
            'delta_videos': self.delta_videos,
            'activity': self.activity,
            'total_longform_views': self.total_longform_views,
            'total_shorts_views': self.total_shorts_views,
            'channel_name': self.channel_name,
            'collection_source': self.collection_source.value,
            'data_quality_flag': self.data_quality_flag.value,
            'week_start_date': self.week_start_date.isoformat() if self.week_start_date else None,
            'week_number': self.week_number,
            'year': self.year,
            'quarter': self.quarter,
            'validation_flags': self.validation_flags,
            'processing_notes': self.processing_notes,
            'created_timestamp': self.created_timestamp.isoformat() if self.created_timestamp else None,
            'modified_timestamp': self.modified_timestamp.isoformat() if self.modified_timestamp else None
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TimeseriesRecord':
        """Create record from dictionary representation"""
        
        # Parse datetime
        dt = datetime.fromisoformat(data['datetime']) if isinstance(data['datetime'], str) else data['datetime']
        
        # Parse optional timestamps
        created_ts = None
        if data.get('created_timestamp'):
            created_ts = datetime.fromisoformat(data['created_timestamp'])
        
        modified_ts = None
        if data.get('modified_timestamp'):
            modified_ts = datetime.fromisoformat(data['modified_timestamp'])
        
        # Parse week start date
        week_start = None
        if data.get('week_start_date'):
            week_start = datetime.fromisoformat(data['week_start_date']).date()
        
        return cls(
            channel=data['channel'],
            datetime=dt,
            category=data.get('category', ''),
            views=data.get('views', 0),
            delta_views=data.get('delta_views', 0),
            subs=data.get('subs', 0),
            delta_subs=data.get('delta_subs', 0),
            videos=data.get('videos', 0),
            delta_videos=data.get('delta_videos', 0),
            activity=data.get('activity', 'unknown'),
            total_longform_views=data.get('total_longform_views', 0),
            total_shorts_views=data.get('total_shorts_views', 0),
            channel_name=data.get('channel_name', ''),
            collection_source=CollectionSource(data.get('collection_source', 'API')),
            data_quality_flag=DataQualityFlag(data.get('data_quality_flag', 'good')),
            week_start_date=week_start,
            week_number=data.get('week_number', 0),
            year=data.get('year', 0),
            quarter=data.get('quarter', 0),
            validation_flags=data.get('validation_flags', []),
            processing_notes=data.get('processing_notes', ''),
            created_timestamp=created_ts,
            modified_timestamp=modified_ts
        )
    
    @classmethod
    def from_original_format(cls, data: Dict[str, Any]) -> 'TimeseriesRecord':
        """Create record from original dataset format"""
        dt = datetime.fromisoformat(data['datetime']) if isinstance(data['datetime'], str) else data['datetime']
        
        return cls(
            channel=data['channel'],
            datetime=dt,
            category=data.get('category', ''),
            views=int(data.get('views', 0)),
            delta_views=int(data.get('delta_views', 0)),
            subs=int(data.get('subs', 0)),
            delta_subs=int(data.get('delta_subs', 0)),
            videos=int(data.get('videos', 0)),
            delta_videos=int(data.get('delta_videos', 0)),
            activity=data.get('activity', 'unknown')
        )
    
    def to_json(self) -> str:
        """Convert to JSON representation"""
        return json.dumps(self.to_dict(), indent=2)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'TimeseriesRecord':
        """Create record from JSON representation"""
        data = json.loads(json_str)
        return cls.from_dict(data)
    
    def __str__(self) -> str:
        """String representation for debugging"""
        return (f"TimeseriesRecord(channel={self.channel}, "
                f"datetime={self.datetime.isoformat()}, "
                f"views={self.views}, subs={self.subs}, "
                f"quality={self.data_quality_flag.value})")
    
    def __repr__(self) -> str:
        """Detailed representation for debugging"""
        return self.__str__()