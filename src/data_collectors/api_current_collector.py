"""
Current YouTube API Data Collector
Phase 4: Final API update and current channel metadata collection

This collector gets current channel metadata including handles, names,
and current statistics to support ViewStats URL construction and
provide final current data validation.

Author: feature-developer
"""

import time
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging

from .base_collector import BaseCollector
from ..models.channel import Channel, ChannelStatus
from ..utils.api_helpers import create_youtube_client, YouTubeAPIClient
from ..core.config import config_manager


class APICurrentCollector(BaseCollector):
    """
    Current YouTube API collector for final metadata and validation
    
    Features:
    - Get current channel handles/custom URLs for ViewStats integration
    - Update current subscriber/view/video counts
    - Validate channel status (active/terminated/suspended)
    - Support ViewStats URL construction with proper handles
    """
    
    def __init__(self):
        config = config_manager.get_youtube_config()
        
        super().__init__(
            collector_name="APICurrentCollector",
            service_name="youtube",
            batch_size=config.batch_size,  # 50 channels per request
            max_workers=1,  # Sequential for quota control
            checkpoint_interval=10,  # Checkpoint every 10 batches
            enable_checkpoints=True
        )
        self.phase_name = "Phase 4: API Current"
        
        # Initialize YouTube API client
        try:
            self.youtube_client = create_youtube_client()
            self.logger.info("YouTube API client initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize YouTube API client: {e}")
            raise
    
    def get_collection_phase_name(self) -> str:
        """Get the name of this collection phase"""
        return "phase4_api_current"
    
    def _process_single_channel(self, channel: Channel) -> Optional[Channel]:
        """Process a single channel - not used for batch API calls"""
        return channel
    
    def collect_batch_data(self, channels: List[Channel]) -> List[Channel]:
        """
        Collect current metadata for a batch of channels
        
        Args:
            channels: List of channels to process
            
        Returns:
            List of processed channels with current metadata
        """
        if not channels:
            return []
        
        try:
            # Extract channel IDs for batch request
            channel_ids = [ch.channel_id for ch in channels]
            
            self.logger.info(f"Collecting current metadata for {len(channel_ids)} channels")
            
            # Make batch API request
            api_data = self._fetch_channel_batch(channel_ids)
            
            # Process results
            processed_channels = []
            for channel in channels:
                try:
                    # Find API data for this channel
                    channel_api_data = api_data.get(channel.channel_id)
                    
                    if channel_api_data:
                        # Update channel with current metadata
                        self._update_channel_with_api_data(channel, channel_api_data)
                        channel.processing_status = "enriched"
                        channel.processing_notes = "Successfully updated with current API data"
                    else:
                        channel.processing_status = "failed"
                        channel.processing_notes = "Channel not found in API response"
                    
                    processed_channels.append(channel)
                    
                except Exception as e:
                    self.logger.error(f"Failed to process channel {channel.channel_id}: {e}")
                    channel.processing_status = "failed"
                    channel.processing_notes = f"Processing error: {e}"
                    processed_channels.append(channel)
            
            return processed_channels
            
        except Exception as e:
            self.logger.error(f"Batch collection failed: {e}")
            # Return channels with failed status
            for channel in channels:
                channel.processing_status = "failed"
                channel.processing_notes = f"Batch collection error: {e}"
            return channels
    
    def _fetch_channel_batch(self, channel_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Fetch channel data from YouTube API in batch
        
        Args:
            channel_ids: List of channel IDs to fetch
            
        Returns:
            Dictionary mapping channel_id to API response data
        """
        try:
            # Use the existing YouTube client to make batch request
            response = self.youtube_client.get_channels(
                channel_ids=channel_ids,
                parts=['snippet', 'statistics', 'status', 'brandingSettings']
            )
            
            # Process API response into channel data dictionary
            channel_data = {}
            for item in response.get('items', []):
                channel_id = item['id']
                channel_data[channel_id] = {
                    'id': channel_id,
                    'snippet': item.get('snippet', {}),
                    'statistics': item.get('statistics', {}),
                    'status': item.get('status', {}),
                    'brandingSettings': item.get('brandingSettings', {})
                }
            
            self.logger.info(f"Successfully fetched data for {len(channel_data)} channels")
            return channel_data
            
        except Exception as e:
            self.logger.error(f"Failed to fetch channel batch: {e}")
            return {}
    
    def _update_channel_with_api_data(self, channel: Channel, api_data: Dict[str, Any]) -> None:
        """
        Update channel object with current API data
        
        Args:
            channel: Channel object to update
            api_data: API response data for the channel
        """
        try:
            snippet = api_data.get('snippet', {})
            statistics = api_data.get('statistics', {})
            status = api_data.get('status', {})
            branding = api_data.get('brandingSettings', {})
            
            # Update basic metadata
            if snippet.get('title'):
                channel.channel_name = snippet['title']
            
            if snippet.get('customUrl'):
                channel.custom_url = snippet['customUrl']
                # Extract handle from custom URL (e.g., @channelname)
                if snippet['customUrl'].startswith('@'):
                    channel.handle = snippet['customUrl']
                else:
                    channel.handle = f"@{snippet['customUrl']}"
            
            # Update current statistics
            current_stats = {
                'collection_date': datetime.now().isoformat(),
                'view_count': int(statistics.get('viewCount', 0)),
                'subscriber_count': int(statistics.get('subscriberCount', 0)),
                'video_count': int(statistics.get('videoCount', 0)),
                'data_source': 'youtube_api_current'
            }
            
            if hasattr(channel, 'current_api_stats'):
                channel.current_api_stats.update(current_stats)
            else:
                channel.current_api_stats = current_stats
            
            # Update channel status
            if status.get('privacyStatus'):
                channel.privacy_status = status['privacyStatus']
            
            # Update category
            if snippet.get('categoryId'):
                channel.category_id = snippet['categoryId']
            
            # Update country
            if snippet.get('country'):
                channel.country = snippet['country']
            
            # Update thumbnail URL
            if snippet.get('thumbnails', {}).get('default', {}).get('url'):
                channel.thumbnail_url = snippet['thumbnails']['default']['url']
            
            # Add collection source
            channel.add_collection_source('youtube_api_current', {
                'collection_date': datetime.now().isoformat(),
                'has_custom_url': bool(snippet.get('customUrl')),
                'has_handle': bool(getattr(channel, 'handle', None)),
                'is_active': status.get('privacyStatus') == 'public'
            })
            
            self.logger.debug(f"Updated channel {channel.channel_id} with current API data")
            
        except Exception as e:
            self.logger.error(f"Failed to update channel {channel.channel_id} with API data: {e}")
            raise