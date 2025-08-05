"""
YouTube API Data Collector for YouNiverse Dataset Enrichment

Phase 1: API enrichment implementation - batch processing of 50 channels per request
with intelligent quota management and comprehensive error handling.

This collector enriches channel data with current metadata from the YouTube Data API v3,
including channel name, category, country, subscriber count, view count, and video count.

Author: feature-developer
"""

import time
from typing import List, Dict, Any, Optional
from datetime import datetime, date
import logging

from .base_collector import BaseCollector
from ..models.channel import Channel, ChannelStatus, ChannelCategory
from ..utils.api_helpers import create_youtube_client, YouTubeAPIClient
from ..core.config import config_manager


class YouTubeAPICollector(BaseCollector):
    """
    YouTube Data API v3 collector for Phase 1 channel enrichment
    
    Features:
    - Batch processing: 50 channels per request
    - Quota management: 10,000 units/day with tracking
    - Rate limiting: 100 requests/second with token bucket
    - Error handling: Channel not found, quota exceeded, API errors
    - Authentication: API key from environment
    """
    
    def __init__(self):
        # Initialize with YouTube-specific configuration
        config = config_manager.get_youtube_config()
        
        super().__init__(
            collector_name="YouTubeAPI",
            service_name="youtube",
            batch_size=config.batch_size,  # 50 channels per request
            max_workers=1,  # Sequential processing for quota control
            checkpoint_interval=5,  # Checkpoint every 5 batches
            enable_checkpoints=True
        )
        
        # Initialize YouTube API client
        try:
            self.youtube_client = create_youtube_client()
            self.logger.info("YouTube API client initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize YouTube API client: {e}")
            raise e
        
        # Quota tracking
        self.quota_used = 0
        self.quota_limit = config.quota_limit
        self.cost_per_request = config.cost_per_request
        
        # Collection metrics
        self.channels_enriched = 0
        self.channels_not_found = 0
        self.api_errors = 0
    
    def get_collection_phase_name(self) -> str:
        """Get the name of this collection phase"""
        return "Phase 1: YouTube API Enrichment"
    
    def _check_quota_availability(self, channels_count: int) -> bool:
        """
        Check if we have enough quota for the request
        
        Args:
            channels_count: Number of channels to process
            
        Returns:
            True if quota is available, False otherwise
        """
        # Calculate requests needed (50 channels per request)
        requests_needed = (channels_count + self.batch_size - 1) // self.batch_size
        quota_needed = requests_needed * self.cost_per_request
        
        remaining_quota = self.quota_limit - self.quota_used
        
        if quota_needed > remaining_quota:
            self.logger.warning(f"Insufficient quota: need {quota_needed}, have {remaining_quota}")
            return False
        
        return True
    
    def _process_youtube_response(self, response_data: Dict[str, Any], 
                                requested_channels: List[Channel]) -> List[Channel]:
        """
        Process YouTube API response and update channel objects
        
        Args:
            response_data: YouTube API response
            requested_channels: Original channels that were requested
            
        Returns:
            List of successfully processed channels
        """
        processed_channels = []
        
        # Create mapping of channel ID to channel object
        channel_map = {ch.channel_id: ch for ch in requested_channels}
        
        # Track which channels were found
        found_channel_ids = set()
        
        # Process each item in the response
        for item in response_data.get('items', []):
            channel_id = item['id']
            found_channel_ids.add(channel_id)
            
            if channel_id not in channel_map:
                self.logger.warning(f"Received data for unexpected channel: {channel_id}")
                continue
            
            channel = channel_map[channel_id]
            
            try:
                # Extract snippet data
                snippet = item.get('snippet', {})
                
                # Update channel metadata
                channel.channel_name = snippet.get('title', channel.channel_name)
                channel.description = snippet.get('description', '')[:500]  # Limit description length
                channel.country = snippet.get('country', '')
                
                # Parse category
                default_category = snippet.get('defaultLanguage', '')
                if default_category:
                    channel.language = default_category
                
                # Parse creation date
                published_at = snippet.get('publishedAt')
                if published_at:
                    try:
                        # Parse ISO format: 2019-09-20T14:30:59Z
                        created_datetime = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
                        channel.created_date = created_datetime.date()
                    except ValueError as e:
                        self.logger.warning(f"Could not parse creation date for {channel_id}: {published_at}")
                
                # Extract statistics
                statistics = item.get('statistics', {})
                
                subscriber_count = int(statistics.get('subscriberCount', 0))
                view_count = int(statistics.get('viewCount', 0))
                video_count = int(statistics.get('videoCount', 0))
                
                # Update current metrics
                channel.update_current_metrics(
                    subscriber_count=subscriber_count,
                    total_view_count=view_count,
                    video_count=video_count
                )
                
                # Extract status information
                status_info = item.get('status', {})
                privacy_status = status_info.get('privacyStatus', 'public')
                
                # Determine channel status
                if privacy_status == 'private':
                    channel.status = ChannelStatus.PRIVATE
                elif privacy_status == 'unlisted':
                    channel.status = ChannelStatus.INACTIVE
                else:
                    channel.status = ChannelStatus.ACTIVE
                
                # Add collection metadata
                channel.add_collection_source('youtube_api', {
                    'timestamp': datetime.now().isoformat(),
                    'privacy_status': privacy_status,
                    'subscriber_count_hidden': not statistics.get('hiddenSubscriberCount', True),
                    'quota_cost': self.cost_per_request
                })
                
                # Update processing status
                channel.processing_status = "enriched"
                channel.processing_notes = "Successfully enriched via YouTube API"
                
                processed_channels.append(channel)
                self.channels_enriched += 1
                self.stats.data_points_collected += 1
                
            except Exception as e:
                self.logger.error(f"Error processing channel {channel_id}: {e}")
                channel.add_validation_flag('youtube_api_processing_error', str(e))
                channel.processing_status = "failed"
                channel.processing_notes = f"YouTube API processing error: {e}"
                self.api_errors += 1
        
        # Handle channels that were not found in the response
        for channel in requested_channels:
            if channel.channel_id not in found_channel_ids:
                self.logger.warning(f"Channel not found in YouTube API: {channel.channel_id}")
                channel.status = ChannelStatus.TERMINATED
                channel.add_validation_flag('youtube_api_not_found', 'Channel not found in YouTube API response')
                channel.processing_status = "not_found"
                channel.processing_notes = "Channel not found in YouTube API"
                self.channels_not_found += 1
                
                # Still add to processed list for tracking
                processed_channels.append(channel)
        
        return processed_channels
    
    def _process_single_batch(self, channels: List[Channel]) -> List[Channel]:
        """
        Process a single batch of channels through YouTube API
        
        Args:
            channels: List of channels to process (up to 50)
            
        Returns:
            List of processed channels
        """
        if not channels:
            return []
        
        # Check quota before making request
        if not self._check_quota_availability(len(channels)):
            raise Exception(f"Insufficient YouTube API quota: {self.quota_used}/{self.quota_limit}")
        
        channel_ids = [ch.channel_id for ch in channels]
        
        self.logger.debug(f"Requesting data for {len(channel_ids)} channels")
        
        try:
            # Make API request
            response_data = self.youtube_client.get_channels(
                channel_ids=channel_ids,
                parts=['snippet', 'statistics', 'status']
            )
            
            # Update quota usage
            self.quota_used += self.cost_per_request
            
            # Process response
            processed_channels = self._process_youtube_response(response_data, channels)
            
            self.logger.info(f"Successfully processed {len(processed_channels)} channels from batch")
            
            return processed_channels
            
        except Exception as e:
            self.logger.error(f"YouTube API request failed: {e}")
            
            # Mark all channels in batch as failed
            for channel in channels:
                channel.add_validation_flag('youtube_api_request_failed', str(e))
                channel.processing_status = "failed"
                channel.processing_notes = f"YouTube API request failed: {e}"
            
            self.api_errors += 1
            raise e
    
    def _process_single_channel(self, channel: Channel) -> Optional[Channel]:
        """
        Process a single channel - not used directly, but required by base class
        
        Note: This collector processes channels in batches, so this method
        delegates to batch processing.
        """
        try:
            processed_channels = self._process_single_batch([channel])
            return processed_channels[0] if processed_channels else None
        except Exception as e:
            self.logger.error(f"Failed to process single channel {channel.channel_id}: {e}")
            return None
    
    def collect(self, channels: List[Channel]) -> List[Channel]:
        """
        Main collection method - overridden to implement batch processing
        
        Args:
            channels: List of channels to enrich
            
        Returns:
            List of enriched channels
        """
        if not channels:
            self.logger.warning("No channels provided for YouTube API collection")
            return []
        
        # Initialize progress
        self.progress.total_channels = len(channels)
        self.progress.start_time = datetime.now()
        self.progress.current_phase = self.get_collection_phase_name()
        
        self._running = True
        self._stop_requested = False
        
        # Load checkpoint if available
        checkpoint = self.load_checkpoint()
        processed_channel_ids = set()
        
        if checkpoint:
            processed_channel_ids = set(checkpoint.get('processed_channels', []))
            self.logger.info(f"Resuming from checkpoint with {len(processed_channel_ids)} already processed channels")
        
        # Filter out already processed channels
        remaining_channels = [ch for ch in channels if ch.channel_id not in processed_channel_ids]
        self.progress.processed_channels = len(processed_channel_ids)
        self.progress.successful_channels = self.progress.processed_channels
        
        self.logger.info(f"Starting YouTube API collection for {len(remaining_channels)} channels")
        self.logger.info(f"Initial quota usage: {self.quota_used}/{self.quota_limit}")
        
        results = []
        
        # Process in batches of 50 (YouTube API limit)
        batch_size = self.batch_size
        batches = [remaining_channels[i:i + batch_size] for i in range(0, len(remaining_channels), batch_size)]
        self.progress.total_batches = len(batches)
        
        try:
            for batch_idx, batch in enumerate(batches):
                if self.should_stop():
                    self.logger.info("Collection stopped by request")
                    break
                
                self.progress.current_batch = batch_idx + 1
                self.logger.info(f"Processing batch {batch_idx + 1}/{len(batches)} ({len(batch)} channels)")
                
                # Process batch
                try:
                    batch_results = self._process_single_batch(batch)
                    results.extend(batch_results)
                    
                    self.progress.successful_channels += len(batch_results)
                    self.progress.processed_channels += len(batch)
                    
                except Exception as e:
                    self.logger.error(f"Batch {batch_idx + 1} failed: {e}")
                    self.progress.failed_channels += len(batch)
                    self.progress.processed_channels += len(batch)
                    
                    # Add failed channels to results for tracking
                    results.extend(batch)
                
                # Save checkpoint periodically
                if self.enable_checkpoints and (batch_idx + 1) % self.checkpoint_interval == 0:
                    all_processed = list(processed_channel_ids) + [ch.channel_id for ch in results]
                    self.save_checkpoint(all_processed)
                
                # Notify progress
                self._notify_progress()
                
                self.logger.info(f"Batch {batch_idx + 1} completed. "
                               f"Success rate: {self.progress.get_success_rate():.1f}%. "
                               f"Quota used: {self.quota_used}/{self.quota_limit}")
                
                # Check if we're approaching quota limits
                if self.quota_used >= self.quota_limit * 0.9:
                    self.logger.warning(f"Approaching quota limit: {self.quota_used}/{self.quota_limit}")
        
        except KeyboardInterrupt:
            self.logger.info("Collection interrupted by user")
            self._stop_requested = True
        
        except Exception as e:
            self.logger.error(f"Unexpected error during collection: {e}")
            raise e
        
        finally:
            self._running = False
            self.progress.end_time = datetime.now()
            
            # Final checkpoint
            if self.enable_checkpoints and results:
                all_processed = list(processed_channel_ids) + [ch.channel_id for ch in results]
                self.save_checkpoint(all_processed)
            
            # Log final statistics
            self.logger.info(f"YouTube API collection completed")
            self.logger.info(f"Total channels: {self.progress.total_channels}")
            self.logger.info(f"Processed: {self.progress.processed_channels}")
            self.logger.info(f"Enriched: {self.channels_enriched}")
            self.logger.info(f"Not found: {self.channels_not_found}")
            self.logger.info(f"API errors: {self.api_errors}")
            self.logger.info(f"Success rate: {self.progress.get_success_rate():.1f}%")
            self.logger.info(f"Final quota usage: {self.quota_used}/{self.quota_limit}")
            self.logger.info(f"Elapsed time: {self.progress.get_elapsed_time()}")
        
        return results
    
    def get_quota_usage(self) -> Dict[str, Any]:
        """Get current quota usage information"""
        return {
            'quota_used': self.quota_used,
            'quota_limit': self.quota_limit,
            'quota_remaining': self.quota_limit - self.quota_used,
            'quota_percentage': (self.quota_used / self.quota_limit) * 100 if self.quota_limit > 0 else 0,
            'cost_per_request': self.cost_per_request
        }
    
    def get_collection_summary(self) -> Dict[str, Any]:
        """Get summary of collection results"""
        return {
            'collector_name': self.collector_name,
            'phase': self.get_collection_phase_name(),
            'total_processed': self.progress.processed_channels,
            'channels_enriched': self.channels_enriched,
            'channels_not_found': self.channels_not_found,
            'api_errors': self.api_errors,
            'success_rate': self.progress.get_success_rate(),
            'quota_usage': self.get_quota_usage(),
            'elapsed_time': str(self.progress.get_elapsed_time()),
            'data_points_collected': self.stats.data_points_collected
        }