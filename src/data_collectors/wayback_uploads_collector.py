"""
Wayback Machine Upload Data Collector
Phase 2B: Historical Social Blade upload data collection (September 2019 - December 2022)

This collector extracts upload/video count data from Social Blade main pages
archived in the Wayback Machine. Focuses on main channel pages to collect 
historical upload counts where available in snapshots.

Author: feature-developer
"""

import sys
import time
import json
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urljoin, quote
import re
from bs4 import BeautifulSoup

from src.data_collectors.base_collector import BaseCollector, CollectionProgress
from src.models.channel import Channel
from src.utils.scraping_utils import AntiDetectionScraper, WaybackMachineHelper
from src.utils.api_helpers import RateLimiter
from src.core.config import config_manager


class WaybackUploadsCollector(BaseCollector):
    """
    Wayback Machine collector for Social Blade historical upload data
    
    Collects upload/video count data from Sep 2019 - Dec 2022 
    using available snapshots of Social Blade main channel pages.
    """
    
    def __init__(self):
        super().__init__(
            collector_name="WaybackUploadsCollector",
            service_name="wayback",  # Use wayback service
            batch_size=10,  # Smaller batches for respectful scraping
            max_workers=1,  # Single worker to respect rate limits
            checkpoint_interval=50,  # Checkpoint every 50 channels
            enable_checkpoints=True
        )
        self.phase_name = "Phase 2B: Wayback Uploads"
        
        # Get configuration
        config = config_manager.get_wayback_config()
        
        # Initialize scraping utilities
        self.scraping_utils = AntiDetectionScraper()
        self.wayback_helper = WaybackMachineHelper(self.scraping_utils)
        
        # Rate limiting: 1 request per second (respectful)
        self.rate_limiter = RateLimiter(
            requests_per_second=config.rate_limiting.requests_per_second,
            burst_allowance=config.rate_limiting.burst_allowance
        )
        
        # Target date range: Sep 2019 - Dec 2022 (same as Phase 2A)
        self.start_date = datetime(2019, 9, 1)
        self.end_date = datetime(2022, 12, 31)
        
        # Upload data extraction patterns for Social Blade main pages
        self.upload_patterns = {
            'video_count_patterns': [
                r'class="youtube-stats-header-uploads"[^>]*>\s*([0-9,]+)',
                r'<strong[^>]*>Uploads</strong>[^<]*<br[^>]*>\s*([0-9,]+)',
                r'id="youtube-stats-header-uploads"[^>]*>\s*([0-9,]+)',
                r'uploads?["\'][^>]*>\s*([0-9,]+)',
                r'video[s]?["\'][^>]*>\s*([0-9,]+)',
                r'(\d+(?:,\d{3})*)\s*(?:uploads?|videos?)',
            ],
            'metadata_patterns': [
                r'<meta property="og:description" content="[^"]*(\d+(?:,\d{3})*)\s*(?:uploads?|videos?)',
                r'<title>[^<]*?(\d+(?:,\d{3})*)\s*(?:uploads?|videos?)',
            ],
            'json_ld_patterns': [
                r'"videoCount":\s*"?(\d+)"?',
                r'"uploads":\s*"?(\d+)"?',
            ]
        }
        
        self.logger.info(f"Initialized {self.collector_name} for date range {self.start_date.date()} to {self.end_date.date()}")
    
    def get_collection_phase_name(self) -> str:
        """Get the name of this collection phase"""
        return "phase2b_wayback_uploads"
    
    def _process_single_channel(self, channel: Channel) -> Optional[Channel]:
        """
        Process a single channel for BaseCollector compatibility
        
        Args:
            channel: Channel to process
            
        Returns:
            Processed channel or None if failed
        """
        try:
            result = self.collect_channel_data(channel)
            
            if result['success'] and result['upload_data']:
                # Update channel with upload data
                if hasattr(channel, 'upload_history'):
                    channel.upload_history.extend(result['upload_data'])
                else:
                    channel.upload_history = result['upload_data']
                
                # Update historical coverage in channel.history object
                upload_data = result['upload_data']
                if upload_data:
                    # Extract dates from upload data
                    dates = [datetime.fromisoformat(point['date']).date() for point in upload_data]
                    first_date = min(dates)
                    last_date = max(dates)
                    total_points = len(upload_data)
                    
                    # Update the channel's history object (upload-specific tracking)
                    if not hasattr(channel.history, 'upload_data_points'):
                        channel.history.upload_data_points = 0
                    channel.history.upload_data_points = total_points
                    
                    # Add collection source
                    channel.add_collection_source('wayback_uploads', {
                        'snapshots_used': result.get('snapshots_used', []),
                        'data_points': total_points,
                        'date_range': f"{first_date} to {last_date}"
                    })
                    
                    # Update processing status
                    channel.processing_status = "enriched"
                    channel.processing_notes = f"Successfully collected {total_points} upload data points"
                else:
                    channel.processing_status = "failed"
                    channel.processing_notes = "No upload data points collected"
                
                return channel
            else:
                self.logger.warning(f"No upload data collected for {channel.channel_id}")
                channel.processing_status = "failed"
                channel.processing_notes = result.get('error', 'No upload data collected')
                return channel  # Return channel even if no data collected
                
        except Exception as e:
            self.logger.error(f"Failed to process channel {channel.channel_id}: {e}")
            channel.processing_status = "failed"
            channel.processing_notes = f"Processing error: {e}"
            return channel
    
    def collect_channel_data(self, channel: Channel) -> Dict[str, Any]:
        """
        Collect historical upload data for a single channel
        
        Args:
            channel: Channel object to collect data for
            
        Returns:
            Dictionary containing historical upload data
        """
        start_time = time.time()
        result = {
            'channel_id': channel.channel_id,
            'collection_phase': 'phase2b_wayback_uploads',
            'collection_timestamp': datetime.now().isoformat(),
            'success': False,
            'error': None,
            'data_points_collected': 0,
            'snapshots_used': [],
            'upload_data': [],  # Upload data points
            'collection_duration': 0
        }
        
        try:
            self.logger.info(f"Starting upload data collection for {channel.channel_id}")
            
            # Step 1: Get channel name for Social Blade URL construction
            channel_name = self._get_channel_name_for_socialblade(channel)
            if not channel_name:
                raise ValueError(f"Cannot determine Social Blade channel name for {channel.channel_id}")
            
            # Step 2: Find available snapshots using CDX API
            snapshots = self._find_available_snapshots(channel_name)
            if not snapshots:
                raise ValueError(f"No suitable Wayback snapshots found for {channel_name}")
            
            result['snapshots_used'] = [s['timestamp'] for s in snapshots]
            self.logger.info(f"Found {len(snapshots)} available snapshots for {channel_name}")
            
            # Step 3: Extract upload data from snapshots
            upload_data = []
            for snapshot in snapshots:
                try:
                    with self.rate_limiter:  # Respect rate limiting
                        data_point = self._extract_upload_data_from_snapshot(snapshot, channel_name)
                        if data_point:
                            upload_data.append(data_point)
                            self.logger.debug(f"Extracted upload data from snapshot {snapshot['timestamp']}")
                except Exception as e:
                    self.logger.warning(f"Failed to extract from snapshot {snapshot['timestamp']}: {e}")
                    continue
            
            # Step 4: Process and validate upload data
            if upload_data:
                processed_data = self._process_upload_data(upload_data, channel)
                result['upload_data'] = processed_data
                result['data_points_collected'] = len(processed_data)
                
                # Validate data quality
                validation_result = self._validate_upload_data(processed_data, channel)
                if validation_result['valid']:
                    result['success'] = True
                    self.logger.info(f"Successfully collected {len(processed_data)} upload data points for {channel.channel_id}")
                else:
                    result['error'] = f"Upload data validation failed: {validation_result['reason']}"
                    self.logger.warning(f"Upload data validation failed for {channel.channel_id}: {validation_result['reason']}")
            else:
                result['error'] = "No upload data extracted from any snapshots"
                self.logger.warning(f"No upload data found for {channel.channel_id}")
        
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"Upload data collection failed for {channel.channel_id}: {e}")
        
        finally:
            result['collection_duration'] = time.time() - start_time
            
            # Apply current data interpolation if we have successful wayback data
            if result['success'] and result['upload_data']:
                try:
                    current_interpolation = self._interpolate_to_current_api(channel, result['upload_data'])
                    if current_interpolation:
                        # Add interpolated points to upload data
                        result['upload_data'].extend(current_interpolation)
                        result['data_points_collected'] = len(result['upload_data'])
                        result['current_interpolation_applied'] = True
                        self.logger.info(f"Added {len(current_interpolation)} current interpolation points")
                    else:
                        result['current_interpolation_applied'] = False
                except Exception as e:
                    self.logger.warning(f"Current data interpolation failed: {e}")
                    result['current_interpolation_applied'] = False
            else:
                result['current_interpolation_applied'] = False
        
        return result
    
    def _get_channel_name_for_socialblade(self, channel: Channel) -> Optional[str]:
        """
        Determine the Social Blade channel identifier for URL construction
        
        Social Blade URLs use:
        - Channel ID directly (e.g., UCuAXFkgsw1L7xaCfnd5JJOw)
        - Channel handle (for newer channels with @handles)
        """
        # For Social Blade, we primarily use the channel ID directly
        # This is the most reliable approach for /channel/ URLs
        if channel.channel_id:
            return channel.channel_id
        
        # Try channel handle as fallback (remove @ symbol)
        if hasattr(channel, 'channel_handle') and channel.channel_handle:
            return channel.channel_handle.replace('@', '')
        
        # This shouldn't happen, but just in case
        return None
    
    def _find_available_snapshots(self, channel_name: str) -> List[Dict[str, Any]]:
        """
        Find available Wayback Machine snapshots for Social Blade main page
        
        Strategy:
        1. Query CDX API for all main page snapshots in target range
        2. Select distributed snapshots across time period
        3. Prioritize snapshots likely to have upload data
        """
        try:
            # Target Social Blade main page (use /channel/ format)
            target_url = f"https://socialblade.com/youtube/channel/{channel_name}"
            
            # Query CDX API for snapshots
            snapshots = self.wayback_helper.get_snapshots(
                url=target_url,
                from_date=self.start_date.strftime('%Y%m%d'),
                to_date=self.end_date.strftime('%Y%m%d'),
                limit=20  # Get up to 20 snapshots distributed across time
            )
            
            if not snapshots:
                self.logger.warning(f"No snapshots found for {target_url}")
                return []
            
            # Filter and score snapshots
            valid_snapshots = []
            for snapshot in snapshots:
                if snapshot.get('statuscode') == '200':
                    valid_snapshots.append(snapshot)
            
            # Distribute snapshots across time period for better coverage
            distributed_snapshots = self._distribute_snapshots_by_time(valid_snapshots)
            
            self.logger.info(f"Selected {len(distributed_snapshots)} distributed snapshots from {len(snapshots)} available")
            return distributed_snapshots
            
        except Exception as e:
            self.logger.error(f"Failed to find snapshots for {channel_name}: {e}")
            return []
    
    def _distribute_snapshots_by_time(self, snapshots: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Distribute snapshots evenly across the time period for better coverage
        """
        if not snapshots:
            return []
        
        # Sort snapshots by timestamp
        sorted_snapshots = sorted(snapshots, key=lambda x: x['timestamp'])
        
        # If we have many snapshots, select a representative subset
        if len(sorted_snapshots) <= 10:
            return sorted_snapshots
        
        # Select ~10 snapshots distributed across time
        total_snapshots = len(sorted_snapshots)
        indices = [int(i * total_snapshots / 10) for i in range(10)]
        
        # Ensure we don't exceed list bounds
        indices = [min(i, total_snapshots - 1) for i in indices]
        
        # Remove duplicates while preserving order
        seen = set()
        distributed = []
        for i in indices:
            if i not in seen:
                distributed.append(sorted_snapshots[i])
                seen.add(i)
        
        return distributed
    
    def _extract_upload_data_from_snapshot(self, snapshot: Dict[str, Any], channel_name: str) -> Optional[Dict[str, Any]]:
        """
        Extract upload count data from a Wayback Machine snapshot
        
        Args:
            snapshot: Snapshot metadata from CDX API
            channel_name: Channel identifier for URL construction
            
        Returns:
            Upload data point or None if extraction failed
        """
        try:
            # Construct Wayback URL
            wayback_url = f"https://web.archive.org/web/{snapshot['timestamp']}/{snapshot['original']}"
            
            self.logger.debug(f"Fetching snapshot: {wayback_url}")
            
            # Fetch the archived page
            response = self.scraping_utils.get(wayback_url)
            
            if not response.text:
                raise ValueError("Empty response from Wayback Machine")
            
            # Parse HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract upload count using multiple strategies
            upload_count = self._extract_upload_count(response.text, soup)
            
            if upload_count is not None:
                # Convert timestamp to datetime
                timestamp_str = snapshot['timestamp']
                snapshot_date = datetime.strptime(timestamp_str, '%Y%m%d%H%M%S')
                
                return {
                    'date': snapshot_date.isoformat(),
                    'upload_count': upload_count,
                    'snapshot_timestamp': timestamp_str,
                    'source': 'wayback_uploads',
                    'url': wayback_url
                }
            else:
                self.logger.debug(f"Could not extract upload count from snapshot {snapshot['timestamp']}")
                return None
                
        except Exception as e:
            self.logger.error(f"Failed to extract upload data from snapshot {snapshot['timestamp']}: {e}")
            return None
    
    def _extract_upload_count(self, html_content: str, soup: BeautifulSoup) -> Optional[int]:
        """
        Extract upload/video count from Social Blade page using multiple strategies
        """
        # Strategy 1: Try specific CSS class/ID patterns
        upload_count = self._try_css_selectors(soup)
        if upload_count is not None:
            return upload_count
        
        # Strategy 2: Try regex patterns on HTML content
        upload_count = self._try_regex_patterns(html_content)
        if upload_count is not None:
            return upload_count
        
        # Strategy 3: Try JSON-LD structured data
        upload_count = self._try_json_ld_extraction(soup)
        if upload_count is not None:
            return upload_count
        
        # Strategy 4: Try metadata extraction
        upload_count = self._try_metadata_extraction(soup)
        if upload_count is not None:
            return upload_count
        
        return None
    
    def _try_css_selectors(self, soup: BeautifulSoup) -> Optional[int]:
        """Try to extract upload count using CSS selectors"""
        try:
            # Common Social Blade CSS patterns for upload counts
            selectors = [
                '.youtube-stats-header-uploads',
                '#youtube-stats-header-uploads',
                '.stats-uploads',
                '.upload-count',
                '.video-count'
            ]
            
            for selector in selectors:
                element = soup.select_one(selector)
                if element:
                    text = element.get_text(strip=True)
                    count = self._parse_number(text)
                    if count is not None:
                        return count
        
        except Exception as e:
            self.logger.debug(f"CSS selector extraction failed: {e}")
        
        return None
    
    def _try_regex_patterns(self, html_content: str) -> Optional[int]:
        """Try to extract upload count using regex patterns"""
        try:
            for pattern_category, patterns in self.upload_patterns.items():
                for pattern in patterns:
                    matches = re.findall(pattern, html_content, re.IGNORECASE)
                    for match in matches:
                        count = self._parse_number(match)
                        if count is not None and count > 0:
                            return count
        
        except Exception as e:
            self.logger.debug(f"Regex pattern extraction failed: {e}")
        
        return None
    
    def _try_json_ld_extraction(self, soup: BeautifulSoup) -> Optional[int]:
        """Try to extract upload count from JSON-LD structured data"""
        try:
            # Look for JSON-LD script tags
            json_scripts = soup.find_all('script', type='application/ld+json')
            
            for script in json_scripts:
                try:
                    data = json.loads(script.string)
                    
                    # Check for video count in various JSON-LD properties
                    if isinstance(data, dict):
                        for key in ['videoCount', 'uploads', 'numberOfVideos']:
                            if key in data:
                                count = self._parse_number(str(data[key]))
                                if count is not None and count > 0:
                                    return count
                
                except json.JSONDecodeError:
                    continue
        
        except Exception as e:
            self.logger.debug(f"JSON-LD extraction failed: {e}")
        
        return None
    
    def _try_metadata_extraction(self, soup: BeautifulSoup) -> Optional[int]:
        """Try to extract upload count from page metadata"""
        try:
            # Check meta tags
            meta_tags = soup.find_all('meta')
            for meta in meta_tags:
                content = meta.get('content', '')
                if content and ('upload' in content.lower() or 'video' in content.lower()):
                    # Look for numbers in the content
                    numbers = re.findall(r'(\d+(?:,\d{3})*)', content)
                    for number in numbers:
                        count = self._parse_number(number)
                        if count is not None and count > 0:
                            return count
            
            # Check title tag
            title = soup.find('title')
            if title:
                title_text = title.get_text()
                if 'upload' in title_text.lower() or 'video' in title_text.lower():
                    numbers = re.findall(r'(\d+(?:,\d{3})*)', title_text)
                    for number in numbers:
                        count = self._parse_number(number)
                        if count is not None and count > 0:
                            return count
        
        except Exception as e:
            self.logger.debug(f"Metadata extraction failed: {e}")
        
        return None
    
    def _parse_number(self, text: str) -> Optional[int]:
        """Parse a number from text, handling commas and other formatting"""
        try:
            if not text:
                return None
            
            # Remove non-digit characters except commas
            cleaned = re.sub(r'[^\d,]', '', str(text).strip())
            if not cleaned:
                return None
            
            # Remove commas and convert to int
            number = int(cleaned.replace(',', ''))
            
            # Sanity check: reasonable upload count range
            if 0 <= number <= 100000:  # 0 to 100K uploads seems reasonable
                return number
            
        except (ValueError, TypeError):
            pass
        
        return None
    
    def _process_upload_data(self, raw_data: List[Dict[str, Any]], channel: Channel) -> List[Dict[str, Any]]:
        """
        Process and clean raw upload data
        
        Steps:
        1. Sort by date
        2. Remove duplicates
        3. Add derived metrics
        4. Validate progression
        """
        processed_data = []
        
        try:
            if not raw_data:
                return []
            
            # Sort by date
            raw_data.sort(key=lambda x: x['date'])
            
            # Remove duplicates by date (keep first occurrence)
            seen_dates = set()
            deduplicated_data = []
            for point in raw_data:
                date_key = point['date'][:10]  # Just date part
                if date_key not in seen_dates:
                    seen_dates.add(date_key)
                    deduplicated_data.append(point)
            
            # Interpolate missing weeks between data points
            interpolated_data = self._interpolate_missing_weeks(deduplicated_data)
            
            # Process each point
            for i, point in enumerate(interpolated_data):
                processed_point = point.copy()
                
                # Calculate delta from previous point
                if i > 0:
                    prev_point = interpolated_data[i-1]
                    processed_point['delta_uploads'] = point['upload_count'] - prev_point['upload_count']
                else:
                    processed_point['delta_uploads'] = 0
                
                # Add quality flags
                processed_point['quality_flags'] = self._assess_upload_point_quality(processed_point, i, interpolated_data)
                
                processed_data.append(processed_point)
            
        except Exception as e:
            self.logger.error(f"Failed to process upload data: {e}")
        
        return processed_data
    
    def _interpolate_missing_weeks(self, data_points: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Interpolate missing weekly upload data points between existing snapshots
        
        Strategy:
        1. Find gaps of > 7 days between consecutive data points
        2. Calculate weekly interpolation steps based on upload count changes
        3. Create weekly data points with realistic upload progression
        4. Mark interpolated points with source flags
        """
        if len(data_points) < 2:
            return data_points
        
        interpolated_data = []
        
        try:
            for i in range(len(data_points)):
                current_point = data_points[i]
                interpolated_data.append(current_point)
                
                # Check if there's a next point to interpolate towards
                if i < len(data_points) - 1:
                    next_point = data_points[i + 1]
                    
                    # Calculate time gap
                    current_date = datetime.fromisoformat(current_point['date'])
                    next_date = datetime.fromisoformat(next_point['date'])
                    days_gap = (next_date - current_date).days
                    
                    # If gap is > 14 days, create weekly interpolated points
                    if days_gap > 14:
                        weeks_to_fill = max(1, days_gap // 7 - 1)  # Number of weeks to interpolate
                        
                        current_uploads = current_point['upload_count']
                        next_uploads = next_point['upload_count']
                        upload_change = next_uploads - current_uploads
                        
                        # Create weekly steps
                        for week in range(1, weeks_to_fill + 1):
                            interpolation_date = current_date + timedelta(weeks=week)
                            
                            # Calculate interpolated upload count
                            # Use smooth progression: uploads typically increase steadily
                            progress = week / (weeks_to_fill + 1)
                            interpolated_uploads = int(current_uploads + (upload_change * progress))
                            
                            # Ensure uploads don't decrease unreasonably
                            if interpolated_uploads < current_uploads:
                                interpolated_uploads = current_uploads
                            
                            # Create interpolated point
                            interpolated_point = {
                                'date': interpolation_date.isoformat(),
                                'upload_count': interpolated_uploads,
                                'snapshot_timestamp': f"interpolated_{week}",
                                'source': 'wayback_uploads_interpolated',
                                'url': f"interpolated_between_{current_point['snapshot_timestamp']}_{next_point['snapshot_timestamp']}",
                                'interpolated': True,
                                'interpolation_source': {
                                    'from_snapshot': current_point['snapshot_timestamp'],
                                    'to_snapshot': next_point['snapshot_timestamp'],
                                    'interpolation_step': week,
                                    'total_steps': weeks_to_fill + 1
                                }
                            }
                            
                            interpolated_data.append(interpolated_point)
                            
            # Sort by date to ensure proper chronological order
            interpolated_data.sort(key=lambda x: x['date'])
            
            self.logger.info(f"Interpolated {len(interpolated_data) - len(data_points)} weekly upload data points")
            return interpolated_data
            
        except Exception as e:
            self.logger.error(f"Failed to interpolate upload data: {e}")
            return data_points  # Return original data if interpolation fails
    
    def _assess_upload_point_quality(self, point: Dict[str, Any], index: int, all_data: List[Dict[str, Any]]) -> List[str]:
        """Assess the quality of a single upload data point"""
        flags = []
        
        try:
            # Check if this is an interpolated point
            if point.get('interpolated', False):
                flags.append('interpolated_data')
            
            # Check for unreasonable values
            if point['upload_count'] < 0:
                flags.append('negative_uploads')
            
            # Check for unreasonable deltas
            delta = point.get('delta_uploads', 0)
            if abs(delta) > 1000:  # More than 1000 uploads change seems suspicious
                flags.append('large_upload_change')
            
            # Check for decreasing uploads (should only increase or stay same)
            # Be more lenient with interpolated points
            if delta < 0:
                if point.get('interpolated', False):
                    flags.append('interpolated_decrease')  # Flag but don't fail
                else:
                    flags.append('decreasing_uploads')
            
        except Exception:
            flags.append('processing_error')
        
        return flags
    
    def _validate_upload_data(self, data_points: List[Dict[str, Any]], channel: Channel) -> Dict[str, Any]:
        """
        Validate upload data quality and consistency
        """
        if not data_points:
            return {'valid': False, 'reason': 'No data points to validate'}
        
        try:
            # Basic validation checks
            upload_counts = [p['upload_count'] for p in data_points]
            
            # Check for reasonable progression (uploads should generally increase)
            if len(upload_counts) > 1:
                first_count = upload_counts[0]
                last_count = upload_counts[-1]
                
                # Allow for some variance, but generally uploads should increase over time
                if last_count < first_count * 0.8:  # Allow up to 20% decrease (account for data errors)
                    return {
                        'valid': False,
                        'reason': f'Upload count decreased significantly: {first_count} to {last_count}',
                        'message': 'Upload data shows unrealistic decrease over time'
                    }
            
            # Check for reasonable upload counts
            max_uploads = max(upload_counts)
            if max_uploads > 50000:  # More than 50K uploads seems suspicious
                return {
                    'valid': False,
                    'reason': f'Unreasonably high upload count: {max_uploads}',
                    'message': 'Upload count exceeds reasonable limits'
                }
            
            # If we get here, validation passed
            return {
                'valid': True,
                'reason': 'Upload data validation passed',
                'message': f"Successfully validated {len(data_points)} upload data points"
            }
            
        except Exception as e:
            return {
                'valid': False,
                'reason': f'Validation error: {e}',
                'message': 'Error during upload data validation'
            }
    
    def _interpolate_to_current_api(self, channel: Channel, wayback_data: List[Dict[str, Any]]) -> Optional[List[Dict[str, Any]]]:
        """
        Interpolate upload data from last wayback point to current API data
        
        This fills the critical gap between Aug 2022 wayback data and current YouTube API data,
        ensuring continuous upload progression with no missing data points.
        
        Args:
            channel: Channel object (should have current API stats if Phase 4 ran)
            wayback_data: Existing wayback upload data points
            
        Returns:
            List of interpolated data points from last wayback to current API
        """
        try:
            if not wayback_data:
                self.logger.debug("No wayback data provided for current interpolation")
                return None
            
            # Get the last wayback data point
            last_wayback = max(wayback_data, key=lambda x: x['date'])
            last_wayback_date = datetime.fromisoformat(last_wayback['date'][:10]).date()
            last_wayback_count = last_wayback['upload_count']
            
            # Try to get current API upload count
            current_count = None
            current_date = datetime.now().date()
            
            # Check if channel has current API stats from Phase 4
            if hasattr(channel, 'current_api_stats') and channel.current_api_stats:
                current_count = channel.current_api_stats.get('video_count')
            
            # If no API stats, try to collect them now
            if current_count is None:
                try:
                    from .api_current_collector import APICurrentCollector
                    api_collector = APICurrentCollector()
                    current_channels = api_collector.collect_batch_data([channel])
                    if current_channels and hasattr(current_channels[0], 'current_api_stats'):
                        current_count = current_channels[0].current_api_stats.get('video_count')
                        self.logger.debug(f"Retrieved current API count: {current_count}")
                except Exception as e:
                    self.logger.debug(f"Could not retrieve current API data: {e}")
            
            if current_count is None:
                self.logger.debug("No current API upload count available for interpolation")
                return None
            
            # Calculate gap and interpolation parameters
            gap_days = (current_date - last_wayback_date).days
            upload_increase = current_count - last_wayback_count
            
            self.logger.debug(f"Interpolation gap: {gap_days} days, upload increase: {upload_increase}")
            
            # Only interpolate if there's a significant gap (> 30 days) and upload increase
            if gap_days <= 30 or upload_increase <= 0:
                self.logger.debug(f"Gap too small or no upload increase - skipping interpolation")
                return None
            
            # Create monthly interpolation points
            interpolated_points = []
            uploads_per_day = upload_increase / gap_days
            
            # Start interpolating from 30 days after last wayback point
            current_interp_date = last_wayback_date + timedelta(days=30)
            
            while current_interp_date < current_date:
                days_since_start = (current_interp_date - last_wayback_date).days
                interpolated_count = int(last_wayback_count + (uploads_per_day * days_since_start))
                
                # Ensure uploads don't exceed final count
                if interpolated_count > current_count:
                    interpolated_count = current_count
                
                interpolated_point = {
                    'date': current_interp_date.isoformat(),
                    'upload_count': interpolated_count,
                    'snapshot_timestamp': f'current_interpolated_{current_interp_date.strftime("%Y%m%d")}',
                    'source': 'current_api_interpolated',
                    'url': f'interpolated_to_current_api_{current_count}',
                    'interpolated': True,
                    'interpolation_source': {
                        'from_wayback_date': last_wayback_date.isoformat(),
                        'from_wayback_count': last_wayback_count,
                        'to_current_date': current_date.isoformat(),
                        'to_current_count': current_count,
                        'interpolation_rate': uploads_per_day
                    }
                }
                
                interpolated_points.append(interpolated_point)
                
                # Move to next month
                current_interp_date += timedelta(days=30)
            
            # Add final current API point
            final_point = {
                'date': current_date.isoformat(),
                'upload_count': current_count,
                'snapshot_timestamp': f'current_api_{current_date.strftime("%Y%m%d")}',
                'source': 'youtube_api_current',
                'url': 'youtube_api_current_data',
                'interpolated': False,
                'api_current': True
            }
            
            interpolated_points.append(final_point)
            
            self.logger.info(f"Generated {len(interpolated_points)} interpolation points to current API data")
            return interpolated_points
            
        except Exception as e:
            self.logger.error(f"Current API interpolation failed: {e}")
            return None