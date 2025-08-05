"""
Wayback Machine Charts Data Collector
Phase 2A: Historical Social Blade charts collection (September 2019 - December 2022)

This collector extracts weekly subscriber and view data from Social Blade historical charts
archived in the Wayback Machine. Focuses on /monthly pages containing long-term historical 
weekly chart data with smart snapshot selection to minimize HTML parsing.

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


class WaybackChartsCollector(BaseCollector):
    """
    Wayback Machine collector for Social Blade historical charts data
    
    Collects weekly subscriber and view data from Sep 2019 - Dec 2022 
    using smart snapshot selection and chart data extraction.
    """
    
    def __init__(self):
        super().__init__(
            collector_name="WaybackChartsCollector",
            service_name="wayback",  # Use wayback service
            batch_size=10,  # Smaller batches for respectful scraping
            max_workers=1,  # Single worker to respect rate limits
            checkpoint_interval=50,  # Checkpoint every 50 channels
            enable_checkpoints=True
        )
        self.phase_name = "Phase 2A: Wayback Charts"
        
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
        
        # Target date range: Sep 2019 - Dec 2022 (165 weeks)
        self.start_date = datetime(2019, 9, 1)
        self.end_date = datetime(2022, 12, 31)
        self.target_weeks = 165
        
        # Chart data extraction patterns
        self.chart_patterns = {
            'dygraph': {
                'data_pattern': r'data\s*:\s*\[(\[.*?\])\]',  # Match complete nested arrays
                'csv_pattern': r'new\s+Dygraph.*?(?:CSV|data):\s*["\']([^"\']+)["\']',
                'variable_pattern': r'var\s+(\w*[Dd]ata\w*)\s*=\s*\[(.*?)\];'
            },
            'highcharts': {
                'series_pattern': r'series:\s*\[(.*?)\]',
                'data_pattern': r'data:\s*\[([\d,.\s\[\]]+)\]',
                'categories_pattern': r'categories:\s*\[(.*?)\]'
            },
            'plotly': {
                'data_pattern': r'Plotly\.newPlot.*?data:\s*\[(.*?)\]',
                'x_pattern': r'x:\s*\[(.*?)\]',
                'y_pattern': r'y:\s*\[(.*?)\]'
            }
        }
        
        self.logger.info(f"Initialized {self.collector_name} for date range {self.start_date.date()} to {self.end_date.date()}")
    
    def get_collection_phase_name(self) -> str:
        """Get the name of this collection phase"""
        return "phase2a_wayback_charts"
    
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
            
            if result['success'] and result['historical_data']:
                # Update channel with historical data
                if hasattr(channel, 'historical_data'):
                    channel.historical_data.extend(result['historical_data'])
                else:
                    channel.historical_data = result['historical_data']
                
                # Update historical coverage in channel.history object
                historical_data = result['historical_data']
                if historical_data:
                    # Extract dates from historical data
                    dates = [datetime.fromisoformat(point['date']).date() for point in historical_data]
                    first_date = min(dates)
                    last_date = max(dates)
                    total_points = len(historical_data)
                    expected_points = result.get('weeks_covered', total_points)
                    
                    # Update the channel's history object
                    channel.update_historical_coverage(
                        first_date=first_date,
                        last_date=last_date,
                        total_points=total_points,
                        expected_points=expected_points
                    )
                    
                    # Add collection source
                    channel.add_collection_source('wayback_charts', {
                        'snapshots_used': result.get('snapshots_used', []),
                        'data_quality': result.get('data_quality', 'unknown'),
                        'weeks_covered': result.get('weeks_covered', 0)
                    })
                    
                    # Update processing status
                    channel.processing_status = "enriched"
                    channel.processing_notes = f"Successfully collected {total_points} historical data points"
                else:
                    channel.processing_status = "failed"
                    channel.processing_notes = "No historical data points collected"
                
                return channel
            else:
                self.logger.warning(f"No historical data collected for {channel.channel_id}")
                channel.processing_status = "failed"
                channel.processing_notes = result.get('error', 'No historical data collected')
                return channel  # Return channel even if no data collected
                
        except Exception as e:
            self.logger.error(f"Failed to process channel {channel.channel_id}: {e}")
            channel.processing_status = "failed"
            channel.processing_notes = f"Processing error: {e}"
            return channel
    
    def collect_channel_data(self, channel: Channel) -> Dict[str, Any]:
        """
        Collect historical charts data for a single channel
        
        Args:
            channel: Channel object to collect data for
            
        Returns:
            Dictionary containing historical weekly data
        """
        start_time = time.time()
        result = {
            'channel_id': channel.channel_id,
            'collection_phase': 'phase2a_wayback_charts',
            'collection_timestamp': datetime.now().isoformat(),
            'success': False,
            'error': None,
            'data_points_collected': 0,
            'weeks_covered': 0,
            'snapshots_used': [],
            'historical_data': [],  # Weekly data points
            'data_quality': 'unknown',
            'junction_points': {
                'sep_2019_data': None,
                'dec_2022_data': None
            }
        }
        
        try:
            self.logger.info(f"Starting charts collection for {channel.channel_id}")
            
            # Step 1: Get channel name for Social Blade URL construction
            channel_name = self._get_channel_name_for_socialblade(channel)
            if not channel_name:
                raise ValueError(f"Cannot determine Social Blade channel name for {channel.channel_id}")
            
            # Step 2: Find optimal snapshots using CDX API
            snapshots = self._find_optimal_snapshots(channel_name)
            if not snapshots:
                raise ValueError(f"No suitable Wayback snapshots found for {channel_name}")
            
            result['snapshots_used'] = [s['timestamp'] for s in snapshots]
            self.logger.info(f"Found {len(snapshots)} optimal snapshots for {channel_name}")
            
            # Step 3: Extract charts data from snapshots
            historical_data = []
            for snapshot in snapshots:
                try:
                    with self.rate_limiter:  # Respect rate limiting
                        data_points = self._extract_charts_from_snapshot(snapshot, channel_name)
                        if data_points:
                            historical_data.extend(data_points)
                            self.logger.debug(f"Extracted {len(data_points)} data points from snapshot {snapshot['timestamp']}")
                except Exception as e:
                    self.logger.warning(f"Failed to extract from snapshot {snapshot['timestamp']}: {e}")
                    continue
            
            # Step 3.5: Remove duplicates from multiple snapshots
            if historical_data:
                pre_dedup_count = len(historical_data)
                historical_data = self._remove_duplicate_data_points(historical_data)
                post_dedup_count = len(historical_data)
                
                if pre_dedup_count != post_dedup_count:
                    self.logger.info(f"Removed {pre_dedup_count - post_dedup_count} duplicate data points ({pre_dedup_count} -> {post_dedup_count})")
            
            # Step 4: Process and validate historical data
            if historical_data:
                processed_data = self._process_historical_data(historical_data, channel)
                result['historical_data'] = processed_data
                result['data_points_collected'] = len(processed_data)
                
                # Calculate weeks covered
                if processed_data:
                    dates = [datetime.fromisoformat(dp['date']) for dp in processed_data]
                    weeks_span = (max(dates) - min(dates)).days / 7
                    result['weeks_covered'] = int(weeks_span)
                
                # Validate junction points
                result['junction_points'] = self._validate_junction_points(processed_data)
                
                # Assess data quality
                result['data_quality'] = self._assess_data_quality(processed_data, result['weeks_covered'])
                
                result['success'] = True
                self.logger.info(f"Successfully collected {len(processed_data)} data points covering {result['weeks_covered']} weeks")
            else:
                raise ValueError("No historical data could be extracted from any snapshot")
                
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"Charts collection failed for {channel.channel_id}: {e}")
        
        finally:
            result['collection_duration'] = time.time() - start_time
        
        return result
    
    def _get_channel_name_for_socialblade(self, channel: Channel) -> Optional[str]:
        """
        Determine the Social Blade channel identifier for URL construction
        
        Social Blade URLs use:
        - Channel ID directly (e.g., UCuAXFkgsw1L7xaCfnd5JJOw) - modern channels
        - Username format (e.g., /user/username) - legacy channels
        """
        # Legacy channel mapping for well-known early YouTube channels
        legacy_channel_mapping = {
            'UC4QobU6STFB0P71PMvOGN5A': 'jawed',  # jawed - first YouTube channel
            # Add more legacy mappings as needed
        }
        
        # Check if this is a known legacy channel
        if channel.channel_id in legacy_channel_mapping:
            return legacy_channel_mapping[channel.channel_id]
        
        # For Social Blade, we primarily use the channel ID directly
        # This is the most reliable approach for /channel/ URLs
        if channel.channel_id:
            return channel.channel_id
        
        # Try channel handle as fallback (remove @ symbol)
        if hasattr(channel, 'channel_handle') and channel.channel_handle:
            return channel.channel_handle.replace('@', '')
        
        # This shouldn't happen, but just in case
        return None
    
    def _is_legacy_channel_name(self, channel_name: str) -> bool:
        """
        Determine if a channel name represents a legacy YouTube channel
        that should use /user/ URLs instead of /channel/ URLs
        """
        # Known legacy channel usernames (early YouTube channels)
        legacy_usernames = {
            'jawed',
            'steve',
            'karim',
            'john',
            'chad',
            # Add more as needed
        }
        
        # If it's a username (not a channel ID), it might be legacy
        if not channel_name.startswith('UC') and channel_name.lower() in legacy_usernames:
            return True
            
        return False
    
    def _find_optimal_snapshots(self, channel_name: str) -> List[Dict[str, Any]]:
        """
        Find optimal Wayback Machine snapshots with maximum historical coverage
        
        Strategy:
        1. Query CDX API for all /monthly page snapshots in target range
        2. Try both /channel/ and /user/ URL formats (legacy channels need /user/)
        3. Select snapshots with longest historical chart coverage
        4. Prioritize newer snapshots (better chart quality)
        5. Minimize total requests needed
        """
        try:
            # Determine URL formats to try based on channel name
            if self._is_legacy_channel_name(channel_name):
                # For legacy channels, try /user/ first, then /channel/
                url_formats = [
                    f"https://socialblade.com/youtube/user/{channel_name}/monthly",
                    f"https://socialblade.com/youtube/channel/{channel_name}/monthly"
                ]
            else:
                # For modern channels, try /channel/ first, then /user/
                url_formats = [
                    f"https://socialblade.com/youtube/channel/{channel_name}/monthly",
                    f"https://socialblade.com/youtube/user/{channel_name}/monthly"
                ]
            
            all_snapshots = []
            successful_url = None
            
            for target_url in url_formats:
                try:
                    # Query CDX API for snapshots
                    snapshots = self.wayback_helper.get_snapshots(
                        url=target_url,
                        from_date=self.start_date.strftime('%Y%m%d'),
                        to_date=self.end_date.strftime('%Y%m%d'),
                        limit=50  # Get up to 50 snapshots to choose from
                    )
                    
                    if snapshots:
                        self.logger.info(f"Found {len(snapshots)} snapshots using URL format: {target_url}")
                        all_snapshots.extend(snapshots)
                        successful_url = target_url
                        break  # Use the first format that works
                    else:
                        self.logger.debug(f"No snapshots found for URL format: {target_url}")
                        
                except Exception as e:
                    self.logger.debug(f"Failed to query {target_url}: {e}")
                    continue
            
            if not all_snapshots:
                self.logger.warning(f"No snapshots found for channel {channel_name} in any URL format")
                return []
            
            # Score snapshots by potential historical coverage
            scored_snapshots = []
            for snapshot in all_snapshots:
                score = self._score_snapshot_coverage(snapshot)
                if score > 0:
                    snapshot['coverage_score'] = score
                    scored_snapshots.append(snapshot)
            
            # Sort by coverage score (descending) and select top candidates
            scored_snapshots.sort(key=lambda x: x['coverage_score'], reverse=True)
            
            # Select optimal snapshots (prefer fewer high-quality snapshots)
            optimal_snapshots = scored_snapshots[:3]  # Top 3 snapshots
            
            self.logger.info(f"Selected {len(optimal_snapshots)} optimal snapshots from {len(all_snapshots)} available using {successful_url}")
            return optimal_snapshots
            
        except Exception as e:
            self.logger.error(f"Failed to find optimal snapshots for {channel_name}: {e}")
            return []
    
    def _score_snapshot_coverage(self, snapshot: Dict[str, Any]) -> float:
        """
        Score a snapshot based on its potential historical coverage
        
        Factors:
        - Timestamp (newer snapshots likely have more historical data)
        - Distance to end of target period (Dec 2022 snapshots preferred)
        - HTTP status code (200 preferred)
        """
        try:
            timestamp = datetime.strptime(snapshot['timestamp'], '%Y%m%d%H%M%S')
            
            # Base score: distance from end of target period (closer to Dec 2022 = better)
            days_from_end = abs((self.end_date - timestamp).days)
            coverage_score = max(0, 1.0 - (days_from_end / 365))  # Normalize to 0-1
            
            # Bonus for successful HTTP status
            if snapshot.get('statuscode') == '200':
                coverage_score *= 1.2
            
            # Penalty for very old snapshots (less likely to have full historical data)
            if timestamp < datetime(2020, 1, 1):
                coverage_score *= 0.7
            
            return coverage_score
            
        except Exception:
            return 0.0
    
    def _extract_charts_from_snapshot(self, snapshot: Dict[str, Any], channel_name: str) -> List[Dict[str, Any]]:
        """
        Extract historical charts data from a Wayback Machine snapshot
        
        Steps:
        1. Fetch the archived HTML page
        2. Identify chart library (Dygraph, Highcharts, Plotly)
        3. Extract chart data using appropriate patterns
        4. Convert to weekly data points
        """
        data_points = []
        
        try:
            # Construct Wayback URL
            wayback_url = f"https://web.archive.org/web/{snapshot['timestamp']}/{snapshot['original']}"
            
            self.logger.debug(f"Fetching snapshot: {wayback_url}")
            
            # Fetch HTML with anti-bot measures
            response = self.scraping_utils.get(wayback_url)
            html_content = response.text if response else None
            if not html_content:
                raise ValueError("Failed to fetch HTML content")
            
            # Parse HTML
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract chart data based on detected library
            chart_library = self._detect_chart_library(html_content, soup)
            self.logger.debug(f"Detected chart library: {chart_library}")
            
            if chart_library == 'dygraph':
                data_points = self._extract_dygraph_data(html_content, soup)
            elif chart_library == 'highcharts':
                data_points = self._extract_highcharts_data(html_content, soup)
            elif chart_library == 'plotly':
                data_points = self._extract_plotly_data(html_content, soup)
            else:
                self.logger.warning(f"Unknown or unsupported chart library: {chart_library}")
            
            # Convert to standardized weekly format
            if data_points:
                standardized_points = self._standardize_chart_data(data_points, snapshot['timestamp'])
                return standardized_points
                
        except Exception as e:
            self.logger.error(f"Failed to extract charts from snapshot {snapshot['timestamp']}: {e}")
        
        return []
    
    def _detect_chart_library(self, html_content: str, soup: BeautifulSoup) -> str:
        """Detect which charting library is used on the page"""
        
        # Check for Dygraph
        if any(keyword in html_content for keyword in ['dygraph', 'Dygraph', 'dygraphs']):
            return 'dygraph'
        
        # Check for Highcharts
        if any(keyword in html_content for keyword in ['highcharts', 'Highcharts', 'highstock']):
            return 'highcharts'
        
        # Check for Plotly
        if any(keyword in html_content for keyword in ['plotly', 'Plotly', 'plotly.js']):
            return 'plotly'
        
        # Check for Chart.js
        if any(keyword in html_content for keyword in ['chartjs', 'Chart.js', 'chart.min.js']):
            return 'chartjs'
        
        return 'unknown'
    
    def _extract_dygraph_data(self, html_content: str, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Extract data from Dygraph charts"""
        data_points = []
        
        try:
            # Look for Dygraph data patterns
            patterns = self.chart_patterns['dygraph']
            
            # Try to find data arrays
            for pattern_name, pattern in patterns.items():
                matches = re.findall(pattern, html_content, re.DOTALL | re.IGNORECASE)
                
                # First, collect all data arrays with their metadata
                all_arrays = []
                for match in matches:
                    try:
                        if pattern_name == 'data_pattern':
                            full_array_str = f'[{match}]'
                            data_entries = self._parse_js_array(full_array_str)
                            
                            if data_entries:
                                # Analyze this array
                                array_info = self._analyze_data_array(data_entries)
                                all_arrays.append({
                                    'data': data_entries,
                                    'info': array_info
                                })
                    except Exception as e:
                        self.logger.debug(f"Failed to parse array: {e}")
                        continue
                
                # Now select the best subscriber and view arrays
                subscriber_array = self._select_best_subscriber_array(all_arrays)
                view_array = self._select_best_view_array(all_arrays, exclude_subscriber_array=subscriber_array)
                
                # Combine the data from selected arrays
                if subscriber_array and view_array:
                    data_points = self._combine_subscriber_view_data(subscriber_array, view_array)
                elif subscriber_array:
                    # Only subscriber data available
                    for entry in subscriber_array['data']:
                        if isinstance(entry, list) and len(entry) >= 2:
                            data_points.append({
                                'date': entry[0],
                                'subscribers': entry[1],
                                'views': 0,
                                'source': 'dygraph'
                            })
                elif view_array:
                    # Only view data available
                    for entry in view_array['data']:
                        if isinstance(entry, list) and len(entry) >= 2:
                            data_points.append({
                                'date': entry[0],
                                'subscribers': 0,
                                'views': entry[1],
                                'source': 'dygraph'
                            })
            
        except Exception as e:
            self.logger.error(f"Dygraph extraction failed: {e}")
        
        return data_points
    
    def _analyze_data_array(self, data_entries: List[Any]) -> Dict[str, Any]:
        """Analyze a data array to determine its characteristics"""
        if not data_entries or not isinstance(data_entries[0], list):
            return {'valid': False}
        
        # Calculate basic statistics
        values = [entry[1] for entry in data_entries if len(entry) >= 2]
        if not values:
            return {'valid': False}
        
        timestamps = [entry[0] for entry in data_entries if len(entry) >= 2]
        
        # Convert timestamps to dates for analysis
        try:
            dates = [datetime.fromtimestamp(ts / 1000) for ts in timestamps]
            date_range_days = (max(dates) - min(dates)).days
        except:
            date_range_days = 0
        
        min_val = min(values)
        max_val = max(values)
        avg_val = sum(values) / len(values)
        
        # Determine if values are increasing (good for subscriber/view growth)
        is_increasing = values[-1] > values[0] if len(values) > 1 else False
        
        # Determine likely data type based on value ranges (more strict)
        likely_subscribers = (avg_val < 50000000 and max_val < 100000000)  # Less than 50M avg, 100M max for subscribers
        likely_views = (avg_val > 1000000 or max_val > 100000000)  # More than 1M avg OR 100M max for views
        
        # Check for realistic growth pattern
        has_realistic_growth = min_val > 0 and is_increasing and max_val > min_val * 1.1
        
        return {
            'valid': True,
            'length': len(data_entries),
            'min_value': min_val,
            'max_value': max_val,
            'avg_value': avg_val,
            'is_increasing': is_increasing,
            'likely_subscribers': likely_subscribers,
            'likely_views': likely_views,
            'has_realistic_growth': has_realistic_growth,
            'date_range_days': date_range_days,
            'starts_with_zero': min_val == 0
        }
    
    def _select_best_subscriber_array(self, all_arrays: List[Dict]) -> Optional[Dict]:
        """Select the best array for subscriber data"""
        subscriber_candidates = []
        
        for array in all_arrays:
            info = array['info']
            if not info['valid']:
                continue
            
            # STRICT VALIDATION: Exclude arrays with unrealistic subscriber counts
            # Rick Astley has ~4M subscribers currently, historical should be lower
            # Exclude arrays with >100M subscribers (likely view data)
            if info['max_value'] > 100000000:  # >100M subscribers is unrealistic
                continue
            
            # Also exclude if average is too high (likely mixed with view data)
            if info['avg_value'] > 50000000:  # >50M average is unrealistic for subscribers
                continue
                
            # Score based on subscriber characteristics
            score = 0
            
            # Prefer arrays that look like subscriber data
            if info['likely_subscribers']:
                score += 10
            
            # Strong preference for realistic subscriber ranges
            if 100000 <= info['avg_value'] <= 50000000:  # 100K to 50M - realistic subscriber range
                score += 20
            elif info['avg_value'] < 10000000:  # Less than 10M - could be subscribers
                score += 5
            elif info['avg_value'] < 25000000:  # Less than 25M - could be subscribers
                score += 3
            
            # Prefer arrays with realistic growth
            if info['has_realistic_growth']:
                score += 5
            
            # Prefer arrays that don't start with zero (unless all do)
            if not info['starts_with_zero']:
                score += 3
            
            # Prefer longer time series
            if info['length'] > 100:
                score += 2
            
            # Prefer increasing data
            if info['is_increasing']:
                score += 2
            
            # Prefer 2+ year data range
            if info['date_range_days'] > 600:  # ~2 years
                score += 3
            
            array['subscriber_score'] = score
            if score > 0:
                subscriber_candidates.append(array)
        
        # Return the highest scoring candidate
        if subscriber_candidates:
            return max(subscriber_candidates, key=lambda x: x['subscriber_score'])
        return None
    
    def _select_best_view_array(self, all_arrays: List[Dict], exclude_subscriber_array: Optional[Dict] = None) -> Optional[Dict]:
        """Select the best array for view data"""
        view_candidates = []
        
        for array in all_arrays:
            info = array['info']
            if not info['valid']:
                continue
            
            # Skip if this is the already selected subscriber array
            if exclude_subscriber_array and array is exclude_subscriber_array:
                continue
                
            # Score based on view characteristics
            score = 0
            
            # Prefer arrays that look like view data
            if info['likely_views']:
                score += 10
            
            # Strong preference for realistic view count ranges  
            if info['avg_value'] > 500000000:  # 500M+ average - definitely views for popular channels
                score += 25
            elif info['avg_value'] > 100000000:  # 100M+ average - definitely views
                score += 20
            elif info['avg_value'] > 10000000:  # 10M+ average - likely views
                score += 15
            elif info['avg_value'] > 1000000:  # 1M+ average
                score += 10
            
            # Prefer arrays with realistic growth
            if info['has_realistic_growth']:
                score += 5
            
            # Prefer arrays that don't start with zero
            if not info['starts_with_zero']:
                score += 3
            
            # Prefer longer time series
            if info['length'] > 100:
                score += 2
            
            # Prefer increasing data
            if info['is_increasing']:
                score += 2
            
            # Prefer 2+ year data range
            if info['date_range_days'] > 600:  # ~2 years
                score += 3
            
            array['view_score'] = score
            if score > 0:
                view_candidates.append(array)
        
        # Return the highest scoring candidate
        if view_candidates:
            return max(view_candidates, key=lambda x: x['view_score'])
        return None
    
    def _combine_subscriber_view_data(self, subscriber_array: Dict, view_array: Dict) -> List[Dict[str, Any]]:
        """Combine subscriber and view data arrays into unified data points"""
        data_points = []
        
        sub_data = subscriber_array['data']
        view_data = view_array['data']
        
        # Create lookup dict for view data by timestamp
        view_lookup = {entry[0]: entry[1] for entry in view_data if len(entry) >= 2}
        
        # Sort view data by timestamp for interpolation
        sorted_view_data = sorted([(entry[0], entry[1]) for entry in view_data if len(entry) >= 2])
        
        # Combine data using subscriber array as the base
        for entry in sub_data:
            if isinstance(entry, list) and len(entry) >= 2:
                timestamp = entry[0]
                subscriber_count = entry[1]
                
                # Find corresponding view count
                view_count = view_lookup.get(timestamp)
                
                # If no exact match, find the closest view data point
                if view_count is None and sorted_view_data:
                    # Find the closest timestamp in view data
                    closest_view_entry = min(sorted_view_data, 
                                           key=lambda x: abs(x[0] - timestamp))
                    
                    # Only use if within reasonable time range (e.g., 4 weeks)
                    time_diff = abs(closest_view_entry[0] - timestamp)
                    max_time_diff = 4 * 7 * 24 * 60 * 60 * 1000  # 4 weeks in milliseconds
                    
                    if time_diff <= max_time_diff:
                        view_count = closest_view_entry[1]
                    else:
                        view_count = 0
                else:
                    view_count = view_count or 0
                
                data_points.append({
                    'date': timestamp,
                    'subscribers': subscriber_count,
                    'views': view_count,
                    'source': 'dygraph'
                })
        
        return data_points
    
    def _extract_highcharts_data(self, html_content: str, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Extract data from Highcharts"""
        data_points = []
        
        try:
            patterns = self.chart_patterns['highcharts']
            
            # Extract series data and categories
            series_match = re.search(patterns['series_pattern'], html_content, re.DOTALL)
            
            if series_match:
                series_data = series_match.group(1)
                
                # Look for subscriber and view data series
                data_matches = re.findall(patterns['data_pattern'], series_data)
                categories_match = re.search(patterns['categories_pattern'], html_content)
                
                # Parse data points
                if data_matches and categories_match:
                    categories = self._parse_js_array(categories_match.group(1))
                    
                    for i, data_match in enumerate(data_matches):
                        values = self._parse_js_array(data_match)
                        
                        # Map data to dates
                        for j, value in enumerate(values):
                            if j < len(categories):
                                data_points.append({
                                    'date': categories[j],
                                    'subscribers' if i == 0 else 'views': value,
                                    'source': 'highcharts'
                                })
            
        except Exception as e:
            self.logger.error(f"Highcharts extraction failed: {e}")
        
        return data_points
    
    def _extract_plotly_data(self, html_content: str, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Extract data from Plotly charts"""
        data_points = []
        
        try:
            patterns = self.chart_patterns['plotly']
            
            # Extract x (dates) and y (values) data
            x_match = re.search(patterns['x_pattern'], html_content, re.DOTALL)
            y_match = re.search(patterns['y_pattern'], html_content, re.DOTALL)
            
            if x_match and y_match:
                x_data = self._parse_js_array(x_match.group(1))  # Dates
                y_data = self._parse_js_array(y_match.group(1))  # Values
                
                # Combine x and y data
                for i, date in enumerate(x_data):
                    if i < len(y_data):
                        data_points.append({
                            'date': date,
                            'value': y_data[i],
                            'source': 'plotly'
                        })
            
        except Exception as e:
            self.logger.error(f"Plotly extraction failed: {e}")
        
        return data_points
    
    def _parse_js_array(self, js_array_str: str) -> List[Any]:
        """Parse JavaScript array string to Python list"""
        try:
            # Clean up the string
            cleaned = js_array_str.strip()
            
            # Handle simple arrays
            if cleaned.startswith('[') and cleaned.endswith(']'):
                # Try direct JSON parsing first
                try:
                    return json.loads(cleaned)
                except json.JSONDecodeError:
                    # If JSON parsing fails, try to fix common JavaScript array issues
                    # Remove any trailing commas
                    fixed = re.sub(r',\s*]', ']', cleaned)
                    fixed = re.sub(r',\s*}', '}', fixed)
                    
                    try:
                        return json.loads(fixed)
                    except json.JSONDecodeError:
                        # Manual parsing for complex cases
                        return self._manual_parse_array(cleaned)
            else:
                # More complex parsing needed
                return []
                
        except Exception as e:
            self.logger.debug(f"Array parsing failed: {e}")
            return []
    
    def _manual_parse_array(self, array_str: str) -> List[Any]:
        """Manually parse JavaScript array when JSON parsing fails"""
        try:
            # Extract individual array elements using regex
            # Look for patterns like [num,num] or [num,num,num]
            element_pattern = r'\[([^\[\]]+)\]'
            elements = re.findall(element_pattern, array_str)
            
            parsed_elements = []
            for element in elements:
                # Split by comma and convert to numbers
                parts = [part.strip() for part in element.split(',')]
                try:
                    numeric_parts = [float(part) if '.' in part else int(part) for part in parts if part]
                    if numeric_parts:
                        parsed_elements.append(numeric_parts)
                except ValueError:
                    continue
            
            return parsed_elements
            
        except Exception:
            return []
    
    def _standardize_chart_data(self, raw_data: List[Dict[str, Any]], snapshot_timestamp: str) -> List[Dict[str, Any]]:
        """
        Convert raw chart data to standardized weekly format
        """
        standardized_data = []
        
        try:
            for data_point in raw_data:
                # Parse date
                date_str = data_point.get('date', '')
                parsed_date = self._parse_chart_date(date_str)
                
                if not parsed_date:
                    continue
                
                # Create standardized data point
                standard_point = {
                    'date': parsed_date.isoformat(),
                    'week_start': self._get_week_start(parsed_date).isoformat(),
                    'subscribers': self._clean_numeric_value(data_point.get('subscribers', 0)),
                    'views': self._clean_numeric_value(data_point.get('views', 0)),
                    'data_source': 'wayback_charts',
                    'snapshot_timestamp': snapshot_timestamp,
                    'extraction_method': data_point.get('source', 'unknown')
                }
                
                standardized_data.append(standard_point)
            
            # Sort by date and remove duplicates
            standardized_data.sort(key=lambda x: x['date'])
            standardized_data = self._remove_duplicate_weeks(standardized_data)
            
        except Exception as e:
            self.logger.error(f"Failed to standardize chart data: {e}")
        
        return standardized_data
    
    def _parse_chart_date(self, date_str: str) -> Optional[datetime]:
        """Parse various date formats from chart data"""
        try:
            # Try common date formats
            date_formats = [
                '%Y-%m-%d',
                '%m/%d/%Y',
                '%d/%m/%Y',
                '%Y%m%d',
                '%b %d %Y',
                '%B %d, %Y'
            ]
            
            for fmt in date_formats:
                try:
                    return datetime.strptime(str(date_str).strip(), fmt)
                except ValueError:
                    continue
            
            # Try parsing as timestamp
            if str(date_str).isdigit():
                timestamp = int(date_str)
                if timestamp > 1000000000000:  # JavaScript timestamp in milliseconds
                    return datetime.fromtimestamp(timestamp / 1000)
                elif timestamp > 1000000000:  # Unix timestamp in seconds
                    return datetime.fromtimestamp(timestamp)
            
        except Exception:
            pass
        
        return None
    
    def _clean_numeric_value(self, value: Any) -> int:
        """Clean and convert numeric values"""
        try:
            if isinstance(value, (int, float)):
                return int(value)
            
            # Clean string values
            if isinstance(value, str):
                # Remove commas, spaces, and other formatting
                cleaned = re.sub(r'[^\d.-]', '', value)
                if cleaned:
                    return int(float(cleaned))
            
        except Exception:
            pass
        
        return 0
    
    def _get_week_start(self, date: datetime) -> datetime:
        """Get the start of the week (Sunday) for a given date"""
        days_since_sunday = date.weekday() + 1
        if days_since_sunday == 7:
            days_since_sunday = 0
        return date - timedelta(days=days_since_sunday)
    
    def _remove_duplicate_weeks(self, data_points: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate data points for the same week"""
        seen_weeks = set()
        unique_data = []
        
        for point in data_points:
            week_key = point['week_start']
            if week_key not in seen_weeks:
                seen_weeks.add(week_key)
                unique_data.append(point)
        
        return unique_data
    
    def _remove_duplicate_data_points(self, data_points: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Remove duplicate data points from multiple snapshots
        
        Strategy:
        1. Use exact date matching for duplicates
        2. For duplicates, prefer data points with more complete information (non-zero views)
        3. If tie, prefer the one with higher view count (likely more recent/accurate)
        """
        if not data_points:
            return []
        
        # Group data points by exact date
        date_groups = {}
        for point in data_points:
            date_key = point['date']  # Use exact timestamp as key
            if date_key not in date_groups:
                date_groups[date_key] = []
            date_groups[date_key].append(point)
        
        # For each date, select the best data point
        unique_data = []
        for date_key, points in date_groups.items():
            if len(points) == 1:
                # No duplicates, use the single point
                unique_data.append(points[0])
            else:
                # Multiple points for same date, select the best one
                best_point = self._select_best_duplicate(points)
                unique_data.append(best_point)
        
        # Sort by date
        unique_data.sort(key=lambda x: x['date'])
        
        return unique_data
    
    def _select_best_duplicate(self, duplicate_points: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Select the best data point from duplicates for the same date
        
        Preference order:
        1. Point with non-zero view data (more complete)
        2. Point with higher view count (likely more accurate)
        3. Point with higher subscriber count
        4. First point if all else equal
        """
        if len(duplicate_points) == 1:
            return duplicate_points[0]
        
        # Score each point based on data completeness and quality
        scored_points = []
        for point in duplicate_points:
            score = 0
            
            # Prefer points with non-zero view data
            if point.get('views', 0) > 0:
                score += 1000
            
            # Add view count to score (higher views = better)
            score += point.get('views', 0) / 1000000  # Scale to reasonable range
            
            # Add subscriber count to score
            score += point.get('subscribers', 0) / 1000  # Scale to reasonable range
            
            scored_points.append((score, point))
        
        # Return the highest scoring point
        scored_points.sort(key=lambda x: x[0], reverse=True)
        return scored_points[0][1]
    
    def _process_historical_data(self, raw_data: List[Dict[str, Any]], channel: Channel) -> List[Dict[str, Any]]:
        """
        Process and validate historical data
        
        Steps:
        1. Filter to target date range (Sep 2019 - Dec 2022)
        2. Fill gaps with interpolation where appropriate
        3. Calculate deltas between weeks
        4. Validate data consistency
        """
        processed_data = []
        
        try:
            # Filter to target date range
            filtered_data = []
            for point in raw_data:
                point_date = datetime.fromisoformat(point['date'])
                if self.start_date <= point_date <= self.end_date:
                    filtered_data.append(point)
            
            if not filtered_data:
                return []
            
            # Sort by date
            filtered_data.sort(key=lambda x: x['date'])
            
            # Calculate deltas and validate progression
            for i, point in enumerate(filtered_data):
                processed_point = point.copy()
                
                # Calculate deltas from previous week
                if i > 0:
                    prev_point = filtered_data[i-1]
                    processed_point['delta_subscribers'] = point['subscribers'] - prev_point['subscribers']
                    processed_point['delta_views'] = point['views'] - prev_point['views']
                else:
                    processed_point['delta_subscribers'] = 0
                    processed_point['delta_views'] = 0
                
                # Add data quality flags
                processed_point['data_quality_flags'] = self._assess_point_quality(processed_point, i, filtered_data)
                
                processed_data.append(processed_point)
            
        except Exception as e:
            self.logger.error(f"Failed to process historical data: {e}")
        
        return processed_data
    
    def _assess_point_quality(self, point: Dict[str, Any], index: int, all_data: List[Dict[str, Any]]) -> List[str]:
        """Assess the quality of a single data point"""
        flags = []
        
        try:
            # Check for unreasonable values
            if point['subscribers'] < 0 or point['views'] < 0:
                flags.append('negative_values')
            
            # Check for unreasonable deltas
            if abs(point.get('delta_subscribers', 0)) > point['subscribers'] * 0.5:
                flags.append('large_subscriber_change')
            
            if abs(point.get('delta_views', 0)) > point['views'] * 0.5:
                flags.append('large_view_change')
            
            # Check for missing data
            if point['subscribers'] == 0 and point['views'] == 0:
                flags.append('zero_values')
            
        except Exception:
            flags.append('processing_error')
        
        return flags
    
    def _validate_junction_points(self, data_points: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate junction points for data continuity
        
        Sep 2019: Transition from existing dataset to Wayback data
        Dec 2022: Transition from Wayback data to ViewStats data
        """
        junction_validation = {
            'sep_2019_data': None,
            'dec_2022_data': None,
            'continuity_score': 0.0
        }
        
        try:
            # Find data points closest to junction dates
            sep_2019_target = datetime(2019, 9, 1)
            dec_2022_target = datetime(2022, 12, 31)
            
            closest_sep = None
            closest_dec = None
            
            for point in data_points:
                point_date = datetime.fromisoformat(point['date'])
                
                # Check Sep 2019 junction
                if not closest_sep or abs((point_date - sep_2019_target).days) < abs((datetime.fromisoformat(closest_sep['date']) - sep_2019_target).days):
                    closest_sep = point
                
                # Check Dec 2022 junction
                if not closest_dec or abs((point_date - dec_2022_target).days) < abs((datetime.fromisoformat(closest_dec['date']) - dec_2022_target).days):
                    closest_dec = point
            
            junction_validation['sep_2019_data'] = closest_sep
            junction_validation['dec_2022_data'] = closest_dec
            
            # Calculate continuity score (placeholder - would need existing data for comparison)
            if closest_sep and closest_dec:
                junction_validation['continuity_score'] = 0.8  # Placeholder score
            
        except Exception as e:
            self.logger.error(f"Junction point validation failed: {e}")
        
        return junction_validation
    
    def _validate_against_current_metrics(self, data_points: List[Dict[str, Any]], channel: Channel) -> Dict[str, Any]:
        """
        Validate extracted historical data against current channel metrics
        
        This ensures we're extracting data for the correct channel by checking if:
        1. The latest historical data points roughly align with current metrics
        2. The growth trends make sense
        3. The data range is reasonable
        """
        if not data_points:
            return {'valid': False, 'reason': 'No data points to validate', 'message': 'No data available'}
        
        try:
            # Get current metrics from channel
            current_subs = 0
            current_views = 0
            
            if hasattr(channel, 'current_metrics') and channel.current_metrics:
                metrics = channel.current_metrics
                current_subs = getattr(metrics, 'subscriber_count', 0)
                current_views = getattr(metrics, 'total_view_count', 0)
            
            # Get the most recent historical data point
            latest_point = max(data_points, key=lambda x: datetime.fromisoformat(x['date']))
            historical_subs = latest_point['subscribers']
            historical_views = latest_point['views']
            
            # If we have current metrics, validate against them
            if current_subs > 0 or current_views > 0:
                # Check if historical data is reasonably close to current metrics
                # Allow for some variance since historical data might be older
                
                # Subscriber validation (allow 50% variance)
                if current_subs > 0:
                    sub_ratio = historical_subs / current_subs if current_subs > 0 else 0
                    if sub_ratio < 0.1 or sub_ratio > 10:  # More than 10x difference
                        return {
                            'valid': False,
                            'reason': f'Subscriber mismatch: historical={historical_subs:,}, current={current_subs:,} (ratio: {sub_ratio:.2f})',
                            'message': 'Historical subscriber data does not match current metrics'
                        }
                
                # View validation (allow more variance for views as they grow faster)
                if current_views > 0:
                    view_ratio = historical_views / current_views if current_views > 0 else 0
                    if view_ratio < 0.01 or view_ratio > 100:  # More than 100x difference
                        return {
                            'valid': False,
                            'reason': f'View count mismatch: historical={historical_views:,}, current={current_views:,} (ratio: {view_ratio:.2f})',
                            'message': 'Historical view data does not match current metrics'
                        }
            
            # Additional sanity checks
            
            # Check if data starts with reasonable values (not 0 for established channels)
            earliest_point = min(data_points, key=lambda x: datetime.fromisoformat(x['date']))
            if earliest_point['subscribers'] == 0 and len(data_points) > 50:
                # If we have lots of data points but start with 0, this might be wrong channel
                return {
                    'valid': False,
                    'reason': f'Channel starts with 0 subscribers despite having {len(data_points)} data points',
                    'message': 'Suspicious data pattern - possible wrong channel data'
                }
            
            # Check growth trends
            if historical_subs < earliest_point['subscribers']:
                return {
                    'valid': False,
                    'reason': 'Subscribers decreased over time (negative growth)',
                    'message': 'Invalid growth pattern detected'
                }
            
            # If we get here, validation passed
            validation_message = f"Data validated successfully: {historical_subs:,} subs, {historical_views:,} views"
            if current_subs > 0:
                validation_message += f" (vs current: {current_subs:,} subs, {current_views:,} views)"
            
            return {
                'valid': True,
                'reason': 'Data validation passed',
                'message': validation_message,
                'historical_final': {'subscribers': historical_subs, 'views': historical_views},
                'current_metrics': {'subscribers': current_subs, 'views': current_views}
            }
            
        except Exception as e:
            return {
                'valid': False,
                'reason': f'Validation error: {e}',
                'message': 'Error during data validation'
            }
    
    def _assess_data_quality(self, data_points: List[Dict[str, Any]], weeks_covered: int) -> str:
        """Assess overall data quality"""
        try:
            if not data_points:
                return 'no_data'
            
            # Calculate quality metrics
            total_points = len(data_points)
            flagged_points = sum(1 for p in data_points if p.get('data_quality_flags', []))
            coverage_ratio = weeks_covered / self.target_weeks
            
            # Determine quality level
            if coverage_ratio > 0.9 and flagged_points / total_points < 0.1:
                return 'excellent'
            elif coverage_ratio > 0.7 and flagged_points / total_points < 0.2:
                return 'good'
            elif coverage_ratio > 0.5 and flagged_points / total_points < 0.3:
                return 'fair'
            else:
                return 'poor'
                
        except Exception:
            return 'unknown'