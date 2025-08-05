"""
Integration Tests for Phase 1 Pipeline (YouTube API Enrichment)

Tests the complete Phase 1 workflow including:
- YouTube API collector end-to-end processing
- Rate limiting enforcement during collection
- Circuit breaker behavior under failures
- Checkpoint save/resume functionality
- Error handling and recovery
- Data validation and quality flags

Author: test-strategist
"""

import pytest
import json
import time
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

from src.data_collectors.youtube_api_collector import YouTubeAPICollector
from src.models.channel import Channel, ChannelStatus, ChannelCategory
from src.utils.api_helpers import YouTubeAPIClient, RateLimiter, CircuitBreaker
from src.core.config import ConfigManager


@pytest.mark.integration
class TestPhase1Pipeline:
    """Integration tests for Phase 1 YouTube API enrichment pipeline"""
    
    def test_youtube_collector_initialization(self, mock_config_manager):
        """Test YouTube collector initializes correctly with real config"""
        with patch('src.data_collectors.youtube_api_collector.create_youtube_client') as mock_create:
            mock_client = Mock(spec=YouTubeAPIClient)
            mock_client.quota_used = 0
            mock_client.quota_limit = 10000
            mock_client.cost_per_request = 1
            mock_create.return_value = mock_client
            
            collector = YouTubeAPICollector()
            
            assert collector.collector_name == "YouTubeAPI"
            assert collector.service_name == "youtube"
            assert collector.youtube_client == mock_client
            assert collector.quota_used == 0
            assert collector.quota_limit == 10000
    
    def test_end_to_end_channel_enrichment(self, mock_config_manager, sample_channels, mock_youtube_api_response):
        """Test complete end-to-end channel enrichment workflow"""
        # Create mock YouTube client
        mock_client = Mock(spec=YouTubeAPIClient)
        mock_client.get_channels.return_value = {
            "items": [
                {
                    "id": "UCtest000",
                    "snippet": {
                        "title": "Enhanced Test Channel 0",
                        "description": "Enhanced description",
                        "country": "US",
                        "publishedAt": "2020-01-01T00:00:00Z"
                    },
                    "statistics": {
                        "viewCount": "2000000",
                        "subscriberCount": "50000",
                        "videoCount": "1000"
                    },
                    "status": {
                        "privacyStatus": "public"
                    }
                }
            ]
        }
        mock_client.quota_used = 0
        mock_client.quota_limit = 10000
        mock_client.cost_per_request = 1
        
        with patch('src.data_collectors.youtube_api_collector.create_youtube_client', return_value=mock_client):
            collector = YouTubeAPICollector()
            
            # Process single channel for testing
            test_channels = sample_channels[:1]
            results = collector.collect(test_channels)
            
            assert len(results) == 1
            result_channel = results[0]
            
            # Verify enrichment occurred
            assert result_channel.channel_name == "Enhanced Test Channel 0"
            assert result_channel.subscriber_count == 50000
            assert result_channel.view_count == 2000000
            assert result_channel.video_count == 1000
            assert result_channel.last_updated is not None
    
    def test_batch_processing_workflow(self, mock_config_manager, sample_channels):
        """Test batch processing of multiple channels"""
        # Create responses for multiple channels
        mock_client = Mock(spec=YouTubeAPIClient)
        
        def mock_get_channels(channel_ids, parts=None):
            return {
                "items": [
                    {
                        "id": channel_id,
                        "snippet": {
                            "title": f"Enhanced {channel_id}",
                            "description": f"Enhanced description for {channel_id}",
                            "country": "US",
                            "publishedAt": "2020-01-01T00:00:00Z"
                        },
                        "statistics": {
                            "viewCount": str(100000 * (i + 1)),
                            "subscriberCount": str(10000 * (i + 1)),
                            "videoCount": str(500 * (i + 1))
                        },
                        "status": {
                            "privacyStatus": "public"
                        }
                    }
                    for i, channel_id in enumerate(channel_ids)
                ]
            }
        
        mock_client.get_channels.side_effect = mock_get_channels
        mock_client.quota_used = 0
        mock_client.quota_limit = 10000
        mock_client.cost_per_request = 1
        
        with patch('src.data_collectors.youtube_api_collector.create_youtube_client', return_value=mock_client):
            collector = YouTubeAPICollector()
            
            # Process 7 channels (should create 2 batches with batch_size=5 from conftest)
            test_channels = sample_channels[:7]
            results = collector.collect(test_channels)
            
            assert len(results) == 7
            
            # Verify all channels were enriched
            for i, result in enumerate(results):
                assert result.channel_name == f"Enhanced {test_channels[i].channel_id}"
                assert result.subscriber_count == 10000 * (i + 1)
                assert result.view_count == 100000 * (i + 1)
    
    def test_quota_management_workflow(self, mock_config_manager, sample_channels):
        """Test quota management and enforcement"""
        mock_client = Mock(spec=YouTubeAPIClient)
        mock_client.quota_used = 9990  # Near quota limit
        mock_client.quota_limit = 10000
        mock_client.cost_per_request = 1
        
        # Mock quota check to raise exception
        def mock_get_channels(*args, **kwargs):
            if mock_client.quota_used + mock_client.cost_per_request > mock_client.quota_limit:
                raise Exception("YouTube API quota exceeded: 9991/10000")
            return {"items": []}
        
        mock_client.get_channels.side_effect = mock_get_channels
        
        with patch('src.data_collectors.youtube_api_collector.create_youtube_client', return_value=mock_client):
            collector = YouTubeAPICollector()
            
            test_channels = sample_channels[:1]
            
            with pytest.raises(Exception, match="quota exceeded"):
                collector.collect(test_channels)
    
    def test_rate_limiting_enforcement(self, mock_config_manager, sample_channels, performance_timer):
        """Test that rate limiting is properly enforced during collection"""
        mock_client = Mock(spec=YouTubeAPIClient)
        mock_client.get_channels.return_value = {"items": []}
        mock_client.quota_used = 0
        mock_client.quota_limit = 10000
        mock_client.cost_per_request = 1
        
        with patch('src.data_collectors.youtube_api_collector.create_youtube_client', return_value=mock_client):
            collector = YouTubeAPICollector()
            
            # Mock rate limiter to enforce delays
            mock_rate_limiter = Mock(spec=RateLimiter)
            mock_rate_limiter.acquire.side_effect = [False, False, True] * 5  # Force some waiting
            mock_rate_limiter.wait_time.return_value = 0.1  # 100ms wait
            collector.rate_limiter = mock_rate_limiter
            
            test_channels = sample_channels[:3]
            
            performance_timer.start()
            results = collector.collect(test_channels)
            performance_timer.stop()
            
            # Should have taken at least some time due to rate limiting
            assert performance_timer.elapsed() >= 0.2  # At least 200ms for rate limit waits
            
            # Verify rate limiter was called
            assert mock_rate_limiter.acquire.call_count >= 3
    
    def test_circuit_breaker_workflow(self, mock_config_manager, sample_channels):
        """Test circuit breaker behavior during failures"""
        mock_client = Mock(spec=YouTubeAPIClient)
        
        # Configure mock to fail consistently
        mock_client.get_channels.side_effect = Exception("API connection failed")
        mock_client.quota_used = 0
        mock_client.quota_limit = 10000
        mock_client.cost_per_request = 1
        
        with patch('src.data_collectors.youtube_api_collector.create_youtube_client', return_value=mock_client):
            collector = YouTubeAPICollector()
            
            # Configure circuit breaker for quick failure
            mock_circuit_breaker = Mock(spec=CircuitBreaker)
            failure_count = 0
            
            def mock_call(func, *args, **kwargs):
                nonlocal failure_count
                failure_count += 1
                if failure_count >= 3:
                    raise Exception("Circuit breaker is open")
                return func(*args, **kwargs)
            
            mock_circuit_breaker.call.side_effect = mock_call
            collector.circuit_breaker = mock_circuit_breaker
            
            test_channels = sample_channels[:5]
            results = collector.collect(test_channels)
            
            # Should have some failures due to circuit breaker
            assert len(results) < len(test_channels)
            assert collector.stats.circuit_breaker_trips > 0
    
    def test_checkpoint_workflow(self, mock_config_manager, sample_channels, clean_temp_dir):
        """Test checkpoint save and resume workflow"""
        mock_client = Mock(spec=YouTubeAPIClient)
        
        # Mock responses for channels
        def mock_get_channels(channel_ids, parts=None):
            return {
                "items": [
                    {
                        "id": channel_id,
                        "snippet": {
                            "title": f"Checkpoint Test {channel_id}",
                            "description": "Test description",
                            "country": "US",
                            "publishedAt": "2020-01-01T00:00:00Z"
                        },
                        "statistics": {
                            "viewCount": "1000000",
                            "subscriberCount": "10000",
                            "videoCount": "500"
                        },
                        "status": {
                            "privacyStatus": "public"
                        }
                    }
                    for channel_id in channel_ids
                ]
            }
        
        mock_client.get_channels.side_effect = mock_get_channels
        mock_client.quota_used = 0
        mock_client.quota_limit = 10000
        mock_client.cost_per_request = 1
        
        with patch('src.data_collectors.youtube_api_collector.create_youtube_client', return_value=mock_client):
            collector = YouTubeAPICollector()
            collector.checkpoint_dir = clean_temp_dir / "checkpoints"
            collector.checkpoint_dir.mkdir(exist_ok=True)
            collector.checkpoint_interval = 2  # Save checkpoint every 2 batches
            
            test_channels = sample_channels[:6]  # Will create 2 batches with batch_size=5
            
            # First run - should create checkpoint
            results1 = collector.collect(test_channels)
            assert len(results1) == 6
            
            # Verify checkpoint was created
            checkpoint_file = collector.checkpoint_dir / "YouTubeAPI_checkpoint.json"
            assert checkpoint_file.exists()
            
            # Create new collector instance to test resume
            collector2 = YouTubeAPICollector()
            collector2.checkpoint_dir = collector.checkpoint_dir
            
            # Mock the client for second collector
            with patch.object(collector2, 'youtube_client', mock_client):
                # Second run should resume from checkpoint (no new processing)
                results2 = collector2.collect(test_channels)
                
                # Should return empty since all channels were already processed
                assert len(results2) == 0
                assert collector2.progress.processed_channels == 6
    
    def test_error_handling_and_recovery(self, mock_config_manager, sample_channels):
        """Test error handling and recovery mechanisms"""
        mock_client = Mock(spec=YouTubeAPIClient)
        
        call_count = 0
        
        def mock_get_channels_with_intermittent_failures(channel_ids, parts=None):
            nonlocal call_count
            call_count += 1
            
            # Fail on first two calls, succeed on third
            if call_count <= 2:
                raise Exception("Intermittent API failure")
            
            return {
                "items": [
                    {
                        "id": channel_id,
                        "snippet": {
                            "title": f"Recovered {channel_id}",
                            "description": "Test description",
                            "country": "US",
                            "publishedAt": "2020-01-01T00:00:00Z"
                        },
                        "statistics": {
                            "viewCount": "1000000",
                            "subscriberCount": "10000",
                            "videoCount": "500"
                        },
                        "status": {
                            "privacyStatus": "public"
                        }
                    }
                    for channel_id in channel_ids
                ]
            }
        
        mock_client.get_channels.side_effect = mock_get_channels_with_intermittent_failures
        mock_client.quota_used = 0
        mock_client.quota_limit = 10000
        mock_client.cost_per_request = 1
        
        with patch('src.data_collectors.youtube_api_collector.create_youtube_client', return_value=mock_client):
            collector = YouTubeAPICollector()
            
            # Override circuit breaker to allow retries
            collector.circuit_breaker = Mock()
            collector.circuit_breaker.call.side_effect = lambda func, *args, **kwargs: func(*args, **kwargs)
            
            test_channels = sample_channels[:1]
            results = collector.collect(test_channels)
            
            # Should eventually succeed after retries
            assert len(results) == 1
            assert results[0].channel_name == f"Recovered {test_channels[0].channel_id}"
            
            # Should have made multiple attempts
            assert call_count == 3
    
    def test_data_validation_workflow(self, mock_config_manager, sample_channels):
        """Test data validation during enrichment"""
        mock_client = Mock(spec=YouTubeAPIClient)
        
        # Mock response with some invalid data
        mock_client.get_channels.return_value = {
            "items": [
                {
                    "id": "UCtest000",
                    "snippet": {
                        "title": "Valid Channel",
                        "description": "Valid description",
                        "country": "US",
                        "publishedAt": "2020-01-01T00:00:00Z"
                    },
                    "statistics": {
                        "viewCount": "1000000",
                        "subscriberCount": "10000",
                        "videoCount": "500"
                    },
                    "status": {
                        "privacyStatus": "public"
                    }
                },
                {
                    "id": "UCtest001", 
                    "snippet": {
                        "title": "",  # Invalid: empty title
                        "description": "Description",
                        "country": "INVALID",  # Invalid country code
                        "publishedAt": "invalid-date"  # Invalid date
                    },
                    "statistics": {
                        "viewCount": "-100",  # Invalid: negative view count
                        "subscriberCount": "not_a_number",  # Invalid: non-numeric
                        "videoCount": "1000"
                    },
                    "status": {
                        "privacyStatus": "private"  # Valid but different status
                    }
                }
            ]
        }
        mock_client.quota_used = 0
        mock_client.quota_limit = 10000
        mock_client.cost_per_request = 1
        
        with patch('src.data_collectors.youtube_api_collector.create_youtube_client', return_value=mock_client):
            collector = YouTubeAPICollector()
            
            test_channels = sample_channels[:2]
            results = collector.collect(test_channels)
            
            # Should handle both valid and invalid data
            assert len(results) >= 1  # At least the valid channel should be processed
            
            # Valid channel should have correct data
            valid_channel = next((r for r in results if r.channel_id == "UCtest000"), None)
            if valid_channel:
                assert valid_channel.channel_name == "Valid Channel"
                assert valid_channel.view_count == 1000000
                assert valid_channel.subscriber_count == 10000
    
    def test_progress_tracking_workflow(self, mock_config_manager, sample_channels):
        """Test progress tracking throughout the workflow"""
        mock_client = Mock(spec=YouTubeAPIClient)
        mock_client.get_channels.return_value = {"items": []}
        mock_client.quota_used = 0
        mock_client.quota_limit = 10000
        mock_client.cost_per_request = 1
        
        progress_updates = []
        
        def progress_callback(progress):
            progress_updates.append({
                'total': progress.total_channels,
                'processed': progress.processed_channels,
                'successful': progress.successful_channels,
                'failed': progress.failed_channels,
                'phase': progress.current_phase,
                'batch': progress.current_batch
            })
        
        with patch('src.data_collectors.youtube_api_collector.create_youtube_client', return_value=mock_client):
            collector = YouTubeAPICollector()
            collector.set_progress_callback(progress_callback)
            
            test_channels = sample_channels[:8]  # Multiple batches
            results = collector.collect(test_channels)
            
            # Should have received progress updates
            assert len(progress_updates) > 0
            
            # First update should show initialization
            first_update = progress_updates[0]
            assert first_update['total'] == 8
            assert first_update['phase'] == "Phase 1: YouTube API Enrichment"
            
            # Last update should show completion
            if progress_updates:
                last_update = progress_updates[-1]
                assert last_update['processed'] == 8
    
    def test_concurrent_safety(self, mock_config_manager, sample_channels):
        """Test thread safety of the collector (even though YouTube uses sequential processing)"""
        mock_client = Mock(spec=YouTubeAPIClient)
        mock_client.get_channels.return_value = {"items": []}
        mock_client.quota_used = 0
        mock_client.quota_limit = 10000
        mock_client.cost_per_request = 1
        
        with patch('src.data_collectors.youtube_api_collector.create_youtube_client', return_value=mock_client):
            collector = YouTubeAPICollector()
            
            # Even with max_workers=1, test that collector handles state correctly
            test_channels = sample_channels[:3]
            results = collector.collect(test_channels)
            
            # Should complete without threading issues
            assert collector.progress.processed_channels == 3
            assert not collector.is_running()


@pytest.mark.integration
@pytest.mark.slow
class TestPhase1LongRunning:
    """Long-running integration tests for Phase 1"""
    
    def test_large_batch_processing(self, mock_config_manager, large_channel_dataset, performance_timer):
        """Test processing of large channel dataset"""
        # Use subset for testing
        test_channels = large_channel_dataset[:50]
        
        mock_client = Mock(spec=YouTubeAPIClient)
        
        def mock_get_channels(channel_ids, parts=None):
            return {
                "items": [
                    {
                        "id": channel_id,
                        "snippet": {
                            "title": f"Large Dataset {channel_id}",
                            "description": "Test description",
                            "country": "US",
                            "publishedAt": "2020-01-01T00:00:00Z"
                        },
                        "statistics": {
                            "viewCount": "1000000",
                            "subscriberCount": "10000",
                            "videoCount": "500"
                        },
                        "status": {
                            "privacyStatus": "public"
                        }
                    }
                    for channel_id in channel_ids
                ]
            }
        
        mock_client.get_channels.side_effect = mock_get_channels
        mock_client.quota_used = 0
        mock_client.quota_limit = 10000
        mock_client.cost_per_request = 1
        
        with patch('src.data_collectors.youtube_api_collector.create_youtube_client', return_value=mock_client):
            collector = YouTubeAPICollector()
            
            performance_timer.start()
            results = collector.collect(test_channels)
            performance_timer.stop()
            
            assert len(results) == 50
            assert performance_timer.elapsed() < 30.0  # Should complete within 30 seconds
            
            # Verify all channels were processed
            assert collector.progress.successful_channels == 50
            assert collector.progress.failed_channels == 0
    
    def test_extended_checkpoint_workflow(self, mock_config_manager, large_channel_dataset, clean_temp_dir):
        """Test checkpoint functionality with extended dataset"""
        test_channels = large_channel_dataset[:20]
        
        mock_client = Mock(spec=YouTubeAPIClient)
        
        def mock_get_channels(channel_ids, parts=None):
            return {
                "items": [
                    {
                        "id": channel_id,
                        "snippet": {
                            "title": f"Extended {channel_id}",
                            "description": "Test description", 
                            "country": "US",
                            "publishedAt": "2020-01-01T00:00:00Z"
                        },
                        "statistics": {
                            "viewCount": "1000000",
                            "subscriberCount": "10000", 
                            "videoCount": "500"
                        },
                        "status": {
                            "privacyStatus": "public"
                        }
                    }
                    for channel_id in channel_ids
                ]
            }
        
        mock_client.get_channels.side_effect = mock_get_channels
        mock_client.quota_used = 0
        mock_client.quota_limit = 10000
        mock_client.cost_per_request = 1
        
        with patch('src.data_collectors.youtube_api_collector.create_youtube_client', return_value=mock_client):
            collector = YouTubeAPICollector()
            collector.checkpoint_dir = clean_temp_dir / "checkpoints"
            collector.checkpoint_dir.mkdir(exist_ok=True)
            collector.checkpoint_interval = 2
            
            # Process in multiple stages
            results1 = collector.collect(test_channels[:10])
            assert len(results1) == 10
            
            # Verify checkpoint
            checkpoint_file = collector.checkpoint_dir / "YouTubeAPI_checkpoint.json"
            assert checkpoint_file.exists()
            
            # Process remaining channels
            collector2 = YouTubeAPICollector()
            collector2.checkpoint_dir = collector.checkpoint_dir
            
            with patch.object(collector2, 'youtube_client', mock_client):
                results2 = collector2.collect(test_channels)
                
                # Should only process the remaining 10 channels
                assert len(results2) == 10
                assert collector2.progress.processed_channels == 20


@pytest.mark.api_integration
@pytest.mark.skipif(True, reason="Requires real API keys - enable manually for full integration testing")
class TestPhase1RealAPI:
    """Real API integration tests - requires actual YouTube API key"""
    
    def test_real_youtube_api_integration(self, integration_test_channels):
        """Test against real YouTube API (requires API key)"""
        # This test would use real API keys and make actual API calls
        # Only run when specifically requested with proper API configuration
        
        collector = YouTubeAPICollector()
        
        # Use a small subset of known test channels
        test_channels = integration_test_channels[:2]
        
        results = collector.collect(test_channels)
        
        # Verify real API enrichment
        assert len(results) <= len(test_channels)  # Some channels might not exist
        
        for result in results:
            assert result.channel_name is not None
            assert result.last_updated is not None
            # Real data should have reasonable values
            assert result.subscriber_count >= 0
            assert result.view_count >= 0
            assert result.video_count >= 0