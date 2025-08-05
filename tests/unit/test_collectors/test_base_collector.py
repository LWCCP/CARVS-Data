"""
Unit Tests for Base Collector

Tests the abstract base collector including progress tracking, checkpoint management,
rate limiting integration, concurrent processing, and error handling.

Author: test-strategist
"""

import pytest
import json
import time
import threading
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

from src.data_collectors.base_collector import (
    BaseCollector,
    CollectionProgress,
    CollectorStats
)
from src.models.channel import Channel, ChannelStatus, ChannelCategory
from src.utils.api_helpers import RateLimiter, CircuitBreaker


class TestCollectorForTesting(BaseCollector):
    """Concrete implementation of BaseCollector for testing"""
    
    def __init__(self, **kwargs):
        super().__init__(
            collector_name="TestCollector",
            service_name="youtube",  # Use existing service for config
            **kwargs
        )
        self.processed_channels = []
        self.should_fail = False
        self.fail_on_channel_ids = set()
        self.processing_delay = 0.0
    
    def _process_single_channel(self, channel: Channel):
        """Mock implementation of channel processing"""
        if self.processing_delay > 0:
            time.sleep(self.processing_delay)
        
        if self.should_fail or channel.channel_id in self.fail_on_channel_ids:
            raise Exception(f"Processing failed for {channel.channel_id}")
        
        # Create a copy with updated metadata
        processed_channel = Channel(
            channel_id=channel.channel_id,
            channel_name=f"Processed {channel.channel_name}",
            channel_url=channel.channel_url,
            status=channel.status,
            category=channel.category,
            country=channel.country,
            subscriber_count=channel.subscriber_count,
            view_count=channel.view_count,
            video_count=channel.video_count,
            created_date=channel.created_date,
            last_updated=datetime.now()
        )
        
        self.processed_channels.append(processed_channel)
        return processed_channel
    
    def get_collection_phase_name(self):
        return "Test Phase"


class TestCollectionProgress:
    """Test suite for CollectionProgress class"""
    
    def test_progress_initialization(self):
        """Test progress object initialization"""
        progress = CollectionProgress(
            total_channels=100,
            processed_channels=25,
            successful_channels=20,
            failed_channels=5
        )
        
        assert progress.total_channels == 100
        assert progress.processed_channels == 25
        assert progress.successful_channels == 20
        assert progress.failed_channels == 5
        assert progress.start_time is None
        assert progress.end_time is None
    
    def test_success_rate_calculation(self, sample_progress):
        """Test success rate calculation"""
        progress = CollectionProgress(
            processed_channels=100,
            successful_channels=85,
            failed_channels=15
        )
        
        assert progress.get_success_rate() == 85.0
        
        # Test with no processed channels
        empty_progress = CollectionProgress()
        assert empty_progress.get_success_rate() == 0.0
    
    def test_elapsed_time_calculation(self):
        """Test elapsed time calculation"""
        start_time = datetime.now() - timedelta(minutes=30)
        
        progress = CollectionProgress(start_time=start_time)
        elapsed = progress.get_elapsed_time()
        
        # Should be approximately 30 minutes
        assert 25 <= elapsed.total_seconds() / 60 <= 35
        
        # Test with no start time
        no_start_progress = CollectionProgress()
        assert no_start_progress.get_elapsed_time() == timedelta(0)
    
    def test_estimated_completion(self):
        """Test estimated completion time calculation"""
        start_time = datetime.now() - timedelta(minutes=10)
        
        progress = CollectionProgress(
            total_channels=100,
            processed_channels=25,
            start_time=start_time
        )
        
        estimated = progress.get_estimated_completion()
        
        # Should estimate completion time based on current rate
        assert estimated is not None
        assert estimated > datetime.now()
    
    def test_progress_to_dict(self):
        """Test progress serialization to dictionary"""
        start_time = datetime.now() - timedelta(minutes=15)
        progress = CollectionProgress(
            total_channels=100,
            processed_channels=40,
            successful_channels=35,
            failed_channels=5,
            start_time=start_time,
            current_phase="Test Phase",
            current_batch=4,
            total_batches=10
        )
        
        progress_dict = progress.to_dict()
        
        assert progress_dict['total_channels'] == 100
        assert progress_dict['processed_channels'] == 40
        assert progress_dict['success_rate'] == 87.5
        assert progress_dict['current_phase'] == "Test Phase"
        assert progress_dict['start_time'] is not None
        assert progress_dict['elapsed_time'] is not None


class TestCollectorStats:
    """Test suite for CollectorStats class"""
    
    def test_stats_initialization(self):
        """Test stats object initialization"""
        stats = CollectorStats()
        
        assert stats.requests_made == 0
        assert stats.requests_successful == 0
        assert stats.requests_failed == 0
        assert stats.total_wait_time == 0.0
        assert stats.circuit_breaker_trips == 0
        assert stats.rate_limit_hits == 0
    
    def test_request_success_rate(self, sample_stats):
        """Test request success rate calculation"""
        assert sample_stats.get_request_success_rate() == 95.0
        
        # Test with no requests
        empty_stats = CollectorStats()
        assert empty_stats.get_request_success_rate() == 0.0
    
    def test_average_wait_time(self, sample_stats):
        """Test average wait time calculation"""
        expected_avg = sample_stats.total_wait_time / sample_stats.requests_made
        assert sample_stats.get_average_wait_time() == expected_avg
        
        # Test with no requests
        empty_stats = CollectorStats()
        assert empty_stats.get_average_wait_time() == 0.0


class TestBaseCollector:
    """Test suite for BaseCollector class"""
    
    def test_collector_initialization(self, mock_config_manager, clean_temp_dir):
        """Test collector initialization"""
        collector = TestCollectorForTesting(
            batch_size=10,
            max_workers=2,
            checkpoint_interval=5,
            enable_checkpoints=True
        )
        
        assert collector.collector_name == "TestCollector"
        assert collector.service_name == "youtube"
        assert collector.batch_size == 10
        assert collector.max_workers == 2
        assert collector.checkpoint_interval == 5
        assert collector.enable_checkpoints == True
        
        assert isinstance(collector.rate_limiter, RateLimiter)
        assert isinstance(collector.circuit_breaker, CircuitBreaker)
        assert isinstance(collector.progress, CollectionProgress)
        assert isinstance(collector.stats, CollectorStats)
    
    def test_progress_callback(self, mock_config_manager):
        """Test progress callback functionality"""
        collector = TestCollectorForTesting()
        
        callback_calls = []
        
        def progress_callback(progress):
            callback_calls.append(progress)
        
        collector.set_progress_callback(progress_callback)
        collector._notify_progress()
        
        assert len(callback_calls) == 1
        assert isinstance(callback_calls[0], CollectionProgress)
    
    def test_error_callback(self, mock_config_manager):
        """Test error callback functionality"""
        collector = TestCollectorForTesting()
        
        error_calls = []
        
        def error_callback(channel_id, error):
            error_calls.append((channel_id, error))
        
        collector.set_error_callback(error_callback)
        
        test_error = Exception("Test error")
        collector._notify_error("UCtest123", test_error)
        
        assert len(error_calls) == 1
        assert error_calls[0][0] == "UCtest123"
        assert error_calls[0][1] == test_error
    
    def test_pause_resume_functionality(self, mock_config_manager):
        """Test pause and resume functionality"""
        collector = TestCollectorForTesting()
        
        assert not collector.is_paused()
        
        collector.pause()
        assert collector.is_paused()
        
        collector.resume()
        assert not collector.is_paused()
    
    def test_stop_functionality(self, mock_config_manager):
        """Test stop functionality"""
        collector = TestCollectorForTesting()
        
        assert not collector.should_stop()
        
        collector.stop()
        assert collector.should_stop()
    
    def test_channel_batching(self, mock_config_manager, sample_channels):
        """Test channel batching logic"""
        collector = TestCollectorForTesting(batch_size=3)
        
        batches = list(collector._batch_channels(sample_channels))
        
        # 10 channels with batch size 3 should create 4 batches
        assert len(batches) == 4
        assert len(batches[0]) == 3
        assert len(batches[1]) == 3
        assert len(batches[2]) == 3
        assert len(batches[3]) == 1  # Last batch has remainder
        
        # Verify all channels are included
        all_batched = [ch for batch in batches for ch in batch]
        assert len(all_batched) == len(sample_channels)
    
    def test_sequential_batch_processing(self, mock_config_manager, sample_channels):
        """Test sequential batch processing"""
        collector = TestCollectorForTesting(max_workers=1)
        
        # Process first 3 channels
        batch = sample_channels[:3]
        results = collector._process_batch_sequential(batch)
        
        assert len(results) == 3
        assert all(result.channel_name.startswith("Processed") for result in results)
        
        # Check stats were updated
        assert collector.stats.requests_made == 3
        assert collector.stats.requests_successful == 3
        assert collector.progress.processed_channels == 3
        assert collector.progress.successful_channels == 3
    
    def test_sequential_processing_with_failures(self, mock_config_manager, sample_channels):
        """Test sequential processing with some failures"""
        collector = TestCollectorForTesting()
        collector.fail_on_channel_ids = {sample_channels[1].channel_id}
        
        batch = sample_channels[:3]
        results = collector._process_batch_sequential(batch)
        
        # Should have 2 successful results (channels 0 and 2)
        assert len(results) == 2
        
        # Check stats reflect the failure
        assert collector.stats.requests_made == 3
        assert collector.stats.requests_successful == 2
        assert collector.stats.requests_failed == 1
        assert collector.progress.failed_channels == 1
    
    def test_concurrent_batch_processing(self, mock_config_manager, sample_channels):
        """Test concurrent batch processing"""
        collector = TestCollectorForTesting(max_workers=3)
        
        batch = sample_channels[:5]
        results = collector._process_batch_concurrent(batch)
        
        assert len(results) == 5
        assert all(result.channel_name.startswith("Processed") for result in results)
        
        # Check stats
        assert collector.stats.requests_made == 5
        assert collector.stats.requests_successful == 5
    
    def test_rate_limiting_integration(self, mock_config_manager, sample_channels):
        """Test integration with rate limiting"""
        collector = TestCollectorForTesting()
        
        # Mock rate limiter to initially fail then succeed
        collector.rate_limiter.acquire = Mock(side_effect=[False, False, True])
        collector.rate_limiter.wait_time = Mock(return_value=0.01)  # Short wait for testing
        
        batch = sample_channels[:1]
        results = collector._process_batch_sequential(batch)
        
        assert len(results) == 1
        # Should have hit rate limit twice
        assert collector.stats.rate_limit_hits == 2
    
    def test_circuit_breaker_integration(self, mock_config_manager, sample_channels):
        """Test integration with circuit breaker"""
        collector = TestCollectorForTesting()
        
        # Mock circuit breaker to trip
        def mock_call(func, *args, **kwargs):
            if func == collector._process_single_channel:
                raise Exception("Circuit breaker is open")
            return func(*args, **kwargs)
        
        collector.circuit_breaker.call = Mock(side_effect=mock_call)
        
        batch = sample_channels[:1]
        results = collector._process_batch_sequential(batch)
        
        # Should have no results due to circuit breaker
        assert len(results) == 0
        assert collector.progress.failed_channels == 1
    
    def test_checkpoint_save_and_load(self, mock_config_manager, sample_channels, clean_temp_dir):
        """Test checkpoint save and load functionality"""
        collector = TestCollectorForTesting(enable_checkpoints=True)
        collector.checkpoint_dir = clean_temp_dir / "checkpoints"
        collector.checkpoint_dir.mkdir(exist_ok=True)
        
        # Update progress for checkpoint
        collector.progress.total_channels = 10
        collector.progress.processed_channels = 5
        collector.progress.successful_channels = 4
        collector.progress.failed_channels = 1
        
        processed_channel_ids = [ch.channel_id for ch in sample_channels[:5]]
        failed_channel_ids = [sample_channels[4].channel_id]
        
        # Save checkpoint
        collector.save_checkpoint(processed_channel_ids, failed_channel_ids)
        
        # Verify checkpoint file exists
        checkpoint_file = collector.checkpoint_dir / f"{collector.collector_name}_checkpoint.json"
        assert checkpoint_file.exists()
        
        # Load checkpoint
        checkpoint_data = collector.load_checkpoint()
        
        assert checkpoint_data is not None
        assert checkpoint_data['collector_name'] == "TestCollector"
        assert len(checkpoint_data['processed_channels']) == 5
        assert len(checkpoint_data['failed_channels']) == 1
    
    def test_checkpoint_disabled(self, mock_config_manager, sample_channels):
        """Test collector with checkpoints disabled"""
        collector = TestCollectorForTesting(enable_checkpoints=False)
        
        # Should do nothing when checkpoints disabled
        collector.save_checkpoint(["UCtest123"])
        checkpoint_data = collector.load_checkpoint()
        
        assert checkpoint_data is None
    
    def test_clear_checkpoint(self, mock_config_manager, clean_temp_dir):
        """Test checkpoint clearing"""
        collector = TestCollectorForTesting(enable_checkpoints=True)
        collector.checkpoint_dir = clean_temp_dir / "checkpoints"
        collector.checkpoint_dir.mkdir(exist_ok=True)
        
        # Create a checkpoint
        collector.save_checkpoint(["UCtest123"])
        
        checkpoint_file = collector.checkpoint_dir / f"{collector.collector_name}_checkpoint.json"
        assert checkpoint_file.exists()
        
        # Clear checkpoint
        collector.clear_checkpoint()
        assert not checkpoint_file.exists()
    
    def test_full_collection_workflow(self, mock_config_manager, sample_channels):
        """Test complete collection workflow"""
        collector = TestCollectorForTesting(
            batch_size=4,
            checkpoint_interval=2
        )
        
        # Run collection
        results = collector.collect(sample_channels)
        
        assert len(results) == len(sample_channels)
        assert all(result.channel_name.startswith("Processed") for result in results)
        
        # Check final progress
        assert collector.progress.total_channels == len(sample_channels)
        assert collector.progress.processed_channels == len(sample_channels)
        assert collector.progress.successful_channels == len(sample_channels)
        assert collector.progress.failed_channels == 0
        assert collector.progress.end_time is not None
        
        # Check that collector is no longer running
        assert not collector.is_running()
    
    def test_collection_with_checkpoint_resume(self, mock_config_manager, sample_channels, clean_temp_dir):
        """Test collection resuming from checkpoint"""
        collector = TestCollectorForTesting(
            batch_size=3,
            enable_checkpoints=True
        )
        collector.checkpoint_dir = clean_temp_dir / "checkpoints"
        collector.checkpoint_dir.mkdir(exist_ok=True)
        
        # Simulate existing checkpoint (half processed)
        checkpoint_data = {
            "collector_name": "TestCollector",
            "timestamp": datetime.now().isoformat(),
            "progress": {
                "total_channels": len(sample_channels),
                "processed_channels": 5,
                "successful_channels": 5,
                "failed_channels": 0
            },
            "processed_channels": [ch.channel_id for ch in sample_channels[:5]],
            "failed_channels": []
        }
        
        checkpoint_file = collector.checkpoint_dir / "TestCollector_checkpoint.json"
        with open(checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f)
        
        # Run collection - should resume from checkpoint
        results = collector.collect(sample_channels)
        
        # Should only process remaining 5 channels
        assert len(results) == 5  # Only newly processed channels returned
        assert collector.progress.processed_channels == len(sample_channels)
    
    def test_collection_with_stop_request(self, mock_config_manager, sample_channels):
        """Test collection stopping on request"""
        collector = TestCollectorForTesting(batch_size=2)
        collector.processing_delay = 0.1  # Add delay to allow stop signal
        
        def stop_after_delay():
            time.sleep(0.05)  # Let processing start
            collector.stop()
        
        # Start stop thread
        stop_thread = threading.Thread(target=stop_after_delay)
        stop_thread.start()
        
        results = collector.collect(sample_channels)
        stop_thread.join()
        
        # Should have stopped early
        assert len(results) < len(sample_channels)
        assert collector.should_stop()
    
    def test_collection_with_pause_resume(self, mock_config_manager, sample_channels):
        """Test collection with pause and resume"""
        collector = TestCollectorForTesting(batch_size=2)
        collector.processing_delay = 0.01
        
        pause_resume_executed = []
        
        def pause_resume_sequence():
            time.sleep(0.02)  # Let some processing happen
            collector.pause()
            pause_resume_executed.append("paused")
            time.sleep(0.05)
            collector.resume()
            pause_resume_executed.append("resumed")
        
        # Start pause/resume thread
        control_thread = threading.Thread(target=pause_resume_sequence)
        control_thread.start()
        
        results = collector.collect(sample_channels)
        control_thread.join()
        
        # Should complete despite pause
        assert len(results) == len(sample_channels)
        assert "paused" in pause_resume_executed
        assert "resumed" in pause_resume_executed
    
    def test_empty_channel_list(self, mock_config_manager):
        """Test collection with empty channel list"""
        collector = TestCollectorForTesting()
        
        results = collector.collect([])
        
        assert len(results) == 0
        assert collector.progress.total_channels == 0
        assert collector.progress.processed_channels == 0
    
    def test_collection_statistics_tracking(self, mock_config_manager, sample_channels):
        """Test that collection properly tracks statistics"""
        collector = TestCollectorForTesting(batch_size=3)
        collector.fail_on_channel_ids = {sample_channels[2].channel_id, sample_channels[7].channel_id}
        
        results = collector.collect(sample_channels)
        
        # Should have processed all channels but 2 failed
        assert len(results) == 8  # 10 - 2 failures
        assert collector.progress.processed_channels == 10
        assert collector.progress.successful_channels == 8
        assert collector.progress.failed_channels == 2
        
        assert collector.stats.requests_made == 10
        assert collector.stats.requests_successful == 8
        assert collector.stats.requests_failed == 2


@pytest.mark.performance
class TestBaseCollectorPerformance:
    """Performance tests for BaseCollector"""
    
    def test_large_dataset_processing(self, mock_config_manager, large_channel_dataset, performance_timer, memory_profiler):
        """Test processing large dataset performance"""
        # Use subset for reasonable test time
        test_channels = large_channel_dataset[:100]
        
        collector = TestCollectorForTesting(
            batch_size=25,
            max_workers=4
        )
        
        memory_profiler.start()
        performance_timer.start()
        
        results = collector.collect(test_channels)
        
        performance_timer.stop()
        memory_profiler.update()
        
        assert len(results) == 100
        assert performance_timer.elapsed() < 10.0  # Should complete within 10 seconds
        
        # Memory usage should be reasonable
        peak_memory_mb = memory_profiler.get_peak_mb()
        if peak_memory_mb > 0:  # If psutil is available
            assert peak_memory_mb < 100  # Should use less than 100MB
    
    def test_concurrent_vs_sequential_performance(self, mock_config_manager, sample_channels, performance_timer):
        """Compare concurrent vs sequential processing performance"""
        test_channels = sample_channels
        
        # Test sequential processing
        sequential_collector = TestCollectorForTesting(max_workers=1, batch_size=5)
        sequential_collector.processing_delay = 0.01  # Add small delay to see difference
        
        performance_timer.start()
        sequential_results = sequential_collector.collect(test_channels)
        performance_timer.stop()
        sequential_time = performance_timer.elapsed()
        
        # Test concurrent processing
        concurrent_collector = TestCollectorForTesting(max_workers=3, batch_size=5)
        concurrent_collector.processing_delay = 0.01
        
        performance_timer.start()
        concurrent_results = concurrent_collector.collect(test_channels)
        performance_timer.stop()
        concurrent_time = performance_timer.elapsed()
        
        # Both should produce same results
        assert len(sequential_results) == len(concurrent_results) == len(test_channels)
        
        # Concurrent should be faster (with processing delay)
        # Note: This might not always be true due to threading overhead with small datasets
        print(f"Sequential: {sequential_time:.3f}s, Concurrent: {concurrent_time:.3f}s")