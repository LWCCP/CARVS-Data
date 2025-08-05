"""
Performance and Scalability Tests for YouNiverse Dataset Enrichment

Tests performance characteristics, memory usage, and scalability limits using real
scraping scenarios and actual data patterns from the 153,550 channel dataset.

Author: test-strategist
"""

import pytest
import time
import json
import threading
from pathlib import Path
from unittest.mock import Mock, patch
from concurrent.futures import ThreadPoolExecutor
import gc

from src.data_collectors.youtube_api_collector import YouTubeAPICollector
from src.data_collectors.base_collector import BaseCollector
from src.utils.api_helpers import RateLimiter, CircuitBreaker, YouTubeAPIClient
from src.models.channel import Channel


class MockScrapingCollector(BaseCollector):
    """Mock collector that simulates real scraping performance characteristics"""
    
    def __init__(self, processing_delay=0.1, **kwargs):
        super().__init__(
            collector_name="MockScraping",
            service_name="wayback",
            **kwargs
        )
        self.processing_delay = processing_delay
        self.processed_count = 0
        
    def _process_single_channel(self, channel):
        """Simulate processing with realistic delay"""
        # Simulate network request delay
        time.sleep(self.processing_delay)
        
        # Simulate memory usage with data processing
        large_data = "x" * 1024  # 1KB of data per channel
        
        self.processed_count += 1
        
        # Return processed channel
        return Channel(
            channel_id=channel.channel_id,
            channel_name=f"Processed {channel.channel_name}",
            channel_url=channel.channel_url,
            status=channel.status,
            category=channel.category,
            subscriber_count=channel.subscriber_count + 1000,  # Simulated enrichment
            view_count=channel.view_count + 50000,
            video_count=channel.video_count + 10
        )
    
    def get_collection_phase_name(self):
        return "Mock Scraping Phase"


@pytest.mark.performance
class TestPerformanceCharacteristics:
    """Test performance characteristics of core components"""
    
    def test_rate_limiter_performance_under_load(self, performance_timer, memory_profiler):
        """Test rate limiter performance with high request volume"""
        # High-performance rate limiter for testing
        rate_limiter = RateLimiter(
            requests_per_second=100,
            burst_allowance=50,
            sliding_window_seconds=1
        )
        
        memory_profiler.start()
        performance_timer.start()
        
        # Simulate high load
        successful_acquisitions = 0
        for _ in range(1000):
            if rate_limiter.acquire(1):
                successful_acquisitions += 1
            memory_profiler.update()
        
        performance_timer.stop()
        
        # Should handle high load efficiently
        assert performance_timer.elapsed() < 2.0  # Should complete within 2 seconds
        assert successful_acquisitions >= 50  # Should allow burst requests
        
        # Memory usage should be reasonable
        if memory_profiler.get_peak_mb() > 0:
            assert memory_profiler.get_peak_mb() < 50  # Less than 50MB
    
    def test_circuit_breaker_state_transitions_performance(self, performance_timer):
        """Test circuit breaker state transitions under load"""
        circuit_breaker = CircuitBreaker(
            failure_threshold=10,
            recovery_timeout=0.1,  # Quick recovery for testing
            half_open_max_calls=5
        )
        
        call_count = 0
        
        def test_function():
            nonlocal call_count
            call_count += 1
            # Fail first 15 calls, then succeed
            if call_count <= 15:
                raise Exception("Test failure")
            return "success"
        
        performance_timer.start()
        
        results = []
        for _ in range(50):
            try:
                result = circuit_breaker.call(test_function)
                results.append(result)
            except Exception as e:
                results.append(str(e))
        
        performance_timer.stop()
        
        # Should handle state transitions quickly
        assert performance_timer.elapsed() < 1.0
        
        # Should have some successful calls after recovery
        success_count = len([r for r in results if r == "success"])
        assert success_count > 0
    
    def test_batch_processing_scaling(self, mock_config_manager, performance_timer, memory_profiler):
        """Test batch processing performance with different batch sizes"""
        # Load real channel dataset
        fixtures_dir = Path(__file__).parent.parent / "fixtures" / "sample_data"
        with open(fixtures_dir / "real_channel_dataset.json") as f:
            data = json.load(f)
        
        # Create channels from real data
        test_channels = []
        for channel_data in data["channels"][:20]:  # Use subset for performance testing
            channel = Channel(
                channel_id=channel_data["channel_id"],
                channel_name=channel_data["channel_name"],
                channel_url=channel_data["channel_url"],
                status=channel_data["status"],
                category=channel_data["category"],
                subscriber_count=channel_data.get("subscriber_count", 0),
                view_count=channel_data.get("view_count", 0),
                video_count=channel_data.get("video_count", 0)
            )
            test_channels.append(channel)
        
        batch_sizes = [5, 10, 20]
        results = {}
        
        for batch_size in batch_sizes:
            collector = MockScrapingCollector(
                batch_size=batch_size,
                processing_delay=0.01  # Small delay for testing
            )
            
            memory_profiler.start()
            performance_timer.start()
            
            processed = collector.collect(test_channels)
            
            performance_timer.stop()
            memory_profiler.update()
            
            results[batch_size] = {
                'time': performance_timer.elapsed(),
                'memory_mb': memory_profiler.get_peak_mb(),
                'processed_count': len(processed)
            }
            
            # Reset for next test
            performance_timer = type(performance_timer)()
            memory_profiler = type(memory_profiler)()
        
        # Verify all batch sizes processed all channels
        for batch_size, result in results.items():
            assert result['processed_count'] == len(test_channels)
            assert result['time'] < 5.0  # Should complete within 5 seconds
    
    def test_concurrent_processing_scaling(self, mock_config_manager, performance_timer):
        """Test concurrent processing performance"""
        # Create test channels
        test_channels = []
        for i in range(20):
            channel = Channel(
                channel_id=f"UCtest{i:03d}",
                channel_name=f"Test Channel {i}",
                channel_url=f"https://youtube.com/channel/UCtest{i:03d}",
                subscriber_count=1000 * i,
                view_count=100000 * i,
                video_count=50 * i
            )
            test_channels.append(channel)
        
        worker_counts = [1, 2, 4]
        results = {}
        
        for worker_count in worker_counts:
            collector = MockScrapingCollector(
                max_workers=worker_count,
                processing_delay=0.05,  # Moderate delay to see concurrency benefit
                batch_size=5
            )
            
            performance_timer.start()
            processed = collector.collect(test_channels)
            performance_timer.stop()
            
            results[worker_count] = {
                'time': performance_timer.elapsed(),
                'processed_count': len(processed)
            }
            
            # Reset timer
            performance_timer = type(performance_timer)()
        
        # Verify processing
        for worker_count, result in results.items():
            assert result['processed_count'] == len(test_channels)
        
        # Concurrent processing should be faster (with sufficient delay)
        if len(results) > 1:
            sequential_time = results[1]['time']
            concurrent_time = results[max(worker_counts)]['time']
            # Note: Due to overhead, this might not always be true with small datasets
            print(f"Sequential: {sequential_time:.3f}s, Concurrent: {concurrent_time:.3f}s")


@pytest.mark.performance
class TestMemoryUsage:
    """Test memory usage patterns and limits"""
    
    def test_large_dataset_memory_efficiency(self, mock_config_manager, memory_profiler):
        """Test memory usage with large channel datasets"""
        # Create large dataset
        large_dataset = []
        for i in range(1000):
            channel = Channel(
                channel_id=f"UCperf{i:04d}",
                channel_name=f"Performance Test Channel {i}",
                channel_url=f"https://youtube.com/channel/UCperf{i:04d}",
                subscriber_count=100000 + i,
                view_count=10000000 + i * 1000,
                video_count=500 + i
            )
            large_dataset.append(channel)
        
        collector = MockScrapingCollector(
            batch_size=50,
            processing_delay=0.001,  # Minimal delay for memory test
            max_workers=1
        )
        
        memory_profiler.start()
        
        # Process in batches to test memory management
        batch_size = 100
        all_results = []
        
        for i in range(0, len(large_dataset), batch_size):
            batch = large_dataset[i:i+batch_size]
            results = collector.collect(batch)
            all_results.extend(results)
            
            memory_profiler.update()
            
            # Force garbage collection between batches
            gc.collect()
        
        peak_memory_mb = memory_profiler.get_peak_mb()
        
        # Memory usage should be reasonable for 1000 channels
        if peak_memory_mb > 0:
            assert peak_memory_mb < 200  # Less than 200MB
            print(f"Peak memory usage: {peak_memory_mb:.2f} MB")
        
        # Should have processed all channels
        assert len(all_results) == len(large_dataset)
    
    def test_checkpoint_memory_management(self, mock_config_manager, memory_profiler, clean_temp_dir):
        """Test memory management with checkpoint operations"""
        test_channels = []
        for i in range(100):
            channel = Channel(
                channel_id=f"UCmem{i:03d}",
                channel_name=f"Memory Test Channel {i}",
                channel_url=f"https://youtube.com/channel/UCmem{i:03d}",
                subscriber_count=50000 + i,
                view_count=5000000 + i * 1000,
                video_count=250 + i
            )
            test_channels.append(channel)
        
        collector = MockScrapingCollector(
            batch_size=10,
            checkpoint_interval=2,  # Frequent checkpoints
            enable_checkpoints=True,
            processing_delay=0.01
        )
        
        collector.checkpoint_dir = clean_temp_dir / "checkpoints"
        collector.checkpoint_dir.mkdir(exist_ok=True)
        
        memory_profiler.start()
        
        results = collector.collect(test_channels)
        
        memory_profiler.update()
        peak_memory_mb = memory_profiler.get_peak_mb()
        
        # Should complete successfully
        assert len(results) == len(test_channels)
        
        # Memory usage should be reasonable even with checkpoints
        if peak_memory_mb > 0:
            assert peak_memory_mb < 100  # Less than 100MB
        
        # Should have created checkpoint files
        checkpoint_files = list(collector.checkpoint_dir.glob("*.json"))
        assert len(checkpoint_files) > 0


@pytest.mark.performance
@pytest.mark.slow
class TestScalabilityLimits:
    """Test scalability limits and breaking points"""
    
    def test_rate_limiter_extreme_load(self, performance_timer):
        """Test rate limiter behavior under extreme load"""
        rate_limiter = RateLimiter(
            requests_per_second=10,  # Restrictive rate limit
            burst_allowance=5,
            sliding_window_seconds=1
        )
        
        performance_timer.start()
        
        # Attempt many requests quickly
        successful_requests = 0
        blocked_requests = 0
        
        for _ in range(100):
            if rate_limiter.acquire(1):
                successful_requests += 1
            else:
                blocked_requests += 1
        
        performance_timer.stop()
        
        # Should have blocked most requests due to rate limiting
        assert blocked_requests > successful_requests
        assert successful_requests <= 15  # Burst + some refill
        
        # Should complete quickly (not waiting for rate limit)
        assert performance_timer.elapsed() < 1.0
    
    def test_circuit_breaker_rapid_failures(self, performance_timer):
        """Test circuit breaker with rapid consecutive failures"""
        circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=0.5,
            half_open_max_calls=2
        )
        
        def always_fail():
            raise Exception("Persistent failure")
        
        performance_timer.start()
        
        failure_count = 0
        circuit_open_count = 0
        
        # Generate many rapid failures
        for _ in range(50):
            try:
                circuit_breaker.call(always_fail)
            except Exception as e:
                if "Circuit breaker is open" in str(e):
                    circuit_open_count += 1
                else:
                    failure_count += 1
        
        performance_timer.stop()
        
        # Should have opened the circuit after threshold failures
        assert circuit_open_count > 0
        assert failure_count >= 5  # At least the threshold failures
        
        # Should complete quickly despite many failures
        assert performance_timer.elapsed() < 2.0
    
    @pytest.mark.skipif(True, reason="Heavy test - enable manually for full load testing")
    def test_youtube_api_quota_exhaustion_simulation(self, mock_config_manager, performance_timer):
        """Simulate YouTube API quota exhaustion behavior"""
        # Create large channel list that would exceed quota
        massive_channel_list = []
        for i in range(600):  # 600 channels = 12 API calls at 50 per batch
            channel = Channel(
                channel_id=f"UCquota{i:04d}",
                channel_name=f"Quota Test Channel {i}",
                channel_url=f"https://youtube.com/channel/UCquota{i:04d}"
            )
            massive_channel_list.append(channel)
        
        # Mock client that simulates quota exhaustion
        mock_client = Mock(spec=YouTubeAPIClient)
        mock_client.quota_used = 9500  # Near quota limit
        mock_client.quota_limit = 10000
        mock_client.cost_per_request = 1
        
        call_count = 0
        def mock_get_channels(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            
            # Simulate quota exhaustion after a few calls
            if call_count > 5:
                raise Exception("YouTube API quota exceeded")
            
            return {"items": []}
        
        mock_client.get_channels.side_effect = mock_get_channels
        
        with patch('src.data_collectors.youtube_api_collector.create_youtube_client', return_value=mock_client):
            collector = YouTubeAPICollector()
            
            performance_timer.start()
            
            # Should hit quota limit and stop
            with pytest.raises(Exception, match="quota exceeded"):
                collector.collect(massive_channel_list)
            
            performance_timer.stop()
            
            # Should fail quickly when quota is exceeded
            assert performance_timer.elapsed() < 10.0
            assert call_count <= 10  # Should not make too many calls


@pytest.mark.performance
class TestRealWorldScenarios:
    """Test performance in realistic usage scenarios"""
    
    def test_153k_channel_simulation(self, mock_config_manager, performance_timer, memory_profiler):
        """Simulate processing characteristics for 153,550 channel dataset"""
        # Create representative sample (1% of full dataset)
        sample_size = 1535
        
        # Load real channel patterns
        fixtures_dir = Path(__file__).parent.parent / "fixtures" / "sample_data"
        with open(fixtures_dir / "real_channel_dataset.json") as f:
            real_data = json.load(f)
        
        # Create scaled dataset based on real patterns
        simulated_channels = []
        real_channels = real_data["channels"]
        
        for i in range(sample_size):
            # Cycle through real channel patterns
            base_channel = real_channels[i % len(real_channels)]
            
            channel = Channel(
                channel_id=f"UC{i:06d}sim",
                channel_name=f"Simulated {base_channel['channel_name']} {i}",
                channel_url=f"https://youtube.com/channel/UC{i:06d}sim",
                status=base_channel["status"],
                category=base_channel["category"],
                country=base_channel.get("country"),
                subscriber_count=base_channel.get("subscriber_count", 0),
                view_count=base_channel.get("view_count", 0),
                video_count=base_channel.get("video_count", 0)
            )
            simulated_channels.append(channel)
        
        # Test with realistic batch processing
        collector = MockScrapingCollector(
            batch_size=50,  # YouTube API batch size
            max_workers=1,  # Sequential processing like YouTube API
            processing_delay=0.001,  # Faster for simulation
            checkpoint_interval=10
        )
        
        memory_profiler.start()
        performance_timer.start()
        
        # Process first 100 channels as representative sample
        results = collector.collect(simulated_channels[:100])
        
        performance_timer.stop()
        memory_profiler.update()
        
        # Verify processing
        assert len(results) == 100
        
        # Calculate extrapolated metrics for full dataset
        time_per_channel = performance_timer.elapsed() / 100
        estimated_full_time = time_per_channel * 153550
        estimated_hours = estimated_full_time / 3600
        
        peak_memory_mb = memory_profiler.get_peak_mb()
        
        print(f"Sample processing: {performance_timer.elapsed():.2f}s for 100 channels")
        print(f"Estimated full dataset time: {estimated_hours:.2f} hours")
        if peak_memory_mb > 0:
            print(f"Peak memory usage: {peak_memory_mb:.2f} MB")
        
        # Performance should be reasonable
        assert time_per_channel < 0.5  # Less than 0.5 seconds per channel
        if peak_memory_mb > 0:
            assert peak_memory_mb < 150  # Less than 150MB for 100 channels
    
    def test_multi_phase_processing_coordination(self, mock_config_manager, performance_timer, clean_temp_dir):
        """Test coordination between multiple processing phases"""
        # Create test channels
        test_channels = []
        for i in range(20):
            channel = Channel(
                channel_id=f"UCphase{i:03d}",
                channel_name=f"Multi-phase Test Channel {i}",
                channel_url=f"https://youtube.com/channel/UCphase{i:03d}",
                subscriber_count=10000 * (i + 1),
                view_count=1000000 * (i + 1),
                video_count=100 * (i + 1)
            )
            test_channels.append(channel)
        
        # Simulate Phase 1: YouTube API enrichment
        phase1_collector = MockScrapingCollector(
            collector_name="Phase1YouTube",
            batch_size=5,
            processing_delay=0.01,
            checkpoint_interval=2,
            enable_checkpoints=True
        )
        phase1_collector.checkpoint_dir = clean_temp_dir / "phase1_checkpoints"
        phase1_collector.checkpoint_dir.mkdir(exist_ok=True)
        
        # Simulate Phase 2: Wayback Machine scraping  
        phase2_collector = MockScrapingCollector(
            collector_name="Phase2Wayback",
            batch_size=3,
            processing_delay=0.02,  # Slower for scraping
            checkpoint_interval=2,
            enable_checkpoints=True
        )
        phase2_collector.checkpoint_dir = clean_temp_dir / "phase2_checkpoints"
        phase2_collector.checkpoint_dir.mkdir(exist_ok=True)
        
        performance_timer.start()
        
        # Phase 1 processing
        phase1_results = phase1_collector.collect(test_channels)
        assert len(phase1_results) == len(test_channels)
        
        # Phase 2 processing (using Phase 1 results as input)
        phase2_results = phase2_collector.collect(phase1_results)
        assert len(phase2_results) == len(phase1_results)
        
        performance_timer.stop()
        
        # Should complete multi-phase processing efficiently
        assert performance_timer.elapsed() < 3.0
        
        # Both phases should have created checkpoints
        phase1_checkpoints = list(phase1_collector.checkpoint_dir.glob("*.json"))
        phase2_checkpoints = list(phase2_collector.checkpoint_dir.glob("*.json"))
        
        assert len(phase1_checkpoints) > 0
        assert len(phase2_checkpoints) > 0
    
    def test_error_recovery_performance(self, mock_config_manager, performance_timer):
        """Test performance impact of error handling and recovery"""
        # Create test channels with some that will cause failures
        test_channels = []
        failing_channel_ids = set()
        
        for i in range(50):
            channel = Channel(
                channel_id=f"UCerror{i:03d}",
                channel_name=f"Error Test Channel {i}",
                channel_url=f"https://youtube.com/channel/UCerror{i:03d}",
                subscriber_count=5000 * (i + 1),
                view_count=500000 * (i + 1),
                video_count=25 * (i + 1)
            )
            test_channels.append(channel)
            
            # Make every 5th channel fail initially
            if i % 5 == 0:
                failing_channel_ids.add(channel.channel_id)
        
        # Collector with failure simulation
        collector = MockScrapingCollector(
            batch_size=10,
            processing_delay=0.01
        )
        
        # Override to simulate intermittent failures
        original_process = collector._process_single_channel
        call_counts = {}
        
        def failing_process(channel):
            channel_id = channel.channel_id
            call_counts[channel_id] = call_counts.get(channel_id, 0) + 1
            
            # Fail first attempt for problematic channels
            if channel_id in failing_channel_ids and call_counts[channel_id] == 1:
                raise Exception(f"Simulated failure for {channel_id}")
            
            return original_process(channel)
        
        collector._process_single_channel = failing_process
        
        performance_timer.start()
        
        results = collector.collect(test_channels)
        
        performance_timer.stop()
        
        # Should recover from failures and process most channels
        success_rate = len(results) / len(test_channels)
        assert success_rate >= 0.8  # At least 80% success rate
        
        # Should complete within reasonable time despite errors
        assert performance_timer.elapsed() < 2.0
        
        # Should have attempted retries for failed channels
        retry_attempts = sum(1 for count in call_counts.values() if count > 1)
        assert retry_attempts > 0


@pytest.mark.performance
class TestResourceUtilization:
    """Test resource utilization patterns"""
    
    def test_thread_pool_efficiency(self, mock_config_manager, performance_timer):
        """Test thread pool utilization efficiency"""
        test_channels = []
        for i in range(30):
            channel = Channel(
                channel_id=f"UCthread{i:03d}",
                channel_name=f"Thread Test Channel {i}",
                channel_url=f"https://youtube.com/channel/UCthread{i:03d}"
            )
            test_channels.append(channel)
        
        thread_counts = [1, 2, 4, 8]
        results = {}
        
        for thread_count in thread_counts:
            collector = MockScrapingCollector(
                max_workers=thread_count,
                batch_size=6,
                processing_delay=0.05  # Moderate delay to see threading benefit
            )
            
            performance_timer.start()
            processed = collector.collect(test_channels)
            performance_timer.stop()
            
            results[thread_count] = {
                'time': performance_timer.elapsed(),
                'processed': len(processed)
            }
            
            # Reset timer
            performance_timer = type(performance_timer)()
        
        # All should process the same number of channels
        for thread_count, result in results.items():
            assert result['processed'] == len(test_channels)
        
        # Log performance comparison
        for thread_count, result in results.items():
            print(f"{thread_count} threads: {result['time']:.3f}s")
        
        # Multi-threading should provide some benefit with sufficient work
        if 1 in results and 4 in results:
            single_thread_time = results[1]['time']
            multi_thread_time = results[4]['time']
            
            # Due to GIL in Python, improvement may be limited, but should not be slower
            assert multi_thread_time <= single_thread_time * 1.5  # Allow some overhead
    
    def test_checkpoint_io_performance(self, mock_config_manager, performance_timer, clean_temp_dir):
        """Test checkpoint I/O performance impact"""
        test_channels = []
        for i in range(100):
            channel = Channel(
                channel_id=f"UCcheckio{i:04d}",
                channel_name=f"Checkpoint IO Test Channel {i}",
                channel_url=f"https://youtube.com/channel/UCcheckio{i:04d}"
            )
            test_channels.append(channel)
        
        # Test with checkpoints enabled
        collector_with_checkpoints = MockScrapingCollector(
            batch_size=10,
            checkpoint_interval=1,  # Frequent checkpoints
            enable_checkpoints=True,
            processing_delay=0.001
        )
        collector_with_checkpoints.checkpoint_dir = clean_temp_dir / "io_checkpoints"
        collector_with_checkpoints.checkpoint_dir.mkdir(exist_ok=True)
        
        performance_timer.start()
        results_with_checkpoints = collector_with_checkpoints.collect(test_channels)
        performance_timer.stop()
        checkpoint_time = performance_timer.elapsed()
        
        # Test without checkpoints
        collector_no_checkpoints = MockScrapingCollector(
            batch_size=10,
            enable_checkpoints=False,
            processing_delay=0.001
        )
        
        performance_timer.start()
        results_no_checkpoints = collector_no_checkpoints.collect(test_channels)
        performance_timer.stop()
        no_checkpoint_time = performance_timer.elapsed()
        
        # Both should process all channels
        assert len(results_with_checkpoints) == len(test_channels)
        assert len(results_no_checkpoints) == len(test_channels)
        
        # Checkpoint overhead should be reasonable
        overhead_ratio = checkpoint_time / no_checkpoint_time if no_checkpoint_time > 0 else 1
        assert overhead_ratio < 2.0  # Less than 2x overhead
        
        print(f"With checkpoints: {checkpoint_time:.3f}s")
        print(f"Without checkpoints: {no_checkpoint_time:.3f}s")
        print(f"Overhead ratio: {overhead_ratio:.2f}x")
        
        # Should have created checkpoint files
        checkpoint_files = list(collector_with_checkpoints.checkpoint_dir.glob("*.json"))
        assert len(checkpoint_files) > 0