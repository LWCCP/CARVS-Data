"""
Base Collector Class for YouNiverse Dataset Enrichment

This module provides the abstract base class for all data collectors,
implementing common functionality including rate limiting, circuit breakers,
retry logic, and progress tracking.

Author: feature-developer
"""

import time
import logging
import asyncio
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Iterator, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
import json
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from ..models.channel import Channel, ChannelStatus
from ..utils.api_helpers import RateLimiter, CircuitBreaker, create_rate_limiter, create_circuit_breaker
from ..utils.retry_utils import RetryManager, AGGRESSIVE_WEB_RETRY
from ..core.config import config_manager


@dataclass
class CollectionProgress:
    """Progress tracking for data collection"""
    total_channels: int = 0
    processed_channels: int = 0
    successful_channels: int = 0
    failed_channels: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    current_phase: str = ""
    current_batch: int = 0
    total_batches: int = 0
    
    def get_success_rate(self) -> float:
        """Get success rate as percentage"""
        if self.processed_channels == 0:
            return 0.0
        return (self.successful_channels / self.processed_channels) * 100
    
    def get_elapsed_time(self) -> timedelta:
        """Get elapsed time since start"""
        if not self.start_time:
            return timedelta(0)
        end = self.end_time or datetime.now()
        return end - self.start_time
    
    def get_estimated_completion(self) -> Optional[datetime]:
        """Estimate completion time based on current progress"""
        if not self.start_time or self.processed_channels == 0:
            return None
        
        elapsed = self.get_elapsed_time()
        rate = self.processed_channels / elapsed.total_seconds()
        
        if rate <= 0:
            return None
        
        remaining = self.total_channels - self.processed_channels
        estimated_seconds = remaining / rate
        
        return datetime.now() + timedelta(seconds=estimated_seconds)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'total_channels': self.total_channels,
            'processed_channels': self.processed_channels,
            'successful_channels': self.successful_channels,
            'failed_channels': self.failed_channels,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'current_phase': self.current_phase,
            'current_batch': self.current_batch,
            'total_batches': self.total_batches,
            'success_rate': self.get_success_rate(),
            'elapsed_time': str(self.get_elapsed_time()),
            'estimated_completion': self.get_estimated_completion().isoformat() if self.get_estimated_completion() else None
        }


@dataclass
class CollectorStats:
    """Statistics for collector performance"""
    requests_made: int = 0
    requests_successful: int = 0
    requests_failed: int = 0
    total_wait_time: float = 0.0
    circuit_breaker_trips: int = 0
    rate_limit_hits: int = 0
    retry_attempts: int = 0
    data_points_collected: int = 0
    bytes_downloaded: int = 0
    
    def get_request_success_rate(self) -> float:
        """Get request success rate"""
        if self.requests_made == 0:
            return 0.0
        return (self.requests_successful / self.requests_made) * 100
    
    def get_average_wait_time(self) -> float:
        """Get average wait time per request"""
        if self.requests_made == 0:
            return 0.0
        return self.total_wait_time / self.requests_made


class BaseCollector(ABC):
    """Abstract base class for all data collectors"""
    
    def __init__(self, 
                 collector_name: str,
                 service_name: str,
                 batch_size: int = 50,
                 max_workers: int = 1,
                 checkpoint_interval: int = 100,
                 enable_checkpoints: bool = True):
        
        self.collector_name = collector_name
        self.service_name = service_name
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.checkpoint_interval = checkpoint_interval
        self.enable_checkpoints = enable_checkpoints
        
        # Initialize components
        self.logger = logging.getLogger(f"{__name__}.{collector_name}")
        self.rate_limiter = create_rate_limiter(service_name)
        self.circuit_breaker = create_circuit_breaker(service_name)
        self.retry_manager = AGGRESSIVE_WEB_RETRY
        
        # Progress tracking
        self.progress = CollectionProgress()
        self.stats = CollectorStats()
        
        # State management
        self._running = False
        self._paused = False
        self._stop_requested = False
        self._lock = threading.Lock()
        
        # Checkpoint management
        self.checkpoint_dir = Path("data/intermediate/checkpoints")
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        
        # Callbacks
        self.progress_callback: Optional[Callable[[CollectionProgress], None]] = None
        self.error_callback: Optional[Callable[[str, Exception], None]] = None
    
    def set_progress_callback(self, callback: Callable[[CollectionProgress], None]) -> None:
        """Set callback for progress updates"""
        self.progress_callback = callback
    
    def set_error_callback(self, callback: Callable[[str, Exception], None]) -> None:
        """Set callback for error handling"""
        self.error_callback = callback
    
    def _notify_progress(self) -> None:
        """Notify progress callback if set"""
        if self.progress_callback:
            try:
                self.progress_callback(self.progress)
            except Exception as e:
                self.logger.warning(f"Progress callback failed: {e}")
    
    def _notify_error(self, channel_id: str, error: Exception) -> None:
        """Notify error callback if set"""
        if self.error_callback:
            try:
                self.error_callback(channel_id, error)
            except Exception as e:
                self.logger.warning(f"Error callback failed: {e}")
    
    def pause(self) -> None:
        """Pause collection"""
        with self._lock:
            self._paused = True
            self.logger.info(f"{self.collector_name} paused")
    
    def resume(self) -> None:
        """Resume collection"""
        with self._lock:
            self._paused = False
            self.logger.info(f"{self.collector_name} resumed")
    
    def stop(self) -> None:
        """Request stop of collection"""
        with self._lock:
            self._stop_requested = True
            self.logger.info(f"{self.collector_name} stop requested")
    
    def is_running(self) -> bool:
        """Check if collector is running"""
        return self._running
    
    def is_paused(self) -> bool:
        """Check if collector is paused"""
        return self._paused
    
    def should_stop(self) -> bool:
        """Check if stop was requested"""
        return self._stop_requested
    
    def _wait_while_paused(self) -> None:
        """Wait while paused"""
        while self._paused and not self._stop_requested:
            time.sleep(0.1)
    
    def save_checkpoint(self, processed_channels: List[str], failed_channels: List[str] = None) -> None:
        """Save checkpoint data"""
        if not self.enable_checkpoints:
            return
        
        checkpoint_data = {
            'collector_name': self.collector_name,
            'timestamp': datetime.now().isoformat(),
            'progress': self.progress.to_dict(),
            'stats': {
                'requests_made': self.stats.requests_made,
                'requests_successful': self.stats.requests_successful,
                'requests_failed': self.stats.requests_failed,
                'data_points_collected': self.stats.data_points_collected
            },
            'processed_channels': processed_channels,
            'failed_channels': failed_channels or []
        }
        
        checkpoint_file = self.checkpoint_dir / f"{self.collector_name}_checkpoint.json"
        try:
            with open(checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(checkpoint_data, f, indent=2)
            self.logger.debug(f"Checkpoint saved to {checkpoint_file}")
        except Exception as e:
            self.logger.error(f"Failed to save checkpoint: {e}")
    
    def load_checkpoint(self) -> Optional[Dict[str, Any]]:
        """Load checkpoint data"""
        if not self.enable_checkpoints:
            return None
        
        checkpoint_file = self.checkpoint_dir / f"{self.collector_name}_checkpoint.json"
        
        if not checkpoint_file.exists():
            return None
        
        try:
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                checkpoint_data = json.load(f)
            
            self.logger.info(f"Checkpoint loaded from {checkpoint_file}")
            return checkpoint_data
        
        except Exception as e:
            self.logger.error(f"Failed to load checkpoint: {e}")
            return None
    
    def clear_checkpoint(self) -> None:
        """Clear checkpoint file"""
        checkpoint_file = self.checkpoint_dir / f"{self.collector_name}_checkpoint.json"
        
        if checkpoint_file.exists():
            try:
                checkpoint_file.unlink()
                self.logger.info("Checkpoint cleared")
            except Exception as e:
                self.logger.error(f"Failed to clear checkpoint: {e}")
    
    def _batch_channels(self, channels: List[Channel]) -> Iterator[List[Channel]]:
        """Split channels into batches"""
        for i in range(0, len(channels), self.batch_size):
            yield channels[i:i + self.batch_size]
    
    def _process_batch_sequential(self, batch: List[Channel]) -> List[Channel]:
        """Process batch sequentially"""
        results = []
        
        for channel in batch:
            if self.should_stop():
                break
            
            self._wait_while_paused()
            
            try:
                # Apply rate limiting
                start_time = time.time()
                while not self.rate_limiter.acquire():
                    if self.should_stop():
                        break
                    wait_time = self.rate_limiter.wait_time()
                    self.stats.rate_limit_hits += 1
                    time.sleep(wait_time)
                
                self.stats.total_wait_time += time.time() - start_time
                
                # Process single channel
                processed_channel = self._process_single_channel(channel)
                if processed_channel:
                    results.append(processed_channel)
                    self.stats.requests_successful += 1
                    self.progress.successful_channels += 1
                else:
                    self.stats.requests_failed += 1
                    self.progress.failed_channels += 1
                
                self.stats.requests_made += 1
                self.progress.processed_channels += 1
                
            except Exception as e:
                self.logger.error(f"Error processing channel {channel.channel_id}: {e}")
                self.stats.requests_failed += 1
                self.progress.failed_channels += 1
                self.progress.processed_channels += 1
                self._notify_error(channel.channel_id, e)
        
        return results
    
    def _process_batch_concurrent(self, batch: List[Channel]) -> List[Channel]:
        """Process batch with concurrent workers"""
        results = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_channel = {
                executor.submit(self._process_single_channel_with_limits, channel): channel
                for channel in batch
            }
            
            # Collect results
            for future in as_completed(future_to_channel):
                if self.should_stop():
                    break
                
                channel = future_to_channel[future]
                
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                        self.stats.requests_successful += 1
                        self.progress.successful_channels += 1
                    else:
                        self.stats.requests_failed += 1
                        self.progress.failed_channels += 1
                    
                    self.stats.requests_made += 1
                    self.progress.processed_channels += 1
                    
                except Exception as e:
                    self.logger.error(f"Error processing channel {channel.channel_id}: {e}")
                    self.stats.requests_failed += 1
                    self.progress.failed_channels += 1
                    self.progress.processed_channels += 1
                    self._notify_error(channel.channel_id, e)
        
        return results
    
    def _process_single_channel_with_limits(self, channel: Channel) -> Optional[Channel]:
        """Process single channel with rate limiting and circuit breaker"""
        if self.should_stop():
            return None
        
        self._wait_while_paused()
        
        # Apply rate limiting
        start_time = time.time()
        while not self.rate_limiter.acquire():
            if self.should_stop():
                return None
            wait_time = self.rate_limiter.wait_time()
            self.stats.rate_limit_hits += 1
            time.sleep(wait_time)
        
        self.stats.total_wait_time += time.time() - start_time
        
        # Process with circuit breaker protection
        try:
            return self.circuit_breaker.call(self._process_single_channel, channel)
        except Exception as e:
            if "Circuit breaker is open" in str(e):
                self.stats.circuit_breaker_trips += 1
                self.logger.warning(f"Circuit breaker tripped for {channel.channel_id}")
            raise e
    
    @abstractmethod
    def _process_single_channel(self, channel: Channel) -> Optional[Channel]:
        """
        Process a single channel - to be implemented by subclasses
        
        Args:
            channel: Channel to process
            
        Returns:
            Processed channel or None if failed
        """
        pass
    
    @abstractmethod
    def get_collection_phase_name(self) -> str:
        """Get the name of this collection phase"""
        pass
    
    def collect(self, channels: List[Channel]) -> List[Channel]:
        """
        Main collection method
        
        Args:
            channels: List of channels to process
            
        Returns:
            List of processed channels
        """
        if not channels:
            self.logger.warning("No channels provided for collection")
            return []
        
        # Initialize progress
        self.progress = CollectionProgress(
            total_channels=len(channels),
            start_time=datetime.now(),
            current_phase=self.get_collection_phase_name()
        )
        
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
        self.progress.successful_channels = self.progress.processed_channels  # Assume previous were successful
        
        self.logger.info(f"Starting {self.collector_name} collection for {len(remaining_channels)} channels")
        
        results = []
        batches = list(self._batch_channels(remaining_channels))
        self.progress.total_batches = len(batches)
        
        try:
            for batch_idx, batch in enumerate(batches):
                if self.should_stop():
                    self.logger.info("Collection stopped by request")
                    break
                
                self.progress.current_batch = batch_idx + 1
                self.logger.info(f"Processing batch {batch_idx + 1}/{len(batches)} ({len(batch)} channels)")
                
                # Process batch
                if self.max_workers > 1:
                    batch_results = self._process_batch_concurrent(batch)
                else:
                    batch_results = self._process_batch_sequential(batch)
                
                results.extend(batch_results)
                
                # Save checkpoint periodically
                if self.enable_checkpoints and (batch_idx + 1) % self.checkpoint_interval == 0:
                    all_processed = list(processed_channel_ids) + [ch.channel_id for ch in results]
                    self.save_checkpoint(all_processed)
                
                # Notify progress
                self._notify_progress()
                
                self.logger.info(f"Batch {batch_idx + 1} completed. "
                               f"Success rate: {self.progress.get_success_rate():.1f}%")
        
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
            self.logger.info(f"{self.collector_name} collection completed")
            self.logger.info(f"Total channels: {self.progress.total_channels}")
            self.logger.info(f"Processed: {self.progress.processed_channels}")
            self.logger.info(f"Successful: {self.progress.successful_channels}")
            self.logger.info(f"Failed: {self.progress.failed_channels}")
            self.logger.info(f"Success rate: {self.progress.get_success_rate():.1f}%")
            self.logger.info(f"Elapsed time: {self.progress.get_elapsed_time()}")
            self.logger.info(f"Request success rate: {self.stats.get_request_success_rate():.1f}%")
        
        return results
    
    def get_progress(self) -> CollectionProgress:
        """Get current progress"""
        return self.progress
    
    def get_stats(self) -> CollectorStats:
        """Get current statistics"""
        return self.stats