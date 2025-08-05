"""
API Authentication and Rate Limiting Utilities for YouNiverse Dataset Enrichment

This module provides authentication helpers, rate limiting infrastructure,
and API client utilities for the data collection pipeline.

Author: feature-developer
"""

import time
import asyncio
import random
from typing import Dict, Any, Optional, List, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import deque
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import threading
from enum import Enum

from ..core.config import config_manager


class CircuitBreakerState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, blocking requests
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class RateLimitBucket:
    """Token bucket for rate limiting"""
    capacity: int
    tokens: float
    refill_rate: float  # tokens per second
    last_refill: float = field(default_factory=time.time)
    
    def consume(self, tokens: int = 1) -> bool:
        """Try to consume tokens from bucket"""
        now = time.time()
        
        # Refill bucket
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now
        
        # Check if we have enough tokens
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False
    
    def time_until_available(self, tokens: int = 1) -> float:
        """Time until enough tokens are available"""
        if self.tokens >= tokens:
            return 0.0
        
        needed_tokens = tokens - self.tokens
        return needed_tokens / self.refill_rate


@dataclass
class CircuitBreakerStats:
    """Circuit breaker statistics"""
    failure_count: int = 0
    last_failure_time: Optional[float] = None
    total_requests: int = 0
    total_failures: int = 0
    state_changed_at: float = field(default_factory=time.time)


class CircuitBreaker:
    """Circuit breaker for API calls"""
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 300, 
                 half_open_max_calls: int = 3):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        
        self.state = CircuitBreakerState.CLOSED
        self.stats = CircuitBreakerStats()
        self.half_open_calls = 0
        self._lock = threading.Lock()
    
    def call(self, func: Callable, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        with self._lock:
            if not self._can_execute():
                raise Exception(f"Circuit breaker is {self.state.value}")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e
    
    def _can_execute(self) -> bool:
        """Check if request can be executed"""
        now = time.time()
        
        if self.state == CircuitBreakerState.CLOSED:
            return True
        elif self.state == CircuitBreakerState.OPEN:
            if now - self.stats.state_changed_at >= self.recovery_timeout:
                self.state = CircuitBreakerState.HALF_OPEN
                self.half_open_calls = 0
                self.stats.state_changed_at = now
                return True
            return False
        elif self.state == CircuitBreakerState.HALF_OPEN:
            return self.half_open_calls < self.half_open_max_calls
        
        return False
    
    def _on_success(self):
        """Handle successful request"""
        with self._lock:
            self.stats.total_requests += 1
            
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.half_open_calls += 1
                if self.half_open_calls >= self.half_open_max_calls:
                    self.state = CircuitBreakerState.CLOSED
                    self.stats.failure_count = 0
                    self.stats.state_changed_at = time.time()
    
    def _on_failure(self):
        """Handle failed request"""
        with self._lock:
            self.stats.total_requests += 1
            self.stats.total_failures += 1
            self.stats.failure_count += 1
            self.stats.last_failure_time = time.time()
            
            if self.stats.failure_count >= self.failure_threshold:
                self.state = CircuitBreakerState.OPEN
                self.stats.state_changed_at = time.time()
            elif self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.OPEN
                self.stats.state_changed_at = time.time()


class ExponentialBackoff:
    """Exponential backoff calculator"""
    
    def __init__(self, initial_delay: float = 1.0, max_delay: float = 60.0, 
                 multiplier: float = 2.0, jitter: bool = True):
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.multiplier = multiplier
        self.jitter = jitter
    
    def get_delay(self, attempt: int) -> float:
        """Calculate delay for given attempt number"""
        delay = min(self.initial_delay * (self.multiplier ** attempt), self.max_delay)
        
        if self.jitter:
            delay *= (0.5 + random.random() * 0.5)  # 50-100% of calculated delay
        
        return delay


class RateLimiter:
    """Advanced rate limiter with multiple strategies"""
    
    def __init__(self, requests_per_second: float = 1.0, burst_allowance: int = 3, 
                 sliding_window_seconds: int = 60):
        self.bucket = RateLimitBucket(
            capacity=burst_allowance,
            tokens=burst_allowance,
            refill_rate=requests_per_second
        )
        
        # Sliding window tracking
        self.sliding_window_seconds = sliding_window_seconds
        self.request_times = deque()
        self._lock = threading.Lock()
    
    def acquire(self, tokens: int = 1) -> bool:
        """Try to acquire permission for request"""
        with self._lock:
            now = time.time()
            
            # Clean old requests from sliding window
            cutoff = now - self.sliding_window_seconds
            while self.request_times and self.request_times[0] < cutoff:
                self.request_times.popleft()
            
            # Check if we can make the request
            if self.bucket.consume(tokens):
                self.request_times.append(now)
                return True
            
            return False
    
    def wait_time(self, tokens: int = 1) -> float:
        """Get wait time until request can be made"""
        with self._lock:
            return self.bucket.time_until_available(tokens)
    
    async def acquire_async(self, tokens: int = 1) -> None:
        """Async version of acquire with automatic waiting"""
        while not self.acquire(tokens):
            wait_time = self.wait_time(tokens)
            await asyncio.sleep(wait_time)
    
    def __enter__(self):
        """Context manager entry - acquire rate limit token"""
        while not self.acquire():
            wait_time = self.wait_time()
            time.sleep(wait_time)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - nothing to do"""
        return False


class APIClient:
    """Base API client with rate limiting and retry logic"""
    
    def __init__(self, base_url: str = "", timeout: int = 30, 
                 rate_limiter: Optional[RateLimiter] = None,
                 circuit_breaker: Optional[CircuitBreaker] = None,
                 backoff: Optional[ExponentialBackoff] = None,
                 max_retries: int = 3,
                 retry_status_codes: List[int] = None,
                 session: Optional[requests.Session] = None):
        
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.rate_limiter = rate_limiter
        self.circuit_breaker = circuit_breaker
        self.backoff = backoff or ExponentialBackoff()
        self.max_retries = max_retries
        self.retry_status_codes = retry_status_codes or [429, 500, 502, 503, 504]
        
        # Create session with retry adapter
        self.session = session or requests.Session()
        if not session:
            retry_strategy = Retry(
                total=0,  # We handle retries manually
                backoff_factor=0.1,
                status_forcelist=self.retry_status_codes,
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            self.session.mount("http://", adapter)
            self.session.mount("https://", adapter)
        
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def _should_retry(self, response: requests.Response, exception: Exception = None) -> bool:
        """Determine if request should be retried"""
        if exception:
            return True
        
        if response.status_code in self.retry_status_codes:
            return True
        
        return False
    
    def _make_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make single HTTP request"""
        full_url = f"{self.base_url}/{url.lstrip('/')}" if self.base_url else url
        
        kwargs.setdefault('timeout', self.timeout)
        
        self.logger.debug(f"Making {method} request to {full_url}")
        
        if self.circuit_breaker:
            return self.circuit_breaker.call(
                self.session.request, method, full_url, **kwargs
            )
        else:
            return self.session.request(method, full_url, **kwargs)
    
    def request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make HTTP request with rate limiting and retries"""
        
        # Apply rate limiting
        if self.rate_limiter:
            while not self.rate_limiter.acquire():
                wait_time = self.rate_limiter.wait_time()
                self.logger.debug(f"Rate limited, waiting {wait_time:.2f}s")
                time.sleep(wait_time)
        
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                response = self._make_request(method, url, **kwargs)
                
                if not self._should_retry(response):
                    return response
                
                self.logger.warning(f"Request failed with status {response.status_code}, attempt {attempt + 1}")
                
            except Exception as e:
                last_exception = e
                self.logger.warning(f"Request failed with exception {e}, attempt {attempt + 1}")
            
            # Don't delay after the last attempt
            if attempt < self.max_retries:
                delay = self.backoff.get_delay(attempt)
                self.logger.debug(f"Retrying in {delay:.2f}s")
                time.sleep(delay)
        
        # All retries exhausted
        if last_exception:
            raise last_exception
        else:
            response.raise_for_status()
            return response
    
    def get(self, url: str, **kwargs) -> requests.Response:
        """Make GET request"""
        return self.request('GET', url, **kwargs)
    
    def post(self, url: str, **kwargs) -> requests.Response:
        """Make POST request"""
        return self.request('POST', url, **kwargs)


class YouTubeAPIClient(APIClient):
    """YouTube Data API v3 client"""
    
    def __init__(self, api_key: str):
        config = config_manager.get_youtube_config()
        
        # Create rate limiter
        rate_limiter = RateLimiter(
            requests_per_second=config.rate_limiting.requests_per_second,
            burst_allowance=config.rate_limiting.burst_allowance,
            sliding_window_seconds=config.rate_limiting.sliding_window_seconds
        )
        
        # Create circuit breaker
        circuit_breaker = CircuitBreaker(
            failure_threshold=config.circuit_breaker.failure_threshold,
            recovery_timeout=config.circuit_breaker.recovery_timeout,
            half_open_max_calls=config.circuit_breaker.half_open_max_calls
        )
        
        # Create backoff strategy
        backoff = ExponentialBackoff(
            initial_delay=config.backoff_strategy.initial_delay,
            max_delay=config.backoff_strategy.max_delay,
            multiplier=config.backoff_strategy.multiplier,
            jitter=config.backoff_strategy.jitter
        )
        
        super().__init__(
            base_url="https://www.googleapis.com/youtube/v3",
            timeout=config.request_timeout,
            rate_limiter=rate_limiter,
            circuit_breaker=circuit_breaker,
            backoff=backoff,
            max_retries=config.retry_policy.max_retries,
            retry_status_codes=config.retry_policy.retry_on_status_codes
        )
        
        self.api_key = api_key
        self.quota_used = 0
        self.quota_limit = config.quota_limit
        self.cost_per_request = config.cost_per_request
    
    def _add_api_key(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Add API key to request parameters"""
        params = params.copy()
        params['key'] = self.api_key
        return params
    
    def _check_quota(self, cost: int = None) -> None:
        """Check if we have quota remaining"""
        cost = cost or self.cost_per_request
        if self.quota_used + cost > self.quota_limit:
            raise Exception(f"YouTube API quota exceeded: {self.quota_used}/{self.quota_limit}")
    
    def get_channels(self, channel_ids: List[str], parts: List[str] = None) -> Dict[str, Any]:
        """Get channel information"""
        parts = parts or ['snippet', 'statistics', 'status']
        
        # YouTube API allows up to 50 channel IDs per request
        batch_size = 50
        all_results = []
        
        for i in range(0, len(channel_ids), batch_size):
            batch_ids = channel_ids[i:i + batch_size]
            
            params = self._add_api_key({
                'part': ','.join(parts),
                'id': ','.join(batch_ids),
                'maxResults': 50
            })
            
            self._check_quota()
            
            response = self.get('/channels', params=params)
            data = response.json()
            
            # Update quota usage
            self.quota_used += self.cost_per_request
            
            if 'items' in data:
                all_results.extend(data['items'])
            
            # Log any errors
            if 'error' in data:
                self.logger.error(f"YouTube API error: {data['error']}")
                raise Exception(f"YouTube API error: {data['error']}")
        
        return {'items': all_results}


def create_youtube_client() -> YouTubeAPIClient:
    """Create YouTube API client from configuration"""
    config = config_manager.get_youtube_config()
    
    if not config.api_key:
        raise ValueError("YouTube API key not found in configuration")
    
    return YouTubeAPIClient(config.api_key)


def create_rate_limiter(service: str) -> RateLimiter:
    """Create rate limiter for specific service"""
    if service == 'youtube':
        config = config_manager.get_youtube_config()
        return RateLimiter(
            requests_per_second=config.rate_limiting.requests_per_second,
            burst_allowance=config.rate_limiting.burst_allowance,
            sliding_window_seconds=config.rate_limiting.sliding_window_seconds
        )
    elif service == 'wayback':
        config = config_manager.get_wayback_config()
        return RateLimiter(
            requests_per_second=config.rate_limiting.requests_per_second,
            burst_allowance=config.rate_limiting.burst_allowance,
            sliding_window_seconds=config.rate_limiting.sliding_window_seconds
        )
    elif service == 'viewstats':
        config = config_manager.get_viewstats_config()
        return RateLimiter(
            requests_per_second=config.rate_limiting.requests_per_second,
            burst_allowance=config.rate_limiting.burst_allowance,
            sliding_window_seconds=config.rate_limiting.sliding_window_seconds
        )
    else:
        raise ValueError(f"Unknown service: {service}")


def create_circuit_breaker(service: str) -> CircuitBreaker:
    """Create circuit breaker for specific service"""
    if service == 'youtube':
        config = config_manager.get_youtube_config()
    elif service == 'wayback':
        config = config_manager.get_wayback_config()
    elif service == 'viewstats':
        config = config_manager.get_viewstats_config()
    else:
        raise ValueError(f"Unknown service: {service}")
    
    return CircuitBreaker(
        failure_threshold=config.circuit_breaker.failure_threshold,
        recovery_timeout=config.circuit_breaker.recovery_timeout,
        half_open_max_calls=config.circuit_breaker.half_open_max_calls
    )