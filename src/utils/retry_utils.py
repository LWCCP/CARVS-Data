"""
Retry Utilities with Exponential Backoff for YouNiverse Dataset Enrichment

This module provides retry decorators and utilities for handling transient failures
in the data collection pipeline with intelligent backoff strategies.

Author: feature-developer
"""

import time
import random
import functools
import logging
from typing import Callable, List, Type, Union, Any, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass
class RetryStats:
    """Statistics for retry operations"""
    total_attempts: int = 0
    total_successes: int = 0
    total_failures: int = 0
    total_retry_time: float = 0.0
    last_attempt_time: Optional[datetime] = None
    
    def record_attempt(self, success: bool, retry_time: float = 0.0):
        """Record an attempt"""
        self.total_attempts += 1
        if success:
            self.total_successes += 1
        else:
            self.total_failures += 1
        self.total_retry_time += retry_time
        self.last_attempt_time = datetime.now()
    
    def get_success_rate(self) -> float:
        """Get success rate"""
        if self.total_attempts == 0:
            return 0.0
        return self.total_successes / self.total_attempts
    
    def get_average_retry_time(self) -> float:
        """Get average retry time"""
        if self.total_failures == 0:
            return 0.0
        return self.total_retry_time / self.total_failures


class RetryError(Exception):
    """Exception raised when all retry attempts are exhausted"""
    
    def __init__(self, message: str, attempts: int, last_exception: Exception = None):
        super().__init__(message)
        self.attempts = attempts
        self.last_exception = last_exception


def exponential_backoff(initial_delay: float = 1.0, max_delay: float = 60.0, 
                       multiplier: float = 2.0, jitter: bool = True) -> Callable[[int], float]:
    """
    Create exponential backoff delay calculator
    
    Args:
        initial_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds
        multiplier: Multiplier for each retry
        jitter: Whether to add random jitter
    
    Returns:
        Function that calculates delay for given attempt number
    """
    def calculate_delay(attempt: int) -> float:
        delay = min(initial_delay * (multiplier ** attempt), max_delay)
        
        if jitter:
            # Add jitter: 50-100% of calculated delay
            delay *= (0.5 + random.random() * 0.5)
        
        return delay
    
    return calculate_delay


def linear_backoff(initial_delay: float = 1.0, increment: float = 1.0, 
                  max_delay: float = 60.0, jitter: bool = True) -> Callable[[int], float]:
    """
    Create linear backoff delay calculator
    
    Args:
        initial_delay: Initial delay in seconds
        increment: Delay increment per attempt
        max_delay: Maximum delay in seconds
        jitter: Whether to add random jitter
    
    Returns:
        Function that calculates delay for given attempt number
    """
    def calculate_delay(attempt: int) -> float:
        delay = min(initial_delay + (increment * attempt), max_delay)
        
        if jitter:
            delay *= (0.8 + random.random() * 0.4)  # 80-120% of calculated delay
        
        return delay
    
    return calculate_delay


def fixed_backoff(delay: float = 1.0, jitter: bool = False) -> Callable[[int], float]:
    """
    Create fixed backoff delay calculator
    
    Args:
        delay: Fixed delay in seconds
        jitter: Whether to add random jitter
    
    Returns:
        Function that returns fixed delay
    """
    def calculate_delay(attempt: int) -> float:
        if jitter:
            return delay * (0.8 + random.random() * 0.4)  # 80-120% of fixed delay
        return delay
    
    return calculate_delay


def should_retry_on_exception(exception: Exception, 
                            retry_exceptions: List[Union[Type[Exception], str]] = None) -> bool:
    """
    Determine if an exception should trigger a retry
    
    Args:
        exception: The exception that occurred
        retry_exceptions: List of exception types or names to retry on
    
    Returns:
        True if should retry, False otherwise
    """
    if retry_exceptions is None:
        # Default retry exceptions for network operations
        retry_exceptions = [
            'ConnectionError', 
            'Timeout', 
            'ReadTimeout', 
            'ConnectTimeout',
            'HTTPError',
            'ChunkedEncodingError'
        ]
    
    for retry_exc in retry_exceptions:
        if isinstance(retry_exc, str):
            if exception.__class__.__name__ == retry_exc:
                return True
        elif isinstance(exception, retry_exc):
            return True
    
    return False


def should_retry_on_status_code(status_code: int, 
                              retry_status_codes: List[int] = None) -> bool:
    """
    Determine if an HTTP status code should trigger a retry
    
    Args:
        status_code: HTTP status code
        retry_status_codes: List of status codes to retry on
    
    Returns:
        True if should retry, False otherwise
    """
    if retry_status_codes is None:
        # Default retry status codes
        retry_status_codes = [429, 500, 502, 503, 504, 520, 521, 522, 523, 524]
    
    return status_code in retry_status_codes


def retry_with_backoff(max_retries: int = 3,
                      backoff_calculator: Callable[[int], float] = None,
                      retry_exceptions: List[Union[Type[Exception], str]] = None,
                      retry_status_codes: List[int] = None,
                      before_retry: Callable[[int, Exception], None] = None,
                      after_retry: Callable[[int, bool, Exception], None] = None,
                      logger: logging.Logger = None):
    """
    Decorator for adding retry logic with backoff to functions
    
    Args:
        max_retries: Maximum number of retry attempts
        backoff_calculator: Function to calculate delay between retries
        retry_exceptions: List of exception types/names to retry on
        retry_status_codes: List of HTTP status codes to retry on
        before_retry: Callback called before each retry attempt
        after_retry: Callback called after each attempt (success or failure)
        logger: Logger for retry events
    
    Returns:
        Decorated function with retry logic
    """
    if backoff_calculator is None:
        backoff_calculator = exponential_backoff()
    
    if logger is None:
        logger = logging.getLogger(__name__)
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            stats = RetryStats()
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    result = func(*args, **kwargs)
                    
                    # Check if result indicates failure (for HTTP responses)
                    if hasattr(result, 'status_code'):
                        if should_retry_on_status_code(result.status_code, retry_status_codes):
                            raise Exception(f"HTTP {result.status_code}")
                    
                    # Success
                    stats.record_attempt(True)
                    if after_retry:
                        after_retry(attempt, True, None)
                    
                    if attempt > 0:
                        logger.info(f"Function {func.__name__} succeeded after {attempt} retries")
                    
                    return result
                
                except Exception as e:
                    last_exception = e
                    should_retry = attempt < max_retries and should_retry_on_exception(e, retry_exceptions)
                    
                    if should_retry:
                        delay = backoff_calculator(attempt)
                        stats.record_attempt(False, delay)
                        
                        logger.warning(f"Function {func.__name__} failed on attempt {attempt + 1}: {e}. "
                                     f"Retrying in {delay:.2f}s...")
                        
                        if before_retry:
                            before_retry(attempt, e)
                        
                        time.sleep(delay)
                        
                        if after_retry:
                            after_retry(attempt, False, e)
                    else:
                        stats.record_attempt(False)
                        if after_retry:
                            after_retry(attempt, False, e)
                        break
            
            # All retries exhausted
            logger.error(f"Function {func.__name__} failed after {max_retries} retries. "
                        f"Success rate: {stats.get_success_rate():.2%}")
            
            raise RetryError(
                f"Function {func.__name__} failed after {max_retries} retries",
                max_retries + 1,
                last_exception
            )
        
        return wrapper
    return decorator


class RetryManager:
    """Manages retry operations with statistics and configuration"""
    
    def __init__(self, max_retries: int = 3,
                 backoff_calculator: Callable[[int], float] = None,
                 retry_exceptions: List[Union[Type[Exception], str]] = None,
                 retry_status_codes: List[int] = None,
                 logger: logging.Logger = None):
        
        self.max_retries = max_retries
        self.backoff_calculator = backoff_calculator or exponential_backoff()
        self.retry_exceptions = retry_exceptions
        self.retry_status_codes = retry_status_codes
        self.logger = logger or logging.getLogger(__name__)
        self.stats = RetryStats()
    
    def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with retry logic"""
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                result = func(*args, **kwargs)
                
                # Check if result indicates failure (for HTTP responses)
                if hasattr(result, 'status_code'):
                    if should_retry_on_status_code(result.status_code, self.retry_status_codes):
                        raise Exception(f"HTTP {result.status_code}")
                
                # Success
                self.stats.record_attempt(True)
                
                if attempt > 0:
                    self.logger.info(f"Function succeeded after {attempt} retries")
                
                return result
            
            except Exception as e:
                last_exception = e
                should_retry = (attempt < self.max_retries and 
                              should_retry_on_exception(e, self.retry_exceptions))
                
                if should_retry:
                    delay = self.backoff_calculator(attempt)
                    self.stats.record_attempt(False, delay)
                    
                    self.logger.warning(f"Function failed on attempt {attempt + 1}: {e}. "
                                      f"Retrying in {delay:.2f}s...")
                    
                    time.sleep(delay)
                else:
                    self.stats.record_attempt(False)
                    break
        
        # All retries exhausted
        self.logger.error(f"Function failed after {self.max_retries} retries. "
                         f"Success rate: {self.stats.get_success_rate():.2%}")
        
        raise RetryError(
            f"Function failed after {self.max_retries} retries",
            self.max_retries + 1,
            last_exception
        )
    
    def get_stats(self) -> RetryStats:
        """Get retry statistics"""
        return self.stats
    
    def reset_stats(self) -> None:
        """Reset retry statistics"""
        self.stats = RetryStats()


# Common retry configurations
DEFAULT_WEB_RETRY = RetryManager(
    max_retries=3,
    backoff_calculator=exponential_backoff(initial_delay=1.0, max_delay=60.0),
    retry_exceptions=['ConnectionError', 'Timeout', 'HTTPError'],
    retry_status_codes=[429, 500, 502, 503, 504]
)

AGGRESSIVE_WEB_RETRY = RetryManager(
    max_retries=5,
    backoff_calculator=exponential_backoff(initial_delay=2.0, max_delay=300.0, multiplier=1.5),
    retry_exceptions=['ConnectionError', 'Timeout', 'HTTPError', 'ReadTimeout'],
    retry_status_codes=[429, 500, 502, 503, 504, 520, 521, 522, 523, 524]
)

CONSERVATIVE_RETRY = RetryManager(
    max_retries=2,
    backoff_calculator=linear_backoff(initial_delay=2.0, increment=2.0, max_delay=10.0),
    retry_exceptions=['ConnectionError', 'Timeout'],
    retry_status_codes=[429, 503]
)