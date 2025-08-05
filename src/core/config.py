"""
Configuration Management for YouNiverse Dataset Enrichment

This module handles loading and managing configuration settings from various sources
including JSON files, environment variables, and command-line arguments.

Author: feature-developer
"""

import os
import json
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime

try:
    from dotenv import load_dotenv
    load_dotenv()  # Load .env file
except ImportError:
    pass  # dotenv not available


@dataclass
class RateLimitConfig:
    """Rate limiting configuration"""
    requests_per_second: float = 1.0
    burst_allowance: int = 3
    sliding_window_seconds: int = 60


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration"""
    failure_threshold: int = 5
    recovery_timeout: int = 300
    half_open_max_calls: int = 3


@dataclass
class BackoffConfig:
    """Exponential backoff configuration"""
    initial_delay: float = 1.0
    max_delay: float = 60.0
    multiplier: float = 2.0
    jitter: bool = True


@dataclass
class RetryConfig:
    """Retry policy configuration"""
    max_retries: int = 3
    retry_on_status_codes: list = field(default_factory=lambda: [429, 500, 502, 503, 504])
    retry_on_exceptions: list = field(default_factory=lambda: ["ConnectionError", "Timeout"])


@dataclass
class CollectorConfig:
    """Base collector configuration"""
    base_delay: float = 1.0
    max_concurrent_requests: int = 3
    request_timeout: int = 30
    rate_limiting: RateLimitConfig = field(default_factory=RateLimitConfig)
    circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
    backoff_strategy: BackoffConfig = field(default_factory=BackoffConfig)
    retry_policy: RetryConfig = field(default_factory=RetryConfig)


@dataclass
class YouTubeAPIConfig(CollectorConfig):
    """YouTube API specific configuration"""
    quota_limit: int = 10000
    quota_period: str = "daily"
    batch_size: int = 50
    cost_per_request: int = 1
    api_key: Optional[str] = None


@dataclass
class WaybackConfig(CollectorConfig):
    """Wayback Machine specific configuration"""
    respectful_delay: float = 1.2
    adaptive_throttling: bool = True
    monitor_response_times: bool = True
    slow_response_threshold: float = 10.0
    throttle_factor: float = 1.5


@dataclass
class ViewStatsConfig(CollectorConfig):
    """ViewStats specific configuration"""
    user_agent_rotation: bool = True
    session_management: bool = True
    request_spacing_variance: float = 0.3


@dataclass
class GlobalConfig:
    """Global system configuration"""
    max_total_concurrent_requests: int = 8
    memory_pressure_threshold: float = 0.85
    disk_space_threshold: float = 0.9
    network_timeout_multiplier: float = 1.5
    health_check_interval: int = 60
    emergency_brake_enabled: bool = True
    error_rate_threshold: float = 0.3
    evaluation_window: int = 300


class ConfigManager:
    """Configuration management singleton"""
    
    _instance = None
    _config: Dict[str, Any] = {}
    _config_loaded: bool = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._config_loaded:
            self.load_config()
    
    def load_config(self, config_path: Optional[str] = None) -> None:
        """Load configuration from files and environment variables"""
        
        # Determine config file path
        if config_path is None:
            project_root = Path(__file__).parent.parent.parent
            config_path = project_root / "config" / "rate_limits.json"
        
        # Load from JSON file
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self._config = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Warning: Could not load config file {config_path}: {e}")
            self._config = self._get_default_config()
        
        # Override with environment variables
        self._load_env_overrides()
        
        self._config_loaded = True
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration if file loading fails"""
        return {
            "youtube_api": {
                "quota_limit": 10000,
                "batch_size": 50,
                "rate_limiting": {
                    "requests_per_second": 100,
                    "burst_allowance": 50
                }
            },
            "wayback_machine": {
                "base_delay": 1.0,
                "respectful_delay": 1.2,
                "rate_limiting": {
                    "requests_per_second": 1,
                    "burst_allowance": 3
                }
            },
            "viewstats": {
                "base_delay": 0.5,
                "rate_limiting": {
                    "requests_per_second": 2,
                    "burst_allowance": 5
                }
            },
            "global_settings": {
                "max_total_concurrent_requests": 8,
                "memory_pressure_threshold": 0.85
            }
        }
    
    def _load_env_overrides(self) -> None:
        """Load configuration overrides from environment variables"""
        
        # Load API keys from environment
        youtube_api_key = os.getenv('YOUTUBE_API_KEY')
        if youtube_api_key:
            if 'youtube_api' not in self._config:
                self._config['youtube_api'] = {}
            self._config['youtube_api']['api_key'] = youtube_api_key
        
        # Load other environment overrides if needed
        viewstats_key = os.getenv('VIEWSTATS_API_KEY')
        if viewstats_key:
            if 'viewstats' not in self._config:
                self._config['viewstats'] = {}
            self._config['viewstats']['api_key'] = viewstats_key
    
    def get_youtube_config(self) -> YouTubeAPIConfig:
        """Get YouTube API configuration"""
        config_data = self._config.get('youtube_api', {})
        
        rate_limit_data = config_data.get('rate_limiting', {})
        rate_limiting = RateLimitConfig(
            requests_per_second=rate_limit_data.get('requests_per_second', 100),
            burst_allowance=rate_limit_data.get('burst_allowance', 50),
            sliding_window_seconds=rate_limit_data.get('sliding_window_seconds', 60)
        )
        
        circuit_breaker_data = config_data.get('circuit_breaker', {})
        circuit_breaker = CircuitBreakerConfig(
            failure_threshold=circuit_breaker_data.get('failure_threshold', 5),
            recovery_timeout=circuit_breaker_data.get('recovery_timeout', 300),
            half_open_max_calls=circuit_breaker_data.get('half_open_max_calls', 3)
        )
        
        backoff_data = config_data.get('backoff_strategy', {})
        backoff_strategy = BackoffConfig(
            initial_delay=backoff_data.get('initial_delay', 1.0),
            max_delay=backoff_data.get('max_delay', 60.0),
            multiplier=backoff_data.get('multiplier', 2.0),
            jitter=backoff_data.get('jitter', True)
        )
        
        retry_data = config_data.get('retry_policy', {})
        retry_policy = RetryConfig(
            max_retries=retry_data.get('max_retries', 3),
            retry_on_status_codes=retry_data.get('retry_on_status_codes', [429, 500, 502, 503, 504]),
            retry_on_exceptions=retry_data.get('retry_on_exceptions', ["ConnectionError", "Timeout"])
        )
        
        return YouTubeAPIConfig(
            base_delay=config_data.get('base_delay', 0.1),
            max_concurrent_requests=config_data.get('max_concurrent_requests', 4),
            request_timeout=config_data.get('request_timeout', 30),
            rate_limiting=rate_limiting,
            circuit_breaker=circuit_breaker,
            backoff_strategy=backoff_strategy,
            retry_policy=retry_policy,
            quota_limit=config_data.get('quota_limit', 10000),
            quota_period=config_data.get('quota_period', 'daily'),
            batch_size=config_data.get('batch_size', 50),
            cost_per_request=config_data.get('cost_per_request', 1),
            api_key=config_data.get('api_key')
        )
    
    def get_wayback_config(self) -> WaybackConfig:
        """Get Wayback Machine configuration"""
        config_data = self._config.get('wayback_machine', {})
        
        rate_limit_data = config_data.get('rate_limiting', {})
        rate_limiting = RateLimitConfig(
            requests_per_second=rate_limit_data.get('requests_per_second', 1),
            burst_allowance=rate_limit_data.get('burst_allowance', 3),
            sliding_window_seconds=rate_limit_data.get('sliding_window_seconds', 10)
        )
        
        circuit_breaker_data = config_data.get('circuit_breaker', {})
        circuit_breaker = CircuitBreakerConfig(
            failure_threshold=circuit_breaker_data.get('failure_threshold', 3),
            recovery_timeout=circuit_breaker_data.get('recovery_timeout', 600),
            half_open_max_calls=circuit_breaker_data.get('half_open_max_calls', 1)
        )
        
        backoff_data = config_data.get('backoff_strategy', {})
        backoff_strategy = BackoffConfig(
            initial_delay=backoff_data.get('initial_delay', 2.0),
            max_delay=backoff_data.get('max_delay', 300.0),
            multiplier=backoff_data.get('multiplier', 1.5),
            jitter=backoff_data.get('jitter', True)
        )
        
        retry_data = config_data.get('retry_policy', {})
        retry_policy = RetryConfig(
            max_retries=retry_data.get('max_retries', 5),
            retry_on_status_codes=retry_data.get('retry_on_status_codes', [429, 500, 502, 503, 504]),
            retry_on_exceptions=retry_data.get('retry_on_exceptions', ["ConnectionError", "Timeout"])
        )
        
        adaptive_data = config_data.get('adaptive_throttling', {})
        
        return WaybackConfig(
            base_delay=config_data.get('base_delay', 1.0),
            max_concurrent_requests=config_data.get('max_concurrent_requests', 2),
            request_timeout=config_data.get('request_timeout', 45),
            rate_limiting=rate_limiting,
            circuit_breaker=circuit_breaker,
            backoff_strategy=backoff_strategy,
            retry_policy=retry_policy,
            respectful_delay=config_data.get('respectful_delay', 1.2),
            adaptive_throttling=adaptive_data.get('enabled', True),
            monitor_response_times=adaptive_data.get('monitor_response_times', True),
            slow_response_threshold=adaptive_data.get('slow_response_threshold', 10.0),
            throttle_factor=adaptive_data.get('throttle_factor', 1.5)
        )
    
    def get_viewstats_config(self) -> ViewStatsConfig:
        """Get ViewStats configuration"""
        config_data = self._config.get('viewstats', {})
        
        rate_limit_data = config_data.get('rate_limiting', {})
        rate_limiting = RateLimitConfig(
            requests_per_second=rate_limit_data.get('requests_per_second', 2),
            burst_allowance=rate_limit_data.get('burst_allowance', 5),
            sliding_window_seconds=rate_limit_data.get('sliding_window_seconds', 30)
        )
        
        circuit_breaker_data = config_data.get('circuit_breaker', {})
        circuit_breaker = CircuitBreakerConfig(
            failure_threshold=circuit_breaker_data.get('failure_threshold', 4),
            recovery_timeout=circuit_breaker_data.get('recovery_timeout', 180),
            half_open_max_calls=circuit_breaker_data.get('half_open_max_calls', 2)
        )
        
        backoff_data = config_data.get('backoff_strategy', {})
        backoff_strategy = BackoffConfig(
            initial_delay=backoff_data.get('initial_delay', 1.0),
            max_delay=backoff_data.get('max_delay', 120.0),
            multiplier=backoff_data.get('multiplier', 2.0),
            jitter=backoff_data.get('jitter', True)
        )
        
        retry_data = config_data.get('retry_policy', {})
        retry_policy = RetryConfig(
            max_retries=retry_data.get('max_retries', 4),
            retry_on_status_codes=retry_data.get('retry_on_status_codes', [429, 500, 502, 503, 504]),
            retry_on_exceptions=retry_data.get('retry_on_exceptions', ["ConnectionError", "Timeout"])
        )
        
        anti_bot_data = config_data.get('anti_bot_measures', {})
        
        return ViewStatsConfig(
            base_delay=config_data.get('base_delay', 0.5),
            max_concurrent_requests=config_data.get('max_concurrent_requests', 3),
            request_timeout=config_data.get('request_timeout', 25),
            rate_limiting=rate_limiting,
            circuit_breaker=circuit_breaker,
            backoff_strategy=backoff_strategy,
            retry_policy=retry_policy,
            user_agent_rotation=anti_bot_data.get('user_agent_rotation', True),
            session_management=anti_bot_data.get('session_management', True),
            request_spacing_variance=anti_bot_data.get('request_spacing_variance', 0.3)
        )
    
    def get_global_config(self) -> GlobalConfig:
        """Get global system configuration"""
        config_data = self._config.get('global_settings', {})
        emergency_data = config_data.get('emergency_brake', {})
        
        return GlobalConfig(
            max_total_concurrent_requests=config_data.get('max_total_concurrent_requests', 8),
            memory_pressure_threshold=config_data.get('memory_pressure_threshold', 0.85),
            disk_space_threshold=config_data.get('disk_space_threshold', 0.9),
            network_timeout_multiplier=config_data.get('network_timeout_multiplier', 1.5),
            health_check_interval=config_data.get('health_check_interval', 60),
            emergency_brake_enabled=emergency_data.get('enabled', True),
            error_rate_threshold=emergency_data.get('error_rate_threshold', 0.3),
            evaluation_window=emergency_data.get('evaluation_window', 300)
        )
    
    def get_config_value(self, key_path: str, default: Any = None) -> Any:
        """Get configuration value by dot-separated key path"""
        keys = key_path.split('.')
        value = self._config
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        
        return value
    
    def update_config(self, key_path: str, value: Any) -> None:
        """Update configuration value by dot-separated key path"""
        keys = key_path.split('.')
        config = self._config
        
        for key in keys[:-1]:
            if key not in config:
                config[key] = {}
            config = config[key]
        
        config[keys[-1]] = value
    
    def reload_config(self, config_path: Optional[str] = None) -> None:
        """Reload configuration from file"""
        self._config_loaded = False
        self.load_config(config_path)
    
    def get_full_config(self) -> Dict[str, Any]:
        """Get full configuration dictionary"""
        return self._config.copy()


# Global config manager instance
config_manager = ConfigManager()