"""
Unit Tests for Configuration Management

Tests the configuration loading, validation, environment variable handling,
and singleton behavior of the ConfigManager class.

Author: test-strategist
"""

import pytest
import json
import os
import tempfile
from pathlib import Path
from unittest.mock import patch, mock_open

from src.core.config import (
    ConfigManager, 
    YouTubeAPIConfig, 
    WaybackConfig, 
    ViewStatsConfig,
    GlobalConfig,
    RateLimitConfig,
    CircuitBreakerConfig,
    BackoffConfig,
    RetryConfig
)


class TestConfigManager:
    """Test suite for ConfigManager class"""
    
    def test_singleton_behavior(self):
        """Test that ConfigManager is a singleton"""
        manager1 = ConfigManager()
        manager2 = ConfigManager()
        
        assert manager1 is manager2
        assert id(manager1) == id(manager2)
    
    def test_default_config_loading(self, mock_config_manager):
        """Test loading default configuration when file doesn't exist"""
        with patch('builtins.open', side_effect=FileNotFoundError()):
            manager = ConfigManager()
            manager._config_loaded = False
            manager.load_config()
            
            # Should load default config
            config = manager.get_full_config()
            assert 'youtube_api' in config
            assert 'wayback_machine' in config
            assert 'viewstats' in config
    
    def test_json_config_loading(self, tmp_path):
        """Test loading configuration from JSON file"""
        config_data = {
            "youtube_api": {
                "quota_limit": 15000,
                "batch_size": 25,
                "rate_limiting": {
                    "requests_per_second": 50,
                    "burst_allowance": 10
                }
            }
        }
        
        config_file = tmp_path / "test_config.json"
        with open(config_file, 'w') as f:
            json.dump(config_data, f)
        
        manager = ConfigManager()
        manager._config_loaded = False
        manager.load_config(str(config_file))
        
        youtube_config = manager.get_youtube_config()
        assert youtube_config.quota_limit == 15000
        assert youtube_config.batch_size == 25
        assert youtube_config.rate_limiting.requests_per_second == 50
    
    def test_environment_variable_overrides(self, tmp_path):
        """Test that environment variables override config file values"""
        config_data = {
            "youtube_api": {
                "quota_limit": 10000
            }
        }
        
        config_file = tmp_path / "test_config.json"
        with open(config_file, 'w') as f:
            json.dump(config_data, f)
        
        with patch.dict(os.environ, {
            'YOUTUBE_API_KEY': 'test_api_key_from_env',
            'VIEWSTATS_API_KEY': 'test_viewstats_key_from_env'
        }):
            manager = ConfigManager()
            manager._config_loaded = False
            manager.load_config(str(config_file))
            
            youtube_config = manager.get_youtube_config()
            assert youtube_config.api_key == 'test_api_key_from_env'
            
            # Check that viewstats key was also loaded
            config = manager.get_full_config()
            assert config['viewstats']['api_key'] == 'test_viewstats_key_from_env'
    
    def test_invalid_json_handling(self, tmp_path):
        """Test handling of invalid JSON configuration files"""
        config_file = tmp_path / "invalid_config.json"
        with open(config_file, 'w') as f:
            f.write("{ invalid json content }")
        
        manager = ConfigManager()
        manager._config_loaded = False
        manager.load_config(str(config_file))
        
        # Should fall back to default config
        config = manager.get_full_config()
        assert 'youtube_api' in config
        assert config['youtube_api']['quota_limit'] == 10000  # Default value
    
    def test_get_config_value_with_dot_notation(self, mock_config_manager):
        """Test getting config values using dot notation"""
        manager = ConfigManager()
        manager._config = {
            'youtube_api': {
                'rate_limiting': {
                    'requests_per_second': 100
                }
            }
        }
        
        value = manager.get_config_value('youtube_api.rate_limiting.requests_per_second')
        assert value == 100
        
        # Test default value
        value = manager.get_config_value('nonexistent.key', 'default')
        assert value == 'default'
    
    def test_update_config_value(self, mock_config_manager):
        """Test updating config values using dot notation"""
        manager = ConfigManager()
        manager._config = {}
        
        manager.update_config('test.nested.value', 42)
        
        assert manager._config['test']['nested']['value'] == 42
    
    def test_config_reload(self, tmp_path):
        """Test reloading configuration"""
        config_data = {
            "youtube_api": {
                "quota_limit": 5000
            }
        }
        
        config_file = tmp_path / "reload_test.json"
        with open(config_file, 'w') as f:
            json.dump(config_data, f)
        
        manager = ConfigManager()
        manager._config_loaded = False
        manager.load_config(str(config_file))
        
        youtube_config = manager.get_youtube_config()
        assert youtube_config.quota_limit == 5000
        
        # Update file
        config_data['youtube_api']['quota_limit'] = 8000
        with open(config_file, 'w') as f:
            json.dump(config_data, f)
        
        manager.reload_config(str(config_file))
        youtube_config = manager.get_youtube_config()
        assert youtube_config.quota_limit == 8000


class TestYouTubeAPIConfig:
    """Test suite for YouTube API configuration"""
    
    def test_youtube_config_creation(self, mock_config_manager):
        """Test creation of YouTube API configuration"""
        manager = ConfigManager()
        manager._config = {
            'youtube_api': {
                'quota_limit': 15000,
                'batch_size': 25,
                'api_key': 'test_key',
                'rate_limiting': {
                    'requests_per_second': 50,
                    'burst_allowance': 10,
                    'sliding_window_seconds': 30
                },
                'circuit_breaker': {
                    'failure_threshold': 3,
                    'recovery_timeout': 180,
                    'half_open_max_calls': 2
                },
                'backoff_strategy': {
                    'initial_delay': 2.0,
                    'max_delay': 120.0,
                    'multiplier': 1.5,
                    'jitter': False
                },
                'retry_policy': {
                    'max_retries': 5,
                    'retry_on_status_codes': [429, 500],
                    'retry_on_exceptions': ['ConnectionError']
                }
            }
        }
        
        config = manager.get_youtube_config()
        
        assert config.quota_limit == 15000
        assert config.batch_size == 25
        assert config.api_key == 'test_key'
        assert config.rate_limiting.requests_per_second == 50
        assert config.rate_limiting.burst_allowance == 10
        assert config.circuit_breaker.failure_threshold == 3
        assert config.backoff_strategy.initial_delay == 2.0
        assert config.retry_policy.max_retries == 5
    
    def test_youtube_config_defaults(self, mock_config_manager):
        """Test YouTube configuration with missing values uses defaults"""
        manager = ConfigManager()
        manager._config = {
            'youtube_api': {}  # Empty config should use defaults
        }
        
        config = manager.get_youtube_config()
        
        assert config.quota_limit == 10000  # Default
        assert config.batch_size == 50  # Default
        assert config.rate_limiting.requests_per_second == 100  # Default
        assert config.circuit_breaker.failure_threshold == 5  # Default


class TestWaybackConfig:
    """Test suite for Wayback Machine configuration"""
    
    def test_wayback_config_creation(self, mock_config_manager):
        """Test creation of Wayback Machine configuration"""
        manager = ConfigManager()
        manager._config = {
            'wayback_machine': {
                'base_delay': 2.0,
                'respectful_delay': 1.5,
                'rate_limiting': {
                    'requests_per_second': 0.5,
                    'burst_allowance': 2
                },
                'adaptive_throttling': {
                    'enabled': True,
                    'monitor_response_times': True,
                    'slow_response_threshold': 15.0,
                    'throttle_factor': 2.0
                }
            }
        }
        
        config = manager.get_wayback_config()
        
        assert config.base_delay == 2.0
        assert config.respectful_delay == 1.5
        assert config.rate_limiting.requests_per_second == 0.5
        assert config.adaptive_throttling == True
        assert config.slow_response_threshold == 15.0
        assert config.throttle_factor == 2.0
    
    def test_wayback_config_defaults(self, mock_config_manager):
        """Test Wayback configuration defaults"""
        manager = ConfigManager()
        manager._config = {'wayback_machine': {}}
        
        config = manager.get_wayback_config()
        
        assert config.base_delay == 1.0
        assert config.respectful_delay == 1.2
        assert config.adaptive_throttling == True


class TestViewStatsConfig:
    """Test suite for ViewStats configuration"""
    
    def test_viewstats_config_creation(self, mock_config_manager):
        """Test creation of ViewStats configuration"""
        manager = ConfigManager()
        manager._config = {
            'viewstats': {
                'base_delay': 0.8,
                'anti_bot_measures': {
                    'user_agent_rotation': True,
                    'session_management': True,
                    'request_spacing_variance': 0.5
                }
            }
        }
        
        config = manager.get_viewstats_config()
        
        assert config.base_delay == 0.8
        assert config.user_agent_rotation == True
        assert config.session_management == True
        assert config.request_spacing_variance == 0.5


class TestGlobalConfig:
    """Test suite for Global configuration"""
    
    def test_global_config_creation(self, mock_config_manager):
        """Test creation of global configuration"""
        manager = ConfigManager()
        manager._config = {
            'global_settings': {
                'max_total_concurrent_requests': 12,
                'memory_pressure_threshold': 0.9,
                'disk_space_threshold': 0.95,
                'emergency_brake': {
                    'enabled': True,
                    'error_rate_threshold': 0.5,
                    'evaluation_window': 600
                }
            }
        }
        
        config = manager.get_global_config()
        
        assert config.max_total_concurrent_requests == 12
        assert config.memory_pressure_threshold == 0.9
        assert config.disk_space_threshold == 0.95
        assert config.emergency_brake_enabled == True
        assert config.error_rate_threshold == 0.5
        assert config.evaluation_window == 600


class TestConfigDataClasses:
    """Test suite for configuration data classes"""
    
    def test_rate_limit_config_defaults(self):
        """Test RateLimitConfig default values"""
        config = RateLimitConfig()
        
        assert config.requests_per_second == 1.0
        assert config.burst_allowance == 3
        assert config.sliding_window_seconds == 60
    
    def test_circuit_breaker_config_defaults(self):
        """Test CircuitBreakerConfig default values"""
        config = CircuitBreakerConfig()
        
        assert config.failure_threshold == 5
        assert config.recovery_timeout == 300
        assert config.half_open_max_calls == 3
    
    def test_backoff_config_defaults(self):
        """Test BackoffConfig default values"""
        config = BackoffConfig()
        
        assert config.initial_delay == 1.0
        assert config.max_delay == 60.0
        assert config.multiplier == 2.0
        assert config.jitter == True
    
    def test_retry_config_defaults(self):
        """Test RetryConfig default values"""
        config = RetryConfig()
        
        assert config.max_retries == 3
        assert 429 in config.retry_on_status_codes
        assert 500 in config.retry_on_status_codes
        assert "ConnectionError" in config.retry_on_exceptions
    
    def test_collector_config_composition(self):
        """Test that CollectorConfig properly composes other configs"""
        from src.core.config import CollectorConfig
        
        config = CollectorConfig()
        
        assert isinstance(config.rate_limiting, RateLimitConfig)
        assert isinstance(config.circuit_breaker, CircuitBreakerConfig)
        assert isinstance(config.backoff_strategy, BackoffConfig)
        assert isinstance(config.retry_policy, RetryConfig)
        
        assert config.base_delay == 1.0
        assert config.max_concurrent_requests == 3
        assert config.request_timeout == 30


@pytest.mark.integration
class TestConfigIntegration:
    """Integration tests for configuration system"""
    
    def test_full_config_workflow(self, tmp_path):
        """Test complete configuration workflow"""
        # Create config file
        config_data = {
            "youtube_api": {
                "quota_limit": 20000,
                "batch_size": 30,
                "rate_limiting": {
                    "requests_per_second": 75
                }
            },
            "global_settings": {
                "max_total_concurrent_requests": 6
            }
        }
        
        config_file = tmp_path / "integration_config.json"
        with open(config_file, 'w') as f:
            json.dump(config_data, f)
        
        # Set environment variables
        with patch.dict(os.environ, {'YOUTUBE_API_KEY': 'integration_test_key'}):
            # Load configuration
            manager = ConfigManager()
            manager._config_loaded = False
            manager.load_config(str(config_file))
            
            # Test YouTube config
            youtube_config = manager.get_youtube_config()
            assert youtube_config.quota_limit == 20000
            assert youtube_config.batch_size == 30
            assert youtube_config.api_key == 'integration_test_key'
            assert youtube_config.rate_limiting.requests_per_second == 75
            
            # Test global config
            global_config = manager.get_global_config()
            assert global_config.max_total_concurrent_requests == 6
            
            # Test config value access
            value = manager.get_config_value('youtube_api.quota_limit')
            assert value == 20000
            
            # Test config update
            manager.update_config('youtube_api.quota_limit', 25000)
            value = manager.get_config_value('youtube_api.quota_limit')
            assert value == 25000
    
    def test_missing_api_key_handling(self, tmp_path):
        """Test handling of missing API keys"""
        config_file = tmp_path / "no_api_key_config.json"
        with open(config_file, 'w') as f:
            json.dump({"youtube_api": {"quota_limit": 5000}}, f)
        
        # No environment variables set
        with patch.dict(os.environ, {}, clear=True):
            manager = ConfigManager()
            manager._config_loaded = False
            manager.load_config(str(config_file))
            
            youtube_config = manager.get_youtube_config()
            assert youtube_config.api_key is None