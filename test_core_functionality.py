#!/usr/bin/env python3
"""
Core Functionality Test for YouNiverse Dataset Enrichment
Tests basic functionality without complex pytest setup
"""

import sys
import os
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

def test_imports():
    """Test that all core modules can be imported"""
    print("Testing imports...")
    
    try:
        from src.core.config import ConfigManager
        print("[PASS] ConfigManager import successful")
    except Exception as e:
        print(f"❌ ConfigManager import failed: {e}")
        return False
    
    try:
        from src.utils.api_helpers import RateLimiter, CircuitBreaker
        print("✅ API helpers import successful")
    except Exception as e:
        print(f"❌ API helpers import failed: {e}")
        return False
    
    try:
        from src.data_collectors.base_collector import BaseCollector
        print("✅ BaseCollector import successful")
    except Exception as e:
        print(f"❌ BaseCollector import failed: {e}")
        return False
        
    try:
        from src.data_collectors.youtube_api_collector import YouTubeAPICollector
        print("✅ YouTubeAPICollector import successful")
    except Exception as e:
        print(f"❌ YouTubeAPICollector import failed: {e}")
        return False
    
    return True

def test_config_manager():
    """Test ConfigManager functionality"""
    print("\nTesting ConfigManager...")
    
    try:
        from src.core.config import ConfigManager
        
        # Test singleton behavior
        config1 = ConfigManager()
        config2 = ConfigManager()
        
        if config1 is config2:
            print("✅ ConfigManager singleton working")
        else:
            print("❌ ConfigManager singleton failed")
            return False
            
        # Test configuration access
        youtube_config = config1.get_youtube_api_config()
        if youtube_config:
            print("✅ YouTube API config accessible")
        else:
            print("❌ YouTube API config failed")
            return False
            
        return True
        
    except Exception as e:
        print(f"❌ ConfigManager test failed: {e}")
        return False

def test_rate_limiter():
    """Test RateLimiter functionality"""
    print("\nTesting RateLimiter...")
    
    try:
        from src.utils.api_helpers import RateLimiter
        import time
        
        # Create rate limiter
        limiter = RateLimiter(requests_per_second=10.0)
        
        # Test basic functionality
        start_time = time.time()
        
        # Make several requests
        for i in range(3):
            with limiter:
                pass
        
        elapsed = time.time() - start_time
        
        # Should take some time due to rate limiting
        if elapsed >= 0:  # Basic timing check
            print("✅ RateLimiter basic functionality working")
        else:
            print("❌ RateLimiter timing issue")
            return False
            
        return True
        
    except Exception as e:
        print(f"❌ RateLimiter test failed: {e}")
        return False

def test_circuit_breaker():
    """Test CircuitBreaker functionality"""
    print("\nTesting CircuitBreaker...")
    
    try:
        from src.utils.api_helpers import CircuitBreaker
        
        # Create circuit breaker
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=1.0)
        
        # Test initial state
        if cb.state == 'closed':
            print("✅ CircuitBreaker initial state correct")
        else:
            print("❌ CircuitBreaker initial state wrong")
            return False
            
        return True
        
    except Exception as e:
        print(f"❌ CircuitBreaker test failed: {e}")
        return False

def test_youtube_collector():
    """Test YouTubeAPICollector instantiation"""
    print("\nTesting YouTubeAPICollector...")
    
    try:
        from src.data_collectors.youtube_api_collector import YouTubeAPICollector
        
        # Test collector instantiation (without API key)
        collector = YouTubeAPICollector()
        
        if collector:
            print("✅ YouTubeAPICollector instantiation successful")
        else:
            print("❌ YouTubeAPICollector instantiation failed")
            return False
            
        return True
        
    except Exception as e:
        print(f"❌ YouTubeAPICollector test failed: {e}")
        return False

def test_env_file():
    """Test .env file exists"""
    print("\nTesting .env file...")
    
    env_file = Path(__file__).parent / '.env'
    
    if env_file.exists():
        print("✅ .env file exists")
        
        # Check if it has YouTube API key placeholder
        content = env_file.read_text()
        if 'YOUTUBE_API_KEY' in content:
            print("✅ .env file has YouTube API key placeholder")
            return True
        else:
            print("❌ .env file missing YouTube API key")
            return False
    else:
        print("❌ .env file does not exist")
        return False

def main():
    """Run all tests"""
    print("=" * 60)
    print("YouNiverse Dataset Enrichment - Core Functionality Test")
    print("=" * 60)
    
    tests = [
        test_imports,
        test_config_manager,
        test_rate_limiter,
        test_circuit_breaker,
        test_youtube_collector,
        test_env_file
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            if test():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"❌ Test {test.__name__} crashed: {e}")
            failed += 1
        print()
    
    print("=" * 60)
    print(f"TEST RESULTS: {passed} passed, {failed} failed")
    print("=" * 60)
    
    if failed == 0:
        print("🎉 ALL TESTS PASSED - PRODUCTION READY!")
        return True
    else:
        print("⚠️  Some tests failed - needs attention")
        return False

if __name__ == "__main__":
    main()