#!/usr/bin/env python3
"""
Test Runner for YouNiverse Dataset Enrichment

Provides convenient commands for running different categories of tests
with appropriate configurations and reporting.

Usage:
    python run_tests.py --help
    python run_tests.py unit
    python run_tests.py integration  
    python run_tests.py performance
    python run_tests.py all

Author: test-strategist
"""

import argparse
import subprocess
import sys
from pathlib import Path


def run_command(cmd, description):
    """Run a command and handle output"""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {' '.join(cmd)}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=False)
        print(f"\n✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n❌ {description} failed with exit code {e.returncode}")
        return False


def run_unit_tests():
    """Run unit tests - fast, isolated tests"""
    cmd = [
        "pytest", 
        "-m", "unit",
        "--tb=short",
        "--durations=5",
        "tests/unit/"
    ]
    return run_command(cmd, "Unit Tests")


def run_integration_tests():
    """Run integration tests - end-to-end workflows"""
    cmd = [
        "pytest",
        "-m", "integration", 
        "--tb=short",
        "--durations=10",
        "tests/integration/"
    ]
    return run_command(cmd, "Integration Tests")


def run_performance_tests():
    """Run performance tests - resource intensive"""
    cmd = [
        "pytest",
        "-m", "performance",
        "--tb=short", 
        "--durations=0",  # Show all durations for performance analysis
        "-v"
    ]
    return run_command(cmd, "Performance Tests")


def run_api_integration_tests():
    """Run real API integration tests - requires API keys"""
    cmd = [
        "pytest",
        "-m", "api_integration",
        "--real-apis",
        "--tb=short",
        "--durations=10",
        "-v"
    ]
    return run_command(cmd, "API Integration Tests (Real APIs)")


def run_smoke_tests():
    """Run smoke tests - quick verification that core functionality works"""
    cmd = [
        "pytest",
        "-m", "unit and not slow", 
        "--maxfail=5",  # Stop after 5 failures
        "--tb=line",
        "-q"  # Quiet output
    ]
    return run_command(cmd, "Smoke Tests")


def run_all_tests():
    """Run all tests with comprehensive reporting"""
    cmd = [
        "pytest",
        "--tb=short",
        "--durations=20",
        "--cov=src",
        "--cov-report=html:reports/coverage",
        "--cov-report=term-missing",
        "--junitxml=reports/junit.xml"
    ]
    return run_command(cmd, "All Tests with Coverage")


def run_phase_specific_tests(phase):
    """Run tests for a specific phase"""
    cmd = [
        "pytest",
        "-m", f"phase{phase}",
        "--tb=short",
        "--durations=10",
        "-v"
    ]
    return run_command(cmd, f"Phase {phase} Tests")


def run_scraping_tests():
    """Run scraping-related tests"""
    cmd = [
        "pytest", 
        "-m", "scraping",
        "--tb=short",
        "--durations=10",
        "-v"
    ]
    return run_command(cmd, "Scraping Tests")


def run_ci_tests():
    """Run tests suitable for CI/CD pipeline"""
    cmd = [
        "pytest",
        "-m", "not (performance or api_integration or slow)",
        "--tb=short",
        "--maxfail=10",
        "--cov=src",
        "--cov-fail-under=85",
        "--junitxml=reports/junit.xml"
    ]
    return run_command(cmd, "CI/CD Pipeline Tests")


def setup_test_environment():
    """Setup test environment and directories"""
    print("Setting up test environment...")
    
    # Create reports directory
    reports_dir = Path("reports")
    reports_dir.mkdir(exist_ok=True)
    
    # Create test data directories if they don't exist
    test_data_dirs = [
        "tests/fixtures/mock_responses",
        "tests/fixtures/sample_data",
        "data/intermediate/checkpoints"
    ]
    
    for dir_path in test_data_dirs:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
    
    print("✅ Test environment setup complete")


def main():
    parser = argparse.ArgumentParser(
        description="Test Runner for YouNiverse Dataset Enrichment",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python run_tests.py unit              # Run unit tests only
    python run_tests.py integration       # Run integration tests
    python run_tests.py performance       # Run performance tests
    python run_tests.py smoke             # Run quick smoke tests
    python run_tests.py phase1            # Run Phase 1 specific tests
    python run_tests.py scraping          # Run scraping tests
    python run_tests.py api --real-apis   # Run real API tests
    python run_tests.py ci                # Run CI/CD suitable tests
    python run_tests.py all               # Run all tests with coverage

Test Categories:
    - unit: Fast, isolated tests for individual components
    - integration: End-to-end workflow tests
    - performance: Resource-intensive scalability tests
    - api: Real API integration tests (requires API keys)
    - scraping: Tests involving actual scraping operations
    - smoke: Quick verification tests
    - ci: Tests suitable for continuous integration
        """
    )
    
    parser.add_argument(
        "test_type",
        choices=[
            "unit", "integration", "performance", "api", "smoke", 
            "all", "scraping", "ci", "phase1", "phase2", "phase3", 
            "phase4", "phase5", "setup"
        ],
        help="Type of tests to run"
    )
    
    parser.add_argument(
        "--real-apis",
        action="store_true",
        help="Include tests that use real APIs (requires API keys)"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output"
    )
    
    parser.add_argument(
        "--fast",
        action="store_true", 
        help="Run only fast tests (exclude slow and performance tests)"
    )
    
    args = parser.parse_args()
    
    # Setup environment first
    setup_test_environment()
    
    # Add global pytest arguments
    if args.verbose:
        # Already handled in individual commands
        pass
    
    success = True
    
    # Route to appropriate test runner
    if args.test_type == "unit":
        success = run_unit_tests()
    elif args.test_type == "integration":
        success = run_integration_tests()
    elif args.test_type == "performance":
        if not args.fast:
            success = run_performance_tests()
        else:
            print("Skipping performance tests due to --fast flag")
    elif args.test_type == "api":
        success = run_api_integration_tests()
    elif args.test_type == "smoke":
        success = run_smoke_tests()
    elif args.test_type == "scraping":
        success = run_scraping_tests()
    elif args.test_type == "ci":
        success = run_ci_tests()
    elif args.test_type == "all":
        if not args.fast:
            success = run_all_tests()
        else:
            # Run all except performance and slow tests
            cmd = [
                "pytest",
                "-m", "not (performance or slow)",
                "--tb=short",
                "--durations=10",
                "--cov=src",
                "--cov-report=html:reports/coverage", 
                "--cov-report=term-missing"
            ]
            success = run_command(cmd, "All Tests (Fast Mode)")
    elif args.test_type.startswith("phase"):
        phase_num = args.test_type[-1]
        success = run_phase_specific_tests(phase_num)
    elif args.test_type == "setup":
        print("Test environment setup completed")
        return 0
    
    # Summary
    print(f"\n{'='*60}")
    if success:
        print(f"🎉 {args.test_type.title()} tests completed successfully!")
        print("\nNext steps:")
        print("- Check reports/coverage/index.html for coverage report")
        print("- Check reports/junit.xml for detailed test results")
        print("- Run 'python run_tests.py all' for comprehensive testing")
    else:
        print(f"💥 {args.test_type.title()} tests failed!")
        print("\nTroubleshooting:")
        print("- Check test output above for specific failures")
        print("- Run with -v flag for more detailed output")
        print("- Check that all dependencies are installed")
    print(f"{'='*60}")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())