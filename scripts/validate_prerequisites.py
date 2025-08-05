#!/usr/bin/env python3
"""
Prerequisites Validation for YouNiverse Dataset Enrichment Pipeline

This script validates all prerequisites, configurations, and dependencies
required for running the YouNiverse data collection pipeline.

Usage:
    python scripts/validate_prerequisites.py
    python scripts/validate_prerequisites.py --detailed
    python scripts/validate_prerequisites.py --fix-issues
"""

import os
import sys
import json
import yaml
import argparse
import logging
import subprocess
import importlib
import platform
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
import urllib.request
import socket
import time
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PrerequisitesValidator:
    """Comprehensive prerequisites validation for YouNiverse pipeline."""
    
    def __init__(self, fix_issues: bool = False, detailed: bool = False):
        self.fix_issues = fix_issues
        self.detailed = detailed
        self.root_dir = Path(__file__).parent.parent
        self.config_dir = self.root_dir / "config"
        self.logs_dir = self.root_dir / "logs"
        self.data_dir = self.root_dir / "data"
        self.requirements_dir = self.root_dir / "requirements"
        
        self.validation_results = {
            "passed": [],
            "warnings": [],
            "errors": [],
            "fixed": []
        }
    
    def run_validation(self) -> bool:
        """Run complete prerequisites validation."""
        logger.info("Starting comprehensive prerequisites validation...")
        
        validation_steps = [
            ("System Requirements", self.validate_system_requirements),
            ("Python Environment", self.validate_python_environment),
            ("Dependencies", self.validate_dependencies),
            ("Configuration Files", self.validate_configuration_files),
            ("Directory Structure", self.validate_directory_structure),
            ("Environment Variables", self.validate_environment_variables),
            ("API Connectivity", self.validate_api_connectivity),
            ("File Permissions", self.validate_file_permissions),
            ("Resource Availability", self.validate_resource_availability),
            ("External Tools", self.validate_external_tools)
        ]
        
        for step_name, validation_func in validation_steps:
            logger.info(f"Validating: {step_name}")
            try:
                validation_func()
            except Exception as e:
                self.validation_results["errors"].append(f"{step_name}: {e}")
                logger.error(f"Validation failed for {step_name}: {e}")
        
        self.print_validation_summary()
        return len(self.validation_results["errors"]) == 0
    
    def validate_system_requirements(self) -> None:
        """Validate system-level requirements."""
        # Python version
        python_version = sys.version_info
        if python_version >= (3, 9):
            self.validation_results["passed"].append(
                f"Python version: {python_version.major}.{python_version.minor}.{python_version.micro}"
            )
        else:
            self.validation_results["errors"].append(
                f"Python 3.9+ required, found {python_version.major}.{python_version.minor}"
            )
        
        # Operating system
        os_info = platform.system()
        self.validation_results["passed"].append(f"Operating System: {os_info}")
        
        # Architecture
        arch = platform.architecture()[0]
        self.validation_results["passed"].append(f"Architecture: {arch}")
        
        # Available memory
        try:
            import psutil
            memory = psutil.virtual_memory()
            memory_gb = memory.total / (1024**3)
            if memory_gb >= 4:
                self.validation_results["passed"].append(f"Available Memory: {memory_gb:.1f} GB")
            else:
                self.validation_results["warnings"].append(f"Low memory: {memory_gb:.1f} GB (4GB+ recommended)")
        except ImportError:
            self.validation_results["warnings"].append("Could not check memory (psutil not installed)")
        
        # Disk space
        try:
            if os.name == 'nt':  # Windows
                import shutil
                free_space = shutil.disk_usage(self.root_dir).free
            else:  # Unix-like
                statvfs = os.statvfs(self.root_dir)
                free_space = statvfs.f_frsize * statvfs.f_bavail
            
            free_space_gb = free_space / (1024**3)
            if free_space_gb >= 10:
                self.validation_results["passed"].append(f"Free Disk Space: {free_space_gb:.1f} GB")
            else:
                self.validation_results["warnings"].append(f"Low disk space: {free_space_gb:.1f} GB (10GB+ recommended)")
        except Exception:
            self.validation_results["warnings"].append("Could not check disk space")
    
    def validate_python_environment(self) -> None:
        """Validate Python environment and virtual environment."""
        # Check if in virtual environment
        if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
            self.validation_results["passed"].append("Running in virtual environment")
        else:
            self.validation_results["warnings"].append("Not running in virtual environment (recommended)")
        
        # Python executable path
        self.validation_results["passed"].append(f"Python executable: {sys.executable}")
        
        # pip availability
        try:
            import pip
            self.validation_results["passed"].append(f"pip version: {pip.__version__}")
        except ImportError:
            self.validation_results["errors"].append("pip not available")
    
    def validate_dependencies(self) -> None:
        """Validate required Python dependencies."""
        # Core dependencies to check
        core_dependencies = [
            ("pandas", "2.1.0"),
            ("numpy", "1.24.0"),
            ("requests", "2.31.0"),
            ("pyyaml", "6.0"),
            ("pydantic", "2.5.0"),
            ("aiohttp", "3.9.0"),
            ("tenacity", "8.2.3"),
            ("beautifulsoup4", "4.12.0"),
            ("python-dateutil", "2.8.2"),
            ("tqdm", "4.66.0")
        ]
        
        for package_name, min_version in core_dependencies:
            try:
                module = importlib.import_module(package_name.replace("-", "_"))
                if hasattr(module, '__version__'):
                    version = module.__version__
                    self.validation_results["passed"].append(f"{package_name}: {version}")
                else:
                    self.validation_results["passed"].append(f"{package_name}: installed")
            except ImportError:
                self.validation_results["errors"].append(f"Missing dependency: {package_name}")
                if self.fix_issues:
                    self.fix_missing_dependency(package_name)
    
    def validate_configuration_files(self) -> None:
        """Validate configuration files."""
        config_validations = [
            ("rate_limits.json", self.validate_rate_limits_config),
            ("logging_config.yaml", self.validate_logging_config),
            ("data_quality_rules.json", self.validate_data_quality_config)
        ]
        
        for config_file, validator in config_validations:
            config_path = self.config_dir / config_file
            if config_path.exists():
                try:
                    validator(config_path)
                    self.validation_results["passed"].append(f"Configuration valid: {config_file}")
                except Exception as e:
                    self.validation_results["errors"].append(f"Invalid configuration {config_file}: {e}")
            else:
                self.validation_results["errors"].append(f"Missing configuration file: {config_file}")
    
    def validate_rate_limits_config(self, config_path: Path) -> None:
        """Validate rate limits configuration."""
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        required_sections = ["youtube_api", "wayback_machine", "viewstats"]
        for section in required_sections:
            if section not in config:
                raise ValueError(f"Missing section: {section}")
            
            section_config = config[section]
            if not isinstance(section_config, dict) or not section_config:
                raise ValueError(f"Empty or invalid section: {section}")
    
    def validate_logging_config(self, config_path: Path) -> None:
        """Validate logging configuration."""
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        required_keys = ["version", "formatters", "handlers", "loggers"]
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing key: {key}")
    
    def validate_data_quality_config(self, config_path: Path) -> None:
        """Validate data quality configuration."""
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        required_sections = ["validation_rules", "quality_thresholds", "error_handling"]
        for section in required_sections:
            if section not in config:
                raise ValueError(f"Missing section: {section}")
    
    def validate_directory_structure(self) -> None:
        """Validate directory structure."""
        required_directories = [
            self.logs_dir,
            self.data_dir / "input",
            self.data_dir / "intermediate",
            self.data_dir / "output",
            self.data_dir / "quality",
            self.config_dir,
            self.requirements_dir
        ]
        
        for directory in required_directories:
            if directory.exists() and directory.is_dir():
                self.validation_results["passed"].append(f"Directory exists: {directory.name}")
            else:
                self.validation_results["errors"].append(f"Missing directory: {directory}")
                if self.fix_issues:
                    self.fix_missing_directory(directory)
    
    def validate_environment_variables(self) -> None:
        """Validate environment variables."""
        env_file = self.root_dir / ".env"
        if env_file.exists():
            self.validation_results["passed"].append("Environment file exists")
            
            # Check for required environment variables
            required_env_vars = [
                "YOUTUBE_API_KEY",
                "ENVIRONMENT",
                "LOG_LEVEL"
            ]
            
            env_content = env_file.read_text()
            for var in required_env_vars:
                if var in env_content:
                    if f"{var}=your_" not in env_content:  # Not using placeholder
                        self.validation_results["passed"].append(f"Environment variable configured: {var}")
                    else:
                        self.validation_results["warnings"].append(f"Environment variable needs configuration: {var}")
                else:
                    self.validation_results["warnings"].append(f"Missing environment variable: {var}")
        else:
            self.validation_results["warnings"].append("No environment file found (.env)")
    
    def validate_api_connectivity(self) -> None:
        """Validate API connectivity."""
        api_endpoints = [
            ("YouTube API", "https://www.googleapis.com/youtube/v3/"),
            ("Internet Archive", "https://web.archive.org/"),
            ("ViewStats", "https://viewstats.org/")
        ]
        
        for api_name, endpoint in api_endpoints:
            try:
                response = urllib.request.urlopen(endpoint, timeout=10)
                if response.getcode() < 400:
                    self.validation_results["passed"].append(f"API accessible: {api_name}")
                else:
                    self.validation_results["warnings"].append(f"API returned error: {api_name} ({response.getcode()})")
            except Exception as e:
                self.validation_results["warnings"].append(f"API not accessible: {api_name} ({e})")
    
    def validate_file_permissions(self) -> None:
        """Validate file permissions."""
        # Check write permissions for key directories
        writable_dirs = [self.logs_dir, self.data_dir, self.root_dir / "temp"]
        
        for directory in writable_dirs:
            if directory.exists():
                try:
                    test_file = directory / ".write_test"
                    test_file.touch()
                    test_file.unlink()
                    self.validation_results["passed"].append(f"Write permission: {directory.name}")
                except Exception as e:
                    self.validation_results["errors"].append(f"No write permission: {directory} ({e})")
            else:
                self.validation_results["warnings"].append(f"Directory does not exist: {directory}")
    
    def validate_resource_availability(self) -> None:
        """Validate system resource availability."""
        try:
            import psutil
            
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            if cpu_percent < 80:
                self.validation_results["passed"].append(f"CPU usage acceptable: {cpu_percent:.1f}%")
            else:
                self.validation_results["warnings"].append(f"High CPU usage: {cpu_percent:.1f}%")
            
            # Memory usage
            memory = psutil.virtual_memory()
            if memory.percent < 80:
                self.validation_results["passed"].append(f"Memory usage acceptable: {memory.percent:.1f}%")
            else:
                self.validation_results["warnings"].append(f"High memory usage: {memory.percent:.1f}%")
            
            # Network connectivity
            try:
                socket.create_connection(("8.8.8.8", 53), timeout=5).close()
                self.validation_results["passed"].append("Network connectivity: OK")
            except socket.error:
                self.validation_results["errors"].append("No network connectivity")
                
        except ImportError:
            self.validation_results["warnings"].append("Could not check system resources (psutil not installed)")
    
    def validate_external_tools(self) -> None:
        """Validate external tools availability."""
        external_tools = ["git", "curl"]
        
        for tool in external_tools:
            try:
                subprocess.run([tool, "--version"], 
                             capture_output=True, check=True, timeout=10)
                self.validation_results["passed"].append(f"External tool available: {tool}")
            except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
                self.validation_results["warnings"].append(f"External tool not available: {tool}")
    
    def fix_missing_dependency(self, package_name: str) -> None:
        """Attempt to fix missing dependency."""
        try:
            subprocess.run([sys.executable, "-m", "pip", "install", package_name], 
                         check=True, capture_output=True)
            self.validation_results["fixed"].append(f"Installed missing dependency: {package_name}")
        except subprocess.CalledProcessError:
            self.validation_results["errors"].append(f"Failed to install: {package_name}")
    
    def fix_missing_directory(self, directory: Path) -> None:
        """Attempt to fix missing directory."""
        try:
            directory.mkdir(parents=True, exist_ok=True)
            self.validation_results["fixed"].append(f"Created missing directory: {directory}")
        except Exception as e:
            self.validation_results["errors"].append(f"Failed to create directory {directory}: {e}")
    
    def print_validation_summary(self) -> None:
        """Print comprehensive validation summary."""
        logger.info("\n" + "="*80)
        logger.info("PREREQUISITES VALIDATION SUMMARY")
        logger.info("="*80)
        
        # Print passed validations
        if self.validation_results["passed"]:
            logger.info(f"\n✅ PASSED ({len(self.validation_results['passed'])} items):")
            for item in self.validation_results["passed"]:
                logger.info(f"   ✓ {item}")
        
        # Print warnings
        if self.validation_results["warnings"]:
            logger.info(f"\n⚠️  WARNINGS ({len(self.validation_results['warnings'])} items):")
            for item in self.validation_results["warnings"]:
                logger.warning(f"   ⚠ {item}")
        
        # Print errors
        if self.validation_results["errors"]:
            logger.info(f"\n❌ ERRORS ({len(self.validation_results['errors'])} items):")
            for item in self.validation_results["errors"]:
                logger.error(f"   ✗ {item}")
        
        # Print fixes applied
        if self.validation_results["fixed"]:
            logger.info(f"\n🔧 FIXES APPLIED ({len(self.validation_results['fixed'])} items):")
            for item in self.validation_results["fixed"]:
                logger.info(f"   🔧 {item}")
        
        # Overall status
        logger.info("\n" + "="*80)
        total_issues = len(self.validation_results["warnings"]) + len(self.validation_results["errors"])
        if total_issues == 0:
            logger.info("🎉 ALL PREREQUISITES VALIDATED SUCCESSFULLY!")
            logger.info("You can now run the YouNiverse data collection pipeline.")
        else:
            logger.info(f"⚠️  VALIDATION COMPLETED WITH {total_issues} ISSUES")
            if self.validation_results["errors"]:
                logger.info("❌ Critical errors must be resolved before running the pipeline.")
            if self.validation_results["warnings"]:
                logger.info("⚠️  Warnings should be reviewed but may not prevent pipeline execution.")
        
        logger.info("="*80)
        
        if self.detailed:
            self.print_system_details()
    
    def print_system_details(self) -> None:
        """Print detailed system information."""
        logger.info("\n" + "="*60)
        logger.info("DETAILED SYSTEM INFORMATION")
        logger.info("="*60)
        
        logger.info(f"Timestamp: {datetime.now().isoformat()}")
        logger.info(f"Platform: {platform.platform()}")
        logger.info(f"Python: {sys.version}")
        logger.info(f"Working Directory: {os.getcwd()}")
        logger.info(f"Root Directory: {self.root_dir}")
        
        # Environment variables
        env_vars = ["PATH", "PYTHONPATH", "VIRTUAL_ENV"]
        for var in env_vars:
            value = os.environ.get(var, "Not set")
            logger.info(f"{var}: {value}")
        
        logger.info("="*60)


def main():
    """Main entry point for the validation script."""
    parser = argparse.ArgumentParser(
        description="Validate YouNiverse Dataset Enrichment Pipeline prerequisites"
    )
    parser.add_argument(
        "--detailed", "-d",
        action="store_true",
        help="Show detailed system information"
    )
    parser.add_argument(
        "--fix-issues", "-f",
        action="store_true",
        help="Attempt to automatically fix detected issues"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Run validation
    validator = PrerequisitesValidator(
        fix_issues=args.fix_issues,
        detailed=args.detailed
    )
    success = validator.run_validation()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()