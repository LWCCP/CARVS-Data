#!/usr/bin/env python3
"""
Environment Setup Automation for YouNiverse Dataset Enrichment Pipeline

This script automates the initial setup and configuration of the development
and production environments for the YouNiverse data collection pipeline.

Usage:
    python scripts/setup_environment.py --env [dev|prod|test]
    python scripts/setup_environment.py --help
"""

import os
import sys
import subprocess
import argparse
import json
import yaml
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import platform
import shutil
import urllib.request
import zipfile
import tarfile

# Setup logging for the setup process
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EnvironmentSetup:
    """Automated environment setup for YouNiverse pipeline."""
    
    def __init__(self, environment: str = "dev"):
        self.environment = environment
        self.root_dir = Path(__file__).parent.parent
        self.config_dir = self.root_dir / "config"
        self.logs_dir = self.root_dir / "logs"
        self.data_dir = self.root_dir / "data"
        self.requirements_dir = self.root_dir / "requirements"
        
        # Environment-specific configurations
        self.env_configs = {
            "dev": {
                "requirements_file": self.requirements_dir / "dev.txt",
                "python_version": "3.9+",
                "install_jupyter": True,
                "setup_pre_commit": True,
                "create_test_data": True,
            },
            "prod": {
                "requirements_file": self.requirements_dir / "prod.txt",
                "python_version": "3.9+",
                "install_jupyter": False,
                "setup_pre_commit": False,
                "create_test_data": False,
            },
            "test": {
                "requirements_file": self.requirements_dir / "dev.txt",
                "python_version": "3.9+",
                "install_jupyter": False,
                "setup_pre_commit": True,
                "create_test_data": True,
            }
        }
    
    def run_setup(self) -> bool:
        """Run the complete environment setup process."""
        logger.info(f"Starting environment setup for: {self.environment}")
        
        try:
            # Step 1: Validate system requirements
            if not self.validate_system_requirements():
                return False
            
            # Step 2: Create directory structure
            self.create_directory_structure()
            
            # Step 3: Setup Python virtual environment
            if not self.setup_virtual_environment():
                return False
            
            # Step 4: Install Python dependencies
            if not self.install_dependencies():
                return False
            
            # Step 5: Configure environment files
            self.setup_environment_files()
            
            # Step 6: Initialize logging directories
            self.setup_logging()
            
            # Step 7: Create configuration templates
            self.setup_configuration_templates()
            
            # Step 8: Setup development tools (if dev environment)
            if self.environment == "dev":
                self.setup_development_tools()
            
            # Step 9: Validate installation
            if not self.validate_installation():
                return False
            
            logger.info("Environment setup completed successfully!")
            self.print_next_steps()
            return True
            
        except Exception as e:
            logger.error(f"Setup failed: {e}")
            return False
    
    def validate_system_requirements(self) -> bool:
        """Validate system requirements."""
        logger.info("Validating system requirements...")
        
        # Check Python version
        python_version = sys.version_info
        if python_version < (3, 9):
            logger.error(f"Python 3.9+ required, found {python_version.major}.{python_version.minor}")
            return False
        
        # Check available disk space (minimum 5GB)
        try:
            statvfs = os.statvfs(self.root_dir)
            free_space_gb = (statvfs.f_frsize * statvfs.f_bavail) / (1024**3)
            if free_space_gb < 5:
                logger.warning(f"Low disk space: {free_space_gb:.1f}GB available")
        except (OSError, AttributeError):
            logger.info("Could not check disk space (Windows/unsupported)")
        
        # Check for essential system tools
        required_tools = ["git", "pip"]
        for tool in required_tools:
            if not shutil.which(tool):
                logger.error(f"Required tool not found: {tool}")
                return False
        
        logger.info("System requirements validated successfully")
        return True
    
    def create_directory_structure(self) -> None:
        """Create necessary directory structure."""
        logger.info("Creating directory structure...")
        
        directories = [
            self.logs_dir,
            self.data_dir / "input",
            self.data_dir / "intermediate",
            self.data_dir / "output",
            self.data_dir / "quality" / "collection_metrics",
            self.data_dir / "quality" / "error_logs",
            self.data_dir / "quality" / "validation_reports",
            self.root_dir / "temp",
            self.root_dir / "cache",
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Created directory: {directory}")
    
    def setup_virtual_environment(self) -> bool:
        """Setup Python virtual environment."""
        logger.info("Setting up virtual environment...")
        
        venv_path = self.root_dir / "venv"
        
        try:
            if not venv_path.exists():
                subprocess.run([
                    sys.executable, "-m", "venv", str(venv_path)
                ], check=True)
                logger.info(f"Created virtual environment: {venv_path}")
            else:
                logger.info("Virtual environment already exists")
            
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to create virtual environment: {e}")
            return False
    
    def install_dependencies(self) -> bool:
        """Install Python dependencies."""
        logger.info("Installing Python dependencies...")
        
        config = self.env_configs[self.environment]
        requirements_file = config["requirements_file"]
        
        if not requirements_file.exists():
            logger.error(f"Requirements file not found: {requirements_file}")
            return False
        
        try:
            # Upgrade pip first
            subprocess.run([
                sys.executable, "-m", "pip", "install", "--upgrade", "pip"
            ], check=True)
            
            # Install requirements
            subprocess.run([
                sys.executable, "-m", "pip", "install", "-r", str(requirements_file)
            ], check=True)
            
            logger.info("Dependencies installed successfully")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to install dependencies: {e}")
            return False
    
    def setup_environment_files(self) -> None:
        """Setup environment configuration files."""
        logger.info("Setting up environment configuration files...")
        
        # Create .env file template
        env_file = self.root_dir / ".env"
        if not env_file.exists():
            env_template = """# YouNiverse Dataset Enrichment Environment Configuration

# API Keys (replace with actual values)
YOUTUBE_API_KEY=your_youtube_api_key_here
VIEWSTATS_API_KEY=your_viewstats_api_key_here

# Environment Settings
ENVIRONMENT={environment}
DEBUG={"true" if self.environment == "dev" else "false"}
LOG_LEVEL={"DEBUG" if self.environment == "dev" else "INFO"}

# Data Processing Settings
MAX_CONCURRENT_REQUESTS=8
BATCH_SIZE=50
RATE_LIMIT_ENABLED=true

# Database Configuration (if using)
DATABASE_URL=sqlite:///youniverse_data.db

# Cache Configuration
REDIS_URL=redis://localhost:6379/0
CACHE_TTL=3600

# Monitoring Configuration
SENTRY_DSN=your_sentry_dsn_here
METRICS_ENABLED=true

# File Storage Configuration
DATA_ROOT_PATH={data_dir}
LOG_ROOT_PATH={logs_dir}
TEMP_PATH={temp_dir}
""".format(
                environment=self.environment,
                data_dir=self.data_dir.absolute(),
                logs_dir=self.logs_dir.absolute(),
                temp_dir=(self.root_dir / "temp").absolute()
            )
            
            env_file.write_text(env_template)
            logger.info(f"Created environment file: {env_file}")
    
    def setup_logging(self) -> None:
        """Initialize logging directories and files."""
        logger.info("Setting up logging configuration...")
        
        # Create log files
        log_files = [
            "application.log",
            "data_collection.log",
            "error.log",
            "performance.log",
            "progress.log"
        ]
        
        for log_file in log_files:
            log_path = self.logs_dir / log_file
            if not log_path.exists():
                log_path.touch()
                logger.debug(f"Created log file: {log_path}")
    
    def setup_configuration_templates(self) -> None:
        """Setup configuration file templates."""
        logger.info("Setting up configuration templates...")
        
        # Validate configuration files exist
        config_files = [
            "rate_limits.json",
            "logging_config.yaml",
            "data_quality_rules.json"
        ]
        
        for config_file in config_files:
            config_path = self.config_dir / config_file
            if config_path.exists():
                logger.info(f"Configuration file exists: {config_file}")
            else:
                logger.warning(f"Configuration file missing: {config_file}")
    
    def setup_development_tools(self) -> None:
        """Setup development-specific tools."""
        logger.info("Setting up development tools...")
        
        try:
            # Setup pre-commit hooks
            if self.env_configs[self.environment].get("setup_pre_commit"):
                subprocess.run([
                    sys.executable, "-m", "pre_commit", "install"
                ], check=True)
                logger.info("Pre-commit hooks installed")
            
            # Setup Jupyter kernel
            if self.env_configs[self.environment].get("install_jupyter"):
                subprocess.run([
                    sys.executable, "-m", "ipykernel", "install", "--user", 
                    "--name", "youniverse", "--display-name", "YouNiverse Pipeline"
                ], check=True)
                logger.info("Jupyter kernel installed")
                
        except subprocess.CalledProcessError as e:
            logger.warning(f"Development tools setup failed: {e}")
    
    def validate_installation(self) -> bool:
        """Validate the installation."""
        logger.info("Validating installation...")
        
        try:
            # Test import of main modules
            test_imports = [
                "pandas", "numpy", "requests", "yaml", "pydantic"
            ]
            
            for module in test_imports:
                __import__(module)
                logger.debug(f"Successfully imported: {module}")
            
            # Test configuration file loading
            config_files = {
                self.config_dir / "rate_limits.json": json.load,
                self.config_dir / "logging_config.yaml": yaml.safe_load,
                self.config_dir / "data_quality_rules.json": json.load,
            }
            
            for config_file, loader in config_files.items():
                if config_file.exists():
                    with open(config_file, 'r') as f:
                        loader(f)
                    logger.debug(f"Successfully loaded: {config_file.name}")
            
            logger.info("Installation validation completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Installation validation failed: {e}")
            return False
    
    def print_next_steps(self) -> None:
        """Print next steps for the user."""
        logger.info("\n" + "="*60)
        logger.info("SETUP COMPLETED SUCCESSFULLY!")
        logger.info("="*60)
        logger.info("\nNext steps:")
        logger.info("1. Update API keys in .env file")
        logger.info("2. Review configuration files in config/ directory")
        logger.info("3. Run validation script: python scripts/validate_prerequisites.py")
        logger.info("4. Test pipeline: python scripts/run_full_pipeline.py --dry-run")
        
        if self.environment == "dev":
            logger.info("5. Start Jupyter: jupyter lab")
            logger.info("6. Run tests: pytest tests/")
        
        logger.info("\nFor more information, see docs/IMPLEMENTATION_GUIDE.md")
        logger.info("="*60)


def main():
    """Main entry point for the setup script."""
    parser = argparse.ArgumentParser(
        description="Setup YouNiverse Dataset Enrichment Pipeline environment"
    )
    parser.add_argument(
        "--env", 
        choices=["dev", "prod", "test"],
        default="dev",
        help="Environment to setup (default: dev)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Run setup
    setup = EnvironmentSetup(args.env)
    success = setup.run_setup()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()