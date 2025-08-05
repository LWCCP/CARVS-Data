#!/usr/bin/env python3
"""
Progress Monitoring for YouNiverse Dataset Enrichment Pipeline

This script monitors the real-time progress of the data collection pipeline,
providing detailed statistics, performance metrics, and status updates.

Usage:
    python scripts/monitoring/check_progress.py
    python scripts/monitoring/check_progress.py --watch
    python scripts/monitoring/check_progress.py --detailed
    python scripts/monitoring/check_progress.py --export-json
"""

import os
import sys
import json
import yaml
import argparse
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import subprocess
import psutil
import threading
from concurrent.futures import ThreadPoolExecutor

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ProgressMonitor:
    """Real-time progress monitoring for YouNiverse pipeline."""
    
    def __init__(self, root_dir: Optional[Path] = None):
        self.root_dir = root_dir or Path(__file__).parent.parent.parent
        self.data_dir = self.root_dir / "data"
        self.logs_dir = self.root_dir / "logs"
        self.config_dir = self.root_dir / "config"
        
        # Load configurations
        self.config = self.load_configurations()
        
        # Initialize monitoring state
        self.monitoring_state = {
            "start_time": datetime.now(),
            "last_update": None,
            "update_interval": 30,  # seconds
            "stats_history": [],
            "alerts": []
        }
        
        # Performance tracking
        self.performance_metrics = {
            "cpu_usage": [],
            "memory_usage": [],
            "disk_usage": [],
            "network_io": [],
            "process_stats": []
        }
    
    def load_configurations(self) -> Dict[str, Any]:
        """Load monitoring configuration."""
        config = {}
        
        try:
            # Load pipeline configurations
            config_files = [
                ("rate_limits", "rate_limits.json"),
                ("logging", "logging_config.yaml"),
                ("data_quality", "data_quality_rules.json")
            ]
            
            for config_name, filename in config_files:
                config_path = self.config_dir / filename
                if config_path.exists():
                    if filename.endswith('.json'):
                        with open(config_path, 'r') as f:
                            config[config_name] = json.load(f)
                    elif filename.endswith('.yaml'):
                        with open(config_path, 'r') as f:
                            config[config_name] = yaml.safe_load(f)
            
            return config
            
        except Exception as e:
            logger.warning(f"Could not load configurations: {e}")
            return {}
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get current pipeline status from various sources."""
        status = {
            "timestamp": datetime.now().isoformat(),
            "pipeline_running": False,
            "current_phase": None,
            "overall_progress": 0.0,
            "phase_progress": {},
            "channel_stats": {
                "total_channels": 153550,
                "processed_channels": 0,
                "successful_channels": 0,
                "failed_channels": 0,
                "skipped_channels": 0,
                "success_rate": 0.0
            },
            "processing_rates": {
                "channels_per_hour": 0,
                "api_requests_per_minute": 0,
                "data_throughput_mb_per_hour": 0
            },
            "quality_metrics": {
                "data_quality_score": 0.0,
                "validation_pass_rate": 0.0,
                "error_rate": 0.0
            },
            "system_health": {
                "cpu_usage_percent": 0.0,
                "memory_usage_percent": 0.0,
                "disk_usage_percent": 0.0,
                "network_active": False
            },
            "estimated_completion": None,
            "recent_errors": [],
            "active_collectors": []
        }
        
        # Check if pipeline is running
        status["pipeline_running"] = self.is_pipeline_running()
        
        # Load checkpoint data if available
        checkpoint_data = self.load_checkpoint_data()
        if checkpoint_data:
            status.update(self.extract_status_from_checkpoint(checkpoint_data))
        
        # Parse log files for recent activity
        log_stats = self.parse_log_files()
        status.update(log_stats)
        
        # Get system metrics
        system_metrics = self.get_system_metrics()
        status["system_health"].update(system_metrics)
        
        # Calculate derived metrics
        status = self.calculate_derived_metrics(status)
        
        return status
    
    def is_pipeline_running(self) -> bool:
        """Check if the pipeline process is currently running."""
        try:
            # Look for python processes running the pipeline script
            for process in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if process.info['name'] == 'python' or process.info['name'].startswith('python'):
                        cmdline = process.info['cmdline']
                        if cmdline and any('run_full_pipeline.py' in cmd for cmd in cmdline):
                            return True
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            return False
        except Exception:
            return False
    
    def load_checkpoint_data(self) -> Optional[Dict[str, Any]]:
        """Load the latest checkpoint data."""
        checkpoint_file = self.data_dir / "checkpoint.json"
        if checkpoint_file.exists():
            try:
                with open(checkpoint_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load checkpoint: {e}")
        return None
    
    def extract_status_from_checkpoint(self, checkpoint_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract status information from checkpoint data."""
        pipeline_state = checkpoint_data.get("pipeline_state", {})
        
        return {
            "current_phase": pipeline_state.get("current_phase"),
            "completed_phases": pipeline_state.get("completed_phases", []),
            "failed_phases": pipeline_state.get("failed_phases", []),
            "channel_stats": pipeline_state.get("channel_stats", {}),
            "phase_stats": pipeline_state.get("phase_stats", {}),
            "checkpoint_timestamp": checkpoint_data.get("timestamp"),
            "pipeline_errors": pipeline_state.get("errors", [])
        }
    
    def parse_log_files(self) -> Dict[str, Any]:
        """Parse log files for recent activity and statistics."""
        log_stats = {
            "recent_errors": [],
            "active_collectors": [],
            "processing_rates": {
                "channels_per_hour": 0,
                "api_requests_per_minute": 0
            }
        }
        
        try:
            # Parse error log
            error_log = self.logs_dir / "error.log"
            if error_log.exists():
                log_stats["recent_errors"] = self.parse_error_log(error_log)
            
            # Parse data collection log
            data_log = self.logs_dir / "data_collection.log"
            if data_log.exists():
                collection_stats = self.parse_data_collection_log(data_log)
                log_stats.update(collection_stats)
            
            # Parse performance log
            perf_log = self.logs_dir / "performance.log"
            if perf_log.exists():
                perf_stats = self.parse_performance_log(perf_log)
                log_stats.update(perf_stats)
                
        except Exception as e:
            logger.warning(f"Error parsing log files: {e}")
        
        return log_stats
    
    def parse_error_log(self, log_file: Path, max_errors: int = 10) -> List[Dict[str, Any]]:
        """Parse recent errors from error log."""
        errors = []
        try:
            with open(log_file, 'r') as f:
                lines = f.readlines()
                
            # Get last N lines that contain ERROR
            error_lines = [line for line in lines[-1000:] if "ERROR" in line]
            
            for line in error_lines[-max_errors:]:
                try:
                    # Parse timestamp and error message
                    parts = line.strip().split(" - ", 3)
                    if len(parts) >= 4:
                        errors.append({
                            "timestamp": parts[0],
                            "logger": parts[1],
                            "level": parts[2],
                            "message": parts[3]
                        })
                except Exception:
                    continue
                    
        except Exception as e:
            logger.warning(f"Could not parse error log: {e}")
        
        return errors
    
    def parse_data_collection_log(self, log_file: Path) -> Dict[str, Any]:
        """Parse data collection statistics from log."""
        stats = {
            "active_collectors": [],
            "processing_rates": {
                "channels_per_hour": 0,
                "api_requests_per_minute": 0
            }
        }
        
        try:
            # Read recent log entries
            with open(log_file, 'r') as f:
                lines = f.readlines()
            
            recent_lines = lines[-500:]  # Last 500 lines
            
            # Extract collector activity
            collectors = set()
            for line in recent_lines:
                if "youniverse.collectors" in line:
                    parts = line.split("youniverse.collectors.")
                    if len(parts) > 1:
                        collector = parts[1].split(" ")[0].split(" - ")[0]
                        collectors.add(collector)
            
            stats["active_collectors"] = list(collectors)
            
            # Calculate processing rates (simplified)
            channel_count = sum(1 for line in recent_lines if "processed channel" in line.lower())
            api_count = sum(1 for line in recent_lines if "api request" in line.lower())
            
            # Estimate rates based on recent activity
            time_window_hours = 1  # Assume 1 hour window
            stats["processing_rates"]["channels_per_hour"] = channel_count / time_window_hours
            stats["processing_rates"]["api_requests_per_minute"] = api_count / 60
            
        except Exception as e:
            logger.warning(f"Could not parse data collection log: {e}")
        
        return stats
    
    def parse_performance_log(self, log_file: Path) -> Dict[str, Any]:
        """Parse performance metrics from log."""
        perf_stats = {
            "quality_metrics": {
                "data_quality_score": 0.0,
                "validation_pass_rate": 0.0,
                "error_rate": 0.0
            }
        }
        
        try:
            with open(log_file, 'r') as f:
                lines = f.readlines()
            
            # Extract performance metrics from recent entries
            recent_lines = lines[-200:]
            
            for line in recent_lines:
                if "quality_score" in line.lower():
                    # Extract quality score
                    try:
                        score = float(line.split("quality_score:")[-1].strip())
                        perf_stats["quality_metrics"]["data_quality_score"] = score
                    except Exception:
                        pass
                        
        except Exception as e:
            logger.warning(f"Could not parse performance log: {e}")
        
        return perf_stats
    
    def get_system_metrics(self) -> Dict[str, Any]:
        """Get current system resource metrics."""
        metrics = {
            "cpu_usage_percent": 0.0,
            "memory_usage_percent": 0.0,
            "disk_usage_percent": 0.0,
            "network_active": False,
            "process_count": 0
        }
        
        try:
            # CPU usage
            metrics["cpu_usage_percent"] = psutil.cpu_percent(interval=1)
            
            # Memory usage
            memory = psutil.virtual_memory()
            metrics["memory_usage_percent"] = memory.percent
            
            # Disk usage
            disk = psutil.disk_usage(str(self.root_dir))
            metrics["disk_usage_percent"] = (disk.used / disk.total) * 100
            
            # Network activity (simplified check)
            network = psutil.net_io_counters()
            metrics["network_active"] = network.bytes_sent > 0 and network.bytes_recv > 0
            
            # Process count
            metrics["process_count"] = len(psutil.pids())
            
        except Exception as e:
            logger.warning(f"Could not get system metrics: {e}")
        
        return metrics
    
    def calculate_derived_metrics(self, status: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate derived metrics and estimates."""
        try:
            # Calculate overall progress
            completed_phases = len(status.get("completed_phases", []))
            total_phases = 5
            status["overall_progress"] = (completed_phases / total_phases) * 100
            
            # Calculate success rate
            channel_stats = status["channel_stats"]
            processed = channel_stats.get("processed_channels", 0)
            successful = channel_stats.get("successful_channels", 0)
            
            if processed > 0:
                channel_stats["success_rate"] = (successful / processed) * 100
            
            # Estimate completion time
            if status["processing_rates"]["channels_per_hour"] > 0:
                remaining_channels = channel_stats["total_channels"] - processed
                remaining_hours = remaining_channels / status["processing_rates"]["channels_per_hour"]
                completion_time = datetime.now() + timedelta(hours=remaining_hours)
                status["estimated_completion"] = completion_time.isoformat()
            
            # Calculate error rate
            failed = channel_stats.get("failed_channels", 0)
            if processed > 0:
                status["quality_metrics"]["error_rate"] = (failed / processed) * 100
                
        except Exception as e:
            logger.warning(f"Could not calculate derived metrics: {e}")
        
        return status
    
    def print_progress_report(self, status: Dict[str, Any], detailed: bool = False) -> None:
        """Print formatted progress report."""
        print("\n" + "="*80)
        print("YOUNIVERSE PIPELINE PROGRESS REPORT")
        print("="*80)
        print(f"Timestamp: {status['timestamp']}")
        print(f"Pipeline Status: {'RUNNING' if status['pipeline_running'] else 'STOPPED'}")
        
        if status.get("current_phase"):
            print(f"Current Phase: {status['current_phase']}")
        
        print(f"Overall Progress: {status['overall_progress']:.1f}%")
        
        # Channel statistics
        print(f"\nChannel Processing:")
        stats = status["channel_stats"]
        print(f"  Total Channels: {stats['total_channels']:,}")
        print(f"  Processed: {stats['processed_channels']:,}")
        print(f"  Successful: {stats['successful_channels']:,}")
        print(f"  Failed: {stats['failed_channels']:,}")
        print(f"  Success Rate: {stats['success_rate']:.1f}%")
        
        # Processing rates
        print(f"\nProcessing Rates:")
        rates = status["processing_rates"]
        print(f"  Channels/Hour: {rates['channels_per_hour']:.1f}")
        print(f"  API Requests/Min: {rates['api_requests_per_minute']:.1f}")
        
        # System health
        print(f"\nSystem Health:")
        health = status["system_health"]
        print(f"  CPU Usage: {health['cpu_usage_percent']:.1f}%")
        print(f"  Memory Usage: {health['memory_usage_percent']:.1f}%")
        print(f"  Disk Usage: {health['disk_usage_percent']:.1f}%")
        
        if status.get("estimated_completion"):
            completion_time = datetime.fromisoformat(status["estimated_completion"])
            print(f"\nEstimated Completion: {completion_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Recent errors
        if status["recent_errors"]:
            print(f"\nRecent Errors ({len(status['recent_errors'])}):")
            for error in status["recent_errors"][-3:]:
                print(f"  {error['timestamp']}: {error['message'][:80]}...")
        
        if detailed:
            self.print_detailed_status(status)
        
        print("="*80)
    
    def print_detailed_status(self, status: Dict[str, Any]) -> None:
        """Print detailed status information."""
        print(f"\nDetailed Information:")
        
        # Active collectors
        if status["active_collectors"]:
            print(f"  Active Collectors: {', '.join(status['active_collectors'])}")
        
        # Completed phases
        if status.get("completed_phases"):
            print(f"  Completed Phases: {status['completed_phases']}")
        
        # Failed phases
        if status.get("failed_phases"):
            print(f"  Failed Phases: {status['failed_phases']}")
        
        # Quality metrics
        quality = status["quality_metrics"]
        print(f"  Data Quality Score: {quality['data_quality_score']:.2f}")
        print(f"  Error Rate: {quality['error_rate']:.2f}%")
    
    def watch_progress(self, interval: int = 30) -> None:
        """Watch progress in real-time with periodic updates."""
        print("Starting real-time progress monitoring... (Press Ctrl+C to stop)")
        
        try:
            while True:
                os.system('clear' if os.name == 'posix' else 'cls')  # Clear screen
                
                status = self.get_pipeline_status()
                self.print_progress_report(status, detailed=False)
                
                print(f"\nNext update in {interval} seconds... (Press Ctrl+C to stop)")
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\nProgress monitoring stopped.")
    
    def export_status_json(self, output_file: Optional[str] = None) -> str:
        """Export current status to JSON file."""
        status = self.get_pipeline_status()
        
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"pipeline_status_{timestamp}.json"
        
        output_path = Path(output_file)
        
        try:
            with open(output_path, 'w') as f:
                json.dump(status, f, indent=2, default=str)
            
            print(f"Status exported to: {output_path.absolute()}")
            return str(output_path.absolute())
            
        except Exception as e:
            logger.error(f"Failed to export status: {e}")
            raise


def main():
    """Main entry point for progress monitoring."""
    parser = argparse.ArgumentParser(
        description="Monitor YouNiverse Dataset Enrichment Pipeline progress"
    )
    parser.add_argument(
        "--watch", "-w",
        action="store_true",
        help="Watch progress in real-time with periodic updates"
    )
    parser.add_argument(
        "--interval", "-i",
        type=int,
        default=30,
        help="Update interval in seconds for watch mode (default: 30)"
    )
    parser.add_argument(
        "--detailed", "-d",
        action="store_true",
        help="Show detailed progress information"
    )
    parser.add_argument(
        "--export-json", "-e",
        type=str,
        nargs='?',
        const='',
        help="Export status to JSON file (optional filename)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Initialize monitor
    monitor = ProgressMonitor()
    
    try:
        if args.watch:
            monitor.watch_progress(interval=args.interval)
        elif args.export_json is not None:
            output_file = args.export_json if args.export_json else None
            monitor.export_status_json(output_file)
        else:
            status = monitor.get_pipeline_status()
            monitor.print_progress_report(status, detailed=args.detailed)
            
    except Exception as e:
        logger.error(f"Progress monitoring failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()