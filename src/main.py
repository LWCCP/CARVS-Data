"""
Main Orchestrator for YouNiverse Dataset Enrichment

5-Phase Pipeline Coordinator:
- Phase 1: YouTube API enrichment (current metadata)
- Phase 2A: Wayback Charts collection (historical weekly data)
- Phase 2B: Wayback Uploads collection (historical upload counts)
- Phase 3: ViewStats collection (current longform/shorts breakdown)
- Phase 4: Final API validation (current week data verification)

Author: feature-developer
"""

import sys
import argparse
import logging
import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime, date, timedelta
import csv

from src.models.channel import Channel, ChannelStatus
from src.data_collectors.youtube_api_collector import YouTubeAPICollector
from src.core.config import config_manager
from src.utils.scraping_utils import cleanup_scrapers


class PipelineOrchestrator:
    """
    Main orchestrator for the 5-phase data collection pipeline
    
    Coordinates all data collectors and manages the overall enrichment process
    for transforming 18.8M records into comprehensive 10-year dataset.
    """
    
    def __init__(self, input_file: str, output_dir: str = "data/output"):
        self.input_file = Path(input_file)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        self.logger = self._setup_logging()
        
        # Pipeline state
        self.channels: List[Channel] = []
        self.pipeline_start_time: Optional[datetime] = None
        self.pipeline_end_time: Optional[datetime] = None
        
        # Statistics
        self.phase_stats: Dict[str, Dict[str, Any]] = {}
        
        self.logger.info(f"Pipeline orchestrator initialized")
        self.logger.info(f"Input file: {self.input_file}")
        self.logger.info(f"Output directory: {self.output_dir}")
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration"""
        logger = logging.getLogger("PipelineOrchestrator")
        logger.setLevel(logging.INFO)
        
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Create file handler
        log_file = self.output_dir / "pipeline.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)
        
        # Add handlers
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
        
        return logger
    
    def load_channels(self) -> None:
        """Load channels from input file"""
        self.logger.info(f"Loading channels from {self.input_file}")
        
        if not self.input_file.exists():
            raise FileNotFoundError(f"Input file not found: {self.input_file}")
        
        channels = []
        
        try:
            if self.input_file.suffix.lower() == '.csv':
                # Load from CSV
                with open(self.input_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        channel_id = row.get('channel_id') or row.get('id')
                        if channel_id:
                            channel = Channel(
                                channel_id=channel_id,
                                channel_name=row.get('channel_name', ''),
                                status=ChannelStatus.UNKNOWN
                            )
                            channels.append(channel)
            
            elif self.input_file.suffix.lower() == '.json':
                # Load from JSON
                with open(self.input_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                    if isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict):
                                channel_id = item.get('channel_id') or item.get('id')
                                if channel_id:
                                    # Try to deserialize full channel if available
                                    try:
                                        channel = Channel.from_dict(item)
                                    except:
                                        # Fallback to basic channel creation
                                        channel = Channel(
                                            channel_id=channel_id,
                                            channel_name=item.get('channel_name', ''),
                                            status=ChannelStatus.UNKNOWN
                                        )
                                    channels.append(channel)
            
            elif self.input_file.suffix.lower() == '.txt':
                # Load from text file (one channel ID per line)
                with open(self.input_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        channel_id = line.strip()
                        if channel_id:
                            channel = Channel(
                                channel_id=channel_id,
                                status=ChannelStatus.UNKNOWN
                            )
                            channels.append(channel)
            
            else:
                raise ValueError(f"Unsupported file format: {self.input_file.suffix}")
        
        except Exception as e:
            self.logger.error(f"Failed to load channels: {e}")
            raise e
        
        self.channels = channels
        self.logger.info(f"Loaded {len(self.channels)} channels")
    
    def save_channels(self, filename: str = None, phase: str = "") -> None:
        """Save channels to output file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            phase_suffix = f"_{phase}" if phase else ""
            filename = f"channels{phase_suffix}_{timestamp}.json"
        
        output_file = self.output_dir / filename
        
        try:
            # Convert channels to dictionaries
            channels_data = [channel.to_dict() for channel in self.channels]
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(channels_data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Saved {len(self.channels)} channels to {output_file}")
        
        except Exception as e:
            self.logger.error(f"Failed to save channels: {e}")
            raise e
    
    def run_phase_1_youtube_api(self) -> None:
        """
        Phase 1: YouTube API Enrichment
        
        Enriches channels with current metadata from YouTube Data API v3
        - Channel name, category, country, language
        - Current subscriber count, view count, video count
        - Channel status and creation date
        """
        self.logger.info("=" * 60)
        self.logger.info("PHASE 1: YouTube API Enrichment")
        self.logger.info("=" * 60)
        
        collector = YouTubeAPICollector()
        
        # Set up progress callback
        def progress_callback(progress):
            if progress.current_batch % 10 == 0 or progress.current_batch == progress.total_batches:
                self.logger.info(
                    f"Phase 1 Progress: {progress.processed_channels}/{progress.total_channels} "
                    f"({progress.get_success_rate():.1f}% success) - "
                    f"Batch {progress.current_batch}/{progress.total_batches}"
                )
        
        collector.set_progress_callback(progress_callback)
        
        try:
            # Run collection
            enriched_channels = collector.collect(self.channels)
            
            # Update channels list
            self.channels = enriched_channels
            
            # Store phase statistics
            self.phase_stats['phase_1'] = collector.get_collection_summary()
            
            # Save intermediate results
            self.save_channels(phase="phase1_youtube_api")
            
            self.logger.info("Phase 1 completed successfully")
            self.logger.info(f"Quota usage: {collector.get_quota_usage()}")
            
        except Exception as e:
            self.logger.error(f"Phase 1 failed: {e}")
            raise e
    
    def run_phase_2a_wayback_charts(self) -> None:
        """
        Phase 2A: Wayback Charts Collection
        
        Collects historical weekly subscriber/view data from Social Blade
        via Wayback Machine snapshots (Sep 2019 - Dec 2022, 165 weeks)
        """
        self.logger.info("=" * 60)
        self.logger.info("PHASE 2A: Wayback Charts Collection")
        self.logger.info("=" * 60)
        
        try:
            from src.data_collectors.wayback_charts_collector import WaybackChartsCollector
            
            collector = WaybackChartsCollector()
            
            phase_results = []
            successful_channels = 0
            
            for i, channel in enumerate(self.channels):
                try:
                    self.logger.info(f"Collecting Wayback charts for {channel.channel_id} ({i+1}/{len(self.channels)})")
                    
                    result = collector.collect_channel_data(channel)
                    phase_results.append(result)
                    
                    if result['success']:
                        successful_channels += 1
                        # Update channel object with historical data
                        if hasattr(channel, 'historical_data'):
                            channel.historical_data.extend(result['historical_data'])
                        else:
                            channel.historical_data = result['historical_data']
                    
                    # Progress reporting
                    success_rate = (successful_channels / (i + 1)) * 100
                    self.logger.info(f"Phase 2A Progress: {successful_channels}/{i+1} ({success_rate:.1f}% success)")
                    
                except Exception as e:
                    self.logger.error(f"Phase 2A failed for channel {channel.channel_id}: {e}")
                    phase_results.append({
                        'channel_id': channel.channel_id,
                        'success': False,
                        'error': str(e),
                        'collection_phase': 'phase2a_wayback_charts'
                    })
            
            # Save phase results
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = self.output_dir / f"channels_phase2a_wayback_charts_{timestamp}.json"
            
            with open(output_file, 'w') as f:
                json.dump(phase_results, f, indent=2, default=str)
            
            self.logger.info(f"Saved {len(phase_results)} channels to {output_file}")
            
            # Update phase statistics
            total_data_points = sum(r.get('data_points_collected', 0) for r in phase_results)
            total_weeks = sum(r.get('weeks_covered', 0) for r in phase_results)
            
            self.phase_stats['phase_2a'] = {
                'status': 'completed',
                'channels_processed': len(self.channels),
                'channels_successful': successful_channels,
                'success_rate': (successful_channels / len(self.channels)) * 100,
                'total_data_points': total_data_points,
                'total_weeks_covered': total_weeks,
                'average_weeks_per_channel': total_weeks / len(self.channels) if self.channels else 0,
                'output_file': str(output_file)
            }
            
            self.logger.info("Phase 2A completed successfully")
            self.logger.info(f"Success rate: {successful_channels}/{len(self.channels)} ({self.phase_stats['phase_2a']['success_rate']:.1f}%)")
            self.logger.info(f"Total historical data points: {total_data_points}")
            
        except Exception as e:
            self.logger.error(f"Phase 2A initialization failed: {e}")
            self.phase_stats['phase_2a'] = {
                'status': 'failed',
                'error': str(e)
            }
    
    def run_phase_2b_wayback_uploads(self) -> None:
        """
        Phase 2B: Wayback Uploads Collection
        
        Collects historical upload counts from Social Blade main pages
        via Wayback Machine snapshots (variable coverage)
        """
        self.logger.info("=" * 60)
        self.logger.info("PHASE 2B: Wayback Uploads Collection")
        self.logger.info("=" * 60)
        
        # TODO: Implement WaybackUploadsCollector
        # collector = WaybackUploadsCollector()
        # enriched_channels = collector.collect(self.channels)
        # self.channels = enriched_channels
        
        self.logger.warning("Phase 2B not yet implemented - placeholder")
        self.phase_stats['phase_2b'] = {
            'status': 'not_implemented',
            'message': 'Wayback Uploads collector pending implementation'
        }
    
    def run_phase_3_viewstats(self) -> None:
        """
        Phase 3: ViewStats Historical Collection
        
        Collects historical weekly data (Aug 2022 - present) from ViewStats
        including subscribers, total views, longform views, and shorts views
        using JavaScript rendering and max button functionality
        """
        self.logger.info("=" * 60)
        self.logger.info("PHASE 3: ViewStats Historical Collection")
        self.logger.info("=" * 60)
        
        start_time = datetime.now()
        
        try:
            from src.data_collectors.viewstats_collector import ViewStatsCollector
            
            collector = ViewStatsCollector()
            self.logger.info(f"ViewStats collector initialized - targeting {collector.target_weeks} weekly data points")
            self.logger.info(f"Date range: {collector.start_date.date()} to {collector.end_date.date()}")
            
            # Run collection
            enriched_channels = collector.collect(self.channels)
            
            # Update channels with results
            self.channels = enriched_channels
            
            # Calculate statistics
            successful_collections = sum(1 for ch in enriched_channels 
                                       if hasattr(ch, 'viewstats_data') and ch.viewstats_data)
            
            phase_duration = datetime.now() - start_time
            
            self.phase_stats['phase_3'] = {
                'status': 'completed',
                'start_time': start_time.isoformat(),
                'end_time': datetime.now().isoformat(),
                'duration': str(phase_duration),
                'channels_processed': len(self.channels),
                'successful_collections': successful_collections,
                'success_rate': (successful_collections / len(self.channels) * 100) if self.channels else 0,
                'target_weeks': collector.target_weeks,
                'date_range': f"{collector.start_date.date()} to {collector.end_date.date()}",
                'data_points_target': collector.target_weeks * len(self.channels)
            }
            
            self.logger.info(f"Phase 3 completed: {successful_collections}/{len(self.channels)} channels successful")
            self.logger.info(f"Success rate: {self.phase_stats['phase_3']['success_rate']:.1f}%")
            
        except Exception as e:
            self.logger.error(f"Phase 3 failed: {e}")
            self.phase_stats['phase_3'] = {
                'status': 'failed',
                'error': str(e),
                'start_time': start_time.isoformat(),
                'end_time': datetime.now().isoformat()
            }
    
    def run_phase_4_api_current(self) -> None:
        """
        Phase 4: Final API Current Collection
        
        Final YouTube API call for current week data validation
        against ViewStats data (junction point validation)
        """
        self.logger.info("=" * 60)
        self.logger.info("PHASE 4: Final API Current Collection")
        self.logger.info("=" * 60)
        
        # TODO: Implement ApiCurrentCollector
        # collector = ApiCurrentCollector()
        # validated_channels = collector.collect(self.channels)
        # self.channels = validated_channels
        
        self.logger.warning("Phase 4 not yet implemented - placeholder")
        self.phase_stats['phase_4'] = {
            'status': 'not_implemented',
            'message': 'API Current collector pending implementation'
        }
    
    def validate_junction_points(self) -> None:
        """
        Junction Point Validation
        
        Validates data consistency at critical junction points:
        - Sep 2019: Original dataset end → Wayback Machine start
        - Aug 2022: Wayback Machine end → ViewStats start
        """
        self.logger.info("=" * 60)
        self.logger.info("JUNCTION POINT VALIDATION")
        self.logger.info("=" * 60)
        
        sep_2019_validated = 0
        aug_2022_validated = 0
        
        for channel in self.channels:
            # Sep 2019 junction point validation
            if 'sep_2019' in channel.history.junction_points_validated:
                sep_2019_validated += 1
            
            # Aug 2022 junction point validation
            if 'aug_2022' in channel.history.junction_points_validated:
                aug_2022_validated += 1
        
        total_channels = len(self.channels)
        
        self.logger.info(f"Sep 2019 junction point validated: {sep_2019_validated}/{total_channels} "
                        f"({sep_2019_validated/total_channels*100:.1f}%)")
        self.logger.info(f"Aug 2022 junction point validated: {aug_2022_validated}/{total_channels} "
                        f"({aug_2022_validated/total_channels*100:.1f}%)")
        
        # Store validation results
        self.phase_stats['junction_validation'] = {
            'sep_2019_validated': sep_2019_validated,
            'aug_2022_validated': aug_2022_validated,
            'total_channels': total_channels,
            'sep_2019_rate': sep_2019_validated/total_channels*100 if total_channels > 0 else 0,
            'aug_2022_rate': aug_2022_validated/total_channels*100 if total_channels > 0 else 0
        }
    
    def generate_pipeline_report(self) -> None:
        """Generate comprehensive pipeline report"""
        report_file = self.output_dir / "pipeline_report.json"
        
        total_time = None
        if self.pipeline_start_time and self.pipeline_end_time:
            total_time = self.pipeline_end_time - self.pipeline_start_time
        
        # Calculate overall statistics
        total_channels = len(self.channels)
        enriched_channels = sum(1 for ch in self.channels if ch.processing_status == "enriched")
        failed_channels = sum(1 for ch in self.channels if ch.processing_status == "failed")
        not_found_channels = sum(1 for ch in self.channels if ch.processing_status == "not_found")
        
        report = {
            'pipeline_summary': {
                'start_time': self.pipeline_start_time.isoformat() if self.pipeline_start_time else None,
                'end_time': self.pipeline_end_time.isoformat() if self.pipeline_end_time else None,
                'total_time': str(total_time) if total_time else None,
                'total_channels': total_channels,
                'enriched_channels': enriched_channels,
                'failed_channels': failed_channels,
                'not_found_channels': not_found_channels,
                'success_rate': (enriched_channels / total_channels * 100) if total_channels > 0 else 0
            },
            'phase_statistics': self.phase_stats,
            'channel_status_distribution': {
                'active': sum(1 for ch in self.channels if ch.status.value == 'active'),
                'inactive': sum(1 for ch in self.channels if ch.status.value == 'inactive'),
                'terminated': sum(1 for ch in self.channels if ch.status.value == 'terminated'),
                'private': sum(1 for ch in self.channels if ch.status.value == 'private'),
                'unknown': sum(1 for ch in self.channels if ch.status.value == 'unknown')
            }
        }
        
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Pipeline report saved to {report_file}")
        
        except Exception as e:
            self.logger.error(f"Failed to save pipeline report: {e}")
    
    def run_full_pipeline(self, phases: List[str] = None) -> None:
        """
        Run the complete 5-phase pipeline
        
        Args:
            phases: List of phases to run. If None, runs all phases.
                   Options: ['phase1', 'phase2a', 'phase2b', 'phase3', 'phase4']
        """
        if phases is None:
            phases = ['phase1', 'phase2a', 'phase2b', 'phase3', 'phase4']
        
        self.pipeline_start_time = datetime.now()
        
        self.logger.info(" Starting YouNiverse Dataset Enrichment Pipeline")
        self.logger.info(f"Target: Transform {len(self.channels)} channels into 10-year comprehensive dataset")
        self.logger.info(f"Phases to run: {', '.join(phases)}")
        
        try:
            # Phase 1: YouTube API Enrichment
            if 'phase1' in phases:
                self.run_phase_1_youtube_api()
            
            # Phase 2A: Wayback Charts Collection
            if 'phase2a' in phases:
                self.run_phase_2a_wayback_charts()
            
            # Phase 2B: Wayback Uploads Collection
            if 'phase2b' in phases:
                self.run_phase_2b_wayback_uploads()
            
            # Phase 3: ViewStats Collection
            if 'phase3' in phases:
                self.run_phase_3_viewstats()
            
            # Phase 4: Final API Current Collection
            if 'phase4' in phases:
                self.run_phase_4_api_current()
            
            # Junction Point Validation
            self.validate_junction_points()
            
            # Final save
            self.save_channels(filename="final_enriched_channels.json")
            
            self.pipeline_end_time = datetime.now()
            
            # Generate report
            self.generate_pipeline_report()
            
            self.logger.info("🎉 Pipeline completed successfully!")
            self.logger.info(f"Total time: {self.pipeline_end_time - self.pipeline_start_time}")
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            self.pipeline_end_time = datetime.now()
            
            # Still try to save what we have
            try:
                self.save_channels(filename="partial_enriched_channels.json")
                self.generate_pipeline_report()
            except:
                pass
            
            raise e
        
        finally:
            # Cleanup resources
            cleanup_scrapers()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="YouNiverse Dataset Enrichment Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m src.main --input channels.csv --output data/output
  python -m src.main --input channels.json --phases phase1,phase3
  python -m src.main --input channel_ids.txt --limit 1000
        """
    )
    
    parser.add_argument(
        '--input', '-i',
        required=True,
        help='Input file containing channel data (CSV, JSON, or TXT)'
    )
    
    parser.add_argument(
        '--output', '-o',
        default='data/output',
        help='Output directory for results (default: data/output)'
    )
    
    parser.add_argument(
        '--phases', '-p',
        default='phase1',
        help='Comma-separated list of phases to run (default: phase1). Options: phase1,phase2a,phase2b,phase3,phase4'
    )
    
    parser.add_argument(
        '--limit', '-l',
        type=int,
        help='Limit number of channels to process (for testing)'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Set logging level (default: INFO)'
    )
    
    args = parser.parse_args()
    
    # Set logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    try:
        # Initialize orchestrator
        orchestrator = PipelineOrchestrator(args.input, args.output)
        
        # Load channels
        orchestrator.load_channels()
        
        # Apply limit if specified
        if args.limit:
            orchestrator.channels = orchestrator.channels[:args.limit]
            orchestrator.logger.info(f"Limited to first {len(orchestrator.channels)} channels")
        
        # Parse phases
        phases = [phase.strip() for phase in args.phases.split(',')]
        
        # Run pipeline
        orchestrator.run_full_pipeline(phases)
        
    except KeyboardInterrupt:
        print("\n  Pipeline interrupted by user")
        sys.exit(1)
    
    except Exception as e:
        print(f" Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()