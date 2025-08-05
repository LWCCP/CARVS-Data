#!/usr/bin/env python3
"""
YouNiverse Dataset Enrichment - Comprehensive Testing Suite
Tests all 5 phases with multiple channels and edge cases
"""

import time
import traceback
from datetime import datetime
from src.data_collectors.youtube_api_collector import YouTubeAPICollector
from src.data_collectors.wayback_charts_collector import WaybackChartsCollector  
from src.data_collectors.wayback_uploads_collector import WaybackUploadsCollector
from src.data_collectors.viewstats_collector import ViewStatsCollector
from src.data_collectors.api_current_collector import APICurrentCollector
from src.models.channel import Channel

def print_header(title):
    print('\n' + '=' * 80)
    print(f'{title.center(80)}')
    print('=' * 80)

def print_section(title):
    print('\n' + '-' * 60)
    print(f'{title}')
    print('-' * 60)

def test_phase_with_channel(phase_name, collector, channel, test_method='collect'):
    """Test a specific phase with a channel"""
    print(f'\nTesting {phase_name} with {channel.channel_name} ({channel.channel_id})')
    
    start_time = time.time()
    success = False
    error_msg = None
    data_points = 0
    notes = ""
    
    try:
        if test_method == 'collect_channel_data':
            result = collector.collect_channel_data(channel)
            success = result.get('success', False)
            error_msg = result.get('error')
            data_points = result.get('data_points_collected', 0)
            if not data_points:
                # Try other data point counts
                data_points = len(result.get('upload_data', []))
                if not data_points:
                    data_points = len(result.get('historical_data', []))
        elif test_method == 'collect_batch_data':
            results = collector.collect_batch_data([channel])
            if results:
                processed_channel = results[0]
                success = processed_channel.processing_status == 'enriched'
                error_msg = processed_channel.processing_notes if not success else None
                # Check for various data attributes
                if hasattr(processed_channel, 'current_api_stats'):
                    data_points = 1
                    notes = f"API Stats: {processed_channel.current_api_stats}"
        elif test_method == 'collect':
            results = collector.collect([channel])
            if results:
                processed_channel = results[0]
                success = processed_channel.processing_status == 'enriched'
                error_msg = processed_channel.processing_notes if not success else None
                data_points = 1
                if hasattr(processed_channel, 'current_metrics'):
                    notes = f"Metrics: subs={processed_channel.current_metrics.subscriber_count:,}, views={processed_channel.current_metrics.total_view_count:,}"
        
        duration = time.time() - start_time
        
        status = "PASS" if success else "FAIL"
        print(f'  Status: {status} ({duration:.2f}s)')
        
        if success:
            if data_points:
                print(f'  Data Points: {data_points}')
            if notes:
                print(f'  Notes: {notes}')
        else:
            print(f'  Error: {error_msg or "Unknown error"}')
        
        return {
            'success': success,
            'duration': duration,
            'data_points': data_points,
            'error': error_msg
        }
        
    except Exception as e:
        duration = time.time() - start_time
        print(f'  Status: FAIL ({duration:.2f}s)')
        print(f'  Exception: {str(e)}')
        return {
            'success': False,
            'duration': duration,
            'data_points': 0,
            'error': str(e)
        }

def main():
    print_header("COMPREHENSIVE 5-PHASE TESTING SUITE")
    print(f'Test Start Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    
    # Test channels with different characteristics
    test_channels = [
        # Popular, established channel with good data coverage
        Channel(channel_id='UCuAXFkgsw1L7xaCfnd5JJOw', channel_name='Rick Astley'),
        
        # Large channel for testing scalability
        Channel(channel_id='UCX6OQ3DkcsbYNE6H8uQQuVA', channel_name='MrBeast'),
        
        # Smaller channel for edge case testing
        Channel(channel_id='UC4QobU6STFB0P71PMvOGN5A', channel_name='penguinz0'),
    ]
    
    # Phase configurations
    phases = [
        {
            'name': 'Phase 1: YouTube API Initial',
            'collector_class': YouTubeAPICollector,
            'test_method': 'collect',
            'critical': True
        },
        {
            'name': 'Phase 2A: Wayback Charts Historical',
            'collector_class': WaybackChartsCollector,
            'test_method': 'collect_channel_data',
            'critical': True
        },
        {
            'name': 'Phase 2B: Wayback Uploads Historical',
            'collector_class': WaybackUploadsCollector,
            'test_method': 'collect_channel_data',
            'critical': True
        },
        {
            'name': 'Phase 3: ViewStats Current',
            'collector_class': ViewStatsCollector,
            'test_method': 'collect_channel_data',
            'critical': False  # ViewStats coverage may be limited
        },
        {
            'name': 'Phase 4: API Current & Handle Collection',
            'collector_class': APICurrentCollector,
            'test_method': 'collect_batch_data',
            'critical': True
        }
    ]
    
    overall_results = {}
    total_start_time = time.time()
    
    # Test each phase
    for phase_config in phases:
        print_section(f"TESTING {phase_config['name']}")
        
        phase_results = {
            'phase_name': phase_config['name'],
            'critical': phase_config['critical'],
            'channels_tested': 0,
            'channels_passed': 0,
            'total_duration': 0,
            'total_data_points': 0,
            'channel_results': []
        }
        
        try:
            # Initialize collector
            collector = phase_config['collector_class']()
            print(f"Initialized {phase_config['collector_class'].__name__}")
            
            # Test with each channel
            for channel in test_channels:
                # Ensure channel has necessary attributes from previous phases
                if phase_config['name'].startswith('Phase 3') or phase_config['name'].startswith('Phase 4'):
                    # These phases need handles, simulate Phase 4 results
                    if not hasattr(channel, 'handle'):
                        if channel.channel_name == 'Rick Astley':
                            channel.handle = '@rickastleyyt'
                        elif channel.channel_name == 'MrBeast':
                            channel.handle = '@MrBeast'
                        elif channel.channel_name == 'penguinz0':
                            channel.handle = '@penguinz0'
                
                result = test_phase_with_channel(
                    phase_config['name'],
                    collector,
                    channel,
                    phase_config['test_method']
                )
                
                phase_results['channels_tested'] += 1
                phase_results['total_duration'] += result['duration']
                phase_results['total_data_points'] += result['data_points']
                phase_results['channel_results'].append({
                    'channel': channel.channel_name,
                    'result': result
                })
                
                if result['success']:
                    phase_results['channels_passed'] += 1
            
            # Calculate phase success rate
            success_rate = (phase_results['channels_passed'] / phase_results['channels_tested']) * 100
            
            print(f'\n{phase_config["name"]} Summary:')
            print(f'  Channels Tested: {phase_results["channels_tested"]}')
            print(f'  Channels Passed: {phase_results["channels_passed"]}')
            print(f'  Success Rate: {success_rate:.1f}%')
            print(f'  Total Duration: {phase_results["total_duration"]:.2f}s')
            print(f'  Total Data Points: {phase_results["total_data_points"]}')
            
            # Determine if phase passes
            if phase_config['critical']:
                phase_passes = success_rate >= 66.7  # At least 2/3 channels must pass for critical phases
            else:
                phase_passes = success_rate >= 33.3  # At least 1/3 channels must pass for non-critical phases
            
            phase_results['passes'] = phase_passes
            phase_results['success_rate'] = success_rate
            
            overall_results[phase_config['name']] = phase_results
            
        except Exception as e:
            print(f'PHASE INITIALIZATION FAILED: {str(e)}')
            traceback.print_exc()
            phase_results['passes'] = False
            phase_results['success_rate'] = 0
            phase_results['error'] = str(e)
            overall_results[phase_config['name']] = phase_results
    
    # Overall summary
    total_duration = time.time() - total_start_time
    
    print_header("COMPREHENSIVE TEST RESULTS SUMMARY")
    
    phases_passed = sum(1 for phase in overall_results.values() if phase['passes'])
    total_phases = len(overall_results)
    
    print(f'Total Test Duration: {total_duration:.2f}s')
    print(f'Phases Passed: {phases_passed}/{total_phases}')
    print(f'Overall Success Rate: {(phases_passed/total_phases)*100:.1f}%')
    
    print('\nPhase-by-Phase Results:')
    for phase_name, phase_data in overall_results.items():
        status = "PASS" if phase_data['passes'] else "FAIL"
        critical_marker = "[CRITICAL]" if phase_data['critical'] else "[OPTIONAL]"
        print(f'  {phase_name:<35}: {status} ({phase_data["success_rate"]:.1f}% success, {phase_data["total_data_points"]} data points) {critical_marker}')
    
    # Data continuity analysis
    print('\nData Continuity Analysis:')
    has_historical = any(phase['passes'] and 'Historical' in phase['phase_name'] for phase in overall_results.values())
    has_current = any(phase['passes'] and ('Current' in phase['phase_name'] or 'API' in phase['phase_name']) for phase in overall_results.values())
    
    print(f'  Historical Data Coverage: {"YES" if has_historical else "NO"}')
    print(f'  Current Data Coverage: {"YES" if has_current else "NO"}')
    print(f'  End-to-End Data Pipeline: {"FUNCTIONAL" if has_historical and has_current else "INCOMPLETE"}')
    
    # Critical issues check
    critical_failures = [phase for phase in overall_results.values() if phase['critical'] and not phase['passes']]
    if critical_failures:
        print(f'\nCRITICAL ISSUES DETECTED:')
        for failure in critical_failures:
            print(f'  - {failure["phase_name"]}: {failure.get("error", "Low success rate")}')
    else:
        print(f'\nALL CRITICAL PHASES OPERATIONAL')
    
    print(f'\nTest Completed: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    
    return overall_results

if __name__ == "__main__":
    results = main()