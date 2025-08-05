#!/usr/bin/env python3
"""
Final Validation Test - All 5 Phases with Optimal Channels
"""

import time
from datetime import datetime
from src.data_collectors.youtube_api_collector import YouTubeAPICollector
from src.data_collectors.wayback_charts_collector import WaybackChartsCollector  
from src.data_collectors.wayback_uploads_collector import WaybackUploadsCollector
from src.data_collectors.viewstats_collector import ViewStatsCollector
from src.data_collectors.api_current_collector import APICurrentCollector
from src.models.channel import Channel

def run_complete_pipeline(channel):
    """Run complete 5-phase pipeline on a single channel"""
    print(f'\n=== PIPELINE TEST: {channel.channel_name} ({channel.channel_id}) ===')
    
    results = {
        'channel': channel,
        'phases': {},
        'total_duration': 0,
        'total_data_points': 0,
        'phases_passed': 0
    }
    
    pipeline_start = time.time()
    
    # Phase 1: YouTube API Initial
    print('\nPhase 1: YouTube API Initial Enrichment')
    try:
        phase1_start = time.time()
        youtube_collector = YouTubeAPICollector()
        enriched_channels = youtube_collector.collect([channel])
        
        if enriched_channels:
            channel = enriched_channels[0]  # Update channel with API data
            phase1_success = channel.processing_status == 'enriched'
            phase1_duration = time.time() - phase1_start
            
            if phase1_success:
                print(f'  ✓ SUCCESS ({phase1_duration:.2f}s): {channel.current_metrics.subscriber_count:,} subs, {channel.current_metrics.total_view_count:,} views')
                results['phases']['phase1'] = {'success': True, 'duration': phase1_duration, 'data_points': 1}
                results['phases_passed'] += 1
            else:
                print(f'  ✗ FAILED ({phase1_duration:.2f}s): {channel.processing_notes}')
                results['phases']['phase1'] = {'success': False, 'duration': phase1_duration, 'error': channel.processing_notes}
        else:
            print('  ✗ FAILED: No channels returned')
            results['phases']['phase1'] = {'success': False, 'duration': time.time() - phase1_start, 'error': 'No channels returned'}
    except Exception as e:
        print(f'  ✗ FAILED: {e}')
        results['phases']['phase1'] = {'success': False, 'duration': time.time() - phase1_start, 'error': str(e)}
    
    # Phase 2A: Wayback Charts
    print('\nPhase 2A: Wayback Charts Historical Data')
    try:
        phase2a_start = time.time()
        charts_collector = WaybackChartsCollector()
        charts_result = charts_collector.collect_channel_data(channel)
        phase2a_duration = time.time() - phase2a_start
        
        if charts_result['success']:
            data_points = charts_result['data_points_collected']
            print(f'  ✓ SUCCESS ({phase2a_duration:.2f}s): {data_points} historical data points')
            results['phases']['phase2a'] = {'success': True, 'duration': phase2a_duration, 'data_points': data_points}
            results['phases_passed'] += 1
            results['total_data_points'] += data_points
        else:
            print(f'  ✗ FAILED ({phase2a_duration:.2f}s): {charts_result["error"]}')
            results['phases']['phase2a'] = {'success': False, 'duration': phase2a_duration, 'error': charts_result['error']}
    except Exception as e:
        print(f'  ✗ FAILED: {e}')
        results['phases']['phase2a'] = {'success': False, 'duration': time.time() - phase2a_start, 'error': str(e)}
    
    # Phase 2B: Wayback Uploads
    print('\nPhase 2B: Wayback Uploads Historical Data')
    try:
        phase2b_start = time.time()
        uploads_collector = WaybackUploadsCollector()
        uploads_result = uploads_collector.collect_channel_data(channel)
        phase2b_duration = time.time() - phase2b_start
        
        if uploads_result['success']:
            data_points = uploads_result['data_points_collected']
            interpolation_applied = uploads_result.get('current_interpolation_applied', False)
            interpolation_note = ' (with current API interpolation)' if interpolation_applied else ''
            
            print(f'  ✓ SUCCESS ({phase2b_duration:.2f}s): {data_points} upload data points{interpolation_note}')
            results['phases']['phase2b'] = {'success': True, 'duration': phase2b_duration, 'data_points': data_points, 'interpolation': interpolation_applied}
            results['phases_passed'] += 1
            results['total_data_points'] += data_points
        else:
            print(f'  ✗ FAILED ({phase2b_duration:.2f}s): {uploads_result["error"]}')
            results['phases']['phase2b'] = {'success': False, 'duration': phase2b_duration, 'error': uploads_result['error']}
    except Exception as e:
        print(f'  ✗ FAILED: {e}')
        results['phases']['phase2b'] = {'success': False, 'duration': time.time() - phase2b_start, 'error': str(e)}
    
    # Phase 4: API Current (run before Phase 3 for handle collection)
    print('\nPhase 4: API Current & Handle Collection')
    try:
        phase4_start = time.time()
        api_collector = APICurrentCollector()
        current_channels = api_collector.collect_batch_data([channel])
        phase4_duration = time.time() - phase4_start
        
        if current_channels and current_channels[0].processing_status == 'enriched':
            channel = current_channels[0]  # Update with current API data
            handle = getattr(channel, 'handle', 'N/A')
            video_count = channel.current_api_stats.get('video_count', 0)
            
            print(f'  ✓ SUCCESS ({phase4_duration:.2f}s): Handle {handle}, {video_count} videos')
            results['phases']['phase4'] = {'success': True, 'duration': phase4_duration, 'data_points': 1}
            results['phases_passed'] += 1
        else:
            error = current_channels[0].processing_notes if current_channels else 'No channels returned'
            print(f'  ✗ FAILED ({phase4_duration:.2f}s): {error}')
            results['phases']['phase4'] = {'success': False, 'duration': phase4_duration, 'error': error}
    except Exception as e:
        print(f'  ✗ FAILED: {e}')
        results['phases']['phase4'] = {'success': False, 'duration': time.time() - phase4_start, 'error': str(e)}
    
    # Phase 3: ViewStats Current
    print('\nPhase 3: ViewStats Current Data Collection')
    try:
        phase3_start = time.time()
        viewstats_collector = ViewStatsCollector()
        viewstats_result = viewstats_collector.collect_channel_data(channel)
        phase3_duration = time.time() - phase3_start
        
        if viewstats_result['success']:
            has_breakdown = viewstats_result.get('has_breakdown', False)
            current_data = viewstats_result.get('current_data', {})
            
            views = current_data.get('total_views', 'N/A')
            subs = current_data.get('subscriber_count', 'N/A')
            breakdown_note = ' (with longform/shorts breakdown)' if has_breakdown else ''
            
            print(f'  ✓ SUCCESS ({phase3_duration:.2f}s): Views: {views}, Subscribers: {subs}{breakdown_note}')
            results['phases']['phase3'] = {'success': True, 'duration': phase3_duration, 'data_points': 1, 'has_breakdown': has_breakdown}
            results['phases_passed'] += 1
        else:
            print(f'  ✗ FAILED ({phase3_duration:.2f}s): {viewstats_result["error"]}')
            results['phases']['phase3'] = {'success': False, 'duration': phase3_duration, 'error': viewstats_result['error']}
    except Exception as e:
        print(f'  ✗ FAILED: {e}')
        results['phases']['phase3'] = {'success': False, 'duration': time.time() - phase3_start, 'error': str(e)}
    
    # Pipeline Summary
    results['total_duration'] = time.time() - pipeline_start
    success_rate = (results['phases_passed'] / 5) * 100
    
    print(f'\n--- PIPELINE SUMMARY ---')
    print(f'Phases Passed: {results["phases_passed"]}/5 ({success_rate:.1f}%)')
    print(f'Total Duration: {results["total_duration"]:.2f}s')
    print(f'Total Data Points: {results["total_data_points"]}')
    
    return results

def main():
    print('=' * 80)
    print('FINAL VALIDATION TEST - COMPREHENSIVE 5-PHASE PIPELINE'.center(80))
    print('=' * 80)
    print(f'Test Start: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    
    # Test with channels known to have good historical coverage
    validation_channels = [
        Channel(channel_id='UCuAXFkgsw1L7xaCfnd5JJOw', channel_name='Rick Astley'),  # Excellent coverage
        Channel(channel_id='UCX6OQ3DkcsbYNE6H8uQQuVA', channel_name='MrBeast'),      # Popular, good coverage
    ]
    
    all_results = []
    
    for channel in validation_channels:
        result = run_complete_pipeline(channel)
        all_results.append(result)
    
    # Overall Analysis
    print('\n' + '=' * 80)
    print('FINAL VALIDATION RESULTS'.center(80))
    print('=' * 80)
    
    total_phases_tested = len(all_results) * 5
    total_phases_passed = sum(r['phases_passed'] for r in all_results)
    overall_success_rate = (total_phases_passed / total_phases_tested) * 100
    
    print(f'Channels Tested: {len(all_results)}')
    print(f'Total Phase Tests: {total_phases_tested}')
    print(f'Total Phase Passes: {total_phases_passed}')
    print(f'Overall Success Rate: {overall_success_rate:.1f}%')
    
    print(f'\nChannel Results:')
    for result in all_results:
        channel_success = (result['phases_passed'] / 5) * 100
        print(f'  {result["channel"].channel_name}: {result["phases_passed"]}/5 phases ({channel_success:.1f}%) - {result["total_data_points"]} data points')
    
    # Phase-by-phase analysis
    phase_names = ['phase1', 'phase2a', 'phase2b', 'phase3', 'phase4']
    phase_labels = [
        'Phase 1: YouTube API Initial',
        'Phase 2A: Wayback Charts Historical', 
        'Phase 2B: Wayback Uploads Historical',
        'Phase 3: ViewStats Current',
        'Phase 4: API Current & Handle Collection'
    ]
    
    print(f'\nPhase Success Rates:')
    for i, phase_name in enumerate(phase_names):
        phase_passes = sum(1 for r in all_results if r['phases'].get(phase_name, {}).get('success', False))
        phase_rate = (phase_passes / len(all_results)) * 100
        status = "EXCELLENT" if phase_rate >= 90 else "GOOD" if phase_rate >= 75 else "ACCEPTABLE" if phase_rate >= 50 else "NEEDS ATTENTION"
        print(f'  {phase_labels[i]:<40}: {phase_passes}/{len(all_results)} ({phase_rate:.1f}%) - {status}')
    
    # Data continuity check
    print(f'\nData Continuity Validation:')
    for result in all_results:
        channel_name = result['channel'].channel_name
        has_historical = result['phases'].get('phase2a', {}).get('success', False) or result['phases'].get('phase2b', {}).get('success', False)
        has_current = result['phases'].get('phase3', {}).get('success', False) or result['phases'].get('phase4', {}).get('success', False)
        has_interpolation = result['phases'].get('phase2b', {}).get('interpolation', False)
        
        print(f'  {channel_name}:')
        print(f'    Historical Data: {"✓" if has_historical else "✗"}')
        print(f'    Current Data: {"✓" if has_current else "✗"}')
        print(f'    Upload Interpolation: {"✓" if has_interpolation else "✗"}')
        print(f'    End-to-End Coverage: {"✓ COMPLETE" if has_historical and has_current else "✗ INCOMPLETE"}')
    
    print(f'\n' + '=' * 80)
    if overall_success_rate >= 80:
        print('VALIDATION STATUS: PIPELINE FULLY OPERATIONAL'.center(80))
    elif overall_success_rate >= 60:
        print('VALIDATION STATUS: PIPELINE MOSTLY FUNCTIONAL'.center(80))
    else:
        print('VALIDATION STATUS: PIPELINE NEEDS IMPROVEMENT'.center(80))
    print('=' * 80)
    
    return all_results

if __name__ == "__main__":
    results = main()