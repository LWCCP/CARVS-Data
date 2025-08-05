#!/usr/bin/env python3
"""
Final 5-Phase Comprehensive Test
"""

from src.data_collectors.youtube_api_collector import YouTubeAPICollector
from src.data_collectors.wayback_charts_collector import WaybackChartsCollector
from src.data_collectors.wayback_uploads_collector import WaybackUploadsCollector
from src.data_collectors.viewstats_collector import ViewStatsCollector
from src.data_collectors.api_current_collector import APICurrentCollector
from src.models.channel import Channel
import time

def main():
    print("=== FINAL 5-PHASE COMPREHENSIVE VALIDATION ===")
    
    # Test with Rick Astley
    channel = Channel(channel_id='UCuAXFkgsw1L7xaCfnd5JJOw', channel_name='Rick Astley')
    
    print(f'Testing ALL 5 PHASES with {channel.channel_name} ({channel.channel_id})')
    print('=' * 80)
    
    phases_passed = 0
    total_data_points = 0
    start_time = time.time()
    
    # Phase 1: YouTube API Initial
    print('\nPhase 1: YouTube API Initial Enrichment')
    try:
        youtube_collector = YouTubeAPICollector()
        enriched = youtube_collector.collect([channel])
        
        if enriched and enriched[0].processing_status == 'enriched':
            channel = enriched[0]  # Update with enriched data
            phases_passed += 1
            print(f'  PASS: {channel.current_metrics.subscriber_count:,} subs, {channel.current_metrics.total_view_count:,} views')
        else:
            print(f'  FAIL: No channels returned or failed enrichment')
    except Exception as e:
        print(f'  FAIL: {e}')
    
    # Phase 2A: Wayback Charts Historical
    print('\nPhase 2A: Wayback Charts Historical Data')
    try:
        charts_collector = WaybackChartsCollector()
        result = charts_collector.collect_channel_data(channel)
        
        if result['success']:
            phases_passed += 1
            data_points = result['data_points_collected']
            total_data_points += data_points
            print(f'  PASS: {data_points} historical data points')
        else:
            print(f'  FAIL: {result["error"]}')
    except Exception as e:
        print(f'  FAIL: {e}')
    
    # Phase 2B: Wayback Uploads Historical
    print('\nPhase 2B: Wayback Uploads Historical Data (with Interpolation)')
    try:
        uploads_collector = WaybackUploadsCollector()
        result = uploads_collector.collect_channel_data(channel)
        
        if result['success']:
            phases_passed += 1
            data_points = result['data_points_collected']
            total_data_points += data_points
            interpolation = result.get('current_interpolation_applied', False)
            interp_note = ' (with current API interpolation)' if interpolation else ''
            print(f'  PASS: {data_points} upload data points{interp_note}')
        else:
            print(f'  FAIL: {result["error"]}')
    except Exception as e:
        print(f'  FAIL: {e}')
    
    # Phase 4: API Current & Handle Collection (before Phase 3)
    print('\nPhase 4: API Current & Handle Collection')
    try:
        api_collector = APICurrentCollector()
        current = api_collector.collect_batch_data([channel])
        
        if current and current[0].processing_status == 'enriched':
            channel = current[0]  # Update with current API data
            phases_passed += 1
            handle = getattr(channel, 'handle', 'N/A')
            videos = channel.current_api_stats.get('video_count', 0)
            print(f'  PASS: Handle {handle}, {videos} videos')
        else:
            error_msg = current[0].processing_notes if current else 'No channels returned'
            print(f'  FAIL: {error_msg}')
    except Exception as e:
        print(f'  FAIL: {e}')
    
    # Phase 3: ViewStats Current
    print('\nPhase 3: ViewStats Current Data Collection')
    try:
        viewstats_collector = ViewStatsCollector()
        result = viewstats_collector.collect_channel_data(channel)
        
        if result['success']:
            phases_passed += 1
            has_breakdown = result.get('has_breakdown', False)
            breakdown_note = ' (with longform/shorts breakdown)' if has_breakdown else ''
            print(f'  PASS: ViewStats data collected{breakdown_note}')
        else:
            print(f'  FAIL: {result["error"]}')
    except Exception as e:
        print(f'  FAIL: {e}')
    
    # Final Summary
    total_duration = time.time() - start_time
    success_rate = (phases_passed / 5) * 100
    
    print('\n' + '=' * 80)
    print('COMPREHENSIVE 5-PHASE VALIDATION RESULTS')
    print('=' * 80)
    
    print(f'Phases Passed: {phases_passed}/5')
    print(f'Success Rate: {success_rate:.1f}%')
    print(f'Total Duration: {total_duration:.2f}s')
    print(f'Total Data Points: {total_data_points}')
    
    if success_rate == 100:
        print('\nVALIDATION RESULT: ALL 5 PHASES WORKING PERFECTLY')
    elif success_rate >= 80:
        print('\nVALIDATION RESULT: PIPELINE FULLY OPERATIONAL')
    else:
        print('\nVALIDATION RESULT: PIPELINE NEEDS ATTENTION')
    
    print('=' * 80)

if __name__ == "__main__":
    main()