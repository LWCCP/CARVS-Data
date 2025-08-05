#!/usr/bin/env python3
"""
YouNiverse Dataset Enrichment - Complete 5-Phase Pipeline Test
End-to-end test of all data collection phases
"""

import time
from datetime import datetime
from src.data_collectors.youtube_api_collector import YouTubeAPICollector
from src.data_collectors.wayback_charts_collector import WaybackChartsCollector  
from src.data_collectors.wayback_uploads_collector import WaybackUploadsCollector
from src.data_collectors.viewstats_collector import ViewStatsCollector
from src.data_collectors.api_current_collector import APICurrentCollector
from src.models.channel import Channel

print('=' * 80)
print('YouNiverse Dataset Enrichment - Complete 5-Phase Pipeline Test')
print('=' * 80)

# Test channel: Rick Astley (good historical data, less complex than MrBeast)
test_channel = Channel(
    channel_id='UCuAXFkgsw1L7xaCfnd5JJOw',
    channel_name='Rick Astley'
)

print(f'\nTest Channel: {test_channel.channel_name} ({test_channel.channel_id})')
print(f'Pipeline Start Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

results = {
    'channel': test_channel,
    'phase_results': {},
    'pipeline_start': time.time(),
    'total_data_points': 0
}

try:
    # Phase 1: YouTube API Initial Enrichment
    print('\n' + '='*50)
    print('PHASE 1: YouTube API Initial Enrichment')
    print('='*50)
    
    phase1_start = time.time()
    
    try:
        youtube_collector = YouTubeAPICollector()
        
        # Process single channel for testing
        enriched_channels = youtube_collector.collect([test_channel])
        
        if enriched_channels:
            test_channel = enriched_channels[0]
            phase1_duration = time.time() - phase1_start
            
            results['phase_results']['phase1'] = {
                'success': test_channel.processing_status == 'enriched',
                'duration': phase1_duration,
                'notes': test_channel.processing_notes
            }
            
            print(f'Status: {test_channel.processing_status}')
            print(f'Duration: {phase1_duration:.2f}s')
            print(f'Channel Name: {getattr(test_channel, "channel_name", "N/A")}')
            
            if hasattr(test_channel, 'current_metrics') and test_channel.current_metrics:
                print(f'Subscribers: {test_channel.current_metrics.subscriber_count:,}')
                print(f'Total Views: {test_channel.current_metrics.total_view_count:,}')
        else:
            phase1_duration = time.time() - phase1_start
            results['phase_results']['phase1'] = {
                'success': False,
                'duration': phase1_duration,
                'notes': 'No channels returned from YouTube API'
            }
            print(f'Status: Failed - No channels returned')
            print(f'Duration: {phase1_duration:.2f}s')
            
    except Exception as e:
        phase1_duration = time.time() - phase1_start
        results['phase_results']['phase1'] = {
            'success': False,
            'duration': phase1_duration,
            'notes': f'YouTube API Error: {str(e)}'
        }
        print(f'Status: Failed - {str(e)}')
        print(f'Duration: {phase1_duration:.2f}s')
    
    # Phase 2A: Wayback Charts Historical Data (Sep 2019 - Dec 2022)
    print('\n' + '='*50)
    print('PHASE 2A: Wayback Charts Historical Data')
    print('='*50)
    
    phase2a_start = time.time()
    charts_collector = WaybackChartsCollector()
    charts_result = charts_collector.collect_channel_data(test_channel)
    
    phase2a_duration = time.time() - phase2a_start
    results['phase_results']['phase2a'] = {
        'success': charts_result['success'],
        'duration': phase2a_duration,
        'data_points': charts_result['data_points_collected'],
        'snapshots_used': len(charts_result.get('snapshots_used', []))
    }
    
    print(f'Status: {"Success" if charts_result["success"] else "Failed"}')
    print(f'Duration: {phase2a_duration:.2f}s')
    print(f'Data Points: {charts_result["data_points_collected"]}')
    print(f'Snapshots Used: {len(charts_result.get("snapshots_used", []))}')
    
    if charts_result['error']:
        print(f'Error: {charts_result["error"]}')
    
    if charts_result['success'] and charts_result.get('historical_data'):
        chart_data = charts_result['historical_data']
        results['total_data_points'] += len(chart_data)
        sample_data = chart_data[:3]
        print('Sample data points:')
        for i, point in enumerate(sample_data):
            date_str = point['date'][:10] if 'date' in point else 'N/A'
            subs = point.get('subscriber_count', 'N/A')
            views = point.get('view_count', 'N/A')
            if isinstance(subs, int) and isinstance(views, int):
                print(f'  {i+1}. {date_str}: {subs:,} subs, {views:,} views')
            else:
                print(f'  {i+1}. {date_str}: {subs} subs, {views} views')

    # Phase 2B: Wayback Uploads Historical Data  
    print('\n' + '='*50)
    print('PHASE 2B: Wayback Uploads Historical Data (with Interpolation)')
    print('='*50)
    
    phase2b_start = time.time()
    uploads_collector = WaybackUploadsCollector()
    uploads_result = uploads_collector.collect_channel_data(test_channel)
    
    phase2b_duration = time.time() - phase2b_start
    results['phase_results']['phase2b'] = {
        'success': uploads_result['success'],
        'duration': phase2b_duration,
        'data_points': uploads_result['data_points_collected'],
        'snapshots_used': len(uploads_result.get('snapshots_used', []))
    }
    
    print(f'Status: {"Success" if uploads_result["success"] else "Failed"}')
    print(f'Duration: {phase2b_duration:.2f}s')
    print(f'Data Points (with interpolation): {uploads_result["data_points_collected"]}')
    print(f'Original Snapshots: {len(uploads_result.get("snapshots_used", []))}')
    
    if uploads_result['error']:
        print(f'Error: {uploads_result["error"]}')
    
    if uploads_result['success'] and uploads_result['upload_data']:
        results['total_data_points'] += len(uploads_result['upload_data'])
        
        # Count actual vs interpolated points
        actual_points = sum(1 for p in uploads_result['upload_data'] if not p.get('interpolated', False))
        interpolated_points = len(uploads_result['upload_data']) - actual_points
        
        print(f'Actual snapshots: {actual_points}, Interpolated: {interpolated_points}')
        
        sample_data = uploads_result['upload_data'][:5]
        print('Sample upload progression:')
        for i, point in enumerate(sample_data):
            date_str = point['date'][:10]
            uploads = point.get('upload_count', 'N/A')
            marker = '[INTERPOLATED]' if point.get('interpolated', False) else '[ACTUAL]'
            print(f'  {i+1}. {date_str}: {uploads} uploads {marker}')

    # Phase 4: API Current Data (moved before Phase 3 for handle collection)
    print('\n' + '='*50)
    print('PHASE 4: API Current Data & Handle Collection')
    print('='*50)
    
    phase4_start = time.time()
    api_current_collector = APICurrentCollector()
    current_channels = api_current_collector.collect_batch_data([test_channel])
    
    phase4_duration = time.time() - phase4_start
    
    if current_channels:
        test_channel = current_channels[0]
        results['phase_results']['phase4'] = {
            'success': test_channel.processing_status == 'enriched',
            'duration': phase4_duration,
            'has_handle': bool(getattr(test_channel, 'handle', None))
        }
        
        print(f'Status: {test_channel.processing_status}')
        print(f'Duration: {phase4_duration:.2f}s')
        print(f'Handle: {getattr(test_channel, "handle", "N/A")}')
        print(f'Custom URL: {getattr(test_channel, "custom_url", "N/A")}')
        
        if hasattr(test_channel, 'current_api_stats'):
            stats = test_channel.current_api_stats
            print(f'Current API Stats:')
            print(f'  - Subscribers: {stats.get("subscriber_count", 0):,}')
            print(f'  - Views: {stats.get("view_count", 0):,}')
            print(f'  - Videos: {stats.get("video_count", 0):,}')

    # Phase 3: ViewStats Current Data (now using handles from Phase 4)
    print('\n' + '='*50) 
    print('PHASE 3: ViewStats Current Data Collection')
    print('='*50)
    
    phase3_start = time.time()
    viewstats_collector = ViewStatsCollector()
    viewstats_result = viewstats_collector.collect_channel_data(test_channel)
    
    phase3_duration = time.time() - phase3_start
    results['phase_results']['phase3'] = {
        'success': viewstats_result['success'],
        'duration': phase3_duration,
        'has_breakdown': viewstats_result.get('has_breakdown', False),
        'url_used': viewstats_result.get('successful_url', None)
    }
    
    print(f'Status: {"Success" if viewstats_result["success"] else "Failed"}')  
    print(f'Duration: {phase3_duration:.2f}s')
    
    if viewstats_result['successful_url']:
        print(f'Working URL: {viewstats_result["successful_url"]}')
    
    if viewstats_result['current_data']:
        data = viewstats_result['current_data']
        print(f'ViewStats Data:')
        views = data.get("total_views", "N/A")
        subs = data.get("subscriber_count", "N/A")
        if isinstance(views, int):
            print(f'  - Views: {views:,}')
        else:
            print(f'  - Views: {views}')
        if isinstance(subs, int):
            print(f'  - Subscribers: {subs:,}')
        else:
            print(f'  - Subscribers: {subs}')
        if 'longform_views' in data:
            print(f'  - Longform: {data["longform_views"]:,}')
        if 'shorts_views' in data:
            print(f'  - Shorts: {data["shorts_views"]:,}')
        print(f'  - Has Breakdown: {viewstats_result["has_breakdown"]}')
    
    if viewstats_result['error']:
        print(f'Error: {viewstats_result["error"]}')

    # Pipeline Summary
    print('\n' + '='*80)
    print('PIPELINE COMPLETION SUMMARY')
    print('='*80)
    
    total_duration = time.time() - results['pipeline_start']
    
    print(f'Total Pipeline Duration: {total_duration:.2f}s')
    print(f'Total Data Points Collected: {results["total_data_points"]:,}')
    
    success_count = sum(1 for phase in results['phase_results'].values() if phase.get('success', False))
    print(f'Successful Phases: {success_count}/5')
    
    print('\nPhase-by-Phase Results:')
    phase_names = {
        'phase1': 'YouTube API Initial',
        'phase2a': 'Wayback Charts', 
        'phase2b': 'Wayback Uploads',
        'phase3': 'ViewStats Current',
        'phase4': 'API Current'
    }
    
    for phase_id, phase_data in results['phase_results'].items():
        status = 'PASS' if phase_data.get('success', False) else 'FAIL'
        duration = phase_data.get('duration', 0)
        phase_name = phase_names.get(phase_id, phase_id)
        print(f'  {phase_name:<20}: {status} ({duration:.2f}s)')
        
        if 'data_points' in phase_data:
            print(f'    Data Points: {phase_data["data_points"]:,}')
    
    print(f'\nFinal Channel Status: {test_channel.processing_status}')
    print(f'Collection Sources: {len(getattr(test_channel, "collection_sources", {}))} sources')
    
    # Data continuity check
    print('\nData Continuity Analysis:')
    has_historical = results['phase_results']['phase2a'].get('success', False) or results['phase_results']['phase2b'].get('success', False)
    has_current = results['phase_results']['phase3'].get('success', False) or results['phase_results']['phase4'].get('success', False)
    
    print(f'  - Historical Coverage (2019-2022): {"YES" if has_historical else "NO"}')
    print(f'  - Current Coverage (2022-Present): {"YES" if has_current else "NO"}')
    print(f'  - End-to-End Coverage: {"YES" if has_historical and has_current else "NO"}')

except Exception as e:
    print(f'\nPIPELINE ERROR: {e}')
    import traceback
    traceback.print_exc()

print('\n' + '='*80)
print('5-Phase Pipeline Test Complete')
print('='*80)