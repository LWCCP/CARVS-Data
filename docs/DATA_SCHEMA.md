# Data Schema Documentation

## Overview
This document defines the comprehensive data schema for the YouNiverse Dataset Enrichment project, supporting transformation from 18.8M records to a 10-year comprehensive dataset with enhanced analytics capabilities.

## Current Schema (Preserved)

The existing dataset `df_timeseries_en.tsv` contains 18,872,499 records across 153,550 channels with weekly granularity from January 2015 to September 2019.

### Base Schema Fields
```
channel | category | datetime | views | delta_views | subs | delta_subs | 
videos | delta_videos | activity
```

**Field Definitions:**
- **channel**: Unique channel identifier (YouTube channel ID)
- **category**: Content category classification
- **datetime**: Weekly timestamp (ISO 8601 format, Sunday-to-Sunday intervals)
- **views**: Total view count at timestamp
- **delta_views**: Weekly change in view count
- **subs**: Total subscriber count at timestamp
- **delta_subs**: Weekly change in subscriber count
- **videos**: Total video count at timestamp
- **delta_videos**: Weekly change in video count
- **activity**: Activity status indicator

## Enhanced Schema (Target)

The enriched dataset will extend the base schema with additional fields to support comprehensive 10-year analysis and data quality assurance.

### Enhanced Schema Fields
```
channel | category | datetime | views | delta_views | subs | delta_subs | 
videos | delta_videos | activity | total_longform_views | total_shorts_views |
channel_name | collection_source | data_quality_flag
```

**New Field Definitions:**
- **total_longform_views**: Total views from longform content (>60 seconds)
- **total_shorts_views**: Total views from YouTube Shorts content (<60 seconds)
- **channel_name**: Current channel display name (from YouTube API)
- **collection_source**: Data source identifier (API|WAYBACK_CHARTS|WAYBACK_UPLOADS|VIEWSTATS|FINAL_API)
- **data_quality_flag**: Quality assessment flag (excellent|good|fair|poor|missing)

## Data Sources and Collection Phases

The YouNiverse Dataset Enrichment employs a sophisticated 5-phase data collection and processing pipeline designed to create a comprehensive 10-year dataset while maintaining data integrity and quality at every stage.

### Phase 1: YouTube API Current Metadata Enhancement
- **Source**: YouTube Data API v3
- **Coverage**: Current channel metadata for all 153,550 channels
- **Processing Approach**: Batch requests with rate limiting (100 channels per request)
- **Fields Enhanced**: 
  - channel_name (current display name)
  - updated category classification
  - channel_handle (@username format)
  - current subscriber/view counts for validation baseline
- **Quality Assurance**: Highest reliability for current data
- **Data Quality Flag**: `excellent` for successful API responses
- **Error Handling**: Inactive/private channels flagged with `missing` quality flag
- **Performance**: ~1,536 API requests, ~15 minutes processing time

### Phase 2A: Wayback Machine Charts Data Collection (Sep 2019 - Aug 2022)
- **Source**: Internet Archive Wayback Machine + Social Blade chart pages
- **Coverage**: Historical subscriber/view progression for 3-year gap period
- **Processing Approach**: 
  - Weekly timestamp generation (Sunday-to-Sunday intervals)
  - Parallel processing of 10 channels per worker thread
  - Checkpoint management every 1,000 channels
- **Target Volume**: 165 weeks × 153,550 channels = 25,335,750 records
- **Data Extraction Strategy**:
  - CDX API queries for snapshot discovery
  - HTML parsing of Social Blade chart data
  - Temporal interpolation for missing weeks
  - Delta calculation based on previous known values
- **Quality Assurance**: Good reliability with statistical validation
- **Data Quality Flags**: 
  - `good` for clean extracted data
  - `fair` for interpolated periods
  - `poor` for snapshots with parsing issues
- **Junction Point Integration**: Sep 2019 junction validation with existing dataset
- **Performance**: ~4-6 hours processing time with 16 parallel workers

### Phase 2B: Wayback Machine Uploads Data Enhancement
- **Source**: Internet Archive Wayback Machine + Social Blade upload pages
- **Coverage**: Video count accuracy and upload timeline refinement
- **Processing Approach**: 
  - Selective enhancement of Phase 2A records
  - Upload date extraction and video count validation
  - Activity status determination based on upload patterns
- **Target Volume**: Variable based on available upload page snapshots
- **Enhancement Strategy**:
  - Video count cross-validation against chart data
  - Upload frequency analysis for activity classification
  - Delta_videos recalculation based on actual upload dates
- **Quality Assurance**: Fair reliability, supplementary data validation
- **Data Quality Flags**:
  - Records upgraded from `wayback_charts` to `wayback_uploads` source
  - Activity status enhanced: `active`, `inactive`, `dormant` classifications
- **Performance**: ~2-3 hours processing time (enhancement of existing records)

### Phase 3: ViewStats Current Data Collection (Aug 2022 - Present)
- **Source**: ViewStats.org API and web scraping
- **Coverage**: Recent historical data bridging to current timeline
- **Processing Approach**:
  - Weekly data collection from Aug 2022 to current week
  - API-first strategy with web scraping fallback
  - Real-time data validation against YouTube API
- **Target Volume**: Dynamic based on execution date (~25-35 weeks initially)
- **Collection Strategy**:
  - Batch requests for efficiency (50 channels per request)
  - Anti-bot detection avoidance with request spacing
  - Data freshness validation (max 48-hour lag tolerance)
- **Quality Assurance**: Good reliability for recent data
- **Data Quality Flags**:
  - `good` for fresh API data
  - `fair` for scraped data
  - `poor` for stale data (>48 hours old)
- **Junction Point Integration**: Aug 2022 junction validation with Wayback data
- **Performance**: ~30-45 minutes per collection cycle

### Phase 4: Final API Update and Current Week Refresh
- **Source**: YouTube Data API v3 (final authoritative update)
- **Coverage**: Current week data validation and final metrics
- **Processing Approach**:
  - Final validation of most recent records
  - Current subscriber/view count verification
  - Enhanced metrics collection (longform vs shorts views)
- **Target Volume**: 153,550 channels (current week records)
- **Validation Strategy**:
  - Cross-reference with Phase 3 data for consistency
  - Statistical anomaly detection for unusual changes
  - Quality flag upgrade for validated current data
- **Quality Assurance**: Highest reliability for current week
- **Data Quality Flags**: Records upgraded to `excellent` with `FINAL_API` source
- **Enhanced Metrics Collection**:
  - total_longform_views and total_shorts_views differentiation
  - Real-time subscriber/view count validation
- **Performance**: ~20-30 minutes processing time

### Phase 5: Comprehensive Data Integration and Quality Assessment
- **Source**: All collected phases integrated into unified timeline
- **Coverage**: Complete 10-year dataset with quality validation
- **Processing Approach**:
  - Temporal sequence validation across all phases
  - Junction point continuity verification
  - Statistical progression analysis
  - Final quality score calculation
- **Integration Strategy**:
  - Weekly granularity enforcement (normalize to Sunday boundaries)
  - Duplicate detection and resolution (highest quality source wins)
  - Delta recalculation for complete timeline consistency
  - Cross-source validation and flag assignment
- **Quality Assurance**: Comprehensive validation framework
- **Final Dataset Characteristics**:
  - Complete 10-year timeline: Jan 2015 - Current
  - Weekly granularity: ~520 weeks per channel
  - Enhanced schema: 15 total fields per record
  - Quality flags: Excellent (40%), Good (35%), Fair (20%), Poor (4%), Missing (1%)
- **Performance**: ~1-2 hours for final integration and validation

## Junction Points and Data Validation

Junction points represent critical transitions between data sources where continuity validation is essential to maintain dataset integrity throughout the 10-year timeline.

### Critical Junction Points

#### 1. September 2019 Junction: Original Dataset → Wayback Machine Transition
- **Timeline**: Existing dataset ends ~September 1, 2019 / Wayback collection begins September 1, 2019
- **Data Sources**: df_timeseries_en.tsv → Internet Archive Wayback Machine
- **Validation Window**: August 15, 2019 - September 15, 2019 (4-week overlap window)
- **Critical Metrics**: subscriber_count, total_view_count, video_count progression
- **Expected Behavior**: Smooth statistical progression with minor source-based variance
- **Tolerance Thresholds**: 
  - Subscriber count: ±15% variance tolerance
  - View count: ±15% variance tolerance  
  - Video count: ±5% variance tolerance (more stable metric)
- **Validation Methodology**:
  - Statistical continuity analysis using linear regression
  - Z-score anomaly detection (threshold: 2.5σ)
  - Growth rate consistency validation
  - Temporal sequence integrity checks
- **Quality Flags Assignment**:
  - `excellent`: Perfect continuity, <5% variance
  - `good`: Minor discontinuity, 5-10% variance
  - `fair`: Moderate discontinuity, 10-15% variance
  - `poor`: Significant discontinuity, >15% variance
- **Failure Handling**: Channels with >15% variance flagged for manual review

#### 2. August 2022 Junction: Wayback Machine → ViewStats Transition  
- **Timeline**: Wayback collection ends ~August 1, 2022 / ViewStats collection begins August 1, 2022
- **Data Sources**: Internet Archive Wayback Machine → ViewStats.org API
- **Validation Window**: July 15, 2022 - August 15, 2022 (4-week overlap window)
- **Critical Metrics**: subscriber_count, total_view_count, video_count, enhanced view metrics
- **Expected Behavior**: Higher precision transition with tighter variance tolerance
- **Tolerance Thresholds**:
  - Subscriber count: ±10% variance tolerance (tighter due to more recent data)
  - View count: ±10% variance tolerance
  - Video count: ±3% variance tolerance
  - Enhanced metrics: ±20% variance (new data categories)
- **Validation Methodology**:
  - Enhanced statistical validation with 3-source cross-referencing
  - Temporal interpolation validation
  - Growth trajectory consistency analysis
  - Enhanced metrics introduction validation
- **Quality Flags Assignment**:
  - `excellent`: Perfect continuity, <3% variance
  - `good`: Minor discontinuity, 3-7% variance  
  - `fair`: Moderate discontinuity, 7-10% variance
  - `poor`: Significant discontinuity, >10% variance
- **Enhanced Metrics Introduction**: Validation of longform vs shorts view breakdown

#### 3. ViewStats → Final API Junction (Current Week Validation)
- **Timeline**: ViewStats data → YouTube API current week validation
- **Data Sources**: ViewStats.org → YouTube Data API v3
- **Validation Window**: Previous week to current week transition
- **Critical Metrics**: Real-time subscriber/view count validation
- **Tolerance Thresholds**: ±5% variance (highest precision for current data)
- **Validation Methodology**: Real-time API cross-validation with statistical trend analysis

### Junction Point Validation Framework

#### Automated Validation Pipeline
1. **Pre-Validation Data Preparation**
   - Extract records within validation windows
   - Normalize timestamps to weekly boundaries
   - Identify channel-specific validation requirements

2. **Statistical Continuity Analysis**
   - Linear regression trend analysis across junction points
   - Growth rate consistency validation (weekly delta analysis)
   - Variance analysis using coefficient of variation
   - Outlier detection using IQR and Z-score methods

3. **Cross-Source Validation**
   - Multi-source data comparison for overlapping periods
   - Source-specific bias detection and correction
   - Temporal accuracy validation (timestamp alignment)
   - Data freshness and staleness detection

4. **Quality Score Calculation**
   - Weighted scoring based on metric importance
   - Junction-specific scoring criteria
   - Temporal consistency scoring
   - Overall continuity assessment

#### Validation Algorithms

**Continuity Validation Algorithm**:
```
For each channel at junction point:
  1. Extract pre-junction trend (4 weeks before)
  2. Extract post-junction trend (4 weeks after)  
  3. Calculate expected progression using linear regression
  4. Compare actual vs expected values
  5. Calculate variance percentage
  6. Assign quality flag based on thresholds
  7. Flag channels exceeding tolerance for review
```

**Statistical Progression Validation**:
```
For each metric (subs, views, videos):
  1. Calculate weekly growth rates (4-week window)
  2. Identify statistical outliers (Z-score > 2.5)
  3. Validate monotonic progression where expected
  4. Check for unrealistic spikes or drops
  5. Cross-validate against similar channels
  6. Apply temporal smoothing where appropriate
```

#### Manual Review Process
- **Trigger Conditions**: >15% variance, statistical outliers, negative growth anomalies
- **Review Criteria**: Channel history analysis, external validation, context assessment
- **Resolution Actions**: Data correction, quality flag adjustment, exclusion decisions
- **Documentation**: Detailed review notes, decision rationale, quality impact assessment

## Data Quality Framework

The comprehensive data quality framework ensures dataset reliability through multi-layered validation, statistical analysis, and continuous monitoring across all collection phases.

### Quality Flag Categories and Scoring Criteria

#### Quality Flag Definitions
- **excellent** (≥95% quality score): Complete data, validated continuity, no anomalies
  - All required fields present and validated
  - Perfect junction point continuity (<5% variance)
  - No statistical outliers or anomalies detected
  - Data freshness within 24 hours for current sources
  - Cross-source validation passed with high confidence
  
- **good** (≥85% quality score): Minor gaps or interpolated data, validated trends
  - Required fields present, minor optional fields missing
  - Good junction point continuity (5-10% variance)
  - Minor interpolation applied (<5% of data points)
  - Statistical trends validated and consistent
  - Acceptable data freshness (24-72 hours)
  
- **fair** (≥75% quality score): Some missing data, acceptable statistical variance
  - Core fields present, some secondary fields missing
  - Moderate junction point discontinuity (10-15% variance)
  - Interpolation applied to <20% of data points
  - Statistical outliers present but within tolerance
  - Data staleness up to 1 week
  
- **poor** (≥60% quality score): Significant gaps, requires manual review
  - Missing important fields, data completeness <80%
  - Significant junction point issues (>15% variance)
  - Extensive interpolation required (>20% of data)
  - Multiple statistical anomalies detected
  - Manual review and correction required
  
- **missing** (<60% quality score): Critical data missing, channel flagged
  - Critical fields missing or corrupted
  - No valid data for extended periods (>4 weeks gaps)
  - Junction point validation failed completely
  - Channel inactive, private, or deleted
  - Excluded from final dataset

#### Quality Score Calculation Algorithm
```
Quality Score = (
  Completeness_Score * 0.25 +
  Continuity_Score * 0.30 +
  Statistical_Score * 0.20 +
  Temporal_Score * 0.15 +
  Freshness_Score * 0.10
)

Where:
- Completeness_Score: Percentage of required fields present
- Continuity_Score: Junction point validation success rate
- Statistical_Score: Absence of outliers and anomalies
- Temporal_Score: Weekly granularity adherence
- Freshness_Score: Data recency for current sources
```

### Data Quality Validation Rules

#### 1. Temporal Validation Framework
- **Weekly Granularity Enforcement**:
  - Standard week definition: Sunday 00:00 UTC to Saturday 23:59 UTC
  - Tolerance: ±3 days for timestamp normalization
  - Gap detection: Identify missing weeks, maximum 2-week interpolation
  - Sequence validation: Ensure chronological ordering
  
- **Date Range Validation**:
  - Minimum date: January 1, 2015
  - Maximum date: Current date + 7 days
  - Historical accuracy: Cross-validate with known YouTube timeline events
  - Future date detection: Flag records with timestamps beyond current week

- **Interval Consistency**:
  - Expected interval: 7 days ± 3 days tolerance
  - Frequency analysis: Identify irregular posting patterns
  - Temporal normalization: Adjust timestamps to Sunday boundaries
  - Interpolation validation: Verify interpolated points follow statistical trends

#### 2. Statistical Validation Framework
- **Growth Rate Analysis**:
  - Subscriber growth: Maximum 20% weekly growth rate
  - View count growth: Maximum 50% weekly growth rate  
  - Video upload rate: Maximum 50 videos per week
  - Negative growth tolerance: Up to 5% decline acceptable
  
- **Outlier Detection Methods**:
  - Z-Score Analysis: Threshold 3.0σ for critical outliers
  - IQR Method: 1.5 × IQR for moderate outliers
  - Seasonal adjustment: Account for holiday/event-driven spikes
  - Channel-specific baselines: Adaptive thresholds based on channel history
  
- **Cross-Metric Validation**:
  - Views-to-subscriber ratio: 10:1 to 10,000:1 acceptable range
  - Video-to-view correlation: Validate upload impact on view growth
  - Engagement consistency: Monitor subscriber-to-view relationship
  - Category-specific validation: Apply category-appropriate thresholds

#### 3. Cross-Source Validation Framework
- **Multi-Source Consistency**:
  - Source precedence: API > ViewStats > Wayback > Interpolated
  - Variance tolerance: ±15% between sources for same time period
  - Conflict resolution: Weighted average based on source reliability
  - Timestamp alignment: Normalize different source time zones
  
- **Data Freshness Validation**:
  - API data: Maximum 24-hour staleness
  - ViewStats data: Maximum 72-hour staleness
  - Wayback data: Historical accuracy validation
  - Interpolated data: Clear flagging and confidence scoring
  
- **Source-Specific Validation**:
  - YouTube API: Response code validation, rate limit handling
  - Wayback Machine: Snapshot authenticity, HTML parsing accuracy
  - ViewStats: Anti-bot detection, data consistency checks
  - Manual corrections: Documentation and approval workflow

#### 4. Completeness Validation Framework
- **Critical Field Requirements**:
  - channel_id: Must be valid YouTube channel identifier
  - datetime: Must be valid timestamp within acceptable range
  - subscriber_count: Non-negative integer, reasonable progression
  - view_count: Non-negative integer, monotonic increase expected
  
- **Important Field Requirements**:
  - channel_name: Current display name from API
  - category: Valid YouTube category classification
  - video_count: Non-negative integer, generally increasing
  - collection_source: Valid source identifier for traceability
  
- **Enhanced Field Requirements**:
  - total_longform_views: Available for post-2020 data
  - total_shorts_views: Available for post-2020 data
  - data_quality_flag: Assigned by validation pipeline
  - activity_status: Derived from upload patterns

#### 5. Junction Point Validation Framework
- **Continuity Analysis**:
  - Pre-junction trend extraction (4-week window)
  - Post-junction trend validation (4-week window)
  - Statistical regression analysis for expected progression
  - Variance calculation and threshold comparison
  
- **Cross-Reference Validation**:
  - Multiple source comparison at junction points
  - Historical context validation (major YouTube events, algorithm changes)
  - Channel-specific pattern analysis
  - Manual review for significant discontinuities

### Validation Pipeline Architecture

#### Real-Time Validation
- **Streaming Validation**: Real-time checks during data collection
- **Immediate Flagging**: Instant quality flag assignment
- **Early Warning System**: Alert on critical validation failures
- **Adaptive Thresholds**: Dynamic adjustment based on collection context

#### Batch Validation  
- **Comprehensive Analysis**: Deep statistical analysis post-collection
- **Cross-Phase Validation**: Validate consistency across all collection phases
- **Quality Score Calculation**: Final quality score assignment
- **Report Generation**: Detailed validation reports and recommendations

#### Continuous Monitoring
- **Quality Trend Analysis**: Monitor quality metrics over time
- **Source Performance Tracking**: Track individual source reliability
- **Validation Rule Optimization**: Refine thresholds based on results
- **Feedback Loop Integration**: Incorporate manual review insights

## Weekly Granularity Standards

### Temporal Requirements
- **Standard Week Definition**: Sunday 00:00 UTC to Saturday 23:59 UTC
- **Data Point Frequency**: Exactly one record per channel per week
- **Gap Tolerance**: Maximum 3-day variance in weekly intervals
- **Interpolation Rules**: Linear interpolation for gaps ≤2 weeks

### Expected Data Volume
- **Historical Period (2015-2019)**: 18,872,499 existing records
- **Gap Period (2019-2022)**: 25,335,750 target records
- **Current Period (2022-Present)**: Dynamic weekly growth
- **Total Target**: ~44M+ records across 10-year span

## Data Types and Constraints

### Numeric Fields
- **views, subs, videos**: INT64, non-negative
- **delta_views, delta_subs, delta_videos**: INT64, can be negative
- **total_longform_views, total_shorts_views**: INT64, non-negative

### String Fields
- **channel**: VARCHAR(24), YouTube channel ID format
- **channel_name**: VARCHAR(100), UTF-8 encoded
- **category**: VARCHAR(50), standardized categories
- **collection_source**: ENUM(API, WAYBACK_CHARTS, WAYBACK_UPLOADS, VIEWSTATS, FINAL_API)
- **data_quality_flag**: ENUM(excellent, good, fair, poor, missing)

### Temporal Fields
- **datetime**: TIMESTAMP, ISO 8601 format, weekly intervals
- **activity**: BOOLEAN or VARCHAR(10), activity status

## Performance and Storage Considerations

### Storage Requirements
- **Base Dataset**: ~2.5GB (18.8M records)
- **Enhanced Dataset**: ~6-8GB (44M+ records with additional fields)
- **Index Requirements**: Composite indexes on (channel, datetime) and (data_quality_flag)

### Processing Performance
- **Memory Requirements**: 16GB+ RAM for full dataset processing
- **Batch Processing**: 1,000 channels per batch recommended
- **Checkpoint Management**: Progress tracking every 10,000 records
- **Error Recovery**: Partial failure handling with resume capability

## Data Validation and Quality Assurance

### Automated Validation Pipeline
1. **Schema Validation**: Field presence and type checking
2. **Temporal Validation**: Weekly granularity and date range validation
3. **Statistical Validation**: Growth rate and trend analysis
4. **Cross-Reference Validation**: Multi-source data consistency
5. **Junction Point Validation**: Continuity at data source transitions

### Quality Reporting
- **Real-time Monitoring**: Processing progress and error rates
- **Quality Metrics**: Completeness, accuracy, and consistency scores
- **Validation Reports**: Daily quality assessment reports
- **Anomaly Detection**: Statistical outlier identification and flagging

## Integration Points

### External System Integration
- **YouTube Data API v3**: Current metadata and verification
- **Internet Archive CDX API**: Historical snapshot discovery
- **ViewStats API**: Recent historical data collection
- **Social Blade Parsing**: Wayback Machine data extraction

### Data Export Formats
- **Primary Format**: TSV (tab-separated values) for compatibility
- **Alternative Formats**: Parquet for analytics, JSON for API integration
- **Compression**: GZIP compression for storage optimization

## Maintenance and Updates

### Ongoing Data Collection
- **Weekly Updates**: Automated collection of current week data
- **Quality Monitoring**: Continuous validation of new data
- **Historical Backfill**: Opportunistic gap filling from new sources
- **Schema Evolution**: Planned enhancements for future requirements

### Data Retention Policy
- **Raw Data**: Permanent retention for all collected data
- **Processed Data**: Versioned datasets with quality metadata
- **Error Logs**: 90-day retention for debugging and analysis
- **Quality Reports**: 1-year retention for trend analysis