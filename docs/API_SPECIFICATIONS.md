# API Specifications

## Overview

This document provides comprehensive technical specifications for all external API integrations in the YouNiverse Dataset Enrichment pipeline. The specifications enable feature-developer implementation of the 5-phase data collection strategy targeting 153,550 channels across a 10-year timespan (2015-2025).

## Integration Architecture

### 5-Phase API Integration Strategy

The YouNiverse enrichment pipeline integrates with four primary external services:

1. **Phase 1**: YouTube Data API v3 - Channel metadata enrichment
2. **Phase 2A**: Internet Archive Wayback Machine CDX API - Historical chart discovery
3. **Phase 2B**: Social Blade Historical Data - Upload tracking via Wayback snapshots
4. **Phase 3**: ViewStats API - Current metrics collection
5. **Phase 4**: YouTube Data API v3 - Final current week update

## Phase 1: YouTube Data API v3 Integration

### API Endpoint Specifications

**Base URL**: `https://www.googleapis.com/youtube/v3/channels`

**Authentication**: API Key-based authentication
- Header: `Authorization: Bearer {API_KEY}`
- Alternative: Query parameter `key={API_KEY}`

**Request Specifications**:
```http
GET /youtube/v3/channels?part=snippet,statistics,status&id={channel_ids}&key={api_key}
```

**Parameters**:
- `part`: Required fields - `snippet,statistics,status`
- `id`: Comma-separated channel IDs (max 50 per request)
- `key`: API authentication key

**Batch Processing Configuration**:
- Batch size: 50 channels per request
- Concurrent requests: 4 parallel workers
- Rate limiting: 10,000 units per day quota
- Cost per request: 1 unit per API call

### Response Schema

**Successful Response Structure**:
```json
{
  "kind": "youtube#channelListResponse",
  "etag": "string",
  "pageInfo": {
    "totalResults": 50,
    "resultsPerPage": 50
  },
  "items": [
    {
      "kind": "youtube#channel",
      "etag": "string",
      "id": "channel_id",
      "snippet": {
        "title": "Channel Name",
        "description": "Channel Description",
        "customUrl": "@channelhandle",
        "publishedAt": "2015-01-01T00:00:00Z",
        "thumbnails": { ... },
        "defaultLanguage": "en",
        "country": "US"
      },
      "statistics": {
        "viewCount": "1000000",
        "subscriberCount": "50000",
        "hiddenSubscriberCount": false,
        "videoCount": "250"
      },
      "status": {
        "privacyStatus": "public",
        "isLinked": true,
        "longUploadsStatus": "allowed",
        "madeForKids": false,
        "selfDeclaredMadeForKids": false
      }
    }
  ]
}
```

**Field Extraction Mapping**:
- `snippet.title` → `channel_name`
- `snippet.publishedAt` → `channel_created_date`
- `statistics.subscriberCount` → `current_subscriber_count`
- `statistics.viewCount` → `current_view_count`
- `statistics.videoCount` → `current_video_count`
- `status.privacyStatus` → `channel_status`

### Error Handling Specifications

**Error Categories and Responses**:

**1. Quota Exceeded (403)**:
```json
{
  "error": {
    "code": 403,
    "message": "The request cannot be completed because you have exceeded your quota.",
    "errors": [
      {
        "message": "Daily Limit Exceeded",
        "domain": "youtube.quota",
        "reason": "dailyLimitExceeded"
      }
    ]
  }
}
```
**Response Strategy**: Circuit breaker activation, resume at daily quota reset

**2. Channel Not Found (404)**:
```json
{
  "error": {
    "code": 404,
    "message": "The channel cannot be found.",
    "errors": [
      {
        "message": "Channel not found",
        "domain": "youtube.channel",
        "reason": "channelNotFound"
      }
    ]
  }
}
```
**Response Strategy**: Mark channel as `inactive`, continue processing

**3. Rate Limiting (429)**:
```json
{
  "error": {
    "code": 429,
    "message": "Too Many Requests"
  }
}
```
**Response Strategy**: Exponential backoff with jitter (2^n seconds, max 300s)

### Rate Limiting Implementation

**Token Bucket Algorithm**:
```python
class YouTubeAPIRateLimiter:
    def __init__(self):
        self.quota_limit = 10000  # units per day
        self.current_quota = 10000
        self.reset_time = datetime.now().replace(hour=0, minute=0, second=0) + timedelta(days=1)
    
    def can_make_request(self, cost=1):
        if datetime.now() >= self.reset_time:
            self.current_quota = self.quota_limit
            self.reset_time += timedelta(days=1)
        
        return self.current_quota >= cost
    
    def consume_quota(self, cost=1):
        self.current_quota -= cost
```

**Retry Logic**:
- Max retries: 5 attempts
- Backoff strategy: Exponential with jitter
- Retry conditions: 5xx errors, network timeouts, rate limits
- No retry conditions: 404 (channel not found), 403 (quota exceeded)

## Phase 2A: Internet Archive Wayback Machine CDX API

### CDX API Endpoint Specifications

**Base URL**: `http://web.archive.org/cdx/search/cdx`

**Request Format**:
```http
GET /cdx/search/cdx?url={social_blade_url}&from={start_date}&to={end_date}&output=json&collapse=timestamp:6&limit=1000
```

**Parameters**:
- `url`: Target URL pattern - `https://socialblade.com/youtube/channel/{channel_id}/monthly`
- `from`: Start date - `20190901` (September 1, 2019)
- `to`: End date - `20221231` (December 31, 2022)
- `output`: Response format - `json`
- `collapse`: Deduplicate by timestamp (6-digit precision for daily)
- `limit`: Maximum snapshots per request - `1000`

**URL Pattern Construction**:
```python
def construct_wayback_url(channel_id):
    base_url = "https://socialblade.com/youtube/channel"
    return f"{base_url}/{channel_id}/monthly"

def construct_cdx_query(channel_id, start_date="20190901", end_date="20221231"):
    target_url = construct_wayback_url(channel_id)
    return {
        "url": target_url,
        "from": start_date,
        "to": end_date,
        "output": "json",
        "collapse": "timestamp:6",
        "limit": "1000"
    }
```

### Response Schema and Processing

**CDX Response Format**:
```json
[
  ["urlkey", "timestamp", "original", "mimetype", "statuscode", "digest", "length"],
  [
    "com,socialblade)/youtube/channel/uc123abc/monthly",
    "20190915142301",
    "https://socialblade.com/youtube/channel/UC123ABC/monthly", 
    "text/html",
    "200",
    "ABC123DEF456",
    "45621"
  ]
]
```

**Snapshot Selection Algorithm**:
```python
def select_optimal_snapshots(cdx_results, target_weeks=165):
    """
    Smart snapshot selection for maximum weekly coverage
    Target: Sep 2019 - Dec 2022 (165 weeks)
    """
    snapshots = []
    for result in cdx_results[1:]:  # Skip header
        timestamp = result[1]
        if result[4] == "200":  # HTTP 200 OK
            snapshots.append({
                'timestamp': timestamp,
                'date': datetime.strptime(timestamp[:8], '%Y%m%d'),
                'url': f"http://web.archive.org/web/{timestamp}/{result[2]}"
            })
    
    # Select weekly representatives
    weekly_snapshots = []
    current_week = None
    
    for snapshot in sorted(snapshots, key=lambda x: x['date']):
        week = snapshot['date'].isocalendar()[1]
        year = snapshot['date'].year
        week_key = f"{year}-{week:02d}"
        
        if week_key != current_week:
            weekly_snapshots.append(snapshot)
            current_week = week_key
            
        if len(weekly_snapshots) >= target_weeks:
            break
    
    return weekly_snapshots
```

### Rate Limiting and Anti-Bot Protection

**Rate Limiting Strategy**:
- Request frequency: 1 request per second maximum
- Respectful delay: 2-5 second random intervals
- Session management: Rotate sessions every 100 requests

**Anti-Bot Countermeasures**:
```python
class WaybackAntiBot:
    def __init__(self):
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
        ]
        self.session = requests.Session()
        
    def get_headers(self):
        return {
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
    
    def make_request(self, url):
        headers = self.get_headers()
        time.sleep(random.uniform(2, 5))  # Random delay
        return self.session.get(url, headers=headers, timeout=30)
```

## Phase 2B: Social Blade Historical Data Extraction

### Chart Data Parsing Specifications

**Target Chart Libraries**:
1. **Dygraphs** - Primary chart library used by Social Blade
2. **Highcharts** - Alternative chart library detection

**HTML Parsing Strategy**:
```python
class SocialBladeParser:
    def __init__(self):
        self.chart_selectors = {
            'dygraph': 'div[id*="chart"], div[class*="dygraph"]',
            'highcharts': 'div[id*="highcharts"], div[class*="highcharts"]'
        }
    
    def extract_chart_data(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Method 1: Extract from JavaScript variables
        script_tags = soup.find_all('script', string=re.compile(r'chart.*data|data.*chart'))
        for script in script_tags:
            data = self.parse_js_chart_data(script.string)
            if data:
                return data
        
        # Method 2: Extract from DOM data attributes
        chart_containers = soup.select(','.join(self.chart_selectors.values()))
        for container in chart_containers:
            data = self.parse_dom_chart_data(container)
            if data:
                return data
        
        return None
```

**Data Point Extraction Pattern**:
```javascript
// Target JavaScript pattern recognition
var chartData = [
  [new Date("2019-09-01"), 1000000, 50000, 250],  // [date, views, subs, videos]
  [new Date("2019-09-08"), 1005000, 50100, 252],
  // ... weekly data points
];

// Alternative CSV-style format
"2019-09-01,1000000,50000,250\n2019-09-08,1005000,50100,252"
```

**Weekly Data Validation**:
```python
def validate_weekly_data(data_points):
    """
    Validate extracted data points for weekly consistency
    Target: 165 weeks from Sep 2019 to Dec 2022
    """
    if len(data_points) < 100:  # Minimum threshold
        return False, "Insufficient data points"
    
    # Check date progression
    dates = [point['date'] for point in data_points]
    date_diffs = [(dates[i+1] - dates[i]).days for i in range(len(dates)-1)]
    
    # Weekly intervals should be ~7 days
    avg_interval = sum(date_diffs) / len(date_diffs)
    if not 6 <= avg_interval <= 8:
        return False, f"Invalid interval: {avg_interval} days"
    
    # Check metric progression logic
    for i in range(len(data_points)-1):
        current = data_points[i]
        next_point = data_points[i+1]
        
        # Views should generally increase
        if next_point['views'] < current['views'] * 0.95:  # Allow 5% decrease
            continue  # Log warning but don't fail
        
        # Subscriber count should not decrease dramatically
        if next_point['subscribers'] < current['subscribers'] * 0.9:
            return False, f"Suspicious subscriber drop at {next_point['date']}"
    
    return True, "Valid weekly progression"
```

### Error Handling for Historical Data

**Parsing Error Categories**:

1. **Chart Library Not Found**:
   - Fallback to text-based data extraction
   - Search for tabular data in HTML
   - Mark as low-quality data source

2. **Incomplete Data Range**:
   - Interpolate missing weeks using linear progression
   - Flag interpolated data points
   - Calculate confidence scores

3. **Data Format Changes**:
   - Implement multiple parsing patterns
   - Version detection for Social Blade layout changes
   - Graceful degradation to available data

## Phase 3: ViewStats Integration

### ViewStats API Specifications

**Base URL**: `https://www.viewstats.com/api/v1/channel/{channel_id}`

**Authentication**: Session-based with anti-bot protection

**Request Configuration**:
```http
GET /api/v1/channel/{channel_id}/metrics?
    from=2022-08-01&
    to=2024-12-31&
    granularity=weekly&
    breakdown=longform,shorts
```

**Parameters**:
- `from`: Start date (August 1, 2022)
- `to`: Current date
- `granularity`: Data interval - `weekly`
- `breakdown`: View type separation - `longform,shorts`

### Response Schema

**ViewStats Response Structure**:
```json
{
  "channel_id": "UC123ABC",
  "data": [
    {
      "date": "2022-08-07",
      "total_views": 1500000,
      "total_subscribers": 75000,
      "longform_views": 1200000,
      "shorts_views": 300000,
      "video_count": 280,
      "estimated_revenue": null
    }
  ],
  "metadata": {
    "collection_date": "2024-01-15T10:30:00Z",
    "data_quality": "high",
    "coverage_percentage": 98.5
  }
}
```

**Field Mapping**:
- `total_views` → `weekly_views`
- `total_subscribers` → `weekly_subscribers`
- `longform_views` → `weekly_longform_views`
- `shorts_views` → `weekly_shorts_views`
- `video_count` → `weekly_video_count`

### Anti-Bot Protection Implementation

**Stealth Layer Configuration**:
```python
class ViewStatsClient:
    def __init__(self):
        self.session = requests.Session()
        self.proxy_pool = self.load_proxy_pool()
        self.user_agent_rotation = UserAgentRotator()
        
    def make_request(self, url, retries=3):
        for attempt in range(retries):
            try:
                # Rotate proxy
                proxy = random.choice(self.proxy_pool)
                self.session.proxies.update(proxy)
                
                # Rotate user agent and headers
                headers = self.user_agent_rotation.get_headers()
                
                # Random delay
                time.sleep(random.uniform(0.5, 2.0))
                
                response = self.session.get(url, headers=headers, timeout=30)
                
                # CAPTCHA detection
                if self.detect_captcha(response):
                    self.handle_captcha(response)
                    continue
                
                return response
                
            except Exception as e:
                if attempt == retries - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
```

**CAPTCHA Detection and Handling**:
```python
def detect_captcha(self, response):
    captcha_indicators = [
        'captcha',
        'challenge',
        'bot detection',
        'unusual traffic'
    ]
    
    content_lower = response.text.lower()
    return any(indicator in content_lower for indicator in captcha_indicators)

def handle_captcha(self, response):
    # Log CAPTCHA encounter
    logger.warning(f"CAPTCHA detected for ViewStats request")
    
    # Switch to different proxy/session
    self.rotate_session()
    
    # Increase delay before next request
    time.sleep(random.uniform(10, 30))
```

### Rate Limiting Strategy

**Request Throttling**:
- Rate limit: 2 requests per second maximum
- Burst allowance: 5 requests per 10-second window
- Cooldown period: 60 seconds after rate limit hit

**Implementation**:
```python
class ViewStatsRateLimiter:
    def __init__(self):
        self.requests = deque()
        self.max_requests = 2
        self.time_window = 1.0  # 1 second
        
    def wait_if_needed(self):
        now = time.time()
        
        # Remove old requests outside time window
        while self.requests and self.requests[0] <= now - self.time_window:
            self.requests.popleft()
        
        # Check if we need to wait
        if len(self.requests) >= self.max_requests:
            sleep_time = self.time_window - (now - self.requests[0])
            if sleep_time > 0:
                time.sleep(sleep_time)
        
        self.requests.append(now)
```

## Phase 4: Final YouTube API Update

### Current Week Data Collection

**Request Specification**:
Same as Phase 1 but targeting only current week data point alignment.

**Validation Requirements**:
- Ensure data aligns with weekly boundary (Sunday-to-Sunday)
- Validate progression from Phase 3 data
- Calculate final weekly deltas

**Implementation**:
```python
def collect_current_week_data(channel_ids):
    """
    Final API call to get current week data point
    """
    current_date = datetime.now()
    
    # Find current Sunday (week boundary)
    days_since_sunday = current_date.weekday() % 7
    current_sunday = current_date - timedelta(days=days_since_sunday)
    
    # Only collect if we're past mid-week to ensure data stability
    if days_since_sunday < 3:
        logger.info("Too early in week for stable data, using previous week")
        return None
    
    # Make YouTube API call
    youtube_client = YouTubeAPIClient()
    current_data = youtube_client.get_channels_batch(channel_ids)
    
    # Add weekly boundary timestamp
    for channel_data in current_data:
        channel_data['collection_date'] = current_sunday.isoformat()
        channel_data['data_source'] = 'youtube_api_final'
    
    return current_data
```

## Junction Point Validation Specifications

### September 2019 Transition (Existing → Wayback)

**Validation Rules**:
1. **Temporal Continuity**: No gaps between last existing data point and first Wayback data point
2. **Metric Consistency**: Views and subscribers should show logical progression
3. **Delta Validation**: Weekly growth rates should be statistically reasonable

**Implementation**:
```python
def validate_sep_2019_junction(existing_data, wayback_data):
    """
    Validate data continuity at September 2019 transition
    """
    # Find junction points
    existing_last = existing_data.loc[existing_data['date'] <= '2019-09-01'].iloc[-1]
    wayback_first = wayback_data.loc[wayback_data['date'] >= '2019-09-01'].iloc[0]
    
    # Calculate time gap
    time_gap = (wayback_first['date'] - existing_last['date']).days
    if time_gap > 14:  # Allow up to 2 weeks gap
        return False, f"Time gap too large: {time_gap} days"
    
    # Validate metric progression
    metrics = ['subscribers', 'views', 'videos']
    for metric in metrics:
        existing_value = existing_last[metric]
        wayback_value = wayback_first[metric]
        
        # Calculate expected growth
        days_between = max(time_gap, 1)
        daily_growth_rate = (wayback_value - existing_value) / existing_value / days_between
        
        # Reasonable daily growth bounds
        if metric == 'subscribers' and daily_growth_rate > 0.05:  # 5% daily growth max
            return False, f"Unrealistic {metric} growth: {daily_growth_rate:.3f}/day"
        elif metric == 'views' and daily_growth_rate > 0.10:  # 10% daily growth max
            return False, f"Unrealistic {metric} growth: {daily_growth_rate:.3f}/day"
    
    return True, "Junction validation passed"
```

### August 2022 Transition (Wayback → ViewStats)

**Enhanced Validation** (includes new longform/shorts breakdown):
```python
def validate_aug_2022_junction(wayback_data, viewstats_data):
    """
    Validate transition to ViewStats with enhanced metrics
    """
    # Standard continuity validation
    continuity_valid, message = validate_basic_continuity(wayback_data, viewstats_data, '2022-08-01')
    if not continuity_valid:
        return False, message
    
    # Validate longform/shorts breakdown
    viewstats_first = viewstats_data.iloc[0]
    total_views_breakdown = viewstats_first['longform_views'] + viewstats_first['shorts_views']
    reported_total = viewstats_first['total_views']
    
    # Allow 5% discrepancy in breakdown
    if abs(total_views_breakdown - reported_total) / reported_total > 0.05:
        return False, f"Longform/shorts breakdown inconsistent: {total_views_breakdown} vs {reported_total}"
    
    return True, "Enhanced junction validation passed"
```

## Security Specifications

### API Key Management

**Security Requirements**:
- API keys stored in environment variables only
- No API keys committed to version control
- Key rotation capability for production deployments
- Usage monitoring and quota alerts

**Implementation**:
```python
import os
from cryptography.fernet import Fernet

class SecureAPIManager:
    def __init__(self):
        self.encryption_key = os.environ.get('API_ENCRYPTION_KEY')
        self.cipher = Fernet(self.encryption_key)
        
    def get_youtube_api_key(self):
        encrypted_key = os.environ.get('YOUTUBE_API_KEY_ENCRYPTED')
        if not encrypted_key:
            raise ValueError("YouTube API key not found in environment")
        return self.cipher.decrypt(encrypted_key.encode()).decode()
    
    def validate_api_access(self):
        """Test API access without consuming quota"""
        test_request = {
            'part': 'id',
            'id': 'UC_x5XG1OV2P6uZZ5FSM9Ttw',  # Google Developers channel
            'maxResults': 1
        }
        # Make test request to validate key
```

### Data Protection

**Sensitive Data Handling**:
- Channel revenue data (if available) encrypted at rest
- Personal channel information anonymized in logs
- Request/response data cached with expiration
- Error logs sanitized of API keys

### Rate Limiting Security

**Quota Protection**:
```python
class QuotaProtection:
    def __init__(self, daily_quota_limit):
        self.daily_limit = daily_quota_limit
        self.safety_buffer = 0.9  # Use only 90% of quota
        self.current_usage = 0
        
    def check_quota_safety(self, required_quota):
        if self.current_usage + required_quota > self.daily_limit * self.safety_buffer:
            raise QuotaExhaustionError(
                f"Quota safety limit reached: {self.current_usage}/{self.daily_limit}"
            )
        return True
```

## Error Recovery Specifications

### Circuit Breaker Implementation

**Multi-Service Circuit Breaker**:
```python
class APICircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
        
    def call(self, func, *args, **kwargs):
        if self.state == 'OPEN':
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = 'HALF_OPEN'
            else:
                raise CircuitBreakerOpenError("Circuit breaker is OPEN")
        
        try:
            result = func(*args, **kwargs)
            if self.state == 'HALF_OPEN':
                self.state = 'CLOSED'
                self.failure_count = 0
            return result
            
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                self.state = 'OPEN'
            
            raise e
```

### Graceful Degradation Strategies

**Data Source Fallback Chain**:
1. **Primary**: Full weekly data collection
2. **Degraded**: Monthly sampling with interpolation
3. **Minimal**: Quarterly snapshots with quality flags

```python
class DataCollectionFallback:
    def collect_with_fallback(self, channel_id):
        strategies = [
            self.collect_weekly_data,
            self.collect_monthly_data,
            self.collect_quarterly_data
        ]
        
        for strategy in strategies:
            try:
                data = strategy(channel_id)
                if self.validate_data_quality(data):
                    return data
            except Exception as e:
                logger.warning(f"Strategy {strategy.__name__} failed: {e}")
                continue
        
        # Final fallback: mark as failed with existing data preserved
        return self.preserve_existing_data(channel_id)
```

## Quality Assurance Specifications

### Data Validation Framework

**Multi-Level Validation**:
1. **Schema Validation**: Data type and format compliance
2. **Business Logic Validation**: Metric progression rules
3. **Statistical Validation**: Outlier detection and trend analysis
4. **Cross-Source Validation**: Consistency between data sources

**Implementation Framework**:
```python
class DataQualityValidator:
    def __init__(self):
        self.validators = [
            SchemaValidator(),
            BusinessLogicValidator(),
            StatisticalValidator(),
            CrossSourceValidator()
        ]
    
    def validate_dataset(self, data):
        quality_report = {
            'overall_score': 0.0,
            'validation_results': [],
            'quality_flags': []
        }
        
        for validator in self.validators:
            result = validator.validate(data)
            quality_report['validation_results'].append(result)
            
            if result['passed']:
                quality_report['overall_score'] += result['weight']
            else:
                quality_report['quality_flags'].extend(result['flags'])
        
        return quality_report
```

### Performance Monitoring

**Real-Time Metrics Collection**:
- API response times per service
- Success/failure rates per collector
- Data quality scores per channel
- Resource utilization monitoring

**Alert Thresholds**:
- API quota usage >80%: WARNING
- Error rate >10%: HIGH
- Memory usage >90%: CRITICAL
- Junction validation failure: CRITICAL

This comprehensive API specification provides the technical foundation for implementing the 5-phase YouNiverse Dataset Enrichment pipeline with robust error handling, security measures, and quality assurance protocols.