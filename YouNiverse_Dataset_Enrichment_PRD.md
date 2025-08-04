# Product Requirements Document (PRD)
## YouNiverse Dataset Enrichment - 3-Day Implementation

**Project Type:** Internal Database Enrichment  
**Timeline:** 3 Days  
**Scope:** Data Collection & Dataset Enhancement  
**Date:** August 4, 2025  
**Location:** C:\Users\Ochow\Desktop\Darwin.v1\youniverse_expansion  

## Agent Task Restrictions
**CRITICAL RULE:** Only agents with explicit permissions for their designated tasks may execute implementation work. Each agent must stay within their defined role boundaries as specified in their agent definitions. No cross-functional implementation without proper delegation through darwin-orchestrator.

**Code Length Restriction:** All individual script files must be ≤500 lines. If implementation exceeds 500 lines, break into multiple modular files with clear separation of concerns.

---

## Project Overview

**Objective:** Enrich the existing YouNiverse timeseries dataset with current channel metadata and extend the temporal coverage from September 2019 to Q2 2025 for enhanced fund analysis capabilities.

**Current State:** 
- Dataset: `df_timeseries_en.tsv` (18,872,499 records, 153,550 channels)
- Coverage: January 2015 - September 2019 (≈245 weeks)
- Weekly data granularity: 1 record per channel per week
- Schema: channel, category, datetime, views, delta_views, subs, delta_subs, videos, delta_videos, activity

**Target State:**
- Enriched dataset with current channel names and categories
- Extended temporal coverage: January 2015 - Current Date (dynamic weekly collection)
- **CRITICAL: All data collection must maintain weekly granularity**
- Enhanced metrics including longform/shorts view breakdown
- Validated dataset ready for investment analysis with ongoing updates

## Data Point Mathematics & Validation

**Expected Data Point Calculations:**
- **Existing Dataset:** 153,550 channels × 245 weeks = 37,619,750 theoretical maximum (actual: 18,872,499 due to incomplete coverage)
- **Phase 1 (YouTube API):** 153,550 channels × 1 metadata record = 153,550 enrichment records
- **Phase 2A (Historical Charts 2019-2022):** 153,550 channels × 165 weeks (Sep 2019 - Dec 2022) = 25,335,750 target records
- **Phase 2B (Upload Data Collection):** Variable records based on available Wayback snapshots
- **Phase 3 (Current ViewStats):** 153,550 channels × (weeks from Aug 2022 to yesterday) = Variable based on execution date
- **Phase 4 (Final API Update):** 153,550 channels × 1 current week = 153,550 final records
- **Dynamic Dataset Target:** 153,550 channels × (weeks from Jan 2015 to current date) = Grows weekly

---

## Technical Implementation Plan

### Day 1: Dataset Loading & YouTube API Enrichment

**Core Deliverable:** Baseline enriched dataset with current channel metadata

**Technical Tasks:**
1. **Dataset Loading**
   - Load `C:\Users\Ochow\Desktop\Darwin.v1\YT Datasets\df_timeseries_en.tsv\df_timeseries_en.tsv`
   - Validate data integrity and schema consistency
   - Extract unique channel identifiers (153,550 channels)

2. **YouTube API Integration**
   - Initialize YouTube Data API v3 using `.env` credentials
   - Implement batch channel metadata retrieval (50 channels per request)
   - Rate limiting: 10,000 units/day quota management
   - Collect: current channel name, category, status

3. **Data Enrichment**
   - Map current channel names to existing channel IDs
   - Update category classifications where changed
   - Flag inactive/terminated channels
   - Create enriched baseline dataset: `enriched_baseline_timeseries.tsv`

**Code Reference:**
- Use existing YouNiverse folder helpers: `C:\Users\Ochow\Desktop\Darwin.v1\YouNiverse`
- Reference preprocessing.ipynb and analyses.ipynb for data handling patterns

**Error Handling:**
- API quota exceeded: implement exponential backoff
- Channel not found: flag as inactive, retain historical data
- Rate limiting: queue management with retry logic

**Expected Output:**
- `enriched_baseline_timeseries.tsv` - Enriched dataset file with updated metadata
- `incomplete_channels_timeseries.tsv` - Secondary dataset containing all partial/failed channel data for incomplete records

**Data Point Validation (Day 1):**
- Target: 153,550 channels enriched with current metadata
- Success Metric: >90% channels successfully enriched (>138,195 channels)
- Quality Check: All existing 18,872,499 weekly records preserved with added metadata columns

### Day 2: Historical Data Collection (2019-2022)

**Core Deliverable:** Historical data extension using Wayback Machine archives (Charts + Uploads)

**SPLIT INTO TWO PHASES:**
- **Phase 2A:** Charts data from /monthly URLs (subscribers/views)  
- **Phase 2B:** Upload data from main Social Blade pages

**Technical Tasks:**
1. **Optimized Wayback Machine Strategy**
   - Implement Internet Archive CDX API queries with smart snapshot selection
   - Target Social Blade weekly chart URLs ONLY: `https://web.archive.org/web/{timestamp}/https://socialblade.com/youtube/user/{channel_name}/monthly`
   - Date range: September 2019 - December 2022 (165 weeks total)
   - **WEEKLY DATA REQUIREMENT:** Extract exactly 165 weekly data points per channel
   - Smart algorithm: Find snapshots with maximum historical weekly chart coverage to minimize HTML parsing
   - Focus exclusively on /monthly pages as they contain long-term historical weekly chart data

2. **Anti-Bot Detection & Stealth Measures**
   - Implement rotating user agents (Chrome, Firefox, Safari variations)
   - Random sleep intervals: 2-5 seconds between requests
   - Session management with cookies and headers rotation
   - Proxy rotation if needed for large-scale collection
   - Request header spoofing to mimic real browser behavior
   - Implement CAPTCHA detection and handling fallbacks such as cookies 

3. **Intelligent Archive Discovery**
   - Query CDX API to find all available snapshots per channel in target timeframe
   - Algorithm: Select snapshot with longest historical chart coverage (e.g., Nov 16 2022 snapshot covering back to Nov 18 2019)
   - Minimize total HTML requests by choosing optimal snapshots
   - Handle channel name variations (e.g., mrbeast6000 → MrBeast) through:
     - Cross-reference with YouTube API current channel data
     - Pattern matching for known channel rebrands
     - Wayback Machine redirect following for channel URL changes

4. **Chart Data Extraction**
   - Parse Social Blade subscriber and view charts from selected snapshots
   - **Chart Library Detection:** Charts will be either Dygraph or Highcharts embedded in HTML
   - **HTML Analysis Requirement:** Download and save HTML files locally for analysis to identify correct data extraction methods
   - **Data Location Options:**
     - Dygraph: Look for JavaScript data arrays or CSV data embedded in script tags
     - Highcharts: Search for series data in JavaScript configuration objects
     - Alternative: Chart data may be in separate JSON endpoints called by the page
   - **CRITICAL:** Extract exactly weekly data points (Sunday-to-Sunday intervals) from identified chart data sources
   - Target: 165 weekly records per channel (Sep 2019 - Dec 2022)
   - Handle different chart formats and library versions across Social Blade layout changes over time
   - Validate extracted weekly data for logical progression and consistency

5. **Upload Data Collection (Phase 2B)**
   - **Target URL:** `https://web.archive.org/web/{timestamp}/https://socialblade.com/youtube/user/{channel_name}` (main page)
   - Query all available snapshots for upload counts in 2019-2022 period
   - Extract upload totals from main page metadata/counters
   - **Data Collection Strategy:** Snapshot-dependent, not weekly continuous
   - **Purpose:** Fill upload/video counts for historical period where available

6. **Data Processing & Validation**
   - Convert chart totals to weekly deltas matching existing schema
   - Cross-validate with existing dataset at junction points (Sep 2019)
   - Handle missing weeks through interpolation or gap flagging
   - **Upload Integration:** Fill upload data from snapshots, interpolate gaps
   - Channel name resolution through historical Social Blade data

**Implementation Details:**
- Maximum anti-detection measures: rotating headers, user agents, delays
- Smart snapshot selection: minimize HTML parsing through optimal archive selection
- **Phase 2A (Charts):** Focus on /monthly URLs for weekly subscriber/view data
- **Phase 2B (Uploads):** Separate main page scraping for upload counts from available snapshots
- Batch processing: 50 channels per batch (reduced for stealth)
- Extended sleep intervals: 2-5 seconds random between requests
- Channel name handling: cross-reference with YouTube API for current names
- Fallback strategies: multiple URL formats, different time periods if primary fails
- **WEEKLY DATA PRIORITY:** All chart data must maintain Sunday-to-Sunday weekly intervals
- Data format: maintain existing TSV schema with source attribution

**Expected Output:**
- `historical_2019_2022.tsv` - Historical extension dataset with charts + upload data
- Update `incomplete_channels_timeseries.tsv` with any failed collections
- Archive optimization report: snapshots used per channel, data coverage achieved

**Data Point Validation (Day 2):**
- **Phase 2A Target:** 153,550 channels × 165 weeks = 25,335,750 total weekly records (charts)
- **Phase 2B Target:** Upload data for available snapshots (variable coverage per channel)
- **Minimum Success:** >70% channels with complete 165-week coverage (>107,485 channels = >17,735,025 records)
- **CRITICAL WEEKLY VALIDATION:** 
  - **Weekly continuity:** No gaps in Sunday-to-Sunday weekly progression (subs/views)
  - **Upload data:** Best-effort coverage from available Wayback snapshots
  - **Weekly alignment:** All data points must align to consistent weekly intervals
  - **Data validation:** Subscriber/view counts show logical weekly progression
  - **Junction validation:** Smooth weekly transition from existing Sep 2019 data

### Day 3: Current Data Collection (2022-Present)

**Core Deliverable:** Current data collection and final dataset assembly

**Technical Tasks:**
1. **ViewStats Integration**
   - Access ViewStats public website scraping 
   - Collect data range: August 2022 - Yesterday (execution date - 1 day)
   - **WEEKLY DATA REQUIREMENT:** Extract weekly data points up to most recent complete week
   - Target metrics: total views, total subscribers, longform/shorts breakdown 
   - **CRITICAL:** Maintain weekly granularity to match existing dataset structure
   - **Cutoff Logic:** Stop at yesterday to allow YouTube API to handle current day data 

2. **Enhanced Metrics Collection**
   - New columns: `total_longform_views`, `total_shorts_views`
   - Maintain backward compatibility with existing schema
   - Calculate deltas for new time periods
   - Video count estimation from available data

3. **Final Dataset Assembly**
   - Merge three datasets: baseline, historical extension, current extension
   - Temporal alignment and consistency validation
   - Schema standardization across all time periods
   - Final dataset export: `youniverse_enriched_2015_2025.tsv`

4. **Data Validation & Quality Assurance**
   - Continuity checks at junction points (Sep 2019, Aug 2022)
   - Statistical validation: outlier detection, trend consistency
   - Coverage report: channels with complete vs. partial data
   - Final data quality documentation

**Expected Output:**
- Complete enriched dataset: `youniverse_enriched_2015_2025.tsv`
- Data quality report with coverage statistics
- Collection methodology documentation
- Error log with failed collections and reasons

---

## COMPREHENSIVE AGENT RESTRICTION FRAMEWORK

### 1. Directory-Level Restrictions

**CRITICAL RULE:** Agents can ONLY create files in their designated directories. Any violation triggers immediate escalation.

#### **Core Development Agents:**

**`feature-developer` (Primary Implementation Agent):**
- ✅ **FULL ACCESS:** `/src/data_collectors/`, `/src/processors/`, `/src/models/`, `/src/utils/`, `/src/main.py`
- ✅ **SHARED ACCESS:** `/tests/unit/`, `/tests/integration/` (must coordinate with test-strategist)
- ❌ **PROHIBITED:** `/docs/`, `/config/`, `/scripts/`, `/requirements/`, `/logs/`
- **COORDINATION REQUIRED:** Must notify `system-architect` for any new core modules

**`data-analytics-expert` (Data Processing Specialist):**
- ✅ **FULL ACCESS:** `/src/processors/data_processor.py`, `/src/processors/validator.py`, `/src/models/`
- ✅ **SHARED ACCESS:** `/src/utils/data_utils.py`, `/src/utils/file_utils.py` (coordinate with feature-developer)
- ❌ **PROHIBITED:** `/src/data_collectors/`, `/config/`, `/docs/`, `/scripts/`
- **COORDINATION REQUIRED:** Must coordinate with `feature-developer` for data model changes

#### **Architecture & Quality Agents:**

**`system-architect` (Architecture Authority):**
- ✅ **FULL ACCESS:** `/docs/ARCHITECTURE.md`, `/docs/API_SPECIFICATIONS.md`, `/docs/DATA_SCHEMA.md`
- ✅ **READ-ONLY:** All `/src/` directories for validation purposes
- ❌ **PROHIBITED:** Direct code creation in `/src/`, `/config/`, `/scripts/`
- **COORDINATION REQUIRED:** Must approve all architectural changes by other agents

**`test-strategist` (Testing Authority):**
- ✅ **FULL ACCESS:** `/tests/` (all subdirectories), `/tests/conftest.py`
- ✅ **CREATE ONLY:** Test-related files (`test_*.py`, `*_test.py`)
- ❌ **PROHIBITED:** `/src/` implementation files, `/config/`, `/docs/`
- **COORDINATION REQUIRED:** Must coordinate test structure with `feature-developer`

**`qa-production-validator` (Quality Gate Authority):**
- ✅ **READ-ONLY:** All directories for validation purposes
- ✅ **FULL ACCESS:** `/logs/` for quality reports, validation documentation
- ❌ **PROHIBITED:** Direct code modification, configuration changes
- **COORDINATION REQUIRED:** Must approve all production-bound code

#### **Infrastructure & Operations Agents:**

**`devops-engineer` (Infrastructure Authority):**
- ✅ **FULL ACCESS:** `/config/`, `/scripts/`, `/requirements/`, root-level config files (`.gitignore`, `setup.py`)
- ✅ **CREATE ONLY:** Infrastructure files (Docker, CI/CD, deployment scripts)
- ❌ **PROHIBITED:** `/src/` source code, `/tests/` test files, `/docs/` documentation
- **COORDINATION REQUIRED:** Must coordinate infrastructure changes with `system-architect`

**`performance-engineer` (Performance Authority):**
- ✅ **FULL ACCESS:** `/logs/performance.log`, `/config/logging_config.yaml`, `/scripts/monitoring/`
- ✅ **READ-ONLY:** `/src/` for performance analysis
- ❌ **PROHIBITED:** Source code modification, business logic changes
- **COORDINATION REQUIRED:** Must coordinate performance requirements with `system-architect`

#### **Documentation & Compliance Agents:**

**`documentation-specialist` (Documentation Authority):**
- ✅ **FULL ACCESS:** `/docs/` (excluding ARCHITECTURE.md, API_SPECIFICATIONS.md, DATA_SCHEMA.md)
- ✅ **CREATE ONLY:** `README.md`, `CHANGELOG.md`, user guides, troubleshooting docs
- ❌ **PROHIBITED:** `/src/`, `/config/`, `/scripts/`, technical specifications
- **COORDINATION REQUIRED:** Must coordinate with `system-architect` for technical documentation

**`security-auditor` (Security Authority):**
- ✅ **READ-ONLY:** All directories for security validation
- ✅ **FULL ACCESS:** Security-related documentation, audit reports
- ❌ **PROHIBITED:** Direct code modification, configuration changes
- **COORDINATION REQUIRED:** Must approve all security-sensitive implementations

### 2. File-Type Restrictions

#### **Python Code Files (*.py):**
- ✅ **AUTHORIZED:** `feature-developer`, `data-analytics-expert` (in designated directories only)
- ❌ **PROHIBITED:** All documentation, infrastructure, and quality agents
- **EXCEPTION:** `test-strategist` can create `test_*.py` files in `/tests/` only

#### **Configuration Files (*.json, *.yaml, *.env):**
- ✅ **AUTHORIZED:** `devops-engineer` (exclusive authority)
- ❌ **PROHIBITED:** All other agents without explicit coordination
- **EXCEPTION:** `performance-engineer` can modify monitoring configs with `devops-engineer` approval

#### **Documentation Files (*.md):**
- ✅ **AUTHORIZED:** `documentation-specialist` (general docs), `system-architect` (technical specs)
- ❌ **PROHIBITED:** Implementation agents creating documentation without request
- **EXCEPTION:** `feature-developer` can create implementation notes in `/src/` comments only

#### **Test Files (test_*.py, *_test.py):**
- ✅ **AUTHORIZED:** `test-strategist` (exclusive authority)
- ❌ **PROHIBITED:** All other agents creating test files
- **COORDINATION:** `feature-developer` must provide test requirements to `test-strategist`

#### **Script Files (*.sh, *.bat, setup.py):**
- ✅ **AUTHORIZED:** `devops-engineer` (exclusive authority)
- ❌ **PROHIBITED:** All other agents
- **EXCEPTION:** System-critical scripts require `system-architect` approval

#### **Data Files (*.tsv, *.csv, *.json for data):**
- ✅ **AUTHORIZED:** `data-analytics-expert` (processing), `feature-developer` (collection)
- ❌ **PROHIBITED:** Infrastructure and documentation agents
- **LOCATION:** Must be stored in `/data/` subdirectories only

### 3. Cross-Agent Coordination Rules

#### **MANDATORY Coordination (24-hour advance notice):**

**Database Schema Changes:**
- **PRIMARY:** `data-analytics-expert`
- **REQUIRED COORDINATION:** `feature-developer`, `system-architect`
- **PROCESS:** Schema proposal → architecture review → implementation approval → testing validation

**API Integration Changes:**
- **PRIMARY:** `feature-developer`
- **REQUIRED COORDINATION:** `system-architect`, `security-auditor`, `performance-engineer`
- **PROCESS:** API spec → security review → performance impact → architecture approval

**Infrastructure Configuration:**
- **PRIMARY:** `devops-engineer`
- **REQUIRED COORDINATION:** `system-architect`, `security-auditor`, `performance-engineer`
- **PROCESS:** Config proposal → security audit → performance review → deployment approval

#### **STANDARD Coordination (4-hour advance notice):**

**New Module Creation:**
- **PRIMARY:** `feature-developer`
- **REQUIRED COORDINATION:** `system-architect`, `test-strategist`
- **PROCESS:** Module spec → architecture review → test strategy → implementation

**Data Processing Changes:**
- **PRIMARY:** `data-analytics-expert`
- **REQUIRED COORDINATION:** `feature-developer`, `qa-production-validator`
- **PROCESS:** Processing logic → integration impact → quality validation

**Performance Optimization:**
- **PRIMARY:** `performance-engineer`
- **REQUIRED COORDINATION:** `feature-developer`, `system-architect`
- **PROCESS:** Performance analysis → implementation impact → architecture alignment

#### **IMMEDIATE Coordination (1-hour advance notice):**

**Bug Fixes:**
- **PRIMARY:** `feature-developer`
- **REQUIRED COORDINATION:** `test-strategist`, `qa-production-validator`
- **PROCESS:** Issue identification → fix implementation → testing validation

**Documentation Updates:**
- **PRIMARY:** `documentation-specialist`
- **REQUIRED COORDINATION:** Relevant technical agent
- **PROCESS:** Content review → technical validation → publication

**Monitoring Adjustments:**
- **PRIMARY:** `performance-engineer`
- **REQUIRED COORDINATION:** `devops-engineer`
- **PROCESS:** Monitoring need → configuration update → deployment

### 4. Escalation Procedures

#### **LEVEL 1: Directory Violation (IMMEDIATE HALT)**

**Trigger:** Agent attempts to create files outside authorized directories
**Response Time:** < 2 minutes
**Actions:**
1. **AUTOMATIC:** Agent detects unauthorized directory access attempt
2. **IMMEDIATE:** Agent reports to user: "I cannot create files in [directory] - this is outside my authorized scope"
3. **USER ACTION REQUIRED:** User clarifies task scope or assigns to correct agent
4. **RESOLUTION:** Task reassignment or scope clarification provided
5. **OUTCOME:** Correct agent proceeds with authorized file creation

**Example Scenarios:**
- `feature-developer` attempts to create config files
- `documentation-specialist` tries to modify source code
- `test-strategist` attempts to create production scripts

#### **LEVEL 2: File-Type Violation (IMMEDIATE REVIEW)**

**Trigger:** Agent creates unauthorized file types
**Response Time:** < 10 minutes
**Actions:**
1. **IMMEDIATE:** Agent asks user: "I'm about to create [file type] which is outside my normal scope - should I proceed?"
2. **USER DECISION:** User confirms if file creation is intentional or assigns to proper agent
3. **VALIDATION:** User verifies the file type matches the intended task
4. **OUTCOME:** File creation proceeds with user approval or task reassigned to proper agent

**Example Scenarios:**
- Implementation agent creating documentation without request
- Infrastructure agent modifying source code
- Quality agent creating configuration files

#### **LEVEL 3: Coordination Failure (WORKFLOW DISRUPTION)**

**Trigger:** Required coordination not completed within timeframes
**Response Time:** < 30 minutes
**Actions:**
1. **IMMEDIATE:** Agent reports: "I need [specific coordination] before I can proceed with this task"
2. **USER COORDINATION:** User facilitates coordination between agents or provides missing information
3. **DEPENDENCY RESOLUTION:** Required approvals/coordination completed
4. **OUTCOME:** Agent proceeds with task once dependencies resolved

**Example Scenarios:**
- Database changes without architecture review
- API modifications without security audit
- Infrastructure updates without performance assessment

#### **LEVEL 4: Architectural Conflict (CRITICAL ESCALATION)**

**Trigger:** Agent implementations conflict with system architecture
**Response Time:** < 15 minutes
**Actions:**
1. **IMMEDIATE:** Agent stops and reports: "This implementation conflicts with [architectural constraint] - I need guidance"
2. **USER REVIEW:** User reviews the conflict and determines resolution approach
3. **ARCHITECTURE DECISION:** User provides architectural guidance or modifies requirements
4. **OUTCOME:** Agent proceeds with clarified architecture or task is redesigned

**Example Scenarios:**
- Implementation violates security requirements
- Data processing conflicts with performance standards
- Integration breaks existing system architecture

#### **LEVEL 5: Production Risk (EMERGENCY RESPONSE)**

**Trigger:** Actions that could compromise production systems or data integrity
**Response Time:** IMMEDIATE (< 5 minutes)
**Actions:**
1. **IMMEDIATE:** Agent halts all operations and reports: "CRITICAL: This action could corrupt data or break the system"
2. **USER INTERVENTION:** User immediately reviews the proposed action and its implications
3. **RISK ASSESSMENT:** User determines if action should proceed, be modified, or cancelled
4. **OUTCOME:** Safe resolution implemented or dangerous action prevented

**Example Scenarios:**
- Unauthorized access to production configurations
- Data corruption risk in processing pipelines
- Security vulnerability introduction

### **Enforcement Implementation:**

#### **Automated Enforcement:**
- **File System Monitoring:** Real-time directory access validation
- **Permission Checking:** Automated agent authority verification
- **Violation Detection:** Immediate notification system for unauthorized actions
- **Workflow Blocking:** Automatic prevention of unauthorized file creation

#### **Manual Oversight:**
- **Daily Compliance Review:** `qa-production-validator` reviews all file creation
- **Weekly Architecture Audit:** `system-architect` validates system coherence
- **Monthly Process Assessment:** `darwin-orchestrator` reviews enforcement effectiveness

#### **Code Execution Safeguards:**
- **Level 1-2:** Agent reports violation attempt and requests user clarification before proceeding
- **Level 3-4:** Agent halts current task and explains coordination requirements needed to user
- **Level 5:** Agent stops all file operations and requests user intervention to resolve conflicts

#### **Code Quality Protection:**
- **Conflict Prevention:** Agents self-check authorization before file creation to prevent code conflicts
- **File Collision Detection:** Agents verify no other agent is modifying same directory/file simultaneously
- **Dependency Validation:** Agents confirm required coordination completed before making changes
- **Scope Verification:** Agents request user confirmation for actions outside their designated scope

**This framework ensures zero-conflict parallel execution while maintaining clean code organization and preventing implementation conflicts.**

---

## Technical Specifications

**Production-Grade File Structure:**
```
C:\Users\Ochow\Desktop\Darwin.v1\youniverse_expansion\
├── docs/
│   ├── YouNiverse_Dataset_Enrichment_PRD.md (this document)
│   ├── ARCHITECTURE.md (system architecture documentation)
│   ├── API_SPECIFICATIONS.md (YouTube API & third-party integrations)
│   ├── DATA_SCHEMA.md (dataset schema definitions)
│   ├── IMPLEMENTATION_GUIDE.md (development guidelines)
│   └── TROUBLESHOOTING.md (common issues and solutions)
├── config/
│   ├── .env.example (environment variable template)
│   ├── logging_config.yaml (structured logging configuration)
│   ├── rate_limits.json (API rate limiting parameters)
│   └── data_quality_rules.json (validation rule definitions)
├── src/
│   ├── __init__.py
│   ├── core/
│   │   ├── __init__.py
│   │   ├── config.py (configuration management)
│   │   ├── logging.py (centralized logging setup)
│   │   ├── exceptions.py (custom exception classes)
│   │   └── constants.py (application constants)
│   ├── data_collectors/
│   │   ├── __init__.py
│   │   ├── base_collector.py (abstract base class)
│   │   ├── youtube_api_collector.py (Phase 1: API enrichment)
│   │   ├── wayback_charts_collector.py (Phase 2A: historical charts)
│   │   ├── wayback_uploads_collector.py (Phase 2B: upload data)
│   │   ├── viewstats_collector.py (Phase 4: current data)
│   │   └── api_current_collector.py (Phase 5: final API update)
│   ├── processors/
│   │   ├── __init__.py
│   │   ├── data_processor.py (core data processing logic)
│   │   ├── merger.py (dataset merging and alignment)
│   │   ├── validator.py (data quality validation)
│   │   └── enricher.py (metadata enrichment logic)
│   ├── models/
│   │   ├── __init__.py
│   │   ├── channel.py (channel data model)
│   │   ├── timeseries.py (timeseries record model)
│   │   ├── metadata.py (metadata model)
│   │   └── quality_report.py (quality metrics model)
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── api_helpers.py (API authentication & rate limiting)
│   │   ├── scraping_utils.py (anti-bot & stealth utilities)
│   │   ├── data_utils.py (data transformation utilities)
│   │   ├── file_utils.py (file I/O operations)
│   │   ├── date_utils.py (date/time processing)
│   │   └── retry_utils.py (exponential backoff & retry logic)
│   └── main.py (entry point orchestration)
├── tests/
│   ├── __init__.py
│   ├── conftest.py (pytest configuration)
│   ├── unit/
│   │   ├── test_collectors/
│   │   ├── test_processors/
│   │   ├── test_models/
│   │   └── test_utils/
│   ├── integration/
│   │   ├── test_api_integration.py
│   │   ├── test_data_flow.py
│   │   └── test_end_to_end.py
│   └── fixtures/
│       ├── sample_data/
│       └── mock_responses/
├── data/
│   ├── input/
│   │   └── df_timeseries_en.tsv (original dataset)
│   ├── intermediate/
│   │   ├── enriched_baseline_timeseries.tsv (Phase 1 output)
│   │   ├── historical_charts_2019_2022.tsv (Phase 2A output)
│   │   ├── historical_uploads_2019_2022.tsv (Phase 2B output)
│   │   └── current_viewstats_data.tsv (Phase 4 output)
│   ├── output/
│   │   ├── youniverse_enriched_2015_current.tsv (final dataset)
│   │   └── incomplete_channels_timeseries.tsv (partial data)
│   └── quality/
│       ├── validation_reports/
│       ├── error_logs/
│       └── collection_metrics/
├── logs/
│   ├── application.log
│   ├── error.log
│   ├── data_collection.log
│   └── performance.log
├── scripts/
│   ├── setup_environment.py (initial setup automation)
│   ├── validate_prerequisites.py (environment validation)
│   ├── run_full_pipeline.py (complete pipeline execution)
│   └── monitoring/
│       ├── check_progress.py (progress monitoring)
│       └── generate_reports.py (quality report generation)
├── requirements/
│   ├── base.txt (core dependencies)
│   ├── dev.txt (development dependencies)
│   └── prod.txt (production dependencies)
├── .gitignore
├── README.md (project overview & quick start)
├── CHANGELOG.md (version history)
├── PROJECT_STATUS.md (implementation tracking)
└── setup.py (package installation)
```

**Enhanced Schema:**
```
channel | category | datetime | views | delta_views | subs | delta_subs | 
videos | delta_videos | activity | total_longform_views | total_shorts_views |
channel_name | collection_source | data_quality_flag
```

**Data Sources Priority:**
1. **YouTube Data API v3** (channel metadata) - Day 1
2. **Wayback Machine API + Social Blade scraping** (2019-2022) - Day 2  
3. **ViewStats API/scraping** (2022-2025) - Day 3

**Rate Limiting Strategy:**
- YouTube API: 10,000 units/day (manage with batch requests)
- Wayback Machine: 1 request/second (respectful scraping)
- ViewStats: 2 requests/second (avoid overloading)

---

## Detailed Implementation Steps

### Step 1: Environment Setup
```bash
# Install required packages
pip install pandas numpy requests beautifulsoup4 python-dotenv google-api-python-client

# Create .env file with YouTube API credentials
YOUTUBE_API_KEY=your_youtube_api_key_here
```

### Step 2: Data Loading & Validation
```python
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Load existing dataset
df_original = pd.read_csv('C:/Users/Ochow/Desktop/Darwin.v1/YT Datasets/df_timeseries_en.tsv/df_timeseries_en.tsv', 
                         sep='\t', parse_dates=['datetime'])

# Validate schema and extract unique channels
channels = df_original['channel'].unique()
print(f"Total channels to process: {len(channels)}")
print(f"Date range: {df_original['datetime'].min()} to {df_original['datetime'].max()}")
```

### Step 3: YouTube API Integration
```python
from googleapiclient.discovery import build
import os
from dotenv import load_dotenv

load_dotenv()
youtube = build('youtube', 'v3', developerKey=os.getenv('YOUTUBE_API_KEY'))

def get_channel_metadata_batch(channel_ids):
    """Retrieve channel metadata in batches of 50"""
    request = youtube.channels().list(
        part='snippet,statistics,status',
        id=','.join(channel_ids[:50])
    )
    response = request.execute()
    return response
```

### Step 4: Wayback Machine Integration
```python
import requests
from bs4 import BeautifulSoup
import time

def get_wayback_snapshots(channel_id, start_date, end_date):
    """Get available Wayback Machine snapshots for Social Blade page"""
    url = f"http://web.archive.org/cdx/search/cdx"
    params = {
        'url': f'socialblade.com/youtube/channel/{channel_id}',
        'from': start_date.strftime('%Y%m%d'),
        'to': end_date.strftime('%Y%m%d'),
        'output': 'json'
    }
    response = requests.get(url, params=params)
    return response.json() if response.status_code == 200 else []
```

### Step 5: ViewStats Integration
```python
def scrape_viewstats_data(channel_id):
    """Scrape ViewStats for current metrics including shorts/longform breakdown"""
    base_url = f"https://www.viewstats.com/youtube-channel-stats/{channel_id}"
    # Implementation will depend on ViewStats structure
    # Focus on total views, subscribers, longform vs shorts data
    pass
```

---

## Success Criteria & Validation

**Quantitative Metrics:**
- **Dataset temporal coverage:** 2015-2025 (10-year span achieved)
- **Channel coverage:** >90% of original 153,550 channels with some enrichment
- **Historical extension:** >70% channels with 2019-2022 data from Wayback Machine
- **Current extension:** >80% channels with 2022-2025 data from ViewStats
- **Data quality:** <5% missing or invalid data points in final dataset

**Deliverable Requirements:**
- Single consolidated dataset file ready for analysis
- Comprehensive data quality documentation
- Collection methodology documentation for reproducibility
- Error handling and retry mechanisms implemented
- Validated data continuity across time periods

**Technical Validation Checklist:**
- [ ] No data corruption in existing historical records (2015-2019)
- [ ] Consistent schema across all time periods
- [ ] Logical progression of cumulative metrics (views, subscribers never decrease unreasonably)
- [ ] Proper handling of channel terminations/suspensions/inactivity
- [ ] New columns (longform/shorts views) properly populated where available
- [ ] Junction points validated (Sep 2019, Aug 2022 transitions)

---

## Risk Mitigation & Contingency Plans

**API Limitations:**
- **YouTube API quota exceeded:** 
  - Implement daily quota tracking dashboard
  - Batch optimization: request exactly 50 channels per call
  - Spread requests across multiple days if needed
- **Wayback Machine rate limiting:** 
  - Implement 1-second delays between requests
  - Fallback to monthly snapshots if weekly unavailable
- **ViewStats access blocked:** 
  - Prepare Social Blade current data as backup
  - Consider direct YouTube channel page scraping as last resort

**Data Quality Risks:**
- **Missing Wayback archives:** 
  - Document gaps clearly, don't interpolate
  - Maintain source attribution for all data points
- **Inconsistent metrics between sources:** 
  - Flag data quality issues with source tags
  - Provide confidence scores for different data sources
- **Schema inconsistencies:** 
  - Maintain strict backward compatibility
  - Add new columns only, never modify existing structure

**Implementation Risks:**
- **Time constraints (3-day limit):** 
  - Prioritize core functionality over edge cases
  - Implement basic error handling first, enhance later
  - Focus on 80/20 rule: 80% coverage with 20% effort
- **Service availability issues:** 
  - Implement robust retry logic with exponential backoff
  - Create checkpoint saves after each major step
- **Data corruption during processing:** 
  - Create backup copies before each processing stage
  - Implement data integrity checks at each step

---

## Quality Assurance Framework

**Data Validation Rules:**
1. **Temporal Consistency:** 
   - No negative deltas unless channel reset/manipulation detected
   - Logical progression of cumulative metrics
   - Date continuity without unexplained gaps

2. **Cross-Source Validation:**
   - Compare overlapping periods between data sources
   - Validate subscriber/view counts at junction points
   - Flag significant discrepancies for manual review

3. **Statistical Validation:**
   - Outlier detection using IQR method
   - Growth rate reasonableness checks
   - Category-specific validation rules

**Error Classification:**
- **Critical:** Data corruption, schema violations, temporal inconsistencies
- **Major:** Missing data for high-priority channels, significant metric discrepancies
- **Minor:** Individual channel access failures, minor formatting issues

---

## Post-Implementation Documentation

**Required Documentation:**
1. **Data Collection Report:**
   - Channel coverage statistics by time period
   - Success/failure rates by data source
   - Quality metrics and validation results

2. **Methodology Documentation:**
   - Detailed scraping procedures and parameters
   - API usage patterns and optimization strategies
   - Data processing and validation logic

3. **User Guide:**
   - Dataset schema explanation
   - Data quality flags and interpretation
   - Recommended analysis approaches

4. **Maintenance Procedures:**
   - Future update procedures
   - Data validation checklist
   - Error resolution protocols

---

## Final Deliverables

**Primary Output:**
- `youniverse_enriched_2015_2025.tsv` - Complete enriched dataset spanning 10 years

**Supporting Files:**
- `channel_metadata_2025.json` - Current channel information mapping
- `data_quality_report.json` - Comprehensive quality assessment
- `collection_methodology.md` - Detailed implementation documentation
- `error_log.txt` - Complete log of failed collections and reasons

**Validation Reports:**
- Coverage statistics by time period and data source
- Data quality metrics and validation results
- Junction point continuity analysis
- Statistical summary of enhanced dataset

This PRD provides the complete framework for a focused, executable 3-day implementation that transforms the existing YouNiverse dataset into a comprehensive 10-year creator analytics database suitable for investment fund analysis.