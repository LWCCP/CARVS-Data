"""
ViewStats Historical Data Collector
Phase 3: Historical weekly data collection (August 2022 - Present)

This collector extracts weekly historical data from ViewStats graphs including
total views, subscribers, longform views, and shorts views. Uses JavaScript
rendering to access graph data and simulate max button functionality to get
complete historical time series data.

Author: feature-developer
"""

import sys
import time  
import json
import re
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urljoin, quote
from bs4 import BeautifulSoup

from src.data_collectors.base_collector import BaseCollector, CollectionProgress
from src.models.channel import Channel
from src.utils.scraping_utils import AntiDetectionScraper, JavaScriptRenderer
from src.utils.api_helpers import RateLimiter
from src.core.config import config_manager

# Enhanced imports for stealth collection
try:
    import undetected_chromedriver as uc
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
    STEALTH_AVAILABLE = True
except ImportError:
    STEALTH_AVAILABLE = False
    print("Warning: undetected-chromedriver not available. Install with: pip install undetected-chromedriver")


class ViewStatsCollector(BaseCollector):
    """
    ViewStats collector for historical weekly data (Aug 2022 - Present)
    
    Collects weekly time series data including total views, subscribers,
    longform views, and shorts views by extracting graph data from ViewStats.
    Uses JavaScript rendering and anti-bot detection to access complete
    historical datasets with 157 weekly data points per channel.
    """
    
    def __init__(self):
        super().__init__(
            collector_name="ViewStatsCollector",
            service_name="viewstats",
            batch_size=10,  # Smaller batches for JavaScript rendering
            max_workers=1,  # Single worker for anti-bot detection
            checkpoint_interval=50,  # Checkpoint every 50 channels
            enable_checkpoints=True
        )
        self.phase_name = "Phase 3: ViewStats Historical"
        
        # Get configuration
        config = config_manager.get_viewstats_config()
        
        # Initialize JavaScript renderer for graph data extraction
        self.js_renderer = JavaScriptRenderer(
            headless=True,
            timeout=30
        )
        
        # Initialize scraping utilities with enhanced anti-bot detection
        self.scraping_utils = AntiDetectionScraper(
            base_delay=2.0,  # Slower for JavaScript rendering
            delay_variance=1.0,
            max_retries=3,
            respect_robots_txt=False  # ViewStats doesn't need robots.txt compliance
        )
        
        # Rate limiting: 2 requests per second (as per PRD)
        self.rate_limiter = RateLimiter(
            requests_per_second=config.rate_limiting.requests_per_second,
            burst_allowance=config.rate_limiting.burst_allowance
        )
        
        # Target date range: Aug 2022 - Yesterday (as per PRD)
        self.start_date = datetime(2022, 8, 1)
        self.end_date = datetime.now() - timedelta(days=1)  # Yesterday per PRD
        self.target_weeks = round((self.end_date - self.start_date).days / 7)  # 157 weeks
        
        # ViewStats URL patterns
        self.viewstats_base_url = "https://viewstats.com"
        
        # ViewStats-specific data extraction patterns (updated for dynamic content)
        self.extraction_patterns = {
            'total_views_patterns': [
                # ViewStats specific patterns with flexible spacing and tags
                r'<div[^>]*class="[^"]*stats-key[^"]*"[^>]*>Total Views</div>[^<]*<[^>]*class="[^"]*value[^"]*"[^>]*>([0-9,KMB.]+)',
                r'<div[^>]*class="[^"]*stats-key[^"]*"[^>]*>Views</div>[^<]*<[^>]*>([0-9,KMB.]+)</[^>]*>',
                r'stats-key[^>]*>(?:Total )?Views</div>[^<]*<[^>]*>([0-9,KMB.]+)',
                # Data attribute patterns
                r'data-metric="total.views"[^>]*data-value="([0-9,KMB.]+)"',
                r'data-value="([0-9,KMB.]+)"[^>]*data-metric="total.views"',
                # Strong number patterns for any views
                r'<strong[^>]*>([0-9,KMB.]+)</strong>[^<]*(?:total\s+)?views?',
                r'(?:total\s+)?views?[^<]*<strong[^>]*>([0-9,KMB.]+)</strong>',
                # Generic patterns
                r'([0-9,]+(?:\.[0-9]+)?[KMB]?)\s*(?:total\s+)?views?',
                r'(?:total\s+)?views?[^\w]*([0-9,]+(?:\.[0-9]+)?[KMB]?)',
            ],
            'subscriber_patterns': [
                # ViewStats subscriber patterns with flexible spacing
                r'<div[^>]*class="[^"]*stats-key[^"]*"[^>]*>Subscribers</div>[^<]*<[^>]*class="[^"]*value[^"]*"[^>]*>([0-9,KMB.]+)',
                r'stats-key[^>]*>Subscribers</div>[^<]*<[^>]*>([0-9,KMB.]+)</[^>]*>',
                # Data attribute patterns
                r'data-metric="subscribers?"[^>]*data-value="([0-9,KMB.]+)"',
                r'data-value="([0-9,KMB.]+)"[^>]*data-metric="subscribers?"',
                # Strong number patterns near subscriber text
                r'<strong[^>]*>([0-9,KMB.]+)</strong>[^<]*subscribers?',
                r'subscribers?[^<]*<strong[^>]*>([0-9,KMB.]+)</strong>',
                # Number followed by subscriber text
                r'([0-9,]+(?:\.[0-9]+)?[KMB]?)\s*(?:subscribers?|subs?)',
                r'(?:subscribers?|subs?)[^\w]*([0-9,]+(?:\.[0-9]+)?[KMB]?)',
                # Look for M or K indicators near subscriber text
                r'([0-9.]+[KMB])[^a-zA-Z]*subscribers?',
                r'subscribers?[^a-zA-Z]*([0-9.]+[KMB])',
            ],
            'longform_patterns': [
                # ViewStats longform patterns
                r'<div[^>]*class="[^"]*stats-key[^"]*"[^>]*>(?:Longform|Long[- ]?form) Views</div>[^<]*<[^>]*class="[^"]*value[^"]*"[^>]*>([0-9,KMB.]+)',
                r'stats-key[^>]*>(?:Longform|Long[- ]?form)[^<]*</div>[^<]*<[^>]*>([0-9,KMB.]+)',
                r'data-metric="longform"[^>]*data-value="([0-9,KMB.]+)"',
                r'<strong[^>]*>([0-9,KMB.]+)</strong>[^<]*longform',
                r'longform[^<]*<strong[^>]*>([0-9,KMB.]+)</strong>',
                r'([0-9,]+(?:\.[0-9]+)?[KMB]?)[^<]*longform',
            ],
            'shorts_patterns': [
                # ViewStats shorts patterns
                r'<div[^>]*class="[^"]*stats-key[^"]*"[^>]*>Shorts Views</div>[^<]*<[^>]*class="[^"]*value[^"]*"[^>]*>([0-9,KMB.]+)',
                r'<div[^>]*class="[^"]*stats-key[^"]*"[^>]*>Shorts</div>[^<]*<[^>]*class="[^"]*value[^"]*"[^>]*>([0-9,KMB.]+)',
                r'stats-key[^>]*>Shorts[^<]*</div>[^<]*<[^>]*>([0-9,KMB.]+)',
                r'data-metric="shorts"[^>]*data-value="([0-9,KMB.]+)"',
                r'<strong[^>]*>([0-9,KMB.]+)</strong>[^<]*shorts?',
                r'shorts?[^<]*<strong[^>]*>([0-9,KMB.]+)</strong>',
                r'([0-9,]+(?:\.[0-9]+)?[KMB]?)[^<]*shorts?',
            ]
        }
        
        # Initialize stealth browser if available
        self.stealth_driver = None
        if STEALTH_AVAILABLE:
            self.logger.info("Stealth browser capabilities available - enhanced anti-bot detection")
        else:
            self.logger.warning("Stealth browser not available - using fallback methods only")
        
        self.logger.info(f"Initialized {self.collector_name} for date range {self.start_date.date()} to {self.end_date.date()}")
    
    def get_collection_phase_name(self) -> str:
        """Get the name of this collection phase"""
        return "phase3_viewstats_current"
    
    def _process_single_channel(self, channel: Channel) -> Optional[Channel]:
        """
        Process a single channel for BaseCollector compatibility
        
        Args:
            channel: Channel to process
            
        Returns:
            Processed channel or None if failed
        """
        try:
            result = self.collect_channel_data(channel)
            
            if result['success'] and result['current_data']:
                # Update channel with current data
                if hasattr(channel, 'viewstats_data'):
                    channel.viewstats_data.update(result['current_data'])
                else:
                    channel.viewstats_data = result['current_data']
                
                # Add collection source
                channel.add_collection_source('viewstats_current', {
                    'collection_date': result['current_data'].get('collection_date'),
                    'has_longform_shorts_breakdown': result.get('has_breakdown', False),
                    'url_used': result.get('successful_url', 'unknown')
                })
                
                channel.processing_status = "enriched"
                channel.processing_notes = "Successfully collected ViewStats data"
                return channel
            else:
                self.logger.warning(f"No ViewStats data collected for {channel.channel_id}")
                channel.processing_status = "failed"
                channel.processing_notes = result.get('error', 'No ViewStats data collected')
                return channel
                
        except Exception as e:
            self.logger.error(f"Failed to process channel {channel.channel_id}: {e}")
            channel.processing_status = "failed"
            channel.processing_notes = f"Processing error: {e}"
            return channel
    
    def collect_channel_data(self, channel: Channel) -> Dict[str, Any]:
        """
        Collect historical weekly ViewStats data for a single channel
        
        Args:
            channel: Channel object to collect data for
            
        Returns:
            Dictionary containing weekly historical data from Aug 2022 to present
        """
        start_time = time.time()
        result = {
            'channel_id': channel.channel_id,
            'collection_phase': 'phase3_viewstats_historical',
            'collection_timestamp': datetime.now().isoformat(),
            'success': False,
            'error': None,
            'data_points_collected': 0,
            'weeks_covered': 0,
            'historical_data': [],  # Weekly data points
            'data_quality': 'unknown',
            'successful_url': None,
            'junction_points': {
                'aug_2022_data': None,
                'latest_data': None
            }
        }
        
        try:
            self.logger.info(f"Starting ViewStats historical collection for {channel.channel_id}")
            
            # Step 1: Access ViewStats page with JavaScript rendering
            viewstats_url = f"{self.viewstats_base_url}/channel/{channel.channel_id}"
            
            # Step 2: Extract historical graph data (Aug 2022 - present)
            historical_data = self._extract_viewstats_historical_data(viewstats_url, channel)
            
            if historical_data:
                result['historical_data'] = historical_data
                result['data_points_collected'] = len(historical_data)
                result['successful_url'] = viewstats_url
                
                # Calculate weeks covered
                if historical_data:
                    dates = [datetime.fromisoformat(dp['date']) for dp in historical_data]
                    weeks_span = (max(dates) - min(dates)).days / 7
                    result['weeks_covered'] = int(weeks_span)
                
                # Validate junction points for data continuity
                result['junction_points'] = self._validate_junction_points(historical_data)
                
                # Assess data quality
                result['data_quality'] = self._assess_data_quality(historical_data, result['weeks_covered'])
                
                result['success'] = True
                self.logger.info(f"Successfully collected {len(historical_data)} weekly data points covering {result['weeks_covered']} weeks")
            else:
                result['error'] = "No historical data could be extracted from ViewStats graphs"
        
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"ViewStats historical collection failed for {channel.channel_id}: {e}")
        
        finally:
            result['collection_duration'] = time.time() - start_time
        
        return result
    
    def _extract_viewstats_historical_data(self, viewstats_url: str, channel: Channel) -> List[Dict[str, Any]]:
        """
        Extract historical weekly data from ViewStats graphs using JavaScript rendering
        
        This method:
        1. Loads ViewStats page with JavaScript rendering
        2. Simulates clicking the "max" button to show full historical data
        3. Extracts graph data including subscribers, total views, longform views, shorts views
        4. Converts to weekly data points from Aug 2022 to present
        
        Args:
            viewstats_url: ViewStats channel URL
            channel: Channel object for context
            
        Returns:
            List of weekly data points with subscriber/view metrics
        """
        historical_data = []
        
        try:
            self.logger.info(f"Accessing ViewStats with JavaScript rendering: {viewstats_url}")
            
            with self.rate_limiter:
                # First try stealth browser if available
                if STEALTH_AVAILABLE:
                    try:
                        page_content = self._extract_with_stealth_browser(viewstats_url)
                        if page_content:
                            self.logger.info("Successfully used stealth browser for data extraction")
                        else:
                            raise ValueError("Stealth browser extraction failed")
                    except Exception as e:
                        self.logger.warning(f"Stealth browser failed: {e}, falling back to regular browser")
                        page_content = None
                
                # Fallback to regular JavaScript renderer
                if not page_content:
                    with self.js_renderer as renderer:
                        # First, load the page and wait for chart elements
                        page_content = renderer.get_rendered_page(
                            url=viewstats_url,
                            wait_for_element='canvas, .chart, [data-chart]',
                            wait_timeout=15
                        )
                        
                        # Try to click max button to show full historical data
                        try:
                            self._click_max_button(renderer)
                            # Wait for graph to update with full data
                            time.sleep(3)
                            # Get updated page content with full data
                            page_content = renderer.driver.page_source
                        except Exception as e:
                            self.logger.warning(f"Could not click max button: {e}, using default view")
                    
                
                if not page_content:
                    raise ValueError("Failed to render ViewStats page with JavaScript")
                
                # Extract graph data from rendered content
                graph_data = self._parse_viewstats_graph_data(page_content, viewstats_url)
                
                if graph_data:
                    # Convert graph data to standardized weekly format
                    historical_data = self._standardize_viewstats_data(graph_data, channel)
                    
                    # Filter to target date range (Aug 2022 - present)
                    historical_data = self._filter_to_target_range(historical_data)
                    
                    self.logger.info(f"Extracted {len(historical_data)} weekly data points from ViewStats graphs")
                else:
                    self.logger.warning(f"No graph data found in ViewStats page: {viewstats_url}")
        
        except Exception as e:
            self.logger.error(f"Failed to extract ViewStats historical data: {e}")
            
        return historical_data
    
    def _create_stealth_driver(self):
        """Create undetected Chrome driver with stealth configuration"""
        if not STEALTH_AVAILABLE:
            self.logger.error("Stealth driver not available - install undetected-chromedriver")
            return None
            
        try:
            options = uc.ChromeOptions()
            
            # Anti-detection measures
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            
            # Stealth configuration
            options.add_argument('--disable-web-security')
            options.add_argument('--allow-running-insecure-content')
            options.add_argument('--disable-extensions')
            options.add_argument('--disable-plugins')
            options.add_argument('--disable-images')
            options.add_argument('--no-first-run')
            options.add_argument('--disable-default-apps')
            
            # Random user agent
            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ]
            user_agent = random.choice(user_agents)
            options.add_argument(f'--user-agent={user_agent}')
            
            # Window size randomization
            width = random.randint(1024, 1920)
            height = random.randint(768, 1080)
            options.add_argument(f'--window-size={width},{height}')
            options.add_argument('--headless=new')
            
            # Create driver
            driver = uc.Chrome(options=options, version_main=None)
            
            # Execute stealth scripts
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            self.logger.info(f"Created stealth Chrome driver with user agent: {user_agent}")
            return driver
            
        except Exception as e:
            self.logger.error(f"Failed to create stealth driver: {str(e)}")
            return None
    
    def _random_delay(self, min_seconds: float = 2.0, max_seconds: float = 5.0):
        """Add random delay to mimic human behavior"""
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)
    
    def _extract_with_stealth_browser(self, url: str) -> Optional[str]:
        """Extract ViewStats data using stealth browser with enhanced anti-detection"""
        stealth_driver = None
        try:
            # Create stealth driver
            stealth_driver = self._create_stealth_driver()
            if not stealth_driver:
                return None
            
            self.logger.info(f"Loading ViewStats page with stealth browser: {url}")
            
            # Navigate to URL with human-like behavior
            stealth_driver.get(url)
            self._random_delay(3, 6)
            
            # Wait for page to load completely
            wait = WebDriverWait(stealth_driver, 20)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            
            # Handle potential popups/cookies
            self._handle_popups(stealth_driver)
            
            # Try to find and click max button for full historical data
            max_button_found = self._find_and_click_max_button(stealth_driver)
            if max_button_found:
                self.logger.info("Successfully clicked max button to show full historical data")
                self._random_delay(4, 8)  # Wait for data to load
            else:
                self.logger.warning("Max button not found, using default view")
            
            # Get final page content
            page_content = stealth_driver.page_source
            self.logger.info(f"Successfully extracted page content ({len(page_content)} characters)")
            
            return page_content
            
        except Exception as e:
            self.logger.error(f"Stealth browser extraction failed: {str(e)}")
            return None
        finally:
            if stealth_driver:
                try:
                    stealth_driver.quit()
                except:
                    pass
    
    def _handle_popups(self, driver):
        """Handle cookie banners and popups"""
        popup_selectors = [
            "button[id*='cookie']",
            "button[class*='accept']",
            ".cookie-banner button",
            "#cookie-consent button",
            "button[class*='close']",
            ".modal-close",
            "[data-dismiss='modal']"
        ]
        
        for selector in popup_selectors:
            try:
                element = driver.find_element(By.CSS_SELECTOR, selector)
                if element.is_displayed() and element.is_enabled():
                    element.click()
                    self._random_delay(0.5, 1.5)
                    self.logger.debug(f"Closed popup with selector: {selector}")
                    break
            except:
                continue
    
    def _find_and_click_max_button(self, driver) -> bool:
        """Find and click max button using stealth browser"""
        max_selectors = [
            "button[data-range='max']",
            "button[data-period='max']", 
            "button[data-time='max']",
            ".time-range-max",
            ".chart-controls button:last-child",
            "button:contains('Max')",
            "button:contains('All')",
            "button:contains('All time')"
        ]
        
        # Try CSS selectors first
        for selector in max_selectors:
            try:
                if ':contains(' not in selector:
                    element = driver.find_element(By.CSS_SELECTOR, selector)
                    if element.is_displayed() and element.is_enabled():
                        element.click()
                        return True
            except:
                continue
        
        # Try XPath selectors for text-based buttons
        xpath_selectors = [
            "//button[text()='Max']",
            "//button[contains(text(), 'Max')]",
            "//button[contains(text(), 'All')]",
            "//button[contains(text(), 'All time')]",
            "//button[@data-range='max']"
        ]
        
        for xpath in xpath_selectors:
            try:
                element = driver.find_element(By.XPATH, xpath)
                if element.is_displayed() and element.is_enabled():
                    element.click()
                    return True
            except:
                continue
        
        # Fallback: try to find any button in range controls
        try:
            range_containers = driver.find_elements(By.CSS_SELECTOR, 
                '.range-selector, .time-range, .date-range, .chart-controls')
            
            for container in range_containers:
                buttons = container.find_elements(By.TAG_NAME, 'button')
                if len(buttons) > 1:
                    # Click the last button (usually max)
                    last_button = buttons[-1]
                    if last_button.is_displayed() and last_button.is_enabled():
                        last_button.click()
                        return True
        except:
            pass
        
        return False
    
    def _click_max_button(self, renderer) -> bool:
        """
        Click the max button on ViewStats graphs to show full historical data
        
        Args:
            renderer: JavaScriptRenderer instance with loaded ViewStats page
            
        Returns:
            True if max button was successfully clicked, False otherwise
        """
        try:
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            
            # Common selectors for max/all time buttons on ViewStats
            max_button_selectors = [
                'button[data-range="max"]',  # ViewStats max button
                'button[data-period="max"]',
                'button[data-time="max"]',
                'button:contains("Max")',
                'button:contains("All")',
                'button:contains("All time")',
                '.range-selector button:last-child',  # Often max is the last button
                '[data-testid="max-button"]',
                '[aria-label*="max" i]',
                '[aria-label*="all time" i]',
                # Generic button patterns near charts
                '.chart-controls button:last-child',
                '.time-range button:last-child',
                '.date-range button:last-child'
            ]
            
            wait = WebDriverWait(renderer.driver, 10)
            
            for selector in max_button_selectors:
                try:
                    self.logger.debug(f"Trying max button selector: {selector}")
                    
                    # Try CSS selector first
                    try:
                        max_button = wait.until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                        )
                        self.logger.debug(f"Found max button with selector: {selector}")
                        max_button.click()
                        self.logger.info("Successfully clicked max button to show full historical data")
                        return True
                        
                    except Exception:
                        # If CSS selector contains text, try XPath
                        if 'contains(' in selector:
                            text_match = selector.split('contains("')[1].split('")')[0]
                            xpath_selector = f"//button[contains(text(), '{text_match}')]"
                            try:
                                max_button = wait.until(
                                    EC.element_to_be_clickable((By.XPATH, xpath_selector))
                                )
                                self.logger.debug(f"Found max button with XPath: {xpath_selector}")
                                max_button.click()
                                self.logger.info("Successfully clicked max button via XPath")
                                return True
                            except Exception:
                                continue
                        continue
                        
                except Exception as e:
                    self.logger.debug(f"Max button selector '{selector}' failed: {e}")
                    continue
            
            # Fallback: try to find any button with text containing "max", "all", etc.
            try:
                all_buttons = renderer.driver.find_elements(By.TAG_NAME, 'button')
                for button in all_buttons:
                    try:
                        button_text = button.text.lower().strip()
                        button_aria = (button.get_attribute('aria-label') or '').lower()
                        
                        # Check if button likely represents max/all time period
                        max_indicators = ['max', 'all', 'all time', 'lifetime', 'total', 'forever']
                        if any(indicator in button_text or indicator in button_aria 
                               for indicator in max_indicators):
                            
                            self.logger.debug(f"Found potential max button: text='{button_text}', aria='{button_aria}'")
                            button.click()
                            self.logger.info(f"Successfully clicked max button: '{button_text}'")
                            return True
                            
                    except Exception:
                        continue
                        
            except Exception as e:
                self.logger.debug(f"Fallback button search failed: {e}")
            
            # Final fallback: look for range/time controls and click the last option
            try:
                range_containers = renderer.driver.find_elements(By.CSS_SELECTOR, 
                    '.range-selector, .time-range, .date-range, .chart-controls')
                
                for container in range_containers:
                    buttons = container.find_elements(By.TAG_NAME, 'button')
                    if len(buttons) > 1:  # Multiple range options
                        last_button = buttons[-1]  # Usually max is the last option
                        try:
                            last_button.click()
                            self.logger.info("Clicked last button in range selector (likely max)")
                            return True
                        except Exception:
                            continue
                            
            except Exception as e:
                self.logger.debug(f"Range container fallback failed: {e}")
            
            self.logger.warning("Could not find or click max button on ViewStats page")
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to click max button: {e}")
            return False
    
    def _parse_viewstats_graph_data(self, page_content: str, url: str) -> Optional[Dict[str, Any]]:
        """
        Parse graph data from ViewStats page content
        
        Looks for:
        - Chart.js data structures
        - Plotly graph data  
        - Next.js embedded data
        - JavaScript variables containing time series data
        """
        try:
            # Look for JSON data embedded in JavaScript
            json_patterns = [
                # Next.js data structures
                r'self\.__next_f\.push\(\[1,"([^"]+)"\]\)',
                r'__NEXT_DATA__\s*=\s*({.+?});',
                
                # Chart data patterns
                r'chartData\s*[:=]\s*({.+?});',
                r'graphData\s*[:=]\s*({.+?});',
                r'seriesData\s*[:=]\s*(\[.+?\]);',
                
                # Generic data patterns
                r'data\s*[:=]\s*(\{.+?"labels":.+?\});',
                r'data\s*[:=]\s*(\[.+?\]);'
            ]
            
            extracted_data = {}
            
            for pattern in json_patterns:
                matches = re.findall(pattern, page_content, re.DOTALL)
                for match in matches:
                    try:
                        # Handle Next.js encoded data
                        if pattern.startswith('self\\.__next_f\\.push'):
                            # Decode Next.js data format
                            decoded_data = self._decode_nextjs_data(match)
                            if decoded_data:
                                extracted_data.update(decoded_data)
                        else:
                            # Parse as JSON
                            data = json.loads(match)
                            if self._is_graph_data(data):
                                extracted_data['graph_data'] = data
                                break
                    except (json.JSONDecodeError, Exception) as e:
                        continue
            
            return extracted_data if extracted_data else None
            
        except Exception as e:
            self.logger.error(f"Failed to parse ViewStats graph data: {e}")
            return None
    
    def _decode_nextjs_data(self, encoded_data: str) -> Optional[Dict[str, Any]]:
        """Decode Next.js encoded data format"""
        try:
            # Next.js encodes data in a specific format - decode it
            # This is simplified - real implementation would need more robust parsing
            if 'channel' in encoded_data and ('views' in encoded_data or 'subscribers' in encoded_data):
                # Extract relevant metrics from the encoded data
                return {'nextjs_data': encoded_data}
        except Exception:
            pass
        return None
    
    def _is_graph_data(self, data: Dict[str, Any]) -> bool:
        """Check if data structure contains graph/chart data"""
        if not isinstance(data, dict):
            return False
            
        # Look for common graph data indicators
        graph_indicators = [
            'labels', 'datasets', 'series', 'data', 'x', 'y', 
            'timestamps', 'values', 'chart', 'graph'
        ]
        
        return any(indicator in data for indicator in graph_indicators)
    
    def _standardize_viewstats_data(self, graph_data: Dict[str, Any], channel: Channel) -> List[Dict[str, Any]]:
        """
        Convert ViewStats graph data to standardized weekly format
        
        Expected output format:
        {
            'date': '2022-08-07',  # ISO format
            'week_start': '2022-08-07', # Week start date
            'subscribers': 1000000,
            'total_views': 500000000,
            'longform_views': 400000000,
            'shorts_views': 100000000,
            'data_source': 'viewstats_historical',
            'collection_method': 'graph_extraction'
        }
        """
        standardized_data = []
        
        try:
            # Extract time series from graph data
            if 'graph_data' in graph_data:
                data = graph_data['graph_data']
                
                # Handle different chart data formats
                if 'datasets' in data and 'labels' in data:
                    # Chart.js format
                    standardized_data = self._parse_chartjs_format(data, channel)
                elif 'series' in data:
                    # Plotly/Highcharts format
                    standardized_data = self._parse_series_format(data, channel)
                
            elif 'nextjs_data' in graph_data:
                # Parse Next.js embedded data
                standardized_data = self._parse_nextjs_format(graph_data['nextjs_data'], channel)
                
        except Exception as e:
            self.logger.error(f"Failed to standardize ViewStats data: {e}")
        
        return standardized_data
    
    def _parse_chartjs_format(self, data: Dict[str, Any], channel: Channel) -> List[Dict[str, Any]]:
        """Parse Chart.js format data"""
        weekly_data = []
        
        try:
            labels = data.get('labels', [])
            datasets = data.get('datasets', [])
            
            # Find subscriber and view datasets
            subscriber_data = None
            total_views_data = None
            longform_views_data = None
            shorts_views_data = None
            
            for dataset in datasets:
                label = dataset.get('label', '').lower()
                dataset_data = dataset.get('data', [])
                
                if 'subscriber' in label:
                    subscriber_data = dataset_data
                elif 'total' in label and 'view' in label:
                    total_views_data = dataset_data
                elif 'longform' in label or 'long' in label:
                    longform_views_data = dataset_data
                elif 'shorts' in label or 'short' in label:
                    shorts_views_data = dataset_data
            
            # Combine data points by timestamp
            for i, label in enumerate(labels):
                if i < len(labels):
                    try:
                        # Parse date from label
                        date_str = self._parse_date_label(label)
                        if date_str:
                            data_point = {
                                'date': date_str,
                                'week_start': self._get_week_start(datetime.fromisoformat(date_str)).isoformat(),
                                'subscribers': int(subscriber_data[i]) if subscriber_data and i < len(subscriber_data) else 0,
                                'total_views': int(total_views_data[i]) if total_views_data and i < len(total_views_data) else 0,
                                'longform_views': int(longform_views_data[i]) if longform_views_data and i < len(longform_views_data) else 0,
                                'shorts_views': int(shorts_views_data[i]) if shorts_views_data and i < len(shorts_views_data) else 0,
                                'data_source': 'viewstats_historical',
                                'collection_method': 'chartjs_extraction'
                            }
                            weekly_data.append(data_point)
                    except (ValueError, IndexError) as e:
                        continue
                        
        except Exception as e:
            self.logger.error(f"Failed to parse Chart.js format: {e}")
        
        return weekly_data
    
    def _parse_series_format(self, data: Dict[str, Any], channel: Channel) -> List[Dict[str, Any]]:
        """Parse series format data (Plotly/Highcharts)"""
        # Implementation for series format parsing
        return []
    
    def _parse_nextjs_format(self, encoded_data: str, channel: Channel) -> List[Dict[str, Any]]:
        """Parse Next.js encoded data format"""
        # Implementation for Next.js data parsing
        return []
    
    def _parse_date_label(self, label: str) -> Optional[str]:
        """Parse date from various label formats"""
        try:
            # Common date formats in ViewStats
            date_formats = [
                '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', 
                '%Y%m%d', '%b %d %Y', '%B %d, %Y'
            ]
            
            for fmt in date_formats:
                try:
                    parsed_date = datetime.strptime(str(label).strip(), fmt)
                    return parsed_date.isoformat()[:10]  # YYYY-MM-DD format
                except ValueError:
                    continue
            
            # Try parsing timestamp
            if str(label).isdigit():
                timestamp = int(label)
                if timestamp > 1000000000000:  # JavaScript timestamp (milliseconds)
                    return datetime.fromtimestamp(timestamp / 1000).isoformat()[:10]
                elif timestamp > 1000000000:  # Unix timestamp (seconds)
                    return datetime.fromtimestamp(timestamp).isoformat()[:10]
                    
        except Exception:
            pass
        
        return None
    
    def _get_week_start(self, date: datetime) -> datetime:
        """Get the start of the week (Sunday) for a given date"""
        days_since_sunday = date.weekday() + 1
        if days_since_sunday == 7:
            days_since_sunday = 0
        return date - timedelta(days=days_since_sunday)
    
    def _filter_to_target_range(self, historical_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter data to target range (Aug 2022 - present)"""
        filtered_data = []
        
        for data_point in historical_data:
            try:
                point_date = datetime.fromisoformat(data_point['date'])
                if self.start_date <= point_date <= self.end_date:
                    filtered_data.append(data_point)
            except (ValueError, KeyError):
                continue
        
        return filtered_data
    
    def _validate_junction_points(self, data_points: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate junction points for data continuity"""
        junction_validation = {
            'aug_2022_data': None,
            'latest_data': None,
            'continuity_score': 0.0
        }
        
        try:
            if data_points:
                # Find earliest and latest data points
                sorted_data = sorted(data_points, key=lambda x: x['date'])
                junction_validation['aug_2022_data'] = sorted_data[0]
                junction_validation['latest_data'] = sorted_data[-1]
                
                # Simple continuity score based on data completeness
                expected_weeks = self.target_weeks
                actual_weeks = len(data_points)
                junction_validation['continuity_score'] = min(actual_weeks / expected_weeks, 1.0)
                
        except Exception as e:
            self.logger.error(f"Junction point validation failed: {e}")
        
        return junction_validation
    
    def _assess_data_quality(self, data_points: List[Dict[str, Any]], weeks_covered: int) -> str:
        """Assess overall data quality"""
        try:
            if not data_points:
                return 'no_data'
            
            coverage_ratio = weeks_covered / self.target_weeks
            
            # Check for data completeness
            complete_points = sum(1 for dp in data_points 
                                if dp.get('subscribers', 0) > 0 and dp.get('total_views', 0) > 0)
            completeness_ratio = complete_points / len(data_points)
            
            # Determine quality level
            if coverage_ratio > 0.9 and completeness_ratio > 0.9:
                return 'excellent'
            elif coverage_ratio > 0.7 and completeness_ratio > 0.8:
                return 'good'
            elif coverage_ratio > 0.5 and completeness_ratio > 0.7:
                return 'fair'
            else:
                return 'poor'
                
        except Exception:
            return 'unknown'
    
    def _get_current_channel_metrics(self, channel: Channel) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        Get current channel metrics from ViewStats only
        
        Returns:
            Tuple of (metrics_dict, successful_url) or (None, None) if failed
        """
        try:
            return self._try_viewstats(channel)
            
        except Exception as e:
            self.logger.error(f"Failed to get current metrics for {channel.channel_id}: {e}")
            return None, None
    
    def _try_viewstats(self, channel: Channel) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """Try to get metrics from ViewStats using JavaScript rendering"""
        try:
            url_attempts = []
            
            # ViewStats URL strategies
            if hasattr(channel, 'handle') and channel.handle:
                handle = channel.handle  # Keep @ symbol for ViewStats
                handle_clean = channel.handle.replace('@', '')  # Also try without @
                url_attempts.extend([
                    f"{self.viewstats_base_url}/{handle}",  # Most reliable pattern
                    f"{self.viewstats_base_url}/@{handle_clean}",
                    f"{self.viewstats_base_url}/youtube/{handle}",
                ])
            
            if hasattr(channel, 'custom_url') and channel.custom_url:
                custom = channel.custom_url.replace('@', '')
                url_attempts.extend([
                    f"{self.viewstats_base_url}/@{custom}",
                    f"{self.viewstats_base_url}/{custom}",
                ])
            
            # Try ViewStats URLs with JavaScript rendering
            for url in url_attempts:
                try:
                    with self.rate_limiter:
                        self.logger.debug(f"Attempting ViewStats with JavaScript rendering: {url}")
                        
                        # Use JavaScript renderer to get fully loaded page
                        with JavaScriptRenderer(headless=True, timeout=30) as renderer:
                            # Wait for ViewStats specific elements that contain data
                            wait_selectors = [
                                '.stats-key',  # ViewStats stats labels
                                '.value-box',  # ViewStats value containers  
                                '[data-value]',  # Elements with data-value attributes
                                '.metric-value',  # Possible metric value containers
                            ]
                            
                            rendered_html = None
                            for selector in wait_selectors:
                                try:
                                    rendered_html = renderer.get_rendered_page(url, wait_for_element=selector, wait_timeout=15)
                                    if rendered_html:
                                        self.logger.debug(f"Successfully waited for selector: {selector}")
                                        break
                                except Exception as e:
                                    self.logger.debug(f"Wait selector '{selector}' failed: {e}")
                                    continue
                            
                            # If no specific selector worked, wait longer for page to fully load
                            if not rendered_html:
                                self.logger.debug("No specific selectors worked, waiting for full page load")
                                rendered_html = renderer.get_rendered_page(url, wait_timeout=12)
                                
                                # Additional wait for dynamic content
                                import time
                                time.sleep(3)
                                rendered_html = renderer.driver.page_source if renderer.driver else rendered_html
                            
                            if rendered_html:
                                # Validate this looks like a successful ViewStats page
                                content_lower = rendered_html.lower()
                                if ('page not found' not in content_lower and 
                                    len(rendered_html) > 10000 and
                                    ('subscriber' in content_lower or 'views' in content_lower)):
                                    
                                    self.logger.debug(f"Successfully rendered ViewStats page: {len(rendered_html)} chars")
                                    
                                    # First try interactive extraction using button clicks
                                    metrics = self._extract_current_metrics_with_interaction(renderer, url)
                                    if metrics:
                                        return metrics, url
                                    
                                    # Fallback to static HTML extraction
                                    self.logger.debug("Interactive extraction failed, trying static HTML extraction")
                                    metrics = self._extract_current_metrics(rendered_html, url)
                                    if metrics:
                                        return metrics, url
                                    else:
                                        self.logger.debug("No metrics extracted from rendered HTML")
                                else:
                                    self.logger.debug(f"Rendered page doesn't look like valid ViewStats content")
                        
                except Exception as e:
                    self.logger.debug(f"ViewStats JavaScript rendering failed for {url}: {e}")
                    continue
            
            return None, None
            
        except Exception as e:
            self.logger.error(f"ViewStats collection failed: {e}")
            return None, None
    
    
    def _extract_current_metrics_with_interaction(self, renderer, url: str) -> Optional[Dict[str, Any]]:
        """
        Extract current metrics using JavaScript interactions with ViewStats
        
        Args:
            renderer: JavaScriptRenderer instance
            url: ViewStats URL
            
        Returns:
            Dictionary with extracted metrics
        """
        import time
        import re
        from selenium.webdriver.common.by import By
        
        metrics = {
            'collection_date': datetime.now().isoformat(),
            'source_url': url,
            'data_source': 'viewstats_current'
        }
        
        try:
            # Extract subscriber count by clicking subscriber button
            try:
                subscriber_buttons = renderer.driver.find_elements(By.CSS_SELECTOR, 'button[data-type="subscribers"]')
                if subscriber_buttons:
                    self.logger.debug("Clicking subscriber button for data extraction")
                    subscriber_buttons[0].click()
                    time.sleep(3)  # Wait for data to load
                    
                    sub_html = renderer.driver.page_source
                    # Look for realistic subscriber numbers (K or M suffixes)
                    sub_numbers = re.findall(r'([0-9,]+(?:\.[0-9]+)?[KM])', sub_html, re.IGNORECASE)
                    
                    # Filter for valid subscriber counts
                    valid_subs = []
                    for num_str in sub_numbers:
                        try:
                            parsed_value = self._parse_number(num_str)
                            if parsed_value and 1000 <= parsed_value <= 500_000_000:  # Reasonable subscriber range
                                valid_subs.append((parsed_value, num_str))
                        except:
                            continue
                    
                    if valid_subs:
                        # Take the most reasonable subscriber count
                        best_sub = max(valid_subs, key=lambda x: x[0])
                        metrics['subscriber_count'] = best_sub[0]
                        self.logger.debug(f"Extracted subscriber count: {best_sub[1]} -> {best_sub[0]:,}")
                        
            except Exception as e:
                self.logger.debug(f"Subscriber extraction failed: {e}")
            
            # Extract view count by clicking views button
            try:
                view_buttons = renderer.driver.find_elements(By.CSS_SELECTOR, 'button[data-type="views"]')
                if view_buttons:
                    self.logger.debug("Clicking views button for data extraction")
                    view_buttons[0].click()
                    time.sleep(3)  # Wait for data to load
                    
                    view_html = renderer.driver.page_source
                    # Look for realistic view numbers (M or B suffixes)
                    view_numbers = re.findall(r'([0-9,]+(?:\.[0-9]+)?[MB])', view_html, re.IGNORECASE)
                    
                    # Filter for valid view counts
                    valid_views = []
                    for num_str in view_numbers:
                        try:
                            parsed_value = self._parse_number(num_str)
                            if parsed_value and 1_000_000 <= parsed_value <= 100_000_000_000:  # Reasonable view range
                                valid_views.append((parsed_value, num_str))
                        except:
                            continue
                    
                    if valid_views:
                        # Take the highest reasonable view count
                        best_view = max(valid_views, key=lambda x: x[0])
                        metrics['total_views'] = best_view[0]
                        self.logger.debug(f"Extracted view count: {best_view[1]} -> {best_view[0]:,}")
                        
            except Exception as e:
                self.logger.debug(f"Views extraction failed: {e}")
            
            # Try to extract longform/shorts breakdown if available
            try:
                # Look for longform and shorts specific patterns in the current HTML
                current_html = renderer.driver.page_source
                
                longform_matches = re.findall(r'longform[^0-9]*([0-9,]+(?:\.[0-9]+)?[KMB])', current_html, re.IGNORECASE)
                if longform_matches:
                    longform_value = self._parse_number(longform_matches[0])
                    if longform_value:
                        metrics['longform_views'] = longform_value
                        self.logger.debug(f"Extracted longform views: {longform_matches[0]} -> {longform_value:,}")
                
                shorts_matches = re.findall(r'shorts?[^0-9]*([0-9,]+(?:\.[0-9]+)?[KMB])', current_html, re.IGNORECASE)
                if shorts_matches:
                    shorts_value = self._parse_number(shorts_matches[0])
                    if shorts_value:
                        metrics['shorts_views'] = shorts_value
                        self.logger.debug(f"Extracted shorts views: {shorts_matches[0]} -> {shorts_value:,}")
                        
            except Exception as e:
                self.logger.debug(f"Longform/shorts extraction failed: {e}")
            
            # Return metrics if we got at least one main metric
            if 'total_views' in metrics or 'subscriber_count' in metrics:
                return metrics
            
            return None
            
        except Exception as e:
            self.logger.error(f"Interactive metrics extraction failed: {e}")
            return None

    def _extract_current_metrics(self, html_content: str, url: str) -> Optional[Dict[str, Any]]:
        """
        Extract current metrics from ViewStats HTML
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            metrics = {
                'collection_date': datetime.now().isoformat(),
                'source_url': url,
                'data_source': 'viewstats_current'
            }
            
            # Extract total views
            total_views = self._extract_metric_value(html_content, self.extraction_patterns['total_views_patterns'])
            if total_views is not None:
                metrics['total_views'] = total_views
            
            # Extract subscriber count
            subscribers = self._extract_metric_value(html_content, self.extraction_patterns['subscriber_patterns'])
            if subscribers is not None:
                metrics['subscriber_count'] = subscribers
            
            # Extract longform views (if available)
            longform_views = self._extract_metric_value(html_content, self.extraction_patterns['longform_patterns'])
            if longform_views is not None:
                metrics['longform_views'] = longform_views
            
            # Extract shorts views (if available)
            shorts_views = self._extract_metric_value(html_content, self.extraction_patterns['shorts_patterns'])
            if shorts_views is not None:
                metrics['shorts_views'] = shorts_views
            
            # Try JSON-LD structured data extraction
            json_data = self.scraping_utils.extract_json_from_script(soup)
            if json_data:
                self._merge_json_metrics(metrics, json_data)
            
            # Return metrics if we found at least views or subscribers
            if 'total_views' in metrics or 'subscriber_count' in metrics:
                return metrics
            
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to extract metrics from ViewStats: {e}")
            return None
    
    def _extract_metric_value(self, html_content: str, patterns: List[str]) -> Optional[int]:
        """Extract a metric value using regex patterns"""
        try:
            for pattern in patterns:
                matches = re.findall(pattern, html_content, re.IGNORECASE)
                for match in matches:
                    value = self._parse_number(match)
                    if value is not None and value > 0:
                        return value
            return None
        except Exception:
            return None
    
    def _parse_number(self, text: str) -> Optional[int]:
        """Parse a number from text, handling K/M/B suffixes and commas"""
        try:
            if not text:
                return None
            
            # Remove non-digit characters except K, M, B suffixes and decimal points
            text = str(text).strip().upper()
            
            # Handle K, M, B suffixes
            multiplier = 1
            if text.endswith('K'):
                multiplier = 1000
                text = text[:-1]
            elif text.endswith('M'):
                multiplier = 1000000
                text = text[:-1]
            elif text.endswith('B'):
                multiplier = 1000000000
                text = text[:-1]
            
            # Remove commas and convert
            cleaned = re.sub(r'[^\d.]', '', text)
            if not cleaned:
                return None
            
            # Convert to number and apply multiplier
            if '.' in cleaned:
                number = float(cleaned) * multiplier
            else:
                number = int(cleaned) * multiplier
            
            return int(number)
            
        except (ValueError, TypeError):
            return None
    
    def _merge_json_metrics(self, metrics: Dict[str, Any], json_data: Dict[str, Any]) -> None:
        """Merge metrics from JSON-LD structured data"""
        try:
            # Common JSON-LD property mappings
            json_mappings = {
                'interactionStatistic': self._extract_interaction_stats,
                'viewCount': lambda x: self._parse_number(str(x)),
                'subscriberCount': lambda x: self._parse_number(str(x)),
            }
            
            for json_key, extractor in json_mappings.items():
                if json_key in json_data:
                    result = extractor(json_data[json_key])
                    if result:
                        if isinstance(result, dict):
                            metrics.update(result)
                        else:
                            # Map single values
                            if json_key == 'viewCount':
                                metrics['total_views'] = result
                            elif json_key == 'subscriberCount':
                                metrics['subscriber_count'] = result
                                
        except Exception as e:
            self.logger.debug(f"Failed to merge JSON metrics: {e}")
    
    def _extract_interaction_stats(self, stats_data: Any) -> Optional[Dict[str, int]]:
        """Extract interaction statistics from JSON-LD"""
        try:
            if not isinstance(stats_data, list):
                return None
            
            extracted = {}
            for stat in stats_data:
                if isinstance(stat, dict):
                    interaction_type = stat.get('@type', '').lower()
                    user_interaction_count = stat.get('userInteractionCount')
                    
                    if user_interaction_count:
                        count = self._parse_number(str(user_interaction_count))
                        if count:
                            if 'view' in interaction_type:
                                extracted['total_views'] = count
                            elif 'subscribe' in interaction_type:
                                extracted['subscriber_count'] = count
            
            return extracted if extracted else None
            
        except Exception:
            return None
    
    def _validate_viewstats_data(self, result: Dict[str, Any], channel: Channel) -> Dict[str, Any]:
        """
        Validate ViewStats data quality and consistency
        """
        if not result['current_data']:
            return {'valid': False, 'reason': 'No data collected'}
        
        try:
            current_data = result['current_data']
            
            # Check for reasonable view counts
            total_views = current_data.get('total_views', 0)
            if total_views > 0:
                # Views should be reasonable (not negative, not impossibly high)
                if total_views > 100_000_000_000:  # 100B views seems like upper limit
                    return {
                        'valid': False,
                        'reason': f'Unreasonably high view count: {total_views:,}',
                        'message': 'View count exceeds reasonable limits'
                    }
            
            # Check subscriber count if available
            subscriber_count = current_data.get('subscriber_count', 0)
            if subscriber_count > 0:
                if subscriber_count > 500_000_000:  # 500M subscribers as upper limit
                    return {
                        'valid': False,
                        'reason': f'Unreasonably high subscriber count: {subscriber_count:,}',
                        'message': 'Subscriber count exceeds reasonable limits'
                    }
            
            # Validate longform/shorts breakdown if available
            longform = current_data.get('longform_views', 0)
            shorts = current_data.get('shorts_views', 0)
            if longform > 0 and shorts > 0 and total_views > 0:
                total_breakdown = longform + shorts
                # Allow some variance, but breakdown should be reasonably close to total
                if abs(total_breakdown - total_views) > total_views * 0.5:  # 50% variance allowed
                    return {
                        'valid': False,
                        'reason': f'Longform/shorts breakdown doesn\'t match total: {total_breakdown:,} vs {total_views:,}',
                        'message': 'View breakdown validation failed'
                    }
            
            return {
                'valid': True,
                'reason': 'ViewStats data validation passed',
                'message': f"Successfully validated ViewStats data"
            }
            
        except Exception as e:
            return {
                'valid': False,
                'reason': f'Validation error: {e}',
                'message': 'Error during ViewStats data validation'
            }