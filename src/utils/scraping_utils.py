"""
Scraping Utilities with Anti-Bot Measures for YouNiverse Dataset Enrichment

This module provides utilities for web scraping with anti-bot detection evasion,
user agent rotation, session management, and respectful delays.

Used by Wayback Machine and ViewStats collectors for historical data extraction.

Author: feature-developer
"""

import time
import random
import requests
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import logging
from urllib.parse import urljoin, urlparse
import json
from pathlib import Path
import re
from bs4 import BeautifulSoup
import threading

from ..core.config import config_manager


@dataclass
class ScrapingSession:
    """Manages a scraping session with anti-bot measures"""
    session: requests.Session
    user_agent: str
    created_at: datetime = field(default_factory=datetime.now)
    requests_made: int = 0
    last_request_time: Optional[datetime] = None
    domain: str = ""
    
    def is_expired(self, max_age_minutes: int = 30) -> bool:
        """Check if session is expired"""
        age = datetime.now() - self.created_at
        return age.total_seconds() > (max_age_minutes * 60)
    
    def should_rotate(self, max_requests: int = 50) -> bool:
        """Check if session should be rotated"""
        return self.requests_made >= max_requests or self.is_expired()


class UserAgentRotator:
    """Rotates user agents to avoid detection"""
    
    def __init__(self):
        self.user_agents = [
            # Chrome on Windows
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
            
            # Firefox on Windows
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/117.0',
            
            # Chrome on macOS
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
            
            # Safari on macOS
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15',
            
            # Chrome on Linux
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        ]
        self.last_used_index = -1
    
    def get_random_user_agent(self) -> str:
        """Get a random user agent"""
        return random.choice(self.user_agents)
    
    def get_next_user_agent(self) -> str:
        """Get next user agent in rotation"""
        self.last_used_index = (self.last_used_index + 1) % len(self.user_agents)
        return self.user_agents[self.last_used_index]


class RequestDelayManager:
    """Manages delays between requests with variance"""
    
    def __init__(self, base_delay: float = 1.0, variance: float = 0.3):
        self.base_delay = base_delay
        self.variance = variance
        self.last_request_time: Optional[float] = None
        self._lock = threading.Lock()
    
    def wait_if_needed(self) -> None:
        """Wait if needed to maintain delay between requests"""
        with self._lock:
            now = time.time()
            
            if self.last_request_time is not None:
                elapsed = now - self.last_request_time
                required_delay = self._calculate_delay()
                
                if elapsed < required_delay:
                    wait_time = required_delay - elapsed
                    time.sleep(wait_time)
            
            self.last_request_time = time.time()
    
    def _calculate_delay(self) -> float:
        """Calculate delay with variance"""
        if self.variance > 0:
            variance_amount = self.base_delay * self.variance
            min_delay = self.base_delay - variance_amount
            max_delay = self.base_delay + variance_amount
            return random.uniform(min_delay, max_delay)
        return self.base_delay


class SessionManager:
    """Manages multiple scraping sessions with rotation"""
    
    def __init__(self, max_sessions: int = 5, max_requests_per_session: int = 50):
        self.max_sessions = max_sessions
        self.max_requests_per_session = max_requests_per_session
        self.sessions: Dict[str, ScrapingSession] = {}
        self.user_agent_rotator = UserAgentRotator()
        self._lock = threading.Lock()
        self.logger = logging.getLogger(__name__)
    
    def _create_session(self, domain: str) -> ScrapingSession:
        """Create a new scraping session"""
        session = requests.Session()
        user_agent = self.user_agent_rotator.get_random_user_agent()
        
        # Set common headers
        session.headers.update({
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        })
        
        # Set timeouts and retries
        session.timeout = 30
        
        scraping_session = ScrapingSession(
            session=session,
            user_agent=user_agent,
            domain=domain
        )
        
        self.logger.debug(f"Created new session for {domain} with UA: {user_agent[:50]}...")
        return scraping_session
    
    def get_session(self, domain: str) -> ScrapingSession:
        """Get or create a session for the domain"""
        with self._lock:
            # Clean up expired sessions
            self._cleanup_expired_sessions()
            
            # Check if we have a valid session for this domain
            if domain in self.sessions:
                session = self.sessions[domain]
                if not session.should_rotate(self.max_requests_per_session):
                    return session
                else:
                    # Close old session and create new one
                    session.session.close()
                    self.logger.debug(f"Rotating session for {domain}")
            
            # Create new session
            new_session = self._create_session(domain)
            self.sessions[domain] = new_session
            
            return new_session
    
    def _cleanup_expired_sessions(self) -> None:
        """Clean up expired sessions"""
        to_remove = []
        
        for domain, session in self.sessions.items():
            if session.is_expired() or session.should_rotate(self.max_requests_per_session):
                session.session.close()
                to_remove.append(domain)
        
        for domain in to_remove:
            del self.sessions[domain]
            self.logger.debug(f"Cleaned up expired session for {domain}")
    
    def close_all_sessions(self) -> None:
        """Close all sessions"""
        with self._lock:
            for session in self.sessions.values():
                try:
                    session.session.close()
                except Exception as e:
                    self.logger.warning(f"Error closing session: {e}")
            
            self.sessions.clear()
            self.logger.info("All sessions closed")


class AntiDetectionScraper:
    """Main scraper class with anti-detection measures"""
    
    def __init__(self, 
                 base_delay: float = 1.0,
                 delay_variance: float = 0.3,
                 max_retries: int = 3,
                 respect_robots_txt: bool = True):
        
        self.session_manager = SessionManager()
        self.delay_manager = RequestDelayManager(base_delay, delay_variance)
        self.max_retries = max_retries
        self.respect_robots_txt = respect_robots_txt
        self.logger = logging.getLogger(__name__)
        
        # Request statistics
        self.requests_made = 0
        self.requests_successful = 0
        self.requests_failed = 0
        self.total_wait_time = 0.0
    
    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL"""
        return urlparse(url).netloc
    
    def _should_respect_robots(self, url: str) -> bool:
        """Check if we should respect robots.txt (simplified)"""
        if not self.respect_robots_txt:
            return True
        
        # For Wayback Machine and ViewStats, we assume permission
        # In production, you might want to implement proper robots.txt checking
        domain = self._extract_domain(url)
        
        # Archive.org explicitly allows researchers
        if 'archive.org' in domain:
            return True
        
        # ViewStats-like services typically allow scraping of public data
        if any(allowed in domain for allowed in ['viewstats', 'socialblade']):
            return True
        
        return True
    
    def get(self, url: str, **kwargs) -> Optional[requests.Response]:
        """
        Make GET request with anti-detection measures
        
        Args:
            url: URL to request
            **kwargs: Additional arguments for requests
            
        Returns:
            Response object or None if failed
        """
        if not self._should_respect_robots(url):
            self.logger.warning(f"Robots.txt disallows access to {url}")
            return None
        
        domain = self._extract_domain(url)
        session_obj = self.session_manager.get_session(domain)
        
        for attempt in range(self.max_retries + 1):
            try:
                # Apply delay
                start_time = time.time()
                self.delay_manager.wait_if_needed()
                self.total_wait_time += time.time() - start_time
                
                # Make request
                response = session_obj.session.get(url, **kwargs)
                
                # Update session stats
                session_obj.requests_made += 1
                session_obj.last_request_time = datetime.now()
                
                # Update global stats
                self.requests_made += 1
                
                # Check response
                if response.status_code == 200:
                    self.requests_successful += 1
                    return response
                elif response.status_code == 429:
                    # Rate limited - wait longer
                    wait_time = min(2 ** attempt, 60)  # Exponential backoff up to 60s
                    self.logger.warning(f"Rate limited on {url}, waiting {wait_time}s")
                    time.sleep(wait_time)
                    continue
                elif response.status_code in [403, 404]:
                    # Don't retry these
                    self.logger.warning(f"HTTP {response.status_code} for {url}")
                    self.requests_failed += 1
                    return response
                else:
                    self.logger.warning(f"HTTP {response.status_code} for {url}, attempt {attempt + 1}")
                    if attempt < self.max_retries:
                        time.sleep(2 ** attempt)  # Exponential backoff
                    continue
            
            except Exception as e:
                self.logger.warning(f"Request failed for {url}, attempt {attempt + 1}: {e}")
                if attempt < self.max_retries:
                    time.sleep(2 ** attempt)
                continue
        
        # All attempts failed
        self.requests_failed += 1
        return None
    
    def parse_html(self, html_content: str, parser: str = 'html.parser') -> Optional[BeautifulSoup]:
        """Parse HTML content with BeautifulSoup"""
        try:
            return BeautifulSoup(html_content, parser)
        except Exception as e:
            self.logger.error(f"Failed to parse HTML: {e}")
            return None
    
    def extract_json_from_script(self, soup: BeautifulSoup, 
                                pattern: str = None) -> Optional[Dict[str, Any]]:
        """Extract JSON data from script tags"""
        try:
            script_tags = soup.find_all('script')
            
            for script in script_tags:
                if not script.string:
                    continue
                
                script_content = script.string.strip()
                
                # Look for JSON patterns
                if pattern:
                    match = re.search(pattern, script_content)
                    if match:
                        json_str = match.group(1)
                        return json.loads(json_str)
                else:
                    # Try to find any JSON-like structure
                    json_patterns = [
                        r'window\.__INITIAL_DATA__\s*=\s*({.*?});',
                        r'var\s+data\s*=\s*({.*?});',
                        r'window\.data\s*=\s*({.*?});',
                        r'__DATA__\s*=\s*({.*?});'
                    ]
                    
                    for json_pattern in json_patterns:
                        match = re.search(json_pattern, script_content, re.DOTALL)
                        if match:
                            try:
                                return json.loads(match.group(1))
                            except json.JSONDecodeError:
                                continue
            
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to extract JSON from script: {e}")
            return None
    
    def close(self) -> None:
        """Close all sessions and clean up"""
        self.session_manager.close_all_sessions()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get scraping statistics"""
        return {
            'requests_made': self.requests_made,
            'requests_successful': self.requests_successful,
            'requests_failed': self.requests_failed,
            'success_rate': (self.requests_successful / self.requests_made * 100) if self.requests_made > 0 else 0,
            'total_wait_time': self.total_wait_time,
            'average_wait_time': (self.total_wait_time / self.requests_made) if self.requests_made > 0 else 0
        }


# Wayback Machine specific utilities
class WaybackMachineHelper:
    """Helper for Wayback Machine CDX API and snapshot access"""
    
    def __init__(self, scraper: AntiDetectionScraper):
        self.scraper = scraper
        self.cdx_api_base = "http://web.archive.org/cdx/search/cdx"
        self.wayback_base = "http://web.archive.org/web"
        self.logger = logging.getLogger(__name__)
    
    def get_snapshots(self, url: str, from_date: str = None, to_date: str = None,
                     limit: int = None) -> List[Dict[str, str]]:
        """
        Get available snapshots for a URL
        
        Args:
            url: URL to search for
            from_date: Start date (YYYYMMDD format)
            to_date: End date (YYYYMMDD format)
            limit: Maximum number of results
            
        Returns:
            List of snapshot information
        """
        params = {
            'url': url,
            'output': 'json',
            'fl': 'timestamp,original,statuscode,digest'
        }
        
        if from_date:
            params['from'] = from_date
        if to_date:
            params['to'] = to_date
        if limit:
            params['limit'] = limit
        
        try:
            response = self.scraper.get(self.cdx_api_base, params=params)
            
            if not response or response.status_code != 200:
                self.logger.error(f"CDX API request failed: {response.status_code if response else 'No response'}")
                return []
            
            data = response.json()
            
            if not data:
                return []
            
            # First row is headers, rest are data
            headers = data[0]
            snapshots = []
            
            for row in data[1:]:
                snapshot = dict(zip(headers, row))
                snapshots.append(snapshot)
            
            return snapshots
            
        except Exception as e:
            self.logger.error(f"Failed to get snapshots for {url}: {e}")
            return []
    
    def get_snapshot_url(self, timestamp: str, original_url: str) -> str:
        """Build snapshot URL"""
        return f"{self.wayback_base}/{timestamp}/{original_url}"
    
    def get_snapshot_content(self, timestamp: str, original_url: str) -> Optional[requests.Response]:
        """Get content from a specific snapshot"""
        snapshot_url = self.get_snapshot_url(timestamp, original_url)
        return self.scraper.get(snapshot_url)


# Chart parsing utilities
def extract_chart_data_dygraph(soup: BeautifulSoup) -> Optional[List[Dict[str, Any]]]:
    """Extract data from Dygraph charts (Social Blade format)"""
    try:
        # Look for Dygraph data in script tags
        script_data = None
        
        for script in soup.find_all('script'):
            if script.string and 'new Dygraph' in script.string:
                script_content = script.string
                
                # Extract data array from Dygraph constructor
                data_match = re.search(r'new Dygraph.*?\[\s*\[(.*?)\]\s*\]', script_content, re.DOTALL)
                if data_match:
                    data_str = data_match.group(1)
                    
                    # Parse the data rows
                    rows = re.findall(r'\[(.*?)\]', data_str)
                    chart_data = []
                    
                    for row in rows:
                        values = [v.strip().strip('"\'') for v in row.split(',')]
                        if len(values) >= 2:
                            chart_data.append({
                                'date': values[0],
                                'value': values[1]
                            })
                    
                    return chart_data
        
        return None
        
    except Exception as e:
        logging.getLogger(__name__).error(f"Failed to extract Dygraph data: {e}")
        return None


def extract_chart_data_highcharts(soup: BeautifulSoup) -> Optional[List[Dict[str, Any]]]:
    """Extract data from Highcharts charts"""
    try:
        # Look for Highcharts data in script tags
        for script in soup.find_all('script'):
            if script.string and 'Highcharts' in script.string:
                script_content = script.string
                
                # Extract series data
                series_match = re.search(r'series:\s*\[\s*\{.*?data:\s*\[(.*?)\]', script_content, re.DOTALL)
                if series_match:
                    data_str = series_match.group(1)
                    
                    # Parse data points
                    points = re.findall(r'\[(.*?)\]', data_str)
                    chart_data = []
                    
                    for point in points:
                        values = [v.strip() for v in point.split(',')]
                        if len(values) >= 2:
                            chart_data.append({
                                'timestamp': values[0],
                                'value': values[1]
                            })
                    
                    return chart_data
        
        return None
        
    except Exception as e:
        logging.getLogger(__name__).error(f"Failed to extract Highcharts data: {e}")
        return None


# Global scraper instances (lazy initialization)
_global_scraper: Optional[AntiDetectionScraper] = None
_global_wayback_helper: Optional[WaybackMachineHelper] = None


def get_scraper() -> AntiDetectionScraper:
    """Get global scraper instance"""
    global _global_scraper
    
    if _global_scraper is None:
        _global_scraper = AntiDetectionScraper(
            base_delay=1.0,
            delay_variance=0.3,
            max_retries=3
        )
    
    return _global_scraper


def get_wayback_helper() -> WaybackMachineHelper:
    """Get global Wayback Machine helper"""
    global _global_wayback_helper
    
    if _global_wayback_helper is None:
        scraper = get_scraper()
        _global_wayback_helper = WaybackMachineHelper(scraper)
    
    return _global_wayback_helper


def cleanup_scrapers():
    """Clean up global scraper instances"""
    global _global_scraper, _global_wayback_helper
    
    if _global_scraper:
        _global_scraper.close()
        _global_scraper = None
    
    _global_wayback_helper = None


class JavaScriptRenderer:
    """
    Browser automation for JavaScript-heavy sites like ViewStats
    
    Uses Selenium WebDriver to render JavaScript content and extract data
    """
    
    def __init__(self, headless: bool = True, timeout: int = 30):
        self.headless = headless
        self.timeout = timeout
        self.driver = None
        self.logger = logging.getLogger(__name__)
    
    def __enter__(self):
        """Context manager entry"""
        self._start_browser()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self._close_browser()
    
    def _start_browser(self):
        """Start the browser with appropriate options"""
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            
            chrome_options = Options()
            if self.headless:
                chrome_options.add_argument('--headless')
            
            # Add options for better compatibility and stealth
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36')
            
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_page_load_timeout(self.timeout)
            self.driver.implicitly_wait(10)
            
            self.logger.debug("Chrome WebDriver started successfully")
            
        except ImportError:
            raise ImportError("Selenium not installed. Run: pip install selenium")
        except Exception as e:
            self.logger.error(f"Failed to start browser: {e}")
            raise
    
    def _close_browser(self):
        """Close the browser"""
        if self.driver:
            try:
                self.driver.quit()
                self.logger.debug("Chrome WebDriver closed")
            except Exception as e:
                self.logger.warning(f"Error closing browser: {e}")
    
    def get_rendered_page(self, url: str, wait_for_element: str = None, wait_timeout: int = 15) -> str:
        """
        Get fully rendered page content after JavaScript execution
        
        Args:
            url: URL to load
            wait_for_element: CSS selector to wait for (optional)
            wait_timeout: How long to wait for element (seconds)
            
        Returns:
            Rendered HTML content
        """
        try:
            if not self.driver:
                self._start_browser()
            
            self.logger.debug(f"Loading page: {url}")
            self.driver.get(url)
            
            # Wait for specific element if provided
            if wait_for_element:
                from selenium.webdriver.support.ui import WebDriverWait
                from selenium.webdriver.support import expected_conditions as EC
                from selenium.webdriver.common.by import By
                
                wait = WebDriverWait(self.driver, wait_timeout)
                try:
                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, wait_for_element)))
                    self.logger.debug(f"Element '{wait_for_element}' found")
                except Exception:
                    self.logger.warning(f"Element '{wait_for_element}' not found within {wait_timeout}s")
            else:
                # Generic wait for page to be interactive
                import time
                time.sleep(3)  # Basic wait for JS to execute
            
            # Get the rendered HTML
            html_content = self.driver.page_source
            self.logger.debug(f"Retrieved {len(html_content)} characters of rendered HTML")
            
            return html_content
            
        except Exception as e:
            self.logger.error(f"Failed to get rendered page: {e}")
            return None
    
    def find_elements_with_text(self, text_pattern: str) -> list:
        """
        Find elements containing specific text patterns
        
        Args:
            text_pattern: Text or regex pattern to search for
            
        Returns:
            List of elements containing the pattern
        """
        try:
            if not self.driver:
                return []
            
            from selenium.webdriver.common.by import By
            import re
            
            # Find all elements and check their text
            all_elements = self.driver.find_elements(By.XPATH, "//*[text()]")
            matching_elements = []
            
            for element in all_elements:
                try:
                    element_text = element.text.strip()
                    if re.search(text_pattern, element_text, re.IGNORECASE):
                        matching_elements.append({
                            'element': element,
                            'text': element_text,
                            'tag': element.tag_name,
                            'classes': element.get_attribute('class')
                        })
                except Exception:
                    continue
            
            return matching_elements
            
        except Exception as e:
            self.logger.error(f"Failed to find elements with text: {e}")
            return []