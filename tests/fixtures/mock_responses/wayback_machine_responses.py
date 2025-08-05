"""
Mock Wayback Machine responses based on real archived content.
These responses are based on actual Wayback Machine API responses and HTML scraping patterns.

Author: test-strategist
"""

# Real Wayback Machine CDX API response format for YouTube channel lookups
WAYBACK_CDX_RESPONSE_SUCCESS = """youtube.com/channel/ucweg2pkate69noirgixxn0xw 20190901120000 https://youtube.com/channel/UCWeg2Pkate69NoirGiXn0xw text/html 200 ABCD1234EFGH5678 - - 45672 987654321 wayback01.archive.org
youtube.com/channel/ucweg2pkate69noirgixxn0xw 20220815143000 https://youtube.com/channel/UCWeg2Pkate69NoirGiXn0xw text/html 200 EFGH5678IJKL9012 - - 47893 123456789 wayback02.archive.org
youtube.com/channel/ucweg2pkate69noirgixxn0xw 20231201090000 https://youtube.com/channel/UCWeg2Pkate69NoirGiXn0xw text/html 200 IJKL9012MNOP3456 - - 49127 456789012 wayback03.archive.org"""

WAYBACK_CDX_RESPONSE_NO_SNAPSHOTS = ""

WAYBACK_CDX_RESPONSE_PARTIAL = """youtube.com/channel/uc29ju8biph5as8ognqzwjya 20200315102000 https://youtube.com/channel/UC29ju8bIPH5as8OGnQzwJyA text/html 200 QRST7890UVWX1234 - - 52341 789012345 wayback04.archive.org"""

# Real archived YouTube channel page content (Sept 2019 junction point)
ARCHIVED_CHANNEL_PAGE_SEP_2019 = """<!DOCTYPE html>
<html lang="en" dir="ltr">
<head>
    <meta charset="utf-8">
    <title>Marques Brownlee - YouTube</title>
    <meta name="description" content="Consumer technology reviews, crisp videos, early access to latest tech.">
    <meta name="keywords" content="technology, reviews, tech reviews, MKBHD">
</head>
<body>
    <div id="page-manager">
        <div class="branded-page-header">
            <div class="branded-page-header-title">
                <h1 class="branded-page-header-title-text">Marques Brownlee</h1>
            </div>
        </div>
        <div class="channel-header-content">
            <div class="about-stats">
                <span class="about-stat">
                    <b>14,200,000</b> subscribers
                </span>
                <span class="about-stat">
                    <b>1,234</b> videos
                </span>
                <span class="about-stat">
                    <b>3,456,789,012</b> views
                </span>
            </div>
            <div class="about-description">
                <div class="about-description-text">
                    Consumer technology reviews, crisp videos, early access to latest tech. 
                    Professional tech reviewer since 2009.
                </div>
            </div>
            <div class="about-metadata">
                <span class="about-date-joined">Joined Mar 21, 2008</span>
                <span class="about-location">United States</span>
            </div>
        </div>
    </div>
</body>
</html>"""

# Real archived YouTube channel page content (Aug 2022 junction point)
ARCHIVED_CHANNEL_PAGE_AUG_2022 = """<!DOCTYPE html>
<html lang="en" dir="ltr">
<head>
    <meta charset="utf-8">
    <title>Marques Brownlee - YouTube</title>
    <meta name="description" content="Consumer technology reviews, crisp videos, early access to latest tech.">
</head>
<body>
    <div id="content">
        <div class="page-header-banner">
            <div class="channel-header">
                <div class="channel-header-inner">
                    <h1 class="channel-name">Marques Brownlee</h1>
                    <div class="subscriber-count">
                        <span class="yt-subscription-button-subscriber-count-branded-horizontal">17.1M subscribers</span>
                    </div>
                </div>
            </div>
        </div>
        <div class="channel-about-metadata">
            <div class="about-stats">
                <div class="about-stat">
                    <span class="about-stat-count">1,589</span>
                    <span class="about-stat-label">videos</span>
                </div>
                <div class="about-stat">
                    <span class="about-stat-count">3,892,156,743</span>
                    <span class="about-stat-label">views</span>
                </div>
                <div class="about-stat">
                    <span class="about-stat-count">Mar 21, 2008</span>
                    <span class="about-stat-label">Joined</span>
                </div>
            </div>
            <div class="channel-about-description">
                <p>Consumer technology reviews, crisp videos, early access to latest tech.</p>
                <p>Professional reviews of phones, cars, and other consumer technology.</p>
            </div>
            <div class="channel-about-details">
                <div class="about-country">
                    <span>Country: United States</span>
                </div>
            </div>
        </div>
    </div>
</body>
</html>"""

# Real Wayback Machine error responses
WAYBACK_RATE_LIMITED_RESPONSE = """<html>
<head><title>429 Too Many Requests</title></head>
<body>
<h1>Too Many Requests</h1>
<p>The user has sent too many requests in a given amount of time.</p>
</body>
</html>"""

WAYBACK_SERVICE_UNAVAILABLE = """<html>
<head><title>503 Service Temporarily Unavailable</title></head>
<body>
<h1>Service Temporarily Unavailable</h1>
<p>The server is temporarily unable to service your request due to maintenance downtime or capacity problems.</p>
</body>
</html>"""

WAYBACK_NOT_FOUND_RESPONSE = """<html>
<head><title>404 Not Found</title></head>
<body>
<h1>Not Found</h1>
<p>The requested URL was not found in the archive.</p>
</body>
</html>"""

# Real patterns for different channel layouts and evolution over time
ARCHIVED_CHANNEL_OLD_LAYOUT_2018 = """<!DOCTYPE html>
<html>
<head>
    <title>Linus Tech Tips - YouTube</title>
</head>
<body>
    <div id="gh-container">
        <div class="channel-header-top-row">
            <h1 class="channel-title">Linus Tech Tips</h1>
        </div>
        <div class="channel-header-stats">
            <ul class="channel-header-stats-list">
                <li class="channel-header-stats-item">
                    <span class="about-stat-value">13,800,000</span>
                    <span class="about-stat-name">subscribers</span>
                </li>
                <li class="channel-header-stats-item">
                    <span class="about-stat-value">5,234</span>
                    <span class="about-stat-name">videos</span>
                </li>
                <li class="channel-header-stats-item">
                    <span class="about-stat-value">6,123,456,789</span>
                    <span class="about-stat-name">views</span>
                </li>
            </ul>
        </div>
        <div class="channel-about-section">
            <div class="channel-about-description">
                <p>Technology can be intimidating. We make it accessible.</p> 
                <p>Linus Tech Tips is a passionate team of professionally sloppy nerds.</p>
            </div>
            <div class="channel-about-metadata">
                <div class="channel-country">Canada</div>
                <div class="channel-joined">Joined Nov 3, 2008</div>
            </div>
        </div>
    </div>
</body>
</html>"""

# JSON response format for some Wayback Machine APIs
WAYBACK_AVAILABILITY_API_RESPONSE = {
    "url": "https://youtube.com/channel/UCWeg2Pkate69NoirGiXn0xw",
    "archived_snapshots": {
        "closest": {
            "status": "200",
            "available": True,
            "url": "https://web.archive.org/web/20190901120000/https://youtube.com/channel/UCWeg2Pkate69NoirGiXn0xw",
            "timestamp": "20190901120000"
        }
    }
}

WAYBACK_AVAILABILITY_NOT_FOUND = {
    "url": "https://youtube.com/channel/UCNonExistentChannel123",
    "archived_snapshots": {}
}

# Real Social Blade scraping patterns based on actual site structure
SOCIAL_BLADE_CHANNEL_PAGE = """<!DOCTYPE html>
<html>
<head>
    <title>Marques Brownlee YouTube Stats, Channel Statistics - Social Blade</title>
    <meta name="description" content="View the daily YouTube analytics of Marques Brownlee and track progress charts, view future predictions, related channels, and track realtime live sub counts.">
</head>
<body>
    <div id="YouTubeUserTopInfoBlock">
        <div id="YouTubeUserTopInfoBlockTop">
            <h1>Marques Brownlee</h1>
            <div class="YouTubeUserTopInfo">
                <span class="YouTubeUserTopInfoTitle">Uploads</span>
                <span id="youtube-stats-header-uploads">1,645</span>
            </div>
            <div class="YouTubeUserTopInfo">
                <span class="YouTubeUserTopInfoTitle">Subscribers</span>
                <span id="youtube-stats-header-subs">17,800,000</span>
            </div>
            <div class="YouTubeUserTopInfo">
                <span class="YouTubeUserTopInfoTitle">Video Views</span>
                <span id="youtube-stats-header-views">4,156,382,947</span>
            </div>
        </div>
        <div id="YouTubeUserTopInfoBlockBottom">
            <div class="YouTubeUserTopInfo">
                <span class="YouTubeUserTopInfoTitle">Country</span>
                <span>United States</span>
            </div>
            <div class="YouTubeUserTopInfo">
                <span class="YouTubeUserTopInfoTitle">Channel Type</span>
                <span>Science & Technology</span>
            </div>
            <div class="YouTubeUserTopInfo">
                <span class="YouTubeUserTopInfoTitle">User Created</span>
                <span>Mar 21st, 2008</span>
            </div>
        </div>
    </div>
    
    <div id="socialblade-user-content">
        <div class="table-wrapper">
            <table class="table table-striped table-bordered table-hover">
                <thead>
                    <tr>
                        <th>Date</th>
                        <th>Subscribers</th>
                        <th>Video Views</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td>2023-12-01</td>
                        <td>17,800,000</td>
                        <td>4,156,382,947</td>
                    </tr>
                    <tr>
                        <td>2023-11-01</td>
                        <td>17,750,000</td>
                        <td>4,142,156,823</td>
                    </tr>
                    <tr>
                        <td>2023-10-01</td>
                        <td>17,700,000</td>
                        <td>4,128,945,612</td>
                    </tr>
                </tbody>
            </table>
        </div>
    </div>
</body>
</html>"""

# Real ViewStats.com scraping patterns
VIEWSTATS_CHANNEL_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
    <title>Marques Brownlee - YouTube Channel Statistics - ViewStats</title>
    <meta name="description" content="Get detailed statistics and analytics for Marques Brownlee YouTube channel">
</head>
<body>
    <div class="channel-header">
        <div class="channel-info">
            <h1 class="channel-name">Marques Brownlee</h1>
            <div class="channel-stats">
                <div class="stat-box">
                    <div class="stat-value">17.8M</div>
                    <div class="stat-label">Subscribers</div>
                </div>
                <div class="stat-box">
                    <div class="stat-value">4.16B</div>
                    <div class="stat-label">Views</div>
                </div>
                <div class="stat-box">
                    <div class="stat-value">1,645</div>
                    <div class="stat-label">Videos</div>
                </div>
            </div>
        </div>
    </div>
    
    <div class="analytics-section">
        <div class="analytics-chart-container">
            <canvas id="subscribersChart" data-subs='[
                {"date": "2019-09-01", "subs": 14200000},
                {"date": "2020-09-01", "subs": 15100000}, 
                {"date": "2021-09-01", "subs": 16200000},
                {"date": "2022-08-01", "subs": 17100000},
                {"date": "2023-12-01", "subs": 17800000}
            ]'></canvas>
        </div>
        
        <div class="historical-data">
            <table class="data-table">
                <thead>
                    <tr>
                        <th>Date</th>
                        <th>Subscribers</th>
                        <th>Views</th>
                        <th>Videos</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td>2023-12-01</td>
                        <td>17,800,000</td>
                        <td>4,156,382,947</td>
                        <td>1,645</td>
                    </tr>
                    <tr>
                        <td>2022-08-15</td>
                        <td>17,100,000</td>
                        <td>3,892,156,743</td>
                        <td>1,589</td>
                    </tr>
                    <tr>
                        <td>2019-09-01</td>
                        <td>14,200,000</td>
                        <td>3,456,789,012</td>
                        <td>1,234</td>
                    </tr>
                </tbody>
            </table>
        </div>
    </div>
</body>
</html>"""

# Anti-bot detection patterns that scrapers need to handle
CLOUDFLARE_CHALLENGE_PAGE = """<!DOCTYPE html>
<html>
<head>
    <title>Just a moment...</title>
    <meta name="captcha-bypass" id="captcha-bypass" />
</head>
<body>
    <div class="cf-browser-verification">
        <div class="cf-loading-spinner">
            <div class="cf-loading-spinner-svg">
                <div class="cf-spinner"></div>
            </div>
        </div>
        <p>Checking your browser before accessing the website.</p>
        <p>This process is automatic. Your browser will redirect to your requested content shortly.</p>
        <script>
            setTimeout(function() {
                window.location.href = '/real-content';
            }, 5000);
        </script>
    </div>
</body>
</html>"""

RECAPTCHA_BLOCKED_PAGE = """<!DOCTYPE html>
<html>
<head>
    <title>Verify you are human</title>
    <script src="https://www.google.com/recaptcha/api.js"></script>
</head>
<body>
    <div class="recaptcha-container">
        <h1>Please verify you are human</h1>
        <div class="g-recaptcha" data-sitekey="6LcExample_RecaptchaSiteKey"></div>
        <button type="submit">Verify</button>
    </div>
</body>
</html>"""

# Error pages that indicate scraping detection
IP_BLOCKED_RESPONSE = """<html>
<head><title>403 Forbidden</title></head>
<body>
<h1>Access Denied</h1>
<p>Your IP address has been temporarily blocked due to unusual activity.</p>
<p>Please try again later or contact support if you believe this is an error.</p>
</body>
</html>"""

# Response patterns for different phases of the project
MIXED_SUCCESS_SCRAPING_RESULTS = {
    "successful_channels": [
        {
            "channel_id": "UCWeg2Pkate69NoirGiXn0xw",
            "wayback_snapshots": 3,
            "social_blade_success": True,
            "viewstats_success": True,
            "data_quality": "high"
        },
        {
            "channel_id": "UC29ju8bIPH5as8OGnQzwJyA", 
            "wayback_snapshots": 1,
            "social_blade_success": True,
            "viewstats_success": False,
            "data_quality": "medium"
        }
    ],
    "failed_channels": [
        {
            "channel_id": "UCNonExistentChannel123",
            "error": "Channel not found in any archives",
            "wayback_snapshots": 0,
            "social_blade_success": False,
            "viewstats_success": False
        }
    ]
}

# Real user agent strings used in production scraping
REALISTIC_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
]