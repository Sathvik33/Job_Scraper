import os
import time
import random
import re
import pandas as pd
from urllib.parse import quote, urljoin
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from collections import Counter
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium_stealth import stealth

BASE_DIR = r'C:\Sathvik-py\Talrn\job_scraper'
DATA_DIR = os.path.join(BASE_DIR, 'Data')
OUTPUT_FILE = os.path.join(DATA_DIR, 'jobs_raw_data.csv')

os.makedirs(DATA_DIR, exist_ok=True)

PAGES_TO_SCRAPE = 2 
SEARCH_QUERIES = {
    'core_engineering': [
        'Software Engineer Remote',
        'Backend Developer Contract',
        'Frontend Developer Remote',
        'Full Stack Developer Contract'
    ],
    'specialized': [
        'Python Developer Remote',
        'DevOps Engineer Contract',
        'Machine Learning Engineer Remote',
        'Cloud Engineer Contract'
    ],
    'modern_stack': [
        'React Developer Remote',
        'Node.js Developer Contract',
        'Data Engineer Remote',
        'Mobile Developer Contract'
    ]
}

# Multi-site configuration with ML-adaptive selectors
SITE_CONFIG = {
    'indeed': {
        'base_url': 'https://in.indeed.com',
        'url_template': 'https://in.indeed.com/jobs?q={query}&l=Remote&jt=contract,fulltime',
        'selectors': {
            'card': ['div.job_seen_beacon', 'div.jobsearch-SerpJobCard', 'div.slider_container'],
            'link': ['h2.jobTitle > a', 'a.jobtitle', 'h2 > a'],
            'next_button': ['a[data-testid="pagination-page-next"]', 'a[aria-label="Next"]'],
            'detail_title': ['h1.jobsearch-JobInfoHeader-title', 'h1.icl-u-xs-mb--xs'],
            'detail_company': ['div[data-testid="inlineHeader-companyName"]', 'div.icl-u-lg-mr--sm'],
            'detail_location': ['div[data-testid="inlineHeader-companyLocation"]', 'div.icl-u-xs-mt--xs'],
            'detail_description': ['div#jobDescriptionText', 'div.jobsearch-jobDescriptionText'],
            'detail_type': ['div#jobDetailsSection', 'span.jobsearch-JobMetadataHeader-item'],
            'detail_date': ['div[data-testid="jobsearch-JobMetadataFooter"]', 'span.date'],
        }
    },
    'naukri': {
        'base_url': 'https://www.naukri.com',
        'url_template': 'https://www.naukri.com/{query}-jobs',
        'selectors': {
            'card': ['div.srp-jobtuple-wrapper', 'article.jobTuple'],
            'link': ['a.title', 'a.jobTuple-title'],
            'next_button': ['a.fright.fs12.btn.brd-rd2.active', 'a[title="Next"]'],
            'detail_title': ['h1.jd-header-title', 'h1'],
            'detail_company': ['div.jd-header-comp-name > a', 'a.comp-name'],
            'detail_location': ['span.location.loc', 'span.loc'],
            'detail_description': ['div.styles_job-desc-container__txpYf', 'div.job-description'],
            'detail_type': ['div.job-details', 'span.type'],
            'detail_date': ['span.posted-date', 'div.job-post-date'],
        }
    },
    'linkedin': {
        'base_url': 'https://www.linkedin.com',
        'url_template': 'https://www.linkedin.com/jobs/search/?keywords={query}&location=Remote&f_WT=2,3',
        'selectors': {
            'card': ['div.job-search-card', 'li.jobs-search-results__list-item'],
            'link': ['a.base-card__full-link', 'a.job-card-list__title'],
            'next_button': ['button[aria-label="View next page"]'],
            'detail_title': ['h1.top-card-layout__title', 'h2.topcard__title'],
            'detail_company': ['a.topcard__org-name-link', 'span.topcard__flavor'],
            'detail_location': ['span.topcard__flavor--bullet', 'span.job-location'],
            'detail_description': ['div.show-more-less-html__markup', 'div.description'],
            'detail_type': ['span.description__job-criteria-text', 'li.job-criteria__item'],
            'detail_date': ['span.posted-time-ago__text', 'time'],
        }
    }
}


class JobTypeClassifier:
    
    def __init__(self):
        self.job_type_keywords = {
            'Remote': ['remote', 'work from home', 'wfh', 'anywhere', 'distributed', 'virtual', 'telecommute', 'location independent'],
            'Contract': ['contract', 'contractor', 'freelance', 'temporary', 'temp', 'c2c', 'corp to corp', 'consulting', 'project basis', 'fixed term'],
            'Full-time': ['full time', 'fulltime', 'full-time', 'permanent', 'fte', 'staff'],
            'Part-time': ['part time', 'part-time', 'parttime'],
            'Internship': ['intern', 'internship', 'trainee', 'graduate program', 'apprentice']
        }
    
    def extract_job_type(self, title, location, description):
        """Extract job type using ML keyword matching"""
        combined = f"{title} {location} {description}".lower()
        
        matches = []
        for job_type, keywords in self.job_type_keywords.items():
            for keyword in keywords:
                if keyword in combined:
                    matches.append(job_type)
                    break
        
        if not matches:
            return "Full-time"
        
        # Return combined types or most common
        counter = Counter(matches)
        if len(counter) > 1:
            return " / ".join([k for k, v in counter.most_common(2)])
        return counter.most_common(1)[0][0]
    
    def is_remote_or_contract(self, job_type, location, description):
        """ML filter for Remote OR Contract roles"""
        combined = f"{job_type} {location} {description}".lower()
        
        remote_keywords = ['remote', 'work from home', 'wfh', 'anywhere', 'distributed', 'virtual', 'telecommute']
        contract_keywords = ['contract', 'contractor', 'freelance', 'temporary', 'c2c', 'consulting', 'project basis']
        
        is_remote = any(kw in combined for kw in remote_keywords)
        is_contract = any(kw in combined for kw in contract_keywords)
        
        return is_remote or is_contract

class ExperienceExtractor:
    """ML-enhanced experience extraction FROM JOB DESCRIPTIONS"""
    
    @staticmethod
    def extract_experience(description_text):
        """
        Extract experience from job description (NOT just title)
        Returns: (min_years, max_years)
        """
        if not description_text:
            return 0, 0
        
        text_lower = description_text.lower()
        
        # Priority patterns for experience extraction
        patterns = [
            # Range patterns (e.g., "3-5 years", "3 to 5 years")
            (r'(\d+)\s*[-–to]+\s*(\d+)\s*(?:years?|yrs?)\s*(?:of)?\s*(?:experience|exp)', 'range'),
            
            # Plus patterns (e.g., "5+ years", "5 plus years")
            (r'(\d+)\s*\+\s*(?:years?|yrs?)\s*(?:of)?\s*(?:experience|exp)', 'plus'),
            
            # Minimum patterns (e.g., "minimum 3 years", "at least 4 years")
            (r'(?:minimum|min|at least|atleast)\s*(\d+)\s*(?:years?|yrs?)', 'minimum'),
            
            # Experience of X years pattern
            (r'(?:experience|exp)\s*(?:of)?\s*(\d+)\s*(?:years?|yrs?)', 'single'),
            
            # X years of experience pattern
            (r'(\d+)\s*(?:years?|yrs?)\s*(?:of)?\s*(?:experience|exp)', 'single'),
            
            # X-Y years experience (without "of")
            (r'(\d+)\s*[-–]\s*(\d+)\s*(?:years?|yrs?)', 'range_simple'),
        ]
        
        for pattern, pattern_type in patterns:
            matches = re.findall(pattern, text_lower)
            if matches:
                if pattern_type == 'range' or pattern_type == 'range_simple':
                    nums = [int(n) for n in matches[0]]
                    return min(nums), max(nums)
                elif pattern_type == 'plus':
                    num = int(matches[0])
                    return num, num + 5  # Assume +5 for "5+" 
                elif pattern_type == 'minimum':
                    num = int(matches[0])
                    return num, num + 3  # Assume +3 for "minimum"
                else:  # single
                    nums = [int(m) for m in matches if 0 < int(m) <= 20]
                    if nums:
                        min_exp = min(nums)
                        max_exp = max(nums) if len(nums) > 1 else min_exp
                        return min_exp, max_exp
        
        # Check for fresher/entry level roles
        fresher_keywords = ['fresher', 'entry level', '0-1 year', 'graduate', 'no experience', 'junior']
        if any(word in text_lower for word in fresher_keywords):
            return 0, 1
        
        return 0, 0

class DateExtractor:
    """ML-based posted date extraction"""
    
    @staticmethod
    def extract_posted_date(soup, date_selectors, page_text=""):
        """Extract posted date from page"""
        
        # Try selectors first
        date_text = safe_find(soup, date_selectors, "")
        
        # If no selector match, search page text
        if not date_text and page_text:
            date_text = page_text
        
        date_text = date_text.lower()
        
        # Parse relative dates
        today = datetime.now()
        
        # Today
        if any(word in date_text for word in ['today', 'just posted', 'just now']):
            return today.strftime('%Y-%m-%d')
        
        # Yesterday
        if 'yesterday' in date_text:
            return (today - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Days ago
        days_match = re.search(r'(\d+)\s*days?\s*ago', date_text)
        if days_match:
            days = int(days_match.group(1))
            return (today - timedelta(days=days)).strftime('%Y-%m-%d')
        
        # Weeks ago
        weeks_match = re.search(r'(\d+)\s*weeks?\s*ago', date_text)
        if weeks_match:
            weeks = int(weeks_match.group(1))
            return (today - timedelta(weeks=weeks)).strftime('%Y-%m-%d')
        
        # Months ago
        months_match = re.search(r'(\d+)\s*months?\s*ago', date_text)
        if months_match:
            months = int(months_match.group(1))
            return (today - timedelta(days=months*30)).strftime('%Y-%m-%d')
        
        # Absolute dates
        date_patterns = [
            r'\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}',
            r'\d{4}-\d{2}-\d{2}',
            r'\d{2}/\d{2}/\d{4}'
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, date_text, re.IGNORECASE)
            if match:
                try:
                    date_str = match.group(0)
                    for fmt in ['%d %b %Y', '%Y-%m-%d', '%d/%m/%Y']:
                        try:
                            parsed = datetime.strptime(date_str, fmt)
                            return parsed.strftime('%Y-%m-%d')
                        except:
                            continue
                except:
                    pass
        
        return "Not specified"

# --- Anti-Detection Setup ---

def setup_stealth_driver():
    """Setup Chrome with advanced ML-based anti-detection"""
    options = Options()
    
    # Core anti-detection
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    
    # Random user agent rotation
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]
    options.add_argument(f"user-agent={random.choice(user_agents)}")
    
    # Random window size
    window_sizes = [(1920, 1080), (1366, 768), (1536, 864)]
    w, h = random.choice(window_sizes)
    options.add_argument(f"--window-size={w},{h}")
    
    # Additional preferences
    prefs = {
        "profile.default_content_setting_values.notifications": 2,
        "credentials_enable_service": False,
        "profile.password_manager_enabled": False,
    }
    options.add_experimental_option("prefs", prefs)
    
    try:
        service = Service()
        driver = webdriver.Chrome(service=service, options=options)
    except Exception as e:
        print(f"Failed to initialize Chrome driver: {e}")
        return None
    
    # Apply selenium-stealth
    stealth(driver,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
    )
    
    # Override navigator properties
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
            window.chrome = {runtime: {}};
        """
    })
    
    return driver

def random_delay(min_sec=2, max_sec=6):
    """Human-like random delays"""
    time.sleep(random.uniform(min_sec, max_sec))

def human_scroll(driver):
    """Simulate human scrolling behavior"""
    try:
        scroll = random.randint(300, 800)
        driver.execute_script(f"window.scrollBy(0, {scroll});")
        time.sleep(random.uniform(0.5, 1.5))
        driver.execute_script(f"window.scrollBy(0, -{random.randint(100, 300)});")
        time.sleep(random.uniform(0.3, 0.8))
    except:
        pass

def safe_find(soup, selectors, default="Not specified"):
    """Try multiple selectors with fallback"""
    if isinstance(selectors, str):
        selectors = [selectors]
    
    for selector in selectors:
        try:
            element = soup.select_one(selector)
            if element:
                text = element.get_text(strip=True)
                if text:
                    return text
        except:
            continue
    
    return default

def safe_find_link(parent, selectors, base_url):
    """Find link with multiple selector attempts"""
    if isinstance(selectors, str):
        selectors = [selectors]
    
    for selector in selectors:
        try:
            element = parent.select_one(selector)
            if element:
                link = element.get('href')
                if link:
                    return urljoin(base_url, link)
        except:
            continue
    
    return None

# --- Main Scraper ---

def run_scraper():
    start_time = time.time()
    
    print("=" * 80)
    print("ML-ENHANCED JOB SCRAPER")
    print("Focus: Remote & Contract Software Development Roles")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # Initialize ML components
    print("\n[1/5] Initializing ML components...")
    job_classifier = JobTypeClassifier()
    exp_extractor = ExperienceExtractor()
    date_extractor = DateExtractor()
    print("    ✓ Job classifier, experience extractor, and date parser ready")
    
    # Setup driver
    print("\n[2/5] Setting up stealth browser...")
    driver = setup_stealth_driver()
    if not driver:
        print("    ✗ Failed to initialize driver")
        return
    
    wait = WebDriverWait(driver, 20)
    print("    ✓ Stealth browser initialized with anti-detection")
    
    # Flatten all queries (NOW ONLY 12 QUERIES!)
    all_queries = []
    for category_queries in SEARCH_QUERIES.values():
        all_queries.extend(category_queries)
    
    print(f"\n[3/5] Collecting job listings...")
    print(f"    Sites: {len(SITE_CONFIG)} job boards")
    print(f"    Queries: {len(all_queries)} optimized search terms")
    print(f"    Estimated time: 8-12 minutes")
    
    unique_jobs = {}  # {url: metadata}
    total_sites_tried = 0
    
    for query_idx, query in enumerate(all_queries, 1):
        print(f"\n  Query [{query_idx}/{len(all_queries)}]: '{query}'")
        
        for site, config in SITE_CONFIG.items():
            total_sites_tried += 1
            print(f"    → {site.title()}...", end=" ", flush=True)
            
            try:
                # Format search URL
                if site == 'indeed':
                    search_url = config['url_template'].format(query=quote(query))
                else:
                    search_url = config['url_template'].format(query=query.replace(' ', '-').lower())
                
                driver.get(search_url)
                random_delay(3, 6)
                
                pages_scraped = 0
                for page in range(PAGES_TO_SCRAPE):
                    try:
                        card_selector = config['selectors']['card'][0]
                        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, card_selector)))
                    except TimeoutException:
                        break
                    
                    human_scroll(driver)
                    soup = BeautifulSoup(driver.page_source, "html.parser")
                    
                    # Find job cards
                    cards = []
                    for selector in config['selectors']['card']:
                        cards = soup.select(selector)
                        if cards:
                            break
                    
                    links_found_this_page = 0
                    for card in cards:
                        link = safe_find_link(card, config['selectors']['link'], config.get('base_url', ''))
                        if link and link not in unique_jobs:
                            unique_jobs[link] = {'site': site, 'query': query}
                            links_found_this_page += 1
                    
                    pages_scraped += 1
                    
                    # Try next page
                    if page < PAGES_TO_SCRAPE - 1 and links_found_this_page > 0 and config['selectors']['next_button']:
                        try:
                            next_clicked = False
                            for next_sel in config['selectors']['next_button']:
                                try:
                                    next_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, next_sel)))
                                    driver.execute_script("arguments[0].scrollIntoView(true);", next_btn)
                                    random_delay(1, 2)
                                    driver.execute_script("arguments[0].click();", next_btn)
                                    random_delay(3, 5)
                                    next_clicked = True
                                    break
                                except:
                                    continue
                            
                            if not next_clicked:
                                break
                        except:
                            break
                    else:
                        break
                
                print(f"✓ {pages_scraped}p | Total: {len(unique_jobs)}")
                
            except Exception as e:
                print(f"✗ {str(e)[:30]}")
            
            # Delay between sites
            random_delay(4, 7)
    
    print(f"\n    ✓ Collected {len(unique_jobs)} unique job links")
    
    # Extract details WITH EXPERIENCE FROM DESCRIPTIONS
    print(f"\n[4/5] Extracting job details (including experience from descriptions)...")
    all_jobs = []
    
    for idx, (link, meta) in enumerate(list(unique_jobs.items()), 1):
        print(f"  [{idx}/{len(unique_jobs)}] ", end="", flush=True)
        
        try:
            driver.get(link)
            random_delay(2, 4)
            human_scroll(driver)
            
            soup = BeautifulSoup(driver.page_source, "html.parser")
            site = meta['site']
            config = SITE_CONFIG[site]
            
            # Extract fields
            title = safe_find(soup, config['selectors']['detail_title'])
            company = safe_find(soup, config['selectors']['detail_company'])
            location = safe_find(soup, config['selectors']['detail_location'])
            
            # IMPORTANT: Get full job description for experience extraction
            description = safe_find(soup, config['selectors']['detail_description'], "")
            
            # ML: Job type classification
            job_type_text = safe_find(soup, config['selectors']['detail_type'], "")
            combined = f"{title} {job_type_text} {location} {description[:500]}"  # Use description snippet
            job_type = job_classifier.extract_job_type(title, location, combined)
            
            # ML: Extract experience FROM DESCRIPTION (not just title)
            min_exp, max_exp = exp_extractor.extract_experience(description)
            experience_years = f"{min_exp}-{max_exp}" if max_exp > min_exp else str(max_exp)
            
            # ML: Posted date extraction
            posted_date = date_extractor.extract_posted_date(
                soup, 
                config['selectors'].get('detail_date', []),
                soup.get_text()
            )
            if job_classifier.is_remote_or_contract(job_type, location, description):
                all_jobs.append({
                    'Job Title': title,
                    'Company': company,
                    'Location': location,
                    'Job Type': job_type,
                    'Experience': experience_years,
                    'Posted Date': posted_date,
                    'Job Link': link,
                })
                print(f"✓ Exp:{experience_years}yrs")
            else:
                print(f"✗ Filtered")
            
        except Exception as e:
            print(f"✗ Error")
        
        # Adaptive delays
        if idx % 5 == 0:
            random_delay(8, 12)  # Longer break every 5 jobs
        else:
            random_delay(3, 5)
    
    driver.quit()
    print("\n    ✓ Browser closed")
    
    # Save results
    print(f"\n[5/5] Saving results...")
    
    if not all_jobs:
        print("    ⚠ No Remote/Contract jobs found")
        return
    
    df = pd.DataFrame(all_jobs)
    
    # Remove duplicates
    initial_count = len(df)
    df.drop_duplicates(subset=['Job Title', 'Company'], keep='first', inplace=True)
    
    # Save to CSV
    df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    
    # Calculate execution time
    execution_time = (time.time() - start_time) / 60
    
    # Print summary
    print("\n" + "=" * 80)
    print("SCRAPING COMPLETE - SUMMARY")
    print("=" * 80)
    print(f"⏱️  Execution Time: {execution_time:.2f} minutes")
    print(f"Total jobs collected: {initial_count}")
    print(f"After deduplication: {len(df)}")
    print(f"\n✓ Job Type Distribution:")
    for job_type, count in df['Job Type'].value_counts().items():
        print(f"    {job_type}: {count}")
    print(f"\n✓ Experience Distribution:")
    if 'Experience' in df.columns:
        exp_with_data = len(df[df['Experience'] != '0-0'])
        print(f"    With experience info: {exp_with_data}")
        print(f"    Without: {len(df) - exp_with_data}")
    print(f"\n✓ Top Companies:")
    for company, count in df['Company'].value_counts().head(5).items():
        print(f"    {company}: {count} jobs")
    print(f"\n✓ Data saved: {OUTPUT_FILE}")
    print(f"✓ Columns: {list(df.columns)}")
    print("=" * 80)

if __name__ == "__main__":
    run_scraper()