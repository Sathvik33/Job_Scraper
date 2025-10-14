import os
import time
import random
import re
import pandas as pd
import numpy as np
from urllib.parse import quote, urljoin
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_stealth import stealth
import concurrent.futures
from threading import Lock

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'Data')
MODELS_DIR = os.path.join(BASE_DIR, 'ML_Models')
OUTPUT_FILE = os.path.join(DATA_DIR, 'jobs_raw_data.csv')

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(MODELS_DIR, exist_ok=True)

PAGES_TO_SCRAPE = 3
MAX_JOBS_TARGET = 2500
MIN_JOBS_TARGET = 1500
MAX_WORKERS = 4

SEARCH_QUERIES = {
    'core_engineering': [
        'Software Engineer', 'Backend Developer', 'Frontend Developer', 
        'Full Stack Developer', 'Software Developer', 'Web Developer'
    ],
    'specialized': [
        'Python Developer', 'Java Developer', 'JavaScript Developer',
        'DevOps Engineer', 'Machine Learning Engineer', 'Data Engineer',
        'Cloud Engineer', 'AI Engineer'
    ],
    'modern_stack': [
        'React Developer', 'Node.js Developer', 'AWS Engineer',
        'Azure Engineer', 'ML Engineer'
    ],
    'internships': [
        'Software Engineer Intern', 'Developer Intern', 'Data Science Intern',
        'Machine Learning Intern', 'Web Development Intern','AI/ML Internship'
    ]
}

SITE_CONFIG = {
    'indeed': {
        'base_url': 'https://in.indeed.com',
        'url_template': 'https://in.indeed.com/jobs?q={query}&l=India',
        'pagination_param': '&start={page}',
        'selectors': {
            'card': ['div.job_seen_beacon'],
            'link': ['h2.jobTitle a'],
            'detail_title': ['h1.jobsearch-JobInfoHeader-title'],
            'detail_company': ['div[data-testid="inlineHeader-companyName"]'],
            'detail_location': ['div[data-testid="inlineHeader-companyLocation"]'],
            'detail_description': ['div#jobDescriptionText'],
            'detail_date': ['div[data-testid="jobsearch-JobMetadataFooter"]'],
        }
    },
    'naukri': {
        'base_url': 'https://www.naukri.com',
        'url_template': 'https://www.naukri.com/{query}-jobs',
        'pagination_param': '-{page}',
        'selectors': {
            'card': ['article.jobTuple'],
            'link': ['a.title'],
            'detail_title': ['h1.jd-header-title'],
            'detail_company': ['a.comp-name'],
            'detail_location': ['span.loc'],
            'detail_description': ['div.job-desc'],
            'detail_date': ['span.posted'],
        }
    },
    'linkedin': {
        'base_url': 'https://www.linkedin.com',
        'url_template': 'https://www.linkedin.com/jobs/search/?keywords={query}&location=India',
        'pagination_param': '&start={page}',
        'selectors': {
            'card': ['div.job-search-card', 'li.jobs-search-results__list-item'],
            'link': ['a.base-card__full-link'],
            'detail_title': ['h1.top-card-layout__title'],
            'detail_company': ['a.topcard__org-name-link'],
            'detail_location': ['span.topcard__flavor--bullet'],
            'detail_description': ['div.show-more-less-html__markup'],
            'detail_date': ['span.posted-time-ago__text'],
        }
    },
    'monster': {
        'base_url': 'https://www.monsterindia.com',
        'url_template': 'https://www.monsterindia.com/search/{query}-jobs',
        'pagination_param': '?page={page}',
        'selectors': {
            'card': ['div.card-apply-content', 'div.job-tile'],
            'link': ['h3.medium a', 'a.job-title'],
            'detail_title': ['h1.job-title'],
            'detail_company': ['div.company h2'],
            'detail_location': ['div.location span'],
            'detail_description': ['div.job-desc'],
            'detail_date': ['div.posted-date'],
        }
    },
    'shine': {
        'base_url': 'https://www.shine.com',
        'url_template': 'https://www.shine.com/job-search/{query}-jobs',
        'pagination_param': '?page={page}',
        'selectors': {
            'card': ['div.search_listing', 'div.jobCard'],
            'link': ['h3.jobTitle a', 'a.job_link'],
            'detail_title': ['h1.job_title'],
            'detail_company': ['div.company_name'],
            'detail_location': ['div.job_location'],
            'detail_description': ['div.job_description'],
            'detail_date': ['div.posted_date'],
        }
    }
}

class CheckboxSolver:
    def __init__(self):
        print("🤖 Initializing Checkbox Solver with DOM-based detection...")
        self.fallback_methods = [
            self._click_by_aria_label,
            self._click_by_text_content,
            self._click_by_class_name,
            self._click_by_id,
            self._click_recaptcha,
            self._refresh_page
        ]
        print("✅ Checkbox solver initialized with DOM detection methods")
    
    def detect_and_click_checkbox(self, driver):
        """Detect and click checkboxes using DOM analysis"""
        return self._use_fallback_methods(driver)
    
    def _use_fallback_methods(self, driver):
        """Use DOM-based detection methods"""
        for method in self.fallback_methods:
            try:
                if method(driver):
                    print(f"✅ Checkbox handled via {method.__name__}")
                    return True
            except Exception as e:
                print(f"❌ {method.__name__} failed: {e}")
                continue
        return False
    
    def _click_by_aria_label(self, driver):
        """Click by aria-label attribute"""
        try:
            checkbox_selectors = [
                "[aria-label*='checkbox']",
                "[aria-label*='robot']",
                "[aria-label*='human']",
                "[aria-label*='verify']",
                "[aria-label*='captcha']",
                "[role='checkbox']",
                "[aria-checked]"
            ]
            
            for selector in checkbox_selectors:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    if element.is_displayed() and element.is_enabled():
                        driver.execute_script("arguments[0].click();", element)
                        time.sleep(1)
                        return True
            return False
        except:
            return False
    
    def _click_by_text_content(self, driver):
        """Click by text content"""
        try:
            text_patterns = [
                "I'm not a robot",
                "I am not a robot", 
                "Verify you are human",
                "robot",
                "Human verification",
                "verification",
                "captcha",
                "I'm a human"
            ]
            
            for pattern in text_patterns:
                # Try different XPath approaches
                xpaths = [
                    f"//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{pattern.lower()}')]",
                    f"//*[contains(translate(@value, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{pattern.lower()}')]",
                    f"//*[contains(translate(@placeholder, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{pattern.lower()}')]"
                ]
                
                for xpath in xpaths:
                    elements = driver.find_elements(By.XPATH, xpath)
                    for element in elements:
                        if element.is_displayed() and element.is_enabled():
                            driver.execute_script("arguments[0].click();", element)
                            time.sleep(1)
                            return True
            return False
        except:
            return False
    
    def _click_by_class_name(self, driver):
        """Click by common class names"""
        try:
            class_patterns = [
                "*[class*='checkbox']",
                "*[class*='recaptcha']",
                "*[class*='captcha']",
                "*[class*='verify']",
                "*[class*='robot']",
                "*[class*='human']",
                ".recaptcha-checkbox",
                ".g-recaptcha",
                ".captcha",
                ".verify-checkbox"
            ]
            
            for pattern in class_patterns:
                elements = driver.find_elements(By.CSS_SELECTOR, pattern)
                for element in elements:
                    if element.is_displayed() and element.is_enabled():
                        driver.execute_script("arguments[0].click();", element)
                        time.sleep(1)
                        return True
            return False
        except:
            return False
    
    def _click_by_id(self, driver):
        """Click by common IDs"""
        try:
            id_patterns = [
                "*[id*='checkbox']",
                "*[id*='recaptcha']",
                "*[id*='captcha']",
                "*[id*='verify']",
                "*[id*='robot']",
                "*[id*='human']"
            ]
            
            for pattern in id_patterns:
                elements = driver.find_elements(By.CSS_SELECTOR, pattern)
                for element in elements:
                    if element.is_displayed() and element.is_enabled():
                        driver.execute_script("arguments[0].click();", element)
                        time.sleep(1)
                        return True
            return False
        except:
            return False
    
    def _click_recaptcha(self, driver):
        """Specifically handle reCAPTCHA"""
        try:
            # Common reCAPTCHA iframes and elements
            recaptcha_selectors = [
                "iframe[src*='recaptcha']",
                "div.recaptcha-checkbox",
                ".g-recaptcha",
                "div[data-sitekey]"
            ]
            
            for selector in recaptcha_selectors:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    if element.is_displayed():
                        driver.execute_script("arguments[0].click();", element)
                        time.sleep(2)
                        return True
            return False
        except:
            return False
    
    def _refresh_page(self, driver):
        """Simple refresh as last resort"""
        try:
            print("🔄 Refreshing page as fallback...")
            driver.refresh()
            time.sleep(3)
            return True
        except:
            return False
    
    def handle_bot_protection(self, driver):
        """Handle bot protection with focus on checkbox solving"""
        try:
            page_source = driver.page_source.lower()
            
            # Check for checkboxes and "I'm not a robot"
            checkbox_indicators = [
                'checkbox', 'i\'m not a robot', 'not a robot', 'recaptcha',
                'verify you are human', 'robot', 'captcha', 'challenge'
            ]
            
            if any(indicator in page_source for indicator in checkbox_indicators):
                print("🛡️ Checkbox protection detected, attempting to solve...")
                if self.detect_and_click_checkbox(driver):
                    time.sleep(2)
                    return True
            
            # Also handle simple delays
            if 'just a moment' in page_source or 'checking your browser' in page_source:
                print("🛡️ Cloudflare detected, waiting...")
                time.sleep(5)
                return True
            
            return False
            
        except Exception as e:
            print(f"❌ Bot protection handling failed: {e}")
            return False

class JobTypeClassifier:
    def predict(self, text):
        text_lower = text.lower()
        
        if any(word in text_lower for word in ['remote', 'work from home', 'wfh']):
            return 'Remote'
        elif any(word in text_lower for word in ['contract', 'contractor', 'freelance']):
            return 'Contract'
        elif any(word in text_lower for word in ['intern', 'internship', 'trainee']):
            return 'Internship'
        elif any(word in text_lower for word in ['part time', 'part-time']):
            return 'Part-time'
        else:
            return 'Full-time'

class SmartExperienceExtractor:
    def extract(self, text, title=""):
        """Enhanced experience extraction with special handling for internships"""
        text_lower = text.lower()
        title_lower = title.lower()
        
        # Check if it's an internship role
        is_internship = any(word in text_lower or word in title_lower 
                           for word in ['intern', 'internship', 'trainee', 'fresher'])
        
        # If it's an internship, handle specially
        if is_internship:
            return self._extract_internship_experience(text_lower)
        
        # For regular jobs, use standard extraction
        return self._extract_regular_experience(text_lower)
    
    def _extract_internship_experience(self, text_lower):
        """Extract experience specifically for internship roles"""
        
        # Patterns to look for in internship descriptions
        internship_patterns = [
            (r'(\d+)\s*[-–to]+\s*(\d+)\s*(months?|years?)', 'range_months_years'),
            (r'(\d+)\s*\+\s*(months?|years?)', 'plus_months_years'),
            (r'(\d+)\s*(months?|years?)', 'single_months_years'),
            (r'(\d+)\s*week', 'weeks'),
            (r'(\d+)\s*month', 'months_single'),
            (r'(\d+)\s*year', 'years_single'),
        ]
        
        # First, try to find explicit experience requirements for internships
        for pattern, pattern_type in internship_patterns:
            match = re.search(pattern, text_lower)
            if match:
                if pattern_type == 'range_months_years':
                    num1, num2 = int(match.group(1)), int(match.group(2))
                    unit = match.group(3)
                    if 'month' in unit:
                        return 0, max(num1, num2) / 12  # Convert months to years
                    else:
                        return num1, num2
                elif pattern_type == 'plus_months_years':
                    num = int(match.group(1))
                    unit = match.group(2)
                    if 'month' in unit:
                        return 0, (num + 6) / 12  # Convert months to years
                    else:
                        return num, num + 1
                elif pattern_type == 'single_months_years':
                    num = int(match.group(1))
                    unit = match.group(2)
                    if 'month' in unit:
                        return 0, num / 12  # Convert months to years
                    else:
                        return num, num
                elif pattern_type == 'weeks':
                    num = int(match.group(1))
                    return 0, num / 52  # Convert weeks to years
                elif pattern_type == 'months_single':
                    num = int(match.group(1))
                    return 0, num / 12  # Convert months to years
                elif pattern_type == 'years_single':
                    num = int(match.group(1))
                    return 0, num
        
        # If no specific experience mentioned for internship, return 0
        return 0, 0
    
    def _extract_regular_experience(self, text_lower):
        """Extract experience for regular job roles"""
        
        patterns = [
            (r'(\d+)\s*[-–to]+\s*(\d+)\s*years?', 'range'),
            (r'(\d+)\s*\+\s*years?', 'plus'),
            (r'minimum\s*(\d+)\s*years?', 'minimum'),
            (r'(\d+)\s*years?', 'single'),
        ]
        
        for pattern, pattern_type in patterns:
            match = re.search(pattern, text_lower)
            if match:
                if pattern_type == 'range':
                    return int(match.group(1)), int(match.group(2))
                elif pattern_type == 'plus':
                    num = int(match.group(1))
                    return num, num + 3
                elif pattern_type == 'minimum':
                    num = int(match.group(1))
                    return num, num + 2
                elif pattern_type == 'single':
                    num = int(match.group(1))
                    return num, num
        
        # Default for regular jobs with no experience mentioned
        return 0, 0

def setup_stealth_driver_fast():
    """Fast driver setup"""
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    # Faster page loads
    options.page_load_strategy = 'eager'
    
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]
    options.add_argument(f"user-agent={random.choice(user_agents)}")
    
    try:
        service = Service()
        driver = webdriver.Chrome(service=service, options=options)
        
        stealth(driver,
            languages=["en-US", "en"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True,
        )
        
        return driver
    except Exception as e:
        print(f"Failed to initialize Chrome driver: {e}")
        return None

def fast_delay(min_sec=0.5, max_sec=1.5):
    """Reduced delays for faster scraping"""
    time.sleep(random.uniform(min_sec, max_sec))

def safe_find(soup, selectors, default="Not specified"):
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
    if isinstance(selectors, str):
        selectors = [selectors]
    
    for selector in selectors:
        try:
            element = parent.select_one(selector)
            if element:
                link = element.get('href')
                if link:
                    if link.startswith('/'):
                        return urljoin(base_url, link)
                    elif link.startswith('http'):
                        return link
                    else:
                        return urljoin(base_url, '/' + link.lstrip('/'))
        except:
            continue
    
    return None

def build_search_url(site, query, page_num=1):
    config = SITE_CONFIG[site]
    formatted_query = query.replace(' ', '-').lower()
    base_url = config['url_template'].format(query=formatted_query)
    
    if page_num == 1:
        return base_url
    
    if site == 'indeed':
        start_param = (page_num - 1) * 10
        return base_url + config['pagination_param'].format(page=start_param)
    elif site == 'naukri':
        return base_url + config['pagination_param'].format(page=page_num)
    elif site == 'linkedin':
        start_param = (page_num - 1) * 25
        return base_url + config['pagination_param'].format(page=start_param)
    else:
        return base_url + config['pagination_param'].format(page=page_num)

def scrape_site_links_parallel(site, query, unique_jobs_lock, unique_jobs):
    """Parallel link scraping"""
    config = SITE_CONFIG[site]
    driver = setup_stealth_driver_fast()
    checkbox_solver = CheckboxSolver()
    
    if not driver:
        return 0
    
    try:
        links_found = 0
        for page_num in range(1, PAGES_TO_SCRAPE + 1):
            if len(unique_jobs) >= MAX_JOBS_TARGET:
                break
                
            search_url = build_search_url(site, query, page_num)
            driver.get(search_url)
            fast_delay(1, 2)
            
            # Handle bot protection with checkbox solver
            checkbox_solver.handle_bot_protection(driver)
            
            soup = BeautifulSoup(driver.page_source, "html.parser")
            
            # Check for no results
            page_text = soup.get_text().lower()
            if any(msg in page_text for msg in ['no results', 'no jobs found']):
                break
            
            # Extract cards
            cards = []
            for selector in config['selectors']['card']:
                cards = soup.select(selector)
                if cards:
                    break
            
            if not cards:
                break
            
            # Extract links
            page_links = 0
            for card in cards[:20]:
                link = safe_find_link(card, config['selectors']['link'], config['base_url'])
                if link:
                    with unique_jobs_lock:
                        if link not in unique_jobs:
                            unique_jobs[link] = {'site': site, 'query': query}
                            links_found += 1
                            page_links += 1
            
            if page_links == 0 and page_num > 1:
                break
                
    except Exception as e:
        print(f"Error scraping {site}: {str(e)}")
    finally:
        driver.quit()
    
    return links_found

def extract_job_details_parallel(link_meta_tuple):
    """Parallel job detail extraction"""
    link, meta = link_meta_tuple
    driver = setup_stealth_driver_fast()
    ml_classifier = JobTypeClassifier()
    exp_extractor = SmartExperienceExtractor()
    checkbox_solver = CheckboxSolver()
    
    if not driver:
        return None
    
    try:
        driver.get(link)
        fast_delay(2, 3)
        
        # Handle bot protection with checkbox solver
        checkbox_solver.handle_bot_protection(driver)
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        site = meta['site']
        config = SITE_CONFIG[site]
        
        # Extract fields
        title = safe_find(soup, config['selectors']['detail_title'])
        company = safe_find(soup, config['selectors']['detail_company'])
        location = safe_find(soup, config['selectors']['detail_location'])
        description = safe_find(soup, config['selectors']['detail_description'], "")
        posted_date = safe_find(soup, config['selectors']['detail_date'])
        
        if title == "Not specified" and company == "Not specified":
            return None
        
        # Classification and SMART experience extraction
        job_type = ml_classifier.predict(f"{title} {description}")
        min_exp, max_exp = exp_extractor.extract(description, title)
        
        # Format experience string
        if min_exp == 0 and max_exp == 0:
            experience_years = "0"
        elif min_exp == max_exp:
            experience_years = f"{max_exp}"
        else:
            experience_years = f"{min_exp}-{max_exp}"
        
        job_data = {
            'Job Title': title,
            'Company': company,
            'Location': location,
            'Job Type': job_type,
            'Experience': experience_years,
            'Posted Date': posted_date,
            'Job Link': link,
        }
        
        print(f"      ✓ {title[:30]}... | Exp: {experience_years} | Type: {job_type}")
        return job_data
        
    except Exception as e:
        print(f"      ✗ Error extracting job: {str(e)[:50]}")
        return None
    finally:
        driver.quit()

def run_scraper_parallel():
    start_time = time.time()
    
    print("=" * 80)
    print(" PARALLEL JOB SCRAPER WITH CHECKBOX SOLVER")
    print(f"Target: {MIN_JOBS_TARGET}-{MAX_JOBS_TARGET} jobs")
    print(f"Workers: {MAX_WORKERS}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # Initialize Checkbox solver
    print("\n[1/4] Loading Checkbox detection...")
    checkbox_solver = CheckboxSolver()
    
    # STEP 2: Parallel link collection
    print(f"\n[2/4] Parallel link collection...")
    unique_jobs = {}
    unique_jobs_lock = Lock()
    
    all_queries = []
    for category_queries in SEARCH_QUERIES.values():
        all_queries.extend(category_queries)
    
    # Parallel link scraping
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        
        for query in all_queries:
            if len(unique_jobs) >= MAX_JOBS_TARGET:
                break
                
            for site in SITE_CONFIG:
                if len(unique_jobs) >= MAX_JOBS_TARGET:
                    break
                    
                future = executor.submit(
                    scrape_site_links_parallel, 
                    site, query, unique_jobs_lock, unique_jobs
                )
                futures.append(future)
        
        # Wait for completion
        for future in concurrent.futures.as_completed(futures):
            try:
                links_found = future.result()
                print(f"    Thread completed: {links_found} links | Total: {len(unique_jobs)}")
            except Exception as e:
                print(f"    Thread failed: {e}")
    
    print(f"\n    ✓ Final collection: {len(unique_jobs)} job links")
    
    # STEP 3: Parallel detail extraction
    print(f"\n[3/4] Parallel job detail extraction...")
    all_jobs = []
    job_links = list(unique_jobs.items())
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = executor.map(extract_job_details_parallel, job_links)
        
        for result in results:
            if result:
                all_jobs.append(result)
    
    # STEP 4: Save results
    print(f"\n[4/4] Saving {len(all_jobs)} jobs...")
    
    if all_jobs:
        df = pd.DataFrame(all_jobs)
        df.drop_duplicates(subset=['Job Link'], keep='first', inplace=True)
        df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
        
        execution_time = (time.time() - start_time) / 60
        
        print("\n" + "=" * 80)
        print(" PARALLEL SCRAPING COMPLETE!")
        print("=" * 80)
        print(f" Execution Time: {execution_time:.2f} minutes")
        print(f" Jobs collected: {len(all_jobs)}")
        print(f" After deduplication: {len(df)}")
        
        # Show internship statistics
        internship_count = len(df[df['Job Type'] == 'Internship'])
        print(f"🎓 Internships found: {internship_count}")
        
        if len(df) >= MIN_JOBS_TARGET:
            print(" SUCCESS: Target achieved!")
        else:
            print(f" Got {len(df)} jobs (target was {MIN_JOBS_TARGET}+)")
        
        print(f"\nJob Type Distribution:")
        for job_type, count in df['Job Type'].value_counts().items():
            print(f"    {job_type}: {count}")
        
        print(f"\nData saved: {OUTPUT_FILE}")
        print("=" * 80)

if __name__ == "__main__":
    run_scraper_parallel()