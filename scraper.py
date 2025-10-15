import os
import time
import random
import pandas as pd
import numpy as np
from urllib.parse import urljoin, quote_plus
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_stealth import stealth
import concurrent.futures
from threading import Lock
import joblib
import re
import gc
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Configuration ---
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'Data')
MODELS_DIR = os.path.join(BASE_DIR, 'ML_Models')
OUTPUT_FILE = os.path.join(DATA_DIR, 'jobs_raw_ml_extracted.csv')

os.makedirs(DATA_DIR, exist_ok=True)

# --- Site & Search Configuration ---
SITE_CONFIG = {
    'indeed': {
        'base_url': 'https://in.indeed.com',
        'search_url': 'https://in.indeed.com/jobs?q={query}&l=India&start={page_num}',
        'link_selector': 'h2.jobTitle a',
        'wait_selector': '#jobDescriptionText'
    },
    'naukri': {
        'base_url': 'https://www.naukri.com',
        'search_url': 'https://www.naukri.com/{query}-jobs-{page_num}',
        'link_selector': 'a.title',
        'wait_selector': 'div.job-desc'
    },
    'linkedin': {
        'base_url': 'https://in.linkedin.com',
        'search_url': 'https://www.linkedin.com/jobs/search/?keywords={query}&location=India&start={page_num}',
        'link_selector': 'a.base-card__full-link',
        'wait_selector': 'div.jobs-details__main-content'
    }
}

SEARCH_QUERIES = [
    'Software Engineer', 'Backend Developer', 'Frontend Developer', 'Full Stack Developer',
    'Java Developer', 'Python Developer', 'React Developer', 'SDE', 'Web Developer'
]

PAGES_TO_SCRAPE = 2
MAX_WORKERS = 2
MAX_JOBS_TARGET = 30  # Further reduced for stability

class AdvancedMLExtractor:
    def __init__(self, models_dir):
        self.pipelines = {}
        self.fields = ['Job Title', 'Company', 'Location', 'Job Type', 'Experience', 'Posted Date']
        self.junk_keywords = [
            'sign in', 'get the app', 'work from home jobs', 'it jobs', 'companies', 
            'skip to main content', 'apply', 'save', 'share', 'report', 'jobs', 
            'similar jobs', 'message', 'company culture', 'highly rated', 'open the app',
            'show', 'hide', 'view', 'more', 'popular categories', 'cookie', 'privacy',
            'terms', 'conditions', 'login', 'sign up', 'subscribe', 'follow us',
            'first name', 'last name', 'email', 'password'  # Added common form fields
        ]
        
        print("🤖 Initializing Advanced ML Extraction Engine...")
        for field in self.fields:
            model_path = os.path.join(models_dir, f"{field.replace(' ', '_').lower()}_pipeline.pkl")
            try:
                self.pipelines[field] = joblib.load(model_path)
                print(f"  -> Loaded model for '{field}'")
            except FileNotFoundError:
                print(f"  [!] Model for '{field}' not found at {model_path}")
        
        if not self.pipelines:
            raise RuntimeError("No ML models found. Please run train_model.py first.")

    def _create_advanced_features(self, text):
        """Create the same features used during training"""
        if not isinstance(text, str) or not text.strip():
            return {
                'text_length': 0,
                'word_count': 0,
                'has_digits': 0,
                'digit_count': 0,
                'has_special_chars': 0,
                'special_char_count': 0,
                'is_uppercase_ratio': 0,
                'has_common_separators': 0,
                'date_like_pattern': 0,
                'experience_like_pattern': 0,
                'url_like_pattern': 0
            }
        
        text = str(text).strip()
        text_length = len(text)
        word_count = len(text.split())
        
        has_digits = 1 if any(char.isdigit() for char in text) else 0
        digit_count = sum(char.isdigit() for char in text)
        
        special_chars = r'[!@#$%^&*()_+\-=\[\]{};\'":|,.<>?/~]'
        has_special_chars = 1 if re.search(special_chars, text) else 0
        special_char_count = len(re.findall(special_chars, text))
        
        uppercase_count = sum(1 for char in text if char.isupper())
        is_uppercase_ratio = uppercase_count / len(text) if text_length > 0 else 0
        
        has_common_separators = 1 if any(sep in text for sep in ['•', '|', '-', '·', '–', '—']) else 0
        
        date_patterns = [
            r'\d{1,2}[-/]\d{1,2}[-/]\d{2,4}',
            r'\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{4}',
            r'(?:today|yesterday|\d+\s+(?:days?|hours?)\s+ago)'
        ]
        date_like_pattern = 1 if any(re.search(pattern, text.lower()) for pattern in date_patterns) else 0
        
        experience_patterns = [
            r'\d+\+?\s*(?:years?|yrs?)',
            r'\d+\s*-\s*\d+\s*(?:years?|yrs?)',
            r'fresher|entry level|experienced'
        ]
        experience_like_pattern = 1 if any(re.search(pattern, text.lower()) for pattern in experience_patterns) else 0
        
        url_like_pattern = 1 if re.search(r'https?://|www\.|\.com|\.in', text.lower()) else 0
        
        return {
            'text_length': text_length,
            'word_count': word_count,
            'has_digits': has_digits,
            'digit_count': digit_count,
            'has_special_chars': has_special_chars,
            'special_char_count': special_char_count,
            'is_uppercase_ratio': is_uppercase_ratio,
            'has_common_separators': has_common_separators,
            'date_like_pattern': date_like_pattern,
            'experience_like_pattern': experience_like_pattern,
            'url_like_pattern': url_like_pattern
        }

    def _get_quality_candidates(self, soup):
        """Extract potential text candidates from HTML"""
        candidates = []
        
        priority_selectors = ['h1', 'h2', 'h3', 'h4', 'div', 'span', 'p', 'li', 'strong', 'b']
        
        for selector in priority_selectors:
            for element in soup.select(selector):
                text = element.get_text(strip=True)
                if text and 2 < len(text) < 200:
                    # Enhanced junk filtering
                    text_lower = text.lower()
                    if any(junk in text_lower for junk in self.junk_keywords):
                        continue
                    
                    # Skip form fields and personal info
                    if any(field in text_lower for field in ['first name', 'last name', 'email', 'phone', 'mobile']):
                        continue
                    
                    if len(re.findall(r'[a-zA-Z]', text)) < 2:
                        continue
                        
                    candidates.append({
                        'text': text,
                        'tag': element.name
                    })
        
        unique_candidates = []
        seen_texts = set()
        for cand in candidates:
            if cand['text'] not in seen_texts:
                unique_candidates.append(cand)
                seen_texts.add(cand['text'])
                
        return unique_candidates

    def predict(self, html_content):
        """Extract job information using ML models"""
        soup = BeautifulSoup(html_content, 'html.parser')
        candidates = self._get_quality_candidates(soup)
        
        if not candidates:
            return {field: "Not Found" for field in self.fields}
        
        candidates_df = pd.DataFrame(candidates)
        features_list = [self._create_advanced_features(text) for text in candidates_df['text']]
        features_df = pd.DataFrame(features_list)
        features_df['tag'] = candidates_df['tag']
        
        extracted_data = {field: "Not Specified" for field in self.fields}
        confidence_scores = {field: 0.0 for field in self.fields}
        used_indices = set()
        
        for field, pipeline in self.pipelines.items():
            try:
                probabilities = pipeline.predict_proba(features_df)[:, 1]
                
                best_score = 0
                best_idx = -1
                
                for idx, score in enumerate(probabilities):
                    if idx not in used_indices and score > best_score and score > 0.6:
                        best_score = score
                        best_idx = idx
                
                if best_idx != -1:
                    extracted_data[field] = candidates_df.loc[best_idx, 'text']
                    confidence_scores[field] = best_score
                    used_indices.add(best_idx)
                    print(f"        {field}: {extracted_data[field][:50]}... (confidence: {best_score:.3f})")
                    
            except Exception as e:
                print(f"        [!] Error predicting {field}: {e}")
                continue
        
        extracted_data['ML_Confidence_Avg'] = np.mean(list(confidence_scores.values())) if confidence_scores else 0
        
        return extracted_data

def create_requests_session():
    """Create a robust requests session with retry strategy"""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Cache-Control': 'max-age=0',
    })
    
    return session

def extract_with_requests_enhanced(link, ml_extractor):
    """Enhanced request-based extraction with better error handling"""
    session = create_requests_session()
    
    try:
        print(f"      📡 Using enhanced requests for: {link[:60]}...")
        response = session.get(link, timeout=30)
        
        if response.status_code == 200:
            # Check if we got a valid HTML page
            if '<!DOCTYPE html' in response.text[:100] or '<html' in response.text[:100]:
                job_data = ml_extractor.predict(response.text)
                job_data['Job Link'] = link
                job_data['Extraction_Method'] = 'requests'
                return job_data
            else:
                print(f"      ⚠️  Received non-HTML response from {link}")
                return None
        elif response.status_code == 403:
            print(f"      🔒 Access forbidden (403) for {link}")
            return None
        elif response.status_code == 429:
            print(f"      🚫 Rate limited (429) for {link}")
            time.sleep(10)
            return None
        else:
            print(f"      ❌ HTTP {response.status_code} for {link}")
            return None
            
    except requests.exceptions.Timeout:
        print(f"      ⏰ Request timeout for {link}")
        return None
    except requests.exceptions.ConnectionError:
        print(f"      🔌 Connection error for {link}")
        return None
    except Exception as e:
        print(f"      ❌ Request error for {link}: {str(e)[:100]}")
        return None

def setup_stealth_driver_light():
    """Minimal driver setup for LinkedIn to avoid detection"""
    options = Options()
    
    # Minimal options for stability
    options.add_argument("--headless")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    
    # Essential anti-detection
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    try:
        service = Service()
        driver = webdriver.Chrome(service=service, options=options)
        
        # Minimal stealth
        stealth(driver,
                languages=["en-US", "en"],
                vendor="Google Inc.", 
                platform="Win32",
                fix_hairline=False,
                )
        return driver
    except Exception as e:
        print(f"Driver initialization failed: {e}")
        return None

def scrape_site_links_enhanced(site, query, unique_links_lock, unique_links):
    """Enhanced link scraping with LinkedIn-specific handling"""
    config = SITE_CONFIG[site]
    
    # For LinkedIn, use requests instead of Selenium
    if site == 'linkedin':
        return scrape_linkedin_links(query, unique_links_lock, unique_links)
    
    # For other sites, use Selenium
    driver = setup_stealth_driver_light()
    if not driver: 
        return 0
    
    links_found_on_site = 0
    try:
        for i in range(PAGES_TO_SCRAPE):
            with unique_links_lock:
                if len(unique_links) >= MAX_JOBS_TARGET:
                    return links_found_on_site
            
            if site == 'indeed': 
                page_num = i * 10
            else: 
                page_num = i + 1
                
            formatted_query = quote_plus(query) if site != 'naukri' else query.replace(' ', '-')
            search_url = config['search_url'].format(query=formatted_query, page_num=page_num)
            
            print(f"  🔍 Scraping {site}: {search_url}")
            driver.get(search_url)
            time.sleep(random.uniform(3, 5))
            
            soup = BeautifulSoup(driver.page_source, "html.parser")
            link_elements = soup.select(config['link_selector'])
            
            if not link_elements:
                print(f"    No links found on {site} page {i+1}")
                break

            for link_el in link_elements:
                href = link_el.get('href')
                if href:
                    full_link = urljoin(config['base_url'], href)
                    with unique_links_lock:
                        if len(unique_links) < MAX_JOBS_TARGET and full_link not in unique_links:
                            unique_links.add(full_link)
                            links_found_on_site += 1
    except Exception as e:
        print(f"Error scraping links from {site} for '{query}': {e}")
    finally:
        driver.quit()
    return links_found_on_site

def scrape_linkedin_links(query, unique_links_lock, unique_links):
    """Scrape LinkedIn links using requests to avoid detection"""
    session = create_requests_session()
    links_found = 0
    
    try:
        for i in range(PAGES_TO_SCRAPE):
            with unique_links_lock:
                if len(unique_links) >= MAX_JOBS_TARGET:
                    return links_found
            
            page_num = i * 25
            formatted_query = quote_plus(query)
            search_url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={formatted_query}&location=India&start={page_num}"
            
            print(f"  🔍 Scraping LinkedIn API: {query} - page {i+1}")
            
            response = session.get(search_url, timeout=30)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")
                link_elements = soup.select('a.base-card__full-link')
                
                for link_el in link_elements:
                    href = link_el.get('href')
                    if href:
                        with unique_links_lock:
                            if len(unique_links) < MAX_JOBS_TARGET and href not in unique_links:
                                unique_links.add(href)
                                links_found += 1
            else:
                print(f"    LinkedIn API returned {response.status_code}")
                break
                
            time.sleep(2)  # Be respectful to LinkedIn
            
    except Exception as e:
        print(f"Error scraping LinkedIn links for '{query}': {e}")
    
    return links_found

def extract_job_details_safe(link, ml_extractor, site):
    """Safe extraction that primarily uses requests, with Selenium as last resort"""
    
    # Always try requests first (more stable)
    result = extract_with_requests_enhanced(link, ml_extractor)
    if result:
        result['Source Site'] = site
        result['Extraction_Method'] = 'requests'
        return result
    
    # Only use Selenium as last resort for non-LinkedIn sites
    if site != 'linkedin':
        print(f"    🚗 Fallback to Selenium for: {link[:60]}...")
        driver = setup_stealth_driver_light()
        if not driver: 
            return None
        
        try:
            driver.get(link)
            time.sleep(3)  # Simple wait instead of complex conditions
            
            page_html = driver.page_source
            job_data = ml_extractor.predict(page_html)
            job_data['Job Link'] = link
            job_data['Source Site'] = site
            job_data['Extraction_Method'] = 'selenium'
            
            print(f"      ✓ Selenium extraction completed")
            return job_data
            
        except Exception as e:
            print(f"      ✗ Selenium extraction failed: {str(e)[:100]}")
            return None
        finally:
            driver.quit()
    
    return None

def run_scraper():
    """Main scraping function with enhanced stability"""
    start_time = time.time()
    print("="*80)
    print(f" STABLE ML JOB SCRAPER (TARGET: {MAX_JOBS_TARGET} JOBS)")
    print("="*80)
    
    try:
        ml_extractor = AdvancedMLExtractor(models_dir=MODELS_DIR)
    except RuntimeError as e:
        print(f"[FATAL ERROR] {e}")
        return

    print("\n[1/3] Collecting job links (LinkedIn via API)...")
    unique_links = set()
    unique_links_lock = Lock()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:  # Single worker for LinkedIn
        all_tasks = [('linkedin', query) for query in SEARCH_QUERIES[:3]]  # Limit queries
        futures = {executor.submit(scrape_linkedin_links, query, unique_links_lock, unique_links) for site, query in all_tasks}
        
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            print(f"    Found {result} new links. Total: {len(unique_links)}")
            if len(unique_links) >= MAX_JOBS_TARGET:
                print("    Target reached. Stopping link collection.")
                executor.shutdown(wait=False, cancel_futures=True)
                break
    
    links_to_process = list(unique_links)[:MAX_JOBS_TARGET]
    print(f"\n    ✓ Collected {len(links_to_process)} unique job links")
    
    # All links are from LinkedIn
    link_site_map = {link: 'linkedin' for link in links_to_process}

    print(f"\n[2/3] Extracting job details using requests (no Selenium for LinkedIn)...")
    all_jobs = []
    
    # Process all links with requests only (no Selenium for LinkedIn)
    for i, link in enumerate(links_to_process):
        print(f"    Processing {i+1}/{len(links_to_process)}: {link[:80]}...")
        
        result = extract_with_requests_enhanced(link, ml_extractor)
        if result:
            result['Source Site'] = 'linkedin'
            all_jobs.append(result)
            print(f"      ✅ Success: {result.get('Job Title', 'N/A')[:40]}...")
        else:
            print(f"      ❌ Failed to extract")
        
        # Small delay between requests
        time.sleep(1)
    
    print(f"\n[3/3] Saving {len(all_jobs)} extracted jobs...")
    if all_jobs:
        df = pd.DataFrame(all_jobs)
        required_columns = ['Job Title', 'Company', 'Location', 'Job Type', 'Experience', 'Posted Date', 'Job Link', 'Source Site']
        for col in required_columns:
            if col not in df.columns:
                df[col] = "Not Found"
        
        df = df[required_columns]
        df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
        print(f"    ✓ Data saved to {OUTPUT_FILE}")
        
        # Print extraction statistics
        print("\n📊 Extraction Statistics:")
        for field in ['Job Title', 'Company', 'Location', 'Job Type', 'Experience', 'Posted Date']:
            found_count = df[df[field] != "Not Specified"][field].count()
            print(f"    {field}: {found_count}/{len(df)} ({found_count/len(df)*100:.1f}%)")
    
    execution_time = (time.time() - start_time) / 60
    print("\n" + "=" * 80)
    print("🎉 SCRAPING COMPLETE!")
    print(f"⏱️  Execution Time: {execution_time:.2f} minutes")
    print(f"📋 Jobs Extracted: {len(all_jobs)}")
    print("="*80)

if __name__ == "__main__":
    run_scraper()