import os
import time
import random
import pandas as pd
import numpy as np
from urllib.parse import urljoin, quote_plus, quote
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_stealth import stealth
from selenium.webdriver.common.action_chains import ActionChains
import concurrent.futures
from threading import Lock
import joblib
import re
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import torch
from transformers import DetrImageProcessor, DetrForObjectDetection
from PIL import Image, ImageDraw
import io
import base64

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'Data')
MODELS_DIR = os.path.join(BASE_DIR, 'ML_Models')
OUTPUT_FILE = os.path.join(DATA_DIR, 'jobs_raw_ml_extracted.csv')

os.makedirs(DATA_DIR, exist_ok=True)

class CAPTCHASolver:
    def __init__(self):
        print("🔄 Loading Facebook DETR model for CAPTCHA solving...")
        try:
            self.processor = DetrImageProcessor.from_pretrained("facebook/detr-resnet-50")
            self.model = DetrForObjectDetection.from_pretrained("facebook/detr-resnet-50")
            print("✅ Facebook DETR model loaded successfully")
        except Exception as e:
            print(f"❌ Failed to load DETR model: {e}")
            self.model = None
            self.processor = None

    def detect_checkboxes(self, screenshot):
        """Detect checkboxes in screenshot using Facebook DETR"""
        if not self.model:
            return []
        
        try:
            # Convert screenshot to PIL Image
            if isinstance(screenshot, bytes):
                image = Image.open(io.BytesIO(screenshot))
            else:
                image = screenshot
            
            # Process image
            inputs = self.processor(images=image, return_tensors="pt")
            outputs = self.model(**inputs)
            
            # Convert outputs to bounding boxes
            target_sizes = torch.tensor([image.size[::-1]])
            results = self.processor.post_process_object_detection(outputs, target_sizes=target_sizes, threshold=0.5)[0]
            
            checkboxes = []
            for score, label, box in zip(results["scores"], results["labels"], results["boxes"]):
                if score > 0.5:
                    box = [round(i, 2) for i in box.tolist()]
                    checkboxes.append({
                        'box': box,
                        'score': round(score.item(), 3),
                        'label': self.model.config.id2label[label.item()]
                    })
            
            return checkboxes
            
        except Exception as e:
            print(f"❌ Checkbox detection failed: {e}")
            return []

    def solve_captcha(self, driver):
        """Attempt to solve CAPTCHA using DETR model"""
        try:
            # Take screenshot
            screenshot = driver.get_screenshot_as_png()
            image = Image.open(io.BytesIO(screenshot))
            
            # Detect checkboxes
            checkboxes = self.detect_checkboxes(image)
            
            # Look for CAPTCHA elements
            captcha_selectors = [
                'iframe[src*="captcha"]',
                'div[class*="captcha"]',
                'div[class*="g-recaptcha"]',
                'img[src*="captcha"]',
                'input[type="checkbox"][role*="captcha"]'
            ]
            
            for selector in captcha_selectors:
                try:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        print("🎯 CAPTCHA element found, attempting to solve...")
                        
                        # Click on the CAPTCHA element
                        driver.execute_script("arguments[0].scrollIntoView();", element)
                        time.sleep(1)
                        
                        # Try to click using JavaScript
                        driver.execute_script("arguments[0].click();", element)
                        print("✅ CAPTCHA interaction attempted")
                        
                        time.sleep(3)
                        return True
                except:
                    continue
            
            # If no specific CAPTCHA elements found, try clicking detected checkboxes
            for checkbox in checkboxes:
                if 'checkbox' in checkbox['label'].lower() or checkbox['score'] > 0.7:
                    x, y = checkbox['box'][0], checkbox['box'][1]
                    try:
                        # Click at checkbox coordinates
                        actions = ActionChains(driver)
                        actions.move_by_offset(x, y).click().perform()
                        print(f"✅ Clicked detected checkbox at ({x}, {y})")
                        time.sleep(2)
                        return True
                    except:
                        continue
            
            return False
            
        except Exception as e:
            print(f"❌ CAPTCHA solving failed: {e}")
            return False

class AdvancedMLExtractor:
    def __init__(self, models_dir):
        self.pipelines = {}
        self.fields = ['Job Title', 'Company', 'Location', 'Job Type', 'Experience', 'Posted Date']
        
        # Enhanced junk keywords with common false positives
        self.junk_keywords = [
            'sign in', 'apply', 'save', 'share', 'report', 'first name', 'last name', 
            'email', 'similar searches', 'industries', 'job', 'posted', 'open jobs',
            'show more', 'show less', 'see more', 'see less', 'load more', 'view all',
            'companies', 'locations', 'salaries', 'profiles', 'about', 'follow',
            'connect', 'message', 'sponsored', 'promoted', 'featured', 'upload resume',
            'create alert', 'job alert', 'email me', 'get notified'
        ]
        
        print("Initializing ML Extraction Engine...")
        for field in self.fields:
            model_path = os.path.join(models_dir, f"{field.replace(' ', '_').lower()}_pipeline.pkl")
            try:
                self.pipelines[field] = joblib.load(model_path)
                print(f"  -> Loaded model for '{field}'")
            except FileNotFoundError:
                print(f"  [!] Model for '{field}' not found")
        
        if not self.pipelines:
            raise RuntimeError("No ML models found. Please run train_model.py first.")

    def _create_advanced_features(self, text, field=None):
        """Create features that match the trained models"""
        if not isinstance(text, str) or not text.strip():
            base_features = {
                'text_length': 0, 'word_count': 0, 'has_digits': 0, 
                'digit_count': 0, 'is_uppercase_ratio': 0, 'has_comma': 0
            }
            
            if field == 'Company':
                base_features.update({'has_company_keywords': 0, 'has_job_title_words': 0})
            elif field == 'Experience':
                base_features.update({'has_experience_keywords': 0, 'has_year_mentions': 0})
            elif field == 'Job Title':
                base_features.update({'has_title_keywords': 0})
            elif field == 'Location':
                base_features.update({'has_location_indicators': 0, 'has_indian_city': 0})
                
            return base_features
        
        text = str(text).strip()
        text_length = len(text)
        word_count = len(text.split())
        has_digits = 1 if any(char.isdigit() for char in text) else 0
        digit_count = sum(char.isdigit() for char in text)
        
        uppercase_count = sum(1 for char in text if char.isupper())
        is_uppercase_ratio = uppercase_count / len(text) if text_length > 0 else 0
        
        has_comma = 1 if ',' in text else 0
        
        features = {
            'text_length': text_length,
            'word_count': word_count,
            'has_digits': has_digits,
            'digit_count': digit_count,
            'is_uppercase_ratio': is_uppercase_ratio,
            'has_comma': has_comma
        }
        
        text_lower = text.lower()
        
        if field == 'Company':
            company_keywords = [
                'technologies', 'solutions', 'systems', 'software', 'services', 
                'consulting', 'group', 'corporation', 'incorporated', 'limited', 
                'ltd', 'inc', 'corp', 'company', 'co.', 'enterprises', 'ventures',
                'holdings', 'international', 'global', 'digital', 'innovations',
                'labs', 'studios', 'networks', 'capital', 'partners', 'pvt'
            ]
            job_title_words = [
                'engineer', 'developer', 'manager', 'analyst', 'specialist',
                'architect', 'consultant', 'coordinator', 'associate', 'lead',
                'head', 'officer', 'executive', 'director', 'president', 'intern'
            ]
            
            features['has_company_keywords'] = 1 if any(keyword in text_lower for keyword in company_keywords) else 0
            features['has_job_title_words'] = 1 if any(word in text_lower for word in job_title_words) else 0
            
        elif field == 'Experience':
            experience_keywords = [
                'years', 'yrs', 'year', 'experience', 'exp', 'fresher', 
                'entry', 'mid', 'senior', 'level', 'experienced'
            ]
            year_patterns = [
                r'\d+\+?\s*(?:years?|yrs?)', r'\d+\s*-\s*\d+\s*(?:years?|yrs?)',
                r'\d+\s*to\s*\d+\s*(?:years?|yrs?)', r'\d+\s*(?:years?|yrs?)',
                r'\d+\.?\d*\s*(?:years?|yrs?)'
            ]
            
            features['has_experience_keywords'] = 1 if any(keyword in text_lower for keyword in experience_keywords) else 0
            features['has_year_mentions'] = 1 if any(re.search(pattern, text_lower) for pattern in year_patterns) else 0
            
        elif field == 'Job Title':
            title_keywords = [
                'engineer', 'developer', 'analyst', 'manager', 'architect',
                'specialist', 'scientist', 'consultant', 'coordinator', 'associate',
                'lead', 'head', 'officer', 'executive', 'director', 'president', 'intern'
            ]
            features['has_title_keywords'] = 1 if any(keyword in text_lower for keyword in title_keywords) else 0
            
        elif field == 'Location':
            location_indicators = [',', 'remote', 'hybrid', 'onsite', 'office', 'location']
            indian_cities = [
                'pune', 'bangalore', 'bengaluru', 'hyderabad', 'chennai', 'mumbai',
                'delhi', 'gurgaon', 'gurugram', 'noida', 'kolkata', 'ahmedabad'
            ]
            features['has_location_indicators'] = 1 if any(indicator in text_lower for indicator in location_indicators) else 0
            features['has_indian_city'] = 1 if any(city in text_lower for city in indian_cities) else 0
            
        return features

    def _is_valid_company(self, text):
        """Enhanced validation for company names"""
        if not text or not isinstance(text, str):
            return False
            
        text_lower = text.lower().strip()
        
        # Check blacklist
        blacklist = ['similar searches', 'industries', 'linkedin', 'indeed', 'naukri']
        if any(blacklisted in text_lower for blacklisted in blacklist):
            return False
        
        # Must have at least 2 characters and contain letters
        if len(text) < 2 or not any(c.isalpha() for c in text):
            return False
        
        return True

    def predict(self, html_content):
        """Main prediction method"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Simple extraction for job listings
        job_data = {
            'Job Title': "Not Specified",
            'Company': "Not Specified", 
            'Location': "Not Specified",
            'Experience': "Not Specified",
            'Job Type': "Not Specified",
            'Posted Date': "Not Specified"
        }
        
        # Try to extract basic info
        try:
            # Job title
            title_selectors = ['h1', 'h2', 'h3', '.job-title', '.title', '[class*="title"]']
            for selector in title_selectors:
                elements = soup.select(selector)
                for elem in elements:
                    text = elem.get_text(strip=True)
                    if text and len(text) > 3 and len(text) < 100:
                        job_data['Job Title'] = text
                        break
                if job_data['Job Title'] != "Not Specified":
                    break
            
            # Company
            company_selectors = ['.company', '.employer', '[class*="company"]', '[class*="employer"]']
            for selector in company_selectors:
                elements = soup.select(selector)
                for elem in elements:
                    text = elem.get_text(strip=True)
                    if text and self._is_valid_company(text):
                        job_data['Company'] = text
                        break
                if job_data['Company'] != "Not Specified":
                    break
            
            # Location
            location_selectors = ['.location', '[class*="location"]', '[class*="address"]']
            for selector in location_selectors:
                elements = soup.select(selector)
                for elem in elements:
                    text = elem.get_text(strip=True)
                    if text and len(text) > 2:
                        job_data['Location'] = text
                        break
                if job_data['Location'] != "Not Specified":
                    break
                    
        except Exception as e:
            print(f"    ⚠️ Extraction error: {e}")
        
        return job_data

def create_selenium_driver(captcha_solver):
    """Create a Selenium driver with enhanced stealth and CAPTCHA solving"""
    chrome_options = Options()
    
    # Remove headless for debugging
    # chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument("--disable-web-security")
    chrome_options.add_argument("--allow-running-insecure-content")
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--disable-popup-blocking")
    
    # Enhanced stealth options
    chrome_options.add_argument("--disable-blink-features")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    
    # Set user agent
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ]
    chrome_options.add_argument(f'--user-agent={random.choice(user_agents)}')
    
    # Additional options to avoid detection
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-plugins")
    chrome_options.add_argument("--disable-images")
    chrome_options.add_argument("--disable-javascript")
    
    try:
        service = Service()
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # Apply stealth
        stealth(driver,
                languages=["en-US", "en"],
                vendor="Google Inc.",
                platform="Win32",
                webgl_vendor="Intel Inc.",
                renderer="Intel Iris OpenGL Engine",
                fix_hairline=True,
                )
        
        # Execute stealth scripts
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.execute_cdp_cmd('Network.setUserAgentOverride', {
            "userAgent": random.choice(user_agents)
        })
        
        return driver
        
    except Exception as e:
        print(f"❌ Failed to create driver: {e}")
        return None

def handle_captcha_and_blocks(driver, captcha_solver, url):
    """Handle CAPTCHAs and blocking mechanisms"""
    max_attempts = 3
    
    for attempt in range(max_attempts):
        try:
            # Check for CAPTCHA
            if captcha_solver.solve_captcha(driver):
                print("✅ CAPTCHA handling attempted")
                time.sleep(5)
            
            # Check for blocking pages
            page_source = driver.page_source.lower()
            block_indicators = ['captcha', 'access denied', 'blocked', 'security check', 'cloudflare']
            
            if any(indicator in page_source for indicator in block_indicators):
                print(f"🚫 Blocking detected on attempt {attempt + 1}")
                
                # Try to solve CAPTCHA
                if captcha_solver.solve_captcha(driver):
                    print("✅ CAPTCHA solved, continuing...")
                    time.sleep(3)
                    continue
                
                # Try refreshing
                driver.refresh()
                time.sleep(5)
                continue
            
            # If we get here, no blocking detected
            return True
            
        except Exception as e:
            print(f"⚠️ Block handling error: {e}")
            time.sleep(3)
    
    return False

def scrape_linkedin_jobs(query, location, max_jobs=20, captcha_solver=None):
    """Scrape job listings directly from LinkedIn with CAPTCHA handling"""
    print(f"🔍 Scraping LinkedIn for: {query} in {location}")
    
    driver = None
    jobs = []
    
    try:
        driver = create_selenium_driver(captcha_solver)
        if not driver:
            return []
        
        # Build search URL
        encoded_query = quote(query)
        encoded_location = quote(location)
        search_url = f"https://www.linkedin.com/jobs/search/?keywords={encoded_query}&location={encoded_location}"
        
        print(f"   📍 Navigating to: {search_url}")
        driver.get(search_url)
        
        # Handle potential blocks
        if not handle_captcha_and_blocks(driver, captcha_solver, search_url):
            print("   ❌ Unable to bypass blocking")
            return []
        
        # Wait for jobs to load with multiple selectors
        wait = WebDriverWait(driver, 20)
        selectors_to_try = [
            ".jobs-search__results-list",
            ".jobs-search-results",
            "[data-test-id='search-results-container']",
            ".scaffold-layout__list"
        ]
        
        for selector in selectors_to_try:
            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                print(f"   ✅ Found jobs container with selector: {selector}")
                break
            except:
                continue
        
        # Scroll to load more jobs
        print("   📜 Scrolling to load more jobs...")
        for i in range(3):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
        
        # Parse job listings
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Multiple selectors for LinkedIn job cards
        job_selectors = [
            'div[class*="job-card-container"]',
            'li[class*="job-card-container"]', 
            'div[class*="base-card"]',
            '.jobs-search-results__list-item'
        ]
        
        job_cards = []
        for selector in job_selectors:
            job_cards = soup.select(selector)
            if job_cards:
                print(f"   ✅ Found {len(job_cards)} job cards with selector: {selector}")
                break
        
        print(f"   📊 Processing {len(job_cards)} job cards from LinkedIn")
        
        for i, card in enumerate(job_cards[:max_jobs]):
            try:
                # Extract basic info with multiple selectors
                title = None
                company = None
                location_text = None
                
                # Title selectors
                title_selectors = [
                    'h3[class*="base-search-card__title"]',
                    '.base-search-card__title',
                    'h3',
                    '[class*="job-card-title"]'
                ]
                
                for selector in title_selectors:
                    title_elem = card.select_one(selector)
                    if title_elem:
                        title = title_elem.get_text(strip=True)
                        break
                
                # Company selectors
                company_selectors = [
                    'a[class*="hidden-nested-link"]',
                    '[class*="company-name"]',
                    '.base-search-card__subtitle',
                    'h4'
                ]
                
                for selector in company_selectors:
                    company_elem = card.select_one(selector)
                    if company_elem:
                        company = company_elem.get_text(strip=True)
                        break
                
                # Location selectors
                location_selectors = [
                    '[class*="job-search-card__location"]',
                    '.job-search-card__location',
                    '[class*="location"]',
                    'span[class*="location"]'
                ]
                
                for selector in location_selectors:
                    location_elem = card.select_one(selector)
                    if location_elem:
                        location_text = location_elem.get_text(strip=True)
                        break
                
                if title and company:
                    job_data = {
                        'Job Title': title,
                        'Company': company,
                        'Location': location_text or "Not Specified",
                        'Experience': "Not Specified",
                        'Job Type': "Not Specified",
                        'Posted Date': "Not Specified",
                        'Job_Link': "Not Specified",
                        'Source': 'LinkedIn'
                    }
                    jobs.append(job_data)
                    print(f"     ✅ LinkedIn Job {i+1}: {title[:30]}... at {company[:20]}...")
                
            except Exception as e:
                print(f"     ❌ Error parsing LinkedIn job {i+1}: {e}")
                continue
        
    except Exception as e:
        print(f"   ❌ LinkedIn scraping failed: {e}")
    
    finally:
        if driver:
            driver.quit()
    
    return jobs

def scrape_indeed_jobs(query, location, max_jobs=20, captcha_solver=None):
    """Scrape job listings directly from Indeed with CAPTCHA handling"""
    print(f"🔍 Scraping Indeed for: {query} in {location}")
    
    driver = None
    jobs = []
    
    try:
        driver = create_selenium_driver(captcha_solver)
        if not driver:
            return []
        
        # Build search URL
        base_url = "https://in.indeed.com/jobs"
        params = {
            'q': query,
            'l': location,
            'sort': 'date'
        }
        query_string = '&'.join([f"{k}={quote(v)}" for k, v in params.items()])
        search_url = f"{base_url}?{query_string}"
        
        print(f"   📍 Navigating to: {search_url}")
        driver.get(search_url)
        
        # Handle potential blocks
        if not handle_captcha_and_blocks(driver, captcha_solver, search_url):
            print("   ❌ Unable to bypass blocking")
            return []
        
        # Wait for jobs to load
        wait = WebDriverWait(driver, 20)
        selectors_to_try = [
            "#mosaic-provider-jobcards",
            ".jobsearch-LeftPane",
            "[data-testid='resultsList']",
            ".jobsearch-SerpMainContent"
        ]
        
        for selector in selectors_to_try:
            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                print(f"   ✅ Found jobs container with selector: {selector}")
                break
            except:
                continue
        
        # Scroll to load more content
        print("   📜 Scrolling to load more jobs...")
        for i in range(2):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
        
        # Parse job listings
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Indeed job card selectors
        job_selectors = [
            'div[class*="job_seen_beacon"]',
            '.cardOutline',
            '.jobsearch-SerpJobCard',
            '[data-tn-component="organicJob"]'
        ]
        
        job_cards = []
        for selector in job_selectors:
            job_cards = soup.select(selector)
            if job_cards:
                print(f"   ✅ Found {len(job_cards)} job cards with selector: {selector}")
                break
        
        print(f"   📊 Processing {len(job_cards)} job cards from Indeed")
        
        for i, card in enumerate(job_cards[:max_jobs]):
            try:
                # Extract basic info
                title = None
                company = None
                location_text = None
                
                # Title
                title_selectors = [
                    'h2[class*="jobTitle"]',
                    '.jobTitle',
                    'h2 a',
                    '[class*="title"]'
                ]
                
                for selector in title_selectors:
                    title_elem = card.select_one(selector)
                    if title_elem:
                        title = title_elem.get_text(strip=True)
                        # Remove "new" from title if present
                        if title.lower().startswith('new'):
                            title = title[3:].strip()
                        break
                
                # Company
                company_selectors = [
                    '[class*="companyName"]',
                    '.companyName',
                    '[data-company-name]',
                    'span[class*="company"]'
                ]
                
                for selector in company_selectors:
                    company_elem = card.select_one(selector)
                    if company_elem:
                        company = company_elem.get_text(strip=True)
                        break
                
                # Location
                location_selectors = [
                    '[class*="companyLocation"]',
                    '.companyLocation',
                    '[class*="location"]',
                    'div[class*="location"]'
                ]
                
                for selector in location_selectors:
                    location_elem = card.select_one(selector)
                    if location_elem:
                        location_text = location_elem.get_text(strip=True)
                        break
                
                if title and company:
                    job_data = {
                        'Job Title': title,
                        'Company': company,
                        'Location': location_text or "Not Specified",
                        'Experience': "Not Specified",
                        'Job Type': "Not Specified",
                        'Posted Date': "Not Specified",
                        'Job_Link': "Not Specified",
                        'Source': 'Indeed'
                    }
                    jobs.append(job_data)
                    print(f"     ✅ Indeed Job {i+1}: {title[:30]}... at {company[:20]}...")
                
            except Exception as e:
                print(f"     ❌ Error parsing Indeed job {i+1}: {e}")
                continue
        
    except Exception as e:
        print(f"   ❌ Indeed scraping failed: {e}")
    
    finally:
        if driver:
            driver.quit()
    
    return jobs

def scrape_naukri_jobs(query, location, max_jobs=20, captcha_solver=None):
    """Scrape job listings directly from Naukri with CAPTCHA handling"""
    print(f"🔍 Scraping Naukri for: {query} in {location}")
    
    driver = None
    jobs = []
    
    try:
        driver = create_selenium_driver(captcha_solver)
        if not driver:
            return []
        
        # Build search URL
        base_url = "https://www.naukri.com"
        params = {
            'k': query,
            'l': location
        }
        query_string = '&'.join([f"{k}={quote(v)}" for k, v in params.items()])
        search_url = f"{base_url}/jobs?{query_string}"
        
        print(f"   📍 Navigating to: {search_url}")
        driver.get(search_url)
        
        # Handle potential blocks
        if not handle_captcha_and_blocks(driver, captcha_solver, search_url):
            print("   ❌ Unable to bypass blocking")
            return []
        
        # Wait for jobs to load
        wait = WebDriverWait(driver, 20)
        selectors_to_try = [
            ".list",
            ".row",
            "[data-testid='tuple']",
            ".srp-tuple"
        ]
        
        for selector in selectors_to_try:
            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                print(f"   ✅ Found jobs container with selector: {selector}")
                break
            except:
                continue
        
        # Scroll to load more content
        print("   📜 Scrolling to load more jobs...")
        for i in range(2):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
        
        # Parse job listings
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Naukri job card selectors
        job_selectors = [
            'article[class*="jobTuple"]',
            '.jobTuple',
            '.row',
            '[data-testid="tuple"]'
        ]
        
        job_cards = []
        for selector in job_selectors:
            job_cards = soup.select(selector)
            if job_cards:
                print(f"   ✅ Found {len(job_cards)} job cards with selector: {selector}")
                break
        
        print(f"   📊 Processing {len(job_cards)} job cards from Naukri")
        
        for i, card in enumerate(job_cards[:max_jobs]):
            try:
                # Extract basic info
                title = None
                company = None
                location_text = None
                experience = None
                
                # Title
                title_selectors = [
                    'a[class*="title"]',
                    '.title',
                    'h2 a',
                    '[class*="jobTitle"]'
                ]
                
                for selector in title_selectors:
                    title_elem = card.select_one(selector)
                    if title_elem:
                        title = title_elem.get_text(strip=True)
                        break
                
                # Company
                company_selectors = [
                    'a[class*="subTitle"]',
                    '.subTitle',
                    '[class*="company"]',
                    '.comp-name'
                ]
                
                for selector in company_selectors:
                    company_elem = card.select_one(selector)
                    if company_elem:
                        company = company_elem.get_text(strip=True)
                        break
                
                # Location
                location_selectors = [
                    'li[class*="location"]',
                    '.location',
                    '[class*="loc"]',
                    '.loc'
                ]
                
                for selector in location_selectors:
                    location_elem = card.select_one(selector)
                    if location_elem:
                        location_text = location_elem.get_text(strip=True)
                        break
                
                # Experience
                exp_selectors = [
                    'li[class*="experience"]',
                    '.exp',
                    '[class*="exp"]'
                ]
                
                for selector in exp_selectors:
                    exp_elem = card.select_one(selector)
                    if exp_elem:
                        experience = exp_elem.get_text(strip=True)
                        break
                
                if title and company:
                    job_data = {
                        'Job Title': title,
                        'Company': company,
                        'Location': location_text or "Not Specified",
                        'Experience': experience or "Not Specified",
                        'Job Type': "Not Specified",
                        'Posted Date': "Not Specified",
                        'Job_Link': "Not Specified",
                        'Source': 'Naukri'
                    }
                    jobs.append(job_data)
                    print(f"     ✅ Naukri Job {i+1}: {title[:30]}... at {company[:20]}...")
                
            except Exception as e:
                print(f"     ❌ Error parsing Naukri job {i+1}: {e}")
                continue
        
    except Exception as e:
        print(f"   ❌ Naukri scraping failed: {e}")
    
    finally:
        if driver:
            driver.quit()
    
    return jobs

def main():
    """Main execution function"""
    print("🚀 Starting Advanced Job Scraping with CAPTCHA Solving...")
    
    # Initialize CAPTCHA solver
    captcha_solver = CAPTCHASolver()
    
    # Initialize ML extractor
    try:
        ml_extractor = AdvancedMLExtractor(MODELS_DIR)
        print("✅ ML Engine initialized successfully")
    except Exception as e:
        print(f"❌ Failed to initialize ML Engine: {e}")
        ml_extractor = None
    
    # Define search parameters
    queries = [
        'Software Engineer',
        'Data Scientist', 
        'Web Developer',
        'Python Developer'
    ]
    locations = ['Bangalore', 'Pune', 'Hyderabad', 'Delhi']
    
    all_jobs = []
    
    # Scrape from all platforms
    for query in queries[:2]:  # Limit queries for testing
        for location in locations[:2]:  # Limit locations for testing
            print(f"\n{'='*60}")
            print(f"🔎 Searching: '{query}' in '{location}'")
            print(f"{'='*60}")
            
            # Scrape from LinkedIn
            linkedin_jobs = scrape_linkedin_jobs(query, location, max_jobs=15, captcha_solver=captcha_solver)
            all_jobs.extend(linkedin_jobs)
            
            time.sleep(random.uniform(3, 6))
            
            # Scrape from Indeed
            indeed_jobs = scrape_indeed_jobs(query, location, max_jobs=15, captcha_solver=captcha_solver)
            all_jobs.extend(indeed_jobs)
            
            time.sleep(random.uniform(3, 6))
            
            # Scrape from Naukri
            naukri_jobs = scrape_naukri_jobs(query, location, max_jobs=15, captcha_solver=captcha_solver)
            all_jobs.extend(naukri_jobs)
            
            time.sleep(random.uniform(5, 8))
    
    # Remove duplicates based on job title and company
    unique_jobs = []
    seen = set()
    
    for job in all_jobs:
        key = (job['Job Title'].lower(), job['Company'].lower())
        if key not in seen:
            seen.add(key)
            unique_jobs.append(job)
    
    print(f"\n📊 Total jobs collected: {len(all_jobs)}")
    print(f"📊 Unique jobs: {len(unique_jobs)}")
    
    # For testing: Only process first 200 jobs
    if len(unique_jobs) > 200:
        unique_jobs = unique_jobs[:200]
        print(f"🧪 TESTING MODE: Limiting to first 200 jobs")
    
    # Save to CSV
    if unique_jobs:
        df = pd.DataFrame(unique_jobs)
        df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
        print(f"\n💾 Data saved to: {OUTPUT_FILE}")
        
        # Print summary
        print(f"\n📈 FINAL SUMMARY:")
        print(f"   Total jobs extracted: {len(unique_jobs)}")
        print(f"   From LinkedIn: {len([j for j in unique_jobs if j.get('Source') == 'LinkedIn'])}")
        print(f"   From Indeed: {len([j for j in unique_jobs if j.get('Source') == 'Indeed'])}")
        print(f"   From Naukri: {len([j for j in unique_jobs if j.get('Source') == 'Naukri'])}")
        print(f"   Job titles: {df['Job Title'].nunique()}")
        print(f"   Companies: {df['Company'].nunique()}")
        print(f"   Locations: {df['Location'].nunique()}")
    
    else:
        print("❌ No jobs were extracted")

if __name__ == "__main__":
    main()