import requests
import csv
import time
import os
import random
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium_stealth import stealth
from tqdm import tqdm
import re
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from transformers import TrOCRProcessor, VisionEncoderDecoderModel
from PIL import Image
from io import BytesIO
import torch

# --- Setup Logging ---
logging.basicConfig(
    filename=os.path.join('Data', f'scraper_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- Setup Directories ---
try:
    Base_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    Base_dir = os.getcwd()

Data_dir = os.path.join(Base_dir, 'Data')
os.makedirs(Data_dir, exist_ok=True)

# --- Stealth Selenium Setup ---
def create_stealth_driver():
    """Create a selenium driver with stealth settings to avoid detection"""
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ]
    options.add_argument(f'user-agent={random.choice(user_agents)}')
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    stealth(driver,
            languages=["en-US", "en"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True)
    
    return driver

# --- Helper Functions ---
def clean_date_posted(date_text):
    """Clean and standardize date posted text"""
    if not date_text:
        return "Not specified"
    
    date_text = date_text.strip()
    
    # Remove common prefixes
    date_text = re.sub(r'^(posted|active|employer\s+)?:?\s*', '', date_text, flags=re.IGNORECASE)
    
    # Extract time patterns
    patterns = [
        r'(\d+\s+(?:day|days|hour|hours|week|weeks|month|months)\s+ago)',
        r'(today|yesterday)',
        r'(\d+[dD])',  # 7d format
        r'(\d+[hH])',  # 2h format
    ]
    
    for pattern in patterns:
        match = re.search(pattern, date_text, re.IGNORECASE)
        if match:
            return match.group(1)
    
    # If it's just a number, assume days
    if re.match(r'^\d+$', date_text):
        return f"{date_text} days ago"
    
    return "Not specified"

def extract_experience_enhanced(text):
    """Enhanced experience extraction with comprehensive patterns"""
    if not text:
        return "0"
    
    text_lower = text.lower()
    
    # Expanded patterns for better matching
    patterns = [
        # Range patterns: 2-4 years, 2 to 4 years, 2 - 4 years
        (r'(\d+)\s*[-–—to]+\s*(\d+)\s*(?:years?|yrs?|y)\s*(?:of)?\s*(?:experience|exp)?', 
         lambda m: f"{m.group(1)}-{m.group(2)}"),
        # Plus patterns: 5+ years, 3+ yrs
        (r'(\d+)\s*\+\s*(?:years?|yrs?|y)\s*(?:of)?\s*(?:experience|exp)?', 
         lambda m: f"{m.group(1)}+"),
        # Minimum patterns: min 3 years, minimum 2 years
        (r'(?:min|minimum|at least|atleast)\s+(\d+)\s*(?:years?|yrs?|y)', 
         lambda m: f"{m.group(1)}+"),
        # Exact years: 3 years, 5 yrs
        (r'(\d+)\s*(?:years?|yrs?|y)(?:\s+(?:of)?\s*(?:experience|exp))?', 
         lambda m: m.group(1)),
        # Senior/lead positions (typically 5+ years)
        (r'\b(senior|lead|principal|sr\.)\b', 
         lambda m: "5+"),
        # Mid-level positions (typically 3-5 years)
        (r'\b(mid[-]?level|mid[-]?senior)\b', 
         lambda m: "3-5"),
        # Junior/entry level (typically 0-2 years)
        (r'\b(junior|jr\.|entry[-]?level)\b', 
         lambda m: "0-2"),
        # Fresher/graduate (0 years)
        (r'\b(fresher|fresh|graduate|recent graduate|no experience)\b', 
         lambda m: "0"),
    ]
    
    # Try all patterns
    for pattern, handler in patterns:
        matches = re.finditer(pattern, text_lower)
        for match in matches:
            try:
                result = handler(match)
                if result:
                    return result
            except:
                continue
    
    # Fallback: Look for any numbers that might indicate experience
    number_matches = re.findall(r'\b(\d+)\s*(?=years?|yrs?|y\b)', text_lower)
    if number_matches:
        return number_matches[0]
    
    return "0"

def filter_experience(exp_text):
    """Keep only jobs that clearly mention >= 2 years of experience"""
    if not exp_text:
        return False  # Exclude if experience not mentioned

    exp_text = exp_text.lower().strip()

    # Exclude obvious fresher/intern roles
    if any(word in exp_text for word in ['fresher', 'intern', 'trainee', 'graduate', 'entry']):
        return False

    # Try to detect explicit experience ranges
    range_match = re.search(r'(\d+)\s*[-–—to]+\s*(\d+)', exp_text)
    if range_match:
        try:
            min_exp = int(range_match.group(1))
            return min_exp >= 2
        except ValueError:
            return False

    # Detect single numeric values like "3 years", "5+ years", "2 yrs"
    num_match = re.search(r'(\d+)\s*(?:\+)?\s*(?:years?|yrs?|y)\b', exp_text)
    if num_match:
        try:
            exp = int(num_match.group(1))
            return exp >= 2
        except ValueError:
            return False

    # Include senior/mid-level keywords even if no numbers
    if any(word in exp_text for word in ['senior', 'lead', 'principal', 'mid']):
        return True

    return False


def solve_captcha_with_huggingface(captcha_url, model_name="anuashok/ocr-captcha-v3"):
    """Solve CAPTCHA using Hugging Face TrOCR-based model."""
    try:
        # Download CAPTCHA image
        response = requests.get(captcha_url)
        image = Image.open(BytesIO(response.content)).convert("RGB")
        
        # Load model and processor (cache to avoid repeated loading)
        processor = TrOCRProcessor.from_pretrained(model_name)
        model = VisionEncoderDecoderModel.from_pretrained(model_name)
        
        # Process image
        pixel_values = processor(image, return_tensors="pt").pixel_values
        generated_ids = model.generate(pixel_values)
        generated_text = processor.batch_decode(generated_ids, skip_special_tokens=True)[0]
        
        logging.info(f"Solved CAPTCHA: {generated_text}")
        return generated_text.strip()
    except Exception as e:
        logging.error(f"Failed to solve CAPTCHA: {e}")
        return None
    

# --- Indeed Scraper ---
def scrape_indeed(max_jobs=50):
    """Scrape up to 150 jobs from Indeed India"""
    logging.info("Starting Indeed scraping")
    print("\n=== Starting Indeed Scraping ===")
    all_jobs = []
    driver = create_stealth_driver()
    
    base_url = "https://in.indeed.com/jobs?q=software+engineer&l=India&explvl=entry_level%2Cmid_level%2Csenior_level&fromage=14"
    
    try:
        driver.get(base_url)
        time.sleep(random.uniform(3, 5))
        
        # Handle pop-ups and consent forms
        try:
                # Try to deny cookie consent or close pop-ups
                deny_selectors = [
                    "button[aria-label*='Deny']",
                    "button[aria-label*='Reject']",
                    "button[class*='deny']",
                    "button[class*='reject']",
                    "button[data-test*='deny']",
                    "button[data-test*='reject']",
                    "button:text('Deny')",
                    "button:text('Reject')",
                    "button:text('No')",
                    "button:text('Not Now')"
                ]
                close_selectors = [
                    "button[aria-label*='Close']",
                    ".modal_closeIcon",
                    "[data-test='close-button']",
                    ".CloseButton",
                    "button[class*='close']",
                    "[data-test='modal-close']",
                    ".e1y5pe2n3",  # Potential modal close (adjust if needed)
                    "button[id*='close']",
                    "div[class*='modal'] button",
                    "button[class*='gd-ui-button']"  # Generic button
                ]
                
                # Attempt to deny cookies first
                for selector in deny_selectors:
                    try:
                        deny_btn = driver.find_element(By.CSS_SELECTOR, selector)
                        driver.execute_script("arguments[0].click();", deny_btn)
                        time.sleep(1.5)
                        logging.info(f"Denied cookies with selector: {selector}")
                        break
                    except:
                        continue
                
                # If no deny option, attempt to close pop-up
                if not any(selector in driver.page_source for selector in deny_selectors):
                    for selector in close_selectors:
                        try:
                            close_btn = driver.find_element(By.CSS_SELECTOR, selector)
                            driver.execute_script("arguments[0].click();", close_btn)
                            time.sleep(1.5)
                            logging.info(f"Closed pop-up with selector: {selector}")
                            break
                        except:
                            continue
        except Exception as e:
            logging.warning(f"Failed to close pop-up/consent on Indeed: {e}")
        
        
        # Check for CAPTCHA
        try:
            captcha_elements = driver.find_elements(By.CSS_SELECTOR, "iframe[title*='CAPTCHA'], div[id*='captcha'], div[class*='captcha']")
            if captcha_elements:
                logging.warning("CAPTCHA detected on Indeed")
                print("CAPTCHA detected on Indeed! Please solve manually and press Enter to continue...")
                time.sleep(10)
                input()
        except Exception as e:
            logging.warning(f"Error checking for CAPTCHA on Indeed: {e}")
        
        jobs_collected = 0
        page = 0
        
        with tqdm(total=max_jobs, desc="Indeed Jobs") as pbar:
            while jobs_collected < max_jobs:
                retries = 3
                for attempt in range(retries):
                    try:
                        # Wait for job cards with multiple selectors
                        job_card_selectors = [
                            (By.CLASS_NAME, "job_seen_beacon"),
                            (By.CSS_SELECTOR, "div.jobsearch-SerpJobCard"),
                            (By.CSS_SELECTOR, "[data-testid='job-card']"),
                            (By.TAG_NAME, "article")
                        ]
                        
                        cards_found = False
                        for by, selector in job_card_selectors:
                            try:
                                WebDriverWait(driver, 20).until(
                                    EC.presence_of_all_elements_located((by, selector))
                                )
                                logging.info(f"Found job cards using selector: {selector}")
                                cards_found = True
                                break
                            except:
                                continue
                        
                        if not cards_found:
                            logging.error(f"Attempt {attempt + 1}/{retries}: No job cards found on Indeed page {page + 1}")
                            if attempt == retries - 1:
                                screenshot_path = os.path.join(Data_dir, f"indeed_timeout_page_{page + 1}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
                                driver.save_screenshot(screenshot_path)
                                logging.info(f"Saved screenshot: {screenshot_path}")
                                print(f"Saved screenshot: {screenshot_path}")
                                raise TimeoutException("No job cards found after retries")
                        
                        # Gradual scrolling to mimic human behavior
                        for i in range(3):
                            driver.execute_script(f"window.scrollTo(0, {500 * (i + 1)});")
                            time.sleep(random.uniform(1, 2))
                        
                        time.sleep(random.uniform(2, 4))
                        
                        soup = BeautifulSoup(driver.page_source, 'html.parser')
                        job_cards = soup.find_all('div', class_='job_seen_beacon')
                        if not job_cards:
                            job_cards = soup.find_all('div', class_='jobsearch-SerpJobCard')
                        if not job_cards:
                            job_cards = soup.find_all('div', {'data-testid': 'job-card'})
                        if not job_cards:
                            job_cards = soup.find_all('article')
                        
                        logging.info(f"Found {len(job_cards)} job cards on Indeed page {page + 1}")
                        print(f"Found {len(job_cards)} job cards on Indeed page {page + 1}")
                        
                        if not job_cards:
                            logging.info("No more jobs found on Indeed")
                            print("No more jobs found on Indeed")
                            break
                        
                        for card in job_cards:
                            if jobs_collected >= max_jobs:
                                break
                            
                            try:
                                # Job title
                                title_elem = card.find('h2', class_='jobTitle')
                                if not title_elem:
                                    title_elem = card.find('h2')
                                if not title_elem:
                                    title_elem = card.find('a', {'data-testid': 'job-title'})
                                if not title_elem:
                                    title_elem = card.find('a', class_=lambda x: x and 'title' in str(x).lower())
                                job_title = title_elem.get_text(strip=True) if title_elem else ''
                                if not job_title or len(job_title) < 3:
                                    logging.warning(f"Skipping job card: No valid job title. Card HTML: {card.prettify()[:500]}")
                                    continue
                                
                                # Job link
                                link_elem = card.find('a', class_='jcs-JobTitle')
                                if not link_elem:
                                    link_elem = card.find('a', href=True)
                                job_link = "https://in.indeed.com" + link_elem['href'] if link_elem and link_elem.get('href') else ''
                                
                                # Company name
                                company_elem = card.find('span', {'data-testid': 'company-name'})
                                if not company_elem:
                                    company_elem = card.find('span', class_=lambda x: x and 'company' in str(x).lower())
                                if not company_elem:
                                    company_elem = card.find('div', class_=lambda x: x and 'company' in str(x).lower())
                                company_name = company_elem.get_text(strip=True) if company_elem else 'Not specified'
                                
                                # Salary
                                salary_elem = card.find('div', class_='salary-snippet-container')
                                if not salary_elem:
                                    salary_elem = card.find('div', class_=lambda x: x and 'salary' in str(x).lower())
                                if not salary_elem:
                                    salary_elem = card.find('span', class_=lambda x: x and 'salary' in str(x).lower())
                                salary = salary_elem.get_text(strip=True) if salary_elem else 'Not Disclosed'
                                
                                # Date posted
                                date_elem = card.find('span', class_='date')
                                if not date_elem:
                                    date_elem = card.find('span', class_=lambda x: x and 'date' in str(x).lower())
                                date_posted_raw = date_elem.get_text(strip=True) if date_elem else ''
                                date_posted = clean_date_posted(date_posted_raw)
                                
                                # Experience
                                snippet_elem = card.find('div', class_='job-snippet')
                                if not snippet_elem:
                                    snippet_elem = card.find('div', class_=lambda x: x and 'snippet' in str(x).lower())
                                snippet_text = snippet_elem.get_text(strip=True) if snippet_elem else card.get_text(strip=True)
                                experience = extract_experience_enhanced(snippet_text)
                                
                                # Relaxed filtering for debugging
                                if job_title:
                                    all_jobs.append({
                                        'job_title': job_title,
                                        'company_name': company_name,
                                        'job_link': job_link,
                                        'experience': experience,
                                        'salary': salary,
                                        'date_posted': date_posted
                                    })
                                    jobs_collected += 1
                                    pbar.update(1)
                                    logging.info(f"Collected job: {job_title} at {company_name} (Experience: {experience})")
                                else:
                                    logging.info(f"Skipped job due to experience filter: {experience}. Card HTML: {card.prettify()[:500]}")
                            
                            except Exception as e:
                                logging.error(f"Error parsing Indeed job card: {e}. Card HTML: {card.prettify()[:500]}")
                                continue
                        
                        # Pagination
                        if jobs_collected < max_jobs:
                            try:
                                next_button = None
                                next_selectors = [
                                    '[data-testid="pagination-page-next"]',
                                    'a[aria-label="Next"]',
                                    'button[aria-label="Next"]',
                                    'a[class*="next"]',
                                    'button[class*="next"]',
                                    'a[href*="start="]'  # Fallback for Indeed's URL-based pagination
]
                                for selector in next_selectors:
                                    try:
                                        next_button = driver.find_element(By.CSS_SELECTOR, selector)
                                        if next_button.is_displayed() and next_button.is_enabled():
                                            break
                                        next_button = None
                                    except:
                                        continue
                                
                                if next_button:
                                    old_html = driver.page_source
                                    driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                                    time.sleep(random.uniform(1, 2))
                                    driver.execute_script("arguments[0].click();", next_button)
                                    WebDriverWait(driver, 20).until(
                                        lambda d: d.page_source != old_html
                                    )
                                    time.sleep(random.uniform(3, 5))
                                    page += 1
                                    logging.info(f"Moving to Indeed page {page + 1}")
                                else:
                                    logging.info("No more pages on Indeed")
                                    print("No more pages on Indeed")
                                    break
                            except (NoSuchElementException, TimeoutException) as e:
                                logging.info(f"No more pages on Indeed: {e}")
                                print(f"No more pages on Indeed: {e}")
                                break
                        else:
                            break
                        
                        break  # Exit retry loop if successful
                    except TimeoutException:
                        logging.warning(f"Attempt {attempt + 1}/{retries}: Timeout waiting for job cards on Indeed")
                        if attempt < retries - 1:
                            time.sleep(random.uniform(5, 10))
                        continue
                        
                if not cards_found:
                    break  # Exit if no cards found after retries
                    
    except Exception as e:
        logging.error(f"Error scraping Indeed: {e}")
        print(f"Error scraping Indeed: {e}")
    finally:
        driver.quit()
        logging.info(f"Collected {len(all_jobs)} jobs from Indeed")
        print(f"✓ Collected {len(all_jobs)} jobs from Indeed")
    
    return all_jobs

# --- Shine Scraper ---
def scrape_shine(max_jobs=50):
    """Scrape up to 50 jobs from Shine.com using Selenium"""
    logging.info("Starting Shine scraping")
    print("\n=== Starting Shine Scraping ===")
    all_jobs = []
    driver = create_stealth_driver()
    
    base_url = "https://www.shine.com/job-search/software-engineer-jobs-in-india"
    
    try:
        driver.get(base_url)
        time.sleep(random.uniform(5, 7))
        
        # Handle pop-ups and consent forms
        try:
            close_selectors = [
                "button[aria-label='Close']",
                ".closeBtn",
                "[data-testid='modal-close']",
                "button[id*='reject']",
                "button[id*='accept']",
                ".gdpr-consent-button",
                ".modal-close"
            ]
            for selector in close_selectors:
                try:
                    close_btn = driver.find_element(By.CSS_SELECTOR, selector)
                    driver.execute_script("arguments[0].click();", close_btn)
                    time.sleep(1)
                    logging.info(f"Closed element with selector: {selector}")
                    break
                except:
                    continue
        except Exception as e:
            logging.warning(f"Failed to close pop-up/consent on Shine: {e}")
        
        # Check for CAPTCHA
        try:
            captcha_elements = driver.find_elements(By.CSS_SELECTOR, "iframe[title*='CAPTCHA'], div[id*='captcha'], div[class*='captcha']")
            if captcha_elements:
                logging.warning("CAPTCHA detected on Indeed")
                print("CAPTCHA detected on Indeed! Please solve manually and press Enter to continue...")
                time.sleep(5)  # Give time to notice the prompt
                input()
        except Exception as e:
            logging.warning(f"Error checking for CAPTCHA on Shine: {e}")
        
        jobs_collected = 0
        page = 1
        
        with tqdm(total=max_jobs, desc="Shine Jobs") as pbar:
            while jobs_collected < max_jobs:
                retries = 3
                for attempt in range(retries):
                    try:
                        # Wait for job cards
                        job_card_selectors = [
                            (By.CSS_SELECTOR, "div[class*='jobCard']"),
                            (By.CSS_SELECTOR, "li[class*='job']"),
                            (By.CSS_SELECTOR, "div[id*='job']"),
                            (By.CSS_SELECTOR, "div[data-job-id]"),
                            (By.TAG_NAME, "article")
                        ]
                        
                        cards_found = False
                        for by, selector in job_card_selectors:
                            try:
                                WebDriverWait(driver, 20).until(
                                    EC.presence_of_all_elements_located((by, selector))
                                )
                                logging.info(f"Found job cards using selector: {selector}")
                                cards_found = True
                                break
                            except:
                                continue
                        
                        if not cards_found:
                            logging.error(f"Attempt {attempt + 1}/{retries}: No job cards found on Shine page {page}")
                            if attempt == retries - 1:
                                screenshot_path = os.path.join(Data_dir, f"shine_timeout_page_{page}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
                                driver.save_screenshot(screenshot_path)
                                logging.info(f"Saved screenshot: {screenshot_path}")
                                print(f"Saved screenshot: {screenshot_path}")
                                raise TimeoutException("No job cards found after retries")
                        
                        time.sleep(random.uniform(3, 5))
                        
                        soup = BeautifulSoup(driver.page_source, 'html.parser')
                        
                        job_cards = soup.find_all('div', {'class': lambda x: x and 'jobCard' in str(x)})
                        if not job_cards:
                            job_cards = soup.find_all('li', {'class': lambda x: x and 'job' in str(x).lower()})
                        if not job_cards:
                            job_cards = soup.find_all('div', {'id': lambda x: x and x.startswith('job')})
                        if not job_cards:
                            job_cards = soup.find_all('div', attrs={'data-job-id': True})
                        if not job_cards:
                            job_cards = soup.find_all('article')
                        
                        logging.info(f"Found {len(job_cards)} job cards on Shine page {page}")
                        print(f"Found {len(job_cards)} job cards on Shine page {page}")
                        
                        if not job_cards:
                            logging.info("No more jobs found on Shine")
                            break
                        
                        for card in job_cards:
                            if jobs_collected >= max_jobs:
                                break
                            
                            try:
                                # Job title
                                title_elem = (
                                    card.find('h2') or 
                                    card.find('h3') or 
                                    card.find('a', {'class': lambda x: x and 'title' in str(x).lower()}) or
                                    card.find('strong') or
                                    card.find('b')
                                )
                                
                                job_title = title_elem.get_text(strip=True) if title_elem else ''
                                if not job_title or len(job_title) < 3:
                                    logging.warning("Skipping job card: No valid job title")
                                    continue
                                
                                # Job link
                                link_elem = card.find('a', href=True)
                                job_link = ''
                                if link_elem:
                                    href = link_elem.get('href', '')
                                    if href:
                                        job_link = href if href.startswith('http') else f"https://www.shine.com{href}"
                                
                                # Company name with enhanced selectors
                                company_elem = None
                                company_selectors = [
                                    ('div', {'class': lambda x: x and 'company' in str(x).lower()}),
                                    ('span', {'class': lambda x: x and 'company' in str(x).lower()}),
                                    ('a', {'class': lambda x: x and 'company' in str(x).lower()}),
                                    ('p', {'class': lambda x: x and 'company' in str(x).lower()}),
                                    ('div', {'data-testid': lambda x: x and 'company' in str(x).lower()}),
                                    ('span', {'data-testid': lambda x: x and 'company' in str(x).lower()}),
                                    ('div', {}),  # Fallback to any div
                                    ('span', {}),  # Fallback to any span
                                ]
                                
                                for tag, attrs in company_selectors:
                                    company_elem = card.find(tag, attrs)
                                    if company_elem and company_elem.get_text(strip=True):
                                        company_name = company_elem.get_text(strip=True)
                                        if len(company_name) > 2 and not any(keyword in company_name.lower() for keyword in ['job', 'description', 'salary', 'experience', 'location']):
                                            break
                                    company_elem = None
                                
                                # Fallback: Search for company name in card text
                                if not company_elem:
                                    card_text = card.get_text(strip=True)
                                    for line in card_text.split('\n'):
                                        line = line.strip()
                                        if len(line) > 2 and not any(keyword in line.lower() for keyword in ['job', 'description', 'salary', 'experience', 'location', 'ago', 'day', 'week', 'month', 'posted']):
                                            company_name = line
                                            break
                                    else:
                                        company_name = 'Not specified'
                                        logging.warning(f"Could not extract company name for job '{job_title}'. Card HTML: {card.prettify()[:500]}")
                                
                                # Experience
                                card_text = card.get_text(strip=True)
                                experience = extract_experience_enhanced(card_text)
                                
                                # Salary
                                salary = 'Not Disclosed'
                                salary_elems = card.find_all(['span', 'div'], limit=10)
                                for elem in salary_elems:
                                    elem_text = elem.get_text(strip=True)
                                    if len(elem_text) < 100 and any(pattern in elem_text.lower() for pattern in ['₹', 'lakh', 'lpa', 'salary']):
                                        if not any(word in elem_text.lower() for word in ['description', 'responsibility', 'requirement', 'skill']):
                                            salary = elem_text
                                            break
                                
                                # Date posted
                                date_posted = "Not specified"
                                date_patterns = ['ago', 'day', 'hour', 'week', 'posted']
                                for elem in card.find_all(['span', 'div', 'time'], limit=10):
                                    elem_text = elem.get_text(strip=True)
                                    if len(elem_text) < 50 and any(pattern in elem_text.lower() for pattern in date_patterns):
                                        date_posted = clean_date_posted(elem_text)
                                        break
                                
                               # Inside the job card loop in scrape_indeed, replace the filtering condition:
                                if job_title and filter_experience(experience):  # Changed from `if job_title`
                                    all_jobs.append({
                                        'job_title': job_title,
                                        'company_name': company_name,
                                        'job_link': job_link,
                                        'experience': experience,
                                        'salary': salary,
                                        'date_posted': date_posted
                                    })
                                    jobs_collected += 1
                                    pbar.update(1)
                                    logging.info(f"Collected job: {job_title} at {company_name} (Experience: {experience})")
                                else:
                                    logging.info(f"Skipped job '{job_title}' due to experience filter: {experience}. Card HTML: {card.prettify()[:500]}")
                            
                            except Exception as e:
                                logging.error(f"Error parsing Shine job card: {e}")
                                continue
                        
                        # Pagination
                        if jobs_collected < max_jobs and len(job_cards) > 0:
                            try:
                                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                                time.sleep(4)
                                
                                next_button = None
                                next_selectors = [
                                    'a[class*="next"]',
                                    'button[class*="next"]',
                                    'a[aria-label="Next"]',
                                    'button[aria-label="Next"]',
                                    'a[href*="page="]'
                                ]
                                
                                for selector in next_selectors:
                                    try:
                                        next_button = driver.find_element(By.CSS_SELECTOR, selector)
                                        if next_button.is_displayed() and next_button.is_enabled():
                                            break
                                        next_button = None
                                    except:
                                        continue
                                
                                if next_button:
                                    old_html = driver.page_source
                                    driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", next_button)
                                    time.sleep(3)
                                    driver.execute_script("arguments[0].click();", next_button)
                                    WebDriverWait(driver, 20).until(
                                        lambda d: d.page_source != old_html
                                    )
                                    time.sleep(random.uniform(5, 7))
                                    page += 1
                                    logging.info(f"Moving to Shine page {page + 1}")
                                else:
                                    logging.info("No next button found on Shine")
                                    print("No next button found on Shine")
                                    break
                            except Exception as e:
                                logging.error(f"Pagination error on Shine page {page}: {e}")
                                print(f"Pagination error on Shine: {e}")
                                break
                        else:
                            break
                        
                        break  # Exit retry loop if successful
                    except TimeoutException:
                        logging.warning(f"Attempt {attempt + 1}/{retries}: Timeout waiting for job cards on Shine page {page}")
                        if attempt < retries - 1:
                            time.sleep(random.uniform(5, 10))
                        continue
                        
                if not cards_found:
                    break  # Exit if no cards found after retries
                    
    except Exception as e:
        logging.error(f"Error scraping Shine: {e}")
        print(f"Error scraping Shine: {e}")
    finally:
        driver.quit()
        logging.info(f"Collected {len(all_jobs)} jobs from Shine")
    
    return all_jobs

# --- Glassdoor Scraper ---
def scrape_glassdoor(max_jobs=50):
    """Scrape up to 50 jobs from Glassdoor"""
    logging.info("Starting Glassdoor scraping")
    print("\n=== Starting Glassdoor Scraping ===")
    all_jobs = []
    driver = create_stealth_driver()
    
    base_url = "https://www.glassdoor.co.in/Job/india-software-engineer-jobs-SRCH_IL.0,5_IN115_KO6,23.htm"
    
    try:
        driver.get(base_url)
        time.sleep(random.uniform(10, 15))  # Increased wait for initial page load
        
        # Handle pop-ups, including cookie consent with deny option
        try:
            # Try to deny cookie consent or close pop-ups
            deny_selectors = [
                "button[aria-label*='Deny']",
                "button[aria-label*='Reject']",
                "button[class*='deny']",
                "button[class*='reject']",
                "button[data-test*='deny']",
                "button[data-test*='reject']",
                "button:text('Deny')",
                "button:text('Reject')",
                "button:text('No')",
                "button:text('Not Now')"
            ]
            close_selectors = [
                "button[aria-label*='Close']",
                ".modal_closeIcon",
                "[data-test='close-button']",
                ".CloseButton",
                "button[class*='close']",
                "[data-test='modal-close']",
                ".e1y5pe2n3",
                "button[id*='close']",
                "div[class*='modal'] button",
                "button[class*='gd-ui-button']" 
            ]
            
            for selector in deny_selectors:
                try:
                    deny_btn = driver.find_element(By.CSS_SELECTOR, selector)
                    driver.execute_script("arguments[0].click();", deny_btn)
                    time.sleep(1.5)
                    logging.info(f"Denied cookies with selector: {selector}")
                    break
                except:
                    continue
            
            # If no deny option, attempt to close pop-up
            if not any(selector in driver.page_source for selector in deny_selectors):
                for selector in close_selectors:
                    try:
                        close_btn = driver.find_element(By.CSS_SELECTOR, selector)
                        driver.execute_script("arguments[0].click();", close_btn)
                        time.sleep(1.5)
                        logging.info(f"Closed pop-up with selector: {selector}")
                        break
                    except:
                        continue
            
        except Exception as e:
            logging.warning(f"Failed to handle pop-up/cookie consent on Glassdoor: {e}")
        
        # Check for CAPTCHA or sign-in prompt with automated solving attempt
        try:
            captcha_elements = driver.find_elements(By.CSS_SELECTOR, 
                "iframe[src*='recaptcha'], div[class*='g-recaptcha'], div[id*='captcha'], div[class*='captcha'], div[class*='signin'], div[id*='SignIn']")
            if captcha_elements:
                logging.warning("CAPTCHA or sign-in prompt detected on Glassdoor")
                try:
                    # Attempt to handle reCAPTCHA (v2/v3)
                    captcha_iframe = driver.find_element(By.CSS_SELECTOR, "iframe[src*='recaptcha']")
                    driver.switch_to.frame(captcha_iframe)
                    
                    # Try to find the challenge image (adjust selector based on Glassdoor's CAPTCHA)
                    captcha_img_elem = driver.find_element(By.CSS_SELECTOR, "img[src*='captcha']")  # Placeholder; inspect for actual src
                    if not captcha_img_elem:
                        captcha_img_elem = driver.find_element(By.TAG_NAME, "img")  # Fallback
                    
                    captcha_url = captcha_img_elem.get_attribute('src')
                    driver.switch_to.default_content()
                    
                    solved_text = solve_captcha_with_huggingface(captcha_url)
                    if solved_text:
                        # Attempt to submit solved text (Glassdoor reCAPTCHA may not have a direct input; adjust)
                        try:
                            input_field = driver.find_element(By.CSS_SELECTOR, "input[name='g-recaptcha-response']")
                            input_field.clear()
                            input_field.send_keys(solved_text)
                            submit_btn = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
                            driver.execute_script("arguments[0].click();", submit_btn)
                            time.sleep(5)  # Wait for verification
                            logging.info(f"Submitted CAPTCHA solution: {solved_text}")
                        except Exception as submit_e:
                            logging.warning(f"Failed to submit CAPTCHA solution: {submit_e}")
                            print("Automated CAPTCHA submission failed. Please solve manually and press Enter...")
                            time.sleep(10)
                            input()
                    else:
                        print("Automated CAPTCHA solving failed. Please solve manually and press Enter...")
                        time.sleep(10)
                        input()
                except Exception as e:
                    logging.warning(f"Automated CAPTCHA solving error: {e}")
                    print("CAPTCHA detected! Please solve manually and press Enter to continue...")
                    time.sleep(10)
                    input()
        except Exception as e:
            logging.warning(f"Error checking for CAPTCHA on Glassdoor: {e}")
        
        jobs_collected = 0
        page = 1
        
        with tqdm(total=max_jobs, desc="Glassdoor Jobs") as pbar:
            while jobs_collected < max_jobs:
                retries = 3
                for attempt in range(retries):
                    try:
                        # Updated selectors for job cards
                        wait_selectors = [
                            '[data-test="jobListing"]',
                            'li[class*="jobListItem"]',
                            'div[class*="JobCard"]',
                            'ul[class*="jobsList"] li',
                            'article[class*="job"]',
                            'li[class*="react-job-listing"]',
                            'div[data-test*="job-card"]'
                        ]
                        
                        cards_found = False
                        for selector in wait_selectors:
                            try:
                                WebDriverWait(driver, 25).until(
                                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector))
                                )
                                logging.info(f"Found job cards using selector: {selector}")
                                cards_found = True
                                break
                            except:
                                continue
                        
                        if not cards_found:
                            logging.error(f"Attempt {attempt + 1}/{retries}: No job cards found on Glassdoor page {page}")
                            if attempt == retries - 1:
                                logging.info(f"No job cards found on Glassdoor page {page} after retries")
                                print(f"No job cards found on Glassdoor page {page} after retries")
                                raise TimeoutException("No job cards found after retries")
                        
                        # Aggressive scrolling to load all jobs
                        for i in range(6):
                            driver.execute_script(f"window.scrollTo(0, {800 * (i + 1)});")
                            time.sleep(random.uniform(2, 3))
                        
                        time.sleep(random.uniform(5, 7))  # Extra wait for dynamic content
                        
                        soup = BeautifulSoup(driver.page_source, 'html.parser')
                        
                        # Updated job card selectors
                        job_cards = soup.find_all('li', {'data-test': 'jobListing'})
                        if not job_cards:
                            job_cards = soup.find_all('li', class_=lambda x: x and 'joblistitem' in str(x).lower())
                        if not job_cards:
                            job_cards = soup.find_all('div', class_=lambda x: x and 'jobcard' in str(x).lower())
                        if not job_cards:
                            job_cards = soup.select('ul[class*="jobsList"] > li')
                        if not job_cards:
                            job_cards = soup.find_all('article', class_=lambda x: x and 'job' in str(x).lower())
                        if not job_cards:
                            job_cards = soup.find_all('li', class_=lambda x: x and 'react-job-listing' in str(x).lower())
                        if not job_cards:
                            job_cards = soup.find_all('div', {'data-test': lambda x: x and 'job-card' in str(x).lower()})
                        
                        logging.info(f"Found {len(job_cards)} jobs on Glassdoor page {page}")
                        print(f"Found {len(job_cards)} jobs on Glassdoor page {page}")
                        
                        if not job_cards:
                            logging.info("No more jobs found on Glassdoor")
                            print("No more jobs found on Glassdoor")
                            break
                        
                        for card in job_cards:
                            if jobs_collected >= max_jobs:
                                break
                            
                            try:
                                # Job title
                                title_elem = None
                                title_selectors = [
                                    ('a', {'data-test': 'job-title'}),
                                    ('a', {'class': lambda x: x and 'jobtitle' in str(x).lower()}),
                                    ('div', {'class': lambda x: x and 'jobtitle' in str(x).lower()}),
                                    ('h2', {}),
                                    ('h3', {}),
                                    ('a', {'href': lambda x: x and '/job-listing/' in str(x)}),
                                    ('span', {'class': lambda x: x and 'title' in str(x).lower()})
                                ]
                                
                                for tag, attrs in title_selectors:
                                    title_elem = card.find(tag, attrs)
                                    if title_elem and title_elem.get_text(strip=True):
                                        break
                                
                                job_title = title_elem.get_text(strip=True) if title_elem else ''
                                if not job_title or len(job_title) < 3:
                                    logging.warning(f"Skipping job card: No valid job title. Card HTML: {card.prettify()[:500]}")
                                    continue
                                
                                # Job link
                                job_link = ''
                                if title_elem and title_elem.name == 'a' and title_elem.get('href'):
                                    href = title_elem['href']
                                    job_link = f"https://www.glassdoor.co.in{href}" if href.startswith('/') else href
                                else:
                                    link_elem = card.find('a', href=lambda x: x and '/job-listing/' in str(x))
                                    if link_elem:
                                        href = link_elem['href']
                                        job_link = f"https://www.glassdoor.co.in{href}" if href.startswith('/') else href
                                    else:
                                        link_elem = card.find('a', href=True)
                                        if link_elem:
                                            href = link_elem['href']
                                            job_link = f"https://www.glassdoor.co.in{href}" if href.startswith('/') else href
                                
                                # Company name
                                company_elem = None
                                company_selectors = [
                                    ('span', {'data-test': 'employer-name'}),
                                    ('div', {'data-test': 'employer-name'}),
                                    ('span', {'class': lambda x: x and 'employer' in str(x).lower()}),
                                    ('div', {'class': lambda x: x and 'employer' in str(x).lower()}),
                                    ('a', {'class': lambda x: x and 'company' in str(x).lower()}),
                                    ('span', {})
                                ]
                                
                                for tag, attrs in company_selectors:
                                    company_elem = card.find(tag, attrs)
                                    if company_elem and company_elem.get_text(strip=True):
                                        company_name = company_elem.get_text(strip=True)
                                        if len(company_name) > 2 and not any(keyword in company_name.lower() for keyword in ['job', 'description', 'salary', 'experience', 'location']):
                                            break
                                    company_elem = None
                                
                                company_name = company_elem.get_text(strip=True) if company_elem else 'Not specified'
                                
                                # Salary
                                salary = 'Not Disclosed'
                                salary_elem = None
                                salary_selectors = [
                                    ('span', {'data-test': 'detailSalary'}),
                                    ('div', {'data-test': 'detailSalary'}),
                                    ('div', {'class': lambda x: x and 'salary' in str(x).lower()}),
                                    ('span', {'class': lambda x: x and 'salary' in str(x).lower()}),
                                    ('p', {'class': lambda x: x and 'salary' in str(x).lower()})
                                ]
                                
                                for tag, attrs in salary_selectors:
                                    salary_elem = card.find(tag, attrs)
                                    if salary_elem and salary_elem.get_text(strip=True):
                                        salary_text = salary_elem.get_text(strip=True)
                                        if len(salary_text) < 100 and not any(keyword in salary_text.lower() for keyword in ['description', 'responsibility', 'requirement', 'skill']):
                                            salary = salary_text
                                            break
                                
                                # Description for experience
                                desc_elem = card.find('div', {'data-test': 'job-description'})
                                if not desc_elem:
                                    desc_elem = card.find('div', class_=lambda x: x and 'description' in str(x).lower())
                                if not desc_elem:
                                    desc_elem = card.find('p', class_=lambda x: x and 'description' in str(x).lower())
                                desc_text = desc_elem.get_text(strip=True) if desc_elem else ''
                                
                                full_text = job_title + ' ' + desc_text + ' ' + company_name + ' ' + card.get_text(strip=True)
                                experience = extract_experience_enhanced(full_text)
                                
                                # Date posted
                                date_posted = 'Not specified'
                                date_elem = None
                                date_selectors = [
                                    ('div', {'data-test': 'job-age'}),
                                    ('div', {'class': lambda x: x and 'age' in str(x).lower()}),
                                    ('span', {'class': lambda x: x and 'posted' in str(x).lower()}),
                                    ('time', {}),
                                    ('span', {'class': lambda x: x and 'date' in str(x).lower()})
                                ]
                                
                                for tag, attrs in date_selectors:
                                    date_elem = card.find(tag, attrs)
                                    if date_elem and date_elem.get_text(strip=True):
                                        date_posted = clean_date_posted(date_elem.get_text(strip=True))
                                        break
                                
                                if job_title and filter_experience(experience):
                                    all_jobs.append({
                                        'job_title': job_title,
                                        'company_name': company_name,
                                        'job_link': job_link,
                                        'experience': experience,
                                        'salary': salary,
                                        'date_posted': date_posted
                                    })
                                    jobs_collected += 1
                                    pbar.update(1)
                                    logging.info(f"Collected job: {job_title} at {company_name} (Experience: {experience})")
                                else:
                                    logging.info(f"Skipped job '{job_title}' due to experience filter: {experience}. Card HTML: {card.prettify()[:500]}")
                            
                            except Exception as e:
                                logging.error(f"Error parsing Glassdoor job card: {e}. Card HTML: {card.prettify()[:500]}")
                                continue
                        
                        # Pagination
                        if jobs_collected < max_jobs and len(job_cards) > 0:
                            try:
                                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                                time.sleep(random.uniform(4, 6))
                                
                                next_button = None
                                next_selectors = [
                                    'button[data-test="pagination-next"]',
                                    'button[aria-label="Next"]',
                                    'a[data-test="pagination-next"]',
                                    'button[class*="next"]',
                                    'a[class*="next"]',
                                    'button[data-testid*="next"]',
                                    'a[href*="page="]',
                                    'button[class*="pagination"]'
                                ]
                                
                                for selector in next_selectors:
                                    try:
                                        next_button = driver.find_element(By.CSS_SELECTOR, selector)
                                        if next_button.is_enabled() and next_button.is_displayed():
                                            break
                                        next_button = None
                                    except:
                                        continue
                                
                                if next_button:
                                    old_html = driver.page_source
                                    driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", next_button)
                                    time.sleep(random.uniform(2, 4))
                                    driver.execute_script("arguments[0].click();", next_button)
                                    WebDriverWait(driver, 25).until(
                                        lambda d: d.page_source != old_html
                                    )
                                    time.sleep(random.uniform(8, 12))
                                    page += 1
                                    logging.info(f"Moving to Glassdoor page {page}")
                                else:
                                    logging.info("No more pages on Glassdoor")
                                    print("No more pages on Glassdoor")
                                    break
                            except Exception as e:
                                logging.error(f"Pagination error on Glassdoor page {page}: {e}")
                                print(f"Pagination error on Glassdoor: {e}")
                                break
                        else:
                            break
                        
                        break  # Exit retry loop if successful
                    except TimeoutException:
                        logging.warning(f"Attempt {attempt + 1}/{retries}: Timeout waiting for job cards on Glassdoor page {page}")
                        if attempt < retries - 1:
                            time.sleep(random.uniform(6, 10))
                        continue
                        
                if not cards_found:
                    break  # Exit if no cards found after retries
                    
    except Exception as e:
        logging.error(f"Error scraping Glassdoor: {e}")
        print(f"Error scraping Glassdoor: {e}")
    finally:
        driver.quit()
        logging.info(f"Collected {len(all_jobs)} jobs from Glassdoor")
        print(f"✓ Collected {len(all_jobs)} jobs from Glassdoor")
    
    return all_jobs

# --- Foundit Scraper ---
def scrape_foundit(max_jobs=50):
    """Scrape up to 150 jobs from Foundit"""
    logging.info("Starting Foundit scraping")
    print("\n=== Starting Foundit Scraping ===")
    all_jobs = []
    driver = create_stealth_driver()
    
    base_url = "https://www.foundit.in/srp/results?query=software%20engineer&locations=India"
    
    try:
        driver.get(base_url)
        time.sleep(random.uniform(8, 12))  # Increased wait for dynamic content
        
        try:
                # Try to deny cookie consent or close pop-ups
                deny_selectors = [
                    "button[aria-label*='Deny']",
                    "button[aria-label*='Reject']",
                    "button[class*='deny']",
                    "button[class*='reject']",
                    "button[data-test*='deny']",
                    "button[data-test*='reject']",
                    "button:text('Deny')",
                    "button:text('Reject')",
                    "button:text('No')",
                    "button:text('Not Now')"
                ]
                close_selectors = [
                    "button[aria-label*='Close']",
                    ".modal_closeIcon",
                    "[data-test='close-button']",
                    ".CloseButton",
                    "button[class*='close']",
                    "[data-test='modal-close']",
                    ".e1y5pe2n3",  # Potential modal close (adjust if needed)
                    "button[id*='close']",
                    "div[class*='modal'] button",
                    "button[class*='gd-ui-button']"  # Generic button
                ]
                
                # Attempt to deny cookies first
                for selector in deny_selectors:
                    try:
                        deny_btn = driver.find_element(By.CSS_SELECTOR, selector)
                        driver.execute_script("arguments[0].click();", deny_btn)
                        time.sleep(1.5)
                        logging.info(f"Denied cookies with selector: {selector}")
                        break
                    except:
                        continue
                
                # If no deny option, attempt to close pop-up
                if not any(selector in driver.page_source for selector in deny_selectors):
                    for selector in close_selectors:
                        try:
                            close_btn = driver.find_element(By.CSS_SELECTOR, selector)
                            driver.execute_script("arguments[0].click();", close_btn)
                            time.sleep(1.5)
                            logging.info(f"Closed pop-up with selector: {selector}")
                            break
                        except:
                            continue
                
        except Exception as e:
            logging.warning(f"Failed to handle pop-up/cookie consent on Foundit: {e}")
        
        # Check for CAPTCHA
        try:
            captcha_elements = driver.find_elements(By.CSS_SELECTOR, "iframe[title*='CAPTCHA'], div[id*='captcha'], div[class*='captcha']")
            if captcha_elements:
                logging.warning("CAPTCHA detected on Foundit")
                print("CAPTCHA detected on Foundit! Please solve manually and press Enter to continue...")
                input()
        except Exception as e:
            logging.warning(f"Error checking for CAPTCHA on Foundit: {e}")
        
        jobs_collected = 0
        page = 1
        
        with tqdm(total=max_jobs, desc="Foundit Jobs") as pbar:
            while jobs_collected < max_jobs:
                retries = 3
                for attempt in range(retries):
                    try:
                        # Updated selectors for job cards
                        wait_selectors = [
                            "cardContainer",
                            "job-card",
                            "jobCard",
                            "div[class*='job']",
                            "article[class*='job']"
                        ]
                        
                        cards_found = False
                        for selector in wait_selectors:
                            try:
                                WebDriverWait(driver, 20).until(
                                    EC.presence_of_all_elements_located((By.CLASS_NAME, selector))
                                )
                                logging.info(f"Found job cards using selector: {selector}")
                                cards_found = True
                                break
                            except:
                                try:
                                    WebDriverWait(driver, 20).until(
                                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector))
                                    )
                                    logging.info(f"Found job cards using selector: {selector}")
                                    cards_found = True
                                    break
                                except:
                                    continue
                        
                        if not cards_found:
                            logging.error(f"Attempt {attempt + 1}/{retries}: No job cards found on Foundit page {page}")
                            if attempt == retries - 1:
                                screenshot_path = os.path.join(Data_dir, f"foundit_timeout_page_{page}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
                                driver.save_screenshot(screenshot_path)
                                logging.info(f"Saved screenshot: {screenshot_path}")
                                print(f"Saved screenshot: {screenshot_path}")
                                raise TimeoutException("No job cards found after retries")
                        
                        # Extended scrolling
                        for i in range(5):
                            driver.execute_script(f"window.scrollTo(0, {600 * (i + 1)});")
                            time.sleep(random.uniform(1.5, 2.5))
                        
                        time.sleep(random.uniform(4, 6))
                        
                        soup = BeautifulSoup(driver.page_source, 'html.parser')
                        
                        # Updated job card selectors
                        job_cards = soup.find_all('div', class_='cardContainer')
                        if not job_cards:
                            job_cards = soup.find_all('div', class_=lambda x: x and 'card' in str(x).lower() and 'job' in str(x).lower())
                        if not job_cards:
                            job_cards = soup.find_all('article', class_=lambda x: x and 'job' in str(x).lower())
                        if not job_cards:
                            job_cards = soup.find_all('div', class_=lambda x: x and 'job' in str(x).lower())
                        
                        logging.info(f"Found {len(job_cards)} job cards on Foundit page {page}")
                        print(f"Found {len(job_cards)} job cards on Foundit page {page}")
                        
                        if not job_cards:
                            logging.info("No more jobs found on Foundit")
                            print("No more jobs found on Foundit")
                            break
                        
                        for card in job_cards:
                            if jobs_collected >= max_jobs:
                                break
                            
                            try:
                                # Job title
                                title_elem = card.find('div', class_='jobTitle')
                                if not title_elem:
                                    title_elem = card.find('a', class_='jobTitle')
                                if not title_elem:
                                    title_elem = card.find('div', class_='infoSection')
                                if not title_elem:
                                    title_elem = card.find('h2')
                                if not title_elem:
                                    title_elem = card.find('h3')
                                if not title_elem:
                                    title_elem = card.find('a', class_=lambda x: x and 'title' in str(x).lower())
                                
                                job_title = title_elem.get_text(strip=True) if title_elem else ''
                                if not job_title or len(job_title) < 3:
                                    logging.warning(f"Skipping job card: No valid job title. Card HTML: {card.prettify()[:500]}")
                                    continue
                                
                                # Job link
                                job_link = ''
                                link_elem = card.find('a', class_='jobTitle')
                                if not link_elem:
                                    link_elem = card.find('a', class_=lambda x: x and 'title' in str(x).lower())
                                if not link_elem:
                                    link_elem = card.find('a', href=lambda x: x and '/job-listings/' in str(x))
                                if not link_elem:
                                    link_elem = card.find('a', href=lambda x: x and 'foundit.in' in str(x))
                                if not link_elem:
                                    link_elem = card.find('a', href=True)
                                
                                if link_elem and link_elem.get('href'):
                                    href = link_elem['href']
                                    if href.startswith('http'):
                                        job_link = href
                                    elif href.startswith('/'):
                                        job_link = f"https://www.foundit.in{href}"
                                    else:
                                        job_link = f"https://www.foundit.in/{href}"
                                
                                if not job_link:
                                    job_id = card.get('data-job-id') or card.get('id') or card.get('data-id')
                                    if job_id:
                                        clean_id = re.sub(r'\D', '', str(job_id))
                                        if clean_id:
                                            job_link = f"https://www.foundit.in/job/{clean_id}"
                                
                                # Company name
                                company_elem = card.find('div', class_='companyName')
                                if not company_elem:
                                    company_elem = card.find('span', class_='companyName')
                                if not company_elem:
                                    company_elem = card.find('a', class_=lambda x: x and 'company' in str(x).lower())
                                if not company_elem:
                                    company_elem = card.find('div', class_=lambda x: x and 'company' in str(x).lower())
                                company_name = company_elem.get_text(strip=True) if company_elem else 'Not specified'
                                
                                # Experience
                                card_body = card.find('div', class_='cardBody')
                                experience = '0'
                                if card_body:
                                    body_rows = card_body.find_all('div', class_='bodyRow')
                                    for row in body_rows:
                                        row_text = row.get_text(strip=True)
                                        if any(keyword in row_text.lower() for keyword in ['year', 'exp', 'experience']):
                                            experience = extract_experience_enhanced(row_text)
                                            if experience and experience != '0':
                                                break
                                
                                if not experience or experience == '0':
                                    card_text = card.get_text(strip=True)
                                    experience = extract_experience_enhanced(card_text)
                                
                                # Salary
                                salary = 'Not Disclosed'
                                if card_body:
                                    for row in card_body.find_all('div', class_='bodyRow'):
                                        row_text = row.get_text(strip=True)
                                        if any(keyword in row_text.lower() for keyword in ['₹', 'lakh', 'lpa']):
                                            if not any(keyword in row_text.lower() for keyword in ['description', 'responsibility', 'requirement']):
                                                salary = row_text
                                                break
                                
                                # Date posted
                                date_posted = 'Not specified'
                                card_footer = card.find('div', class_='cardFooter')
                                if card_footer:
                                    time_elem = card_footer.find('div', class_='jobAddedTime')
                                    if time_elem:
                                        time_text = time_elem.find('span', class_='timeText')
                                        if time_text:
                                            date_posted = clean_date_posted(time_text.get_text(strip=True))
                                        else:
                                            date_posted = clean_date_posted(time_elem.get_text(strip=True))
                                    else:
                                        for elem in card_footer.find_all(['span', 'div'], limit=5):
                                            elem_text = elem.get_text(strip=True)
                                            if any(keyword in elem_text.lower() for keyword in ['ago', 'day', 'week', 'month', 'posted']):
                                                date_posted = clean_date_posted(elem_text)
                                                break
                                
                                if job_title and filter_experience(experience):
                                    all_jobs.append({
                                        'job_title': job_title,
                                        'company_name': company_name,
                                        'job_link': job_link,
                                        'experience': experience,
                                        'salary': salary,
                                        'date_posted': date_posted
                                    })
                                    jobs_collected += 1
                                    pbar.update(1)
                                    logging.info(f"Collected job: {job_title} at {company_name} (Experience: {experience})")
                                else:
                                    logging.info(f"Skipped job '{job_title}' due to experience filter: {experience}. Card HTML: {card.prettify()[:500]}")
                            
                            except Exception as e:
                                logging.error(f"Error parsing Foundit job card: {e}. Card HTML: {card.prettify()[:500]}")
                                continue
                        
                        # Pagination
                        if jobs_collected < max_jobs and len(job_cards) > 0:
                            try:
                                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                                time.sleep(random.uniform(3, 5))
                                
                                next_button = None
                                next_selectors = [
                                    "//a[contains(@class, 'next')]",
                                    "//button[contains(text(), 'Next')]",
                                    "//a[contains(text(), 'Next')]",
                                    "//button[@aria-label='Next']",
                                    "//a[@aria-label='Next']",
                                    "//a[contains(@href, 'page=')]"
                                ]
                                
                                for selector in next_selectors:
                                    try:
                                        next_button = driver.find_element(By.XPATH, selector)
                                        if next_button.is_displayed() and next_button.is_enabled():
                                            break
                                        next_button = None
                                    except:
                                        continue
                                
                                if next_button:
                                    old_html = driver.page_source
                                    driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", next_button)
                                    time.sleep(random.uniform(2, 3))
                                    driver.execute_script("arguments[0].click();", next_button)
                                    WebDriverWait(driver, 20).until(
                                        lambda d: d.page_source != old_html
                                    )
                                    time.sleep(random.uniform(6, 8))
                                    page += 1
                                    logging.info(f"Moving to Foundit page {page}")
                                else:
                                    logging.info("No next button found on Foundit")
                                    print("No next button found on Foundit")
                                    break
                            except Exception as e:
                                logging.error(f"Pagination error on Foundit page {page}: {e}")
                                print(f"Pagination error on Foundit: {e}")
                                break
                        else:
                            break
                        
                        break  # Exit retry loop if successful
                    except TimeoutException:
                        logging.warning(f"Attempt {attempt + 1}/{retries}: Timeout waiting for job cards on Foundit page {page}")
                        if attempt < retries - 1:
                            time.sleep(random.uniform(5, 10))
                        continue
                        
                if not cards_found:
                    break  # Exit if no cards found after retries
                    
    except Exception as e:
        logging.error(f"Error scraping Foundit: {e}")
        print(f"Error scraping Foundit: {e}")
    finally:
        driver.quit()
        logging.info(f"Collected {len(all_jobs)} jobs from Foundit")
    
    return all_jobs

# --- Save Function ---
def save_to_csv(job_list, filename):
    """Save jobs to CSV file"""
    if not job_list:
        logging.warning("No jobs to save!")
        print("No jobs to save!")
        return
    
    keys = ['job_title', 'company_name', 'job_link', 'experience', 'salary', 'date_posted']
    
    with open(filename, 'w', newline='', encoding='utf-8') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(job_list)
    
    logging.info(f"Saved {len(job_list)} jobs to {filename}")
    print(f"\n✓ Saved {len(job_list)} jobs to {filename}")

# --- Main Execution ---
if __name__ == "__main__":
    print("=" * 60)
    print("MULTI-SITE JOB SCRAPER (TEST MODE: 50 JOBS PER SITE)")
    print("Scraping Software Engineer jobs (Experience >= 2 years)")
    print("=" * 60)
    
    all_jobs = []
    
    # Scrape from each site
    try:
        indeed_jobs = scrape_indeed(max_jobs=50)
        all_jobs.extend(indeed_jobs)
        print(f"✓ Collected {len(indeed_jobs)} jobs from Indeed")
    except Exception as e:
        logging.error(f"Failed to scrape Indeed: {e}")
        print(f"✗ Failed to scrape Indeed: {e}")
    
    time.sleep(random.uniform(3, 5))
    
    try:
        shine_jobs = scrape_shine(max_jobs=50)
        all_jobs.extend(shine_jobs)
        print(f"✓ Collected {len(shine_jobs)} jobs from Shine")
    except Exception as e:
        logging.error(f"Failed to scrape Shine: {e}")
        print(f"✗ Failed to scrape Shine: {e}")
    
    time.sleep(random.uniform(3, 5))
    
    try:
        foundit_jobs = scrape_foundit(max_jobs=50)
        all_jobs.extend(foundit_jobs)
        print(f"✓ Collected {len(foundit_jobs)} jobs from Foundit")
    except Exception as e:
        logging.error(f"Failed to scrape Foundit: {e}")
        print(f"✗ Failed to scrape Foundit: {e}")
    
    time.sleep(random.uniform(3, 5))
    
    try:
        glassdoor_jobs = scrape_glassdoor(max_jobs=50)
        all_jobs.extend(glassdoor_jobs)
        print(f"✓ Collected {len(glassdoor_jobs)} jobs from Glassdoor")
    except Exception as e:
        logging.error(f"Failed to scrape Glassdoor: {e}")
        print(f"✗ Failed to scrape Glassdoor: {e}")
    
    # Save all jobs
    if all_jobs:
        output_filename = os.path.join(Data_dir, f"jobs_raw_ml_extracted.csv")
        save_to_csv(all_jobs, output_filename)
        
        print("\n" + "=" * 60)
        print(f"TEST SCRAPING COMPLETE!")
        print(f"Total jobs collected: {len(all_jobs)}")
        print(f"Output file: {output_filename}")
        print("=" * 60)
    else:
        logging.warning("No jobs were collected from any site!")
        print("\n✗ No jobs were collected from any site!")