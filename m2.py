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
from selenium.webdriver.common.action_chains import ActionChains

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

# --- Job Roles ---
job_roles = [
    "Software Developer", "Contract DevOps Engineer", "Platform Engineer", "Python Developer", 
    "Cloud Network Engineer", "Senior Fullstack Engineer", "Data Conversion Team Administrator", 
    "Lead- DOT NET developer", "Content Operations Engineer", "C++ Technical Architect", 
    "Senior SAP Project Manager", "Embedded C++ – Freelance – Consultants-Trainer", 
    "Senior Cloud Engineer", "Dot Net Engineer", "Senior C# Developer", "Senior Android Developer", 
    "Backend Software Engineer", "Senior Independent Software Developer", "Data Engineer", 
]

# --- Global tracking ---
seen_job_links = set()

# --- Stealth Selenium Setup ---
def create_stealth_driver():
    """Create a selenium driver with stealth settings"""
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
    
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

# --- Helper Functions ---
def clean_date_posted(date_text):
    """Clean and standardize date posted text"""
    if not date_text:
        return "Not specified"
    
    date_text = date_text.strip()
    date_text = re.sub(r'^(posted|active|employer\s+)?:?\s*', '', date_text, flags=re.IGNORECASE)
    
    patterns = [
        r'(\d+\s+(?:day|days|hour|hours|week|weeks|month|months)\s+ago)',
        r'(today|yesterday)',
        r'(\d+[dD])',
        r'(\d+[hH])',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, date_text, re.IGNORECASE)
        if match:
            return match.group(1)
    
    if re.match(r'^\d+$', date_text):
        return f"{date_text} days ago"
    
    return "Not specified"

def extract_experience_enhanced(text):
    """Enhanced experience extraction with comprehensive patterns"""
    if not text:
        return "0"
    
    text_lower = text.lower()
    
    patterns = [
        (r'(\d+)\s*[-–—to]+\s*(\d+)\s*(?:years?|yrs?|y)\s*(?:of)?\s*(?:experience|exp)?', 
         lambda m: f"{m.group(1)}-{m.group(2)}"),
        (r'(\d+)\s*\+\s*(?:years?|yrs?|y)\s*(?:of)?\s*(?:experience|exp)?', 
         lambda m: f"{m.group(1)}+"),
        (r'(?:min|minimum|at least|atleast)\s+(\d+)\s*(?:years?|yrs?|y)', 
         lambda m: f"{m.group(1)}+"),
        (r'(\d+)\s*(?:years?|yrs?|y)(?:\s+(?:of)?\s*(?:experience|exp))?', 
         lambda m: m.group(1)),
        (r'\b(senior|lead|principal|sr\.)\b', 
         lambda m: "5+"),
        (r'\b(mid[-]?level|mid[-]?senior)\b', 
         lambda m: "3-5"),
        (r'\b(junior|jr\.|entry[-]?level)\b', 
         lambda m: "0-2"),
        (r'\b(fresher|fresh|graduate|recent graduate|no experience)\b', 
         lambda m: "0"),
    ]
    
    for pattern, handler in patterns:
        matches = re.finditer(pattern, text_lower)
        for match in matches:
            try:
                result = handler(match)
                if result:
                    return result
            except:
                continue
    
    number_matches = re.findall(r'\b(\d+)\s*(?=years?|yrs?|y\b)', text_lower)
    if number_matches:
        return number_matches[0]
    
    return "0"

def filter_experience(exp_text):
    """Keep only jobs that clearly mention >= 2 years of experience"""
    if not exp_text:
        return False

    exp_text = exp_text.lower().strip()

    if any(word in exp_text for word in ['fresher', 'intern', 'trainee', 'graduate', 'entry']):
        return False

    range_match = re.search(r'(\d+)\s*[-–—to]+\s*(\d+)', exp_text)
    if range_match:
        try:
            min_exp = int(range_match.group(1))
            return min_exp >= 2
        except ValueError:
            return False

    num_match = re.search(r'(\d+)\s*(?:\+)?\s*(?:years?|yrs?|y)\b', exp_text)
    if num_match:
        try:
            exp = int(num_match.group(1))
            return exp >= 2
        except ValueError:
            return False

    if any(word in exp_text for word in ['senior', 'lead', 'principal', 'mid']):
        return True

    return False

def is_duplicate_job(job_link):
    """Check if job link has already been processed"""
    if not job_link:
        return True
    
    normalized_link = job_link.strip().lower()
    if not normalized_link:
        return True
    
    if normalized_link in seen_job_links:
        return True
    
    seen_job_links.add(normalized_link)
    return False

def handle_captcha(driver, max_attempts=3):
    """Detect and handle CAPTCHA"""
    for attempt in range(max_attempts):
        try:
            captcha_elements = driver.find_elements(By.CSS_SELECTOR, ".cf-turnstile, iframe[src*='recaptcha'], div[class*='g-recaptcha']")
            if not captcha_elements:
                return True

            logging.warning(f"CAPTCHA detected on attempt {attempt + 1}")
            screenshot_path = os.path.join(Data_dir, f"captcha_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
            driver.save_screenshot(screenshot_path)
            
            time.sleep(random.uniform(2, 4))
            
            try:
                checkbox = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "input[type='checkbox'][id*='recaptcha'], div[class*='recaptcha-checkbox']"))
                )
                ActionChains(driver).move_to_element(checkbox).click().perform()
                time.sleep(4)
                return True
            except:
                print(f"Manual CAPTCHA intervention needed. Screenshot: {screenshot_path}")
                input("Solve CAPTCHA manually and press Enter...")
                return True

        except Exception as e:
            logging.error(f"Error handling CAPTCHA: {e}")
            if attempt == max_attempts - 1:
                return False
            time.sleep(10)

    return False

def extract_all_jobs_single_page(driver, role):
    """
    Extract ALL jobs from the current page in a single pass
    Shine loads all results on one page - no pagination needed
    """
    print(f"\n🔄 Extracting all jobs from single page...")
    
    # First, scroll to bottom to ensure all content is loaded
    print("  📜 Scrolling to load all content...")
    last_height = driver.execute_script("return document.body.scrollHeight")
    
    for scroll_attempt in range(5):  # Max 5 scrolls to load everything
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
    
    # Scroll back to top
    driver.execute_script("window.scrollTo(0, 0);")
    time.sleep(1)
    
    # Now parse ALL jobs at once
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    
    job_cards = (
        soup.find_all('div', {'class': lambda x: x and 'jobCard' in str(x)}) or
        soup.find_all('li', {'class': lambda x: x and 'job' in str(x).lower()}) or
        soup.find_all('article') or
        []
    )
    
    print(f"  📊 Found {len(job_cards)} total job cards")
    
    all_jobs_data = []
    filtered_out = 0
    duplicates = 0
    errors = 0
    
    for idx, card in enumerate(job_cards, 1):
        try:
            # Extract job title
            title_elem = (
                card.find('h2') or card.find('h3') or 
                card.find('a', {'class': lambda x: x and 'title' in str(x).lower()})
            )
            job_title = title_elem.get_text(strip=True) if title_elem else ''
            
            if not job_title or len(job_title) < 3:
                continue
            
            # Extract job link
            link_elem = card.find('a', href=True)
            job_link = ''
            if link_elem:
                href = link_elem.get('href', '')
                if href.startswith('/'):
                    job_link = f"https://www.shine.com{href}"
                elif href.startswith('http'):
                    job_link = href
            
            # Skip duplicates
            if is_duplicate_job(job_link):
                duplicates += 1
                continue
            
            # Extract company
            company_name = 'Not specified'
            company_elem = card.find(['div', 'span', 'a'], {'class': lambda x: x and 'company' in str(x).lower()})
            if company_elem:
                company_name = company_elem.get_text(strip=True)
            
            # Extract salary
            salary = 'Not Disclosed'
            salary_elem = card.find('span', class_=lambda x: x and 'salary' in str(x).lower())
            if salary_elem:
                salary = salary_elem.get_text(strip=True)
            
            # Extract date
            date_posted = 'Not specified'
            date_elem = card.find('span', class_=lambda x: x and 'date' in str(x).lower())
            if date_elem:
                date_posted = clean_date_posted(date_elem.get_text(strip=True))
            
            # Extract experience
            experience = extract_experience_enhanced(card.get_text(strip=True))
            
            # Filter and save
            if filter_experience(experience):
                job_data = {
                    'job_title': job_title,
                    'company_name': company_name,
                    'job_link': job_link,
                    'experience': experience,
                    'salary': salary,
                    'date_posted': date_posted
                }
                
                all_jobs_data.append(job_data)
            else:
                filtered_out += 1
            
        except Exception as e:
            errors += 1
            logging.error(f"Error parsing job card {idx}: {e}")
            continue
    
    print(f"\n  ✅ Extracted: {len(all_jobs_data)} jobs")
    print(f"  🚫 Filtered out (< 2 years exp): {filtered_out}")
    print(f"  🔄 Duplicates skipped: {duplicates}")
    if errors > 0:
        print(f"  ⚠️  Errors: {errors}")
    
    return all_jobs_data

# --- Shine Scraper ---
def scrape_shine():
    """Scrape Shine with scroll-based loading"""
    logging.info("Starting Shine scraping with scroll-based loading")
    print("\n=== Starting Shine Scraping ===")
    all_jobs = []
    driver = create_stealth_driver()
    
    try:
        for role_idx, role in enumerate(job_roles):
            print(f"\n{'='*60}")
            print(f"🎯 Scraping role {role_idx + 1}/{len(job_roles)}: {role}")
            print(f"{'='*60}")
            
            query = requests.utils.quote(role.replace('/', '-').replace(':', ''))
            base_url = f"https://www.shine.com/job-search/{query}-jobs"
            
            driver.get(base_url)
            time.sleep(random.uniform(8, 12))
            
            # Handle pop-ups
            try:
                close_selectors = [
                    "button[aria-label='Close']", ".closeBtn", ".modal-close",
                    "button[id*='reject']", "button[id*='accept']"
                ]
                for selector in close_selectors:
                    try:
                        close_btn = WebDriverWait(driver, 5).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                        )
                        driver.execute_script("arguments[0].click();", close_btn)
                        time.sleep(2)
                        break
                    except:
                        continue
            except:
                pass
            
            if not handle_captcha(driver):
                print(f"❌ CAPTCHA issue for {role}. Skipping.")
                continue
            
            # Wait for initial content
            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='jobCard'], li[class*='job'], article"))
                )
            except TimeoutException:
                print(f"⚠️ No jobs found for {role}")
                continue
            
            # Scroll and load all jobs
            role_jobs = extract_all_jobs_single_page(driver, role)
            all_jobs.extend(role_jobs)
            
            print(f"\n✅ Finished {role}: {len(role_jobs)} jobs collected")
            time.sleep(random.uniform(10, 15))
            
    except Exception as e:
        logging.error(f"Error scraping Shine: {e}")
        print(f"❌ Error scraping Shine: {e}")
    finally:
        driver.quit()
        print(f"\n🎉 Total collected: {len(all_jobs)} unique jobs from Shine")
    
    return all_jobs

# --- Foundit Scraper (keeping original logic for now) ---
def scrape_foundit():
    """Scrape Foundit - placeholder for now"""
    logging.info("Foundit scraping skipped in this version")
    print("\n⚠️ Foundit scraping not included in this version")
    return []

# --- Save to CSV ---
def save_to_csv(jobs, filename):
    """Save jobs to CSV file"""
    if not jobs:
        print(f"⚠️ No jobs to save for {filename}")
        return
    
    filepath = os.path.join(Data_dir, filename)
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['job_title', 'company_name', 'job_link', 'experience', 'salary', 'date_posted']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(jobs)
        print(f"✅ Saved {len(jobs)} jobs to {filepath}")
        logging.info(f"Saved {len(jobs)} jobs to {filepath}")
    except Exception as e:
        logging.error(f"Error saving to CSV: {e}")
        print(f"❌ Error saving to CSV: {e}")

# --- Main Execution ---
def main():
    """Main function"""
    global seen_job_links
    seen_job_links = set()
    
    print("\n" + "="*60)
    print("🚀 JOB SCRAPER WITH SCROLL-BASED LOADING")
    print("="*60)
    
    start_time = time.time()
    
    shine_jobs = scrape_shine()
    save_to_csv(shine_jobs, f'shine_jobs_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"\n" + "="*60)
    print(f"🎉 Scraping Completed!")
    print(f"⏱️  Duration: {duration:.2f} seconds")
    print(f"📊 Total jobs: {len(shine_jobs)}")
    print(f"💾 Saved to: {Data_dir}")
    print("="*60)
    
    logging.info(f"Scraping completed. Total: {len(shine_jobs)} jobs, Time: {duration:.2f}s")

if __name__ == "__main__":
    main()