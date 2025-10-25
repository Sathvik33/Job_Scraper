import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium_stealth import stealth
import csv
import time
import logging
import random
import re

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Keywords
KEYWORDS = [
    "Software Developer", "Java Developer", "Frontend Engineer",
    # Add more keywords as needed
]

# CSV setup
Base_dir = os.path.dirname(os.path.abspath(__file__))
Data_dir = os.path.join(Base_dir, 'Data')
os.makedirs(Data_dir, exist_ok=True)
csv_path = os.path.join(Data_dir, "linkedin_jobs.csv")

csv_file = open(csv_path, "w", newline="", encoding="utf-8")
writer = csv.writer(csv_file)
writer.writerow([
    "Keyword", "Job Title", "Company", "Location", "Posted Date",
    "Experience Level", "Salary", "Job Link", "Hiring Manager", "Work Type"
])
seen_links = set()

# Create stealth driver
def create_stealth_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
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

# Function to close pop-ups
def close_popup(driver):
    logging.info("Checking for pop-ups")
    close_selectors = [
        "button[aria-label='Dismiss']",
        "button[aria-label='Sign in modal dismiss']",  # Login pop-up
        "button[data-tracking-control-name='overlay.close']",
        "button[data-tracking-control-name='ga-cookie.consent.deny.v1']",
        "button[class*='modal__dismiss']",
        "button[class*='close']",
        "button[title='Close']",
        "button[aria-label*='Close']",
        "button[data-test-id='welcome-message-close-button']",
        "button[class*='sign-in-modal__dismiss']",
        "button[class*='artdeco-modal__dismiss']",
        "div[class*='close'] button",
        "button[data-tracking-control-name*='guest_homepage_basic_guest-homepage-jobs-seo_sign-in-modal_dismiss']"
    ]
    for selector in close_selectors:
        try:
            close_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
            )
            driver.execute_script("arguments[0].click();", close_button)
            logging.info(f"Closed pop-up with selector: {selector}")
            time.sleep(1)
            return True
        except:
            continue
    logging.info("No pop-up found or unable to close")
    return False

# Function to retry finding job cards
def get_job_cards(driver):
    for attempt in range(3):
        try:
            job_cards = WebDriverWait(driver, 15).until(
                EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, "ul.jobs-search__results-list li")
                )
            )
            logging.info(f"Found {len(job_cards)} job cards on attempt {attempt + 1}")
            return job_cards
        except:
            logging.warning(f"Attempt {attempt + 1} failed to find job cards. Retrying...")
            close_popup(driver)
            time.sleep(3)
    logging.error("Failed to find job cards after retries")
    return []

# Function to extract and normalize experience
def extract_experience_enhanced(text):
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

# Function to filter jobs with >= 2 years experience
def filter_experience(exp_text):
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

# Main scraping function
def scrape_linkedin():
    logging.info("Starting LinkedIn scraping")
    driver = create_stealth_driver()
    all_jobs = 0
    
    try:
        for role in KEYWORDS:
            logging.info(f"Searching contract + remote jobs for: {role}")
            query = role.replace(' ', '%20')
            url = (
                f"https://www.linkedin.com/jobs/search/?keywords={query}"
                "&location=Worldwide"
                "&f_E=2,3,4,5,6"
                "&f_JT=C"
                "&f_WT=2"
            )
            driver.get(url)
            logging.info("Waiting for page to load")
            time.sleep(random.uniform(5, 8))
            
            # Close pop-ups
            close_popup(driver)
            
            page = 1
            max_pages = 25
            role_jobs_count = 0
            
            while page <= max_pages:
                logging.info(f"Scraping page {page} for {role} (Contract + Remote)")
                
                # Scroll to load content
                for i in range(3):
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    logging.info(f"Scrolling {i+1}/3")
                    time.sleep(random.uniform(1.5, 2.5))
                
                # Get job cards
                job_cards = get_job_cards(driver)
                if not job_cards:
                    logging.error(f"No job cards found on page {page} for {role}")
                    break
                
                jobs_found_on_page = 0
                remote_jobs_found = 0
                
                for card in job_cards:
                    # Job title
                    try:
                        title = card.find_element(By.CSS_SELECTOR, "h3.base-search-card__title").text.strip()
                    except:
                        title = "N/A"
                    
                    # Company
                    try:
                        company = card.find_element(By.CSS_SELECTOR, "h4.base-search-card__subtitle a").text.strip()
                    except:
                        company = "N/A"
                    
                    # Location
                    try:
                        location = card.find_element(By.CSS_SELECTOR, "span.job-search-card__location").text.strip()
                        is_remote = "remote" in location.lower()
                    except:
                        location = "N/A"
                        is_remote = False
                    
                    # Skip non-remote jobs
                    if not is_remote:
                        continue
                    
                    remote_jobs_found += 1
                    jobs_found_on_page += 1
                    
                    # Posted date
                    try:
                        posted_date = card.find_element(By.CSS_SELECTOR, "time.job-search-card__listdate").get_attribute("datetime").strip()
                    except:
                        posted_date = "N/A"
                    
                    # Job link
                    try:
                        job_link = card.find_element(By.CSS_SELECTOR, "a.base-card__full-link").get_attribute("href")
                    except:
                        job_link = "N/A"
                    
                    # Skip duplicates
                    if job_link in seen_links or job_link == "N/A":
                        continue
                    seen_links.add(job_link)
                    
                    # Job details
                    experience = "N/A"
                    salary = "N/A"
                    hiring_details = "N/A"
                    work_type = "Contract + Remote"
                    
                    try:
                        # Open job details page
                        driver.execute_script("window.open(arguments[0]);", job_link)
                        driver.switch_to.window(driver.window_handles[1])
                        time.sleep(random.uniform(1.5, 2.5))
                        
                        # Close pop-ups on job details page
                        close_popup(driver)
                        
                        # Experience level
                        try:
                            job_details = driver.find_element(By.CSS_SELECTOR, "div.description__text").text.strip()
                            experience = extract_experience_enhanced(job_details)
                            if not filter_experience(experience):
                                driver.close()
                                driver.switch_to.window(driver.window_handles[0])
                                continue
                        except:
                            experience = "N/A"
                        
                        # Salary
                        try:
                            salary = driver.find_element(By.CSS_SELECTOR, "div.salary, div.compensation__text").text.strip()
                        except:
                            salary = "N/A"
                        
                        # Hiring Manager
                        try:
                            hiring_section = driver.find_element(By.CSS_SELECTOR, "div.job-details-people-who-can-help__section")
                            try:
                                hiring_name = hiring_section.find_element(By.CSS_SELECTOR, "span.jobs-poster__name").text.strip()
                            except:
                                hiring_name = "N/A"
                            try:
                                hiring_role = hiring_section.find_element(By.CSS_SELECTOR, "div.text-body-small").text.strip()
                            except:
                                hiring_role = "N/A"
                            try:
                                profile_link = hiring_section.find_element(By.CSS_SELECTOR, "a[data-test-app-aware-link]").get_attribute("href")
                            except:
                                profile_link = "N/A"
                            hiring_details = f"{hiring_name} | {hiring_role} | {profile_link}"
                        except:
                            hiring_details = "N/A"
                        
                        driver.close()
                        driver.switch_to.window(driver.window_handles[0])
                    except:
                        driver.close()
                        driver.switch_to.window(driver.window_handles[0])
                    
                    writer.writerow([role, title, company, location, posted_date, experience, salary, job_link, hiring_details, work_type])
                    logging.info(f"Found: {title} at {company} ({location})")
                    role_jobs_count += 1
                    all_jobs += 1
                
                logging.info(f"Found {remote_jobs_found} remote contract jobs on page {page}")
                
                # Pagination
                try:
                    next_button = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable(
                            (By.CSS_SELECTOR, f"button[aria-label='Page {page + 1}'], button[aria-label*='Next']")
                        )
                    )
                    driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                    time.sleep(1)
                    driver.execute_script("arguments[0].click();", next_button)
                    page += 1
                    time.sleep(random.uniform(3, 5))
                    close_popup(driver)
                except:
                    logging.info(f"No more pages found. Total remote contract jobs for {role}: {role_jobs_count}")
                    break
                
                time.sleep(random.uniform(5, 8))
            
            logging.info(f"Finished {role}: {role_jobs_count} jobs")
        
    except Exception as e:
        logging.error(f"Error scraping LinkedIn: {e}")
    finally:
        driver.quit()
        logging.info(f"Scraping complete. Total jobs: {all_jobs}. Data saved in {csv_path}")

# Main execution
if __name__ == "__main__":
    scrape_linkedin()
    csv_file.close()