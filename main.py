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
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
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
    "Software Developer",
    "Contract DevOps Engineer",
    "Platform Engineer",
    "Python Developer",
    "Cloud Network Engineer",
    "Senior Fullstack Engineer",
    "Data Conversion Team Administrator",
    "Lead DOT NET Developer",
    "Content Operations Engineer",
    "C++ Technical Architect",
    "Senior SAP Project Manager",
    "Embedded C++ Consultant",
    "Senior Cloud Engineer",
    "Dot Net Engineer",
    "Senior C# Developer",
    "Senior Android Developer",
    "Backend Software Engineer",
    "Senior Independent Software Developer",
    "Data Engineer",
    "JDE Developer",
    "Senior AI/ML Engineer",
    "AWS BI Architect",
    "AI Data Engineer",
    "Sr. Denodo Admin",
    "OIC PaaS Developer",
    "ProC Developer",
    "Composable Commerce Architect",
    "Senior Windchill Developer",
    "Databricks Data Engineer",
    "Magic XPA Subject Matter Expert",
    "Node.js Developer (Backend)",
    "ServiceNow Developer",
    "Sr. Contact Center Engineer",
    ".NET Developer",
    "Senior Tableau Developer",
    "IT Infrastructure/Cloud Engineer",
    "Cloud Technology Consultant",
    "SAP Global Trade Services Consultant",
    "Cyber Security Engineer",
    "SDE I - Android Developer",
    "Senior Consultant - Full Stack Developer",
    "Senior Mainframe Security Analyst",
    "DevOps Engineer III",
    "BRM Developer",
    "React.js + Electron.js Developer",
    "Oracle Integration Cloud (OIC) Developer",
    "Sr. Software Engineer",
    "MicroStrategy Architect / Developer",
    "AKS Cloud Engineer",
    "Java Full Stack Lead",
    "Senior MEAN Full Stack Developer",
    "Senior Full Stack Software Developer",
    "EDI Developer",
    "Sr. Salesforce Engineer",
    "Level 2 Managed Systems Engineer",
    "Software Engineer (C/C++/Go)",
    "ETL Integration Engineer",
    "Salesforce Copado DevOps Engineer",
    "Software Engineer - SQL Databases",
    "Cloud Engineer",
    "WebFOCUS Developer",
    "Golang Developer",
    "Senior Java Software Engineer",
    "Technical Support Engineer",
    "Senior Salesforce Developer",
    "CCAI BOT Developer",
    "Full Stack Development Engineer",
    "Senior Node.js Engineer",
    "IT Helpdesk Specialist",
    "Integrations Engineer",
    "Full Stack .NET Developer",
    "Salesforce CPQ Developer",
    "HubSpot Developer",
    "AEM Developer",
    "Python ML Developer",
    "Capacity Planning Engineer",
    "Backend Developer",
    "Sr. CRM Developer",
    "UI/UX Lead",
    "Node Full Stack Developer",
    "DevOps Architect",
    "Cognos Developer",
    "SAP Developer",
    "Technical Curriculum Developer",
    "Senior Salesforce Commerce Cloud Developer",
    "Zoho Specialist",
    "BASE24/UPF Engineer",
    "Python Fullstack Developer",
    "Azure Apps Engineer",
    "Statistical Programmer",
    "JavaScript Developer",
    "Structural Engineer",
    "Senior Angular Developer",
    "Microsoft Dynamics AX Technical Lead",
    "IDMC Architect",
    "SAP SuccessFactors Project Manager",
    "Frontend Engineer",
    "Technical Consultant",
    "Senior Software Designer",
    "PHP Laravel Developer",
    "Tech Lead",
    "Third Party Contractor",
    "Backend Developer",
    "BI Developer",
    "Oracle Cloud Payroll Consultant",
    "EBX Architect / Senior Developer for MDM",
    "UI/UX Developer",
    "Senior Developer",
    "iOS Engineer",
    "Senior AdTech Data Solution Architect",
    "AWS Cloud Engineer",
    "Global Infrastructure Engineer",
    "UI Automation Tester",
    "Senior Frontend Engineer",
    "Mirth Connect Developer",
    "Java Spring Boot Developer",
    "Full Stack PHP Developer",
    "Platform Engineer Contractor",
    "Senior React Native Developer",
    "Guidewire Developer",
    "Full Stack Engineer",
    "SDET",
    "UX Software Engineer",
    "SAP ABAP Developer",
    "IBM TRIRIGA Developer",
    "Full Stack Developer (.NET/Vue)",
    "Senior AEM Developer",
    "Senior MicroStrategy Developer",
    "NGP Fullstack Developer",
    "Spark/Scala Developer",
    "Java Backend Developer",
    "SAP Hybris Developer",
    "Senior iOS Developer",
    "Scala QA Engineer",
    "Artificial Intelligence Engineer",
    "Power BI Developer",
    "Data Science Engineer",
    "AI Developer",
    "React Native Developer",
    "Senior Full-Stack Java Developer",
    "QA Automation Engineer",
    "Technical Content Developer",
    "MuleSoft Developer",
    "Java Developer",
    "Software Test Engineer",
    "React/PHP Developer",
    "Senior Software QA Engineer",
    "Fullstack Developer (Java/React)",
    "Senior Data Engineer",
    "Java Spring MVC Developer",
    "Compliance Analyst - Contract",
    "AWS DevOps Engineer",
    "Jaspersoft Developer",
    "AI Automation Engineer",
    "Low-Code / No-Code Developer",
    "UI/UX Designer",
    "SAP Datasphere Engineer",
    "SAP AFS Consultant",
    "Informatica IDMC Spark Engineer",
    "MS Word/VBA Specialist",
    "SQL Expert (AI Initiative)",
    "Web Producer",
    "Cloud/AI Specialist",
    "QA Specialist",
    "Ingenium Architect / Developer",
    "BI Visualization Developer",
    "Senior Automation Lead",
    "Support Engineer",
    "Profisee MDM Developer",
    "Data Architect",
    ".NET Architect",
    "Alfabet Specialist",
    "Evaluation Specialist",
    "Java Full Stack Developer",
    "Senior Test Engineer",
    "SAS Solution Designer",
    "Developer Engagement Representative",
    "Technical Customer Support Engineer",
    "Supply Chain Analyst - SAP S/4Hana",
    "Technical Product Manager",
    "Denodo Architect",
    "CCaaS NICE Implementation Manager",
    "IFS Cloud Developer",
    "Drupal Frontend Developer",
    "Software Architect - PHP",
    "ServiceNow Platform Architect",
    "Chief Engineer",
    "Blockchain Developer",
    "Adobe Migration Developer",
    "Certinia Developer",
    "SAP Consultant",
    "QA Lead",
    "SAP APO Consultant",
    "Rust Engineer (CDK)",
    "MS Dynamics 365 Developer",
    "Localization Specialist",
    "Google CCAI BOT Developer",
    "Angular Developer",
    "Python FastAPI Developer",
    "Salesforce Service Cloud Developer",
    "Network Engineer",
    "Oracle EBS SCM Consultant",
    "AI Quality Control Engineer",
    "CIAM Engineer",
    "Techno-Functional Consultant",
    "Database Administrator",
    "MSD365 F&O Consultant",
    "Documentum AWS Consultant",
    "Securities and Commodities Specialist",
    "Power BI Specialist",
    "Security Analyst (L2)",
    "Freelance ServiceNow Developer",
    "Oracle Banking Product Developer",
    "Head of AI and SLM",
    "Software Engineer III",
    "Siebel CRM Developer",
    "Website Development Engineer",
    "Data & AI Consultant",
    "Customer Success Engineer",
    "Software Development Engineer 2 (SDE 2)",
    "Salesforce Data Cloud Engineer",
    "Informatica MDM CAI Developer",
    "Technical Architect",
    "PTC FlexPLM Consultant",
    "Golang Developer (Intermediate)",
    "QA Engineer Lead",
    "Manual QA Specialist",
    "Informatica Customer 360 Expert",
    "Freelance Developer",
    "Azure Infrastructure Engineer",
    "Salesforce QA Engineer",
    "Workday Integrations Consultant",
    "Azure Security Architect",
    "SAP BI/BW Consultant",
    "SAP PP Consultant",
    "DevOps Engineer (GCP)",
    "Solutions Architect",
    "People Services Specialist",
    "eLearning Project Manager",
    "Principal Consultant - Data Engineering",
    "Senior .NET Developer",
    "Pharma Business Analyst",
    "Sourcing Consultant",
    "Mechanical Design Engineer",
    "SAP IS-U Consultant",
    "SAP Ariba Consultant",
    "DBA (HANA)",
    "Azure Data Engineer",
    "Biostatistician",
    "C Developer",
    "Site Reliability Engineer",
    "BPC Backend Consultant",
    "AWS Engineer"
]


seen_job_links = set()

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

# --- Helper Functions ---
def find_and_click_next_button(driver, role, page):
    """Click numbered pagination links (2 through 10) for Shine"""
    if page >= 10:
        logging.info(f"Reached maximum page limit (10) for {role}")
        return False
    
    # Target numbered pagination links
    page_selectors = [
        f"//a[contains(@class, 'pagination') and text()='{page + 1}']",  # Direct page number (e.g., "2")
        f"//a[contains(@class, 'cls_pagination') and text()='{page + 1}']",  # Shine-specific class
        f"//a[contains(@href, 'page={page + 1}')]",  # Links with page number in href
        f"//li[contains(@class, 'pagination') or contains(@class, 'cls_pagination')]//a[text()='{page + 1}']",
        f"//div[contains(@class, 'pagination')]//a[text()='{page + 1}']",
    ]
    
    try:
        # Scroll to bottom to ensure pagination elements are loaded
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(random.uniform(1.5, 2.5))
        
        for selector in page_selectors:
            try:
                # Wait for the page number link to be present
                page_link = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, selector))
                )
                logging.info(f"Found page {page + 1} link with selector: {selector}")
                
                # Check if link is clickable
                if page_link.is_displayed() and page_link.is_enabled():
                    # Get current state to detect page change
                    old_url = driver.current_url
                    old_page_source = driver.page_source[:1000]
                    
                    # Scroll to link and click
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", page_link)
                    time.sleep(random.uniform(0.5, 1))
                    driver.execute_script("arguments[0].click();", page_link)
                    
                    # Wait for page change
                    WebDriverWait(driver, 15).until(
                        lambda d: d.current_url != old_url or d.page_source[:1000] != old_page_source
                    )
                    logging.info(f"Successfully clicked page {page + 1} link for {role}")
                    return True
                else:
                    logging.warning(f"Page {page + 1} link found but not clickable: {selector}")
            
            except (TimeoutException, NoSuchElementException, StaleElementReferenceException) as e:
                logging.debug(f"Selector {selector} for page {page + 1} failed: {str(e)}")
                continue
        
        # Fallback: Try constructing pagination URL
        logging.info(f"No clickable page {page + 1} link found for {role}. Attempting URL-based pagination.")
        current_url = driver.current_url
        next_page = page + 1
        if 'page=' in current_url:
            next_url = re.sub(r'page=\d+', f'page={next_page}', current_url)
        else:
            separator = '&' if '?' in current_url else '?'
            next_url = f"{current_url}{separator}page={next_page}"
        
        try:
            driver.get(next_url)
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='jobCard'], li[class*='job'], article, [data-job-id]"))
            )
            logging.info(f"Successfully navigated to page {next_page} for {role} using URL: {next_url}")
            return True
        except Exception as e:
            logging.warning(f"URL-based pagination failed for {next_url}: {e}")
        
        # Fallback: Try infinite scroll
        logging.info(f"No page {page + 1} link or URL worked for {role}. Attempting infinite scroll.")
        scroll_height = driver.execute_script("return document.body.scrollHeight")
        driver.execute_script(f"window.scrollTo(0, {scroll_height});")
        time.sleep(random.uniform(2, 4))
        new_scroll_height = driver.execute_script("return document.body.scrollHeight")
        if new_scroll_height > scroll_height:
            logging.info(f"Infinite scroll triggered new content for {role} on page {page}")
            return True
        
        logging.info(f"No page {page + 1} found for {role}")
        return False
    
    except Exception as e:
        logging.error(f"Error in find_and_click_next_button for {role} on page {page}: {e}")
        return False
    
    
def click_numbered_page(driver, role, target_page):
    """Use direct URL pattern for pagination - this is the most reliable method"""
    try:
        current_url = driver.current_url
        logging.info(f"Current URL: {current_url}")
        
        # Clean the current URL first - remove any existing page parameters
        clean_url = re.sub(r'[?&]page=\d+', '', current_url)
        clean_url = re.sub(r'-jobs-\d+', '', clean_url)
        clean_url = re.sub(r'/jobs/\d+', '', clean_url)
        
        # Remove double ? or & characters
        clean_url = re.sub(r'\?&', '?', clean_url)
        clean_url = re.sub(r'&&', '&', clean_url)
        clean_url = clean_url.rstrip('?&')
        
        # Construct the new URL with the target page
        # Try multiple URL patterns that Shine.com uses
        url_patterns = [
            f"{clean_url}-jobs-{target_page}",
            f"{clean_url}/jobs/{target_page}",
            f"{clean_url}?page={target_page}",
            f"{clean_url}&page={target_page}"
        ]
        
        for new_url in url_patterns:
            try:
                logging.info(f"Trying URL pattern: {new_url}")
                driver.get(new_url)
                
                # Wait for page to load completely
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "body"))
                )
                
                # Wait for job cards specifically
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='jobCard'], li[class*='job'], article, [data-job-id]"))
                )
                
                # Verify we're on a different page by checking URL
                current_url_after = driver.current_url
                logging.info(f"URL after navigation: {current_url_after}")
                
                # Check if page number appears in the new URL
                if (str(target_page) in current_url_after or 
                    f'page={target_page}' in current_url_after or
                    f'-jobs-{target_page}' in current_url_after):
                    logging.info(f"✓ Successfully navigated to page {target_page}")
                    time.sleep(random.uniform(3, 5))
                    return True
                else:
                    # Even if URL doesn't show page number, check if content changed
                    logging.info(f"Page number not in URL, but navigation completed")
                    time.sleep(random.uniform(3, 5))
                    return True
                    
            except Exception as e:
                logging.warning(f"URL pattern failed: {new_url} - {e}")
                continue
        
        logging.error(f"All URL patterns failed for page {target_page}")
        return False
        
    except Exception as e:
        logging.error(f"URL pagination failed for page {target_page}: {e}")
        return False
    

def handle_captcha(driver, max_attempts=3):
    """Detect and handle CAPTCHA with improved logic"""
    for attempt in range(max_attempts):
        try:
            captcha_elements = driver.find_elements(By.CSS_SELECTOR, ".cf-turnstile, iframe[src*='recaptcha'], div[class*='g-recaptcha']")
            if not captcha_elements:
                logging.info("No CAPTCHA detected.")
                return True

            logging.info(f"CAPTCHA detected on attempt {attempt + 1}")
            screenshot_path = os.path.join(Data_dir, f"captcha_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
            driver.save_screenshot(screenshot_path)
            logging.info(f"Saved CAPTCHA screenshot: {screenshot_path}")

            time.sleep(random.uniform(1.5, 2.5))

            try:
                # Try clicking CAPTCHA checkbox
                checkbox = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "input[type='checkbox'][id*='recaptcha'], div[class*='recaptcha-checkbox']"))
                )
                ActionChains(driver).move_to_element(checkbox).click().perform()
                logging.info("Clicked reCAPTCHA checkbox")
                time.sleep(4)

                # Check if CAPTCHA is resolved
                captcha_elements = driver.find_elements(By.CSS_SELECTOR, ".cf-turnstile, iframe[src*='recaptcha'], div[class*='g-recaptcha']")
                if not captcha_elements:
                    logging.info("CAPTCHA resolved after checkbox click")
                    return True
            except TimeoutException:
                logging.warning("Could not find clickable reCAPTCHA checkbox")

            # Check for image-based CAPTCHA
            try:
                challenge_iframe = driver.find_element(By.CSS_SELECTOR, "iframe[title*='challenge']")
                logging.warning("CAPTCHA escalated to image challenge")
                print(f"Image-based CAPTCHA detected. Manual intervention required. Screenshot: {screenshot_path}")
                input("Solve CAPTCHA manually and press Enter to continue...")
                return False
            except NoSuchElementException:
                logging.info("No image-based CAPTCHA detected")
                return True

        except Exception as e:
            logging.error(f"Error handling CAPTCHA on attempt {attempt + 1}: {e}")
            if attempt == max_attempts - 1:
                logging.error("Max CAPTCHA attempts reached. Giving up.")
                return False
            time.sleep(random.uniform(10, 15))

    return False

def is_duplicate_job(job_link):
    """Check if job link has already been processed with relaxed normalization"""
    if not job_link:
        return True
    
    # Normalize by removing query parameters and trailing slashes
    normalized_link = re.sub(r'\?.*$', '', job_link.strip().lower()).rstrip('/')
    if normalized_link in seen_job_links:
        return True
    
    seen_job_links.add(normalized_link)
    return False


def extract_salary_text(text):
    """Extract and normalize salary data from given raw text."""
    if not text:
        return "Not Disclosed"
    
    text = text.strip()
    patterns = [
        r"\₹?\s?[\d,\.]+(?:\s?[-–to]+\s?[\d,\.]+)?\s?(?:LPA|PA|per annum|lakhs|lakh|k|K|₹)?",  # e.g. ₹8L - ₹18L PA, 10-15 LPA, 5,00,000 - 8,00,000
        r"\$[\d,\.]+(?:\s?[-–to]+\s?[\d,\.]+)?\s?(?:per year|per annum|yearly)?",             # e.g. $50,000 - $70,000 per year
        r"[\d,\.]+\s?(?:to|[-–])\s?[\d,\.]+\s?(?:USD|INR|EUR|GBP|AED)?",
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(0)
    
    return text


def scrape_shine():
    """Scrape ALL jobs from Shine for each job role (unlimited)"""
    logging.info("Starting unlimited Shine scraping")
    print("\n=== Starting Unlimited Shine Scraping ===")
    all_jobs = []
    driver = create_stealth_driver()
    
    try:
        for role_idx, role in enumerate(job_roles):
            print(f"\n--- Scraping role {role_idx + 1}/{len(job_roles)}: {role} ---")
            query = requests.utils.quote(role.replace('/', '-').replace(':', ''))
            base_url = f"https://www.shine.com/job-search/{query}-jobs"
            
            driver.get(base_url)
            time.sleep(random.uniform(8, 12))
            
            # Handle initial pop-ups
            try:
                close_selectors = [
                    "button[aria-label='Close']", ".closeBtn", "[data-testid='modal-close']", 
                    "button[id*='reject']", "button[id*='accept']", ".gdpr-consent-button", 
                    ".modal-close", ".close-icon", ".popup-close"
                ]
                for selector in close_selectors:
                    try:
                        close_btn = WebDriverWait(driver, 5).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                        )
                        driver.execute_script("arguments[0].click();", close_btn)
                        time.sleep(2)
                        logging.info(f"Closed pop-up with selector: {selector}")
                        break
                    except:
                        continue
            except Exception as e:
                logging.warning(f"Failed to close pop-up on Shine: {e}")
            
            if not handle_captcha(driver):
                logging.error(f"Failed to handle CAPTCHA for role: {role}. Skipping.")
                print(f"✗ Failed to handle CAPTCHA for {role}. Skipping.")
                time.sleep(random.uniform(10, 15))
                continue
            
            page = 1
            max_pages = 25
            role_jobs_count = 0
            consecutive_duplicate_pages = 0
            max_consecutive_duplicates = 3
            
            with tqdm(desc=f"Shine {role[:20]}...", unit="job") as pbar:
                while page <= max_pages and consecutive_duplicate_pages < max_consecutive_duplicates:
                    retries = 3
                    page_success = False
                    
                    for attempt in range(retries):
                        try:
                            # Check for CAPTCHA on each page
                            if not handle_captcha(driver):
                                logging.error(f"CAPTCHA detected on page {page} for {role}. Skipping role.")
                                break
                            
                            # Wait for job cards
                            WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='jobCard'], li[class*='job'], article, [data-job-id]"))
                            )
                            
                            # Scroll to load content
                            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                            time.sleep(2)
                            driver.execute_script("window.scrollTo(0, 0);")
                            time.sleep(1)
                            
                            soup = BeautifulSoup(driver.page_source, 'html.parser')
                            
                            job_cards = (
                                soup.find_all('div', {'class': lambda x: x and 'jobCard' in str(x)}) or
                                soup.find_all('li', {'class': lambda x: x and 'job' in str(x).lower()}) or
                                soup.find_all('div', {'id': lambda x: x and 'job' in str(x).lower()}) or
                                soup.find_all('div', attrs={'data-job-id': True}) or
                                soup.find_all('article') or
                                []
                            )
                            
                            logging.info(f"Found {len(job_cards)} job cards for {role} on page {page}")
                            
                            if not job_cards:
                                logging.info(f"No job cards found on page {page} for {role}")
                                consecutive_duplicate_pages += 1
                                break
                            
                            page_jobs_count = 0
                            page_duplicate_count = 0
                            page_new_jobs = 0
                            
                            for card in job_cards:
                                try:
                                    title_elem = (
                                        card.find('h2') or card.find('h3') or 
                                        card.find('a', {'class': lambda x: x and 'title' in str(x).lower()}) or
                                        card.find('strong') or card.find('b') or
                                        card.find('span', {'class': lambda x: x and 'title' in str(x).lower()})
                                    )
                                    job_title = title_elem.get_text(strip=True) if title_elem else ''
                                    
                                    if not job_title or len(job_title) < 3:
                                        continue
                                    
                                    link_elem = card.find('a', href=True)
                                    job_link = ''
                                    if link_elem:
                                        href = link_elem.get('href', '')
                                        if href.startswith('/'):
                                            job_link = f"https://www.shine.com{href}"
                                        elif href.startswith('http'):
                                            job_link = href
                                    
                                    if is_duplicate_job(job_link):
                                        page_duplicate_count += 1
                                        continue
                                    
                                    company_name = 'Not specified'
                                    company_selectors = [
                                        ('div', {'class': lambda x: x and 'company' in str(x).lower()}),
                                        ('span', {'class': lambda x: x and 'company' in str(x).lower()}),
                                        ('a', {'class': lambda x: x and 'company' in str(x).lower()}),
                                        ('p', {'class': lambda x: x and 'company' in str(x).lower()}),
                                    ]
                                    
                                    for tag, attrs in company_selectors:
                                        company_elem = card.find(tag, attrs)
                                        if company_elem and company_elem.get_text(strip=True):
                                            company_name = company_elem.get_text(strip=True)
                                            break
                                    
                                    # FIXED: Better salary extraction for Shine
                                    salary = "Not Disclosed"
                                    salary_selectors = [
                                        ('li', {'class': lambda x: x and 'salary' in str(x).lower()}),
                                        ('div', {'class': lambda x: x and 'salary' in str(x).lower()}),
                                        ('span', {'class': lambda x: x and 'salary' in str(x).lower()}),
                                        ('div', {'class': 'salaryRange'}),
                                        ('span', {'class': 'salaryRange'}),
                                        ('div', {'class': lambda x: x and 'package' in str(x).lower()}),
                                    ]
                                    
                                    for tag, attrs in salary_selectors:
                                        salary_elem = card.find(tag, attrs)
                                        if salary_elem:
                                            raw_salary = salary_elem.get_text(strip=True)
                                            if raw_salary and 'lpa' in raw_salary.lower():
                                                salary = extract_salary_text(raw_salary)
                                                break
                                    
                                    # FIXED: Better date posted extraction for Shine
                                    date_posted = 'Not specified'
                                    date_selectors = [
                                        ('li', {'class': lambda x: x and 'time' in str(x).lower()}),
                                        ('div', {'class': lambda x: x and 'time' in str(x).lower()}),
                                        ('span', {'class': lambda x: x and 'time' in str(x).lower()}),
                                        ('div', {'class': lambda x: x and 'date' in str(x).lower()}),
                                        ('span', {'class': lambda x: x and 'date' in str(x).lower()}),
                                        ('time', {}),  # HTML5 time element
                                        ('div', {'class': 'jobAge'}),
                                        ('span', {'class': 'jobAge'}),
                                    ]
                                    
                                    for tag, attrs in date_selectors:
                                        date_elem = card.find(tag, attrs)
                                        if date_elem:
                                            date_text = date_elem.get_text(strip=True)
                                            if date_text and any(keyword in date_text.lower() for keyword in ['day', 'hour', 'week', 'month', 'ago', 'today', 'yesterday']):
                                                date_posted = clean_date_posted(date_text)
                                                break
                                    
                                    # Alternative: Extract from card text if specific elements not found
                                    card_text = card.get_text(strip=True)
                                    
                                    # If salary not found, try to extract from card text
                                    if salary == "Not Disclosed":
                                        salary_match = re.search(r'₹?\s?[\d,\.]+\s?[-–to]+\s?[\d,\.]+\s?(?:LPA|Lakh|Lac|PA|per annum)', card_text, re.IGNORECASE)
                                        if salary_match:
                                            salary = extract_salary_text(salary_match.group(0))
                                    
                                    # If date not found, try to extract from card text
                                    if date_posted == 'Not specified':
                                        date_match = re.search(r'(\d+\s*(?:day|days|hour|hours|week|weeks|month|months)\s*ago|today|yesterday|\d+[dDhH])', card_text, re.IGNORECASE)
                                        if date_match:
                                            date_posted = clean_date_posted(date_match.group(1))
                                    
                                    experience = extract_experience_enhanced(card.get_text(strip=True))
                                    
                                    if filter_experience(experience):
                                        job_data = {
                                            'job_title': job_title,
                                            'company_name': company_name,
                                            'job_link': job_link,
                                            'experience': experience,
                                            'salary': salary,
                                            'date_posted': date_posted
                                        }
                                        
                                        all_jobs.append(job_data)
                                        role_jobs_count += 1
                                        page_jobs_count += 1
                                        page_new_jobs += 1
                                        pbar.update(1)
                                        logging.info(f"Collected: {job_title} | {company_name} | Salary: {salary} | Posted: {date_posted}")
                                    
                                except Exception as e:
                                    logging.error(f"Error parsing job card: {e}")
                                    continue
                            
                            if page_new_jobs == 0:
                                consecutive_duplicate_pages += 1
                            else:
                                consecutive_duplicate_pages = 0
                            
                            print(f"Page {page}: {page_new_jobs} new, {page_duplicate_count} duplicates (Total: {role_jobs_count})")
                            
                            if consecutive_duplicate_pages >= max_consecutive_duplicates:
                                logging.info(f"Stopping {role} - {consecutive_duplicate_pages} consecutive duplicate pages")
                                break
                            
                            # Move to next page using URL-based pagination
                            if page < max_pages:
                                time.sleep(random.uniform(3, 5))
                                next_page = page + 1
                                
                                # Use the improved URL-based pagination
                                if click_numbered_page(driver, role, next_page):
                                    page = next_page
                                    page_success = True
                                    logging.info(f"Successfully moved to page {page} for {role}")
                                    break
                                else:
                                    logging.info(f"Pagination failed for {role} after page {page}")
                                    break
                            
                            page_success = True
                            break
                            
                        except TimeoutException as e:
                            logging.warning(f"Attempt {attempt + 1}/{retries}: Timeout on page {page}: {e}")
                            if attempt < retries - 1:
                                time.sleep(random.uniform(10, 15))
                                driver.refresh()
                                time.sleep(5)
                            else:
                                break
                        except Exception as e:
                            logging.error(f"Error on page {page}: {e}")
                            if attempt < retries - 1:
                                time.sleep(random.uniform(10, 15))
                            else:
                                break
                    
                    if not page_success:
                        break
                    time.sleep(random.uniform(5, 8))
                        
            print(f"✓ Finished {role}: {role_jobs_count} jobs")
            time.sleep(random.uniform(10, 15))
            
    except Exception as e:
        logging.error(f"Error scraping Shine: {e}")
        print(f"Error scraping Shine: {e}")
    finally:
        driver.quit()
        logging.info(f"Collected {len(all_jobs)} unique jobs from Shine")
        print(f"✓ Total collected {len(all_jobs)} unique jobs from Shine")
    
    return all_jobs

# --- Save to CSV ---
def save_to_csv(jobs, filename):
    """Save jobs to CSV file"""
    if not jobs:
        logging.warning(f"No jobs to save for {filename}")
        print(f"No jobs to save for {filename}")
        return
    
    filepath = os.path.join(Data_dir, filename)
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['job_title', 'company_name', 'job_link', 'experience', 'salary', 'date_posted']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(jobs)
        logging.info(f"Successfully saved {len(jobs)} jobs to {filepath}")
        print(f"✓ Successfully saved {len(jobs)} jobs to {filepath}")
    except Exception as e:
        logging.error(f"Error saving to CSV {filename}: {e}")
        print(f"Error saving to CSV {filename}: {e}")

# --- Main Execution ---
def main():
    """Main function to run both scrapers"""
    global seen_job_links
    seen_job_links = set() 
    
    print("Starting Unlimited Job Scraping...")
    logging.info("Starting unlimited job scraping process")
    
    start_time = time.time()
    
    # Scrape Shine
    shine_jobs = scrape_shine()
    save_to_csv(shine_jobs, f'shine_jobs.csv')
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"\nScraping completed!")
    print(f"Total time: {duration:.2f} seconds")
    print(f"Total unique jobs collected: {len(shine_jobs)}")
    print(f"Jobs saved to: {Data_dir}")
    logging.info(f"Scraping completed. Total unique jobs: {len(shine_jobs)}, Time: {duration:.2f}s")

if __name__ == "__main__":
    main()