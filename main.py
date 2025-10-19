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
    "(JDE) Developer", "Senior AI/ML Engineer", "AWS BI Architect", "AI Data Engineer designs", 
    "Sr. Denodo Admin", "OIC PaaS developer", "ProC Developer", "Composable Commerce Architect", 
    "Senior Windchill Developer", "Data bricks Data Engineer", "Magic XPA Subject Matter Expert", 
    "Node.JS Developer (Backend)", "ServiceNow Developer", "Sr. Contact Center Engineer", 
    ".NET Developer", "Senior Tableau Developer", "IT Infrastructure/Cloud area", 
    "Cloud Technology Consultant", "Contractor - SAP Global Trade Services", 
    "Cyber Security Engineer", "SDE I - Android Developer", "Senior Consultant- FullStack Developer", 
    "Senior Mainframe Security Analyst", "DevOps Engineer III", "BRM Developer", 
    "React.js + Electron.js Developer", "Oracle Integration Cloud (OIC) Developer", 
    "Sr. Software Engineer", "MicroStrategy Architect / Developer", "AKS Cloud Engineer", 
    "Java Full Stack Lead", "Senior MEAN Full-Stack Developers", "Senior Full Stack Software Developer", 
    "EDI Developer", "Sr. Salesforce Engineer", "Level 2 Managed Systems Engineer", 
    "Software Engineer (C/C++, Go)", "ETL Integration Engineer", "Salesforce Copado DevOps Engineer", 
    "Software Engineer - SQL Databases", "Cloud Engineer", "WebFOCUS Developer", 
    "Golang Developer", "Senior Java Software Engineer", "Technical Support Engineer", 
    "Senior Salesforce Developer", "CCAI BOT Developer", "Full Stack Development Engineer", 
    "Senior Node.js Engineer", "IT Helpdesk Specialist", "Integrations Engineer", 
    "Full stack Dot Net Developer", "Salesforce CPQ Developer", "HubSpot CMS", "AEM Developer", 
    "Python ML Developer", "Capacity Planning Engineer", "Backend Developer", "Sr CRM Developer", 
    "UI / UX Lead", "Node Full Stack Developer", "DevOps Architect", "Cognos Developer", 
    "SAP Developer", "Technical Curriculum Developer", "Senior Salesforce Commerce Cloud Developer", 
    "Zoho Specialist", "BASE24/EPS/UPF Engineer", "Python Fullstack Developer", "Azure Apps Engineer", 
    "Statistical Programmer", "Javascript Developer", "Structural Engineering", 
    "Senior Angular Developer", "Microsoft Dynamics AX Technical Lead", "IDMC Architect", 
    "SAP SuccessFactors Project Manager", "Frontend Engineer", ": Technical Consultant", 
    "Senior Software Designer", "PHP Laravel Developer", "Tech Lead", "Third Party Contractor", 
    "Back End Developer", "BI Developer", "Oracle Cloud Payroll Consultant", "EBX Architect/Senior Developer for MDM", 
    "UI/UX Developer", "Senior Developer", "iOS Engineer", "Senior AdTech Customer Data Solution Architect", 
    "AWS Cloud Engineers", "Global Senior Infrastructure Engineer", "UI Automation tester", 
    "Senior Frontend/FullStack Engineer", "Mirth Connect (HL7) Developer", "Java Springboot", 
    "Full Stack PHP Developer", "Platform Engineer Contractor", "Senior React Native Developer", 
    "Guidewire Digital Portals", "Mid/Sr. Full Stack Engineer", "SDET", "UX Software Engineer", 
    "SAP ABAP Developer", ": IBM TRIRIGA Developers", "Full Stack Developer (.NET/Vue)", 
    "Senior AEM Developer", "Senior MicroStrategy Developer", "NGP Fullstack Developer", 
    "Spark/Scala Developer", "Java Web Back End developer", "SAP Hybris Developer", 
    "Senior iOS Developer", "Scala QA Engineer", "Artificial Intelligence Engineer", 
    "Power BI Developer", "Data Science Engineer", "AI Developer", "React Native Developer", 
    "Software Developer - Java", "Senior Full-Stack Developer, Java", "Sr. QA Automation Engineer", 
    "Technical Content Developer", "MuleSoft Developer", "Sr. Java Developer", 
    "Software Test Engineer (on site)", "React/PHP Freelancer", "Senior Software QA Engineer", 
    "Fullstack Developer - Java, React", "Senior Data Engineer", "Java Spring MVC Developer", 
    "Compliance Senior Analyst - Contract", "Sr. AWS DevOps Engineer", "Full Stack Engineer", 
    "Jaspersoft Report Developer", "AI Automation & Software Engineering", 
    "No-Code / Low-Code Systems Developer", "UI/UX Designer", "SAP Datasphere", "SAP AFS", 
    "Informatics IDMC to PySpark", "MS Word/VBA Specialist", "SQL Experts for AI Initiative", 
    "Web Producer", "Cloud, AI", "QA Specialist", "Ingenium Architect / BAS / Developers / SME", 
    "BI Visualization Developer", "Senior Automation Lead", "Support Engineer", 
    "Profisee MDM Developer", "Data Architect", ".Net Architect", "Alfabet Specialist", 
    "Evaluation Specialist", "Java Full Stack Developer", "Senior Test Engineer", 
    "Senior Data Architect", "SAS Solution Designer", "Developer Engagement Representative", 
    "Technical Customer Support", "Supply Chain Management Analyst - SAP s4 Hana", 
    "Sr. Technical Product Manager", "Denodo Architect", "CCaaS NICE Implementation Project Manager", 
    "IFS Cloud Developer", "Drupal Frontend Developer", "Software Architect - PHP", 
    "ServiceNow Platform Architect", "Chief Engineer", "Blockchain Developer", 
    "Adobe Cloud Migration Developer", "Certinia Developer", "SAP ABAP Consultant", 
    "QA Lead", "SAP APO Consultant", "Senior Rust Engineer, CDK", "MS Dynamics 365 Developer", 
    "Localization Specialist", "Google CCAI BOT Developer", "Angular", "Python + FastAPI Developer", 
    "Senior Salesforce Service Cloud Developer", "Network Engineer", 
    "Oracle EBS SCM Functional Consultant", "Java Backend Developer", 
    "AI Quality Control Coding Specialist", "CIAM Engineer", "Techno-Functional Consultant", 
    "Senior Database Administrator", "MSD365 F&O Technical Consultant", 
    "Documentum AWS Consultant", "Securities and Commodities Specialist", "Power BI Specialist", 
    "L2 Security Analyst", "Freelance ServiceNow Developer", "Oracle Banking Product Developer", 
    "Head for AI and SLM", "Software Engineer III", "Siebel CRM setup", "IC - Website Development", 
    "Data and AI Consultant", "Customer Success Engineer", "SDE 2", "Salesforce Data Cloud Engineer", 
    "Informatica MDM CAI Developer", "Senior Technical Architect", "PTC FlexPLM Consultant", 
    "Middle Golang Developer", "Lead Quality Assurance (QA) Engineer", "Manual QA Specialist", 
    "Data Architect", "Informatica Customer 360 Cloud MDM Expert", "Freelance Coding Specialists", 
    "Azure Infrastructure Engineer", "Salesforce QA", "Senior Workday Integrations Consultant", 
    "Azure Security Presales Architect", "SAP BI/BW Consultant", "SAP PP Consultant", 
    "Senior DevOps Engineer with GCP", "Solutions Architect", ".Net Engineer", 
    "People Services Specialist", "eLearning Project Manager", "Principal Consultant - Data engineering", 
    "Sr .Net Developer", "Pharma Business Analyst", "Sourcing Consultant", "Mechanical Design Engineer", 
    "Sap Is-u Device Mnagement", "SAP Ariba Senior Consultants", "DBA with HANA", 
    "Senior Azure / Data Engineer", "Senior Biostatistician", "C++ Developer", "Site Reliability Engineer", 
    "BPC Backend Consultant", "AWS Engineer"
]

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

def handle_captcha(driver, max_attempts=3):
    """Detect and handle CAPTCHA by clicking checkbox and prompting for manual intervention if needed"""
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
                checkbox = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "input[type='checkbox'][id*='recaptcha'], div[class*='recaptcha-checkbox']"))
                )
                ActionChains(driver).move_to_element(checkbox).click().perform()
                logging.info("Clicked reCAPTCHA checkbox")
            except TimeoutException:
                logging.warning("Could not find clickable reCAPTCHA checkbox")
                return False

            time.sleep(4)

            try:
                challenge_iframe = driver.find_element(By.CSS_SELECTOR, "iframe[title*='challenge']")
                logging.warning("CAPTCHA escalated to image challenge")
                print(f"Image-based CAPTCHA detected. Manual intervention required. Screenshot: {screenshot_path}")
                input("Solve CAPTCHA manually and press Enter to continue...")
                return False
            except NoSuchElementException:
                logging.info("CAPTCHA likely resolved after checkbox click")
                return True

        except Exception as e:
            logging.error(f"Error handling CAPTCHA on attempt {attempt + 1}: {e}")
            if attempt == max_attempts - 1:
                logging.error("Max CAPTCHA attempts reached. Giving up.")
                return False
            time.sleep(random.uniform(10, 15))

    return False

# --- Shine Scraper ---
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
            time.sleep(random.uniform(6, 10))
            
            try:
                close_selectors = [
                    "button[aria-label='Close']", ".closeBtn", "[data-testid='modal-close']", "button[id*='reject']",
                    "button[id*='accept']", ".gdpr-consent-button", ".modal-close"
                ]
                for selector in close_selectors:
                    try:
                        close_btn = driver.find_element(By.CSS_SELECTOR, selector)
                        driver.execute_script("arguments[0].click();", close_btn)
                        time.sleep(2)
                        logging.info(f"Closed element with selector: {selector}")
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
            max_pages = 100  # Set a high limit instead of stopping at 50 jobs
            role_jobs_count = 0
            
            with tqdm(desc=f"Shine Jobs ({role[:30]}...)", unit="jobs") as pbar:
                while page <= max_pages:
                    retries = 5
                    cards_found = False
                    
                    for attempt in range(retries):
                        try:
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
                                    WebDriverWait(driver, 30).until(
                                        EC.presence_of_all_elements_located((by, selector))
                                    )
                                    cards_found = True
                                    logging.info(f"Found job cards using selector: {selector}")
                                    break
                                except:
                                    continue
                            
                            if not cards_found:
                                logging.error(f"Attempt {attempt + 1}/{retries}: No job cards found for {role}")
                                if attempt == retries - 1:
                                    screenshot_path = os.path.join(Data_dir, f"shine_timeout_{role}_page_{page}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
                                    driver.save_screenshot(screenshot_path)
                                    logging.info(f"Saved screenshot: {screenshot_path}")
                                    print(f"Saved screenshot: {screenshot_path}")
                                    break
                            
                            soup = BeautifulSoup(driver.page_source, 'html.parser')
                            job_cards = soup.find_all('div', {'class': lambda x: x and 'jobCard' in str(x)}) or \
                                        soup.find_all('li', {'class': lambda x: x and 'job' in str(x).lower()}) or \
                                        soup.find_all('div', {'id': lambda x: x and x.startswith('job')}) or \
                                        soup.find_all('div', attrs={'data-job-id': True}) or \
                                        soup.find_all('article')
                            
                            logging.info(f"Found {len(job_cards)} job cards for {role} on page {page}")
                            if not job_cards:
                                break
                            
                            page_jobs_count = 0
                            for card in job_cards:
                                try:
                                    title_elem = card.find('h2') or card.find('h3') or \
                                                 card.find('a', {'class': lambda x: x and 'title' in str(x).lower()}) or \
                                                 card.find('strong') or card.find('b')
                                    job_title = title_elem.get_text(strip=True) if title_elem else ''
                                    if not job_title or len(job_title) < 3:
                                        continue
                                    
                                    link_elem = card.find('a', href=True)
                                    job_link = f"https://www.shine.com{link_elem['href']}" if link_elem and link_elem.get('href').startswith('/') else link_elem.get('href', '') if link_elem else ''
                                    
                                    company_elem = None
                                    for tag, attrs in [
                                        ('div', {'class': lambda x: x and 'company' in str(x).lower()}),
                                        ('span', {'class': lambda x: x and 'company' in str(x).lower()}),
                                        ('a', {'class': lambda x: x and 'company' in str(x).lower()}),
                                        ('p', {'class': lambda x: x and 'company' in str(x).lower()}),
                                    ]:
                                        company_elem = card.find(tag, attrs)
                                        if company_elem and company_elem.get_text(strip=True):
                                            break
                                    company_name = company_elem.get_text(strip=True) if company_elem else 'Not specified'
                                    
                                    salary_elem = card.find('span', class_=lambda x: x and 'salary' in str(x).lower())
                                    salary = salary_elem.get_text(strip=True) if salary_elem else 'Not Disclosed'
                                    
                                    date_elem = card.find('span', class_=lambda x: x and 'date' in str(x).lower())
                                    date_posted = clean_date_posted(date_elem.get_text(strip=True) if date_elem else '')
                                    
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
                                        # Avoid duplicates
                                        if job_data not in all_jobs:
                                            all_jobs.append(job_data)
                                            role_jobs_count += 1
                                            page_jobs_count += 1
                                            pbar.update(1)
                                            logging.info(f"Collected job: {job_title} at {company_name} (Experience: {experience})")
                                    else:
                                        logging.debug(f"Skipped job: {job_title} due to experience filter: {experience}")
                                    
                                except Exception as e:
                                    logging.error(f"Error parsing job card for {role}: {e}")
                                    continue
                            
                            print(f"Page {page}: Collected {page_jobs_count} jobs (Total for {role}: {role_jobs_count})")
                            
                            if page_jobs_count == 0 and page > 1:
                                logging.info(f"No new jobs found on page {page} for {role}. Stopping pagination.")
                                break
                            
                            # Pagination
                            next_button = None
                            for selector in ["//a[contains(@class, 'next')]", "//button[contains(text(), 'Next')]", "//a[contains(@title, 'Next')]"]:
                                try:
                                    next_button = driver.find_element(By.XPATH, selector)
                                    if next_button.is_displayed() and next_button.is_enabled():
                                        break
                                    next_button = None
                                except:
                                    continue
                            
                            if next_button:
                                old_html = driver.page_source
                                driver.execute_script("arguments[0].click();", next_button)
                                WebDriverWait(driver, 30).until(lambda d: d.page_source != old_html)
                                time.sleep(random.uniform(6, 10))
                                page += 1
                                logging.info(f"Moving to Shine page {page} for {role}")
                            else:
                                logging.info(f"No next button found for {role} on page {page}")
                                break
                            
                            break
                        except TimeoutException as e:
                            logging.warning(f"Attempt {attempt + 1}/{retries}: Timeout for {role} on page {page}: {e}")
                            if attempt < retries - 1:
                                time.sleep(random.uniform(10, 15))
                            continue
                    
                    if not cards_found or page > max_pages:
                        break
                        
            print(f"✓ Collected {role_jobs_count} jobs for {role}")
            time.sleep(random.uniform(10, 15))
            
    except Exception as e:
        logging.error(f"Error scraping Shine: {e}")
        print(f"Error scraping Shine: {e}")
    finally:
        driver.quit()
        logging.info(f"Collected {len(all_jobs)} jobs from Shine")
        print(f"✓ Total collected {len(all_jobs)} jobs from Shine")
    
    return all_jobs

# --- Foundit Scraper ---
def scrape_foundit():
    """Scrape ALL jobs from Foundit for each job role (unlimited)"""
    logging.info("Starting unlimited Foundit scraping")
    print("\n=== Starting Unlimited Foundit Scraping ===")
    all_jobs = []
    driver = create_stealth_driver()
    
    try:
        for role_idx, role in enumerate(job_roles):
            print(f"\n--- Scraping role {role_idx + 1}/{len(job_roles)}: {role} ---")
            query = requests.utils.quote(role.replace('/', '-').replace(':', ''))
            base_url = f"https://www.foundit.in/srp/results?query={query}"
            
            driver.get(base_url)
            time.sleep(random.uniform(10, 15))
            
            # Handle pop-ups
            try:
                popup_selectors = [
                    "button[class*='close']",
                    "button[aria-label*='Close']", 
                    ".close",
                    "button[class*='accept']",
                    "button[class*='agree']"
                ]
                for selector in popup_selectors:
                    try:
                        close_btn = driver.find_element(By.CSS_SELECTOR, selector)
                        driver.execute_script("arguments[0].click();", close_btn)
                        time.sleep(2)
                        logging.info(f"Closed pop-up with selector: {selector}")
                        break
                    except:
                        continue
            except Exception as e:
                logging.warning(f"Failed to handle pop-up on Foundit: {e}")
            
            page = 1
            max_pages = 100  # Set a high limit
            role_jobs_count = 0
            
            with tqdm(desc=f"Foundit Jobs ({role[:30]}...)", unit="jobs") as pbar:
                while page <= max_pages:
                    retries = 3
                    for attempt in range(retries):
                        try:
                            # Wait for job cards to load
                            WebDriverWait(driver, 20).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "div.cardContainer, div[class*='job-card'], [data-job-id]"))
                            )
                            
                            # Scroll to load all content
                            for i in range(5):
                                driver.execute_script(f"window.scrollTo(0, {1000 * (i + 1)});")
                                time.sleep(random.uniform(1, 2))
                            
                            time.sleep(random.uniform(3, 5))
                            
                            soup = BeautifulSoup(driver.page_source, 'html.parser')
                            
                            # Find job cards
                            job_cards = soup.find_all('div', class_='cardContainer')
                            if not job_cards:
                                job_cards = soup.find_all('div', attrs={'data-job-id': True})
                            if not job_cards:
                                job_cards = soup.find_all('div', class_=lambda x: x and 'job' in str(x).lower() and 'card' in str(x).lower())
                            
                            logging.info(f"Found {len(job_cards)} job cards on Foundit page {page}")
                            
                            if not job_cards:
                                logging.info("No more jobs found on Foundit")
                                break
                            
                            page_jobs_count = 0
                            for card in job_cards:
                                try:
                                    # 1. JOB TITLE
                                    job_title = ''
                                    title_elem = card.find('div', class_='jobTitle')
                                    if title_elem:
                                        job_title = title_elem.get_text(strip=True)
                                    
                                    if not job_title or len(job_title) < 3:
                                        continue
                                    
                                    # 2. JOB LINK - FIXED: Construct from job ID
                                    job_link = ''
                                    job_id = card.get('id')  # Get the ID from cardContainer
                                    
                                    if job_id and job_id.isdigit():
                                        # Construct job URL using the ID
                                        job_link = f"https://www.foundit.in/job/{job_id}"
                                    else:
                                        # Alternative: Try to find clickable elements that might have data attributes
                                        clickable_elem = card.find(['div', 'section'], attrs={'data-job-id': True})
                                        if clickable_elem:
                                            job_id = clickable_elem.get('data-job-id')
                                            if job_id and job_id.isdigit():
                                                job_link = f"https://www.foundit.in/job/{job_id}"
                                    
                                    # 3. COMPANY NAME
                                    company_name = 'Not specified'
                                    company_elem = card.find('div', class_='companyName')
                                    if company_elem:
                                        company_name = company_elem.get_text(strip=True)
                                    
                                    # 4. EXPERIENCE
                                    experience = '0'
                                    card_body = card.find('div', class_='cardBody')
                                    if card_body:
                                        # Look for experience in the experienceSalary section
                                        experience_section = card_body.find('div', class_='experienceSalary')
                                        if experience_section:
                                            experience_span = experience_section.find('span', class_='details')
                                            if experience_span:
                                                experience_text = experience_span.get_text(strip=True)
                                                experience = extract_experience_enhanced(experience_text)
                                        
                                        # If not found, search all body rows
                                        if experience == '0':
                                            body_rows = card_body.find_all('div', class_='bodyRow')
                                            for row in body_rows:
                                                row_text = row.get_text(strip=True)
                                                if any(keyword in row_text.lower() for keyword in ['years', 'year', 'exp', 'experience', 'yrs']):
                                                    experience = extract_experience_enhanced(row_text)
                                                    if experience and experience != '0':
                                                        break
                                    
                                    # 5. SALARY - Check if it exists in the HTML structure
                                    salary = 'Not Disclosed'
                                    
                                    # Look for salary in experienceSalary section (might be next to experience)
                                    if card_body:
                                        experience_salary_section = card_body.find('div', class_='experienceSalary')
                                        if experience_salary_section:
                                            # Look for salary-specific elements
                                            salary_elements = experience_salary_section.find_all(['span', 'div'], string=re.compile(r'₹|LPA|lakh|salary', re.IGNORECASE))
                                            for elem in salary_elements:
                                                salary_text = elem.get_text(strip=True)
                                                if any(keyword in salary_text.lower() for keyword in ['₹', 'lpa', 'lakh', 'salary']):
                                                    salary = salary_text
                                                    break
                                            
                                            # If not found, check if there are multiple details spans
                                            details_spans = experience_salary_section.find_all('span', class_='details')
                                            if len(details_spans) > 1:
                                                # Second span might be salary
                                                salary_span = details_spans[1]
                                                salary_text = salary_span.get_text(strip=True)
                                                if any(keyword in salary_text.lower() for keyword in ['₹', 'lpa', 'lakh', 'salary']):
                                                    salary = salary_text
                                    
                                    # 6. DATE POSTED
                                    date_posted = 'Not specified'
                                    card_footer = card.find('div', class_='cardFooter')
                                    if card_footer:
                                        time_elem = card_footer.find('div', class_='jobAddedTime')
                                        if time_elem:
                                            time_text = time_elem.find('p', class_='timeText')
                                            if time_text:
                                                date_text = time_text.get_text(strip=True)
                                                if date_text:
                                                    date_posted = clean_date_posted(date_text)
                                    
                                    # Create job data
                                    job_data = {
                                        'job_title': job_title,
                                        'company_name': company_name,
                                        'job_link': job_link,
                                        'experience': experience,
                                        'salary': salary,
                                        'date_posted': date_posted
                                    }
                                    
                                    # Filter by experience and add to results
                                    if filter_experience(experience) and job_data not in all_jobs:
                                        all_jobs.append(job_data)
                                        role_jobs_count += 1
                                        page_jobs_count += 1
                                        pbar.update(1)
                                        logging.info(f"Collected: {job_title} | Company: {company_name} | Exp: {experience} | Salary: {salary}")
                                    else:
                                        logging.info(f"Skipped '{job_title}' - Experience: {experience}")
                                    
                                except Exception as e:
                                    logging.error(f"Error parsing job card: {e}")
                                    continue
                            
                            print(f"Page {page}: Collected {page_jobs_count} jobs (Total for {role}: {role_jobs_count})")
                            
                            if page_jobs_count == 0 and page > 1:
                                logging.info(f"No new jobs found on page {page} for {role}. Stopping pagination.")
                                break
                            
                            # Pagination
                            if len(job_cards) > 0:
                                try:
                                    next_button = None
                                    next_selectors = [
                                        "a[class*='next']",
                                        "button[class*='next']", 
                                        "a[title*='Next']",
                                        "button[title*='Next']",
                                        "//a[contains(text(), 'Next')]",
                                        "//button[contains(text(), 'Next')]"
                                    ]
                                    
                                    for selector in next_selectors:
                                        try:
                                            if selector.startswith('//'):
                                                next_button = driver.find_element(By.XPATH, selector)
                                            else:
                                                next_button = driver.find_element(By.CSS_SELECTOR, selector)
                                            
                                            if next_button.is_displayed() and next_button.is_enabled():
                                                break
                                            next_button = None
                                        except:
                                            continue
                                    
                                    if next_button:
                                        old_html = driver.page_source
                                        driver.execute_script("arguments[0].click();", next_button)
                                        time.sleep(random.uniform(5, 8))
                                        
                                        # Wait for new content to load
                                        WebDriverWait(driver, 20).until(
                                            lambda d: d.page_source != old_html
                                        )
                                        
                                        page += 1
                                        logging.info(f"Moving to Foundit page {page} for {role}")
                                    else:
                                        logging.info(f"No next button found for {role} on page {page}")
                                        break
                                        
                                except Exception as e:
                                    logging.error(f"Error with pagination for {role}: {e}")
                                    break
                            else:
                                break
                            
                            break
                            
                        except TimeoutException as e:
                            logging.warning(f"Attempt {attempt + 1}/{retries}: Timeout for {role} on page {page}: {e}")
                            if attempt < retries - 1:
                                time.sleep(random.uniform(10, 15))
                                driver.refresh()
                                time.sleep(random.uniform(5, 8))
                            else:
                                break
                    
                    if page > max_pages:
                        logging.info(f"Reached max pages ({max_pages}) for {role}")
                        break
                        
            print(f"✓ Collected {role_jobs_count} jobs for {role}")
            time.sleep(random.uniform(10, 15))
            
    except Exception as e:
        logging.error(f"Error scraping Foundit: {e}")
        print(f"Error scraping Foundit: {e}")
    finally:
        driver.quit()
        logging.info(f"Collected {len(all_jobs)} jobs from Foundit")
        print(f"✓ Total collected {len(all_jobs)} jobs from Foundit")
    
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
    print("🚀 Starting Unlimited Job Scraping...")
    logging.info("Starting unlimited job scraping process")
    
    start_time = time.time()
    
    # Scrape Shine
    shine_jobs = scrape_shine()
    save_to_csv(shine_jobs, f'shine_jobs_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
    
    # Scrape Foundit
    foundit_jobs = scrape_foundit()
    save_to_csv(foundit_jobs, f'foundit_jobs_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
    
    # Combine all jobs
    all_jobs = shine_jobs + foundit_jobs
    save_to_csv(all_jobs, f'all_jobs_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"\n🎉 Scraping completed!")
    print(f"⏱️  Total time: {duration:.2f} seconds")
    print(f"📊 Total jobs collected: {len(all_jobs)}")
    print(f"💾 Jobs saved to: {Data_dir}")
    logging.info(f"Scraping completed. Total jobs: {len(all_jobs)}, Time: {duration:.2f}s")

if __name__ == "__main__":
    main()