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

# --- Indeed Scraper ---
def scrape_indeed(max_jobs=50):
    """Scrape jobs from Indeed for each job role, up to max_jobs total"""
    logging.info("Starting Indeed scraping")
    print("\n=== Starting Indeed Scraping ===")
    all_jobs = []
    driver = create_stealth_driver()
    
    try:
        for role in job_roles:
            if len(all_jobs) >= max_jobs:
                break
                
            query = requests.utils.quote(role)
            base_url = f"https://in.indeed.com/jobs?q={query}"
            logging.info(f"Scraping Indeed for role: {role} with URL: {base_url}")
            
            driver.get(base_url)
            time.sleep(random.uniform(8, 12))
            
            # Handle pop-ups
            try:
                popup_selectors = [
                    "button[aria-label*='close']",
                    "button[aria-label*='Close']", 
                    ".icl-CloseButton",
                    "button[class*='close']",
                    "button[class*='dismiss']",
                    ".popover-close-link"
                ]
                for selector in popup_selectors:
                    try:
                        btn = driver.find_element(By.CSS_SELECTOR, selector)
                        driver.execute_script("arguments[0].click();", btn)
                        time.sleep(2)
                        logging.info(f"Closed pop-up with selector: {selector}")
                        break
                    except:
                        continue
            except Exception as e:
                logging.warning(f"Failed to close pop-up on Indeed: {e}")

            jobs_collected = len(all_jobs)
            page = 1
            
            with tqdm(total=max_jobs - jobs_collected, desc=f"Indeed Jobs ({role})", dynamic_ncols=True, smoothing=0.1, unit=" jobs") as pbar:
                while jobs_collected < max_jobs:
                    retries = 3
                    for attempt in range(retries):
                        try:
                            # Wait for job cards to load with better selectors
                            WebDriverWait(driver, 20).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "div.job_seen_beacon, div[data-jk], [data-testid*='job'], .jobsearch-SerpJobCard"))
                            )
                            
                            # Scroll to load content
                            for i in range(4):
                                driver.execute_script(f"window.scrollTo(0, {1000 * (i + 1)});")
                                time.sleep(random.uniform(1, 2))
                            
                            time.sleep(random.uniform(3, 5))
                            
                            soup = BeautifulSoup(driver.page_source, 'html.parser')
                            
                            # Find job cards - Indeed specific selectors
                            job_cards = soup.find_all('div', class_='job_seen_beacon')
                            if not job_cards:
                                job_cards = soup.find_all('div', class_='jobsearch-SerpJobCard')
                            if not job_cards:
                                job_cards = soup.find_all('div', attrs={'data-jk': True})  # Indeed job key
                            if not job_cards:
                                job_cards = soup.find_all('div', attrs={'data-testid': True})
                            
                            logging.info(f"Found {len(job_cards)} job cards for {role} on page {page}")
                            
                            if not job_cards:
                                logging.info(f"No job cards found for {role} on page {page}")
                                break
                            
                            for card in job_cards:
                                if jobs_collected >= max_jobs:
                                    break
                                
                                try:
                                    # Debug: Save first card HTML
                                    if jobs_collected == 0 and page == 1:
                                        with open('indeed_card_debug.html', 'w', encoding='utf-8') as f:
                                            f.write(str(card.prettify()))
                                        print("✓ Saved Indeed card HTML to 'indeed_card_debug.html'")
                                    
                                    # 1. JOB TITLE
                                    job_title = ''
                                    title_elem = card.find('h2', class_='jobTitle')
                                    if not title_elem:
                                        title_elem = card.find('h2')
                                    if not title_elem:
                                        title_elem = card.find('a', class_='jcs-JobTitle')
                                    
                                    if title_elem:
                                        # Get text from span inside h2 if exists
                                        title_span = title_elem.find('span')
                                        if title_span and title_span.get_text(strip=True):
                                            job_title = title_span.get_text(strip=True)
                                        else:
                                            job_title = title_elem.get_text(strip=True)
                                    
                                    if not job_title or len(job_title) < 3:
                                        continue
                                    
                                    # 2. JOB LINK
                                    job_link = ''
                                    link_elem = card.find('a', class_='jcs-JobTitle')
                                    if not link_elem:
                                        link_elem = card.find('h2').find('a') if card.find('h2') else None
                                    
                                    if link_elem and link_elem.get('href'):
                                        href = link_elem['href']
                                        if href.startswith('/'):
                                            job_link = f"https://in.indeed.com{href}"
                                        elif href.startswith('http'):
                                            job_link = href
                                        else:
                                            job_link = f"https://in.indeed.com/{href}"
                                    
                                    # 3. COMPANY NAME
                                    company_name = 'Not specified'
                                    company_elem = card.find('span', {'data-testid': 'company-name'})
                                    if not company_elem:
                                        company_elem = card.find('span', class_='companyName')
                                    if company_elem:
                                        company_name = company_elem.get_text(strip=True)
                                    
                                    # 4. SALARY
                                    salary = 'Not Disclosed'
                                    salary_elem = card.find('div', class_='salary-snippet-container')
                                    if not salary_elem:
                                        salary_elem = card.find('span', class_='estimated-salary')
                                    if salary_elem:
                                        salary = salary_elem.get_text(strip=True)
                                    
                                    # 5. DATE POSTED
                                    date_posted = 'Not specified'
                                    date_elem = card.find('span', class_='date')
                                    if not date_elem:
                                        date_elem = card.find('span', class_='result-link-bar')
                                    if date_elem:
                                        date_text = date_elem.get_text(strip=True)
                                        date_posted = clean_date_posted(date_text)
                                    
                                    # 6. EXPERIENCE - Extract from job snippet
                                    experience = '0'
                                    snippet_elem = card.find('div', class_='job-snippet')
                                    if snippet_elem:
                                        snippet_text = snippet_elem.get_text(strip=True)
                                        experience = extract_experience_enhanced(snippet_text)
                                    
                                    # If no experience found in snippet, check entire card
                                    if experience == '0':
                                        card_text = card.get_text(strip=True)
                                        experience = extract_experience_enhanced(card_text)
                                    
                                    # Add job if it passes experience filter
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
                                        jobs_collected += 1
                                        pbar.update(1)
                                        logging.info(f"Collected: {job_title} | Company: {company_name} | Exp: {experience} | Salary: {salary}")
                                    else:
                                        logging.info(f"Skipped '{job_title}' - Experience: {experience}")
                                    
                                except Exception as e:
                                    logging.error(f"Error parsing Indeed job card: {e}")
                                    continue
                            
                            # PAGINATION
                            if jobs_collected < max_jobs:
                                try:
                                    next_button = None
                                    next_selectors = [
                                        "a[data-testid='pagination-page-next']",
                                        "a[aria-label='Next Page']",
                                        "a[aria-label='Next']",
                                        "//a[contains(text(), 'Next')]",
                                        "//span[contains(text(), 'Next')]"
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
                                        driver.execute_script("arguments[0].scrollIntoView();", next_button)
                                        time.sleep(2)
                                        driver.execute_script("arguments[0].click();", next_button)
                                        time.sleep(random.uniform(5, 8))
                                        page += 1
                                        logging.info(f"Moving to Indeed page {page}")
                                    else:
                                        logging.info(f"No more pages for {role}")
                                        break
                                        
                                except Exception as e:
                                    logging.error(f"Pagination error on Indeed: {e}")
                                    break
                            else:
                                break
                                
                            break  # Break out of retry loop if successful
                            
                        except TimeoutException:
                            logging.warning(f"Attempt {attempt + 1}/{retries}: Timeout on Indeed page {page}")
                            if attempt < retries - 1:
                                time.sleep(random.uniform(8, 12))
                            else:
                                break
                    else:
                        break  # No cards found after retries
                        
            # Add delay between roles
            time.sleep(random.uniform(5, 8))
            
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
    """Scrape jobs from Shine for each job role, up to max_jobs total"""
    logging.info("Starting Shine scraping")
    print("\n=== Starting Shine Scraping ===")
    all_jobs = []
    driver = create_stealth_driver()
    
    try:
        for role in job_roles:
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
            
            jobs_collected = len(all_jobs)
            page = 1
            
            with tqdm(total=max_jobs - jobs_collected, desc=f"Shine Jobs ({role})") as pbar:
                while jobs_collected < max_jobs:
                    retries = 5
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
                            
                            for card in job_cards:
                                if len(all_jobs) >= max_jobs:
                                    break
                                
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
                                        logging.debug(f"Skipped job: {job_title} due to experience filter: {experience}")
                                    
                                except Exception as e:
                                    logging.error(f"Error parsing job card for {role}: {e}")
                                    continue
                            
                            if jobs_collected < max_jobs:
                                next_button = None
                                for selector in ["//a[contains(@class, 'next')]", "//button[contains(text(), 'Next')]"]:
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
                            
                    if not cards_found:
                        break
                        
                    if len(all_jobs) >= max_jobs:
                        break
                        
            if len(all_jobs) >= max_jobs:
                break
            time.sleep(random.uniform(10, 15))
            
    except Exception as e:
        logging.error(f"Error scraping Shine: {e}")
        print(f"Error scraping Shine: {e}")
    finally:
        driver.quit()
        logging.info(f"Collected {len(all_jobs)} jobs from Shine")
        print(f"✓ Collected {len(all_jobs)} jobs from Shine")
    
    return all_jobs

# --- Foundit Scraper ---
def scrape_foundit(max_jobs=50):
    """Scrape up to 50 jobs from Foundit, stopping after the first role"""
    logging.info("Starting Foundit scraping")
    print("\n=== Starting Foundit Scraping ===")
    all_jobs = []
    driver = create_stealth_driver()
    
    try:
        for role in job_roles:
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
            
            jobs_collected = len(all_jobs)
            page = 1
            
            with tqdm(total=max_jobs - jobs_collected, desc=f"Foundit Jobs ({role})", dynamic_ncols=True, smoothing=0.1, unit=" jobs") as pbar:
                while jobs_collected < max_jobs:
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
                            
                            for card in job_cards:
                                if jobs_collected >= max_jobs:
                                    break
                                
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
                                    if filter_experience(experience):
                                        all_jobs.append(job_data)
                                        jobs_collected += 1
                                        pbar.update(1)
                                        logging.info(f"Collected: {job_title} | Company: {company_name} | Exp: {experience} | Salary: {salary} | Link: {job_link}")
                                    else:
                                        logging.info(f"Skipped '{job_title}' - Experience: {experience}")
                                    
                                except Exception as e:
                                    logging.error(f"Error parsing job card: {e}")
                                    continue
                            
                            # Pagination
                            if jobs_collected < max_jobs and len(job_cards) > 0:
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
                                        driver.execute_script("arguments[0].scrollIntoView();", next_button)
                                        time.sleep(2)
                                        driver.execute_script("arguments[0].click();", next_button)
                                        time.sleep(random.uniform(5, 8))
                                        page += 1
                                        logging.info(f"Moving to page {page}")
                                    else:
                                        logging.info("No more pages available")
                                        break
                                        
                                except Exception as e:
                                    logging.error(f"Pagination error: {e}")
                                    break
                            else:
                                break
                                
                            break  # Break out of retry loop if successful
                            
                        except TimeoutException:
                            logging.warning(f"Attempt {attempt + 1}/{retries}: Timeout on page {page}")
                            if attempt < retries - 1:
                                time.sleep(random.uniform(5, 8))
                            else:
                                break
                    else:
                        break  # No cards found after retries
                        
            # Stop after first role as requested
            break
            
    except Exception as e:
        logging.error(f"Error scraping Foundit: {e}")
        print(f"Error scraping Foundit: {e}")
    finally:
        driver.quit()
        logging.info(f"Collected {len(all_jobs)} jobs from Foundit")
        print(f"✓ Collected {len(all_jobs)} jobs from Foundit")
    
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
    all_jobs = []
    
    try:
        indeed_jobs = scrape_indeed(max_jobs=50)
        all_jobs.extend(indeed_jobs)
        print(f"✓ Collected {len(indeed_jobs)} jobs from Indeed")
    except Exception as e:
        logging.error(f"Failed to scrape Indeed: {e}")
        print(f"✗ Failed to scrape Indeed: {e}")
    
    # time.sleep(random.uniform(10, 15))
    
    # try:
    #     shine_jobs = scrape_shine(max_jobs=50)
    #     all_jobs.extend(shine_jobs)
    #     print(f"✓ Collected {len(shine_jobs)} jobs from Shine")
    # except Exception as e:
    #     logging.error(f"Failed to scrape Shine: {e}")
    #     print(f"✗ Failed to scrape Shine: {e}")
    
    # time.sleep(random.uniform(10, 15))
    
    # try:
    #     foundit_jobs = scrape_foundit(max_jobs=50)
    #     all_jobs.extend(foundit_jobs)
    #     print(f"✓ Collected {len(foundit_jobs)} jobs from Foundit")
    # except Exception as e:
    #     logging.error(f"Failed to scrape Foundit: {e}")
    #     print(f"✗ Failed to scrape Foundit: {e}")
    
    if all_jobs:
        output_filename = os.path.join(Data_dir, f"jobs_raw_ml_extracted.csv")
        save_to_csv(all_jobs, output_filename)
        
        print("\n" + "=" * 60)
        print(f"SCRAPING COMPLETE!")
        print(f"Total jobs collected: {len(all_jobs)}")
        print(f"Output file: {output_filename}")
        print("=" * 60)
    else:
        logging.warning("No jobs were collected from any site!")
        print("\n✗ No jobs were collected from any site!")