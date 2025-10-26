import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium_stealth import stealth
import csv
import time
import logging
import random
import re

# Setup logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Keywords to search
KEYWORDS = [
    "Software Developer",
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

# CSV setup
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'Data')
os.makedirs(DATA_DIR, exist_ok=True)
CSV_PATH = os.path.join(DATA_DIR, "linkedin_jobs.csv")

# Global variables
seen_links = set()
MAX_PAGES = 30

def create_stealth_driver():
    """Create a stealth Chrome driver with timeout settings"""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-images")
    options.add_argument("--disable-extensions")
    options.add_argument("--dns-prefetch-disable")
    options.add_argument("--disable-background-networking")
    
    # Add page load strategy
    options.page_load_strategy = 'eager'  # Don't wait for everything to load
    
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    ]
    options.add_argument(f'user-agent={user_agents[0]}')
    
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), 
        options=options
    )
    
    # Set timeouts
    driver.set_page_load_timeout(60)  # Max 60 seconds to load page
    driver.set_script_timeout(30)      # Max 30 seconds for scripts
    driver.implicitly_wait(10)         # Wait up to 10 seconds for elements
    
    stealth(driver,
            languages=["en-US", "en"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True)
    
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return driver

def close_popup(driver):
    """Close any pop-ups quickly"""
    try:
        close_btn = driver.find_element(By.CSS_SELECTOR, "button[aria-label='Dismiss']")
        close_btn.click()
        time.sleep(0.5)
    except:
        pass

def extract_experience_from_card(card):
    """Extract experience from job card - check both title and description snippet"""
    try:
        # Get title
        title = card.find_element(By.CSS_SELECTOR, "h3.base-search-card__title").text.strip()
        title_lower = title.lower()
        
        # Get all card text (includes description snippets)
        card_text = card.text.lower()
        
        # Senior/Lead = 5+ years (ACCEPT)
        if any(word in title_lower for word in ['senior', 'sr.', 'sr ', 'lead', 'principal', 'staff', 'architect']):
            return "5+"
        
        # Mid-level = 3-5 years (ACCEPT)
        if any(word in title_lower for word in ['mid-level', 'mid level', 'intermediate']):
            return "3-5"
        
        # Junior/Entry = 0-2 years (REJECT)
        if any(word in title_lower for word in ['junior', 'jr.', 'jr ', 'entry', 'fresher', 'graduate', 'intern', 'trainee']):
            return "0-2"
        
        # Look for explicit years in card text (includes snippets)
        # Pattern: "2+ years", "3-5 years", "minimum 2 years", etc.
        patterns = [
            r'(\d+)\s*\+\s*(?:years?|yrs?)',  # "2+ years"
            r'(\d+)\s*[-–—to]\s*(\d+)\s*(?:years?|yrs?)',  # "2-5 years"
            r'(?:minimum|min|at least)\s*(\d+)\s*(?:years?|yrs?)',  # "minimum 2 years"
            r'(\d+)\s*(?:years?|yrs?)\s*(?:of)?\s*experience',  # "2 years experience"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, card_text)
            if match:
                # Get the first number
                num = int(match.group(1))
                if num >= 2:
                    if '-' in match.group(0) or 'to' in match.group(0):
                        # It's a range like "2-5"
                        num2 = int(match.group(2)) if len(match.groups()) > 1 else num
                        return f"{num}-{num2}"
                    elif '+' in match.group(0):
                        return f"{num}+"
                    else:
                        return f"{num}+"
                else:
                    # Less than 2 years mentioned
                    return "0-2"
        
        # If no experience found, mark as Not specified
        return "Not specified"
        
    except Exception as e:
        return "Not specified"

def should_include_job(title, experience):
    """Quick filter based on title and experience - STRICT >= 2 years only"""
    if not title:
        return False
    
    title_lower = title.lower()
    
    # Reject if junior/entry/fresher
    if any(word in title_lower for word in ['junior', 'jr.', 'entry', 'fresher', 'graduate', 'intern', 'trainee']):
        return False
    
    # Reject if experience is not specified or clearly less than 2 years
    if experience in ['0', '0-1', '0-2', '1', 'Not specified']:
        return False
    
    # Accept only if we have confirmed 2+ years (Senior, Mid, or explicit numbers >= 2)
    accepted_experiences = ['3-5', '5+']
    if experience in accepted_experiences:
        return True
    
    # Check if it's a number >= 2
    if experience.endswith('+'):
        try:
            num = int(experience[:-1])
            return num >= 2
        except:
            return False
    
    # Check ranges like "2-5", "3-7"
    if '-' in experience:
        try:
            min_exp = int(experience.split('-')[0])
            return min_exp >= 2
        except:
            return False
    
    # If we reach here, we couldn't confirm 2+ years - REJECT
    return False

def get_job_cards(driver):
    """Get job cards from page"""
    try:
        job_cards = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, "ul.jobs-search__results-list li")
            )
        )
        return job_cards
    except:
        return []

def is_remote_job(text):
    """Verify job is actually remote"""
    if not text:
        return False
    
    text_lower = text.lower()
    
    # Check for remote keywords
    remote_keywords = ['remote', 'work from home', 'wfh', 'work-from-home']
    has_remote = any(kw in text_lower for kw in remote_keywords)
    
    # Exclude on-site/hybrid
    exclude_keywords = ['on-site', 'onsite', 'on site', 'hybrid', 'office', 'in-office']
    has_exclude = any(kw in text_lower for kw in exclude_keywords)
    
    return has_remote and not has_exclude

def is_contract_job(text):
    """Verify job is actually contract"""
    if not text:
        return False
    
    text_lower = text.lower()
    
    # Check for contract keywords
    contract_keywords = ['contract', 'freelance', 'temporary', 'contractor', 'c2h', 'contractual']
    has_contract = any(kw in text_lower for kw in contract_keywords)
    
    # Exclude full-time/permanent
    exclude_keywords = ['full-time', 'full time', 'fulltime', 'permanent', 'permanent position']
    has_exclude = any(kw in text_lower for kw in exclude_keywords)
    
    return has_contract and not has_exclude

def normalize_job_link(link):
    """Remove tracking parameters from job link to detect duplicates"""
    if not link:
        return ""
    
    # Remove everything after '?' to strip tracking parameters
    base_link = link.split('?')[0]
    return base_link.strip().lower()

def extract_job_from_card_fast(card):
    """Extract job info from card ONLY - NO page visits"""
    try:
        # Get all card text for validation
        card_text = card.text
        
        # Title
        title_elem = card.find_element(By.CSS_SELECTOR, "h3.base-search-card__title")
        title = title_elem.text.strip()
        
        # Company
        try:
            company = card.find_element(By.CSS_SELECTOR, "h4.base-search-card__subtitle a").text.strip()
        except:
            company = "N/A"
        
        # Location
        try:
            location = card.find_element(By.CSS_SELECTOR, "span.job-search-card__location").text.strip()
        except:
            location = "N/A"
        
        # Posted date
        try:
            posted_date = card.find_element(By.CSS_SELECTOR, "time.job-search-card__listdate").get_attribute("datetime").strip()
        except:
            posted_date = "N/A"
        
        # Job link
        try:
            job_link = card.find_element(By.CSS_SELECTOR, "a.base-card__full-link").get_attribute("href")
        except:
            return None
        
        # Normalize link for duplicate detection
        normalized_link = normalize_job_link(job_link)
        
        # Check for duplicates using normalized link
        if normalized_link in seen_links:
            return None
        
        # VALIDATION: Check if actually remote + contract
        combined_text = f"{title} {location} {card_text}".lower()
        
        if not is_remote_job(combined_text):
            return None
        
        if not is_contract_job(combined_text):
            return None
        
        # Extract experience from card (title + description snippets)
        experience = extract_experience_from_card(card)
        
        # Try to get salary from card (usually not available on listing)
        salary = "N/A"
        try:
            salary = card.find_element(By.CSS_SELECTOR, "span.job-search-card__salary-info").text.strip()
        except:
            pass
        
        # Quick filter for experience
        if not should_include_job(title, experience):
            return None
        
        # Add normalized link to seen_links
        seen_links.add(normalized_link)
        
        return {
            'title': title,
            'company': company,
            'location': location,
            'posted_date': posted_date,
            'experience': experience,
            'salary': salary,
            'job_link': job_link
        }
        
    except Exception as e:
        return None

def scrape_jobs_for_keyword(driver, writer, keyword):
    """Scrape jobs for a single keyword - FAST"""
    logging.info(f"{'='*60}")
    logging.info(f"Starting: {keyword}")
    logging.info(f"{'='*60}")
    
    query = keyword.replace(' ', '%20')
    base_url = (
        f"https://www.linkedin.com/jobs/search/?keywords={query}"
        "&location=Worldwide"
        "&f_E=2,3,4,5,6"  # Exclude internship
        "&f_JT=C"          # Contract only
        "&f_WT=2"          # Remote only
    )
    
    keyword_jobs_count = 0
    consecutive_empty = 0
    
    for page in range(MAX_PAGES):
        url = base_url if page == 0 else f"{base_url}&start={page * 25}"
        
        logging.info(f"Page {page + 1}/{MAX_PAGES}")
        
        try:
            driver.get(url)
            time.sleep(random.uniform(3, 5))  # Increased delay
            close_popup(driver)
        except Exception as e:
            logging.error(f"Error loading page {page + 1}: {e}")
            time.sleep(10)  # Wait longer before retry
            continue
        
        # Quick scroll
        try:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.5)
        except:
            pass
        
        job_cards = get_job_cards(driver)
        
        if not job_cards:
            logging.warning(f"No job cards found")
            consecutive_empty += 1
            if consecutive_empty >= 3:
                break
            continue
        
        logging.info(f"Processing {len(job_cards)} job cards...")
        
        page_jobs = 0
        
        for card in job_cards:
            job_data = extract_job_from_card_fast(card)
            
            if not job_data:
                continue
            
            # Write to CSV immediately (duplicate check already done in extract function)
            writer.writerow([
                job_data['title'],
                job_data['company'],
                job_data['location'],
                job_data['posted_date'],
                job_data['experience'],
                job_data['salary'],
                job_data['job_link']
            ])
            
            keyword_jobs_count += 1
            page_jobs += 1
            
            # Compact logging
            logging.info(
                f"✓ #{keyword_jobs_count}: {job_data['title'][:35]}... | "
                f"{job_data['company'][:20]}... | Exp: {job_data['experience']}"
            )
        
        logging.info(f"Page {page + 1} complete: {page_jobs} jobs added")
        
        if page_jobs == 0:
            consecutive_empty += 1
            if consecutive_empty >= 3:
                logging.info("3 consecutive empty pages, moving to next keyword")
                break
        else:
            consecutive_empty = 0
        
        time.sleep(random.uniform(3, 6))
    
    logging.info(f"Finished '{keyword}': {keyword_jobs_count} total jobs\n")
    return keyword_jobs_count

def scrape_linkedin():
    start_time = time.time()
    
    
    driver = create_stealth_driver()
    total_jobs = 0
    
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow([
            "Job Title", "Company", "Location", "Posted Date",
            "Experience Level", "Salary", "Job Link"
        ])
        
        try:
            for idx, keyword in enumerate(KEYWORDS, 1):
                logging.info(f"Keyword {idx}/{len(KEYWORDS)}")
                jobs_count = scrape_jobs_for_keyword(driver, writer, keyword)
                total_jobs += jobs_count
                
                if idx < len(KEYWORDS):
                    time.sleep(random.uniform(2, 3))
                
        except KeyboardInterrupt:
            logging.info("\nScraping interrupted by user")
        except Exception as e:
            logging.error(f"Error: {e}")
        finally:
            driver.quit()
            
            elapsed = time.time() - start_time
            minutes = int(elapsed // 60)
            seconds = int(elapsed % 60)
            
            logging.info("SCRAPING COMPLETE!")
            logging.info(f"Total Jobs: {total_jobs}")
            logging.info(f"Time Taken: {minutes}m {seconds}s")
            logging.info(f"CSV saved: {CSV_PATH}")

if __name__ == "__main__":
    scrape_linkedin()