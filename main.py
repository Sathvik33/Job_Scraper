import requests
import csv
import time
import os
import random
import logging
import json
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


try:
    Base_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    Base_dir = os.getcwd()

Data_dir = os.path.join(Base_dir, 'Data')
os.makedirs(Data_dir, exist_ok=True)

job_roles = [
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

# --- MODIFIED Job Filtering Functions ---

def is_remote_job(job_text):
    """Check if job is remote/work from home"""
    if not job_text:
        return False
    
    job_text_lower = job_text.lower()
    remote_keywords = [
        'remote', 'work from home', 'wfh', 'work-from-home', 
        'virtual', 'telecommute', 'work anywhere', 'distributed team'
    ]
    
    return any(keyword in job_text_lower for keyword in remote_keywords)

def is_contract_job(job_text):
    """Check if job is contract/freelance"""
    if not job_text:
        return False
    
    job_text_lower = job_text.lower()
    contract_keywords = [
        'contract', 'freelance', 'temporary', '6 month', '12 month',
        'contractor', 'contract basis', 'project basis', 'short term',
        'contract to hire', 'c2h', 'contractual', 'fixed term'
    ]
    
    return any(keyword in job_text_lower for keyword in contract_keywords)

def has_excluded_keywords(job_text):
    if not job_text:
        return True
    
    job_text_lower = job_text.lower()
    
    # CRITICAL: Exclude ALL full-time jobs
    fulltime_keywords = [
        'full time', 'full-time', 'fulltime', 
        'permanent', 'permanent position', 'permanent role',
        'direct hire', 'employee', 'employment'
    ]
    
    # Exclude on-site jobs
    onsite_keywords = [
        'on-site', 'onsite', 'on site',
        'work from office', 'office based', 'office-based',
        'in-office', 'in office',
        'must relocate', 'relocation required'
    ]
    
    # Exclude hybrid jobs
    hybrid_keywords = ['hybrid']

    # Exclude part-time
    parttime_keywords = ['part-time', 'part time', 'parttime']
    
    # Combine all exclusion keywords
    all_excluded = fulltime_keywords + onsite_keywords + hybrid_keywords + parttime_keywords
    
    # Check if any excluded keyword exists
    for keyword in all_excluded:
        if keyword in job_text_lower:
            return True
    
    return False

def filter_experience(exp_text):
    """Filter for 2+ years experience"""
    if not exp_text or exp_text == "0" or exp_text == "Not specified":
        return False

    exp_text_lower = exp_text.lower().strip()

    # Explicitly reject fresher/entry-level positions
    reject_keywords = ['fresher', 'intern', 'trainee', 'graduate', 'entry level', 'entry-level', '0-1', '0-2', '0 year', '1 year']
    if any(word in exp_text_lower for word in reject_keywords):
        return False

    # Handle ranges like "2-5 years"
    range_match = re.search(r'(\d+)\s*[-–—to]+\s*(\d+)', exp_text_lower)
    if range_match:
        try:
            min_exp = int(range_match.group(1))
            return min_exp >= 2
        except ValueError:
            return False

    # Handle "3+" or "3+ years"
    plus_match = re.search(r'(\d+)\s*\+', exp_text_lower)
    if plus_match:
        try:
            exp = int(plus_match.group(1))
            return exp >= 2
        except ValueError:
            return False

    # Handle simple numbers like "3 years"
    num_match = re.search(r'(\d+)\s*(?:years?|yrs?|y)\b', exp_text_lower)
    if num_match:
        try:
            exp = int(num_match.group(1))
            return exp >= 2
        except ValueError:
            return False

    return False

def meets_all_criteria(job_title, company_name, job_text, experience_text):
    """MODIFIED: Now requires BOTH remote AND contract, not just one or the other"""
    if not job_title or not job_text:
        return False
    
    # STEP 1: Check EXCLUSIONS first (if any excluded keyword found, reject immediately)
    if has_excluded_keywords(job_text):
        return False
    
    # STEP 2: Check INCLUSIONS (MUST have BOTH Remote AND Contract)
    is_remote = is_remote_job(job_text)
    is_contract = is_contract_job(job_text)
    
    if not (is_remote and is_contract):
        return False
    
    # STEP 3: Check experience requirement (must be 2+ years)
    if not filter_experience(experience_text):
        return False
    
    # All criteria met!
    return True

# --- Extraction Functions for Company Name & Date Posted ---

def extract_company_name(card):
    company_name = 'Not specified'
    
    # Primary Strategy: Direct class name matching
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
    
    # Fallback Strategy: If still not found, try broader patterns
    if company_name == 'Not specified':
        fallback_selectors = [
            ('div', {'class': lambda x: x and 'employer' in str(x).lower()}),
            ('div', {'class': lambda x: x and 'organization' in str(x).lower()}),
            ('span', {'data-company': True}),
            ('div', {'itemprop': 'hiringOrganization'}),
        ]
        
        for tag, attrs in fallback_selectors:
            company_elem = card.find(tag, attrs)
            if company_elem and company_elem.get_text(strip=True):
                company_name = company_elem.get_text(strip=True)
                break
    
    # Additional fallback: Look for common text patterns
    if company_name == 'Not specified':
        # Look for any element that might contain company name
        all_elements = card.find_all(['div', 'span', 'p', 'h3', 'h4', 'h5'])
        for elem in all_elements:
            text = elem.get_text(strip=True)
            # Skip if text is too short, looks like a date, or contains common job-related terms
            if (text and len(text) > 2 and 
                not any(word in text.lower() for word in ['apply', 'job', 'posted', 'days ago']) and
                not re.search(r'\d+\s*(day|hour|week|month|ago)', text.lower())):
                company_name = text
                break
    
    # Clean up the company name
    company_name = clean_company_name(company_name)
    
    return company_name if company_name and company_name != 'Not specified' else 'Not specified'

def clean_company_name(company_text):
    if not company_text or company_text == 'Not specified':
        return 'Not specified'
    
    # Remove common prefixes
    prefixes_to_remove = [
        r'^company:\s*',
        r'^employer:\s*',
        r'^at\s+',
        r'^by\s+',
        r'^posted by\s+',
        r'^hiring:\s*',
    ]
    
    for prefix in prefixes_to_remove:
        company_text = re.sub(prefix, '', company_text, flags=re.IGNORECASE)
    
    # Remove job board artifacts
    artifacts_to_remove = [
        r'\s*\|\s*shine\.com.*$',
        r'\s*-\s*shine\.com.*$',
        r'\s*\(.*?\bverified\b.*?\)',
        r'\s*★.*$',  # Remove ratings
        r'\s*\d+\.\d+\s*$',  # Remove standalone ratings
    ]
    
    for artifact in artifacts_to_remove:
        company_text = re.sub(artifact, '', company_text, flags=re.IGNORECASE)
    
    # Clean whitespace
    company_text = ' '.join(company_text.split())
    company_text = company_text.strip()
    
    # Remove quotes if they wrap the entire name
    if company_text.startswith('"') and company_text.endswith('"'):
        company_text = company_text[1:-1]
    if company_text.startswith("'") and company_text.endswith("'"):
        company_text = company_text[1:-1]
    
    # Validate: must be at least 2 characters and not just numbers
    if len(company_text) < 2 or company_text.isdigit():
        return 'Not specified'
    
    return company_text

def extract_date_posted(card):
    date_posted = 'Not specified'
    
    # Strategy 1: Look for time-related class names
    date_selectors = [
        {'class': lambda x: x and 'time' in str(x).lower()},
        {'class': lambda x: x and 'date' in str(x).lower()},
        {'class': lambda x: x and 'posted' in str(x).lower()},
        {'class': lambda x: x and 'ago' in str(x).lower()},
        {'data-posted': True},
        {'itemprop': 'datePosted'}
    ]
    
    for selector in date_selectors:
        # Try different tags
        for tag in ['span', 'div', 'li', 'p', 'time']:
            elem = card.find(tag, selector)
            if elem:
                date_text = elem.get_text(strip=True)
                if date_text and len(date_text) > 1:
                    date_posted = clean_date_posted(date_text)
                    if date_posted != 'Not specified':
                        return date_posted
    
    # Strategy 2: Look for <time> tag
    time_tag = card.find('time')
    if time_tag:
        # Try datetime attribute first
        if time_tag.get('datetime'):
            date_posted = clean_date_posted(time_tag.get('datetime'))
            if date_posted != 'Not specified':
                return date_posted
        # Try text content
        date_text = time_tag.get_text(strip=True)
        if date_text:
            date_posted = clean_date_posted(date_text)
            if date_posted != 'Not specified':
                return date_posted
    
    # Strategy 3: Search for date patterns in entire card text
    card_text = card.get_text()
    date_patterns = [
        r'(posted|active|updated)?\s*:?\s*(\d+\s+(?:day|days|hour|hours|week|weeks|month|months)\s+ago)',
        r'(today|yesterday|just now)',
        r'(\d+[dDhHwWmM])\s+ago',
        r'posted\s+on\s+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
    ]
    
    for pattern in date_patterns:
        match = re.search(pattern, card_text, re.IGNORECASE)
        if match:
            # Get the captured date part (usually last group)
            date_str = match.group(match.lastindex) if match.lastindex > 1 else match.group(0)
            date_posted = clean_date_posted(date_str)
            if date_posted != 'Not specified':
                return date_posted
    
    # Strategy 4: Extract from structured data (JSON-LD)
    script_tags = card.find_all('script', {'type': 'application/ld+json'})
    for script in script_tags:
        try:
            data = json.loads(script.string)
            if isinstance(data, dict) and 'datePosted' in data:
                date_posted = clean_date_posted(data['datePosted'])
                if date_posted != 'Not specified':
                    return date_posted
        except:
            continue
    
    return date_posted

def clean_date_posted(date_text):
    if not date_text:
        return "Not specified"
    
    date_text = str(date_text).strip()
    
    # Remove common prefixes
    prefixes = [
        r'^(posted|active|updated|date|time|employer)\s*:?\s*',
        r'^\|\s*',
        r'^-\s*',
    ]
    for prefix in prefixes:
        date_text = re.sub(prefix, '', date_text, flags=re.IGNORECASE)
    
    date_text = date_text.strip()
    
    # Pattern 1: "X days/hours/weeks/months ago"
    pattern1 = r'(\d+)\s*(day|days|hour|hours|week|weeks|month|months)\s*ago'
    match1 = re.search(pattern1, date_text, re.IGNORECASE)
    if match1:
        num = match1.group(1)
        unit = match1.group(2).lower()
        # Normalize plural forms
        if unit == 'days':
            unit = 'day'
        elif unit == 'hours':
            unit = 'hour'
        elif unit == 'weeks':
            unit = 'week'
        elif unit == 'months':
            unit = 'month'
        
        if int(num) > 1 and not unit.endswith('s'):
            unit += 's'
        
        return f"{num} {unit} ago"
    
    if re.search(r'\btoday\b', date_text, re.IGNORECASE):
        return "Today"
    if re.search(r'\byesterday\b', date_text, re.IGNORECASE):
        return "Yesterday"
    if re.search(r'\bjust\s+now\b', date_text, re.IGNORECASE):
        return "Just now"
    
    pattern3 = r'(\d+)\s*([dDhHwWmM])'
    match3 = re.search(pattern3, date_text)
    if match3:
        num = match3.group(1)
        unit_abbr = match3.group(2).lower()
        
        unit_map = {
            'd': 'day',
            'h': 'hour',
            'w': 'week',
            'm': 'month'
        }
        unit = unit_map.get(unit_abbr, 'day')
        
        if int(num) > 1:
            unit += 's'
        
        return f"{num} {unit} ago"
    
    iso_pattern = r'(\d{4})-(\d{2})-(\d{2})'
    iso_match = re.search(iso_pattern, date_text)
    if iso_match:
        try:
            date_obj = datetime.strptime(iso_match.group(0), '%Y-%m-%d')
            today = datetime.now()
            diff = today - date_obj
            
            if diff.days == 0:
                return "Today"
            elif diff.days == 1:
                return "Yesterday"
            elif diff.days < 7:
                return f"{diff.days} days ago"
            elif diff.days < 30:
                weeks = diff.days // 7
                return f"{weeks} week{'s' if weeks > 1 else ''} ago"
            else:
                months = diff.days // 30
                return f"{months} month{'s' if months > 1 else ''} ago"
        except:
            pass
    
    if re.match(r'^\d+$', date_text):
        num = int(date_text)
        unit = 'day' if num == 1 else 'days'
        return f"{num} {unit} ago"
    
    date_pattern = r'(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})'
    date_match = re.search(date_pattern, date_text)
    if date_match:
        return date_text  
    if len(date_text) > 2 and len(date_text) < 100:
        cleaned = re.sub(r'[^\w\s]', '', date_text)
        if cleaned:
            return cleaned.strip()
    
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
        (r'(\d+)\s*(?:years?|yrs?|y)(?:\s+(?:of)?\s*(?:experience|exp))?', 
         lambda m: m.group(1)),
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

def extract_salary_text(text):
    """Extract and normalize salary data from given raw text."""
    if not text:
        return "Not Disclosed"
    
    text = text.strip()
    patterns = [
        r"₹?\s?[\d,\.]+(?:\s?[-–to]+\s?[\d,\.]+)?\s?(?:LPA|PA|per annum|lakhs|lakh|k|K|₹)?",
        r"\$[\d,\.]+(?:\s?[-–to]+\s?[\d,\.]+)?\s?(?:per year|per annum|yearly)?",
        r"[\d,\.]+\s?(?:to|[-–])\s?[\d,\.]+\s?(?:USD|INR|EUR|GBP|AED)?",
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(0)
    
    return text

def is_duplicate_job(job_link):
    """Check if job link has already been processed"""
    if not job_link:
        return True
    
    normalized_link = re.sub(r'\?.*$', '', job_link.strip().lower()).rstrip('/')
    if normalized_link in seen_job_links:
        return True
    
    seen_job_links.add(normalized_link)
    return False


def scrape_shine():
    
    all_jobs = []
    driver = create_stealth_driver()
    
    try:
        for role_idx, role in enumerate(job_roles):
            print(f"\n{'='*60}")
            print(f"Scraping role {role_idx + 1}/{len(job_roles)}: {role}")
            print(f"{'='*60}")
            
            # MODIFIED: Search for both remote AND contract
            query = requests.utils.quote(f"remote contract {role}")
            search_url = f"https://www.shine.com/job-search/{query}-jobs"
            
            print(f"Search URL: {search_url}")
            driver.get(search_url)
            time.sleep(random.uniform(8, 12))
            
            # Handle initial pop-ups
            try:
                close_buttons = driver.find_elements(By.CSS_SELECTOR, "button[aria-label='Close'], .closeBtn, [data-testid='modal-close']")
                for btn in close_buttons:
                    try:
                        btn.click()
                        time.sleep(1)
                    except:
                        continue
            except:
                pass
            
            page = 1
            max_pages = 300 
            role_jobs_count = 0
            consecutive_zero_pages = 0
            max_consecutive_zero = 5
            
            while page <= max_pages and consecutive_zero_pages < max_consecutive_zero:
                print(f"\n--- Page {page} ---")
                
                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='jobCard'], li[class*='job'], article, [data-job-id]"))
                    )
                    
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)
                    
                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    
                    job_cards = (
                        soup.find_all('div', {'class': lambda x: x and 'jobCard' in str(x)}) or
                        soup.find_all('li', {'class': lambda x: x and 'job' in str(x).lower()}) or
                        soup.find_all('article') or
                        soup.find_all('div', attrs={'data-job-id': True}) or
                        []
                    )
                    
                    print(f"Found {len(job_cards)} total job cards")
                    
                    if not job_cards:
                        print("No job cards found, stopping...")
                        consecutive_zero_pages += 1
                        break
                    
                    page_jobs = 0
                    
                    for card in job_cards:
                        try:
                            title_elem = (
                                card.find('h2') or card.find('h3') or 
                                card.find('a', {'class': lambda x: x and 'title' in str(x).lower()}) or
                                card.find('strong') or card.find('span', {'class': lambda x: x and 'title' in str(x).lower()})
                            )
                            job_title = title_elem.get_text(strip=True) if title_elem else ''
                            
                            if not job_title:
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
                                continue
                            
                            company_name = extract_company_name(card)
                            
                            card_text = card.get_text(strip=True)
                            
                            experience = extract_experience_enhanced(card_text)
                            
                            if not meets_all_criteria(job_title, company_name, card_text, experience):
                                continue
                            
                            salary = "Not Disclosed"
                            salary_elem = (
                                card.find('li', {'class': lambda x: x and 'salary' in str(x).lower()}) or
                                card.find('div', {'class': lambda x: x and 'salary' in str(x).lower()}) or
                                card.find('span', {'class': lambda x: x and 'salary' in str(x).lower()})
                            )
                            if salary_elem:
                                raw_salary = salary_elem.get_text(strip=True)
                                salary = extract_salary_text(raw_salary)
                            
                            date_posted = extract_date_posted(card)
                            
                            # MODIFIED: Since we're only getting remote+contract jobs, work_type is always "Remote + Contract"
                            work_type_str = "Remote + Contract"
                            
                            # Save job data
                            job_data = {
                                'job_title': job_title,
                                'company_name': company_name,
                                'job_link': job_link,
                                'experience': experience,
                                'salary': salary,
                                'date_posted': date_posted,
                                'work_type': work_type_str
                            }
                            
                            all_jobs.append(job_data)
                            role_jobs_count += 1
                            page_jobs += 1
                            
                            print(f"✓ QUALIFIED: {job_title[:50]}... | {company_name[:30]}... | {work_type_str}")
                            
                        except Exception as e:
                            continue
                    
                    print(f"Page {page}: Found {page_jobs} qualified jobs")
                    
                    if page_jobs == 0:
                        consecutive_zero_pages += 1
                        print(f"⚠ No qualified jobs on page {page}. Consecutive zero pages: {consecutive_zero_pages}")
                    else:
                        consecutive_zero_pages = 0 
                    if consecutive_zero_pages >= max_consecutive_zero:
                        print(f"Stopping {role} - {consecutive_zero_pages} consecutive pages with no qualified jobs")
                        break
                    
                    if page < max_pages:
                        try:
                            next_selectors = [
                                f"a[href*='page={page+1}']",
                                f"a[href*='-jobs-{page+1}']",
                                "a.pagination-next",
                                "button[aria-label*='next']",
                            ]
                            
                            next_found = False
                            for selector in next_selectors:
                                try:
                                    next_btn = driver.find_element(By.CSS_SELECTOR, selector)
                                    driver.execute_script("arguments[0].click();", next_btn)
                                    time.sleep(random.uniform(3, 5))
                                    next_found = True
                                    break
                                except:
                                    continue
                            
                            if not next_found:
                                current_url = driver.current_url
                                if 'page=' in current_url:
                                    next_url = current_url.replace(f'page={page}', f'page={page+1}')
                                else:
                                    separator = '&' if '?' in current_url else '?'
                                    next_url = f"{current_url}{separator}page={page+1}"
                                
                                driver.get(next_url)
                                time.sleep(random.uniform(3, 5))
                            
                            page += 1
                            
                        except Exception as e:
                            print(f"Could not navigate to page {page+1}: {e}")
                            break
                    else:
                        break
                        
                except TimeoutException:
                    print(f"Timeout on page {page}, stopping...")
                    consecutive_zero_pages += 1
                    break
                except Exception as e:
                    print(f"Error on page {page}: {e}")
                    consecutive_zero_pages += 1
                    break
            
            print(f"Finished {role}: {role_jobs_count} qualified jobs")
            
            # If we stopped due to consecutive zero pages, print message
            if consecutive_zero_pages >= max_consecutive_zero:
                print(f"Moving to next role after {consecutive_zero_pages} consecutive pages with no qualified jobs")
            
            time.sleep(random.uniform(5,8))
            
    except Exception as e:
        print(f"Error scraping Shine: {e}")
    finally:
        driver.quit()
        print(f" Total collected {len(all_jobs)} qualified Remote+Contract jobs from Shine")
    
    return all_jobs

def save_to_csv(jobs, filename):
    if not jobs:
        print(f"No jobs to save for {filename}")
        return
    
    filepath = os.path.join(Data_dir, filename)
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['job_title', 'company_name', 'job_link', 'experience', 'salary', 'date_posted', 'work_type']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(jobs)
        print(f"Successfully saved {len(jobs)} jobs to {filepath}")
    except Exception as e:
        print(f"Error saving to CSV: {e}")

def main():
    global seen_job_links
    seen_job_links = set()
    
    start_time = time.time()
    
    # Scrape Shine
    shine_jobs = scrape_shine()
    save_to_csv(shine_jobs, 'remote_contract_software_jobs.csv')
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"\nScraping completed in {duration:.2f} seconds!")
    print(f"Total qualified jobs: {len(shine_jobs)}")
    print(f"Jobs saved to: {Data_dir}")

if __name__ == "__main__":
    main()