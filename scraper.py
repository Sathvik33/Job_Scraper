import os
import time
import random
import pandas as pd
from urllib.parse import quote, urljoin
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import undetected_chromedriver as uc

# Define paths using os.path.join
BASE_DIR = r'C:\Sathvik-py\Talrn\job_scraper'
DATA_DIR = os.path.join(BASE_DIR, 'Data')
OUTPUT_FILE = os.path.join(DATA_DIR, 'jobs_raw_data.csv')

# Create Data directory if it doesn't exist
os.makedirs(DATA_DIR, exist_ok=True)

PAGES_TO_SCRAPE = 3
SEARCH_QUERIES = [
    "remote python developer", "contract software engineer", "freelance web developer",
    "remote devops engineer", "software engineer", "software developer",
    "full stack developer", "frontend developer", "backend developer",
    "data scientist", "machine learning engineer", "ai engineer",
    "Machine Learning Internship", "Software Engineer Internship", "Data Science Internship",
    "DevOps Internship", "Web Development Internship", "Cloud Computing Internship",
    "Cybersecurity Internship", "Mobile App Development Internship", "UI/UX Design Internship",
    "Artificial Intelligence Internship", "Blockchain Internship",
    "Artificial Intelligence and Machine Learning Internship", "Data Analytics Internship",
    "Big Data Internship", "Full Stack Development Internship", "Software Development Internship",
    "Front End Development Internship", "Back End Development Internship",
]

SITE_CONFIG = {
    'indeed': {
        'url_template': 'https://in.indeed.com/jobs?q={query}',
        'selectors': {
            'card': 'div.job_seen_beacon',
            'title': 'h2.jobTitle > a > span',
            'company': 'span.companyName',
            'location': 'div.companyLocation',
            'link': 'h2.jobTitle > a',
            'date': 'span.date',
            'next_button': 'a[data-testid="pagination-page-next"]'
        }
    },
    'naukri': {
        'url_template': 'https://www.naukri.com/{query}-jobs',
        'selectors': {
            'card': 'div.srp-jobtuple-wrapper',
            'title': 'a.title',
            'company': 'a.comp-name',
            'location': 'span.loc-text',
            'link': 'a.title',
            'date': 'span.fleft.postedDate',
            'next_button': 'a.fright.fs12.btn.brd-rd2.active'
        }
    }
}

class HeuristicLayoutDetector:
    def predict_element_type(self, element):
        text = element.get_text(strip=True).lower()
        job_indicators = ['developer', 'engineer', 'analyst', 'scientist', 'architect', 'programmer', 'intern', 'freelance', 'contract']
        score = 0
        if any(indicator in text for indicator in job_indicators): score += 2
        if 25 < len(text) < 400: score += 1
        if element.find('a', href=True): score += 1
        return score >= 3

class AntiDetectionManager:
    def __init__(self):
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        ]

    def human_like_delay(self, min_delay=3, max_delay=7):
        time.sleep(random.uniform(min_delay, max_delay))

    def human_like_scroll(self, driver):
        scroll_points = sorted([random.random() for _ in range(random.randint(2, 4))])
        for point in scroll_points:
            driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * {point});")
            time.sleep(random.uniform(0.5, 1.5))

    def rotate_user_agent(self, options):
        new_agent = random.choice(self.user_agents)
        options.add_argument(f'--user-agent={new_agent}')

    def random_mouse_movements(self, driver):
        action = ActionChains(driver)
        for _ in range(random.randint(2, 5)):
            try:
                x_offset, y_offset = random.randint(-100, 100), random.randint(-100, 100)
                action.move_by_offset(x_offset, y_offset).perform()
                time.sleep(random.uniform(0.1, 0.3))
            except:
                pass

def get_element_text(parent, selector):
    element = parent.select_one(selector)
    return element.get_text(strip=True) if element else "Not specified"

def get_element_href(parent, selector, base_url):
    element = parent.select_one(selector)
    if element and element.has_attr('href'):
        return urljoin(base_url, element['href'])
    return "Not specified"

def extract_job_data(soup, layout_detector, config, query):
    job_data = []
    query_lower = query.lower()
    job_type_from_query = "Contract" if "contract" in query_lower or "freelance" in query_lower else "Remote" if "remote" in query_lower else "Internship" if "intern" in query_lower else "Not specified"
    job_cards = soup.select(config['selectors']['card'])
    source = "CSS Selectors"
    if not job_cards:
        print("CSS selectors failed. Falling back to heuristic detection...")
        job_cards = [elem for elem in soup.find_all(['div', 'li', 'article']) if layout_detector.predict_element_type(elem)]
        source = "Heuristic Detection"
    for card in job_cards:
        if source == "CSS Selectors":
            title = get_element_text(card, config['selectors']['title'])
            company = get_element_text(card, config['selectors']['company'])
            location = get_element_text(card, config['selectors']['location'])
            link = get_element_href(card, config['selectors']['link'], config['base_url'])
            date = get_element_text(card, config['selectors']['date'])
        else:
            title = get_element_text(card, 'h2, h3, a[href]')
            company = get_element_text(card, 'span[class*="company"], a[class*="company"]')
            location = get_element_text(card, 'div[class*="location"], span[class*="location"]')
            link = get_element_href(card, 'a[href]', config['base_url'])
            date = get_element_text(card, 'span[class*="date"], time')
        if title != "Not specified" and link != "Not specified":
            job_data.append({"Job Title": title, "Company Name": company, "Location": location, "Job Type": job_type_from_query, "Posted Date": date, "Job Link": link, "Source": source})
    return job_data

def run_scraper():
    print("Initializing enhanced scraper...")
    layout_detector = HeuristicLayoutDetector()
    anti_detection = AntiDetectionManager()
    all_jobs_data = []
    chromedriver_path = os.path.join(BASE_DIR, 'chromedriver-win64', 'chromedriver.exe')
    print(f"Using Chromedriver from path: {chromedriver_path}")

    for site, config in SITE_CONFIG.items():
        print(f"\n--- Starting new session for site: {site.title()} ---")
        options = uc.ChromeOptions()
        anti_detection.rotate_user_agent(options)
        try:
            driver = uc.Chrome(driver_executable_path=chromedriver_path, options=options)
            wait = WebDriverWait(driver, 15)
        except Exception as e:
            print(f"Failed to initialize Chrome driver for {site.title()}: {e}")
            continue

        for query in SEARCH_QUERIES:
            print(f"--- Scraping '{query}' on {site.title()} ---")
            
            search_url = config['url_template'].format(query=quote(query) if site == 'indeed' else query.replace(' ', '-'))
            try:
                driver.get(search_url)
                anti_detection.human_like_delay()
            except Exception as e:
                print(f"Failed to load URL for query '{query}'. Moving on. Reason: {e}")
                continue # Move to the next query if the page can't load

            for page_num in range(PAGES_TO_SCRAPE):
                print(f"Scraping page {page_num + 1}...")
                try: # ** CORRECTED LOGIC: Exception handling is INSIDE the page loop **
                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'body')))
                    anti_detection.human_like_scroll(driver)
                    soup = BeautifulSoup(driver.page_source, "html.parser")
                    job_data_from_page = extract_job_data(soup, layout_detector, config, query)
                    if not job_data_from_page:
                        print("No job data found on this page. Ending pagination for this query.")
                        break
                    all_jobs_data.extend(job_data_from_page)
                    print(f"Extracted {len(job_data_from_page)} jobs from page {page_num + 1}")
                    next_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, config['selectors']['next_button'])))
                    driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                    time.sleep(1)
                    driver.execute_script("arguments[0].click();", next_button)
                    anti_detection.human_like_delay()
                except Exception as e:
                    print(f"Could not process page {page_num + 1} or find next button. Ending pagination for this query. Reason: {e}")
                    break # Break out of the page loop and move to the next query

        driver.quit()
        print(f"--- Session for {site.title()} complete. Pausing before next site. ---")
        time.sleep(random.uniform(15, 30))

    if not all_jobs_data:
        print("No data was collected.")
        return

    df = pd.DataFrame(all_jobs_data)
    df.drop_duplicates(subset=['Job Link'], keep='first', inplace=True)
    df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    print(f"\nScraping complete. {len(df)} unique job listings saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    run_scraper()