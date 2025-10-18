# shine.py
import requests
from bs4 import BeautifulSoup
import csv
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import ElementClickInterceptedException, TimeoutException, StaleElementReferenceException
from tqdm import tqdm
import os

# --- Define Base and Data Directories (More Robust) ---
try:
    Base_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    Base_dir = os.getcwd()

Data_dir = os.path.join(Base_dir, 'Data')
print(f"Ensuring data directory exists at: {Data_dir}")
os.makedirs(Data_dir, exist_ok=True)

filename = "shine_jobs.csv"
full_output_path = os.path.join(Data_dir, filename)

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def parse_jobs_soup(soup):
    jobs = []
    # Multiple possible selectors for job cards on Shine
    job_cards = soup.find_all('li', class_='search_list_row')
    if not job_cards:
        job_cards = soup.find_all('div', class_='search_list_row')
    if not job_cards:
        job_cards = soup.find_all('div', class_='jobCard')
    
    print(f"Found {len(job_cards)} job cards")
    
    for job_card in job_cards:
        # Try multiple selectors for each field
        title_selectors = [
            job_card.find('h2'),
            job_card.find('h3'),
            job_card.find('a', class_='job_title_anchor'),
            job_card.find('span', class_='jobTitle')
        ]
        job_title = ''
        for selector in title_selectors:
            if selector:
                job_title = selector.get_text(strip=True)
                if job_title:
                    break
        
        company_selectors = [
            job_card.find('li', class_='snp_cnm'),
            job_card.find('span', class_='company_name'),
            job_card.find('div', class_='companyName')
        ]
        company_name = ''
        for selector in company_selectors:
            if selector:
                company_name = selector.get_text(strip=True)
                if company_name:
                    break
        
        experience_selectors = [
            job_card.find('li', class_='snp_yoe'),
            job_card.find('span', class_='experience'),
            job_card.find('div', class_='exp')
        ]
        experience = ''
        for selector in experience_selectors:
            if selector:
                experience = selector.get_text(strip=True)
                if experience:
                    break
        
        location_selectors = [
            job_card.find('li', class_='snp_loc'),
            job_card.find('span', class_='loc'),
            job_card.find('div', class_='location')
        ]
        location = ''
        for selector in location_selectors:
            if selector:
                location = selector.get_text(strip=True)
                if location:
                    break
        
        salary_selectors = [
            job_card.find('li', class_='snp_sal'),
            job_card.find('span', class_='salary'),
            job_card.find('div', class_='salary_package')
        ]
        salary = 'Not Disclosed'
        for selector in salary_selectors:
            if selector:
                salary_text = selector.get_text(strip=True)
                if salary_text and salary_text.lower() != 'not disclosed':
                    salary = salary_text
                    break
        
        # Job link
        job_link_tag = job_card.find('a', href=True)
        job_link = job_link_tag['href'] if job_link_tag else ''
        if job_link and not job_link.startswith('http'):
            job_link = "https://www.shine.com" + job_link
        
        # Date posted
        date_selectors = [
            job_card.find('li', class_='snp_date'),
            job_card.find('span', class_='date'),
            job_card.find('div', class_='posted_date')
        ]
        date_posted = ''
        for selector in date_selectors:
            if selector:
                date_posted = selector.get_text(strip=True)
                if date_posted:
                    break
        
        if job_title:  # Only add if we found at least a job title
            jobs.append({
                'job title': job_title,
                'company name': company_name,
                'experience': experience,
                'location': location,
                'job link': job_link,
                'salary': salary,
                'date posted': date_posted
            })
    
    return jobs if jobs else None

def get_next_page_url_soup(soup, current_page_num):
    # Try multiple selectors for next page
    next_selectors = [
        soup.find('a', class_='next'),
        soup.find('a', class_='pagination-next'),
        soup.find('a', string='Next'),
        soup.find('a', string=lambda x: x and 'next' in x.lower())
    ]
    
    for next_link in next_selectors:
        if next_link and next_link.has_attr('href'):
            href = next_link['href']
            if not href.startswith('http'):
                return "https://www.shine.com" + href
            return href
    
    return None

def scrape_with_requests(base_url, max_retries=3):
    all_jobs = []
    current_page_num = 1
    current_url = base_url
    retry_count = 0

    max_pages = 5  # Reduced for testing
    max_iter = max_pages if max_pages else 10

    with tqdm(total=max_iter, desc='Scraping pages (requests)') as pbar:
        while current_page_num <= max_iter:
            print(f"Requesting page {current_page_num} using requests...")
            try:
                res = requests.get(current_url, headers=headers, timeout=10)
                res.raise_for_status()
                soup = BeautifulSoup(res.text, 'html.parser')
                
                # Debug: Save HTML for inspection
                if current_page_num == 1:
                    with open('debug_page1.html', 'w', encoding='utf-8') as f:
                        f.write(soup.prettify())
                
                jobs = parse_jobs_soup(soup)
                if not jobs:
                    print(f"Warning: No jobs found on requests page {current_page_num}")
                    print(f"URL: {current_url}")
                    print(f"Status Code: {res.status_code}")
                    retry_count += 1
                    if retry_count >= max_retries:
                        print(f"Max retries reached on page {current_page_num} with requests.")
                        raise RuntimeError("Repeated zero job results with requests.")
                    else:
                        print("Retrying after delay...")
                        time.sleep(5)
                        continue
                
                retry_count = 0
                all_jobs.extend(jobs)
                print(f"Found {len(jobs)} jobs on page {current_page_num}")
                
                next_url = get_next_page_url_soup(soup, current_page_num)
                if not next_url:
                    print("No next page found, ending pagination.")
                    break
                
                current_url = next_url
                current_page_num += 1
                pbar.update(1)
                time.sleep(2)  # Reduced delay
                
            except requests.RequestException as e:
                print(f"Request failed: {e}")
                retry_count += 1
                if retry_count >= max_retries:
                    raise RuntimeError(f"Request failed: {e}")
                time.sleep(5)
                continue

    return all_jobs

def scrape_with_selenium(base_url):
    all_jobs = []
    options = Options()
    options.headless = True
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    driver = webdriver.Chrome(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    try:
        driver.get(base_url)
        print(f"Current URL: {driver.current_url}")

        current_page_num = 1
        max_pages = 5  # Reduced for testing

        with tqdm(total=max_pages, desc='Scraping pages (selenium)') as pbar:
            while current_page_num <= max_pages:
                print(f"Processing page {current_page_num} with Selenium...")
                
                # Wait for job elements with multiple possible selectors
                selectors_to_try = [
                    "li.search_list_row",
                    "div.search_list_row", 
                    "div.jobCard",
                    ".job-tuple"
                ]
                
                job_elements = None
                for selector in selectors_to_try:
                    try:
                        wait = WebDriverWait(driver, 10)
                        job_elements = wait.until(
                            EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector))
                        )
                        if job_elements:
                            print(f"Found {len(job_elements)} jobs using selector: {selector}")
                            break
                    except TimeoutException:
                        continue
                
                if not job_elements:
                    print(f"No job listings found on page {current_page_num}")
                    # Save page source for debugging
                    with open('debug_selenium_page.html', 'w', encoding='utf-8') as f:
                        f.write(driver.page_source)
                    break
                
                time.sleep(2)
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                jobs = parse_jobs_soup(soup)
                
                if not jobs:
                    print(f"Warning: No jobs parsed on selenium page {current_page_num}")
                    break
                
                all_jobs.extend(jobs)
                print(f"Parsed {len(jobs)} jobs from page {current_page_num}")

                # Try to find and click next button
                next_selectors = [
                    "a.next",
                    ".pagination-next a",
                    "a[class*='next']",
                    "//a[contains(text(), 'Next')]"
                ]
                
                next_found = False
                for selector in next_selectors:
                    try:
                        if selector.startswith('//'):
                            next_button = driver.find_element(By.XPATH, selector)
                        else:
                            next_button = driver.find_element(By.CSS_SELECTOR, selector)
                        
                        driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                        time.sleep(1)
                        
                        # Check if button is enabled
                        if next_button.is_enabled():
                            next_button.click()
                            time.sleep(3)  # Wait for page load
                            next_found = True
                            break
                            
                    except Exception as e:
                        continue
                
                if not next_found:
                    print("No next button found or reached last page")
                    break
                
                current_page_num += 1
                pbar.update(1)

    except Exception as e:
        print(f"Selenium error: {e}")
    finally:
        driver.quit()

    return all_jobs

def save_to_csv(job_list, filename):
    if not job_list:
        print("No jobs to save")
        return
        
    keys = job_list[0].keys()
    with open(filename, 'w', newline='', encoding='utf-8') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(job_list)

if __name__ == "__main__":
    # Try multiple possible URL patterns for Shine
    base_urls = [
        "https://www.shine.com/job-search/python-developer-jobs-in-hyderabad",
        "https://www.shine.com/job-search/software-developer-jobs",
        "https://www.shine.com/job-search/it-jobs"
    ]
    
    all_job_data = []
    
    for base_url in base_urls:
        print(f"\nTrying URL: {base_url}")
        try:
            job_data = scrape_with_requests(base_url)
            if job_data:
                all_job_data.extend(job_data)
                print(f"Successfully scraped {len(job_data)} jobs from {base_url}")
                break  # Stop if successful
            else:
                print(f"No jobs found from {base_url}")
        except Exception as e:
            print(f"Requests failed for {base_url}: {e}")
    
    # If requests failed for all URLs, try Selenium
    if not all_job_data:
        print("\nTrying Selenium as fallback...")
        for base_url in base_urls:
            print(f"Trying Selenium with URL: {base_url}")
            job_data = scrape_with_selenium(base_url)
            if job_data:
                all_job_data.extend(job_data)
                print(f"Successfully scraped {len(job_data)} jobs using Selenium")
                break
    
    if all_job_data:
        save_to_csv(all_job_data, full_output_path)
        print(f"Saved {len(all_job_data)} jobs to {full_output_path}")
    else:
        print("Failed to scrape any jobs from all attempted URLs")