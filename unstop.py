# unstop.py
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

filename = "unstop_jobs.csv"
full_output_path = os.path.join(Data_dir, filename)

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def parse_jobs_soup(soup):
    jobs = []
    # Unstop job cards - these are common class names
    job_cards = soup.find_all('div', class_='opportunity-card')
    if not job_cards:
        job_cards = soup.find_all('div', class_='job-card')
    if not job_cards:
        job_cards = soup.find_all('div', class_='listing-card')
    if not job_cards:
        job_cards = soup.find_all('div', class_='card')
    
    print(f"Found {len(job_cards)} job cards")
    
    for job_card in job_cards:
        try:
            # Job Title
            title_elem = job_card.find(['h3', 'h2', 'h4'])
            if not title_elem:
                title_elem = job_card.find('div', class_='title')
            job_title = title_elem.get_text(strip=True) if title_elem else 'N/A'
            
            # Company Name
            company_elem = job_card.find('div', class_='company-name')
            if not company_elem:
                company_elem = job_card.find('span', class_='company-name')
            if not company_elem:
                company_elem = job_card.find('div', class_='organization')
            company_name = company_elem.get_text(strip=True) if company_elem else 'N/A'
            
            # Location
            location_elem = job_card.find('div', class_='location')
            if not location_elem:
                location_elem = job_card.find('span', class_='location')
            location = location_elem.get_text(strip=True) if location_elem else 'N/A'
            
            # Salary/Stipend
            salary_elem = job_card.find('div', class_='stipend')
            if not salary_elem:
                salary_elem = job_card.find('div', class_='salary')
            if not salary_elem:
                salary_elem = job_card.find('span', class_='stipend-amount')
            salary = salary_elem.get_text(strip=True) if salary_elem else 'Not Disclosed'
            
            # Deadline
            deadline_elem = job_card.find('div', class_='application-deadline')
            if not deadline_elem:
                deadline_elem = job_card.find('span', class_='deadline')
            deadline = deadline_elem.get_text(strip=True) if deadline_elem else 'N/A'
            
            # Job Link
            job_link_elem = job_card.find('a', href=True)
            job_link = job_link_elem['href'] if job_link_elem else 'N/A'
            if job_link and not job_link.startswith('http'):
                job_link = "https://unstop.com" + job_link
            
            # Experience (if available)
            experience_elem = job_card.find('div', class_='experience')
            if not experience_elem:
                experience_elem = job_card.find('span', class_='experience')
            experience = experience_elem.get_text(strip=True) if experience_elem else 'N/A'
            
            if job_title and job_title != 'N/A':
                jobs.append({
                    'job_title': job_title,
                    'company_name': company_name,
                    'location': location,
                    'salary': salary,
                    'deadline': deadline,
                    'experience': experience,
                    'job_link': job_link
                })
                print(f"  - {job_title} at {company_name}")
                
        except Exception as e:
            print(f"Error parsing job card: {e}")
            continue
    
    return jobs if jobs else None

def get_next_page_url_soup(soup, current_page_num):
    # Unstop pagination
    next_link = soup.find('a', class_='next-page')
    if not next_link:
        next_link = soup.find('a', class_='pagination-next')
    if not next_link:
        next_link = soup.find('a', string='Next')
    
    if next_link and next_link.has_attr('href'):
        href = next_link['href']
        if not href.startswith('http'):
            return "https://unstop.com" + href
        return href
    
    # Try page number based navigation
    next_page = soup.find('a', href=lambda x: x and f'page={current_page_num + 1}' in x)
    if next_page:
        href = next_page['href']
        if not href.startswith('http'):
            return "https://unstop.com" + href
        return href
    
    return None

def scrape_with_requests(base_url, max_retries=3):
    all_jobs = []
    current_page_num = 1
    current_url = base_url
    retry_count = 0

    max_pages = 3
    max_iter = max_pages

    with tqdm(total=max_iter, desc='Scraping pages (requests)') as pbar:
        while current_page_num <= max_iter:
            print(f"\n=== Page {current_page_num} ===")
            try:
                res = requests.get(current_url, headers=headers, timeout=10)
                res.raise_for_status()
                soup = BeautifulSoup(res.text, 'html.parser')
                
                jobs = parse_jobs_soup(soup)
                if not jobs:
                    print(f"Warning: No jobs parsed on page {current_page_num}")
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
                print(f"Successfully parsed {len(jobs)} jobs on page {current_page_num}")
                
                next_url = get_next_page_url_soup(soup, current_page_num)
                if not next_url:
                    print("No next page found, ending pagination.")
                    break
                
                current_url = next_url
                current_page_num += 1
                pbar.update(1)
                time.sleep(2)
                
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
    options.headless = False  # Set to True to run in background
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    driver = webdriver.Chrome(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    try:
        print(f"Loading page: {base_url}")
        driver.get(base_url)
        time.sleep(5)  # Wait longer for Unstop to load

        current_page_num = 1
        max_pages = 3

        with tqdm(total=max_pages, desc='Scraping pages (selenium)') as pbar:
            while current_page_num <= max_pages:
                print(f"\n=== Processing page {current_page_num} with Selenium ===")
                
                # Wait for job elements to load
                try:
                    wait = WebDriverWait(driver, 15)
                    # Try multiple selectors for Unstop
                    selectors = [
                        "div.opportunity-card",
                        "div.job-card", 
                        "div.listing-card",
                        "div.card"
                    ]
                    
                    job_elements = None
                    for selector in selectors:
                        try:
                            job_elements = wait.until(
                                EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector))
                            )
                            if job_elements:
                                print(f"Found {len(job_elements)} job elements using {selector}")
                                break
                        except TimeoutException:
                            continue
                    
                    if not job_elements:
                        print("No job listings found within timeout period")
                        break
                        
                except TimeoutException:
                    print("No job listings found within timeout period")
                    break
                
                # Parse the page source
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                jobs = parse_jobs_soup(soup)
                
                if not jobs:
                    print(f"Warning: No jobs parsed on selenium page {current_page_num}")
                    break
                
                all_jobs.extend(jobs)
                print(f"Successfully parsed {len(jobs)} jobs from page {current_page_num}")

                # Try to click next button
                try:
                    next_selectors = [
                        "a.next-page",
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
                            
                            if next_button.is_enabled():
                                print("Clicking next button...")
                                next_button.click()
                                time.sleep(4)  # Wait for page load
                                next_found = True
                                break
                                
                        except Exception as e:
                            continue
                    
                    if not next_found:
                        print("No next button found or reached last page")
                        break
                    
                except Exception as e:
                    print(f"Error clicking next button: {e}")
                    break
                
                current_page_num += 1
                pbar.update(1)

    except Exception as e:
        print(f"Selenium error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        driver.quit()

    return all_jobs

def save_to_csv(job_list, filename):
    if not job_list:
        print("No jobs to save")
        return
        
    keys = job_list[0].keys() if job_list else []
    with open(filename, 'w', newline='', encoding='utf-8') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(job_list)
    print(f"Saved {len(job_list)} jobs to {filename}")

if __name__ == "__main__":
    base_url = "https://unstop.com/jobs?searchTerm=Software%20Engineer"
    
    print("Starting Unstop scraper...")
    try:
        job_data = scrape_with_requests(base_url)
        if not job_data:
            print("Requests returned no data, trying Selenium...")
            job_data = scrape_with_selenium(base_url)
    except RuntimeError as e:
        print(f"Requests scraping failed: {e}. Switching to Selenium.")
        job_data = scrape_with_selenium(base_url)
    except Exception as e:
        print(f"Unexpected error: {e}")
        job_data = []
    
    if job_data:
        save_to_csv(job_data, full_output_path)
        print(f"Successfully saved {len(job_data)} jobs to {full_output_path}")
    else:
        print("Failed to scrape any jobs")