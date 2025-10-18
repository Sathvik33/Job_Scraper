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
    # This gets the directory where the .py script is located
    Base_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Fallback for interactive environments (like notebooks) where __file__ isn't defined
    Base_dir = os.getcwd()

Data_dir = os.path.join(Base_dir, 'Data')

# --- Create the Data directory if it doesn't exist ---
print(f"Ensuring data directory exists at: {Data_dir}")
os.makedirs(Data_dir, exist_ok=True)

filename = "naukri_jobs.csv"
full_output_path = os.path.join(Data_dir, filename)

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
}


def parse_jobs_soup(soup):
    jobs = []
    job_divs = soup.find_all('div', class_='cust-job-tuple')
    if not job_divs:
        return None
    for job_div in job_divs:
        title_tag = job_div.find('h2').find('a', class_='title')
        job_title = title_tag.get_text(strip=True) if title_tag else ''
        job_link = title_tag['href'] if title_tag else ''
        company_tag = job_div.find('a', class_='comp-name')
        company_name = company_tag.get_text(strip=True) if company_tag else ''
        exp_span = job_div.find('span', class_='expwdth')
        experience = exp_span.get_text(strip=True) if exp_span else ''

        # Improved salary extraction with title attribute
        salary_span = job_div.find('span', class_='')
        if salary_span and salary_span.has_attr('title'):
            salary = salary_span['title'].strip()
        else:
            # fallback to normal text extraction
            salary_span_alt = job_div.find('span', class_='salary')
            salary = salary_span_alt.get_text(strip=True) if salary_span_alt else 'Not Disclosed'

        date_posted_span = job_div.find('span', class_='job-post-day')
        date_posted = date_posted_span.get_text(strip=True) if date_posted_span else ''

        jobs.append({
            'job title': job_title,
            'company name': company_name,
            'job link': job_link,
            'experience': experience,
            'salary': salary,
            'date posted': date_posted
        })
    return jobs


def get_next_page_url_soup(soup, current_page_num):
    next_num = current_page_num + 1
    next_page = soup.find('a', href=lambda x: x and f'-{next_num}' in x)
    if next_page:
        return "https://www.naukri.com" + next_page['href']
    next_span = soup.find('span', string="Next")
    if next_span:
        parent = next_span.parent
        if parent.name == 'a' and 'href' in parent.attrs:
            return "https://www.naukri.com" + parent['href']
    return None


def scrape_with_requests(base_url, max_retries=3):
    all_jobs = []
    current_page_num = 1
    current_url = base_url
    retry_count = 0

    max_pages = None
    max_iter = max_pages if max_pages else 20

    with tqdm(total=max_iter, desc='Scraping pages (requests)') as pbar:
        while True:
            print(f"Requesting page {current_page_num} using requests...")
            res = requests.get(current_url, headers=headers)
            soup = BeautifulSoup(res.text, 'html.parser')
            jobs = parse_jobs_soup(soup)
            if not jobs:
                print(f"Warning: No jobs found on requests page {current_page_num}")
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
            next_url = get_next_page_url_soup(soup, current_page_num)
            if not next_url:
                break
            current_url = next_url
            current_page_num += 1
            pbar.update(1)
            time.sleep(3)
            if max_pages and current_page_num > max_pages:
                break
    return all_jobs


def scrape_with_selenium(base_url):
    all_jobs = []
    options = Options()
    options.headless = True
    # options.add_argument("--headless=new") # Use this if the line above gives a warning
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(options=options)
    driver.get(base_url)

    current_page_num = 1
    max_pages = None
    max_iter = max_pages if max_pages else 20

    with tqdm(total=max_iter, desc='Scraping pages (selenium)') as pbar:
        while True:
            wait = WebDriverWait(driver, 10)
            try:
                # Wait for the job list container to be present before parsing
                wait.until(EC.presence_of_element_located((By.CLASS_NAME, "cust-job-tuple")))
            except TimeoutException:
                print(f"No job listings found on page {current_page_num}, stopping.")
                break
            
            time.sleep(1) # Give a brief moment for JS to settle
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            jobs = parse_jobs_soup(soup)
            if not jobs:
                print(f"Warning: No jobs parsed on selenium page {current_page_num}, stopping.")
                break
            all_jobs.extend(jobs)

            try:
                # --- MODIFIED LOGIC TO PREVENT STALE ELEMENT ---

                # 1. First, wait for any potential popups (like login prompts) to disappear.
                try:
                    short_wait = WebDriverWait(driver, 2) # Shorter wait for optional element
                    short_wait.until(
                        EC.invisibility_of_element_located((By.CSS_SELECTOR, ".styles_ppContainer__eeZyG"))
                    )
                except TimeoutException:
                    # This is OK, it just means the popup wasn't there or didn't disappear
                    pass 

                # 2. NOW, find the 'Next' button. We wait for it to be clickable.
                #    This gets a *fresh* reference to the button *after* popups are handled.
                next_button = wait.until(
                    EC.element_to_be_clickable((By.XPATH, "//span[text()='Next']/parent::a"))
                )

                # 3. Scroll to the button and click it.
                driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                time.sleep(0.5) # Short pause after scroll

                try:
                    next_button.click()
                except ElementClickInterceptedException:
                    # Fallback to JavaScript click if something is covering it
                    driver.execute_script("arguments[0].click();", next_button)
                except StaleElementReferenceException:
                    # Failsafe: If it's *still* stale, re-find and click one last time
                    print("Caught StaleElementReferenceException, retrying click...")
                    next_button = wait.until(
                        EC.element_to_be_clickable((By.XPATH, "//span[text()='Next']/parent::a"))
                    )
                    driver.execute_script("arguments[0].click();", next_button)


                # Wait for the page to transition by waiting for the old button to go stale
                try:
                    wait.until(EC.staleness_of(next_button))
                except TimeoutException:
                    # If it doesn't go stale, just sleep
                    time.sleep(3) 

                current_page_num += 1
                pbar.update(1)
                if max_pages and current_page_num > max_pages:
                    break
            except TimeoutException:
                print("No clickable next button found, ending pagination.")
                break

    driver.quit()
    return all_jobs


def save_to_csv(job_list, filename):
    keys = job_list[0].keys() if job_list else []
    with open(filename, 'w', newline='', encoding='utf-8') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(job_list)


if __name__ == "__main__":
    base_url = "https://www.naukri.com/software-contract-jobs-in-india?k=software%2C%20contract&l=india&nignbevent_src=jobsearchDeskGNB&experience=10&wfhType=2&ctcFilter=15to25&ctcFilter=25to50&ctcFilter=50to75&ctcFilter=75to100"
    try:
        job_data = scrape_with_requests(base_url)
    except RuntimeError as e:
        print(f"Requests scraping failed: {e}. Switching to Selenium.")
        job_data = scrape_with_selenium(base_url)
    
    if job_data:
        # 'full_output_path' is now used here, and the directory is created at the top
        save_to_csv(job_data, full_output_path)
        print(f"Saved {len(job_data)} jobs to {full_output_path}")
    else:
        print("Failed to scrape jobs.")