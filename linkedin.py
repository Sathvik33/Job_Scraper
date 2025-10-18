from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import csv, time
import os

Base_dir = os.path.dirname(os.path.abspath(__file__))
Data_dir = os.path.join(Base_dir, 'Data')
os.makedirs(Data_dir, exist_ok=True)
output_path = os.path.join(Data_dir, "linkedin_jobs.csv")

# --- Keywords ---
KEYWORDS = [
    "Software Developer", "Java Developer", "Frontend Engineer",
    "Full Stack Developer", "Python Developer", "DevOps Engineer",
    "QA Engineer", "AI Developer", "ML Engineer",
    "React Developer", "Angular Developer", "Cloud Engineer",
    "C++ Developer", "SAP Developer"
]
# --- Setup ---
driver = webdriver.Chrome()
wait = WebDriverWait(driver, 15)
# --- CSV setup ---
csv_file = open(output_path, "w", newline="", encoding="utf-8")
writer = csv.writer(csv_file)
writer.writerow([
    "Keyword", "Job Title", "Company", "Location",
    "Posted Date", "Job Link", "Hiring Manager"
])
seen_links = set()
# --- Loop through keywords ---
for keyword in KEYWORDS:
    print(f"\n:mag: Searching jobs for: {keyword}...")
    url = (
        "https://www.linkedin.com/jobs/search/"
        f"?keywords={keyword.replace(' ', '%20')}"
        "&location=India"
        "&geoId=102713980"
        "&f_E=2"      # Experience: 2+ years
        "&f_WT=2"     # Remote jobs
        "&f_JT=C"     # Contract only
    )
    driver.get(url)
    time.sleep(3)
    page = 1
    while True:
        print(f" :page_facing_up: Scraping Page {page} for {keyword}...")
        # Scroll to load more jobs
        for _ in range(3):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
        # Job cards
        try:
            job_cards = wait.until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.base-card"))
            )
        except:
            print(f" :x: No job cards found on page {page} for {keyword}")
            break
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
                try:
                    company = card.find_element(By.CSS_SELECTOR, "h4.base-search-card__subtitle").text.strip()
                except:
                    company = "N/A"
            # Location
            try:
                location = card.find_element(By.CSS_SELECTOR, "span.job-search-card__location").text.strip()
            except:
                location = "N/A"
            # Posted date
            try:
                posted_date = card.find_element(By.CSS_SELECTOR, "time").text.strip()
            except:
                posted_date = "N/A"
            # Job link
            try:
                job_link = card.find_element(By.CSS_SELECTOR, "a.base-card__full-link").get_attribute("href")
            except:
                job_link = "N/A"
            # Skip duplicate jobs
            if job_link in seen_links or job_link == "N/A":
                continue
            seen_links.add(job_link)
            # --- Hiring Manager ---
            hiring_details = "N/A"
            try:
                # Open job details page
                driver.execute_script("window.open(arguments[0]);", job_link)
                driver.switch_to.window(driver.window_handles[1])
                time.sleep(3)
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
                # Close job tab
                driver.close()
                driver.switch_to.window(driver.window_handles[0])
            except:
                hiring_details = "N/A"
            # Save row
            writer.writerow([keyword, title, company, location, posted_date, job_link, hiring_details])
        # Pagination
        try:
            next_button = wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, f"button[aria-label='Page {page+1}']"))
            )
            next_button.click()
            page += 1
            time.sleep(3)
        except:
            print("  :white_check_mark: No more pages found.")
            break
# --- Cleanup ---
csv_file.close()
driver.quit()
print("\n:white_check_mark: Scraping complete. Data saved in linkedin_jobs.csv")