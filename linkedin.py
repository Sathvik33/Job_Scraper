import csv
import time
import os
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# CONFIG
KEYWORDS = ["Software Developer", "Software Engineer", "Backend Developer", "Frontend Developer", "Full Stack Developer"," DevOps Engineer", "Cloud Engineer", "Data Engineer", "Machine Learning Engineer",
            "AI Engineer", "Mobile App Developer", "Web Developer", "Systems Engineer", "Site Reliability Engineer", "Database Administrator"," Security Engineer"]
EXP_FILTER = "3,4,5,6"
MAX_PAGES = 4
JOBS_PER_PAGE = 25

BASE_PARAMS = {
    "location": "India",
    "geoId": "102713980",
    "f_WT": "2",
    "f_JT": "C",
    "f_E": EXP_FILTER
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

def random_user_agent():
    return random.choice(USER_AGENTS)

options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--disable-notifications")
options.add_argument(f"user-agent={random_user_agent()}")

driver = webdriver.Chrome(options=options)
wait = WebDriverWait(driver, 15)

base_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(base_dir, 'Data')
os.makedirs(data_dir, exist_ok=True)

csv_path = os.path.join(data_dir, "linkedin_jobs_guest.csv")
csv_file = open(csv_path, "w", newline="", encoding="utf-8")
writer = csv.writer(csv_file)
writer.writerow(["job_title", "company_name", "jobUrl", "salary", "location", "postedTime", "experienceLevel"])

seen_links = set()

def build_url(keyword, start):
    kw = keyword.replace(" ", "%20")
    params = "&".join([f"{k}={v}" for k, v in BASE_PARAMS.items()])
    return f"https://www.linkedin.com/jobs/search/?keywords={kw}&{params}&start={start}"

def handle_popups():
    popup_selectors = [
        "button[aria-label='Dismiss']",
        ".msg-overlay-bubble-header__control--close",
        "button[data-test-modal-close-btn]",
        ".artdeco-modal__dismiss",
        "button[aria-label='Close']"
    ]
    for selector in popup_selectors:
        for btn in driver.find_elements(By.CSS_SELECTOR, selector):
            try:
                if btn.is_displayed():
                    driver.execute_script("arguments[0].click();", btn)
                    time.sleep(0.5)
            except:
                pass

def close_login_popup():
    """Accurately detect and close visible LinkedIn login/signup popups."""
    try:
        login_selectors = [
            "div.sign-in-modal",
            "div#base-contextual-sign-in-modal",
            "div.join-form",
            "div.artdeco-modal__overlay",
            "div[role='dialog']",
            "button[aria-label='Dismiss']",
            "button[data-test-modal-close-btn]",
            "button[aria-label='Close']",
            ".modal__dismiss",
        ]

        closed_any = False

        for selector in login_selectors:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for el in elements:
                try:
                    # Check real visibility using JS
                    visible = driver.execute_script(
                        "const s = window.getComputedStyle(arguments[0]);"
                        "return !(s.visibility === 'hidden' || s.display === 'none' || s.opacity === '0');",
                        el
                    )
                    if visible:
                        driver.execute_script("arguments[0].click();", el)
                        time.sleep(0.6)
                        closed_any = True
                except Exception:
                    continue

        if closed_any:
            print("Login popup closed.")
            return True
        return False
    except Exception:
        return False


def extract_job_data(card):
    job_data = {
        "job_title": "N/A",
        "company_name": "N/A",
        "jobUrl": "N/A",
        "salary": "Not Disclosed",
        "location": "N/A",
        "postedTime": "N/A",
        "experienceLevel": "N/A"
    }

    try:
        title_el = card.find_element(By.CSS_SELECTOR, "a.base-card__full-link, a.job-card-list__title, a.job-card-container__link")
        job_data["job_title"] = title_el.text.strip()
        job_data["jobUrl"] = title_el.get_attribute("href").split("?")[0]
    except:
        pass

    try:
        company_el = card.find_element(By.CSS_SELECTOR, "span.base-search-card__subtitle, .job-card-container__company-name, h4.base-search-card__subtitle a")
        job_data["company_name"] = company_el.text.strip()
    except:
        pass

    try:
        loc = card.find_element(By.CSS_SELECTOR, "span.job-search-card__location")
        job_data["location"] = loc.text.strip()
    except:
        pass

    try:
        posted = card.find_element(By.CSS_SELECTOR, "time")
        job_data["postedTime"] = posted.get_attribute("datetime").split("T")[0]
    except:
        pass

    # Extract salary text if visible
    try:
        full_text = card.text
        if any(s in full_text for s in ["₹", "LPA", "lpa", "per month", "salary", "CTC"]):
            for line in full_text.split("\n"):
                if any(s in line for s in ["₹", "LPA", "lpa", "per month", "CTC"]):
                    job_data["salary"] = line.strip()
                    break
    except:
        pass

    # Extract experience level more intelligently
    try:
        exp_keywords = {
            "intern": "Internship",
            "entry": "Entry level",
            "associate": "Associate",
            "junior": "Junior",
            "mid": "Mid-Senior level",
            "senior": "Senior",
            "manager": "Manager",
            "lead": "Lead",
            "director": "Director",
            "executive": "Executive"
        }

        text = card.text.lower()

        # Check aria-labels or hidden tags if visible text fails
        hidden_attrs = []
        for attr in ["aria-label", "title"]:
            try:
                val = card.get_attribute(attr)
                if val:
                    hidden_attrs.append(val.lower())
            except:
                pass
        combined_text = text + " " + " ".join(hidden_attrs)

        for key, label in exp_keywords.items():
            if key in combined_text:
                job_data["experienceLevel"] = label
                break
    except:
        pass

    # Try fallback from description preview (sometimes visible in hover cards)
    if job_data["experienceLevel"] == "N/A":
        try:
            desc = card.find_element(By.CSS_SELECTOR, "p.job-card-list__insight").text.lower()
            for key, label in exp_keywords.items():
                if key in desc:
                    job_data["experienceLevel"] = label
                    break
        except:
            pass

    return job_data


try:
    for keyword in KEYWORDS:
        print("Searching:", keyword)
        for page in range(MAX_PAGES):
            start = page * JOBS_PER_PAGE
            url = build_url(keyword, start)
            print(" Page", page + 1, "URL:", url)
            driver.get(url)
            time.sleep(random.uniform(2.0, 4.0))

            handle_popups()
            close_login_popup()

            try:
                wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "ul.jobs-search__results-list li")))
            except:
                print("No jobs found on this page.")
                continue

            cards = driver.find_elements(By.CSS_SELECTOR, "ul.jobs-search__results-list li")
            print(" Found", len(cards), "cards")

            for i, card in enumerate(cards):
                job_data = extract_job_data(card)
                if job_data["jobUrl"] == "N/A" or job_data["jobUrl"] in seen_links:
                    continue

                seen_links.add(job_data["jobUrl"])
                print(f"  [{i+1}] {job_data['job_title'][:60]}")

                writer.writerow([
                    job_data["job_title"],
                    job_data["company_name"],
                    job_data["jobUrl"],
                    job_data["salary"],
                    job_data["location"],
                    job_data["postedTime"],
                    job_data["experienceLevel"]
                ])
                csv_file.flush()

                handle_popups()
                close_login_popup()
                time.sleep(random.uniform(1.0, 2.0))

except KeyboardInterrupt:
    print("Interrupted by user.")
except Exception as e:
    import traceback
    traceback.print_exc()
    print("Unexpected error:", e)
finally:
    csv_file.close()
    driver.quit()
    print("Scraping finished. Output saved to linkedin_jobs_guest.csv")
