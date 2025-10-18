from flask import Flask, render_template, request, jsonify, send_file
import threading
import csv
import os
import time
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup

app = Flask(__name__)
scrape_status = {'running': False, 'message': '', 'progress': 0, 'total': 0}

# Adzuna API credentials, replace with real ones
ADZUNA_APP_ID = '83a75a0e'
ADZUNA_APP_KEY = '39fe58982c2cb92aaf0dd2ec76ba8b72'

def save_to_csv(job_list, filename):
    if not job_list:
        return
    keys = job_list[0].keys()
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, keys)
        writer.writeheader()
        writer.writerows(job_list)

def selenium_dynamic_scraper(site_url):
    jobs = []
    options = Options()
    options.headless = False  # visible browser for manual login if needed
    driver = webdriver.Chrome(options=options)
    try:
        driver.get(site_url)
        if "linkedin.com" in site_url:
            scrape_status['message'] = "Please log in manually in the opened browser window (120s)..."
            time.sleep(120)
        for _ in range(7):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        if "linkedin.com" in site_url:
            cards = soup.select("ul.jobs-search__results-list li")
            for card in cards:
                title = card.find('h3')
                company = card.find('h4')
                date = card.find('time')
                link = card.find('a', href=True)
                jobs.append({
                    "job title": title.get_text(strip=True) if title else "N/A",
                    "company name": company.get_text(strip=True) if company else "N/A",
                    "job link": link['href'] if link else "N/A",
                    "date posted": date.get_text(strip=True) if date else "N/A"
                })
        elif "naukri.com" in site_url:
            cards = soup.find_all('div', class_='cust-job-tuple')
            for card in cards:
                title_tag = card.find('a', class_='title')
                company_tag = card.find('a', class_='comp-name')
                date_tag = card.find('span', class_='job-post-day')
                jobs.append({
                    "job title": title_tag.get_text(strip=True) if title_tag else "N/A",
                    "company name": company_tag.get_text(strip=True) if company_tag else "N/A",
                    "job link": title_tag['href'] if title_tag else "N/A",
                    "date posted": date_tag.get_text(strip=True) if date_tag else "N/A"
                })
        elif "glassdoor.com" in site_url:
            cards = soup.find_all('li', class_='react-job-listing')
            for card in cards:
                title_tag = card.find('a', class_='jobLink')
                company_tag = card.find('div', class_='jobHeader')
                date_tag = card.find('div', class_='d-flex align-items-end pl-std css-mi55ob')
                jobs.append({
                    "job title": title_tag.get_text(strip=True) if title_tag else "N/A",
                    "company name": company_tag.get_text(strip=True) if company_tag else "N/A",
                    "job link": "https://glassdoor.com" + title_tag['href'] if title_tag else "N/A",
                    "date posted": date_tag.get_text(strip=True) if date_tag else "N/A"
                })
        else:
            cards = soup.find_all('div')
            for card in cards:
                title_elem = card.find(['a', 'h2', 'h3'], href=True)
                if not title_elem:
                    continue
                job_title = title_elem.get_text(strip=True)
                job_link = title_elem['href']
                company_elem = card.find(class_=lambda c: c and 'company' in c.lower()) or card.find('span', class_=lambda c: c and 'company' in c.lower())
                company_name = company_elem.get_text(strip=True) if company_elem else 'N/A'
                date_elem = card.find(class_=lambda c: c and ('date' in c.lower() or 'posted' in c.lower()))
                date_posted = date_elem.get_text(strip=True) if date_elem else 'N/A'
                jobs.append({
                    'job title': job_title,
                    'company name': company_name,
                    'job link': job_link,
                    'date posted': date_posted
                })
        scrape_status['progress'] = len(jobs)
        scrape_status['total'] = len(jobs)
    finally:
        driver.quit()
    return jobs

def fetch_jobs_adzuna(query, location, max_days=7, page=1):
    url = f"https://api.adzuna.com/v1/api/jobs/gb/search/{page}"
    params = {
        "app_id": ADZUNA_APP_ID,
        "app_key": ADZUNA_APP_KEY,
        "results_per_page": 50,
        "what": query,
        "where": location,
        "max_days_old": max_days,
        "content-type": "application/json"
    }
    response = requests.get(url, params=params)
    jobs = []
    if response.status_code == 200:
        data = response.json()
        for job in data.get("results", []):
            jobs.append({
                "job title": job.get("title"),
                "company name": job.get("company", {}).get("display_name"),
                "job link": job.get("redirect_url"),
                "date posted": job.get("created")
            })
    return jobs

def threaded_scrape(url=None, use_adzuna=False, query=None, location=None):
    scrape_status['running'] = True
    scrape_status['progress'] = 0
    scrape_status['message'] = "Starting job fetching..."
    try:
        if use_adzuna:
            jobs = fetch_jobs_adzuna(query or '', location or '')
        else:
            jobs = selenium_dynamic_scraper(url)
        if jobs:
            save_to_csv(jobs, 'jobs_results.csv')
            scrape_status['message'] = f"Done! Found {len(jobs)} jobs. Download ready."
        else:
            scrape_status['message'] = "No jobs found or failed."
    except Exception as e:
        scrape_status['message'] = f"Error: {e}"
    scrape_status['running'] = False

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/scrape', methods=['POST'])
def scrape():
    url = request.form.get('url')
    use_adzuna = request.form.get('use_adzuna') == 'true'
    query = request.form.get('query')
    location = request.form.get('location')
    if use_adzuna:
        thread = threading.Thread(target=threaded_scrape, kwargs={'use_adzuna': True, 'query': query, 'location': location})
    else:
        if not url:
            return jsonify({'error': 'No URL provided'}), 400
        thread = threading.Thread(target=threaded_scrape, kwargs={'url': url, 'use_adzuna': False})
    thread.start()
    return jsonify({'status': 'started'})

@app.route('/status')
def status():
    return jsonify(scrape_status)

@app.route('/download')
def download():
    if os.path.exists('jobs_results.csv'):
        return send_file('jobs_results.csv', as_attachment=True)
    return "No CSV file found.", 404

if __name__ == '__main__':
    app.run(debug=True)
