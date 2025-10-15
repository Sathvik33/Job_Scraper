import os
import time
import random
import pandas as pd
import numpy as np
from urllib.parse import urljoin, quote_plus
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_stealth import stealth
import concurrent.futures
from threading import Lock
import joblib
import re
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'Data')
MODELS_DIR = os.path.join(BASE_DIR, 'ML_Models')
OUTPUT_FILE = os.path.join(DATA_DIR, 'jobs_raw_ml_extracted.csv')

os.makedirs(DATA_DIR, exist_ok=True)

SITE_CONFIG = {
    'indeed': {
        'base_url': 'https://in.indeed.com',
        'search_url': 'https://in.indeed.com/jobs?q={query}&l=India&start={page_num}',
        'link_selector': 'h2.jobTitle a',
        'wait_selector': '#jobDescriptionText'
    },
    'naukri': {
        'base_url': 'https://www.naukri.com',
        'search_url': 'https://www.naukri.com/{query}-jobs-{page_num}',
        'link_selector': 'a.title',
        'wait_selector': 'div.job-desc'
    },
    'linkedin': {
        'base_url': 'https://in.linkedin.com',
        'search_url': 'https://www.linkedin.com/jobs/search/?keywords={query}&location=India&start={page_num}',
        'link_selector': 'a.base-card__full-link',
        'wait_selector': 'div.jobs-details__main-content'
    }
}

SEARCH_QUERIES = ['Software Engineer', 'Backend Developer', 'Frontend Developer', 'Java Developer', 'Python Developer']
PAGES_TO_SCRAPE = 2
MAX_WORKERS = 2
MAX_JOBS_TARGET = 30

class AdvancedMLExtractor:
    def __init__(self, models_dir):
        self.pipelines = {}
        self.fields = ['Job Title', 'Company', 'Location', 'Job Type', 'Experience', 'Posted Date']
        self.junk_keywords = ['sign in', 'apply', 'save', 'share', 'report', 'first name', 'last name', 'email']
        
        print("🤖 Initializing ML Extraction Engine...")
        for field in self.fields:
            model_path = os.path.join(models_dir, f"{field.replace(' ', '_').lower()}_pipeline.pkl")
            try:
                self.pipelines[field] = joblib.load(model_path)
                print(f"  -> Loaded model for '{field}'")
            except FileNotFoundError:
                print(f"  [!] Model for '{field}' not found")
        
        if not self.pipelines:
            raise RuntimeError("No ML models found. Please run train_model.py first.")

    def _create_advanced_features(self, text):
        if not isinstance(text, str) or not text.strip():
            return {'text_length': 0, 'word_count': 0, 'has_digits': 0, 'digit_count': 0, 'has_special_chars': 0,
                    'special_char_count': 0, 'is_uppercase_ratio': 0, 'has_common_separators': 0, 'date_like_pattern': 0,
                    'experience_like_pattern': 0, 'url_like_pattern': 0, 'has_comma': 0, 'has_parentheses': 0,
                    'company_like_pattern': 0, 'location_like_pattern': 0}
        
        text = str(text).strip()
        text_length = len(text)
        word_count = len(text.split())
        has_digits = 1 if any(char.isdigit() for char in text) else 0
        digit_count = sum(char.isdigit() for char in text)
        
        special_chars = r'[!@#$%^&*()_+\-=\[\]{};\'":|,.<>?/~]'
        has_special_chars = 1 if re.search(special_chars, text) else 0
        special_char_count = len(re.findall(special_chars, text))
        
        uppercase_count = sum(1 for char in text if char.isupper())
        is_uppercase_ratio = uppercase_count / len(text) if text_length > 0 else 0
        
        has_common_separators = 1 if any(sep in text for sep in ['•', '|', '-', '·', '–', '—']) else 0
        has_comma = 1 if ',' in text else 0
        has_parentheses = 1 if '(' in text and ')' in text else 0
        
        date_patterns = [r'\d{1,2}[-/]\d{1,2}[-/]\d{2,4}', r'\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{4}', r'(?:today|yesterday|\d+\s+(?:days?|hours?|months?)\s+ago)']
        date_like_pattern = 1 if any(re.search(pattern, text.lower()) for pattern in date_patterns) else 0
        
        experience_patterns = [r'\d+\+?\s*(?:years?|yrs?)', r'\d+\s*-\s*\d+\s*(?:years?|yrs?)', r'fresher|entry level|experienced']
        experience_like_pattern = 1 if any(re.search(pattern, text.lower()) for pattern in experience_patterns) else 0
        
        url_like_pattern = 1 if re.search(r'https?://|www\.|\.com|\.in', text.lower()) else 0
        company_like_pattern = 1 if re.search(r'\b(?:inc|llc|ltd|corp|corporation|company|co\.)\b', text) else 0
        location_like_pattern = 1 if re.search(r'[A-Z][a-z]+,\s*[A-Z][a-z]+', text) else 0
        
        return {
            'text_length': text_length, 'word_count': word_count, 'has_digits': has_digits, 'digit_count': digit_count,
            'has_special_chars': has_special_chars, 'special_char_count': special_char_count, 'is_uppercase_ratio': is_uppercase_ratio,
            'has_common_separators': has_common_separators, 'date_like_pattern': date_like_pattern, 'experience_like_pattern': experience_like_pattern,
            'url_like_pattern': url_like_pattern, 'has_comma': has_comma, 'has_parentheses': has_parentheses,
            'company_like_pattern': company_like_pattern, 'location_like_pattern': location_like_pattern
        }

    def _get_quality_candidates(self, soup):
        candidates = []
        priority_selectors = ['h1', 'h2', 'h3', 'h4', 'div', 'span', 'p', 'li', 'strong', 'b']
        
        for selector in priority_selectors:
            for element in soup.select(selector):
                text = element.get_text(strip=True)
                if text and 2 < len(text) < 200:
                    text_lower = text.lower()
                    if any(junk in text_lower for junk in self.junk_keywords): continue
                    if any(field in text_lower for field in ['first name', 'last name', 'email', 'phone']): continue
                    if len(re.findall(r'[a-zA-Z]', text)) < 2: continue
                    candidates.append({'text': text, 'tag': element.name})
        
        unique_candidates = []
        seen_texts = set()
        for cand in candidates:
            if cand['text'] not in seen_texts:
                unique_candidates.append(cand)
                seen_texts.add(cand['text'])
        return unique_candidates

    def _post_process_extracted_data(self, extracted_data):
        cleaned_data = extracted_data.copy()
        
        if cleaned_data.get('Job Title') != "Not Specified":
            title = cleaned_data['Job Title']
            title = re.sub(r'\s*at\s+[A-Z][a-zA-Z\s]+$', '', title)
            title = re.sub(r'\s*-\s*[A-Z][a-zA-Z\s]+$', '', title)
            title = re.sub(r',\s*[A-Z][a-zA-Z,\s]+$', '', title)
            title = re.sub(r'\s*\([^)]*\)$', '', title)
            cleaned_data['Job Title'] = title.strip()
        
        if cleaned_data.get('Company') != "Not Specified":
            company = cleaned_data['Company']
            if company.lower() in ['linkedin', 'indeed', 'naukri']:
                cleaned_data['Company'] = "Not Specified"
        
        if cleaned_data.get('Location') != "Not Specified":
            location = cleaned_data['Location']
            if 'bahasa' in location.lower() or 'malaysia' in location.lower():
                cleaned_data['Location'] = "Not Specified"
            location = re.sub(r'\s*\([^)]*\)', '', location)
            cleaned_data['Location'] = location.strip()
        
        if cleaned_data.get('Experience') != "Not Specified":
            exp = cleaned_data['Experience']
            if not any(word in exp.lower() for word in ['year', 'yr', 'exp', 'fresher']):
                cleaned_data['Experience'] = "Not Specified"
        
        return cleaned_data

    def predict(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        candidates = self._get_quality_candidates(soup)
        if not candidates: return {field: "Not Found" for field in self.fields}
        
        candidates_df = pd.DataFrame(candidates)
        features_list = [self._create_advanced_features(text) for text in candidates_df['text']]
        features_df = pd.DataFrame(features_list)
        features_df['tag'] = candidates_df['tag']
        
        extracted_data = {field: "Not Specified" for field in self.fields}
        confidence_scores = {field: 0.0 for field in self.fields}
        used_indices = set()
        
        for field, pipeline in self.pipelines.items():
            try:
                probabilities = pipeline.predict_proba(features_df)[:, 1]
                best_score = 0
                best_idx = -1
                
                for idx, score in enumerate(probabilities):
                    if idx not in used_indices and score > best_score and score > 0.6:
                        best_score = score
                        best_idx = idx
                
                if best_idx != -1:
                    extracted_data[field] = candidates_df.loc[best_idx, 'text']
                    confidence_scores[field] = best_score
                    used_indices.add(best_idx)
                    print(f"        {field}: {extracted_data[field][:40]}... (confidence: {best_score:.3f})")
                    
            except Exception as e:
                print(f"        [!] Error predicting {field}: {e}")
                continue
        
        cleaned_data = self._post_process_extracted_data(extracted_data)
        cleaned_data['ML_Confidence_Avg'] = np.mean(list(confidence_scores.values())) if confidence_scores else 0
        return cleaned_data

def create_requests_session():
    session = requests.Session()
    retry_strategy = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    })
    return session

def extract_with_requests_enhanced(link, ml_extractor):
    session = create_requests_session()
    try:
        print(f"      📡 Using requests for: {link[:60]}...")
        response = session.get(link, timeout=30)
        if response.status_code == 200:
            if '<!DOCTYPE html' in response.text[:100] or '<html' in response.text[:100]:
                job_data = ml_extractor.predict(response.text)
                job_data['Job Link'] = link
                job_data['Extraction_Method'] = 'requests'
                return job_data
        return None
    except Exception as e:
        print(f"      ❌ Request error: {str(e)[:50]}")
        return None

def scrape_linkedin_links(query, unique_links_lock, unique_links):
    session = create_requests_session()
    links_found = 0
    try:
        for i in range(PAGES_TO_SCRAPE):
            with unique_links_lock:
                if len(unique_links) >= MAX_JOBS_TARGET: return links_found
            
            page_num = i * 25
            formatted_query = quote_plus(query)
            search_url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={formatted_query}&location=India&start={page_num}"
            
            print(f"  🔍 Scraping LinkedIn: {query} - page {i+1}")
            response = session.get(search_url, timeout=30)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")
                link_elements = soup.select('a.base-card__full-link')
                for link_el in link_elements:
                    href = link_el.get('href')
                    if href:
                        with unique_links_lock:
                            if len(unique_links) < MAX_JOBS_TARGET and href not in unique_links:
                                unique_links.add(href)
                                links_found += 1
            time.sleep(2)
    except Exception as e:
        print(f"Error scraping LinkedIn: {e}")
    return links_found

def run_scraper():
    start_time = time.time()
    print("="*80)
    print(f" STABLE ML JOB SCRAPER")
    print("="*80)
    
    try:
        ml_extractor = AdvancedMLExtractor(models_dir=MODELS_DIR)
    except RuntimeError as e:
        print(f"[FATAL ERROR] {e}")
        return

    print("\n[1/3] Collecting LinkedIn job links...")
    unique_links = set()
    unique_links_lock = Lock()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        all_tasks = [('linkedin', query) for query in SEARCH_QUERIES[:3]]
        futures = {executor.submit(scrape_linkedin_links, query, unique_links_lock, unique_links) for site, query in all_tasks}
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            print(f"    Found {result} new links. Total: {len(unique_links)}")
            if len(unique_links) >= MAX_JOBS_TARGET:
                executor.shutdown(wait=False, cancel_futures=True)
                break
    
    links_to_process = list(unique_links)[:MAX_JOBS_TARGET]
    print(f"\n    ✓ Collected {len(links_to_process)} job links")
    
    print(f"\n[2/3] Extracting job details...")
    all_jobs = []
    
    for i, link in enumerate(links_to_process):
        print(f"    Processing {i+1}/{len(links_to_process)}: {link[:80]}...")
        result = extract_with_requests_enhanced(link, ml_extractor)
        if result:
            result['Source Site'] = 'linkedin'
            all_jobs.append(result)
            print(f"      ✅ Success")
        else:
            print(f"      ❌ Failed")
        time.sleep(1)
    
    print(f"\n[3/3] Saving {len(all_jobs)} jobs...")
    if all_jobs:
        df = pd.DataFrame(all_jobs)
        required_columns = ['Job Title', 'Company', 'Location', 'Job Type', 'Experience', 'Posted Date', 'Job Link', 'Source Site']
        for col in required_columns:
            if col not in df.columns: df[col] = "Not Found"
        df = df[required_columns]
        df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
        print(f"    ✓ Data saved to {OUTPUT_FILE}")
        
        print("\n📊 Extraction Statistics:")
        for field in ['Job Title', 'Company', 'Location', 'Job Type', 'Experience', 'Posted Date']:
            found_count = df[df[field] != "Not Specified"][field].count()
            print(f"    {field}: {found_count}/{len(df)} ({found_count/len(df)*100:.1f}%)")
    
    execution_time = (time.time() - start_time) / 60
    print("\n" + "=" * 80)
    print(f"🎉 COMPLETE! Time: {execution_time:.2f} min | Jobs: {len(all_jobs)}")
    print("="*80)

if __name__ == "__main__":
    run_scraper()