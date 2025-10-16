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

JOB_CONFIG = {
    'categories': {
        'professional': True,
        'internships': True,
        'freshers': True,
        'specialized': True,
    },
    'experience_level': 'all',
    'locations': ['India'],
    'job_types': ['full-time', 'internship'],
    'platforms': ['linkedin'],
}

JOB_CATEGORIES = {
    'professional': [
        'Software Engineer', 'Backend Developer', 'Frontend Developer', 'Full Stack Developer',
        'Java Developer', 'Python Developer', 'JavaScript Developer', 'React Developer',
        'Node.js Developer', 'DevOps Engineer', 'Cloud Engineer', 'Data Scientist',
        'Data Analyst', 'Machine Learning Engineer', 'AI Engineer', 'Mobile Developer',
    ],
    'internships': [
        'Software Engineer Intern', 'Software Development Intern', 'Backend Developer Intern',
        'Frontend Developer Intern', 'Data Science Intern', 'Machine Learning Intern',
    ],
    'freshers': [
        'Fresher Software Engineer', 'Entry Level Developer', 'Recent Graduate Developer',
        'Campus Hiring', 'New Grad Software Engineer',
    ],
    'specialized': [
        'React Native Developer', 'Vue.js Developer', 'Angular Developer', 'Django Developer',
        'Spring Boot Developer', '.NET Developer'
    ]
}

def build_search_queries(config):
    search_queries = []
    for category, enabled in config['categories'].items():
        if enabled:
            search_queries.extend(JOB_CATEGORIES[category])
    search_queries = list(set(search_queries))
    print(f"Built {len(search_queries)} search queries")
    return search_queries

SEARCH_QUERIES = build_search_queries(JOB_CONFIG)

class AdvancedMLExtractor:
    def __init__(self, models_dir):
        self.pipelines = {}
        self.fields = ['Job Title', 'Company', 'Location', 'Job Type', 'Experience', 'Posted Date']
        
        # Enhanced junk keywords with common false positives
        self.junk_keywords = [
            'sign in', 'apply', 'save', 'share', 'report', 'first name', 'last name', 
            'email', 'similar searches', 'industries', 'job', 'posted', 'open jobs',
            'show more', 'show less', 'see more', 'see less', 'load more', 'view all',
            'companies', 'locations', 'salaries', 'profiles', 'about', 'follow',
            'connect', 'message', 'sponsored', 'promoted', 'featured'
        ]
        
        # Company blacklist - common false positives
        self.company_blacklist = [
            'similar searches', 'industries', 'linkedin', 'indeed', 'naukri',
            'job title', 'location', 'experience', 'posted date', 'job type',
            'companies', 'show more', 'see all', 'open jobs', 'job search'
        ]
        
        # Job type blacklist
        self.job_type_blacklist = [
            'industries', 'similar searches', 'companies', 'locations',
            'open jobs', 'show more', 'see all'
        ]
        
        print("Initializing ML Extraction Engine...")
        for field in self.fields:
            model_path = os.path.join(models_dir, f"{field.replace(' ', '_').lower()}_pipeline.pkl")
            try:
                self.pipelines[field] = joblib.load(model_path)
                print(f"  -> Loaded model for '{field}'")
            except FileNotFoundError:
                print(f"  [!] Model for '{field}' not found")
        
        if not self.pipelines:
            raise RuntimeError("No ML models found. Please run train_model.py first.")

    def _create_advanced_features(self, text, field=None):
        """Create features that match the trained models"""
        if not isinstance(text, str) or not text.strip():
            base_features = {
                'text_length': 0, 'word_count': 0, 'has_digits': 0, 
                'digit_count': 0, 'is_uppercase_ratio': 0, 'has_comma': 0
            }
            
            if field == 'Company':
                base_features.update({'has_company_keywords': 0, 'has_job_title_words': 0})
            elif field == 'Experience':
                base_features.update({'has_experience_keywords': 0, 'has_year_mentions': 0})
            elif field == 'Job Title':
                base_features.update({'has_title_keywords': 0})
            elif field == 'Location':
                base_features.update({'has_location_indicators': 0, 'has_indian_city': 0})
                
            return base_features
        
        text = str(text).strip()
        text_length = len(text)
        word_count = len(text.split())
        has_digits = 1 if any(char.isdigit() for char in text) else 0
        digit_count = sum(char.isdigit() for char in text)
        
        uppercase_count = sum(1 for char in text if char.isupper())
        is_uppercase_ratio = uppercase_count / len(text) if text_length > 0 else 0
        
        has_comma = 1 if ',' in text else 0
        
        features = {
            'text_length': text_length,
            'word_count': word_count,
            'has_digits': has_digits,
            'digit_count': digit_count,
            'is_uppercase_ratio': is_uppercase_ratio,
            'has_comma': has_comma
        }
        
        text_lower = text.lower()
        
        if field == 'Company':
            company_keywords = [
                'technologies', 'solutions', 'systems', 'software', 'services', 
                'consulting', 'group', 'corporation', 'incorporated', 'limited', 
                'ltd', 'inc', 'corp', 'company', 'co.', 'enterprises', 'ventures',
                'holdings', 'international', 'global', 'digital', 'innovations',
                'labs', 'studios', 'networks', 'capital', 'partners', 'pvt'
            ]
            job_title_words = [
                'engineer', 'developer', 'manager', 'analyst', 'specialist',
                'architect', 'consultant', 'coordinator', 'associate', 'lead',
                'head', 'officer', 'executive', 'director', 'president', 'intern'
            ]
            
            features['has_company_keywords'] = 1 if any(keyword in text_lower for keyword in company_keywords) else 0
            features['has_job_title_words'] = 1 if any(word in text_lower for word in job_title_words) else 0
            
        elif field == 'Experience':
            experience_keywords = [
                'years', 'yrs', 'year', 'experience', 'exp', 'fresher', 
                'entry', 'mid', 'senior', 'level', 'experienced'
            ]
            year_patterns = [
                r'\d+\+?\s*(?:years?|yrs?)', r'\d+\s*-\s*\d+\s*(?:years?|yrs?)',
                r'\d+\s*to\s*\d+\s*(?:years?|yrs?)', r'\d+\s*(?:years?|yrs?)',
                r'\d+\.?\d*\s*(?:years?|yrs?)'
            ]
            
            features['has_experience_keywords'] = 1 if any(keyword in text_lower for keyword in experience_keywords) else 0
            features['has_year_mentions'] = 1 if any(re.search(pattern, text_lower) for pattern in year_patterns) else 0
            
        elif field == 'Job Title':
            title_keywords = [
                'engineer', 'developer', 'analyst', 'manager', 'architect',
                'specialist', 'scientist', 'consultant', 'coordinator', 'associate',
                'lead', 'head', 'officer', 'executive', 'director', 'president', 'intern'
            ]
            features['has_title_keywords'] = 1 if any(keyword in text_lower for keyword in title_keywords) else 0
            
        elif field == 'Location':
            location_indicators = [',', 'remote', 'hybrid', 'onsite', 'office', 'location']
            indian_cities = [
                'pune', 'bangalore', 'bengaluru', 'hyderabad', 'chennai', 'mumbai',
                'delhi', 'gurgaon', 'gurugram', 'noida', 'kolkata', 'ahmedabad'
            ]
            features['has_location_indicators'] = 1 if any(indicator in text_lower for indicator in location_indicators) else 0
            features['has_indian_city'] = 1 if any(city in text_lower for city in indian_cities) else 0
            
        return features

    def _is_valid_company(self, text):
        """Enhanced validation for company names"""
        if not text or not isinstance(text, str):
            return False
            
        text_lower = text.lower().strip()
        
        # Check blacklist
        if any(blacklisted in text_lower for blacklisted in self.company_blacklist):
            return False
        
        # Must have at least 2 characters and contain letters
        if len(text) < 2 or not any(c.isalpha() for c in text):
            return False
        
        # Reject if it contains too many job title indicators
        job_indicators = ['engineer', 'developer', 'manager', 'analyst', 'intern', 'specialist']
        if sum(1 for indicator in job_indicators if indicator in text_lower) >= 2:
            return False
        
        # Reject if it's just numbers or special characters
        if text.replace(' ', '').replace(',', '').replace('.', '').isdigit():
            return False
            
        return True
    
    def _is_valid_job_type(self, text):
        """Enhanced validation for job types"""
        if not text or not isinstance(text, str):
            return False
            
        text_lower = text.lower().strip()
        
        # Check blacklist
        if any(blacklisted in text_lower for blacklisted in self.job_type_blacklist):
            return False
        
        # Valid job types
        valid_types = [
            'full-time', 'full time', 'fulltime', 
            'part-time', 'part time', 'parttime',
            'internship', 'intern', 'contract', 'temporary', 'freelance',
            'remote', 'hybrid', 'onsite', 'on-site'
        ]
        
        # Check if text contains any valid job type
        if any(valid_type in text_lower for valid_type in valid_types):
            return True
            
        return False

    def _is_valid_experience_text(self, text):
        """Enhanced validation for experience text"""
        if not text or not isinstance(text, str):
            return False
            
        text_lower = text.lower().strip()
        
        # Skip if it contains blacklisted terms
        blacklist_terms = ['similar search', 'industries', 'companies', 'show more']
        if any(term in text_lower for term in blacklist_terms):
            return False
        
        # Must contain experience-related keywords or numbers
        experience_indicators = [
            'year', 'yr', 'experience', 'exp', 'fresher', 'entry', 
            'senior', 'junior', 'mid', 'level'
        ]
        
        has_experience_keywords = any(indicator in text_lower for indicator in experience_indicators)
        has_numbers = any(char.isdigit() for char in text)
        
        return has_experience_keywords or has_numbers

    def _get_quality_candidates(self, soup):
        """Extract candidate text elements with better filtering"""
        candidates = []
        priority_selectors = ['h1', 'h2', 'h3', 'h4', 'div', 'span', 'p', 'li', 'strong', 'b', 'a']
        
        for selector in priority_selectors:
            for element in soup.select(selector):
                text = element.get_text(strip=True)
                
                # Basic filtering
                if not text or len(text) < 2 or len(text) > 200:
                    continue
                
                text_lower = text.lower()
                
                # Skip junk keywords
                if any(junk in text_lower for junk in self.junk_keywords):
                    continue
                
                # Skip form fields
                if any(field in text_lower for field in ['first name', 'last name', 'email', 'phone', 'password']):
                    continue
                
                # Must have some alphabetic characters
                if len(re.findall(r'[a-zA-Z]', text)) < 2:
                    continue
                
                candidates.append({'text': text, 'tag': element.name})
        
        # Remove duplicates while preserving order
        unique_candidates = []
        seen_texts = set()
        for cand in candidates:
            if cand['text'] not in seen_texts:
                unique_candidates.append(cand)
                seen_texts.add(cand['text'])
        
        return unique_candidates

    def _extract_experience_from_text_enhanced(self, text):
        """ENHANCED: Extract experience years from text with better patterns"""
        if not text or not isinstance(text, str):
            return "0"
        
        text_lower = text.lower().strip()
        
        # Remove common noise words that might confuse the extraction
        noise_words = ['similar search', 'industries', 'companies', 'show more', 'see all']
        for noise in noise_words:
            text_lower = text_lower.replace(noise, '')
        
        # Pattern 1: Range format "2-4 years" or "2 - 4 years" or "2 to 4 years"
        range_patterns = [
            r'(\d+)\s*[-–—]\s*(\d+)\s*(?:years?|yrs?)',
            r'(\d+)\s*to\s*(\d+)\s*(?:years?|yrs?)',
            r'(\d+)\s*-\s*(\d+)\s*(?:years?|yrs?)'
        ]
        
        for pattern in range_patterns:
            range_match = re.search(pattern, text_lower)
            if range_match:
                min_years = int(range_match.group(1))
                max_years = int(range_match.group(2))
                return f"{min_years}-{max_years}"
        
        # Pattern 2: Single number with plus "2+ years" or "2 + years"
        plus_match = re.search(r'(\d+)\s*\+\s*(?:years?|yrs?)', text_lower)
        if plus_match:
            return f"{plus_match.group(1)}+"
        
        # Pattern 3: Exact years "2 years" or "2 yrs" or "2 year"
        exact_match = re.search(r'(\d+\.?\d*)\s*(?:years?|yrs?)', text_lower)
        if exact_match:
            years = exact_match.group(1)
            # Convert decimal to whole number if needed
            if '.' in years:
                years = str(int(float(years)))
            return years
        
        # Pattern 4: Just numbers that likely represent years
        # Look for numbers in context of experience keywords
        if any(keyword in text_lower for keyword in ['year', 'yr', 'experience', 'exp']):
            number_matches = re.findall(r'\b(\d+)\b', text_lower)
            if number_matches:
                # Take the first number found in experience context
                for num in number_matches:
                    num_int = int(num)
                    if 0 <= num_int <= 50:  # Reasonable experience range
                        return str(num_int)
        
        # Enhanced fresher detection
        fresher_keywords = [
            'fresher', 'entry level', 'entry-level', '0 year', 'no experience', 
            'new grad', 'recent graduate', 'fresh graduate', 'campus hire',
            'student', 'trainee', 'beginner'
        ]
        if any(keyword in text_lower for keyword in fresher_keywords):
            return "0"
        
        # Enhanced experience level mapping
        level_keywords = {
            'senior': "5+",
            'lead': "5+", 
            'principal': "8+",
            'architect': "6+",
            'mid-level': "3-5",
            'mid level': "3-5",
            'intermediate': "2-4",
            'junior': "1-2",
            'associate': "1-2",
            'entry': "0-1"
        }
        
        for keyword, exp_range in level_keywords.items():
            if keyword in text_lower:
                return exp_range
        
        return "0"

    def _normalize_job_type(self, text):
        """Normalize job type to standard values"""
        if not text or not isinstance(text, str):
            return "Not Specified"
        
        text_lower = text.lower().strip()
        
        # Full-time variations
        if any(term in text_lower for term in ['full-time', 'full time', 'fulltime', 'permanent']):
            return "Full-time"
        
        # Part-time variations
        if any(term in text_lower for term in ['part-time', 'part time', 'parttime']):
            return "Part-time"
        
        # Internship variations
        if any(term in text_lower for term in ['internship', 'intern', 'trainee']):
            return "Internship"
        
        # Contract variations
        if any(term in text_lower for term in ['contract', 'contractor', 'temporary', 'temp']):
            return "Contract"
        
        # Freelance variations
        if any(term in text_lower for term in ['freelance', 'freelancer', 'consultant']):
            return "Freelance"
        
        return "Not Specified"

    def _post_process_extracted_data_enhanced(self, extracted_data):
        """Enhanced post-processing with better experience handling"""
        cleaned_data = extracted_data.copy()
        
        # Clean Job Title
        if cleaned_data.get('Job Title') != "Not Specified":
            title = cleaned_data['Job Title']
            # Remove company names at the end
            title = re.sub(r'\s*at\s+[A-Z][a-zA-Z\s]+$', '', title)
            title = re.sub(r'\s*-\s*[A-Z][a-zA-Z\s]+$', '', title)
            title = re.sub(r',\s*[A-Z][a-zA-Z,\s]+$', '', title)
            title = re.sub(r'\s*\([^)]*\)$', '', title)
            cleaned_data['Job Title'] = title.strip()
        
        # Validate and clean Company
        if cleaned_data.get('Company') != "Not Specified":
            company = cleaned_data['Company']
            if not self._is_valid_company(company):
                cleaned_data['Company'] = "Not Specified"
            else:
                # Clean up the company name
                company = company.strip()
                # Remove common prefixes that shouldn't be there
                company = re.sub(r'^(Similar Searches?|Industries?|Companies?)\s*', '', company, flags=re.IGNORECASE)
                cleaned_data['Company'] = company.strip()
        
        # Clean Location
        if cleaned_data.get('Location') != "Not Specified":
            location = cleaned_data['Location']
            # Remove unwanted terms
            if any(term in location.lower() for term in ['bahasa', 'malaysia', 'similar search']):
                cleaned_data['Location'] = "Not Specified"
            else:
                location = re.sub(r'\s*\([^)]*\)', '', location)
                cleaned_data['Location'] = location.strip()
        
        # Validate and normalize Job Type
        if cleaned_data.get('Job Type') != "Not Specified":
            job_type = cleaned_data['Job Type']
            if not self._is_valid_job_type(job_type):
                cleaned_data['Job Type'] = "Not Specified"
            else:
                cleaned_data['Job Type'] = self._normalize_job_type(job_type)
        
        # ENHANCED: Experience extraction with the new method
        if cleaned_data.get('Experience') != "Not Specified":
            exp_text = cleaned_data['Experience']
            extracted_exp = self._extract_experience_from_text_enhanced(exp_text)
            cleaned_data['Experience'] = extracted_exp
            print(f"        Experience extracted: '{exp_text}' -> '{extracted_exp}'")
        else:
            # If no experience field found, try to extract from all candidates
            cleaned_data['Experience'] = "0"
        
        return cleaned_data

    def predict(self, html_content):
        """Main prediction method with enhanced experience extraction"""
        soup = BeautifulSoup(html_content, 'html.parser')
        candidates = self._get_quality_candidates(soup)
        
        if not candidates: 
            result = {field: "Not Specified" for field in self.fields}
            result['Experience'] = "0"
            result['Job Type'] = "Not Specified"
            return result
        
        candidates_df = pd.DataFrame(candidates)
        
        extracted_data = {field: "Not Specified" for field in self.fields}
        confidence_scores = {field: 0.0 for field in self.fields}
        used_indices = set()
        
        # Extract fields with validation - EXPERIENCE FIRST for better accuracy
        field_priority = ['Experience', 'Job Title', 'Company', 'Location', 'Job Type', 'Posted Date']
        
        for field in field_priority:
            pipeline = self.pipelines.get(field)
            if not pipeline:
                continue
                
            try:
                features_list = [self._create_advanced_features(text, field) for text in candidates_df['text']]
                features_df = pd.DataFrame(features_list)
                features_df['tag'] = candidates_df['tag']
                
                probabilities = pipeline.predict_proba(features_df)[:, 1]
                
                # Find best candidate with field-specific validation
                best_score = 0
                best_idx = -1
                
                for idx, score in enumerate(probabilities):
                    if idx in used_indices:
                        continue
                    
                    candidate_text = candidates_df.loc[idx, 'text']
                    
                    # Apply field-specific validation
                    is_valid = True
                    if field == 'Company':
                        is_valid = self._is_valid_company(candidate_text)
                    elif field == 'Job Type':
                        is_valid = self._is_valid_job_type(candidate_text)
                    elif field == 'Experience':
                        # ENHANCED: Use the new validation
                        is_valid = self._is_valid_experience_text(candidate_text)
                    
                    # Adjusted confidence thresholds
                    min_confidence = 0.6 if field == 'Experience' else 0.7 if field in ['Company', 'Job Type'] else 0.5
                    
                    if is_valid and score > best_score and score > min_confidence:
                        best_score = score
                        best_idx = idx
                
                if best_idx != -1:
                    extracted_data[field] = candidates_df.loc[best_idx, 'text']
                    confidence_scores[field] = best_score
                    used_indices.add(best_idx)
                    print(f"        {field}: {extracted_data[field][:50]}... (confidence: {best_score:.3f})")
                else:
                    print(f"        {field}: No valid candidate found")
                    
            except Exception as e:
                print(f"        [!] Error predicting {field}: {e}")
                continue
        
        # Enhanced post-processing with better experience extraction
        cleaned_data = self._post_process_extracted_data_enhanced(extracted_data)
        
        # Final experience fallback - check job title for experience hints
        if cleaned_data.get('Experience') in ["0", "Not Specified"]:
            title = cleaned_data.get('Job Title', '').lower()
            if 'intern' in title or 'fresher' in title:
                cleaned_data['Experience'] = "0"
            elif 'senior' in title or 'lead' in title:
                cleaned_data['Experience'] = "5+"
            elif 'junior' in title:
                cleaned_data['Experience'] = "1-2"
        
        cleaned_data['ML_Confidence_Avg'] = np.mean(list(confidence_scores.values())) if confidence_scores else 0
        
        return cleaned_data


def create_requests_session():
    """Create a robust session with better headers and retry strategy"""
    session = requests.Session()
    
    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=100, pool_maxsize=100)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/avif,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Cache-Control': 'max-age=0',
    })
    
    return session

def extract_with_requests_enhanced(link, ml_extractor):
    """Enhanced extraction with better error handling"""
    session = create_requests_session()
    
    for attempt in range(3):
        try:
            print(f"     Attempt {attempt+1} for: {link[:60]}...")
            
            if attempt > 0:
                delay = random.uniform(5, 15)
                print(f"      Waiting {delay:.1f}s before retry...")
                time.sleep(delay)
            
            response = session.get(link, timeout=25)
            
            if response.status_code == 200:
                if any(indicator in response.text[:200] for indicator in ['<!DOCTYPE', '<html', '<!doctype']):
                    job_data = ml_extractor.predict(response.text)
                    job_data['Job Link'] = link
                    job_data['Extraction_Method'] = 'requests'
                    return job_data
                else:
                    print(f"      ⚠️  No HTML content received")
                    continue
                    
            elif response.status_code == 429:
                print(f"      🚫 Rate limited, waiting longer...")
                time.sleep(30)
                continue
                
            elif response.status_code == 403:
                print(f"      Access forbidden - LinkedIn blocking")
                return None
                
            else:
                print(f"      HTTP {response.status_code}")
                continue
                
        except requests.exceptions.Timeout:
            print(f"      Timeout on attempt {attempt+1}")
            continue
            
        except requests.exceptions.ConnectionError as e:
            print(f"      Connection error: {str(e)[:50]}")
            continue
            
        except Exception as e:
            print(f"      Error on attempt {attempt+1}: {str(e)[:50]}")
            continue
    
    print(f"      All attempts failed for this link")
    return None

def scrape_linkedin_links(query, location, unique_links_lock, unique_links, max_jobs):
    """Scrape LinkedIn job links - NOW RESPECTS MAX_JOBS"""
    session = create_requests_session()
    links_found = 0
    
    try:
        for i in range(10):  # Max 10 pages (25 jobs per page = 250 max)
            # ADDED: Stop if we've reached max_jobs
            with unique_links_lock:
                if len(unique_links) >= max_jobs:
                    print(f"   ✓ Reached max jobs limit ({max_jobs}), stopping")
                    break
            
            page_num = i * 25
            formatted_query = quote_plus(query)
            formatted_location = quote_plus(location)
            
            search_url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={formatted_query}&location={formatted_location}&start={page_num}"
            
            print(f"   LinkedIn: '{query}' in {location} - page {i+1}")
            
            try:
                response = session.get(search_url, timeout=25)
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, "html.parser")
                    link_elements = soup.select('a.base-card__full-link')
                    
                    new_links = 0
                    for link_el in link_elements:
                        href = link_el.get('href')
                        if href:
                            with unique_links_lock:
                                if len(unique_links) >= max_jobs:
                                    break
                                if href not in unique_links:
                                    unique_links.add(href)
                                    links_found += 1
                                    new_links += 1
                    
                    print(f"    Page {i+1}: Found {new_links} new links (Total: {len(unique_links)})")
                    
                    if not link_elements:
                        print(f"     No more links found on page {i+1}, stopping")
                        break
                        
                elif response.status_code == 429:
                    print(f"    Rate limited, stopping this query")
                    break
                    
                else:
                    print(f"    HTTP {response.status_code} on page {i+1}")
                    
            except Exception as e:
                print(f"    Error on page {i+1}: {str(e)[:50]}")
                continue
            
            time.sleep(random.uniform(3, 7))
            
    except Exception as e:
        print(f"Fatal error scraping LinkedIn for '{query}': {e}")
    
    return links_found

def run_scraper_with_config(user_config=None):
    """Main scraper function - NOW USES MAX_JOBS"""
    start_time = time.time()
    
    config = user_config if user_config else JOB_CONFIG
    
    # ADDED: Extract max_jobs from config
    max_jobs = config.get('max_jobs', 50)
    
    if user_config and 'job_titles' in user_config and user_config['job_titles']:
        search_queries = user_config['job_titles']
        print(f"Using {len(search_queries)} job titles directly from UI configuration.")
    else:
        search_queries = build_search_queries(config)

    print("="*80)
    print(f"ENHANCED JOB SCRAPER")
    print("="*80)
    print(f"Configuration:")
    print(f"   • Queries: {len(search_queries)}")
    print(f"   • Max Jobs: {max_jobs}")
    print(f"   • Workers: 3")
    print("="*80)
    
    try:
        ml_extractor = AdvancedMLExtractor(models_dir=MODELS_DIR)
    except RuntimeError as e:
        print(f"[FATAL ERROR] {e}")
        return

    print("\n[1/3] Collecting job links from LinkedIn...")
    unique_links = set()
    unique_links_lock = Lock()
    
    limited_queries = search_queries[:10]
    print(f"🔧 Processing {len(limited_queries)} queries")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        
        for location in config['locations']:
            for query in limited_queries:
                future = executor.submit(
                    scrape_linkedin_links, query, location, unique_links_lock, unique_links, max_jobs
                )
                futures.append(future)
        
        completed = 0
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            completed += 1
            print(f"    Completed {completed}/{len(futures)} - {result} links found")
            
            # ADDED: Stop if we've reached max_jobs
            if len(unique_links) >= max_jobs:
                print(f"\n    ✓ Reached target of {max_jobs} jobs!")
                break
    
    # ADDED: Limit links to max_jobs
    links_to_process = list(unique_links)[:max_jobs]
    print(f"\n    Collected {len(links_to_process)} job links (limit: {max_jobs})")
    
    if not links_to_process:
        print("No links found. LinkedIn might be blocking requests.")
        return
    
    print(f"\n[2/3] Extracting job details...")
    all_jobs = []
    
    batch_size = 50
    batches = [links_to_process[i:i + batch_size] for i in range(0, len(links_to_process), batch_size)]
    
    for batch_num, batch in enumerate(batches):
        print(f"\n    Processing batch {batch_num + 1}/{len(batches)} ({len(batch)} links)")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future_to_link = {
                executor.submit(extract_with_requests_enhanced, link, ml_extractor): link 
                for link in batch
            }
            
            batch_jobs = []
            for i, future in enumerate(concurrent.futures.as_completed(future_to_link)):
                result = future.result()
                if result:
                    result['Source Site'] = 'linkedin'
                    batch_jobs.append(result)
                    print(f"      {i+1}/{len(batch)} - Success")
                else:
                    print(f"      {i+1}/{len(batch)} - Failed")
            
            all_jobs.extend(batch_jobs)
            print(f"    Batch {batch_num + 1}: {len(batch_jobs)}/{len(batch)} successful")
        
        if batch_num < len(batches) - 1:
            delay = random.uniform(10, 20)
            print(f"    Waiting {delay:.1f}s before next batch...")
            time.sleep(delay)
    
    print(f"\n[3/3] Saving {len(all_jobs)} jobs...")
    if all_jobs:
        df = pd.DataFrame(all_jobs)
        required_columns = ['Job Title', 'Company', 'Location', 'Job Type', 'Experience', 'Posted Date', 'Job Link', 'Source Site']
        for col in required_columns:
            if col not in df.columns:
                if col == 'Experience':
                    df[col] = "0"
                elif col == 'Job Type':
                    df[col] = "Not Specified"
                else:
                    df[col] = "Not Specified"
        
        df = df[required_columns]
        df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
        print(f"    ✓ Data saved to {OUTPUT_FILE}")
        
        print("\nEXTRACTION QUALITY REPORT:")
        print("="*80)
        for field in ['Job Title', 'Company', 'Location', 'Job Type', 'Experience', 'Posted Date']:
            found_count = df[df[field] != "Not Specified"][field].count()
            percentage = (found_count/len(df)*100) if len(df) > 0 else 0
            
            # Color coding for quality
            if percentage >= 80:
                status = "✅ EXCELLENT"
            elif percentage >= 60:
                status = "✓ GOOD"
            elif percentage >= 40:
                status = "⚠️ FAIR"
            else:
                status = "❌ POOR"
            
            print(f"  {field:20s}: {found_count:4d}/{len(df):4d} ({percentage:5.1f}%) {status}")
        
        # Additional quality checks
        print("\nDATA QUALITY CHECKS:")
        print("="*80)
        
        # Check for common issues
        similar_searches_count = df[df['Company'].str.contains('Similar Search', case=False, na=False)].shape[0]
        industries_count = df[df['Job Type'].str.contains('Industries', case=False, na=False)].shape[0]
        
        if similar_searches_count > 0:
            print(f"  ⚠️ Found {similar_searches_count} 'Similar Searches' in Company field (cleaned)")
        else:
            print(f"  ✓ No 'Similar Searches' found in Company field")
        
        if industries_count > 0:
            print(f"  ⚠️ Found {industries_count} 'Industries' in Job Type field (cleaned)")
        else:
            print(f"  ✓ No 'Industries' found in Job Type field")
        
        # Enhanced experience statistics
        zero_exp_count = df[df['Experience'] == "0"].shape[0]
        exp_with_range = df[df['Experience'].str.contains('-', na=False)].shape[0]
        exp_with_plus = df[df['Experience'].str.contains('+', na=False)].shape[0]
        valid_exp_count = len(df) - zero_exp_count
        
        print(f"  📊 Experience Statistics:")
        print(f"     • Jobs with valid experience: {valid_exp_count}/{len(df)} ({valid_exp_count/len(df)*100:.1f}%)")
        print(f"     • Jobs with experience range: {exp_with_range}/{len(df)} ({exp_with_range/len(df)*100:.1f}%)")
        print(f"     • Jobs with 'X+ years': {exp_with_plus}/{len(df)} ({exp_with_plus/len(df)*100:.1f}%)")
        print(f"     • Jobs marked as fresher (0): {zero_exp_count}/{len(df)} ({zero_exp_count/len(df)*100:.1f}%)")
        
        # Experience value distribution
        print(f"\n  📈 Experience Value Distribution:")
        exp_counts = df['Experience'].value_counts().head(10)
        for exp, count in exp_counts.items():
            percentage = (count/len(df)*100)
            print(f"     • {exp:8s}: {count:3d} jobs ({percentage:5.1f}%)")
        
        not_specified_job_type = df[df['Job Type'] == "Not Specified"].shape[0]
        print(f"\n  ℹ️ Jobs with unspecified type: {not_specified_job_type}/{len(df)} ({not_specified_job_type/len(df)*100:.1f}%)")
        
        # Company statistics
        unique_companies = df[df['Company'] != "Not Specified"]['Company'].nunique()
        print(f"  🏢 Unique companies found: {unique_companies}")
        
        # Location statistics
        unique_locations = df[df['Location'] != "Not Specified"]['Location'].nunique()
        print(f"  📍 Unique locations found: {unique_locations}")
        
        # Job Type statistics
        job_type_counts = df['Job Type'].value_counts()
        print(f"\n  💼 Job Type Distribution:")
        for job_type, count in job_type_counts.items():
            percentage = (count/len(df)*100)
            print(f"     • {job_type:12s}: {count:3d} jobs ({percentage:5.1f}%)")
    
    execution_time = (time.time() - start_time) / 60
    print("\n" + "=" * 80)
    print(f"✅ SCRAPING COMPLETED!")
    print(f"⏱️ Time: {execution_time:.2f} minutes")
    print(f"📊 Jobs: {len(all_jobs)}")
    print(f"🎯 Target: {max_jobs} jobs")
    print(f"📈 Success Rate: {len(all_jobs)/max_jobs*100:.1f}%")
    print("=" * 80)
    
    # Final summary with enhanced experience metrics
    if all_jobs:
        df = pd.DataFrame(all_jobs)
        exp_extraction_rate = (df[df['Experience'] != "0"]['Experience'].count() / len(df)) * 100
        print(f"\n🎯 EXPERIENCE EXTRACTION SUMMARY:")
        print(f"   • Successful experience extraction: {exp_extraction_rate:.1f}%")
        print(f"   • Enhanced patterns applied: ✓")
        print(f"   • Fallback logic active: ✓")
        
        # Show sample of extracted experiences
        print(f"\n📋 Sample of Extracted Experiences:")
        sample_experiences = df[df['Experience'] != "0"][['Job Title', 'Experience']].head(5)
        for _, row in sample_experiences.iterrows():
            print(f"   • {row['Job Title'][:30]}... -> {row['Experience']}")
    
    return all_jobs

# Test function for experience extraction
def test_experience_extraction():
    """Test the enhanced experience extraction on sample data"""
    print("🧪 TESTING ENHANCED EXPERIENCE EXTRACTION")
    print("=" * 60)
    
    try:
        ml_extractor = AdvancedMLExtractor(models_dir=MODELS_DIR)
        print("✅ ML Extractor loaded successfully")
    except Exception as e:
        print(f"❌ Failed to load ML Extractor: {e}")
        return
    
    # Test cases for experience extraction
    test_cases = [
        ("2-4 years of experience", "2-4"),
        ("5+ years experience", "5+"),
        ("3 years exp", "3"),
        ("1 to 3 years", "1-3"),
        ("7+ yrs", "7+"),
        ("fresher", "0"),
        ("entry level", "0"),
        ("senior developer", "5+"),
        ("junior role", "1-2"),
        ("mid-level position", "3-5"),
        ("0-1 years", "0-1"),
        ("10 years experience", "10"),
        ("2.5 years", "2"),
        ("1 year", "1"),
        ("no experience required", "0"),
        ("recent graduate", "0"),
        ("minimum 3 years", "3"),
        ("4-6 yrs", "4-6"),
        ("8+ years of exp", "8+"),
        ("fresh graduate", "0"),
        ("0 years", "0"),
        ("15+ years", "15+"),
        ("2 - 5 years", "2-5"),
    ]
    
    print("\n📊 Testing Text Pattern Extraction:")
    print("-" * 40)
    
    success_count = 0
    for test_text, expected in test_cases:
        result = ml_extractor._extract_experience_from_text_enhanced(test_text)
        status = "✅" if result == expected else "❌"
        print(f"{status} '{test_text}' -> '{result}' (expected: '{expected}')")
        if result == expected:
            success_count += 1
    
    accuracy = success_count/len(test_cases)*100
    print(f"\n🎯 Pattern Extraction Accuracy: {success_count}/{len(test_cases)} ({accuracy:.1f}%)")
    
    if accuracy >= 80:
        print("✅ Experience extraction is working well!")
    elif accuracy >= 60:
        print("⚠️ Experience extraction needs improvement")
    else:
        print("❌ Experience extraction needs significant improvement")

if __name__ == "__main__":
    # Check if we should run tests or full scraping
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == 'test':
        test_experience_extraction()
    else:
        # Run with 50 samples for testing
        test_config = JOB_CONFIG.copy()
        test_config['max_jobs'] = 50
        print("🚀 Running enhanced scraper with 50 samples...")
        run_scraper_with_config(test_config)