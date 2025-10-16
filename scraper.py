import os
import time
import random
import pandas as pd
import numpy as np
from urllib.parse import urljoin, quote_plus, quote
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
    'platforms': ['linkedin', 'indeed', 'naukri'],
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
    """Build better search queries that target specific experience levels"""
    base_titles = config.get('job_titles', [])
    experience_keywords = [
        "",  # No experience filter
        "senior", "lead", "principal",
        "mid level", "mid-level", 
        "junior", "entry level", "fresher",
        "2 years experience", "3 years experience", "5 years experience"
    ]
    
    enhanced_queries = []
    for title in base_titles:
        for exp_keyword in experience_keywords:
            if exp_keyword:
                enhanced_queries.append(f"{title} {exp_keyword}")
            else:
                enhanced_queries.append(title)
    
    return list(set(enhanced_queries))  # Remove duplicates

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
            'connect', 'message', 'sponsored', 'promoted', 'featured', 'upload resume',
            'create alert', 'job alert', 'email me', 'get notified'
        ]
        
        # Company blacklist - common false positives
        self.company_blacklist = [
            'similar searches', 'industries', 'linkedin', 'indeed', 'naukri',
            'job title', 'location', 'experience', 'posted date', 'job type',
            'companies', 'show more', 'see all', 'open jobs', 'job search',
            'upload resume', 'create alert', 'get notified'
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
            'remote', 'hybrid', 'onsite', 'on-site', 'permanent'
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
        """Enhanced experience extraction with more patterns"""
        if not text:
            return "0"
        
        text_lower = text.lower()
        
        # Expanded patterns for better matching
        patterns = [
            # Range patterns: 2-4 years, 2 to 4 years, 2 - 4 years
            (r'(\d+)\s*[-–—to]+\s*(\d+)\s*(?:years?|yrs?|y)', lambda m: f"{m.group(1)}-{m.group(2)}"),
            # Plus patterns: 5+ years, 3+ yrs
            (r'(\d+)\s*\+\s*(?:years?|yrs?|y)', lambda m: f"{m.group(1)}+"),
            # Minimum patterns: min 3 years, minimum 2 years
            (r'(?:min|minimum)\s+(\d+)\s*(?:years?|yrs?|y)', lambda m: f"{m.group(1)}+"),
            # Exact years: 3 years, 5 yrs
            (r'(\d+)\s*(?:years?|yrs?|y)(?:\s+experience)?', lambda m: m.group(1)),
            # Senior/lead positions (typically 5+ years)
            (r'\b(senior|lead|principal|sr\.)\b', lambda m: "5+"),
            # Mid-level positions (typically 3-5 years)
            (r'\b(mid[-]?level|mid[-]?senior)\b', lambda m: "3-5"),
            # Junior/entry level (typically 0-2 years)
            (r'\b(junior|jr\.|entry[-]?level)\b', lambda m: "0-2"),
            # Fresher/graduate (0 years)
            (r'\b(fresher|fresh|graduate|recent graduate|no experience|0\s*years?)\b', lambda m: "0"),
        ]
        
        # Try all patterns
        for pattern, handler in patterns:
            matches = re.finditer(pattern, text_lower)
            for match in matches:
                try:
                    result = handler(match)
                    if result:
                        return result
                except:
                    continue
        
        # Fallback: Look for any numbers that might indicate experience
        number_matches = re.findall(r'\b(\d+)\s*(?=years?|yrs?|y\b)', text_lower)
        if number_matches:
            return number_matches[0]
        
        return "0"

    def _extract_experience_from_description(self, html_content):
        """Extract experience from job description/requirements with enhanced patterns"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Look for description sections - common selectors for job descriptions
        description_selectors = [
            '.description', '.job-description', '.description__text',
            '.jobs-description', '.job-details', '.description-content',
            '[data-testid="description"]', '.description__container',
            'div[class*="description"]', 'div[class*="Description"]',
            'section[class*="description"]', '.jobs-box__html-content',
            # LinkedIn specific
            '.description__text--rich', '.show-more-less-html__markup',
            # Indeed specific  
            '.jobsearch-JobComponent-description', '#jobDescriptionText',
            # Naukri specific
            '.dang-inner-html', '.job-desc'
        ]
        
        description_text = ""
        
        # Try to find description using multiple selectors
        for selector in description_selectors:
            elements = soup.select(selector)
            for element in elements:
                text = element.get_text(strip=True)
                if len(text) > 200:  # Likely a real description
                    description_text = text
                    break
            if description_text:
                break
        
        # If no description found with selectors, get all text and find the largest block
        if not description_text:
            all_text = soup.get_text()
            # Split by lines and find the largest text block (likely description)
            lines = [line.strip() for line in all_text.split('\n') if line.strip()]
            if lines:
                # Find the longest continuous text block
                description_text = max(lines, key=len)
        
        if not description_text:
            return "0"
        
        # Enhanced experience patterns for descriptions
        text_lower = description_text.lower()
        
        # More comprehensive patterns for description text
        patterns = [
            # Range patterns with various formats
            (r'(\d+)\s*[-–—to]+\s*(\d+)\s*(?:years?|yrs?|y)\s*(?:of)?\s*(?:experience|exp)', 
             lambda m: f"{m.group(1)}-{m.group(2)}"),
            (r'(\d+)\s*-\s*(\d+)\s*(?:years?|yrs?|y)\s*(?:of)?\s*(?:experience|exp)', 
             lambda m: f"{m.group(1)}-{m.group(2)}"),
            
            # Plus patterns
            (r'(\d+)\s*\+\s*(?:years?|yrs?|y)\s*(?:of)?\s*(?:experience|exp)', 
             lambda m: f"{m.group(1)}+"),
            
            # Minimum/at least patterns
            (r'(?:min|minimum|at least|atleast)\s+(\d+)\s*(?:years?|yrs?|y)\s*(?:of)?\s*(?:experience|exp)', 
             lambda m: f"{m.group(1)}+"),
            
            # Exact years with context
            (r'(\d+)\s*(?:years?|yrs?|y)\s*(?:of)?\s*(?:experience|exp)', 
             lambda m: m.group(1)),
            
            # Seniority-based extraction
            (r'\b(senior|sr\.|lead|principal|staff)\s+(?:.*?\s+)?(?:engineer|developer|analyst|scientist)\b', 
             lambda m: "5+"),
            (r'\b(mid[-]?level|mid[-]?senior|experienced)\b', 
             lambda m: "3-5"),
            (r'\b(junior|jr\.|entry[-]?level|associate)\b', 
             lambda m: "0-2"),
            (r'\b(fresher|fresh|graduate|recent graduate|no experience|0\s*years?)\b', 
             lambda m: "0"),
            
            # Industry standard ranges
            (r'\b(one|two|three|four|five|six|seven|eight|nine|ten)\+?\s+(?:years?|yrs?)\b', 
             lambda m: self._word_to_number(m.group(1))),
        ]
        
        # Try all patterns
        for pattern, handler in patterns:
            matches = re.finditer(pattern, text_lower)
            for match in matches:
                try:
                    result = handler(match)
                    if result and result != "0":  # Only return if we found something meaningful
                        print(f"        📝 Found experience in description: '{match.group()}' -> '{result}'")
                        return result
                except:
                    continue
        
        # Fallback: Look for any number that's likely experience
        number_patterns = [
            r'\b(\d+)\s*[-–—to]+\s*\d+\s*(?:years?|yrs?|y)',
            r'\b(\d+)\s*\+\s*(?:years?|yrs?|y)',
            r'\b(\d+)\s*(?:years?|yrs?|y)\s*(?:of)?\s*experience'
        ]
        
        for pattern in number_patterns:
            matches = re.findall(pattern, text_lower)
            if matches:
                print(f"        📝 Found experience number: {matches[0]}")
                return matches[0]
        
        return "0"

    def _word_to_number(self, word):
        """Convert word numbers to digits"""
        word_map = {
            'one': '1', 'two': '2', 'three': '3', 'four': '4', 'five': '5',
            'six': '6', 'seven': '7', 'eight': '8', 'nine': '9', 'ten': '10'
        }
        return word_map.get(word.lower(), "0")

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
        
        # ENHANCED: Experience is already extracted from description, just validate
        exp_value = cleaned_data.get('Experience', '0')
        if exp_value == "0":
            # If we got 0 from description, check if we should mark as "Not Specified" instead
            cleaned_data['Experience'] = "0"  # Keep as 0 for now
        
        return cleaned_data

    def predict(self, html_content):
        """Main prediction method with enhanced experience extraction from description"""
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
        
        # Extract basic fields first
        # Extract basic fields first - ONE FIELD AT A TIME
        field_priority = ['Job Title', 'Company', 'Location', 'Job Type', 'Posted Date']

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
                best_candidates = []
                
                # First, collect all valid candidates for this field
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
                    elif field == 'Location':
                        # Basic location validation
                        is_valid = len(candidate_text) > 2 and ',' in candidate_text
                    elif field == 'Job Title':
                        # Basic title validation - should not be too long
                        is_valid = 2 <= len(candidate_text) <= 100
                    
                    min_confidence = 0.7 if field in ['Company', 'Job Type'] else 0.6
                    
                    if is_valid and score >= min_confidence:
                        best_candidates.append((idx, score, candidate_text))
                
                # Now select the BEST candidate (highest score) for this field
                if best_candidates:
                    # Sort by confidence score descending
                    best_candidates.sort(key=lambda x: x[1], reverse=True)
                    best_idx, best_score, best_text = best_candidates[0]
                    
                    # Additional validation to ensure we pick the most appropriate one
                    if field == 'Job Title':
                        # For job titles, prefer shorter, more specific titles
                        filtered_titles = [cand for cand in best_candidates if len(cand[2]) <= 80]
                        if filtered_titles:
                            best_idx, best_score, best_text = filtered_titles[0]
                    
                    extracted_data[field] = best_text
                    confidence_scores[field] = best_score
                    used_indices.add(best_idx)
                    print(f"        {field}: {extracted_data[field][:50]}... (confidence: {best_score:.3f})")
                    
                    # Debug: Show why we rejected others
                    if len(best_candidates) > 1:
                        print(f"          ⚡ Rejected {len(best_candidates)-1} other candidates for {field}")
                else:
                    print(f"        {field}: No valid candidate found (tried {len(probabilities)} candidates)")
                    
            except Exception as e:
                print(f"        [!] Error predicting {field}: {e}")
                continue
        
        # ENHANCED: Extract experience from description (most important)
        print("        🔍 Searching for experience in description...")
        experience_from_description = self._extract_experience_from_description(html_content)
        extracted_data['Experience'] = experience_from_description
        
        # Enhanced post-processing
        cleaned_data = self._post_process_extracted_data_enhanced(extracted_data)
        
        # Final experience fallback - check job title for experience hints
        if cleaned_data.get('Experience') in ["0", "Not Specified"]:
            title = cleaned_data.get('Job Title', '').lower()
            if any(word in title for word in ['intern', 'fresher', 'trainee', 'graduate']):
                cleaned_data['Experience'] = "0"
            elif any(word in title for word in ['senior', 'lead', 'principal', 'staff', 'sr.']):
                cleaned_data['Experience'] = "5+"
            elif any(word in title for word in ['mid-level', 'midlevel', 'experienced']):
                cleaned_data['Experience'] = "3-5"
            elif any(word in title for word in ['junior', 'jr.', 'entry', 'associate']):
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
                print(f"      Access forbidden - Site blocking")
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

def scrape_linkedin_links(query, location, unique_links_lock, unique_links):
    """Enhanced LinkedIn scraping with better headers and rotation"""
    
    # Enhanced headers to look more like a real browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
    }
    
    try:
        # Encode search parameters
        search_query = f"{query} {location}"
        encoded_query = quote(search_query)
        
        # Multiple LinkedIn search URL patterns
        search_urls = [
            f"https://www.linkedin.com/jobs/search/?keywords={encoded_query}",
            f"https://www.linkedin.com/jobs/search/?keywords={quote(query)}&location={quote(location)}",
        ]
        
        links_found = 0
        
        for search_url in search_urls:
            try:
                print(f"      Searching: {query} in {location}")
                
                response = requests.get(
                    search_url, 
                    headers=headers,
                    timeout=30,
                    allow_redirects=True
                )
                
                if response.status_code != 200:
                    print(f"      ❌ LinkedIn returned status {response.status_code}")
                    continue
                
                # Parse the page for job links
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Multiple possible selectors for LinkedIn job links
                link_selectors = [
                    'a[href*="/jobs/view/"]',
                    'a[data-tracking-control-name*="public_jobs"]',
                    '.job-search-card',
                    '.jobs-search-results__list-item',
                ]
                
                for selector in link_selectors:
                    job_cards = soup.select(selector)
                    for card in job_cards:
                        link = card.get('href')
                        if link and '/jobs/view/' in link:
                            full_link = link if link.startswith('http') else f"https://www.linkedin.com{link}"
                            
                            with unique_links_lock:
                                if full_link not in unique_links:
                                    unique_links.add(full_link)
                                    links_found += 1
                
                # Also look for job IDs in data attributes
                job_elements = soup.find_all(attrs={"data-job-id": True})
                for element in job_elements:
                    job_id = element.get('data-job-id')
                    if job_id:
                        job_link = f"https://www.linkedin.com/jobs/view/{job_id}"
                        with unique_links_lock:
                            if job_link not in unique_links:
                                unique_links.add(job_link)
                                links_found += 1
                
                if links_found > 0:
                    break  # Found links, no need to try other URLs
                    
            except Exception as e:
                print(f"      ⚠️ Error with URL {search_url}: {e}")
                continue
        
        # Random delay between requests to avoid blocking
        time.sleep(random.uniform(2, 5))
        
        return links_found
        
    except Exception as e:
        print(f"      ❌ Failed to scrape LinkedIn for {query} in {location}: {e}")
        return 0


def scrape_indeed_links(query, location, unique_links_lock, unique_links):
    """Scrape job links from Indeed"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        base_url = "https://in.indeed.com/jobs"
        params = {
            'q': query,
            'l': location,
            'sort': 'date'
        }
        
        response = requests.get(base_url, params=params, headers=headers, timeout=30)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Indeed job link selectors
            selectors = [
                'a[data-jk]',
                '.jobtitle a',
                'a[class*="jobTitle"]'
            ]
            
            links_found = 0
            for selector in selectors:
                links = soup.select(selector)
                for link in links:
                    href = link.get('href')
                    if href and 'jk=' in href:
                        full_url = f"https://in.indeed.com{href}" if href.startswith('/') else href
                        with unique_links_lock:
                            if full_url not in unique_links:
                                unique_links.add(full_url)
                                links_found += 1
            
            time.sleep(random.uniform(1, 3))
            return links_found
            
    except Exception as e:
        print(f"      ❌ Indeed scraping failed: {e}")
        return 0

def scrape_naukri_links(query, location, unique_links_lock, unique_links):
    """Scrape job links from Naukri.com"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        base_url = "https://www.naukri.com/jobs"
        params = {
            'k': query,
            'l': location
        }
        
        response = requests.get(base_url, params=params, headers=headers, timeout=30)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            links_found = 0
            links = soup.find_all('a', href=True)
            for link in links:
                href = link['href']
                if '/job/' in href and 'naukri.com' in href:
                    with unique_links_lock:
                        if href not in unique_links:
                            unique_links.add(href)
                            links_found += 1
            
            time.sleep(random.uniform(1, 3))
            return links_found
            
    except Exception as e:
        print(f"      ❌ Naukri scraping failed: {e}")
        return 0
    
def run_scraper_with_config(user_config=None):
    """Main scraper function with multiple sources - FIXED FOR UI"""
    start_time = time.time()
    
    config = user_config if user_config else JOB_CONFIG
    
    # Handle UI config vs default config structure
    if 'job_titles' in config:
        # This is from UI - use job_titles directly
        search_queries = config['job_titles']
        print(f"Using {len(search_queries)} job titles from UI configuration")
    else:
        # This is default config - build from categories
        search_queries = []
        for category, enabled in config.get('categories', {}).items():
            if enabled and category in JOB_CATEGORIES:
                search_queries.extend(JOB_CATEGORIES[category])
        search_queries = list(set(search_queries))
    
    # Get scraping intensity from UI or use default
    scraping_intensity = config.get('scraping_intensity', 'balanced')
    
    # Determine limits based on intensity
    if scraping_intensity == 'light':
        max_queries = min(10, len(search_queries))
        max_locations = min(3, len(config['locations']))
        max_workers = 2
    elif scraping_intensity == 'comprehensive':
        max_queries = len(search_queries)  # ALL queries
        max_locations = len(config['locations'])  # ALL locations
        max_workers = 4
    else:  # balanced (default)
        max_queries = min(20, len(search_queries))
        max_locations = min(5, len(config['locations']))
        max_workers = 3
    
    # Apply limits
    queries_to_process = search_queries[:max_queries]
    locations_to_process = config['locations'][:max_locations]
    
    print("="*80)
    print(f"ENHANCED JOB SCRAPER - {scraping_intensity.upper()} MODE")
    print("="*80)
    print(f"Configuration:")
    print(f"   • Queries: {len(queries_to_process)}/{len(search_queries)}")
    print(f"   • Locations: {len(locations_to_process)}/{len(config['locations'])}")
    print(f"   • Workers: {max_workers}")
    print(f"   • Platforms: {config.get('platforms', ['linkedin'])}")
    print("="*80)
    
    try:
        ml_extractor = AdvancedMLExtractor(models_dir=MODELS_DIR)
    except RuntimeError as e:
        print(f"[FATAL ERROR] {e}")
        return []

    print("\n[1/3] Collecting job links from multiple sources...")
    unique_links = set()
    unique_links_lock = Lock()
    
    print(f"🔧 Processing {len(queries_to_process)} queries across {len(locations_to_process)} locations")
    
    # Get platforms from config - ensure it's a list
    platforms = config.get('platforms', ['linkedin'])
    if isinstance(platforms, str):
        platforms = [platforms]  # Convert string to list if needed
        
    print(f"🔄 Scraping from platforms: {', '.join(platforms)}")

    # Scrape from selected sources only
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        
        for location in locations_to_process:
            for query in queries_to_process:
                for platform in platforms:
                    if platform == 'linkedin':
                        print(f"      🔍 LinkedIn: {query} in {location}")
                        future = executor.submit(
                            scrape_linkedin_links, query, location, unique_links_lock, unique_links
                        )
                        futures.append(future)
                        
                    elif platform == 'indeed':
                        print(f"      🔍 Indeed: {query} in {location}")
                        future = executor.submit(
                            scrape_indeed_links, query, location, unique_links_lock, unique_links
                        )
                        futures.append(future)
                        
                    elif platform == 'naukri':
                        print(f"      🔍 Naukri: {query} in {location}")
                        future = executor.submit(
                            scrape_naukri_links, query, location, unique_links_lock, unique_links
                        )
                        futures.append(future)
                    
                    else:
                        print(f"      ⚠️ Unknown platform: {platform}")
        
        completed = 0
        total_futures = len(futures)
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            completed += 1
            progress = (completed / total_futures) * 100
            print(f"    Progress: {completed}/{total_futures} ({progress:.1f}%) - {result} links found")
    
    links_to_process = list(unique_links)
    print(f"\n    ✅ Collected {len(links_to_process)} job links from all sources")
    
    if not links_to_process:
        print("❌ No links found. Job sites might be blocking requests.")
        return []
    
    print(f"\n[2/3] Extracting job details from {len(links_to_process)} links...")
    all_jobs = []
    
    # Process in smaller batches to avoid overwhelming servers
    batch_size = 20
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
                    result['Source Site'] = 'multiple'
                    batch_jobs.append(result)
                    print(f"      {i+1}/{len(batch)} - Success")
                else:
                    print(f"      {i+1}/{len(batch)} - Failed")
            
            all_jobs.extend(batch_jobs)
            print(f"    ✅ Batch {batch_num + 1}: {len(batch_jobs)}/{len(batch)} successful")
        
        # Delay between batches to be respectful to servers
        if batch_num < len(batches) - 1:
            delay = random.uniform(5, 10)
            print(f"    ⏳ Waiting {delay:.1f}s before next batch...")
            time.sleep(delay)
    
    print(f"\n[3/3] Saving {len(all_jobs)} jobs...")
    if all_jobs:
        df = pd.DataFrame(all_jobs)
        
        # Ensure all required columns exist
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
        print(f"    ✅ Data saved to {OUTPUT_FILE}")
        
        # Generate quality report directly in main function
        total_jobs = len(all_jobs)
        print("\nEXTRACTION QUALITY REPORT:")
        print("="*80)
        
        for field in ['Job Title', 'Company', 'Location', 'Job Type', 'Experience', 'Posted Date']:
            found_count = df[df[field] != "Not Specified"][field].count()
            percentage = (found_count/total_jobs*100) if total_jobs > 0 else 0
            
            # Color coding for quality
            if percentage >= 80:
                status = "✅ EXCELLENT"
            elif percentage >= 60:
                status = "✓ GOOD"
            elif percentage >= 40:
                status = "⚠️ FAIR"
            else:
                status = "❌ POOR"
            
            print(f"  {field:20s}: {found_count:4d}/{total_jobs:4d} ({percentage:5.1f}%) {status}")
        
        # Enhanced experience statistics (with regex fix)
        print("\nDATA QUALITY CHECKS:")
        print("="*80)
        
        zero_exp_count = df[df['Experience'] == "0"].shape[0]
        exp_with_range = df[df['Experience'].str.contains('-', na=False, regex=False)].shape[0]
        exp_with_plus = df[df['Experience'].str.contains('+', na=False, regex=False)].shape[0]
        valid_exp_count = total_jobs - zero_exp_count
        
        print(f"  📊 Experience Statistics:")
        print(f"     • Jobs with valid experience: {valid_exp_count}/{total_jobs} ({valid_exp_count/total_jobs*100:.1f}%)")
        print(f"     • Jobs with experience range: {exp_with_range}/{total_jobs} ({exp_with_range/total_jobs*100:.1f}%)")
        print(f"     • Jobs with 'X+ years': {exp_with_plus}/{total_jobs} ({exp_with_plus/total_jobs*100:.1f}%)")
        print(f"     • Jobs marked as fresher (0): {zero_exp_count}/{total_jobs} ({zero_exp_count/total_jobs*100:.1f}%)")
        
        # Experience value distribution
        print(f"\n  📈 Experience Value Distribution:")
        exp_counts = df['Experience'].value_counts().head(10)
        for exp, count in exp_counts.items():
            percentage = (count/total_jobs*100)
            print(f"     • {str(exp):8s}: {count:3d} jobs ({percentage:5.1f}%)")
        
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
            percentage = (count/total_jobs*100)
            print(f"     • {job_type:12s}: {count:3d} jobs ({percentage:5.1f}%)")
    
    execution_time = (time.time() - start_time) / 60
    print("\n" + "=" * 80)
    print(f"✅ SCRAPING COMPLETED!")
    print(f"⏱️  Time: {execution_time:.2f} minutes")
    print(f"📊 Jobs: {len(all_jobs)}")
    print("=" * 80)
    
    # Final summary with enhanced experience metrics
    if all_jobs:
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
        print("🚀 Running enhanced scraper without job limits...")
        run_scraper_with_config()