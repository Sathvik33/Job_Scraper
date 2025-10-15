import pandas as pd
import numpy as np
import os
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier
import joblib
from datetime import datetime

# --- Configuration ---
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'Data')
MODELS_DIR = os.path.join(BASE_DIR, 'ML_Models')
INPUT_FILE = os.path.join(DATA_DIR, 'jobs_raw_ml_extracted.csv')
OUTPUT_FILE = os.path.join(DATA_DIR, 'jobs_advanced.csv')

os.makedirs(MODELS_DIR, exist_ok=True)

class DateConverter:
    @staticmethod
    def convert_to_standard_date(date_str):
        if not date_str or pd.isna(date_str) or date_str == 'Not specified':
            return 'Recent'
        
        date_str_lower = str(date_str).lower().strip()
        
        if any(keyword in date_str_lower for keyword in ['hour', 'just now', 'today']):
            return "Today"
        if 'yesterday' in date_str_lower:
            return "1 day ago"
            
        try:
            match = re.search(r'(\d+)\+?\s*days?', date_str_lower)
            if match:
                days = int(match.group(1))
                return f"{days} days ago"
        except:
            pass
        
        return "Recent"

class MLRoleClassifier:
    def __init__(self):
        self.role_model = None
        self.vectorizer = None
        self.role_mappings = {
            'Backend Developer': ['backend', 'api', 'microservices', 'java', 'python', 'node'],
            'Frontend Developer': ['frontend', 'ui', 'ux', 'javascript', 'react', 'vue', 'angular'],
            'Full Stack Developer': ['full stack', 'fullstack'],
            'Data Scientist': ['data science', 'analytics', 'statistics'],
            'ML/AI Engineer': ['machine learning', 'ai', 'deep learning', 'nlp'],
            'DevOps Engineer': ['devops', 'cloud', 'aws', 'azure', 'gcp', 'ci/cd'],
            'Software Engineer': ['software engineer', 'sde', 'developer', 'programmer']
        }
    
    def _rule_based_classify(self, title):
        title_lower = str(title).lower()
        for role, keywords in self.role_mappings.items():
            if any(keyword in title_lower for keyword in keywords):
                return role
        return 'Software Engineer'

    def train_or_load(self, job_titles):
        model_path = os.path.join(MODELS_DIR, 'role_classifier.pkl')
        vectorizer_path = os.path.join(MODELS_DIR, 'role_vectorizer.pkl')
        try:
            self.role_model = joblib.load(model_path)
            self.vectorizer = joblib.load(vectorizer_path)
            print("Loaded pre-trained role classification model.")
        except FileNotFoundError:
            print("Training new role classification model...")
            training_labels = [self._rule_based_classify(title) for title in job_titles]
            self.vectorizer = TfidfVectorizer(max_features=500, ngram_range=(1, 3))
            X = self.vectorizer.fit_transform(job_titles)
            self.role_model = RandomForestClassifier(n_estimators=100, random_state=42)
            self.role_model.fit(X, training_labels)
            joblib.dump(self.role_model, model_path)
            joblib.dump(self.vectorizer, vectorizer_path)
            print("Role classifier trained and saved.")

    def predict(self, job_title):
        if self.role_model is None or self.vectorizer is None:
            return self._rule_based_classify(job_title)
        X = self.vectorizer.transform([str(job_title)])
        return self.role_model.predict(X)[0]

def run_processing_pipeline():
    print("\n" + "=" * 80)
    print("Running Post-Processing and Data Enrichment Pipeline")
    print("=" * 80)

    # 1. Load Data
    try:
        df = pd.read_csv(INPUT_FILE)
        print(f"[1/4] Loaded {len(df)} ML-extracted job listings.")
    except FileNotFoundError:
        print(f"[ERROR] Input file not found: {INPUT_FILE}. Please run scraper.py first.")
        return

    # 2. Clean and Deduplicate
    df.drop_duplicates(subset=['Job Link'], keep='first', inplace=True)
    print(f"[2/4] Cleaned data, {len(df)} unique jobs remaining.")
    
    # 3. Apply ML Role Classification
    print("[3/4] Applying ML Role Classification...")
    role_classifier = MLRoleClassifier()
    role_classifier.train_or_load(df['Job Title'].astype(str))
    df['Job Role (ML)'] = df['Job Title'].astype(str).apply(role_classifier.predict)
    
    # 4. Standardize other fields
    if 'Posted Date' in df.columns:
        date_converter = DateConverter()
        df['Posted Date Cleaned'] = df['Posted Date'].apply(date_converter.convert_to_standard_date)

    # Reorder for clarity
    final_cols = ['Job Title', 'Job Role (ML)', 'Company', 'Location', 'Job Type', 'Experience', 'Posted Date Cleaned', 'Job Link']
    available_cols = [col for col in final_cols if col in df.columns]
    df_final = df[available_cols]

    df_final.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    
    print("\n" + "=" * 80)
    print("PROCESSING COMPLETE")
    print(f"  -> Final clean data saved to: {OUTPUT_FILE}")
    print("  -> Role Distribution:")
    print(df_final['Job Role (ML)'].value_counts().to_string())
    print("=" * 80)

if __name__ == "__main__":
    run_processing_pipeline()