import pandas as pd
import numpy as np
import os
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.cluster import DBSCAN
import joblib
from collections import Counter
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'Data')
MODELS_DIR = os.path.join(BASE_DIR, 'ML_Models')
INPUT_FILE = os.path.join(DATA_DIR, 'jobs_raw_data.csv')
OUTPUT_FILE = os.path.join(DATA_DIR, 'jobs_advanced.csv')

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(MODELS_DIR, exist_ok=True)

class DateConverter:
    """Convert various date formats to standardized format"""
    
    @staticmethod
    def convert_to_standard_date(date_str):
        """Convert date strings to 'X days ago' format or actual date to 'days ago'"""
        if not date_str or pd.isna(date_str) or date_str == 'Not specified':
            return 'Recent'
        
        date_str = str(date_str).strip()
        
        # If already in "X days ago" or "X hours ago" format, keep as is
        if any(keyword in date_str.lower() for keyword in ['ago', 'day', 'hour', 'week', 'month']):
            return DateConverter._normalize_relative_date(date_str)
        
        # If it's an actual date like "2025-10-04", convert to "days ago"
        elif re.match(r'\d{4}-\d{2}-\d{2}', date_str):
            return DateConverter._convert_absolute_to_relative(date_str)
        
        # For any other format, try to parse
        else:
            return DateConverter._try_parse_date(date_str)
    
    @staticmethod
    def _normalize_relative_date(date_str):
        """Normalize relative date strings to 'X days ago' format"""
        date_lower = date_str.lower()
        
        # Handle hours
        hour_match = re.search(r'(\d+)\s*hour', date_lower)
        if hour_match:
            hours = int(hour_match.group(1))
            if hours >= 24:
                days = hours // 24
                return f"{days} days ago"
            else:
                return "Today"
        
        # Handle days
        day_match = re.search(r'(\d+)\s*day', date_lower)
        if day_match:
            days = int(day_match.group(1))
            return f"{days} days ago"
        
        # Handle weeks
        week_match = re.search(r'(\d+)\s*week', date_lower)
        if week_match:
            weeks = int(week_match.group(1))
            days = weeks * 7
            return f"{days} days ago"
        
        # Handle months
        month_match = re.search(r'(\d+)\s*month', date_lower)
        if month_match:
            months = int(month_match.group(1))
            days = months * 30  # Approximate
            return f"{days} days ago"
        
        # Handle "just now", "recently", etc.
        if any(word in date_lower for word in ['just now', 'recently', 'today', 'new']):
            return "Today"
        
        return "Recent"
    
    @staticmethod
    def _convert_absolute_to_relative(date_str):
        """Convert absolute date (YYYY-MM-DD) to 'X days ago' format"""
        try:
            # Parse the date
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            today = datetime.now()
            
            # Calculate difference
            delta = today - date_obj
            days_ago = delta.days
            
            if days_ago == 0:
                return "Today"
            elif days_ago == 1:
                return "1 day ago"
            elif days_ago < 7:
                return f"{days_ago} days ago"
            elif days_ago < 30:
                weeks = days_ago // 7
                return f"{weeks} weeks ago"
            else:
                months = days_ago // 30
                return f"{months} months ago"
                
        except ValueError:
            return "Recent"
    
    @staticmethod
    def _try_parse_date(date_str):
        """Try to parse various date formats"""
        date_lower = date_str.lower()
        
        # Common job site date formats
        if 'today' in date_lower or 'just now' in date_lower:
            return "Today"
        elif 'yesterday' in date_lower:
            return "1 day ago"
        elif 'week' in date_lower:
            return "7 days ago"
        elif 'month' in date_lower:
            return "30 days ago"
        
        # Try to extract numbers with time units
        match = re.search(r'(\d+)\s*(\w+)', date_lower)
        if match:
            number = int(match.group(1))
            unit = match.group(2).lower()
            
            if 'hour' in unit:
                return "Today" if number < 24 else f"{number//24} days ago"
            elif 'day' in unit:
                return f"{number} days ago"
            elif 'week' in unit:
                return f"{number * 7} days ago"
            elif 'month' in unit:
                return f"{number * 30} days ago"
        
        return "Recent"

class MLRoleClassifier:
    """ML-powered role classification with dynamic keyword expansion"""
    
    def __init__(self):
        self.role_model = None
        self.vectorizer = TfidfVectorizer(max_features=1000, ngram_range=(1, 3))
        self.role_mappings = {
            'Backend Developer': ['backend', 'server', 'api', 'database', 'microservices'],
            'Frontend Developer': ['frontend', 'ui', 'ux', 'javascript', 'react'],
            'Full Stack Developer': ['full stack', 'fullstack', 'end-to-end'],
            'Data Scientist': ['data science', 'analytics', 'machine learning', 'statistics'],
            'ML/AI Engineer': ['machine learning', 'ai', 'deep learning', 'neural networks'],
            'DevOps Engineer': ['devops', 'cloud', 'infrastructure', 'cicd'],
            'Mobile Developer': ['mobile', 'android', 'ios', 'react native'],
            'Data Engineer': ['data engineer', 'etl', 'data pipeline', 'big data'],
            'QA Engineer': ['qa', 'testing', 'quality assurance', 'automation'],
            'Software Engineer': ['software engineer', 'developer', 'programmer']
        }
        
    def train_role_classifier(self, job_titles, job_descriptions=None):
        """Train ML model for role classification"""
        print("Training ML role classification model...")
        
        # Combine titles and descriptions for better context
        if job_descriptions is not None:
            training_data = [f"{title} {desc}" for title, desc in zip(job_titles, job_descriptions)]
        else:
            training_data = job_titles
        
        # Generate training labels using rule-based approach first
        training_labels = [self._rule_based_classify(title) for title in job_titles]
        
        # Vectorize text data
        X = self.vectorizer.fit_transform(training_data)
        
        # Train Random Forest classifier
        self.role_model = RandomForestClassifier(
            n_estimators=100, 
            random_state=42,
            max_depth=20
        )
        self.role_model.fit(X, training_labels)
        
        # Save model
        joblib.dump(self.role_model, os.path.join(MODELS_DIR, 'role_classifier.pkl'))
        joblib.dump(self.vectorizer, os.path.join(MODELS_DIR, 'role_vectorizer.pkl'))
        
        print("ML role classifier trained and saved")
        
    def predict_role(self, job_title, job_description=""):
        """Predict role using ML model"""
        if self.role_model is None:
            return self._rule_based_classify(job_title)
        
        text_data = f"{job_title} {job_description}"
        X = self.vectorizer.transform([text_data])
        prediction = self.role_model.predict(X)[0]
        return prediction
    
    def _rule_based_classify(self, title):
        """Fallback rule-based classification"""
        if not title or pd.isna(title):
            return 'Other'
            
        title_lower = str(title).lower()
        for role, keywords in self.role_mappings.items():
            if any(keyword in title_lower for keyword in keywords):
                return role
        return 'Other'
    
    def expand_keywords_ml(self, new_job_titles):
        """Dynamically expand role keywords using ML clustering"""
        print("Expanding role keywords using ML clustering...")
        
        if len(new_job_titles) < 10:
            return self.role_mappings
        
        # Vectorize new job titles
        title_vectorizer = TfidfVectorizer(max_features=500, stop_words='english')
        title_vectors = title_vectorizer.fit_transform(new_job_titles)
        
        # Apply clustering to discover new patterns
        clustering = DBSCAN(eps=0.5, min_samples=2)
        clusters = clustering.fit_predict(title_vectors.toarray())
        
        # Analyze each cluster for new keywords
        for cluster_id in set(clusters):
            if cluster_id != -1:  # Ignore noise
                cluster_titles = [new_job_titles[i] for i in range(len(new_job_titles)) 
                                if clusters[i] == cluster_id]
                
                if len(cluster_titles) > 3:  # Significant cluster
                    new_keywords = self._extract_cluster_keywords(cluster_titles)
                    closest_role = self._find_closest_role(new_keywords)
                    
                    if closest_role:
                        # Expand existing role with new keywords
                        self.role_mappings[closest_role].extend(new_keywords)
                        self.role_mappings[closest_role] = list(set(self.role_mappings[closest_role]))
                        print(f"Expanded {closest_role} with: {new_keywords}")
        
        return self.role_mappings
    
    def _extract_cluster_keywords(self, titles):
        """Extract significant keywords from title cluster"""
        all_text = ' '.join(titles).lower()
        words = re.findall(r'\b[a-z]{4,15}\b', all_text)
        
        # Remove common stop words
        stop_words = {'software', 'engineer', 'developer', 'senior', 'junior', 'lead'}
        filtered_words = [word for word in words if word not in stop_words]
        
        word_freq = Counter(filtered_words)
        return [word for word, count in word_freq.most_common(5) if count > 1]
    
    def _find_closest_role(self, keywords):
        """Find closest existing role for new keywords"""
        best_role = None
        best_score = 0
        
        for role, role_keywords in self.role_mappings.items():
            score = sum(1 for keyword in keywords if any(role_kw in keyword for role_kw in role_keywords))
            if score > best_score:
                best_score = score
                best_role = role
        
        return best_role if best_score > 0 else None

def load_or_train_models(df):
    """Load existing models or train new ones"""
    role_classifier = MLRoleClassifier()
    
    # Try to load existing models
    try:
        role_classifier.role_model = joblib.load(os.path.join(MODELS_DIR, 'role_classifier.pkl'))
        role_classifier.vectorizer = joblib.load(os.path.join(MODELS_DIR, 'role_vectorizer.pkl'))
        print("Loaded pre-trained role classification model")
    except:
        print("Training new role classification model...")
        job_titles = df['Job Title'].fillna('').astype(str).tolist()
        job_descriptions = df.get('Job Description', pd.Series([''] * len(df))).fillna('').astype(str).tolist()
        role_classifier.train_role_classifier(job_titles, job_descriptions)
    
    return role_classifier

def run_ml_enhanced_pipeline():
    """ML-enhanced data processing pipeline"""
    print("=" * 80)
    print("ML ENHANCED JOB DATA PROCESSING PIPELINE")
    print("=" * 80)
    
    # Load data
    print("\n[1/4] Loading job data...")
    try:
        df = pd.read_csv(INPUT_FILE)
        print(f"Loaded {len(df)} job listings")
    except Exception as e:
        print(f"Error loading data: {e}")
        return
    
    # Remove duplicates
    print("\n[2/4] Data cleaning and deduplication...")
    initial_count = len(df)
    df = df.drop_duplicates(subset=['Job Link'], keep='first')
    print(f"Removed {initial_count - len(df)} duplicates")
    
    # Convert dates to standardized format
    print("\n[2.5/4] Converting dates to standardized format...")
    if 'Posted Date' in df.columns:
        date_converter = DateConverter()
        df['Posted Date'] = df['Posted Date'].apply(date_converter.convert_to_standard_date)
        print("Dates converted to standardized format")
    
    # Load or train ML models
    print("\n[3/4] ML Model Processing...")
    role_classifier = load_or_train_models(df)
    
    # Apply ML classifications
    print("Applying ML classifications...")
    
    # Role classification with dynamic keyword expansion
    df['Role_ML'] = df.apply(
        lambda row: role_classifier.predict_role(
            row.get('Job Title', ''),
            row.get('Job Description', '')
        ), axis=1
    )
    
    # Expand keywords with new data
    new_titles = df['Job Title'].fillna('').astype(str).tolist()
    role_classifier.expand_keywords_ml(new_titles)
    
    # Final processing
    print("\n[4/4] Final data preparation...")
    
    # Reorder columns
    preferred_order = [
        'Job Title', 'Company', 'Location', 'Job Type', 'Role_ML',
        'Posted Date', 'Job Link'
    ]
    
    final_columns = [col for col in preferred_order if col in df.columns]
    remaining_cols = [col for col in df.columns if col not in final_columns]
    final_columns.extend(remaining_cols)
    
    df_final = df[final_columns]
    
    # Save results
    df_final.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    
    # Summary
    print("\n" + "=" * 80)
    print("ML PROCESSING COMPLETE")
    print("=" * 80)
    print(f"Processed jobs: {len(df_final)}")
    print(f"ML Classifications applied:")
    print(f"  Roles: {df_final['Role_ML'].nunique()} categories")
    
    # Show date distribution
    if 'Posted Date' in df_final.columns:
        print(f"Date distribution:")
        date_counts = df_final['Posted Date'].value_counts().head(5)
        for date, count in date_counts.items():
            print(f"  {date}: {count} jobs")
    
    print(f"Output: {OUTPUT_FILE}")
    print("=" * 80)

if __name__ == "__main__":
    run_ml_enhanced_pipeline()