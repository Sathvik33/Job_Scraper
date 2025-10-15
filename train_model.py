import os
import pandas as pd
import numpy as np
import joblib
import re
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import accuracy_score, classification_report

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'Data')
MODELS_DIR = os.path.join(BASE_DIR, 'ML_Models')
RAW_DATA_FILE = os.path.join(DATA_DIR, 'jobs_raw_data.csv')

os.makedirs(MODELS_DIR, exist_ok=True)

def clean_text_for_training(text):
    if not isinstance(text, str):
        return ""
    text = str(text).strip()
    patterns_to_remove = [r'http\S+', r'@\w+', r'#\w+', r'\b\d{10,}\b']
    for pattern in patterns_to_remove:
        text = re.sub(pattern, '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def create_field_specific_features(text, field):
    """Create field-specific features for better accuracy"""
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
    
    text = clean_text_for_training(text)
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

def transform_raw_to_labeled(df):
    labeled_rows = []
    required_fields = ['Job Title', 'Company', 'Location', 'Job Type', 'Experience', 'Posted Date']
    
    for _, row in df.iterrows():
        for field in required_fields:
            if field in row and pd.notna(row[field]) and str(row[field]).strip():
                clean_text = clean_text_for_training(str(row[field]))
                if is_valid_field_text(field, clean_text):
                    labeled_rows.append({'text': clean_text, 'tag': 'span', 'label': field})
    
    return pd.DataFrame(labeled_rows)

def is_valid_field_text(field, text):
    """Enhanced validation for training data"""
    if not text or len(text) < 2: 
        return False
        
    text_lower = text.lower()
    
    # Common junk terms to exclude from ALL fields
    junk_terms = ['similar search', 'industries', 'show more', 'see all', 'open jobs']
    if any(junk in text_lower for junk in junk_terms):
        return False
    
    if field == 'Job Title': 
        return len(text) < 100 and any(c.isalpha() for c in text)
        
    elif field == 'Company': 
        # Stricter validation for companies
        job_title_words = ['engineer', 'developer', 'manager', 'analyst', 'specialist', 'intern', 'architect']
        
        # Reject if it contains too many job title indicators
        if sum(1 for word in job_title_words if word in text_lower) >= 2:
            return False
        
        # Must not be a common UI element
        if any(term in text_lower for term in ['similar search', 'industries', 'companies', 'linkedin', 'indeed', 'naukri']):
            return False
            
        return len(text) < 50 and any(c.isalpha() for c in text)
        
    elif field == 'Location': 
        return any(indicator in text_lower for indicator in [',', 'remote', 'hybrid', 'india', 'bangalore', 'delhi', 'mumbai', 'pune', 'hyderabad'])
        
    elif field == 'Experience': 
        exp_indicators = ['years', 'yrs', 'year', 'experience', 'exp', 'fresher', 'entry', 'senior']
        return any(indicator in text_lower for indicator in exp_indicators) or any(char.isdigit() for char in text)
    
    elif field == 'Job Type':
        # Valid job types
        valid_types = ['full-time', 'full time', 'part-time', 'part time', 'internship', 'intern', 'contract', 'temporary', 'freelance', 'remote', 'hybrid', 'onsite']
        
        # Must contain at least one valid job type indicator
        if not any(job_type in text_lower for job_type in valid_types):
            return False
        
        # Must not be a UI element
        if any(term in text_lower for term in ['industries', 'similar search', 'companies']):
            return False
            
        return True
        
    elif field == 'Posted Date': 
        return any(indicator in text_lower for indicator in ['ago', 'day', 'week', 'month', 'year', 'today', 'yesterday'])
        
    return True

def train_individual_model(field, df, features_df):
    """Train a model for a specific field with enhanced features"""
    print(f"    -> Training enhanced model for: '{field}'...")
    
    # Create field-specific features
    features_list = [create_field_specific_features(text, field) for text in df['text']]
    field_features_df = pd.DataFrame(features_list)
    field_features_df['tag'] = df['tag']
    
    # Positive examples (this field)
    positive_mask = df['label'] == field
    X_positive = field_features_df[positive_mask]
    y_positive = np.ones(len(X_positive))
    
    if len(X_positive) < 15:
        print(f"        Skipping '{field}' - insufficient positive examples ({len(X_positive)})")
        return None, 0
    
    # Negative examples (other fields) - sample same amount
    negative_mask = ~positive_mask
    X_negative = field_features_df[negative_mask].sample(
        n=min(len(X_positive), len(field_features_df[negative_mask])), 
        random_state=42
    )
    y_negative = np.zeros(len(X_negative))
    
    # Combine datasets
    X_combined = pd.concat([X_positive, X_negative])
    y_combined = np.concatenate([y_positive, y_negative])
    
    # Train-test split
    X_train, X_test, y_train, y_test = train_test_split(
        X_combined, y_combined, test_size=0.25, random_state=42, stratify=y_combined
    )
    
    # Create pipeline
    numeric_features = [col for col in field_features_df.columns if col != 'tag']
    categorical_features = ['tag']
    
    preprocessor = ColumnTransformer(transformers=[
        ('num', StandardScaler(), numeric_features),
        ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_features)
    ])
    
    # Field-specific classifier parameters
    if field == 'Company':
        classifier_params = {
            'n_estimators': 350,
            'max_depth': 35,
            'min_samples_split': 2,
            'min_samples_leaf': 1,
            'class_weight': {0: 1, 1: 2},  # Emphasize company detection
            'max_features': 'sqrt'
        }
    elif field == 'Job Type':
        classifier_params = {
            'n_estimators': 300,
            'max_depth': 30,
            'min_samples_split': 2,
            'min_samples_leaf': 1,
            'class_weight': {0: 1, 1: 2},  # Emphasize job type detection
            'max_features': 'sqrt'
        }
    elif field == 'Experience':
        classifier_params = {
            'n_estimators': 250,
            'max_depth': 25,
            'min_samples_split': 3,
            'min_samples_leaf': 2,
            'class_weight': 'balanced'
        }
    else:
        classifier_params = {
            'n_estimators': 200,
            'max_depth': 25,
            'min_samples_split': 5,
            'min_samples_leaf': 2,
            'class_weight': 'balanced'
        }
    
    pipeline = Pipeline(steps=[
        ('preprocessor', preprocessor),
        ('classifier', RandomForestClassifier(**classifier_params, random_state=42))
    ])
    
    # Train model
    pipeline.fit(X_train, y_train)
    
    # Evaluate
    predictions = pipeline.predict(X_test)
    accuracy = accuracy_score(y_test, predictions)
    
    print(f"        Accuracy: {accuracy:.3f} | Examples: {len(X_positive)}+{len(X_negative)}")
    
    # Show classification report for important fields
    if field in ['Company', 'Job Type']:
        print(f"\n        Classification Report for {field}:")
        print(classification_report(y_test, predictions, target_names=['Not ' + field, field], zero_division=0))
    
    # Save model
    model_filename = os.path.join(MODELS_DIR, f"{field.replace(' ', '_').lower()}_pipeline.pkl")
    joblib.dump(pipeline, model_filename)
    print(f"        ✓ Enhanced model saved to {model_filename}\n")
    
    return pipeline, accuracy

def train():
    print("="*80)
    print(" ENHANCED FIELD-SPECIFIC ML MODEL TRAINING v2.0")
    print("="*80)
    
    try:
        raw_df = pd.read_csv(RAW_DATA_FILE)
        print(f"Loaded raw data with {len(raw_df)} rows")
        print(f"Columns: {list(raw_df.columns)}")
    except FileNotFoundError:
        print(f"[ERROR] Training data not found at {RAW_DATA_FILE}")
        return
    
    # Transform to labeled data
    df = transform_raw_to_labeled(raw_df)
    print(f"Created {len(df)} training examples")
    print("\nLabel distribution:")
    print(df['label'].value_counts())
    print()
    
    models_performance = {}
    
    # Train models for each field with enhanced features
    for field in ['Company', 'Job Type', 'Experience', 'Job Title', 'Location', 'Posted Date']:
        pipeline, accuracy = train_individual_model(field, df, None)
        if pipeline:
            models_performance[field] = accuracy
    
    print("\n" + "="*80)
    print(" TRAINING SUMMARY")
    print("="*80)
    for field, accuracy in sorted(models_performance.items(), key=lambda x: x[1], reverse=True):
        status = "✅" if accuracy >= 0.85 else "⚠️ " if accuracy >= 0.75 else "❌"
        print(f"    {status} {field:20s}: {accuracy:.3f} accuracy")
    
    # Special focus on problematic fields
    print("\n" + "="*80)
    print(" KEY MODEL PERFORMANCE")
    print("="*80)
    
    company_acc = models_performance.get('Company', 0)
    job_type_acc = models_performance.get('Job Type', 0)
    experience_acc = models_performance.get('Experience', 0)
    
    print(f"    Company Model:    {company_acc:.3f} (Target: >0.85) {'✅' if company_acc >= 0.85 else '⚠️'}")
    print(f"    Job Type Model:   {job_type_acc:.3f} (Target: >0.85) {'✅' if job_type_acc >= 0.85 else '⚠️'}")
    print(f"    Experience Model: {experience_acc:.3f} (Target: >0.80) {'✅' if experience_acc >= 0.80 else '⚠️'}")
    
    if company_acc < 0.85:
        print(f"\n⚠️  Company model needs improvement:")
        print(f"    - Add more diverse company names to training data")
        print(f"    - Ensure training data excludes 'Similar Searches' and UI elements")
    
    if job_type_acc < 0.85:
        print(f"\n⚠️  Job Type model needs improvement:")
        print(f"    - Add more examples of 'Full-time', 'Part-time', 'Internship', etc.")
        print(f"    - Ensure training data excludes 'Industries' and UI elements")
    
    if experience_acc < 0.80:
        print(f"\n⚠️  Experience model needs improvement:")
        print(f"    - Check training data quality for experience field")
        print(f"    - Ensure proper format like '2-4 years', '0 years', etc.")
    
    print("\n" + "="*80)
    print(" ✅ ENHANCED TRAINING COMPLETE!")
    print("="*80)

if __name__ == "__main__":
    train()