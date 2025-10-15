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
from sklearn.metrics import accuracy_score

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

def create_enhanced_features(text):
    if not isinstance(text, str) or not text.strip():
        return {
            'text_length': 0, 'word_count': 0, 'has_digits': 0, 'digit_count': 0,
            'has_special_chars': 0, 'special_char_count': 0, 'is_uppercase_ratio': 0,
            'has_common_separators': 0, 'date_like_pattern': 0, 'experience_like_pattern': 0,
            'url_like_pattern': 0, 'has_comma': 0, 'has_parentheses': 0,
            'company_like_pattern': 0, 'location_like_pattern': 0
        }
    
    text = clean_text_for_training(text)
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
    
    date_patterns = [
        r'\d{1,2}[-/]\d{1,2}[-/]\d{2,4}', r'\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{4}',
        r'(?:today|yesterday|\d+\s+(?:days?|hours?|months?)\s+ago)', r'\d+\s*(?:day|week|month|year)s?\s+ago'
    ]
    date_like_pattern = 1 if any(re.search(pattern, text.lower()) for pattern in date_patterns) else 0
    
    experience_patterns = [
        r'\d+\+?\s*(?:years?|yrs?)', r'\d+\s*-\s*\d+\s*(?:years?|yrs?)',
        r'\d+\s*to\s*\d+\s*(?:years?|yrs?)', r'fresher|entry level|experienced|mid.level|senior'
    ]
    experience_like_pattern = 1 if any(re.search(pattern, text.lower()) for pattern in experience_patterns) else 0
    
    url_like_pattern = 1 if re.search(r'https?://|www\.|\.com|\.in|\.org', text.lower()) else 0
    
    company_patterns = [r'\b(?:inc|llc|ltd|corp|corporation|company|co\.)\b']
    company_like_pattern = 1 if any(re.search(pattern, text) for pattern in company_patterns) else 0
    
    location_patterns = [r'[A-Z][a-z]+,\s*[A-Z][a-z]+,\s*[A-Z][a-z]+', r'[A-Z][a-z]+,\s*[A-Z][a-z]+', r'\b(?:remote|hybrid|onsite|office)\b']
    location_like_pattern = 1 if any(re.search(pattern, text) for pattern in location_patterns) else 0
    
    return {
        'text_length': text_length, 'word_count': word_count, 'has_digits': has_digits, 'digit_count': digit_count,
        'has_special_chars': has_special_chars, 'special_char_count': special_char_count, 'is_uppercase_ratio': is_uppercase_ratio,
        'has_common_separators': has_common_separators, 'date_like_pattern': date_like_pattern, 'experience_like_pattern': experience_like_pattern,
        'url_like_pattern': url_like_pattern, 'has_comma': has_comma, 'has_parentheses': has_parentheses,
        'company_like_pattern': company_like_pattern, 'location_like_pattern': location_like_pattern
    }

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
    if not text or len(text) < 2: return False
    text_lower = text.lower()
    
    if field == 'Job Title': return len(text) < 100 and any(c.isalpha() for c in text)
    elif field == 'Company': return not any(word in text_lower for word in ['engineer', 'developer', 'manager', 'analyst', 'specialist'])
    elif field == 'Location': return any(indicator in text_lower for indicator in [',', 'remote', 'hybrid', 'india', 'pune', 'bangalore', 'delhi'])
    elif field == 'Experience': return any(indicator in text_lower for indicator in ['years', 'yrs', 'fresher', 'experience', '0', '1', '2', '3', '4', '5'])
    elif field == 'Posted Date': return any(indicator in text_lower for indicator in ['ago', 'day', 'week', 'month', 'year', 'today', 'yesterday'])
    return True

def train():
    print("="*80)
    print(" ENHANCED ML MODEL TRAINING")
    print("="*80)
    
    try:
        raw_df = pd.read_csv(RAW_DATA_FILE)
        print(f"Loaded raw data with {len(raw_df)} rows")
    except FileNotFoundError:
        print(f"[ERROR] Training data not found at {RAW_DATA_FILE}")
        return
    
    df = transform_raw_to_labeled(raw_df)
    print(f"Created {len(df)} training examples")
    print("Label distribution:\n", df['label'].value_counts())
    
    features_list = [create_enhanced_features(text) for text in df['text']]
    features_df = pd.DataFrame(features_list)
    features_df['tag'] = df['tag']
    
    X = features_df
    unique_labels = df['label'].unique()
    models_performance = {}
    
    for label in unique_labels:
        if pd.isna(label): continue
        print(f"    -> Training model for: '{label}'...")
        
        y = np.where(df['label'] == label, 1, 0)
        positive_count = sum(y)
        
        if positive_count < 20:
            print(f"        Skipping '{label}' - insufficient examples ({positive_count})")
            continue
        
        numeric_features = [col for col in features_df.columns if col != 'tag']
        categorical_features = ['tag']
        
        preprocessor = ColumnTransformer(transformers=[
            ('num', StandardScaler(), numeric_features),
            ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_features)
        ])
        
        pipeline = Pipeline(steps=[
            ('preprocessor', preprocessor),
            ('classifier', RandomForestClassifier(n_estimators=200, random_state=42, class_weight='balanced', max_depth=25))
        ])
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42, stratify=y)
        pipeline.fit(X_train, y_train)
        
        predictions = pipeline.predict(X_test)
        accuracy = accuracy_score(y_test, predictions)
        
        print(f"        Accuracy: {accuracy:.3f} | Examples: {positive_count}")
        
        model_filename = os.path.join(MODELS_DIR, f"{label.replace(' ', '_').lower()}_pipeline.pkl")
        joblib.dump(pipeline, model_filename)
        print(f"        ✓ Model saved to {model_filename}")
        
        models_performance[label] = accuracy
    
    print("\nTraining Summary:")
    for label, accuracy in models_performance.items():
        print(f"    {label}: {accuracy:.3f} accuracy")
    
    print("\n" + "="*80)
    print(" TRAINING COMPLETE!")
    print("="*80)

if __name__ == "__main__":
    train()