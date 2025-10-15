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
from sklearn.metrics import classification_report, accuracy_score
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.impute import SimpleImputer

# --- Configuration ---
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'Data')
MODELS_DIR = os.path.join(BASE_DIR, 'ML_Models')
RAW_DATA_FILE = os.path.join(DATA_DIR, 'jobs_raw_data.csv')

os.makedirs(MODELS_DIR, exist_ok=True)

def create_advanced_features(text):
    """Create dynamic features that learn patterns from data"""
    if not isinstance(text, str) or not text.strip():
        return {
            'text_length': 0,
            'word_count': 0,
            'has_digits': 0,
            'digit_count': 0,
            'has_special_chars': 0,
            'special_char_count': 0,
            'is_uppercase_ratio': 0,
            'has_common_separators': 0,
            'date_like_pattern': 0,
            'experience_like_pattern': 0,
            'url_like_pattern': 0
        }
    
    text = str(text).strip()
    text_length = len(text)
    word_count = len(text.split())
    
    # Character type features
    has_digits = 1 if any(char.isdigit() for char in text) else 0
    digit_count = sum(char.isdigit() for char in text)
    
    # Special characters
    special_chars = r'[!@#$%^&*()_+\-=\[\]{};\'":|,.<>?/~]'
    has_special_chars = 1 if re.search(special_chars, text) else 0
    special_char_count = len(re.findall(special_chars, text))
    
    # Case analysis
    uppercase_count = sum(1 for char in text if char.isupper())
    is_uppercase_ratio = uppercase_count / len(text) if text_length > 0 else 0
    
    # Structural patterns
    has_common_separators = 1 if any(sep in text for sep in ['•', '|', '-', '·', '–', '—']) else 0
    
    # Domain-specific patterns
    date_patterns = [
        r'\d{1,2}[-/]\d{1,2}[-/]\d{2,4}',
        r'\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{4}',
        r'(?:today|yesterday|\d+\s+(?:days?|hours?)\s+ago)'
    ]
    date_like_pattern = 1 if any(re.search(pattern, text.lower()) for pattern in date_patterns) else 0
    
    experience_patterns = [
        r'\d+\+?\s*(?:years?|yrs?)',
        r'\d+\s*-\s*\d+\s*(?:years?|yrs?)',
        r'fresher|entry level|experienced'
    ]
    experience_like_pattern = 1 if any(re.search(pattern, text.lower()) for pattern in experience_patterns) else 0
    
    url_like_pattern = 1 if re.search(r'https?://|www\.|\.com|\.in', text.lower()) else 0
    
    return {
        'text_length': text_length,
        'word_count': word_count,
        'has_digits': has_digits,
        'digit_count': digit_count,
        'has_special_chars': has_special_chars,
        'special_char_count': special_char_count,
        'is_uppercase_ratio': is_uppercase_ratio,
        'has_common_separators': has_common_separators,
        'date_like_pattern': date_like_pattern,
        'experience_like_pattern': experience_like_pattern,
        'url_like_pattern': url_like_pattern
    }

def extract_text_features_from_data(df):
    """Analyze the training data to learn patterns for each field"""
    field_patterns = {}
    
    for field in ['Job Title', 'Company', 'Location', 'Job Type', 'Experience', 'Posted Date']:
        if field in df.columns:
            field_data = df[field].dropna().astype(str)
            if len(field_data) > 0:
                avg_length = field_data.str.len().mean()
                digit_ratio = field_data.apply(lambda x: sum(c.isdigit() for c in x) / len(x) if len(x) > 0 else 0).mean()
                upper_ratio = field_data.apply(lambda x: sum(c.isupper() for c in x) / len(x) if len(x) > 0 else 0).mean()
                
                field_patterns[field] = {
                    'avg_length': avg_length,
                    'digit_ratio': digit_ratio,
                    'upper_ratio': upper_ratio
                }
    
    return field_patterns

def transform_raw_to_labeled(df):
    """Convert raw data format to labeled training examples"""
    labeled_rows = []
    
    for _, row in df.iterrows():
        for field in ['Job Title', 'Company', 'Location', 'Job Type', 'Experience', 'Posted Date']:
            if field in row and pd.notna(row[field]) and str(row[field]).strip():
                labeled_rows.append({
                    'text': str(row[field]),
                    'tag': 'span',
                    'label': field
                })
    
    return pd.DataFrame(labeled_rows)

def train():
    print("="*80)
    print(" ADVANCED ML MODEL TRAINING (Learning from raw_data.csv)")
    print("="*80)
    
    # [1/4] Load and prepare training data
    print("[1/4] Loading and preparing training data...")
    try:
        raw_df = pd.read_csv(RAW_DATA_FILE)
        print(f"Loaded raw data with {len(raw_df)} rows and columns: {list(raw_df.columns)}")
    except FileNotFoundError:
        print(f"[ERROR] Training data not found at {RAW_DATA_FILE}")
        return
    
    # Analyze data patterns
    field_patterns = extract_text_features_from_data(raw_df)
    print("Learned field patterns:", field_patterns)
    
    # Transform to labeled data
    df = transform_raw_to_labeled(raw_df)
    df.dropna(subset=['text'], inplace=True)
    df['text'] = df['text'].astype(str)
    
    print(f"Created {len(df)} training examples")
    print("Label distribution:\n", df['label'].value_counts())
    
    # [2/4] Engineer advanced features
    print("\n[2/4] Engineering advanced features...")
    features_list = []
    for text in df['text']:
        features_list.append(create_advanced_features(text))
    
    features_df = pd.DataFrame(features_list)
    features_df['tag'] = df['tag']
    
    print(f"Created {len(features_df.columns)} features")
    print("Feature columns:", list(features_df.columns))
    
    # [3/4] Train one classifier per label
    print("\n[3/4] Training specialized classifiers...")
    X = features_df
    unique_labels = df['label'].unique()
    
    models_performance = {}
    
    for label in unique_labels:
        if pd.isna(label): 
            continue
            
        print(f"    -> Training model for: '{label}'...")
        
        y = np.where(df['label'] == label, 1, 0)
        positive_count = sum(y)
        negative_count = len(y) - positive_count
        
        if positive_count < 10:
            print(f"        Skipping '{label}' - insufficient positive examples ({positive_count})")
            continue
        
        # Create preprocessing pipeline
        numeric_features = [col for col in features_df.columns if col != 'tag']
        categorical_features = ['tag']
        
        preprocessor = ColumnTransformer(
            transformers=[
                ('num', StandardScaler(), numeric_features),
                ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_features)
            ]
        )
        
        pipeline = Pipeline(steps=[
            ('preprocessor', preprocessor),
            ('classifier', RandomForestClassifier(
                n_estimators=150, 
                random_state=42, 
                class_weight='balanced',
                max_depth=20,
                min_samples_split=5,
                min_samples_leaf=2
            ))
        ])
        
        # Train-test split with stratification
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.25, random_state=42, stratify=y
        )
        
        pipeline.fit(X_train, y_train)
        
        # Evaluate
        predictions = pipeline.predict(X_test)
        accuracy = accuracy_score(y_test, predictions)
        
        print(f"        Accuracy for '{label}': {accuracy:.3f}")
        print(f"        Positive examples: {positive_count}/{len(y)} ({positive_count/len(y)*100:.1f}%)")
        
        # Save model
        model_filename = os.path.join(MODELS_DIR, f"{label.replace(' ', '_').lower()}_pipeline.pkl")
        joblib.dump(pipeline, model_filename)
        print(f"        ✓ Model saved to {model_filename}")
        
        models_performance[label] = accuracy
    
    # [4/4] Summary
    print("\n[4/4] Training Summary:")
    for label, accuracy in models_performance.items():
        print(f"    {label}: {accuracy:.3f} accuracy")
    
    print("\n" + "="*80)
    print(" ADVANCED ML Training Complete!")
    print("="*80)

if __name__ == "__main__":
    train()