import pandas as pd
import re

def clean_extracted_data():
    input_file = "Data/jobs_raw_ml_extracted.csv"
    output_file = "Data/jobs_cleaned.csv"
    
    try:
        df = pd.read_csv(input_file)
        print(f"Cleaning {len(df)} records...")
    except FileNotFoundError:
        print(f"Input file {input_file} not found. Run scraper.py first.")
        return
    
    df['Job Title'] = df['Job Title'].apply(clean_job_title)
    df['Company'] = df['Company'].apply(clean_company)
    df['Location'] = df['Location'].apply(clean_location)
    df['Experience'] = df['Experience'].apply(clean_experience)
    
    df.to_csv(output_file, index=False)
    print(f"✅ Cleaned data saved to {output_file}")
    
    print("\n📊 Final Data Quality:")
    for field in ['Job Title', 'Company', 'Location', 'Job Type', 'Experience', 'Posted Date']:
        found_count = df[df[field] != "Not Specified"][field].count()
        print(f"    {field}: {found_count}/{len(df)} ({found_count/len(df)*100:.1f}%)")

def clean_job_title(title):
    if pd.isna(title) or title == "Not Specified": return title
    title = str(title)
    patterns = [
        r'\s*at\s+[A-Z][a-zA-Z\s]+$', r'\s*-\s*[A-Z][a-zA-Z\s]+$', 
        r',\s*[A-Z][a-zA-Z,\s]+$', r'\s*\([^)]*\)$',
        r'[A-Z][a-z]+\d+[A-Z][a-z]+.*$'
    ]
    for pattern in patterns: title = re.sub(pattern, '', title)
    return title.strip()

def clean_company(company):
    if pd.isna(company) or company == "Not Specified": return company
    company = str(company)
    if company.lower() in ['linkedin', 'indeed', 'naukri']: return "Not Specified"
    return company.strip()

def clean_location(location):
    if pd.isna(location) or location == "Not Specified": return location
    location = str(location)
    if any(word in location.lower() for word in ['bahasa', 'malaysia', 'industries']): return "Not Specified"
    return location.strip()

def clean_experience(exp):
    if pd.isna(exp) or exp == "Not Specified": return exp
    exp = str(exp)
    if not any(word in exp.lower() for word in ['year', 'yr', 'exp', 'fresher']): return "Not Specified"
    return exp.strip()

if __name__ == "__main__":
    clean_extracted_data()