import pandas as pd
import re

def validate_data():
    input_file = "Data/jobs_accurate_extracted.csv"
    
    try:
        df = pd.read_csv(input_file)
        print("🔍 Validating extracted data...")
        
        issues_found = 0
        
        for idx, row in df.iterrows():
            # Check for obvious errors
            title = str(row['Job Title'])
            company = str(row['Company'])
            location = str(row['Location'])
            
            # Job Title validation
            if any(bad in title.lower() for bad in ['sign', 'password', 'login', 'email']):
                print(f"❌ Row {idx}: Invalid Job Title - {title}")
                issues_found += 1
            
            # Company validation  
            if any(bad in company.lower() for bad in ['password', 'industries']):
                print(f"❌ Row {idx}: Invalid Company - {company}")
                issues_found += 1
            
            # Location validation
            if 'industries' in location.lower():
                print(f"❌ Row {idx}: Invalid Location - {location}")
                issues_found += 1
        
        if issues_found == 0:
            print("✅ All data looks good!")
        else:
            print(f"⚠️  Found {issues_found} issues that need manual review")
            
    except FileNotFoundError:
        print("❌ No data file found. Run scraper.py first.")

if __name__ == "__main__":
    validate_data()