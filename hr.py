import pandas as pd
import re
import os

# Function to normalize company names for matching
def normalize_company_name(name):
    if not isinstance(name, str):
        return ""
    name = name.lower().strip()
    name = re.sub(r'\b(inc\.?|incorporated|limited|ltd\.?|llc|corp\.?|corporation|private|pvt\.?)\b', '', name)
    return name.strip()

# File paths
job_file = r"C:\Sathvik-py\Talrn\job_scraper\Data\linkedin_jobs.csv"
hr_file = r"C:\Sathvik-py\Talrn\job_scraper\Data\Linked leads.csv"
output_file = r"C:\Sathvik-py\Talrn\job_scraper\Data\linked_merged.csv"

# Read input files
jobs_df = pd.read_csv(job_file)
hr_df = pd.read_csv(hr_file)

# Normalize company names
jobs_df['normalized_company'] = jobs_df['company_name'].apply(normalize_company_name)
hr_df['normalized_company'] = hr_df['company_name'].apply(normalize_company_name)

# Define target columns (from other half (1).csv)
target_columns = [
    'job title', 'company name', 'job link', 'experience', 'salary', 'date posted', 'company_norm',
    'First Name', 'Last Name', 'Title', 'Company Name', 'Company Name for Emails', 'Email', 'Email Status',
    'Primary Email Source', 'Primary Email Verification Source', 'Email Confidence', 'Primary Email Catch-all Status',
    'Primary Email Last Verified At', 'Seniority', 'Departments', 'Contact Owner', 'Work Direct Phone',
    'Home Phone', 'Mobile Phone', 'Corporate Phone', 'Other Phone', 'Stage', 'Lists', 'Last Contacted',
    'Account Owner', '# Employees', 'Industry', 'Keywords', 'Person Linkedin Url', 'Website',
    'Company Linkedin Url', 'Facebook Url', 'Twitter Url', 'City', 'State', 'Country', 'Company Address',
    'Company City', 'Company State', 'Company Country', 'Company Phone', 'Technologies', 'Annual Revenue',
    'Total Funding', 'Latest Funding', 'Latest Funding Amount', 'Last Raised At', 'Subsidiary of',
    'Email Sent', 'Email Open', 'Email Bounced', 'Replied', 'Demoed', 'Number of Retail Locations',
    'Apollo Contact Id', 'Apollo Account Id', 'Secondary Email', 'Secondary Email Source',
    'Secondary Email Status', 'Secondary Email Verification Source', 'Tertiary Email', 'Tertiary Email Source',
    'Tertiary Email Status', 'Tertiary Email Verification Source', 'Unnamed: 63', 'Unnamed: 64'
]

# Initialize output DataFrame
output_rows = []

# Iterate over job postings
for _, job_row in jobs_df.iterrows():
    job_company = job_row['company_name']
    normalized_job_company = job_row['normalized_company']
    
    # Find matching HR contacts
    matching_hr = hr_df[hr_df['normalized_company'] == normalized_job_company]
    
    if not matching_hr.empty:
        # For each HR contact, create a row for this job
        for _, hr_row in matching_hr.iterrows():
            row = {col: '' for col in target_columns}  # Initialize empty row
            # Map job fields
            row['job title'] = job_row['job_title']
            row['company name'] = job_row['company_name']
            row['job link'] = job_row['job_link']
            row['experience'] = job_row['experience']
            row['salary'] = job_row['salary']
            row['date posted'] = job_row['date_posted']
            row['company_norm'] = job_row['company_name']  # From second file
            row['Company Name'] = job_row['company_name']
            row['Company Name for Emails'] = job_row['company_name']
            # Map HR fields
            row['First Name'] = hr_row['first_name']
            row['Last Name'] = hr_row['last_name']
            row['Title'] = hr_row['job_title']
            row['Email'] = hr_row['email']
            row['Email Status'] = 'Verified' if pd.notna(hr_row['email']) and hr_row['email'] else ''
            row['Seniority'] = hr_row['seniority_level']
            row['# Employees'] = hr_row['company_size'] if pd.notna(hr_row['company_size']) else ''
            row['Industry'] = hr_row['industry']
            row['Keywords'] = hr_row['keywords']
            row['Website'] = hr_row['company_website']
            row['Company Linkedin Url'] = hr_row['company_linkedin']
            row['City'] = hr_row['city']
            row['State'] = hr_row['state']
            row['Country'] = hr_row['country']
            row['Company Address'] = hr_row['company_full_address']
            row['Company City'] = hr_row['company_city']
            row['Company State'] = hr_row['company_state']
            row['Company Country'] = hr_row['company_country']
            row['Company Phone'] = hr_row['company_phone']
            row['Mobile Phone'] = hr_row['company_phone']
            row['Technologies'] = hr_row['company_technologies']
            row['Annual Revenue'] = hr_row['company_annual_revenue'] if pd.notna(hr_row['company_annual_revenue']) else ''
            row['Total Funding'] = hr_row['company_total_funding'] if pd.notna(hr_row['company_total_funding']) else ''
            row['Secondary Email'] = hr_row['personal_email']
            row['Stage'] = 'Cold'  # Default value from sample
            output_rows.append(row)
    # Remove rows with no HR matches by skipping the else block

# Create output DataFrame
output_df = pd.DataFrame(output_rows, columns=target_columns)

# Save to CSV
os.makedirs('Data', exist_ok=True)
output_path = os.path.join('Data', output_file)
output_df.to_csv(output_path, index=False)

print(f"Output saved to {output_path}, {len(output_df)} rows")