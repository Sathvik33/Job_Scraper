import pandas as pd
import re
from pathlib import Path

# ----------------------------------------------------------------------
# 1. FILE PATHS (YOUR PATHS)
# ----------------------------------------------------------------------
leads_path  = r"C:\Sathvik-py\Talrn\job_scraper\Data\dataset_leads-finder_2025-10-27_17-13-40-747 (1).csv"      # HR contacts
jobs_path   = r"C:\Sathvik-py\Talrn\job_scraper\Data\linkedin_jobs_old.csv"    # Job postings
target_path = r"C:\Sathvik-py\Talrn\job_scraper\Data\other half.csv"       # For column order
out_dir     = r"C:\Sathvik-py\Talrn\job_scraper\Data"                       # Output folder
output_file = "l_hr.csv"                                                   # Output filename


def normalise_company(name: str) -> str:
    if not isinstance(name, str):
        return ""
    name = name.lower().strip()
    # Remove legal suffixes
    name = re.sub(r'\b(inc\.?|incorporated|ltd\.?|llc|corp\.?|corporation|private|pvt\.?|llp|group|services|technologies|consulting|consultancy|solutions|labs|systems)\b', '', name)
    # Remove punctuation & extra spaces
    name = re.sub(r'[^a-z0-9\s]', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

# ----------------------------------------------------------------------
# 3. Load CSVs
# ----------------------------------------------------------------------
leads_df = pd.read_csv(leads_path)
jobs_df  = pd.read_csv(jobs_path)
target_df = pd.read_csv(target_path)

# ----------------------------------------------------------------------
# 4. Normalise company names
# ----------------------------------------------------------------------
leads_df['norm_company'] = leads_df['company_name'].apply(normalise_company)
jobs_df['norm_company']  = jobs_df['company_name'].apply(normalise_company)

jobs_df = jobs_df[jobs_df['norm_company'] != 'turing']

# Build lookup: norm_company → list of HR rows
hr_lookup = {}
for _, row in leads_df.iterrows():
    key = row['norm_company']
    if key not in hr_lookup:
        hr_lookup[key] = []
    hr_lookup[key].append(row.to_dict())

# ----------------------------------------------------------------------
# 5. Target columns (65 in exact order)
# ----------------------------------------------------------------------
target_columns = target_df.columns.tolist()

# ----------------------------------------------------------------------
# 6. Merge: Job + HR (only if name matches), use companyUrl for LinkedIn
# ----------------------------------------------------------------------
output_rows = []

for _, job in jobs_df.iterrows():
    job_norm = job['norm_company']
    company_linkedin = job.get('companyUrl', '')  # From jobs file

    if job_norm not in hr_lookup:
        continue  # No HR → skip job

    for hr in hr_lookup[job_norm]:
        row = {col: '' for col in target_columns}

        # ----- Job fields -------------------------------------------------
        row['job title']       = job.get('job_title', '')
        row['company name']    = job.get('company_name', '')
        row['job link']        = job.get('jobUrl', '')
        row['experience']      = job.get('experienceLevel', '')
        row['salary']          = job.get('salary', '')
        row['date posted']     = job.get('postedTime', '')
        row['company_norm']    = job.get('company_name', '')
        row['Company Name']    = job.get('company_name', '')
        row['Company Name for Emails'] = job.get('company_name', '')

        # ----- HR fields --------------------------------------------------
        row['First Name']      = hr.get('first_name', '')
        row['Last Name']       = hr.get('last_name', '')
        row['Title']           = hr.get('job_title', '')
        row['Email']           = hr.get('email', '')
        row['Email Status']    = 'Verified' if pd.notna(hr.get('email')) and hr.get('email') else ''
        row['Seniority']       = hr.get('seniority_level', '')
        row['# Employees']     = hr.get('company_size', '')
        row['Industry']        = hr.get('industry', '')
        row['Keywords']        = hr.get('keywords', '')
        row['Website']         = hr.get('company_website', '')
        row['Company Linkedin Url'] = company_linkedin  # FROM JOBS FILE
        row['City']            = hr.get('city', '')
        row['State']           = hr.get('state', '')
        row['Country']         = hr.get('country', '')
        row['Company Address'] = hr.get('company_full_address', '')
        row['Company City']    = hr.get('company_city', '')
        row['Company State']   = hr.get('company_state', '')
        row['Company Country'] = hr.get('company_country', '')
        row['Company Phone']   = hr.get('company_phone', '')
        row['Mobile Phone']    = hr.get('company_phone', '')
        row['Technologies']    = hr.get('company_technologies', '')
        row['Annual Revenue']  = hr.get('company_annual_revenue', '')
        row['Total Funding']   = hr.get('company_total_funding', '')
        row['Secondary Email'] = hr.get('personal_email', '')

        # ----- Static -----------------------------------------------------
        row['Stage'] = 'Cold'

        output_rows.append(row)


# DEBUG: Show which companies matched
matched_companies = set(jobs_df[jobs_df['norm_company'].isin(hr_lookup.keys())]['company_name'])
print("\nMATCHED COMPANIES:")
for c in sorted(matched_companies):
    job_count = len(jobs_df[jobs_df['norm_company'] == normalise_company(c)])
    hr_count = len(hr_lookup.get(normalise_company(c), []))
    print(f"   • {c} → {job_count} job(s) × {hr_count} HR = {job_count * hr_count} row(s)")


# ----------------------------------------------------------------------
# 7. Save to l_hr.csv
# ----------------------------------------------------------------------
final_df = pd.DataFrame(output_rows, columns=target_columns)

# Ensure output directory exists
Path(out_dir).mkdir(parents=True, exist_ok=True)
output_path = Path(out_dir) / output_file

final_df.to_csv(output_path, index=False)

print(f"SUCCESS!")
print(f"   {len(final_df):,} rows merged")
print(f"   Saved to: {output_path}")