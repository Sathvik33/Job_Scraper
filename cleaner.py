import pandas as pd

def remove_duplicates_by_joburl(input_file, output_file):
    try:
        # Read the CSV file
        print(f"Reading data from: {input_file}")
        df = pd.read_csv(input_file)
        
        # Display original data info
        print(f"Original data shape: {df.shape}")
        print(f"Number of unique joburls: {df['jobUrl'].nunique()}")
        
        # Remove duplicates based on joburl column
        df_cleaned = df.drop_duplicates(subset=['jobUrl'], keep='first')
        
        # Display cleaned data info
        print(f"Cleaned data shape: {df_cleaned.shape}")
        print(f"Number of duplicates removed: {len(df) - len(df_cleaned)}")
        
        # Save the cleaned data to new CSV file
        df_cleaned.to_csv(output_file, index=False)
        print(f"Cleaned data saved to: {output_file}")
        
        return df_cleaned
        
    except FileNotFoundError:
        print(f"Error: Input file not found at {input_file}")
        return None
    except KeyError:
        print("Error: 'joburl' column not found in the CSV file")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

input_file = r"C:\Sathvik-py\Talrn\job_scraper\Data\Linkedin Only India and remote and contract.csv"
output_file = r"C:\Sathvik-py\Talrn\job_scraper\Data\linkedin_jobs.csv"

cleaned_data = remove_duplicates_by_joburl(input_file, output_file)

if cleaned_data is not None:
    print("\nProcess completed successfully!")
    print(f"Final dataset has {len(cleaned_data)} unique job entries")
else:
    print("\nProcess failed!")

c=0
for i in cleaned_data['companyName']:
    if i=='Turing':
        c+=1

print(f"\nNumber of job listings from Turing: {c}")