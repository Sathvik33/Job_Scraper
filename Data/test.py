import pandas as pd

# Define the input and a new output filename
input_filename = "jobs_raw_data.csv"
output_filename = "jobswithout.csv"

try:
    # Load the dataset from your local file
    df = pd.read_csv(input_filename)
    print(f"Successfully loaded '{input_filename}'.")

    # Check if the 'Full Description' column exists
    if 'Full Description' in df.columns:
        # Remove the 'Full Description' column
        df_modified = df.drop(columns=['Full Description'])
        print("Removing 'Full Description' column...")

        # Save the modified dataset to a new CSV file
        df_modified.to_csv(output_filename, index=False)

        print(f"Successfully saved the updated data to '{output_filename}'.")
    else:
        print("The 'Full Description' column was not found in the dataset.")

except FileNotFoundError:
    print(f"Error: Make sure the file '{input_filename}' is in the same folder as this script.")
except Exception as e:
    print(f"An error occurred: {e}")