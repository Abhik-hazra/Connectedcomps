import pandas as pd
import os

# Function to find column containing party information
def find_party_column(df):
    party_columns = [col for col in df.columns if 'party' in col.lower() or 'pty' in col.lower()]
    if party_columns:
        return party_columns[0]
    else:
        return None

# Function to extract distinct parties from CSV files
def extract_parties(csv_files):
    parties = []
    for file in csv_files:
        df = pd.read_csv(file)
        party_column = find_party_column(df)
        if party_column:
            parties.extend(df[party_column].unique())
    return parties

# Function to create DataFrame with parties and file names
def create_party_dataframe(csv_files):
    party_list = extract_parties(csv_files)
    party_df = pd.DataFrame({'Party': party_list})
    party_df['File Name'] = [os.path.basename(file) for file in csv_files]
    return party_df

# Function to write party DataFrame to CSV
def write_party_csv(party_df, output_file):
    party_df.to_csv(output_file, index=False)

# Function to filter data based on group number(s)
def filter_data(csv_file, group_numbers):
    df = pd.read_csv(csv_file)
    filtered_tables = {}
    for group_number in group_numbers:
        filtered_tables[group_number] = df[df['GROUP'] == group_number]
    return filtered_tables

# Function to prompt user for group number(s)
def get_group_numbers():
    group_input = input("Enter group number(s) separated by commas: ")
    group_numbers = [int(num.strip()) for num in group_input.split(",")]
    return group_numbers

# Main function
def main():
    csv_files = [file for file in os.listdir() if file.endswith('.csv')]
    
    # Task 1: Filter data based on group numbers
    csv_file = input("Enter the path to the CSV file: ")
    file_prefix = input("Enter the prefix for output files (XYZ): ")
    group_numbers = get_group_numbers()
    filtered_tables = filter_data(csv_file, group_numbers)
    for group_number, table in filtered_tables.items():
        filename = f"{file_prefix}_Group{group_number}.csv"
        table.to_csv(filename, index=False)
    
    # Task 2: Create CSV containing distinct parties and file names
    party_df = create_party_dataframe(csv_files)
    output_file = input("Enter the name of the output CSV file for parties: ")
    write_party_csv(party_df, output_file)

if __name__ == "__main__":
    main()
