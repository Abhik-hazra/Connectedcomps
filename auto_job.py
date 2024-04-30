import pandas as pd
from datetime import datetime

# Task 1
csv_file = input("Enter the name of the CSV file: ")
df = pd.read_csv(csv_file)

# Task 2
group_numbers = input("Enter the group numbers separated by commas: ").split(',')

# Task 3
# Subtask 3.1
group_dataframes = {}
for group_no in group_numbers:
    group_df = df[df.columns[df.columns.str.contains('GRP|GROUP|GROUP_NO', case=False)]]
    group_dataframes["Ring_dtl_" + group_no] = group_df

# Subtask 3.2
party_ids = []
group_names = []
for group_name, group_df in group_dataframes.items():
    party_id_col_names = group_df.columns[group_df.columns.str.contains('PTYID|PTY_ID|Party|PTY|Active_PTY', case=False)]
    for col_name in party_id_col_names:
        party_ids.extend(group_df[col_name].unique())
        group_names.extend([group_name] * len(group_df[col_name].unique()))

party_df = pd.DataFrame({'Party_ID': party_ids, 'Group_No': group_names})

# Task 4
# Write 'n' DataFrames
for group_name, group_df in group_dataframes.items():
    group_df.to_csv(f"{group_name}.csv", index=False)

# Write the party DataFrame with the specified naming convention
current_date = datetime.now().strftime("%Y%m%d")
party_df.to_csv(f"REF_PTYIDS_{current_date}.csv", index=False)
