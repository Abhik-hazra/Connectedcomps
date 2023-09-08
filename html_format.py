import pandas as pd

# Sample DataFrame
data = {'Name': ['Andrea', 'Brian', 'Charles', 'Alan'],
         'Flag': [1, 0, 1, 0]}
df = pd.DataFrame(data)

# Convert DataFrame to HTML table with conditional row styling
def apply_row_style(row):
    if row['Flag'] == 1:
        return 'background-color: red;'
    return ''

# Create the HTML table with conditional row styling
html_table = '<table>'
html_table += '<tr><th>Name</th><th>Flag</th></tr>'
for index, row in df.iterrows():
    row_style = apply_row_style(row)
    html_table += f'<tr style="{row_style}"><td>{row["Name"]}</td><td>{row["Flag"]}</td></tr>'
html_table += '</table>'

# Print the HTML table
print(html_table)
