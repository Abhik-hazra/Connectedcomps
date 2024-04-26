from datetime import datetime, timedelta

# Get today's date
today = datetime.today()

# Calculate the date six months ago
six_months_ago = today - timedelta(days=6*30) # approximate calculation

# Get the 1st day of the month
first_day_of_six_months_ago = six_months_ago.replace(day=1)

# Format the date as a string
date_string = first_day_of_six_months_ago.strftime('%Y-%m-%d')

print("Date of the 1st day of the month six months back:", date_string)
