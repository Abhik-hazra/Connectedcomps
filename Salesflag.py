
import pandas as pd

def calculate_sales_growth(df):
    # Calculate the growth rate column-wise and convert to percentages
    df['GrowthPercentage'] = ((df['day2'] - df['day1']) / df['day1']) * 100
    
    # Create a flag for sales growth above 20%
    df['HighGrowthFlag'] = df['GrowthPercentage'] > 20
    
    return df

# Sample DataFrame with hypothetical data
data = {'Name': ['Alice', 'Bob', 'Charlie'],
        'day1': [100, 150, 200],
        'day2': [120, 180, 220]}
df = pd.DataFrame(data)

# Call the function to calculate sales growth and flags
result_df = calculate_sales_growth(df)

print(result_df)
