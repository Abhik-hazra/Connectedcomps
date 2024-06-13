import pandas as pd

# Sample DataFrame
data = {
    'variable': ['var1', 'var2', 'var3'],
    'coefficient': [0.5, -0.3, 1.2],
    'support': [100, 150, 200],
    'rule': [
        '(var1 >= -0.5) & (var2 < 0.5 or var2 is NA) & (var3 >= 123 or Var3 is NA)',
        '(var1 < 1.5) & (var2 >= 1.0 or var2 is NA) & (var4 < 50 or var4 is NA)',
        '(var5 >= 0.0) & (var6 < 100 or var6 is NA) & (var7 >= 10 or var7 is NA)'
    ]
}

df = pd.DataFrame(data)

def split_rule(rule):
    # Remove the parts mentioning "is NA"
    rule = rule.replace(' or ', ' & ').replace('is NA', '')
    # Split by '&' and clean up the rules
    parts = [r.strip() for r in rule.split('&')]
    # Return the three parts or fill with None if less than 3
    return parts[:3] + [None] * (3 - len(parts))

# Apply the function to split the rules
split_rules = df['rule'].apply(split_rule)

# Convert the result to a DataFrame and name the columns
split_df = pd.DataFrame(split_rules.tolist(), columns=['rule_1', 'rule_2', 'rule_3'])

# Concatenate the original DataFrame with the new columns
df = pd.concat([df, split_df], axis=1)

# Drop the original 'rule' column if not needed
df.drop(columns=['rule'], inplace=True)

# Display the DataFrame
print(df)
