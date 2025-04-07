import pandas as pd
import itertools
from sklearn.metrics import precision_score

# Assuming your dataframe is called df,
# and it has a column 'frd' with 0/1 for actual fraud vs. non-fraud.

# 1) Identify your ruleset columns
ruleset_cols = [col for col in df.columns if col.startswith('Ruleset_')]

# 2) Prepare a list to store (combination, precision) results
results = []

# 3) Loop through subset sizes from 1 to 6
for r in range(1, 7):
    # Generate all combinations of the ruleset columns of size r
    for combo in itertools.combinations(ruleset_cols, r):
        
        # Combine them with logical OR (any rule triggers 'fraud')
        # Another approach is: (df[list(combo)].max(axis=1)), but summation>0 is essentially the same.
        y_pred = (df[list(combo)].sum(axis=1) > 0).astype(int)
        
        # Calculate precision
        precision = precision_score(df['frd'], y_pred)
        
        # Store result as (tuple_of_rule_names, precision)
        results.append((combo, precision))

# 4) Create a DataFrame of results
results_df = pd.DataFrame(results, columns=['Combination', 'Precision'])

# 5) Sort by precision descending
results_df = results_df.sort_values(by='Precision', ascending=False, ignore_index=True)

# 6) Show the top combos
print(results_df.head(20))  # e.g., top 20
