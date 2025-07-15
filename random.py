import pandas as pd
import numpy as np

# Convert H2O frame to pandas DataFrame
new_data_pnh = data_pnh[['fraud', 'num_pgrnk_score_nb']].as_data_frame(use_pandas=True)

# Separate zero and non-zero scores
zero_mask = new_data_pnh['num_pgrnk_score_nb'] == 0
nonzero_mask = ~zero_mask

# Assign a special bin for zero values
new_data_pnh.loc[zero_mask, 'Quantile_Bin'] = -1  # or use 'zero' as a string label

# Bin non-zero values into quantiles
num_bins = 10
new_data_pnh.loc[nonzero_mask, 'Quantile_Bin'] = pd.qcut(
    new_data_pnh.loc[nonzero_mask, 'num_pgrnk_score_nb'],
    q=num_bins,
    labels=False,
    duplicates='drop'
)

# Convert Quantile_Bin to int for sorting
new_data_pnh['Quantile_Bin'] = new_data_pnh['Quantile_Bin'].astype(int)

# Prepare amount column
new_data_pnh['total_good_amount'] = (1 - new_data_pnh['fraud'])

# Group by Quantile_Bin
vals = new_data_pnh.groupby('Quantile_Bin').agg(
    nrow=('fraud', 'count'),
    fraud=('fraud', 'sum')
).reset_index()
vals['non_fraud'] = vals['nrow'] - vals['fraud']

# Cumulative metrics (sorted so zero bin comes first)
vals = vals.sort_values('Quantile_Bin').reset_index(drop=True)
vals['cum_fraud'] = vals['fraud'].cumsum()
vals['cum_non_fraud'] = vals['non_fraud'].cumsum()

# Evaluation metrics
total_fraud = vals['fraud'].sum()
total_non_fraud = vals['non_fraud'].sum()
vals['Fraud_Detection_Rate'] = np.round(vals['cum_fraud'] / total_fraud * 100, 2)
vals['False_Positive_Rate'] = np.round(vals['cum_non_fraud'] / total_non_fraud * 100, 2)
vals['Row_Rate'] = np.round(vals['nrow'] / vals['nrow'].sum() * 100, 2)

# Score bands for non-zero bins
score_bins = new_data_pnh[nonzero_mask].groupby('Quantile_Bin')['num_pgrnk_score_nb'].agg(['min', 'max']).reset_index()
vals = vals.merge(score_bins, on='Quantile_Bin', how='left')

# For the zero bin, set min and max as 0
vals.loc[vals['Quantile_Bin'] == -1, ['min', 'max']] = 0

# Display the final table
print(vals)
