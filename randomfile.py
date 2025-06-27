import pandas as pd
import numpy as np

# 1. Convert H2O frame to Pandas DataFrame
new_data = data_w_pred[['fraud', 'addr_prgnk', 'tdp_client_amt']].as_data_frame(use_pandas=True)

# 2. Create 4 equal-frequency bins based on 'addr_prgnk'
new_data['Quantile_Bin'] = pd.qcut(new_data['addr_prgnk'], q=4, labels=False, duplicates='drop')

# 3. Clean and prepare amount column (optional but retained from your logic)
new_data['fraud_amount'] = pd.to_numeric(new_data['fraud'], errors='coerce') * pd.to_numeric(new_data['tdp_client_amt'], errors='coerce')
new_data['total_good_amount'] = (1 - new_data['fraud']) * pd.to_numeric(new_data['tdp_client_amt'], errors='coerce')

# 4. Group by the new bin
vals = new_data.groupby('Quantile_Bin').agg(
    nrow=('fraud', 'count'),
    fraud=('fraud', 'sum')
).reset_index()

vals['non_fraud'] = vals['nrow'] - vals['fraud']

# 5. Cumulative metrics
vals['cum_fraud'] = vals['fraud'].cumsum()
vals['cum_non_fraud'] = vals['non_fraud'].cumsum()

# 6. Evaluation metrics
total_fraud = vals['fraud'].sum()
total_non_fraud = vals['non_fraud'].sum()

vals['Fraud_Detection_Rate'] = np.round(vals['cum_fraud'] / total_fraud * 100, 2)
vals['False_Positive_Rate'] = np.round(vals['cum_non_fraud'] / total_non_fraud * 100, 2)
vals['Fraud_Rate'] = np.round(vals['fraud'] / vals['nrow'] * 100, 2)

# 7. Print results
print(vals)
