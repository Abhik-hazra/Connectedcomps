# Calculate bin-wise rates
vals['Fraud_Rate'] = (vals['fraud'] / vals['nrow'] * 100).round(2)
vals['False_Positive_Rate_Bin'] = (vals['non_fraud'] / vals['nrow'] * 100).round(2)

# The existing cumulative metrics remain unchanged
total_fraud = vals['fraud'].sum()
total_non_fraud = vals['non_fraud'].sum()
vals['Detection_Rate'] = (vals['cum_fraud'] / total_fraud * 100).round(2)
vals['False_Positive_Rate'] = (vals['cum_non_fraud'] / total_non_fraud * 100).round(2)
vals['Row_Rate'] = (vals['nrow'] / vals['nrow'].sum() * 100).round(2)
