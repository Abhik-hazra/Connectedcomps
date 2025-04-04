import pandas as pd

# Assuming target column is named "frd" where 1 = fraud and 0 = non-fraud

total_fraud = df["frd"].sum()

info_list = []
for col in df.columns:
    if col != "frd":
        # Boolean mask of rows where the column is NaN
        missing_mask = df[col].isna()  
        missing_count = missing_mask.sum()
        
        # Fraud among the missing rows
        missing_fraud_count = df.loc[missing_mask, "frd"].sum()
        
        # % of total fraud contributed by these missing rows
        pct_of_total_fraud = 0
        if total_fraud > 0:
            pct_of_total_fraud = (missing_fraud_count / total_fraud) * 100
        
        info_list.append({
            "Column": col,
            "Missing Count": missing_count,
            "Fraud Count in Missing": missing_fraud_count,
            "% of Total Fraud in Missing": round(pct_of_total_fraud, 2)
        })

# Create a summary DataFrame
missing_info_df = pd.DataFrame(info_list)
print(missing_info_df)
