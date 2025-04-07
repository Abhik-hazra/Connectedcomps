import pandas as pd

# Assume 'df' is your main DataFrame, with target column 'frd'
# Assume 'pd_rules' is the DataFrame with rule_1, rule_2, rule_3

rule_metrics = []

for idx, row in pd_rules.iterrows():
    rule_conditions = []
    
    for rule_col in ["rule_1", "rule_2", "rule_3"]:
        condition = row[rule_col]
        if pd.notnull(condition) and condition != "None":
            rule_conditions.append(condition.strip())

    # Only proceed if we have at least one valid rule
    if rule_conditions:
        # Combine rule string into one pandas-evaluable condition
        rule_expr = " & ".join([f"({cond})" for cond in rule_conditions])
        
        try:
            # Apply the combined rule on the dataframe
            rule_mask = df.eval(rule_expr)
        except Exception as e:
            print(f"Error evaluating rule on row {idx}: {e}")
            continue
        
        # Add rule flag column (optional if you want to inspect later)
        df[f"Ruleset_{idx+1}"] = rule_mask.astype(int)
        
        # Subset where rule matches
        rule_df = df[rule_mask]
        
        fraud_count = rule_df["frd"].sum()
        total_count = rule_df.shape[0]
        nonfraud_count = total_count - fraud_count
        
        # Avoid division by zero
        fraud_pct = (fraud_count / total_count * 100) if total_count else 0
        nonfraud_pct = (nonfraud_count / total_count * 100) if total_count else 0
        precision = (fraud_count / total_count) if total_count else 0
        recall = (fraud_count / df["frd"].sum()) if df["frd"].sum() else 0

        rule_metrics.append({
            "Ruleset": f"Ruleset_{idx+1}",
            "Rule Expression": rule_expr,
            "Fraud Count": int(fraud_count),
            "Non-Fraud Count": int(nonfraud_count),
            "% Fraud": round(fraud_pct, 2),
            "% Non-Fraud": round(nonfraud_pct, 2),
            "Precision": round(precision, 4),
            "Recall": round(recall, 4)
        })

# Create summary DataFrame
ruleset_summary = pd.DataFrame(rule_metrics)

# Show result
import ace_tools as tools; tools.display_dataframe_to_user(name="Ruleset Summary", dataframe=ruleset_summary)
