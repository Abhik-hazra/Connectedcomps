# Define the rule
rule_1_mask = (
    (df["avg_custtimeinfile_nb"] >= 8.98) &
    (df["force_0038"] < 1.5) &
    (df["force_0039"] < 1.5)
)

# Create a new column for rule_1: 1 if the rule is satisfied, else 0
df["rule_1"] = rule_1_mask.astype(int)


# Get total frauds in full dataset
total_fraud = df["frd"].sum()

# Get rows where rule_1 is triggered
rule_1_df = df[df["rule_1"] == 1]

# Count frauds and non-frauds under the rule
fraud_under_rule = rule_1_df["frd"].sum()
nonfraud_under_rule = rule_1_df.shape[0] - fraud_under_rule

# Fraud % and Non-Fraud % under the rule
fraud_pct = fraud_under_rule / rule_1_df.shape[0] * 100
nonfraud_pct = nonfraud_under_rule / rule_1_df.shape[0] * 100

# Precision: of those flagged by the rule, how many are fraud
precision = fraud_under_rule / rule_1_df.shape[0]

# Recall: of all frauds, how many were captured by the rule
recall = fraud_under_rule / total_fraud

# Display results
print(f"Rule 1 Metrics:")
print(f" - Fraud Count under Rule: {fraud_under_rule}")
print(f" - Non-Fraud Count under Rule: {nonfraud_under_rule}")
print(f" - Fraud % under Rule: {fraud_pct:.2f}%")
print(f" - Non-Fraud % under Rule: {nonfraud_pct:.2f}%")
print(f" - Precision: {precision:.4f}")
print(f" - Recall: {recall:.4f}")


