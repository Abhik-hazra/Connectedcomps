# Pivot to get fraud (frd=1) and non-fraud (frd=0) counts side by side
pivot_df = grouped_counts.pivot(index="force_0058", columns="frd", values="count").fillna(0)
pivot_df.columns = ["non_fraud_count", "fraud_count"]  # assuming frd=0 is first, frd=1 is second

# Calculate total and fraud rate
pivot_df["total"] = pivot_df["fraud_count"] + pivot_df["non_fraud_count"]
pivot_df["fraud_rate"] = pivot_df["fraud_count"] / pivot_df["total"]

# Sort categories by fraud rate descending
pivot_df_sorted = pivot_df.sort_values(by="fraud_rate", ascending=False)


import matplotlib.pyplot as plt

# Plotting
fig, ax = plt.subplots(figsize=(12, 6))

pivot_df_sorted[["non_fraud_count", "fraud_count"]].plot(
    kind="bar",
    stacked=True,
    ax=ax,
    color=["lightgray", "red"]
)

plt.title("Distribution of Fraud and Non-Fraud by force_0058 Category")
plt.xlabel("force_0058")
plt.ylabel("Count")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.show()


pivot_df_sorted["fraud_rate"].plot(kind="bar", figsize=(12, 5), color="orange")
plt.title("Fraud Rate by force_0058 Category")
plt.xlabel("force_0058")
plt.ylabel("Fraud Rate")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.show()
