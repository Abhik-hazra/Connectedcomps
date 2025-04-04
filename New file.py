# Subset of rows that fell into the bin (-0.001, 61.375]
target_bin_mask = df["force_002_bin"] == pd.Interval(-0.001, 61.375, closed="right")

subset_df = df[target_bin_mask].copy()
print(subset_df.shape)

subset_df["force_002_subbin"] = pd.qcut(subset_df["force_002"], q=5, duplicates="drop")

grouped = subset_df.groupby("force_002_subbin")

fraud_count = grouped["frd"].sum()
total_count = grouped["frd"].count()

summary_subset = pd.DataFrame({
    "Bin": fraud_count.index.astype(str),
    "Fraud Count": fraud_count,
    "Total Count": total_count,
    "% Fraud": round(fraud_count / total_count * 100, 2),
})

print(summary_subset)

import matplotlib.pyplot as plt

subset_df["force_002"].hist(bins=20)
plt.title("Distribution of force_002 in the bin (-0.001, 61.375]")
plt.xlabel("force_002")
plt.ylabel("Frequency")
plt.show()
