import pandas as pd
import matplotlib.pyplot as plt

# Step 1: Select and convert to pandas
sample_df = df.select("feature1", "frd").toPandas()
sample_df = sample_df.dropna(subset=["feature1", "frd"])

# Step 2: Bin using quantiles
sample_df["feature1_bin"] = pd.qcut(sample_df["feature1"], q=5, duplicates='drop')

# Step 3: Aggregation
bin_stats = sample_df.groupby("feature1_bin").agg(
    total_count=("frd", "count"),
    fraud_count=("frd", "sum"),
    nonfraud_count=("frd", lambda x: (x == 0).sum()),
).reset_index()

# Step 4: Overall totals for % calculations
total_frauds = bin_stats["fraud_count"].sum()
total_nonfrauds = bin_stats["nonfraud_count"].sum()
total_rows = bin_stats["total_count"].sum()

# Step 5: Derived metrics
bin_stats["bad_rate"] = bin_stats["fraud_count"] / bin_stats["total_count"]
bin_stats["fraud_pct"] = bin_stats["fraud_count"] / total_frauds
bin_stats["nonfraud_pct"] = bin_stats["nonfraud_count"] / total_nonfrauds
bin_stats["fraud_lift"] = bin_stats["fraud_pct"] / (total_frauds / total_rows)

# View it
print(bin_stats)
