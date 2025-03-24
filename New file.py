# Step 1: Select the two columns and convert to Pandas
sample_df = df.select("feature1", "frd").toPandas()

# Step 2: Drop NA if any
sample_df = sample_df.dropna(subset=["feature1", "frd"])

# Step 3: Create 5 quantile bins using pd.qcut
sample_df["feature1_bin"] = pd.qcut(sample_df["feature1"], q=5, duplicates='drop')

# Step 4: Group by bin and calculate metrics
bin_stats = sample_df.groupby("feature1_bin").agg(
    total_count=("frd", "count"),
    fraud_count=("frd", "sum"),
    nonfraud_count=("frd", lambda x: (x == 0).sum()),
)

# Step 5: Compute bad rate
bin_stats["bad_rate"] = bin_stats["fraud_count"] / bin_stats["total_count"]

# Optional: reset index to flatten the bin column
bin_stats = bin_stats.reset_index()

# Show the result
print(bin_stats)
