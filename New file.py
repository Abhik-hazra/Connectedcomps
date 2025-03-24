import pandas as pd
import matplotlib.pyplot as plt

def analyze_fraud_by_feature_bins(spark_df, features, fraud_col="frd", bins=5):
    # Step 1: Convert all required columns at once
    required_cols = features + [fraud_col]
    print(f"Converting columns to pandas: {required_cols}")
    df_pd = spark_df.select(*required_cols).toPandas().dropna()

    # Step 2: Analyze each feature
    for feature in features:
        print(f"\nAnalyzing feature: {feature}")
        
        try:
            # Bin into quantiles
            df_pd[f"{feature}_bin"] = pd.qcut(df_pd[feature], q=bins, duplicates='drop')
        except ValueError as e:
            print(f"Could not bin feature {feature}: {e}")
            continue

        # Aggregation
        bin_stats = df_pd.groupby(f"{feature}_bin").agg(
            total_count=(fraud_col, "count"),
            fraud_count=(fraud_col, "sum"),
            nonfraud_count=(fraud_col, lambda x: (x == 0).sum())
        ).reset_index()

        # Derived metrics
        total_frauds = bin_stats["fraud_count"].sum()
        total_nonfrauds = bin_stats["nonfraud_count"].sum()
        total_rows = bin_stats["total_count"].sum()

        bin_stats["bad_rate"] = bin_stats["fraud_count"] / bin_stats["total_count"]
        bin_stats["fraud_pct"] = bin_stats["fraud_count"] / total_frauds
        bin_stats["nonfraud_pct"] = bin_stats["nonfraud_count"] / total_nonfrauds
        bin_stats["fraud_lift"] = bin_stats["fraud_pct"] / (total_frauds / total_rows)

        print(bin_stats)

        # Plot
        fig, ax1 = plt.subplots(figsize=(10, 6))
        ax1.bar(bin_stats.index, bin_stats["nonfraud_count"], label="Non-Fraud", alpha=0.6)
        ax1.bar(bin_stats.index, bin_stats["fraud_count"], bottom=bin_stats["nonfraud_count"],
                label="Fraud", alpha=0.9)
        ax1.set_ylabel("Transaction Count")
        ax1.set_xlabel(f"{feature} Bins")
        ax1.set_xticks(bin_stats.index)
        ax1.set_xticklabels([f"Bin {i}" for i in range(len(bin_stats))], rotation=45)
        ax1.legend(loc="upper left")

        ax2 = ax1.twinx()
        ax2.plot(bin_stats.index, bin_stats["bad_rate"], color="red", marker="o", label="Bad Rate")
        ax2.set_ylabel("Bad Rate", color="red")
        ax2.tick_params(axis="y", labelcolor="red")
        ax2.legend(loc="upper right")

        plt.title(f"Fraud Analysis by {feature} Bins")
        plt.tight_layout()
        plt.show()
