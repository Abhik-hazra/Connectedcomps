# Plot
fig, ax1 = plt.subplots(figsize=(10, 6))

# Bar plot for fraud and non-fraud volume
ax1.bar(bin_stats.index, bin_stats["nonfraud_count"], label="Non-Fraud", alpha=0.6)
ax1.bar(bin_stats.index, bin_stats["fraud_count"], label="Fraud", alpha=0.9, bottom=bin_stats["nonfraud_count"])
ax1.set_ylabel("Transaction Count")
ax1.set_xlabel("Feature1 Bins")
ax1.set_xticks(bin_stats.index)
ax1.set_xticklabels([f"Bin {i}" for i in range(len(bin_stats))], rotation=45)
ax1.legend(loc="upper left")

# Add a second Y-axis for bad rate
ax2 = ax1.twinx()
ax2.plot(bin_stats.index, bin_stats["bad_rate"], color="red", marker="o", label="Bad Rate")
ax2.set_ylabel("Bad Rate", color="red")
ax2.tick_params(axis="y", labelcolor="red")
ax2.legend(loc="upper right")

plt.title("Fraud and Non-Fraud Distribution by Feature1 Bins")
plt.tight_layout()
plt.show()
