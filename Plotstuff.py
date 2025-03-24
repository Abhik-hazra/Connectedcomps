from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, udf
from pyspark.sql.types import IntegerType

# Step 1: Create Spark Session
spark = SparkSession.builder.appName("FeatureImpactBinning").getOrCreate()

# Step 2: Example data - replace this with your actual DataFrame
data = [
    (100.0, 0), (150.0, 0), (200.0, 1), (250.0, 1), (300.0, 0),
    (350.0, 0), (400.0, 1), (450.0, 1), (500.0, 0), (550.0, 1),
    (600.0, 0), (650.0, 0), (700.0, 1), (750.0, 1), (800.0, 0),
    (850.0, 1), (900.0, 0), (950.0, 1), (1000.0, 0), (1050.0, 1)
]
columns = ["feature1", "is_fraud"]
df = spark.createDataFrame(data, columns)

# Step 3: Define parameters
features = ["feature1"]  # You can add more features here
num_bins = 5

# Step 4: Create quantile bin edges
def create_quantile_bins(df, col_name, num_bins):
    quantiles = df.approxQuantile(col_name, [i/num_bins for i in range(1, num_bins)], 0.01)
    bin_edges = [-float('inf')] + quantiles + [float('inf')]
    return bin_edges

# Step 5: Assign bins using UDF
def assign_bin(value, edges):
    for i in range(len(edges) - 1):
        if edges[i] <= value < edges[i + 1]:
            return i
    return len(edges) - 2

def bin_column(df, col_name, edges):
    assign_bin_udf = udf(lambda x: assign_bin(x, edges), IntegerType())
    return df.withColumn(f"{col_name}_bin", assign_bin_udf(col(col_name)))

# Step 6: Compute fraud stats
def compute_bin_stats(df, bin_col):
    return df.groupBy(bin_col).agg(
        count("*").alias("total_count"),
        _sum("is_fraud").alias("fraud_count"),
        (count("*") - _sum("is_fraud")).alias("nonfraud_count"),
        (_sum("is_fraud") / count("*")).alias("bad_rate")
    ).orderBy(bin_col)

# Step 7: Process each feature
results = {}
for feature in features:
    bin_edges = create_quantile_bins(df, feature, num_bins)
    df_binned = bin_column(df, feature, bin_edges)
    bin_stats_df = compute_bin_stats(df_binned, f"{feature}_bin")
    results[feature] = bin_stats_df

# Show the results for each feature
for feature, stats_df in results.items():
    print(f"\n--- Stats for {feature} ---")
    stats_df.show()
