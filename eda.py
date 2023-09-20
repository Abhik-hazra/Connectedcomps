# Import necessary libraries
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import matplotlib.pyplot as plt
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.classification import RuleFit
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# Create a Spark session
spark = SparkSession.builder.appName("FraudPrediction").getOrCreate()

# Load your data into a DataFrame (replace 'your_data.csv' with your actual data source)
data = spark.read.csv("your_data.csv", header=True, inferSchema=True)

# Check the first few rows of the DataFrame
data.show(5)

# Get basic statistics of numeric columns
data.describe().show()

# Check the data types of each column
data.printSchema()

# Check for missing values
missing_data = data.select([F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)).alias(c) for c in data.columns])
missing_data.show()

# Explore categorical variables
categorical_cols = [col for col, dtype in data.dtypes if dtype == "string"]
for col in categorical_cols:
    data.groupBy(col).count().show()

# Explore numeric variables
numeric_cols = [col for col, dtype in data.dtypes if dtype != "string"]

# IQR method for detecting outliers for a specific numeric variable ('age')
for col in numeric_cols:
    if col == 'age':  # Specify the variable you want to check for outliers
        # Calculate IQR and filter potential outliers
        Q1 = data.approxQuantile(col, [0.25], 0.01)[0]
        Q3 = data.approxQuantile(col, [0.75], 0.01)[0]
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        # Create a new column to flag potential outliers using IQR
        data = data.withColumn(col + "_outlier_iqr", (F.col(col) < lower_bound) | (F.col(col) > upper_bound))

# Z-score validation for potential outliers
for col in numeric_cols:
    if col == 'age':  # Specify the variable you want to check for outliers
        # Calculate mean and standard deviation
        mean_col = data.select(F.mean(col)).collect()[0][0]
        std_col = data.select(F.stddev(col)).collect()[0][0]

        # Calculate Z-score and flag potential outliers using Z-score
        data = data.withColumn(col + "_z_score", ((F.col(col) - mean_col) / std_col).cast("double"))
        data = data.withColumn(col + "_outlier_z_score", (F.abs(F.col(col + "_z_score")) > 3))  # Adjust the threshold as needed

# Create a variable for class weights to handle class imbalance
# Replace 'target_col' with the name of your target variable
target_col = "frd"
class_weights = data.groupBy(target_col).count().withColumn("weight", F.when(F.col("count") > 0, F.max("count") / F.col("count")).otherwise(0))

# Join the class weights with the original data
data = data.join(class_weights, on=target_col, how="left")

# Fill missing class weights with a default value (e.g., 1.0)
data = data.fillna(1.0, subset=["weight"])

# Continue with RuleFit modeling
# Example: One-hot encoding for a categorical column 'color'
encoder = OneHotEncoder(inputCol="color", outputCol="color_encoded")
indexed_data = encoder.transform(data)

# Example: Label encoding for a categorical column 'education'
indexer = StringIndexer(inputCol="education", outputCol="education_encoded")
encoded_data = indexer.fit(indexed_data).transform(indexed_data)

# Select features and target variable
selected_features = [...  # List of feature columns ]
target = "frd"

# Assemble feature columns into a vector column
feature_assembler = VectorAssembler(inputCols=selected_features, outputCol="features")
assembled_data = feature_assembler.transform(encoded_data)

# Initialize and train the RuleFit model
rule_fit = RuleFit(featuresCol="features", labelCol=target, weightCol="weight")
rule_fit_model = rule_fit.fit(assembled_data)

# Review generated rules
generated_rules = rule_fit_model.getRules()
generated_rules.show()

# Evaluate the RuleFit model (use appropriate metrics for your problem)
evaluator = BinaryClassificationEvaluator(labelCol=target)
area_under_roc = evaluator.evaluate(rule_fit_model.transform(assembled_data))
print(f"Area under ROC: {area_under_roc}")

# Stop the Spark session
spark.stop()
