from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import matplotlib.pyplot as plt
import numpy as np

# Initialize Spark session
spark = SparkSession.builder.appName("FraudDetection").getOrCreate()

# Assuming your data is loaded into a DataFrame called 'df'
# Features provided by the user
selected_features_list = ['feature1', 'feature2', 'feature3', 'feature4']

# Handle string columns using StringIndexer
string_columns = [col for col, dtype in df.dtypes if dtype == 'string']
indexers = [
    StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid="keep")
    for col in string_columns
]

# Assemble selected features into a single vector column
assembler = VectorAssembler(
    inputCols=selected_features_list + [f"{col}_index" for col in string_columns],
    outputCol="features"
)

# Define RandomForestClassifier
rf = RandomForestClassifier(labelCol="frd", featuresCol="features", numTrees=100)

# Pipeline for preprocessing
pipeline = Pipeline(stages=indexers + [assembler, rf])

# Fit the pipeline to transform the data
pipeline_model = pipeline.fit(df)
transformed_df = pipeline_model.transform(df)

# Split the data into train and test sets
(train, test) = transformed_df.randomSplit([0.8, 0.2], seed=123)

# Train the RandomForestClassifier model
rf_model = rf.fit(train)

# Make predictions on the test set
predictions = rf_model.transform(test)

# Evaluate model performance using BinaryClassificationEvaluator
evaluator = BinaryClassificationEvaluator(labelCol="frd", metricName="areaUnderROC")
roc = evaluator.evaluate(predictions)

print(f"Area under ROC curve: {roc}")

# Extracting probabilities of being in class 1 (fraud)
probs = np.array(predictions.select("probability").rdd.map(lambda x: x[0][1]).collect())

# Sort the probabilities and get corresponding indexes
sorted_indexes = np.argsort(probs)

# Calculate cumulative gains and plot the gain chart
cumulative_fraud = np.cumsum(np.array(test.select("frd").rdd.map(lambda x: x[0]).collect())[sorted_indexes])
total_frauds = np.sum(np.array(test.select("frd").rdd.map(lambda x: x[0]).collect()))
cumulative_fraction = np.arange(1, len(cumulative_fraud) + 1) / len(cumulative_fraud)

plt.figure(figsize=(8, 6))
plt.plot(cumulative_fraction, cumulative_fraud / total_frauds, label='RandomForestClassifier')
plt.plot([0, 1], [0, 1], 'k--', label='Random')
plt.xlabel('Fraction of samples')
plt.ylabel('Cumulative gains')
plt.title('Gain Chart')
plt.legend()
plt.show()

# Stop the Spark session
spark.stop()
