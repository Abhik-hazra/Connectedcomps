from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString
from pyspark.ml import Pipeline
from pyspark.ml.feature import ChiSqSelector
import matplotlib.pyplot as plt

# Create a Spark session
spark = SparkSession.builder.appName("InformationGain").getOrCreate()

# Assuming you have a DataFrame 'df' with your data where 'label' is the target variable
# and columns 'feature1', 'feature2', etc., representing your features

# Assemble features into a single vector column
feature_columns = ['feature1', 'feature2', 'feature3']  # Replace these with your actual feature column names
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
assembled_data = assembler.transform(df)

# Indexing the label column
indexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(assembled_data)
assembled_data = indexer.transform(assembled_data)

# Indexing categorical features
featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(assembled_data)
data = featureIndexer.transform(assembled_data)

# Apply ChiSqSelector for feature selection
selector = ChiSqSelector(numTopFeatures=15, featuresCol="indexedFeatures",
                         outputCol="selectedFeatures", labelCol="indexedLabel")
selected_data = selector.fit(data).transform(data)

# Create a RandomForestClassifier to calculate feature importances
rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="selectedFeatures", numTrees=100)
model = rf.fit(selected_data)

# Get feature importances
importances = model.featureImportances.toArray()

# Create a list of feature names and their importance scores
feature_importance = list(zip(feature_columns, importances))
feature_importance.sort(key=lambda x: x[1], reverse=True)

# Extract top 15 important features and their importance scores
top_features = [feat[0] for feat in feature_importance[:15]]
top_scores = [feat[1] for feat in feature_importance[:15]]

# Visualize information gain scores
plt.figure(figsize=(10, 6))
plt.barh(top_features, top_scores, color='skyblue')
plt.xlabel('Information Gain')
plt.title('Top 15 Feature Importance Scores')
plt.gca().invert_yaxis()  # Invert y-axis to display highest gain at the top
plt.tight_layout()
plt.show()

# Display selected top 15 features
print("Top 15 Selected Features:")
for feature, score in zip(top_features, top_scores):
    print(f"Feature: {feature}, Importance: {score}")
