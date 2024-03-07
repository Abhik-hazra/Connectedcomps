import matplotlib.pyplot as plt

# Assuming df is your PySpark DataFrame containing the target column 'frd' and other variables

# Define the variable you want to visualize
variable_to_visualize = 'your_variable_name'

# Filter the DataFrame for fraud and non-fraud cases
fraud_data = df.filter(df['frd'] == 1)
non_fraud_data = df.filter(df['frd'] == 0)

# Plotting for fraud cases
plt.figure(figsize=(10, 6))
plt.hist(fraud_data.select(variable_to_visualize).rdd.flatMap(lambda x: x).collect(), bins=20, color='r', alpha=0.5)
plt.title(f'Distribution of {variable_to_visualize} for Fraud Cases')
plt.xlabel(variable_to_visualize)
plt.ylabel('Frequency')
plt.grid(True)
plt.show()

# Plotting for non-fraud cases
plt.figure(figsize=(10, 6))
plt.hist(non_fraud_data.select(variable_to_visualize).rdd.flatMap(lambda x: x).collect(), bins=20, color='b', alpha=0.5)
plt.title(f'Distribution of {variable_to_visualize} for Non-Fraud Cases')
plt.xlabel(variable_to_visualize)
plt.ylabel('Frequency')
plt.grid(True)
plt.show()
