import pandas as pd
import h2o

# Initialize H2O cluster
h2o.init()

# Sample Pandas DataFrame with cleaned rules
data = {
    'variable': ['var1', 'var2', 'var3'],
    'coefficient': [0.5, -0.3, 1.2],
    'support': [200, 150, 100],
    'rule_1': ['var1 >= -0.5', 'var1 < 1.5', 'var5 >= 0.0'],
    'rule_2': ['var2 < 0.5', 'var2 >= 1.0', 'var6 < 100'],
    'rule_3': ['var3 >= 123', 'var4 < 50', 'var7 >= 10']
}

df_rules = pd.DataFrame(data)

# Assuming we have a larger H2O frame "Dev_data"
# Load or convert your data into H2O frame
# For demonstration, let's create a sample H2O frame
import numpy as np
sample_data = {
    'var1': np.random.randn(1000),
    'var2': np.random.randn(1000),
    'var3': np.random.randn(1000),
    'var4': np.random.randn(1000),
    'var5': np.random.randn(1000),
    'var6': np.random.randn(1000),
    'var7': np.random.randn(1000),
    'frd': np.random.randint(0, 2, 1000)
}
Dev_data = h2o.H2OFrame(pd.DataFrame(sample_data))

# Function to apply rules on H2O frame and calculate metrics
def apply_rules_and_calculate_metrics(df_rules, h2o_df, top_n=6):
    # Sort by support and take top N
    df_top_rules = df_rules.sort_values(by='support', ascending=False).head(top_n)
    
    rule_metrics = []
    total_fraud_cases = h2o_df['frd'].sum()
    
    for index, row in df_top_rules.iterrows():
        rule_1 = row['rule_1']
        rule_2 = row['rule_2']
        rule_3 = row['rule_3']
        
        combined_rule = f'({rule_1}) & ({rule_2}) & ({rule_3})'
        
        # Apply the rule on H2O frame
        h2o_df['rule_hit'] = h2o_df.eval(combined_rule)
        
        total_hit = h2o_df[h2o_df['rule_hit'] == 1].nrow
        total_fraud_hit = h2o_df[(h2o_df['rule_hit'] == 1) & (h2o_df['frd'] == 1)].nrow
        
        rule_set_hit_rate = total_fraud_hit / total_hit if total_hit != 0 else 0
        rule_total_fraud_capture = total_fraud_hit / total_fraud_cases if total_fraud_cases != 0 else 0
        
        rule_metrics.append([row['support'], rule_1, rule_2, rule_3, rule_set_hit_rate, rule_total_fraud_capture])
        
    # Convert to Pandas DataFrame
    metrics_df = pd.DataFrame(rule_metrics, columns=['Support', 'rule_1', 'rule_2', 'rule_3', 'rule_set_hit_rate', 'rule_total_fraud_capture'])
    
    return metrics_df

# Calculate the metrics
result_df = apply_rules_and_calculate_metrics(df_rules, Dev_data)

# Display the result
print(result_df)
