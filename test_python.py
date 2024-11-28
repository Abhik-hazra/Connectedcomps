# Test pandas and numpy installations
import pandas as pd
import numpy as np

try:
    # Test numpy
    print("Testing numpy...")
    array = np.array([1, 2, 3, 4, 5])
    print("Numpy array:", array)
    print("Array mean:", np.mean(array))

    # Test pandas
    print("\nTesting pandas...")
    data = {'Name': ['Alice', 'Bob', 'Charlie'], 'Age': [25, 30, 35]}
    df = pd.DataFrame(data)
    print("Pandas DataFrame:")
    print(df)

    print("\nBoth numpy and pandas are working correctly!")

except ImportError as e:
    print("Error: One or more libraries are not installed.")
    print(e)
