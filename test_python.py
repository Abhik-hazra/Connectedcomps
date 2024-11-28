# Test pandas and numpy installations, including NumPy's C extension
import pandas as pd
import numpy as np

try:
    # Test numpy
    print("Testing numpy...")
    # Verify NumPy's C extension
    if hasattr(np.core, '_multiarray_umath'):
        print("NumPy's C extension (_multiarray_umath) is loaded correctly.")
    else:
        raise RuntimeError("NumPy's C extension is not loaded properly.")

    # Perform basic numpy operations
    array = np.array([1, 2, 3, 4, 5])
    print("Numpy array:", array)
    print("Array mean:", np.mean(array))

    # Test pandas
    print("\nTesting pandas...")
    data = {'Name': ['Alice', 'Bob', 'Charlie'], 'Age': [25, 30, 35]}
    df = pd.DataFrame(data)
    print("Pandas DataFrame:")
    print(df)

    print("\nAll tests passed. NumPy and pandas are installed and working correctly!")

except ImportError as e:
    print("Error: One or more libraries are not installed.")
    print(e)

except Exception as e:
    print("An error occurred during testing.")
    print(e)
