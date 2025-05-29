import pandas as pd

# Replace with your actual file path or read from MinIO if needed
df = pd.read_parquet("Visor/restaurants_kpi.parquet")
print(df.head())        # First few rows
print(df.columns)       # Column names
print(df.describe())    # Basic stats (numeric columns)
print(df.info())        # Types and nulls
