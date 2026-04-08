import pandas as pd
from pathlib import Path

files = list(Path("data/raw/weather").glob("*.parquet"))
df = pd.read_parquet(files[-1])

print(df.head())
print(df.shape)