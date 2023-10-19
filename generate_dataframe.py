import math
import pandas as pd
import hashlib
import random
import numpy as np

TRANSFORMS = 10

# this value could be x2, x10, x1000, x10000 etc
ROWS = 20_000_000

random.seed(42)


class GenerateDataFrame:
    """A transform that adds a column of random data"""
    def __init__(self, row_count: int, column_count: int, max_chunk: int = 5_000_000):
        self.row_count = row_count
        self.column_count = column_count
        self.max_chunk = max_chunk
        self.chunk_count = math.ceil(self.row_count / self.max_chunk)
        self.hash = []

    def generate_data(self) -> dict:
        """Generate data for the dataframe"""
        if self.row_count >= self.max_chunk:
            self.row_count -= self.max_chunk
            data = list(self.generator(self.max_chunk))
            return {f"v{i}": data for i in range(self.column_count)}
        else:
            data = self.generator(self.max_chunk)
            return {f"v{i}": data for i in range(self.column_count)}

    def set_index(self, df: pd.DataFrame, start_index: int, end_index: int) -> pd.DataFrame:
        """Set the index of the dataframe"""
        index = pd.Index(range(start_index, end_index))
        return df.set_index(index, verify_integrity=True)

    def calculate_hash(self, df) -> np.ndarray:
        """Calculate the hash of the dataframe"""
        return pd.util.hash_pandas_object(df, index=True).values

    def get_hash(self) -> np.ndarray:
        return np.concatenate(self.hash)

    def generator(self, rows: int) -> list:
        """Generate random data"""
        for _ in range(rows):
            yield random.random()
        
    def run(self):
        """Run the pipeline"""
        for i in range(self.chunk_count):
            print(f"Chunk {i} of {self.chunk_count}")
            df_temp = pd.DataFrame(self.generate_data())
            
            start_index = i * self.max_chunk
            end_index = len(df_temp) + start_index
            df_temp = self.set_index(df_temp, start_index, end_index)
            self.hash.append(self.calculate_hash(df_temp))

if __name__ == "__main__":
    dataframe = GenerateDataFrame(ROWS, TRANSFORMS)
    df = dataframe.run()

    # Dont break the test
    df_hash = hashlib.sha256(dataframe.get_hash()).hexdigest()

    print("df_hash", df_hash)

    test_hash = "867567dc7d46f77af2bca9804ac366a5165d27612de100461b699bd23094ab90"

    assert df_hash == test_hash

    print("df hash was okay!")


# Generate DataFrame with constant ram usage

## Steps
"""
    I create dataframes with chunks. 
"""

# 1. Generate data
# 2. Set index
# 3. Calculate hash
# 4. Store hash
# 5. Repeat
