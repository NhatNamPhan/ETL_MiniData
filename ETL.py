import pandas as pd

#Extract
def extract(file_path: str) -> pd.DataFrame:
    print("Extract data...")
    df = pd.read_csv(file_path)
    return df

#Transform

