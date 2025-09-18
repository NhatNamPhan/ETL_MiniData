import pandas as pd

#Extract
def extract(file_path: str) -> pd.DataFrame:
    print("Extracting data...")
    df = pd.read_csv(file_path)
    return df

#Transform
def transform(df: pd.DataFrame) -> pd.DataFrame:
    print("Transforming data...")
    #Remove unnecessary columns and rename columns
    df = df.drop(['LongName','playerUrl','photoUrl','POT', 'Loan Date End'],axis=1)
    df = df.rename(columns={'↓OVA':'OVA'})
    #Remove downstream characters
    df['Club'] = df['Club'].str.replace('\n\n\n\n','')
    return df

def run_pipeline():
    file_path = "data/fifa21_raw_data_v2.csv"
    df = extract(file_path)
    df = transform(df)
    print(df.head())

if __name__ == "__main__":
    run_pipeline()