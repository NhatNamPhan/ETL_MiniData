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
    #Convert the height and weight column to numerical forms
    def ft_to_cm(x):
        if "'" in x:
            ft = x.replace("'", ".").replace('"','')
            cm = round(float(ft)*30.48,0)
            return int(cm)
        else:
            return x
    def lbs_to_kg(x):
        if "lbs" in x:
            lbs = x.replace("lbs","")
            kg = round(float(lbs)*0.4356,0)
            return int(kg)
        else:
            return x
    df['Height'] = df['Height'].str.strip('cm')
    df['Weight'] = df['Weight'].str.strip('kg')
    df['Height'] = df['Height'].apply(ft_to_cm)
    df['Weight'] = df['Weight'].apply(lbs_to_kg)
    df.rename(columns={
        'Height': 'Height in cm',
        'Weight': 'Weight in kg'
    }, inplace= True)
    #Convert money string to numerical 
    def money(x):
        if '€' in x:
            x = x.replace('€','')
        if 'M' in x:
            x = x.replace('M','')
            return int(float(x)*1000000)
        if 'K' in x:
            x = x.replace('K','')
            return int(float(x))
    df['Value'] = df['Value'].apply(money)/1000000
    df['Wage'] = df['Wage'].apply(money)
    df['Release Clause'] = df['Release Clause'].apply(money)/1000000
    df.rename(columns={
        'Value': 'Value in Euro Million',
        'Wage': 'Wage in Euro',
        'Release Clause': 'Release Clause in Euro Million'
    }, inplace= True)
    #Remove character '★' in columns W/f, SM, IR
    df['W/F'] = df['W/F'].str.replace('★','')
    df['SM'] = df['SM'].str.replace('★','')
    df['IR'] = df['IR'].str.replace('★','')
    df.rename(columns={
        'W/F': 'W/F Rating',
        'SM': 'SM Rating',
        'IR': 'IR Rating'
    },inplace= True)
    return df

def run_pipeline():
    file_path = "data/fifa21_raw_data_v2.csv"
    df = extract(file_path)
    df = transform(df)
    print(df.head())

if __name__ == "__main__":
    run_pipeline()