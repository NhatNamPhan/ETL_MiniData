import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import logging

#--------Logging Config--------
logging.basicConfig(
    level=logging.INFO, #Level log: DEBUG, INFO, WARNING, ERROR, CRITICAL
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("etl_pipeline.log"), #Logs to file
        logging.StreamHandler()                  #Print log to console
    ]
)

#--------Extract--------
def extract(file_path: str) -> pd.DataFrame:
    try:
        logging.info('Extracting data...')
        df = pd.read_csv(file_path)
        logging.info(f'Extracted {df.shape[0]} row and {df.shape[1]} columns')
        return df
    except FileNotFoundError:
        logging.error(f'File not found: {file_path}')
        raise
    except Exception as e:
        logging.error(f'Error while extracting: {e}')
        raise

#--------Transform--------
def transform(df: pd.DataFrame) -> pd.DataFrame:
    try:
        logging.info('Transforming data...')
        
        # Remove unnecessary columns and rename columns
        df = df.drop(['LongName', 'playerUrl', 'photoUrl', 'POT'], axis=1)
        df = df.rename(columns={'↓OVA':'OVA'})
        df['Club'] = df['Club'].str.replace('\n\n\n\n','')
        
        # Convert height and weight
        def ft_to_cm(x):
            if isinstance(x, str) and "'" in x:
                ft = x.replace("'", ".").replace('"', '')
                return int(round(float(ft) * 30.48, 0))
            return pd.to_numeric(x, errors='coerce')

        def lbs_to_kg(x):
            if isinstance(x, str) and "lbs" in x:
                lbs = x.replace("lbs", "")
                return int(round(float(lbs) * 0.4356, 0))
            return pd.to_numeric(x, errors='coerce')
        
        df['Height'] = df['Height'].str.strip('cm')
        df['Weight'] = df['Weight'].str.strip('kg')
        df['Height'] = df['Height'].apply(ft_to_cm)
        df['Weight'] = df['Weight'].apply(lbs_to_kg)
        df.rename(columns={'Height': 'Height in cm', 'Weight': 'Weight in kg'}, inplace= True)
        
        # Convert money string 
        def money(x):
            if pd.isna(x):
                return np.nan
            try:
                if '€' in x: x = x.replace('€','')
                if 'M' in x: return int(float(x.replace('M','')) * 1_000_000)
                if 'K' in x: return int(float(x.replace('K','')) * 1_000)
                return int(x)
            except:
                logging.warning(f'Cannot parse money value {x}')
                raise
            
        df['Value'] = df['Value'].apply(money)/1_000_000
        df['Wage'] = df['Wage'].apply(money)
        df['Release Clause'] = df['Release Clause'].apply(money)/1_000_000
        df.rename(columns={
            'Value': 'Value in Euro Million',
            'Wage': 'Wage in Euro',
            'Release Clause': 'Release Clause in Euro Million'
        }, inplace= True)
        
        # Remove '★'
        for col in ['W/F', 'SM', 'IR']:
            df[col] = df[col].str.replace('★','')
        df.rename(columns={'W/F': 'W/F Rating', 'SM': 'SM Rating', 'IR': 'IR Rating'}, inplace= True)
        
        # Contract info 
        def type_contract(x):
            if 'Free' in x: return 'Free'
            if 'Loan' in x: return 'Loan'
            if '~' in x: return 'Contract'
            return pd.NA
        
        def start_time_contract(x):
            try:
                if '~' in x: return int(x[:4])
                if 'Loan' in x:
                    return datetime.strptime(x.strip(' On Loan'), "%b %d, %Y").date()
            except Exception:
                return pd.NA
            return pd.NA
            
        def end_time_contract(type, contract, loan):
            try:
                if type == 'Contract': return int(contract[-4:])
                if type == 'Loan': return datetime.strptime(loan, "%b %d, %Y").date()
            except Exception:
                return pd.NA
            return pd.NA
            
        df['Type of Contract'] = df['Contract'].apply(type_contract)
        df['Start year'] = df['Contract'].apply(start_time_contract)
        df['End year'] = df.apply(lambda row: end_time_contract(row['Type of Contract'], row['Contract'], row['Loan Date End']),axis=1)
        
        #Reorder columns
        cols = list(df.columns)
        cols.remove('Loan Date End')
        
        block1 = ['Type of Contract', 'Start year', 'End year']
        block2 = ['Best Position']
        
        for c in block1 + block2:
            cols.remove(c)
            
        pos_contract = cols.index('Contract') + 1
        for i, c in enumerate(block1):
            cols.insert(pos_contract + i, c)
            
        pos_position = cols.index('Positions') + 1
        cols.insert(pos_position, 'Best Position')
        
        df = df[cols]
        
        # Ratings normalize 
        df['Attacking'] = (df['Attacking'] / 5).round().astype('int64')
        df['Skill'] = (df['Skill'] / 5).round().astype('int64')
        df['Movement'] = (df['Movement'] / 5).round().astype('int64')
        df['Power'] = (df['Power'] / 5).round().astype('int64')
        df['Mentality'] = (df['Mentality'] / 6).round().astype('int64')
        df['Defending'] = (df['Defending'] / 3).round().astype('int64')
        df['Goalkeeping'] = (df['Goalkeeping'] / 5).round().astype('int64')

        df.rename(columns={
            'Attacking':'Attacking AVG', 'Skill':'Skill AVG', 'Movement':'Movement AVG', 'Power':'Power AVG',
            'Mentality':'Mentality AVG', 'Defending':'Defending AVG', 'Goalkeeping':'Goalkeeping AVG'
        }, inplace= True)
        # Hits in K
        def covert_hits(x):
            if pd.isna(x):
                return np.nan
            elif 'K' in str(x):
                return float(x[:-1])
            else:
                return float(x)

        df['Hits in K'] = df['Hits'].apply(covert_hits)
        df.drop(columns=['Hits'], inplace= True)
        df.fillna({'Hits in K': df['Hits in K'].mean()}, inplace= True)
        
        logging.info('Transform finished successfully.')
        return df
    
    except Exception as e:
        logging.error(f'Error while transforming: {e}')
        raise

#--------Load to CSV--------
def load_to_csv(df: pd.DataFrame, output_path: str = 'data/fifa21_clean.csv'):
    try:
        logging.info('Loading data to CSV...')
        df.to_csv(output_path, index= False, encoding='utf-8-sig')
        logging.info(f'Data saved to {output_path}')
    except Exception as e:
        logging.error(f'Error while saving CSV: {e}')
        raise
    
#--------Load to PostgreSQL--------
load_dotenv()

def load_to_postgres(df, table_name= 'fifa21_clean'):
    try:
        logging.info('Loading data to PostgreSQL...')
        DB_NAME = os.getenv("DB_NAME")
        DB_USER = os.getenv("DB_USER")
        DB_PASSWORD = os.getenv("DB_PASSWORD")
        DB_HOST = os.getenv("DB_HOST")
        DB_PORT = os.getenv("DB_PORT")
    
        engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        df.to_sql(table_name, engine, if_exists='replace', index= False)
        logging.info(f'Data loaded to PostgreSQL table {table_name}')
    except Exception as e:
        logging.error(f'Error while saving to PostgreSQL: {e}')
        raise
    
#--------Run pipeline--------    
def run_pipeline():
    try:
        file_path = 'data/fifa21_raw_data_v2.csv'
        df = extract(file_path)
        df = transform(df)
        load_to_csv(df)
        load_to_postgres(df, table_name='fifa21_clean')
        logging.info('ETL pipeline finished successfully.')
    except Exception as e:
        logging.critical(f'Pipeline failed: {e}')

if __name__ == '__main__':
    run_pipeline()