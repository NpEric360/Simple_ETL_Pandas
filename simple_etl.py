import requests
import pandas as pd
from sqlalchemy import create_engine #SQLLITE DATABASE


def extract():
    API_URL = "http://universities.hipolabs.com/search?country=United+States"
    data = requests.get(API_URL).json()
    return data

#sample transformation function
def transform(data:dict) -> pd.DataFrame:
    #Transforms the dataset into desired structure and filters
    df = pd.DataFrame(data)
    total_number_entries = len(data)
    print(f"Total Number of universities from API {total_number_entries}")
    
    #filter all dataframe entries containing 'California' in the university name
    df = df[df["name"].str.contains("California")]
    print(f"Number of universities in california {len(df)}")
    
    
    df['domains'] = [','.join(map(str, l)) for l in df['domains']]
    df['web_pages'] = [','.join(map(str, l)) for l in df['web_pages']]
    
    df = df.reset_index(drop=True)

    return df[["domains","country","web_pages","name"]]

def load(df:pd.DataFrame)-> None:
    """ Loads data into a sqllite database"""
    disk_engine = create_engine('sqlite:///my_lite_store.db')
    df.to_sql('cal_uni', disk_engine, if_exists='replace')

#Step 1: Extract
data = extract()

#Step 2: Transform
df = pd.DataFrame(data)
result = transform(data)

#Step 3: Load into destination database
load(result)
print(result.sample(n=1))