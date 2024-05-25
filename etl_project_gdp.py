# Importing libraries
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sqlite3

def main():
    # Extract
    log_progress('Preliminaries complete. Initiating extract process')
    url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
    df = extract(url)
    # Transform
    log_progress('Data extraction complete. Initiating transform process')
    df = transform(df)
    # Load to CSV
    log_progress('Data transformation complete. Initiating loading process')
    load_to_csv(df)
    log_progress('Data saved to CSV file')
    # Load to database
    load_to_db(df)
    log_progress('Data loaded to Database as table. Running the query')
    # Run query
    query = '''
    SELECT *
    FROM gdp
    WHERE GDP_USD_billions >= 100
    ORDER BY GDP_USD_billions DESC;
    '''    
    run_query(df,query)
    log_progress('Process Complete.')

def log_progress(message):
    with open('log.txt', 'a') as f:
        f.write(f'{datetime.now()}: '+message + '\n')

def extract(url):
    res = requests.get(url)
    df = pd.DataFrame(columns=['Country', 'GDP_USD_millions'])
    data = BeautifulSoup(res.text, 'html.parser')
    tables = data.find_all('table')
    table = tables[2]
    rows = table.tbody.find_all('tr',recursive=False)
    for row in rows[3:]:
        try:
            cells = row.find_all('td',recursive=False)
            cell_dict = {'Country': [cells[0].get_text(strip=True)],
                        'GDP_USD_millions': [cells[2].get_text(strip=True)]}
            df1 = pd.DataFrame(cell_dict)
            df = pd.concat([df, df1], ignore_index=True)
        except:
            pass
    return df

def transform(df):
    df['GDP_USD_millions'] = df['GDP_USD_millions'].apply(lambda x: np.NaN if x=='—' else float(x.replace(',','')))
    df['GDP_USD_billions'] = df['GDP_USD_millions']/1000
    df = df.drop(columns=['GDP_USD_millions'])
    return df

def load_to_csv(df):
    df.to_csv('gdp.csv', index=False)

def load_to_db(df):
    with sqlite3.connect('gdp.db') as con:
        df.to_sql('gdp', con, if_exists='replace',index=False)

def run_query(df, query):
    with sqlite3.connect('gdp.db') as con:
        df = pd.read_sql(query, con)
    print(df)

if __name__=='__main__':
    main()