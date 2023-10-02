#Project
"""For this project, you will assume the role of data engineer working for an international financial analysis company. 
Your company tracks stock prices, commodities, forex rates, inflation rates.  
Your job is to extract financial data from various sources like websites, APIs and files provided by various financial analysis firms. 
After you collect the data, you extract the data of interest to your company and transform it based on the requirements given to you. 
Once the transformation is complete you load that data into a database.

In this project you will:
1. Collect data using webscraping.
2. Collect data using APIs
3. Download files to process.    
4. Read csv, xml and json file types.
5. Extract data from the above file types.
6. Transform data.
7. Use the built in logging module.
8. Save the transformed data in a ready-to-load format which data engineers can use to load the data."""

#1. Collect Data Using Webscraping
"""The wikipedia webpage https://web.archive.org/web/20200318083015/https://en.wikipedia.org/wiki/List_of_largest_banks provides information about largest banks in the world by various parameters. 
Scrape the data from the table 'By market capitalization' and store it in a JSON file.
"""
from bs4 import BeautifulSoup
import html5lib
import requests
import pandas as pd

#Gather the contents of the webpage in text format using the `requests` library and assign it to the variable html_data
url = "https://web.archive.org/web/20200318083015/https://en.wikipedia.org/wiki/List_of_largest_banks"
html_data  = requests.get(url).text 
#print(html_data[760:783])

soup = BeautifulSoup(html_data,"html5lib")  #create a soup object using the variable 'html_data'
table_rows = soup.find_all('tr') #find all tags with this tag and their children

table_list = []
for banks in table_rows[128:198]:
    child = banks.find_all('td')
    table_list.append({'Name':child[1].get_text().strip(),'Market Cap (US$ Billion)':child[2].get_text().strip()})

df = pd.DataFrame(table_list)
#print(df.head())

df.to_json('bank_market_cap.json')

#2. Collect data using APIs
"""Collect exchange rate data using an API
store the data as a csv"""

url = "http://api.exchangeratesapi.io/v1/latest?base=EUR&access_key=b9ff09888ebef2afc07e6a753affdff9" 
rate_data = requests.get(url)

rate_data_json = rate_data.json() #creates a dictionary class

data_list = []
dict = rate_data_json['rates'] #to get the nested dictionary 

for key,value in dict.items():
    data_list.append({'Currency':key,'rate':value}) #creating a list where we can use to set up dataframe

df2 = pd.DataFrame(data_list)
df2 = df2.set_index('Currency') #set the column 'currency' as the index
#print(df2)

df2.to_csv('output.csv')

#ETL process
"""In this final part you will:
- Run the ETL process
- Extract bank and market cap data from the JSON file `bank_market_cap.json`
- Transform the market cap currency using the exchange rate data
- Load the transformed data into a seperate CSV
"""
import glob
from datetime import datetime

logfile = 'logfile.txt'            # all event logs will be stored in this file
targetfile = 'bank_market_cap_gbp.csv'   # file where transformed data is stored

#json extract function
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)
    return dataframe

#Define the extract function that finds JSON file `bank_market_cap_1.json` and calls the function created above to extract data from them
#Store the data in a `pandas` dataframe. Use the following list for the columns.

def extract():
    extracted_data = pd.DataFrame(columns=['Name','Market Cap (US$ Billion)']) #create an empty data frame to hold extracted data

    #process all json files
    for jsonfile in glob.glob("*bank_market_cap_1.json"):
        extracted_data = extracted_data._append(extract_from_json(jsonfile), ignore_index=True)
    
    return extracted_data

#Load the file exchange_rates.csv as a dataframe and 
#find the exchange rate for British pounds with the symbol GBP, 
#store it in the variable exchange_rate, you will be asked for the number. 
#Hint: set the parameter index_col to 0.

df3 = pd.read_csv('exchange_rates.csv', index_col=0) #create a dataframe from the csv file and make the index column as the first column of data

exchange_rate = df3.loc['GBP']['Rates']
print(exchange_rate)

#Transform Process
"""Using exchange_rate and the `exchange_rates.csv` file find the exchange rate of USD to GBP. Write a transform function that
1. Changes the `Market Cap (US$ Billion)` column from USD to GBP
2. Rounds the Market Cap (US$ Billion)` column to 3 decimal places
3. Rename `Market Cap (US$ Billion)` to `Market Cap (GBP$ Billion)`
"""

def transform(data):
        data = data.astype({'Market Cap (US$ Billion)':float,'Name':str}) #Convert the datatype of the column into float
        data['Market Cap (US$ Billion)'] = round(data['Market Cap (US$ Billion)'] * exchange_rate,3) #Rounds the Market Cap (US$ Billion)` column to 3 decimal places
        
        data.rename(columns = {'Market Cap (US$ Billion)':'Market Cap (GBP$ Billion)'}, inplace = True) #inplace parameter is there to change the name successfully and not create a new column

        return data

#Load Process
"""Create a function that takes a dataframe and load it to a csv named `bank_market_cap_gbp.csv`. Make sure to set `index` to `False`.
"""
def load(targetfile, data_to_load):
     data_to_load.to_csv(targetfile,index=False) #load all the transformed data in a csv format, and no index will be created   

#Logging Process
def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)

    with open("logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')
    
log("ETL Job Started")

log("Extract phase Started")
extracted_data = extract()
log("Extract phase Ended")

log("Transform phase Started")
transformed_data = transform(extracted_data)
log("Transform phase Ended")

log("Load phase Started")
load(targetfile,transformed_data)
log("Load phase Ended")
    
log("ETL Job Ended")