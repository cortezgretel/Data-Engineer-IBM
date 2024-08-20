#import libraries 

import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

#initialize known entities

url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name = 'Movies.db'
table_name = 'Top_50'
csv_path = '/home/project/top_50_films.csv'
df = pd.DataFrame(columns = ["Film", "Year", "Rotten Tomatoes' Top 100"])
count = 0

#Loading the webpage
#First, load the entire web page as an HTML document using requests.get().text
#Then parser the text HTML format using BeautifulSoup

html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser')

# tables: get the body of all the tables in the web page

tables = data.find_all('tbody')

# rows: gets all the rows of the first table

rows = tables[0].find_all('tr')

# iterate over the rows to find the required data

for row in rows:
    if count<25:
        col = row.find_all('td')
        if len(col)!=0 :
            year = col[2].contents[0]
            if year.isdigit() and (2000 <= int(year)<= 2010):
                data_dict = {"Film": col[1].contents[0],
                            "Year": col[2].contents[0],
                            "Rotten Tomatoes' Top 100": col[3].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
                count+=1
    else:
        break

print(df)


#Storing the data in a file CSV

df.to_csv(csv_path)

#Storing tha data in a database

conn = sqlite3.connect(db_name)
df.to_sql(table_name, conn, if_exists='replace', index=False)
conn.close()
