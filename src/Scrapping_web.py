# Import Library
import requests
from requests import get
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import os
import csv
import mysql.connector

URL_FILE = "File/"

loop = 1

class _IMDB100Scrapper:
    def __init__(self, url):
        self._imdb_url = url
        self._headers = {"Accept-Language": "en-US, en;q=0.5"}
        self._response = None
        self._movie_soup = None
        self._movie_div = None
        self._movies = None
        self._movie_name = []
        self._movie_years = []
        self._movie_runtime = []
        self._ratings = []
        self._metascores = []
        self._number_votes = []
        self._us_gross = []
    
    def _increment_loop(self):
        global loop
        loop += 1
    
    def _fetch_page(self):
        self._response = requests.get(self._imdb_url, headers=self._headers)
        if self._response.status_code == 200:
            self._movie_soup = BeautifulSoup(self._response.text, "html.parser")
    
    def _extract_movie_details(self):
        self._movie_div = self._movie_soup.find_all('div', class_='lister-item mode-advanced')
        
        for container in self._movie_div:
            # Name
            name = container.h3.a.text
            self._movie_name.append(name)
            # Year
            year = container.h3.find('span', class_='lister-item-year').text
            self._movie_years.append(year)
            # Runtime
            runtime = container.p.find('span', class_='runtime').text if container.p.find('span', class_='runtime').text else '-'
            self._movie_runtime.append(runtime)
            # Rating
            imdb = float(container.strong.text)
            self._ratings.append(imdb)
            # MetaScore
            m_score = container.find('span', class_='metascore').text if container.find('span', class_='metascore') else '-'
            self._metascores.append(m_score)
            # NumberVotes
            nv = container.find_all('span', attrs={'name': 'nv'})
            # filter nv for votes
            vote = nv[0].text
            self._number_votes.append(vote)
            # filter nv for gross
            grosses = nv[1].text if len(nv) > 1 else '-'
            self._us_gross.append(grosses)
    
    def _generate_dataframe(self):
        self._movies = pd.DataFrame({
            'movie_name': self._movie_name,
            'movie_year': self._movie_years,
            'movie_runtime': self._movie_runtime,
            'imdb_ratings': self._ratings,
            'metascore': self._metascores,
            'number_votes': self._number_votes,
            'us_gross_millions': self._us_gross,
            })
        return self._movies
    
    def get_top_movies_csv(self):
        self._fetch_page()
        self._extract_movie_details()
        generate_pdf = self._generate_dataframe()
        
        int_type_extract = ['movie_year', 'movie_runtime']
        int_type_replace = ['number_votes']
        int_type_gross   = ['us_gross_millions']
        for titles in generate_pdf:
            if titles in int_type_extract:
                self._movies[f'{titles}'] = self._movies[f'{titles}'].str.extract('(\d+)').astype(int)
            elif titles in int_type_replace:
                self._movies[f'{titles}'] = self._movies[f'{titles}'].str.replace(',', '').astype(int)
            elif titles in int_type_gross:
                self._movies[f'{titles}'] = self._movies[f'{titles}'].map(lambda x: x.lstrip('$').rstrip('M'))
                self._movies[f'{titles}'] = pd.to_numeric(self._movies[f'{titles}'], errors='coerce')
            
        # generate top 100 movies
        try:
            global loop
            if os.path.exists(URL_FILE + "top_100_movies.csv"):
                self._movies.to_csv(f'{URL_FILE}top_100_movies{loop}.csv', index=False)
                self._increment_loop()
                return print("CSV file saved successfully.")
            else:
                self._movies.to_csv(f'{URL_FILE}top_100_movies{loop}.csv', index=False)
                self._increment_loop()
                return print("CSV file saved successfully.")
        except Exception as e:
            return print("An error occurred while saving the CSV file:", e)


class _Database:
    def __init__(self):
        self._connection = None
        self._cursor = None
    
    def _connect_database_1(self):
        self._connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password=''
        )
    
    def _connect_database_2(self):
        self._connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='',
            database='db_test_api_movie'
        )
    
    def _create_database(self):
        self._connect_database_1()
        self._cursor = self._connection.cursor()
        try:
            query = f"CREATE DATABASE db_test_api_movie"
            self._cursor.execute(query)
            print("Create Database executed successfully.")
        except mysql.connector.Error as err:
            print(f"Error: {err}")
        self._cursor.close()
        self._connection.close()
    
    def _create_table(self):
        self._connect_database_2()
        self._cursor = self._connection.cursor()
        try:
            query = """
            CREATE TABLE movie(
                movie_id INT AUTO_INCREMENT PRIMARY KEY,
                movie_name VARCHAR(255),
                movie_year INT(4),
                movie_runtime INT(4),
                imdb_ratings FLOAT,
                metascore INT(3),
                number_votes INT,
                us_gross_millions FLOAT
            )
            """
            self._cursor.execute(query)
            self._connection.commit()
            print("Create Table executed successfully.")
            self._cursor.close()
            self._connection.close()
            return True
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            self._cursor.close()
            self._connection.close()
            return False
        
    
    def _insert_data_table(self):
        self._connect_database_2()
        self._cursor = self._connection.cursor()
        data_to_insert = []
        with open(URL_FILE+'/'+'top_100_movies_final.csv', 'r') as file:
            csv_reader = csv.reader(file)
            header = next(csv_reader) 
            for row in csv_reader:
                data_to_insert.append(row)
                
        try:    
            insert_query = f"INSERT INTO movie ( {', '.join(header)}) VALUES ({', '.join(['%s'] * len(header))})"
            self._cursor.executemany(insert_query, data_to_insert)
            self._cursor.close()
            self._connection.commit()
            print("Insert Data executed successfully.")
            
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            
    
    def run_database(self):
        self._create_database()
        check = self._create_table()
        
        if(check==True):
            self._insert_data_table()

def concat_df():
    
    dfs_to_concat = []
    if os.path.exists(URL_FILE) and os.path.isdir(URL_FILE):
        files = os.listdir(URL_FILE)
        for file in files:
            if os.path.isfile(os.path.join(URL_FILE, file)):
                with open(URL_FILE+'/'+file, 'r') as file:
                    df = pd.read_csv(file)
                    dfs_to_concat.append(df)
    else:
        print(f"The folder '{URL_FILE}' does not exist or is not a valid directory.")
    
    result = pd.concat(dfs_to_concat, ignore_index=True)
    result.to_csv(f'{URL_FILE}top_100_movies_final.csv', index=False)
    return True

def main():
    urls = [
        "https://www.imdb.com/search/title/?groups=top_100&ref_=adv_prv",
        "https://www.imdb.com/search/title/?groups=top_100&start=51&ref_=adv_nxt"
    ]
    
    for url in urls:    
        scrapper = _IMDB100Scrapper(url)
        scrapper.get_top_movies_csv()
    
    try:
        concat_df()
        print("\nFile Final saved successfully")
    except:
        print("\nFile Final saved Not successfully")
    
    # DATABASE
    database = _Database()
    database.run_database()


if __name__ == "__main__":
    main()