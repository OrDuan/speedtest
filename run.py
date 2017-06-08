from configparser import ConfigParser

import psycopg2

from aggregate import Aggregate
from scraper import SpeedTestScraper

settings = ConfigParser()
settings.read('settings.cfg')

db_settings = settings['DB']
conn = psycopg2.connect(
    dbname=db_settings['NAME'],
    user=db_settings['USER'],
    password=db_settings['PASSWORD'],
    host=db_settings['HOST'],
)


def create_tables():
    with conn.cursor() as cur:
        # print(cur.execute("""
        #     SELECT * from test;
        # """))
        cur.execute("""
            CREATE TABLE if NOT EXISTS public.test (
              id SERIAL PRIMARY KEY,
              created TIMESTAMP DEFAULT now(),
              scrape json DEFAULT '{}'
            );        
        """)
    conn.commit()

if __name__ == '__main__':
    print('Settings up database')
    create_tables()

    choice = input('Do you want to scrape data? y/N').lower() or 'N'
    if choice == 'y':
        print('Start scraping...')
        sts = SpeedTestScraper(conn)
        sts.scrape()

    choice = input('Do you want to aggregate data? y/N').lower() or 'N'
    if choice == 'y':
        print('aggregating...')
        agg = Aggregate(conn)
        path = agg.to_csv()
        print(f'Output file in {path}')

