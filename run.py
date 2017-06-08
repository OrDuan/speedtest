from configparser import ConfigParser

import psycopg2

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
    create_tables()
    sts = SpeedTestScraper(conn)
    sts.scrape()
