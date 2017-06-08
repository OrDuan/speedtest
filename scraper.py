from concurrent.futures import ThreadPoolExecutor
from time import sleep

import requests


class SpeedTestScraper:
    def __init__(self, conn, max_workers=300):
        self.conn = conn
        self.max_workers = max_workers
        self.count = 0

    def scrape(self):
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for i in range(5342964424, 6342964424):
                executor.submit(self._scrape_page, i)
                while executor._work_queue.qsize() > 5000:
                    sleep(1)
                    print(self.count)

    def _scrape_page(self, test_id):
        response = requests.get(f'http://beta.speedtest.net/result/{test_id}')
        if response.status_code == 404:
            return

        scrape = self._parse_page(response.text)
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO test (scrape) VALUES (%s)
            """, (scrape,))
        self.conn.commit()
        self.count += 1

    def _parse_page(self, text):
        try:
            parsed = text.split('INIT_DATA=')[1].split(',window.OOKLA.globals')[0]
        except:
            print('No data found for this id.')
            return

        return parsed
