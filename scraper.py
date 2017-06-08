from concurrent.futures import ThreadPoolExecutor
from time import sleep

import requests


class SpeedTestScraper:
    """
    The scraper to get the data from the website 
    """
    def __init__(self, conn, max_workers=300):
        self.conn = conn
        self.max_workers = max_workers

        # Id range to scan
        self.start_id = 5342964424
        self.stop_id = 6342964424

        # Counts how many tests we scraped
        self._count = 0

    def scrape(self):
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for i in range(self.start_id, self.stop_id):
                executor.submit(self._scrape_page, i)
                # Prevent from the the queue to be too big
                while executor._work_queue.qsize() > 5000:
                    sleep(1)
                    print(self._count)

    def _scrape_page(self, test_id):
        response = requests.get(f'http://beta.speedtest.net/result/{test_id}')
        if response.status_code == 404:
            return

        scrape = self._parse_page(test_id, response.text)

        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO test (scrape) VALUES (%s)
            """, (scrape,))
        self.conn.commit()
        self._count += 1

    def _parse_page(self, test_id, text):
        try:
            parsed = text.split('INIT_DATA=')[1].split(',window.OOKLA.globals')[0]
        except:
            print(f'No data found for f{test_id}.')
            return

        return parsed
