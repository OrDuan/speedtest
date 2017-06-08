import csv
import os
from contextlib import suppress
from pathlib import Path

import time


class Aggregate:
    """
    Take all the data from the database and aggregate it into a file.
    """
    def __init__(self, conn):
        self.conn = conn
        current_path = Path(os.getcwd())
        self.output_path = current_path / 'output'

    def to_csv(self):
        os.makedirs(self.output_path, exist_ok=True)
        filename = f'output-{int(time.time())}.csv'

        fieldnames = ['id', 'scrape_timestamp', 'scrape', 'date_tested',
                      'download_speed', 'upload_speed', 'latency', 'distance',
                      'server_name', 'server_id', 'sponsor_name', 'path']

        path = self.output_path / filename
        with open(path, 'w+') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for test in self._get_data():
                writer.writerow(vars(test))

        return path

    def _get_data(self):
        with self.conn.cursor() as cur:
            cur.execute('SELECT * FROM test ORDER BY id;')
            rows = cur.fetchall()
        return Test.from_list(rows)


class Test:
    """
    Represent one speedtest on speedtest.net website
    """
    def __init__(self, data):
        self.id = data[0]
        self.scrape_timestamp = data[1].isoformat()
        scrape = data[2]['result']
        self.date_tested = scrape['date']
        self.download_speed = scrape['download']
        self.upload_speed = scrape['upload']
        self.latency = scrape['latency']
        self.distance = scrape['distance']
        self.server_name = scrape['server_name']
        self.server_id = scrape['server_id']
        self.sponsor_name = scrape['sponsor_name']
        self.path = scrape['path']

    @classmethod
    def from_list(cls, list_of_datas):
        for data in list_of_datas:
            # Suppress badly scraped tests
            with suppress(TypeError, KeyError):
                yield cls(data)
