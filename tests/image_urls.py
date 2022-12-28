import csv
import multiprocessing.dummy as mt
import urllib.request
import logging
import os
import sys

base_path = sys.argv[1] if len(sys.argv) > 1 else '.'

def check_url(url):
    if url is None or not url:
        return True

    try:
        response = urllib.request.urlopen(url)
    except Exception as e:
        logging.error('Failed to load %s : %s', url, str(e))
        return False

    if response.getcode() != 200:
        logging.error('Failed to load %s : code %d', url, response.getcode())
        return False

    return True


if __name__ == '__main__':
    print('Verifying images')
    with mt.Pool(20) as pool:
        source_data = [
            (os.path.join(base_path, 'data/collections.csv'), 'image_url'),
            (os.path.join(base_path, 'data/interests.csv'), 'icon_url'),
            (os.path.join(base_path, 'data/categories.csv'), 'icon_url'),
            (os.path.join(base_path, 'data/countries.csv'), 'flag_w32_waving_url'),
            (os.path.join(base_path, 'data/countries.csv'), 'flag_w40_url'),
        ]
        results = []

        for source_file,url_field in source_data:
            with open(source_file) as csvfile:
                reader = csv.DictReader(csvfile)
                urls = [row[url_field] for row in reader if url_field in row]
            results.append(pool.map_async(check_url, urls))

        check_results = []
        for res in results:
            check_results += res.get()

    assert all(check_results), "Some images failed to load"