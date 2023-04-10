from config import apiKey as key
import requests
import gzip
import shutil
import os
import concurrent.futures as futures
import time
import datetime
import json

def preprocessing():
    year, month, day = get_2_days_ago()
    file = f'movie_ids_{month}_{day}_{year}.json.gz'
    download_data_file(file)
    data = read_file(file[:-3])
    my_ids = get_non_adult(data)
    batches = convert_to_batches(my_ids)
    process_batches(batches, -1, 0)
    process_batches(batches, -1, 1)
    with open('preprocessing_complete.txt', 'w') as fout:
        fout.write(f"True")

def all_processes():
    if not os.path.exists('preprocessing_complete.txt'):
        preprocessing()


def processing_data():

    with open('movie_cast_data.tsv', 'r', encoding='utf-8') as fin:
        lines = fin.readlines()
    for line in lines:
        movie_id, cast_data = line.strip().split('\t')
        cast_data = json.loads(cast_data)
        movie_id = cast_data['id']
        cast_data = cast_data['cast']
        this_cast = []
        for c in cast_data:
            this_actor = {}
            this_actor['adult'] = c['adult']
            this_actor['id'] = c['id']
            this_cast['gender'] = c['gender']
            print(c)
            print(type(c))
            time.sleep(3000)
        time.sleep(3000)

def process_batches(batches, completed, phase):
    for count in range(len(batches)):
        if count>completed:
            print(f'Processing batch {count + 1} of {len(batches)}, phase {phase}')
            try:
                file = do_scrape(phase, batches[count])
                if phase==0:
                    with futures.ThreadPoolExecutor(max_workers=100) as executor:
                        results = executor.map(scrape_movie, batches[count])
                    file = 'movie_data.tsv'
                    with open(file, 'a', encoding='utf-8') as fout:
                        for result in results:
                            fout.write(f"{result}\n")
                else:
                    with futures.ThreadPoolExecutor(max_workers=100) as executor:
                        results = executor.map(scrape_cast, batches[count])
                    file = 'movie_cast_data.tsv'
                    with open(file, 'a', encoding='utf-8') as fout:
                        for result in results:
                            tmp = json.loads(result)
                            fout.write(f"{tmp['id']}\t{result}\n")

            except:
                process_batches(batches, count - 1)

def do_scrape(phase, batch):
    if phase == 0:
        method = scrape_movie
        file = 'movie_data.tsv'
    else:
        method = scrape_cast
        file = 'movie_cast_data.tsv'

def scrape_cast(movie_id):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}/credits?api_key={key}&language=en-US"
    return requests.get(url).text
def scrape_movie(movie_id):
    url = f'https://api.themoviedb.org/3/movie/{movie_id}?api_key={key}&language=en-US'
    return requests.get(url).text

def convert_to_batches(my_ids):
    print('Batching data')
    batches = []
    batch = []
    for id in my_ids:
        if len(batch)==100:
            batches.append(batch)
            batch = []
        batch.append(id)
    batches.append(batch)
    return batches


def get_non_adult(data):
    my_ids = []
    for d in data:
        d = json.loads(d)
        if d['adult'] is False:
            my_ids.append(d['id'])
    my_ids.sort()
    return my_ids

def read_file(file):
    print('Reading data')
    with open(file, 'r', encoding='utf-8') as fin:
        lines = fin.readlines()
    os.remove(file)
    return lines

def download_data_file(file):
    print('Downloading file')
    url = f"http://files.tmdb.org/p/exports/{file}"
    r = requests.get(url, stream=True)
    with open(file, 'wb') as f:
        f.write(r.content)
    with gzip.open(file, 'rb') as fin:
        with open(file[:-3], 'wb') as fout:
            shutil.copyfileobj(fin, fout)
    os.remove(file)

def get_2_days_ago():
    today = datetime.date.today()
    today -= datetime.timedelta(2)
    today = str(today)
    year, month, day = today.split('-')
    return year, month, day