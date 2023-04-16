import time

from config import apiKey as key
import requests
import concurrent.futures as futures
import json

# Called to process all data reading for actors and their movies,
# as well as, movies and their casts for 6 degrees from Kevin Bacon
def read_data_movies(actors, movies, phase):
    print(f'Starting phase {phase}')
    if phase < 7:
        print(f'Iterating actors in phase {phase}')
        these_actors = build_actor_list(actors, phase)
        print(f'{len(these_actors)} actors to search')
        results = do_concurrent(scrape_movies, these_actors)
        these_movies, movies = build_movies(results, movies, phase)
        print(f'Iterating movies in phase {phase}')
        print(f'{len(these_movies)} movies to search')
        results = do_concurrent(scrape_credits, these_movies)
        actors = build_cast(results, actors, phase)
        read_data_movies(actors, movies, phase + 1)
    else:
        write_file('actors.tsv', actors)
        write_file('movies.tsv', movies)

# Called by read_data_movies() to do concurrency
def do_concurrent(method, my_list):
    with futures.ThreadPoolExecutor(max_workers=100) as executor:
        results = executor.map(method, my_list)
    return results

# Writes dictionary to a file
def write_file(file, this_dict):
    with open(file, 'w', encoding='utf-8') as fout:
        for k, v in this_dict.items():
            fout.write(f"{k}\t{v}\n")

# Called by do_concurrent to scrape credits for a movie
# returns data in text format that was scraped from themoviedb api
def scrape_credits(movie):
    url = f"https://api.themoviedb.org/3/movie/{movie}/credits?api_key={key}&language=en-US"
    return requests.get(url).text

# Called by do_concurrent to scrape the movies an actor appeared in
# returns data in a text format that was scraped from themoviedb api
def scrape_movies(actor):
    url = f'https://api.themoviedb.org/3/person/{actor}/movie_credits?api_key={key}&language=en-US'
    return requests.get(url).text

# Iterates over a dictionary to create a list of actor IDs
# for just the current phase, returns list of actor IDs
def build_actor_list(actors, phase):
    these_actors = []
    for k, v in actors.items():
        if v == phase - 1:
            these_actors.append(k)
    return these_actors

# Iterates over scraped results to find movies added in this phase
# Adds movies from this phase to dict
# Returns list of movies from this phase and updated movies dict
def build_movies(results, movies, phase):
    these_movies = []
    for result in results:
        try:
            cast = json.loads(result)['cast']
            for c in cast:
                if c['id'] not in movies.keys():
                    movies[c['id']] = phase
                    these_movies.append(c['id'])
        except KeyError:
            pass
    return these_movies, movies

# Iterates over scraped results, adding new actors to dict and applying phase
# Also, appends new movies and their casts to movie_cast.tsv
# Returns updated actors dict
def build_cast(results, actors, phase):
    for result in results:
        try:
            current_movie_id = json.loads(result)['id']
            this_cast = ''
            cast = json.loads(result)['cast']
            for c in cast:
                if len(this_cast) > 0:
                    this_cast += ','
                this_cast += str(c['id'])
                if c['id'] not in actors.keys():
                    actors[c['id']] = phase
            with open('movie_cast.tsv', 'a', encoding='utf-8') as fout:
                fout.write(f'{current_movie_id}\t{this_cast}\n')
        except KeyError:
            pass
    return actors

# Reads file into dictionary and returns list of IDs
def read_file_into_list(file, keep):
    my_list = []
    with open(file, 'r', encoding='utf-8') as fin:
        lines = fin.readlines()
    for line in lines:
        if not keep:
            this_id, data = line.strip().split('\t')
            my_list.append(this_id)
        else:
            my_list.append(line.strip())
    my_list.sort()
    return my_list

def batch_list(my_list):
    batches = []
    batch = []
    for item in my_list:
        if len(batch) == 1000:
            batches.append(batch)
            batch = []
        batch.append(item)
    batches.append(batch)
    return batches

# Scrapes actor and movie data and writes to actor_data.tsv and movie_data.tsv
def read_in_actor_and_movie_data():
    actor_list = read_file_into_list('actors.tsv', False)
    movie_list = read_file_into_list('movies.tsv', False)
    batches = batch_list(actor_list)
    del actor_list
    iterate_batches(batches, 'actors', -1)
    batches = batch_list(movie_list)
    del movie_list
    iterate_batches(batches, 'movies', -1)

def scrape_actor_data(actor_id):
    # print(actor_id)
    url = f"https://api.themoviedb.org/3/person/{actor_id}?api_key={key}&language=en-US"
    try:
        return requests.get(url).text
    except:
        pass

def scrape_movie_data(movie):
    url = f"https://api.themoviedb.org/3/movie/{movie}?api_key={key}&language=en-US"
    return requests.get(url).text

def write_raw_data_to_file(file, results):
    with open(file, 'a', encoding='utf-8') as fout:
        for result in results:
            fout.write(f"{result}\n")

def iterate_batches(batches, this_type, start):
    lng = len(batches)
    for x in range(len(batches)):
        if x > start:
            if this_type == 'actors':
                results = do_concurrent(scrape_actor_data, batches[x])
                write_raw_data_to_file('actor_data.tsv', results)
            elif this_type == 'movies':
                results = do_concurrent(scrape_movie_data, batches[x])
                write_raw_data_to_file('movie_data.tsv', results)
            elif this_type == 'series':
                results = do_concurrent(scrape_series, batches[x])
                write_raw_data_to_file('series_data.tsv', results)
            elif this_type == 'actor-series':
                results = do_concurrent(scrape_actor_series, batches[x])
                write_actor_series_to_file(results)
            elif this_type == 'episode_data':
                results = do_concurrent(scrape_episode_data, batches[x])
                write_episode_data_to_file(results, 'episode_data.tsv')
            elif this_type == 'episode_cast':
                results = do_concurrent(scrape_episode_cast, batches[x])
                write_episode_data_to_file(results, 'episode_cast.tsv')
        print(f"Batch {x} of {lng} complete for {this_type}")

def scrape_episode_cast(data):
    url = f'https://api.themoviedb.org/3/tv/{data[0]}/season/{data[1]}/episode/{data[2]}/credits?api_key={key}&language=en-US'
    result = requests.get(url).text
    tmp = (data, result)
    return tmp

def write_episode_data_to_file(results, file):
    with open(file, 'a', encoding='utf-8') as fout:
        for result in results:
            sse = result[0]
            result = json.loads(result[1])
            if 'success' not in result:
                fout.write(f'{sse}\t{result}\n')

def scrape_episode_data(data):
    series = data[0]
    season = data[1]
    episode = data[2]
    url = f'https://api.themoviedb.org/3/tv/{series}/season/{season}/episode/{episode}?api_key={key}&language=en-US'
    result = requests.get(url).text
    tmp = (data, result)
    return tmp

def read_in_data_file(file):
    with open(file, 'r', encoding='utf-8') as fin:
        lines = fin.readlines()
    data = {}
    for line in lines:
        try:
            line = json.loads(line.strip())
            try:
                if line['id'] not in data.keys():
                    data[line['id']] = line
            except KeyError:
                pass
        except:
            pass
    return data

def read_actor_series():
    actor_list = read_file_into_list('actors.tsv', False)
    batches = batch_list(actor_list)
    del actor_list
    iterate_batches(batches, 'actor-series', 1298)

def scrape_actor_series(actor):
    url = f"https://api.themoviedb.org/3/person/{actor}/tv_credits?api_key={key}&language=en-US"
    return requests.get(url).text

def write_actor_series_to_file(results):
    with open('actor-series.tsv', 'a', encoding='utf-8') as fout:
        for result in results:
            try:
                result = json.loads(result)
                actor_id = result['id']
                cast = result['cast']
                for c in cast:
                    fout.write(f"{actor_id}\t{c['id']}\n")
            except KeyError:
                pass

def get_series_data():
    series = get_series_from_actor_series()
    batches = batch_list(series)
    del series
    iterate_batches(batches, 'series', -1)

def get_series_from_actor_series():
    data = read_file_into_list('actor-series.tsv', True)
    all_series = {}
    for line in data:
        _, series = line.strip().split('\t')
        if series not in all_series.keys():
            all_series[series] = ''
    series_list = []
    for k in all_series.keys():
        series_list.append(k)
    return series_list

def scrape_series(series):
    url = f'https://api.themoviedb.org/3/tv/{series}?api_key={key}&language=en-US'
    return requests.get(url).text

def use_series_data_to_scrape_episode_cast():
    with open('series_data.tsv', 'r', encoding='utf-8') as fin:
        lines = fin.readlines()
    all_series = []
    for line in lines:
        try:
            line = json.loads(line.strip())
            seasons = line['seasons']
            my_seasons = []
            for s in seasons:
                tmp = (s['season_number'], s['episode_count'])
                my_seasons.append(tmp)
            tmp = (line['id'], my_seasons)
            all_series.append(tmp)
        except KeyError:
            pass
    all_series = build_broken_down_list(all_series)
    batches = batch_list(all_series)
    iterate_batches(batches, 'episode_cast', -1)

def read_in_series_data_to_get_episodes_data():
    with open('series_data.tsv', 'r', encoding='utf-8') as fin:
        lines = fin.readlines()
    all_series = []
    for line in lines:
        try:
            line = json.loads(line.strip())
            seasons = line['seasons']
            my_seasons = []
            for s in seasons:
                tmp = (s['season_number'], s['episode_count'])
                my_seasons.append(tmp)
            tmp = (line['id'], my_seasons)
            all_series.append(tmp)
        except KeyError:
            pass
    all_series = build_broken_down_list(all_series)
    batches = batch_list(all_series)
    iterate_batches(batches, 'episode_data', -1)

def build_broken_down_list(series):
    all_series_matchups = []
    for item in series:
        s = item[0]
        season_eps = item[1]
        for s_e in season_eps:
            season_num = s_e[0]
            episode_count = s_e[1]
            for e in range(0, episode_count+1):
                all_series_matchups.append((s, season_num, e))
    return all_series_matchups