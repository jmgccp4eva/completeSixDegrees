from config import apiKey as key
import requests
import gzip
import shutil
import os
import concurrent.futures as futures
import time
import datetime
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
        this_id, data = line.strip().split('\t')
        my_list.append(this_id)
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
    iterate_batches(batches, 'actors', 0)
    batches = batch_list(movie_list)
    del movie_list
    iterate_batches(batches, 'movies', 0)

def scrape_actor_data(actor_id):
    return requests.get(f"https://api.themoviedb.org/3/person/{actor_id}?api_key={key}&language=en-US").text

def scrape_movie_data(movie):
    return requests.get(f"https://api.themoviedb.org/3/{movie}/819?api_key={key}&language=en-US").text

def write_raw_data_to_file(file, results):
    with open(file, 'a', encoding='utf-8') as fout:
        for result in results:
            fout.write(f"{result}\n")

def iterate_batches(batches, this_type, start):
    lng = len(batches)
    for x in range(len(batches)):
        if x >= start:
            if this_type == 'actors':
                results = do_concurrent(scrape_actor_data, batches[x])
                write_raw_data_to_file('actor_data.tsv', results)
            else:
                results = do_concurrent(scrape_movie_data, batches[x])
                write_raw_data_to_file('movie_data.tsv', results)
        print(f"Batch {x} of {lng} complete for {this_type}")