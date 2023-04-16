import time

import kb_data_collection
import confirmation_testing

def main():
    # data_collection.all_processes()
    actors = {}
    movies = {}
    actors[4724] = 0
    # kb_data_collection.read_data_movies(actors, movies, 1)
    # kb_data_collection.read_in_actor_and_movie_data()
    # confirmation_testing.corrector()
    # kb_data_collection.read_actor_series()
    # kb_data_collection.get_series_data()
    # kb_data_collection.read_in_series_data_to_get_episodes_data()
    # kb_data_collection.use_series_data_to_scrape_episode_cast()
    with open('episode_cast.tsv', 'r', encoding='utf-8') as fin:
        lines = fin.readlines()
    records = []
    duplicates = []
    for line in lines:
        data, _ = line.strip().split('\t')
        if data not in records:
            records.append(data)
        else:
            duplicates.append(data)
    print(len(lines))
    print(len(duplicates))
    print(len(records))


if __name__=='__main__':
    main()