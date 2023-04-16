import os
import time

from config import apiKey as key
import requests
import concurrent.futures as futures
import json
import kb_data_collection

def confirming_all_read(file, file2, my_type, remove_file):
    main_list = kb_data_collection.read_file_into_list(file, False)
    data_list = kb_data_collection.read_in_data_file(file2)
    if len(main_list) > len(data_list):
        for item in main_list:
            item = int(item)
            if item not in data_list.keys():
                if my_type == 'actors':
                    result = kb_data_collection.scrape_actor_data(item)
                else:
                    result = kb_data_collection.scrape_movie_data(item)
                if 'success' not in json.loads(result):
                    with open(file2, 'a', encoding='utf-8') as fout:
                        fout.write(f'{result}\n')
                else:
                    with open(remove_file, 'a', encoding='utf-8') as fout:
                        fout.write(f'{item}\n')
    print(len(main_list))
    print(len(data_list))

def remove_to_removes(file, remove_file):
    if os.path.exists(remove_file):
        with open(remove_file, 'r', encoding='utf-8') as fin:
            lines = fin.readlines()
        to_remove = []
        for line in lines:
            to_remove.append(line.strip())
        keeps = []
        data = kb_data_collection.read_file_into_list(file, True)
        for line in data:
            num, phase = line.strip().split('\t')
            if num not in to_remove:
                keeps.append(line.strip())
        with open(file, 'w', encoding='utf-8') as fout:
            for item in keeps:
                fout.write(f'{item}\n')
    if os.path.exists(remove_file):
        os.remove(remove_file)

def corrector():
    confirming_all_read('actors.tsv', 'actor_data.tsv', 'actors', 'actors_to_remove.txt')
    confirming_all_read('movies.tsv', 'movie_data.tsv', 'movies', 'movies_to_remote.txt')
    remove_to_removes('movies.tsv', 'movies_to_remote.txt')
    remove_to_removes('actors.tsv', 'actors_to_remove.txt')
    confirming_all_read('actors.tsv', 'actor_data.tsv', 'actors', 'actors_to_remove.txt')
    confirming_all_read('movies.tsv', 'movie_data.tsv', 'movies', 'movies_to_remote.txt')