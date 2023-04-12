import kb_data_collection

def main():
    # data_collection.all_processes()
    actors = {}
    movies = {}
    actors[4724] = 0
    # kb_data_collection.read_data_movies(actors, movies, 1)
    kb_data_collection.read_in_actor_and_movie_data()

if __name__=='__main__':
    main()