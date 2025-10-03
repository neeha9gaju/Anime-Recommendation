import search
import random
import pandas as pd
import time

if __name__ == "__main__":
    samples = input("how many samples:")
    samples = int(samples)
    usernames = pd.read_csv("datasets/users-details-2023.csv", usecols=['Username', 'Mal ID'])
    animenames = pd.read_csv("datasets/anime-dataset-2023.csv", usecols=['anime_id', 'Name'])
    hadoop_total_time = 0
    pandas_total_time = 0
    for i in range(samples):
        if random.randint(0,1) == 0:  # searching username
            if random.randint(0,1) == 0:  # searching by id
                criteria = usernames['Mal ID'].sample(1)
            else:                         # serch by prefix
                sample = usernames['Username'].sample(1).iloc[0]
                criteria = sample[:random.randint(0,len(sample))]
            start = time.time()
            result = search.search_user(criteria)
            hadoop_total_time += time.time() - start
            start = time.time()
            result = usernames[usernames['Mal ID' if isinstance(criteria, int) else 'Username'] == criteria]
            pandas_total_time += time.time() - start
            
        else:                         # searching anime
            if random.randint(0,1) == 0:  # searching by id
                criteria = animenames['anime_id'].sample(1).iloc[0]
            else:
                sample = animenames['Name'].sample(1).iloc[0]
                criteria = sample[:random.randint(0,len(sample))]
            start = time.time()
            result = search.search_anime(criteria)
            hadoop_total_time += time.time() - start
            start = time.time()
            result = animenames[animenames['anime_id' if isinstance(criteria, int) else 'Name'] == criteria]
            pandas_total_time += time.time() - start

    print("hadoop_total_time:", hadoop_total_time)
    print("pandas_total_time:", pandas_total_time)
