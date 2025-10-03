# Import necessary libraries and modules

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import rand
from pyspark.ml.recommendation import ALSModel
from random import sample

# Import custom modules and files
import paths
from schemas import schema
import consts
from infer import recommend

# Import Flask and CORS for creating a web server
from flask import Flask
from flask_cors import CORS
from flask import jsonify

# Create a Spark session
spark = SparkSession.builder.master("local").appName("session").getOrCreate()

# Load the pre-trained ALS model
model_path = "ALS_Model"
model = ALSModel.load(model_path)

# Define file paths and read data into Spark DataFrame
file_anime = paths.anime_ds
df_anime = spark.read.format('csv').option("header", True).schema(schema[file_anime]).load(file_anime)

# Function to retrieve top-rated items from a DataFrame
def top_rated(df, rating_col_name, id_col_name):
    # Drop rows with missing values in the DataFrame
    # Sort the DataFrame based on the specified rating column
    # Take the top 20 rows with the highest ratings
    result = df.na.drop().sort(rating_col_name).take(20)
    
    # Convert the Spark Rows to dictionaries for easier consumption
    result = [i.asDict() for i in result]
    return result


# Function to search for items by a list of IDs
def ids_search(df_ids, df, id_col_name):
    # Filter the DataFrame to include only rows where the specified ID column matches any of the provided IDs
    result = df.filter(col(id_col_name).isin(df_ids)).collect()
    
    # Convert the Spark Rows to dictionaries for easier consumption
    result = [i.asDict() for i in result]
    return result

# Function to search for an item by a single ID
def id_search(df_id, df, id_col_name, name_col_name):
    # Filter the DataFrame to include only rows where the specified ID column matches the provided ID
    result = df.filter(col(id_col_name) == df_id).collect()
    # Check if exactly one result is found
    if len(result) == 1:
        # If a single result is found, convert the Spark Row to a dictionary and return it
        return result[0].asDict()
    else:
        # If no result or more than one result is found, return an empty dictionary
        return {}

# Function to retrieve random items from a DataFrame based on genre
def random_by_genre(df, genre, genre_col_name, id_col_name):
    # Select the ID column from the DataFrame, filtering by rows where the genre column contains the specified genre
    result = df.select(id_col_name).filter(col(genre_col_name).contains(genre))
    
    # Order the result randomly and limit it to a predefined constant number of results
    result.orderBy(rand()).limit(consts.RESULT_LIMIT).collect()
    
    # Extract the IDs from the result and convert them to a list
    id_list = result.select(id_col_name).rdd.flatMap(lambda x: x).collect()
    
    # Sample a subset of the IDs, limiting to the minimum between the length of the list and a specified maximum
    id_list = sample(id_list, min(len(id_list),20))
    
    # Return the search results using the sampled IDs
    return ids_search(id_list, df, id_col_name)
    
# Function to search for items by a substring in a specific column
def prefix_search(substring, df, col_name, id_col_name):
    # Select the specified columns from the DataFrame, filtering rows where the column contains the specified substring
    result = df.select(col_name, id_col_name).filter(col(col_name).contains(substring)).take(consts.RESULT_LIMIT)
    
    # Convert the Spark Rows to dictionaries, extracting the name and ID from each row
    result = [i.asDict() for i in result] # extract the name and id from spark.sql.Row
    return result
    
# Function to search for anime based on criteria
def search_anime(criteria):
    # Flags to determine the type of search (ID, multiple IDs, or name)
    use_id_flag = False
    multi_ids_flag = False

    # Try converting the criteria to an integer to check if it's a single ID
    try:
        int(criteria)
        use_id_flag = True
    except:
        # If conversion to integer fails, attempt to split the criteria into a list of integers
        try:
            criteria = [int(i.strip()) for i in criteria.split(',')]
            multi_ids_flag = True
            use_id_flag = True
        except:
            # If both attempts fail, criteria is neither a single ID nor a list of IDs
            pass
    
    # Define file paths and column names
    file = paths.anime_ds
    anime_name_column = 'English name'
    anime_id_column = 'anime_id'
    
    # Read anime data into a Spark DataFrame
    df = spark.read.format('csv').option("header", True).schema(schema[file]).load(file)
    
    # Determine the type of search and execute the corresponding function
    if use_id_flag and multi_ids_flag == False:
        # Search by a single ID
        results = id_search(criteria, df, anime_id_column, anime_name_column)
    elif use_id_flag and multi_ids_flag:
        # Search by a multiple IDs
        results = ids_search(criteria, df, anime_id_column, anime_name_column)
    else:
        # Search by anime name, first with title case and then without
        results = prefix_search(criteria.title(), df, anime_name_column, anime_id_column)
        if len(results) == 0:
            results = prefix_search(criteria, df, anime_name_column, anime_id_column)

    return results

# Function to search for users based on criteria
def search_user(criteria):
    # Flags to determine the type of search (ID, multiple IDs, or username)
    use_id_flag = False
    multi_ids_flag = False
    
    # Try converting the criteria to an integer to check if it's a single ID
    try:
        int(criteria)
        use_id_flag = True
    
    # If conversion to integer fails, attempt to split the criteria into a list of integers
    except:
        try:
            criteria = [int(i.strip()) for i in criteria.split(',')]
            multi_ids_flag = True
            use_id_flag = True
        
        # If both attempts fail, criteria is neither a single ID nor a list of IDs
        except:
            pass
    
    # Define file paths and column names for user data
    file = paths.user_ds
    id_column = 'Mal ID'
    name_column = 'Username'
    
    # Read user data into a Spark DataFrame
    df = spark.read.format('csv').option("header", True).schema(schema[file]).load(file)
    
    # Determine the type of search and execute the corresponding function
    if use_id_flag and multi_ids_flag == False:
        # Search by a single user ID
        results = id_search(criteria, df, id_column, name_column)
    elif use_id_flag and multi_ids_flag:
        # Search by a multiple user IDs
        results = ids_search(criteria, df, id_column, name_column)
    else:
        # Search by username, first with the original case and then with title case
        results = prefix_search(criteria, df, name_column, id_column)
        if len(results) == 0:
            results = prefix_search(criteria.title(), df, name_column, id_column)

    return results

# Create Flask app and enable CORS
app = Flask(__name__)
CORS(app)

# Route to get anime information by anime ID
@app.route('/anime/<anime_id>')
def get_anime_info(anime_id):
    result = search_anime(anime_id)
    return jsonify({'anime': result})

# Route to get random anime by genre
@app.route('/genre/<genre_name>')
def get_genres(genre_name):
    file = paths.anime_ds
    df = spark.read.format('csv').option("header", True).schema(schema[file]).load(file)
    result = random_by_genre(df, genre_name.title(), "Genres", "anime_id")
    return jsonify({'anime': result})

# Route to get top-rated anime
@app.route('/top')
def get_top():
    rating_col_name = "rank"
    id_col_name = "anime_id"
    result = top_rated(df_anime, rating_col_name, id_col_name)
    return jsonify({'anime': result})

# Route to get recommended anime for a user
@app.route('/recommend')
def get_recommend():
    user_id = 1
    result = recommend(model, user_id, spark)
    id_col_name = "anime_id"
    result = ids_search(result, df_anime, id_col_name)
    return jsonify({'anime': result})


# Run the Flask app if the script is executed directly
if __name__ == "__main__":
    app.run()