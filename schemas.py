from pyspark.sql.types import DoubleType, StringType, BooleanType, ArrayType, IntegerType, TimestampType, StringType, StructField, StructType
import paths


# Define schema structures for different datasets using PySpark StructType and StructField
schema = {
    # Structure for the "anime" dataset
    paths.anime_ds:StructType([
        StructField("anime_id", IntegerType(), True),
        StructField('Name', StringType(), True),
        StructField('English name', StringType(), True),
        StructField('Other name', StringType(), True),
        StructField("Score", DoubleType(), True),
        StructField('Genres', StringType(), True),
        StructField('Type', StringType(), True),
        StructField("Episodes", DoubleType(), True),
        StructField('Aired', StringType(), True),
        StructField('Premiered', StringType(), True),
        StructField('Status', StringType(), True),
        StructField('Producers', StringType(), True),
        StructField('Licensors', StringType(), True),
        StructField('Studios', StringType(), True),
        StructField('Source', StringType(), True),
        StructField('Duration', StringType(), True),
        StructField("Rating", StringType(), True),
        StructField("Rank", DoubleType(), True),
        StructField("Popularity", IntegerType(), True),
        StructField("Favorites", IntegerType(), True),
        StructField("Scored By", DoubleType(), True),
        StructField("Members", IntegerType(), True),
        StructField('Image URL', StringType(), True)
    ]),
    # Structure for the "anime_filtered" dataset
    paths.anime_filtered_ds:StructType([
        StructField("anime_id", IntegerType(), True),
        StructField('Name', StringType(), True),
        StructField("Score", DoubleType(), True),
        StructField("Genres", StringType(), True),
        StructField('English name', StringType(), True),
        StructField('Japanese name', StringType(), True),
        StructField('Type', StringType(), True),
        StructField("Episodes", IntegerType(), True),
        StructField('Aired', StringType(), True),
        StructField('Premiered', StringType(), True),
        StructField('Producers', StringType(), True),
        StructField('Licensors', StringType(), True),
        StructField('Studios', StringType(), True),
        StructField('Source', StringType(), True),
        StructField('Duration', StringType(), True),
        StructField("Rating", StringType(), True),
        StructField("Ranked", DoubleType(), True),
        StructField("Popularity", IntegerType(), True),
        StructField("Members", IntegerType(), True),
        StructField("Favorites", IntegerType(), True),
        StructField("Watching", IntegerType(), True),
        StructField("Completed", IntegerType(), True),
        StructField("On-Hold", IntegerType(), True),
        StructField("Dropped", IntegerType(), True)
    ]),
    # Structure for the "final_anime" dataset
    paths.final_anime_ds:StructType([
        StructField('username', StringType(), True),
        StructField("anime_id", IntegerType(), True),
        StructField("my_score", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField('gender', StringType(), True),
        StructField('title', StringType(), True),
        StructField('type', StringType(), True),
        StructField('source', StringType(), True),
        StructField('score', DoubleType(), True),
        StructField('scored_by', IntegerType(), True),
        StructField("rank", DoubleType(), True),
        StructField("popularity", IntegerType(), True),
        StructField("genre", StringType(), True),
    ]),
    # Structure for the "user_rating" dataset
    paths.user_rating_ds:StructType([
        StructField("user_id", IntegerType(), True),
        StructField("anime_id", IntegerType(), True),
        StructField("rating", IntegerType(), True),
    ]),
    # Structure for the "user" dataset
    paths.user_ds:StructType([
        StructField('Mal ID', IntegerType(), True),
        StructField("Username", StringType(), True),
        StructField('Gender', StringType(), True),
        StructField('Birthday', StringType(), True),
        StructField('Location', StringType(), True),
        StructField('Joined', StringType(), True), # TODO this is timestamp
        StructField('Days Watched', DoubleType(), True),
        StructField("Mean Score", DoubleType(), True),
        StructField("Watching", DoubleType(), True),
        StructField("Completed", DoubleType(), True),
        StructField("On Hold", DoubleType(), True),
        StructField("Dropped", DoubleType(), True),
        StructField("Plan to Watch", DoubleType(), True),
        StructField("Total Entries", DoubleType(), True),
        StructField("Rewatched", DoubleType(), True),
        StructField("Episodes Watched", DoubleType(), True),
    ]),
    # Structure for the "username_rating" dataset
    paths.username_rating_ds:StructType([
        StructField('user_id', IntegerType(), True),
        StructField("Username", StringType(), True),
        StructField('anime_id', IntegerType(), True),
        StructField('Anime Title', StringType(), True),
        StructField('rating', IntegerType(), True),
    ])
}
