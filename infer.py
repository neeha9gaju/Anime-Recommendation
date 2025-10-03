from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import IntegerType
from pyspark.ml.recommendation import ALSModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row

# Function to recommend anime for a given user
def recommend(model: ALSModel, test_user_id, spark):
    # Create a DataFrame with a single row containing the specified test user ID
    users_df = spark.createDataFrame([Row(user_id=test_user_id)])

    # Get anime recommendations for the user, limiting to 10 recommendations
    recommendations = model.recommendForUserSubset(users_df, 10)
    
    # Initialize an empty list to store anime IDs
    anime_ids = []
    
    # Extract user ID and recommendation information from the result DataFrame
    for row in recommendations.collect():
        user_id = row['user_id']
        # Extract anime IDs from the 'recommendations' column
        anime_ids = [r for r in row['recommendations']]
    
    # Extract the anime IDs from the recommendation list
    anime_ids = [r['anime_id'] for r in anime_ids]

    return anime_ids


# test script
if __name__ == "__main__":
    import paths
    spark = SparkSession.builder.master("local").appName("session").getOrCreate()
    sqlContext = SQLContext(spark)

    model_path = "ALS_Model"
    model = ALSModel.load(model_path)

    test_user_id = 1
    recommendations = recommend(model, test_user_id)
    print(f"Recommendations for user {test_user_id}: {recommendations}")
